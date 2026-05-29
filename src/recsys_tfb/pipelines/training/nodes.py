"""Pure functions for the training pipeline."""

import logging
import shutil
import time
from pathlib import Path
from typing import Optional

import mlflow
import numpy as np
import optuna
import pandas as pd

from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import (
    compute_macro_per_item_map,
    compute_mean_ap,
)
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.utils.hdfs import copy_hdfs_to_local, get_hive_table_location

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

_CACHE_PATH_LAYOUT: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "calibration_model_input": (
        "base_dataset_version",
        "calibration_variants",
        "calibration_variant_id",
    ),
}


# cache name → source Hive table (under parameters["hive"]["db"])
_CACHE_SOURCE_TABLE: dict[str, str] = {
    "val_model_input": "val_model_input",
    "test_model_input": "test_model_input",
    "train_model_input": "train_model_input",
    "train_dev_model_input": "train_dev_model_input",
    "calibration_model_input": "calibration_model_input",
}

# Outer (string) Hive partitions encoding the variant boundaries.
# Mirrors catalog.yaml's `partition_filter` keys; copy these as the
# subtree root, then `snap_date=*` is the inner glob pattern.
_CACHE_OUTER_PARTITIONS: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variant_id"),
    "calibration_model_input": ("base_dataset_version", "calibration_variant_id"),
}


def _populate_cache_from_hive(
    spark, dataset_name: str, parameters: dict, local_dst: str
) -> None:
    """Copy the relevant Hive partition subtree to driver-local fs.

    Local layout after copy:
        <local_dst>/snap_date=.../prod_name=.../*.parquet

    Source-table resolution:
      1. parameters['_cache_source_tables'][dataset_name] — auto-injected by
         __main__.py:_run_pipeline from catalog_config (HiveTableDataset.table).
         This is the production path and works across envs that prefix table
         names (e.g. 'recsys_prod_train_model_input').
      2. _CACHE_SOURCE_TABLE[dataset_name] — fallback used by unit tests that
         don't go through __main__.py and therefore have no auto-injection.
    """
    db = parameters["hive"]["db"]
    source_tables = parameters.get("_cache_source_tables", {})
    table = source_tables.get(dataset_name, _CACHE_SOURCE_TABLE[dataset_name])
    location = get_hive_table_location(spark, db, table)
    outer = "/".join(
        f"{tok}={parameters[tok]}"
        for tok in _CACHE_OUTER_PARTITIONS[dataset_name]
    )
    src_glob = f"{location.rstrip('/')}/{outer}/snap_date=*"
    copy_hdfs_to_local(spark, src_glob, local_dst, glob=True)


def inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Auto-derive cache source_tables from catalog_config and write into parameters.

    Mutates `parameters` to add `_cache_source_tables` mapping (cache logical
    name → actual Hive table name). Cache nodes read this in
    _populate_cache_from_hive.

    For each known cache name in _CACHE_SOURCE_TABLE, look up the catalog entry.
    If present and `type: HiveTableDataset`, take its `table` field. Skips
    entries that aren't HiveTableDataset and missing entries.

    Operates on raw catalog_config dict (not DataCatalog instance) — the yaml
    schema is the public contract; we don't access dataset instance internals.

    No-op (does not write the key) when no cache entries match.

    Called by __main__.py:_run_pipeline before DataCatalog construction so the
    cache nodes see the auto-derived mapping at runtime.
    """
    auto: dict[str, str] = {}
    for cache_name in _CACHE_SOURCE_TABLE:
        entry = catalog_config.get(cache_name)
        if entry and entry.get("type") == "HiveTableDataset":
            table = entry.get("table")
            if table:
                auto[cache_name] = table
    if auto:
        parameters["_cache_source_tables"] = auto


def _resolve_cache_path(dataset_name: str, parameters: dict) -> str:
    """Compose the local-cache parquet directory path for a model_input dataset.

    Mirrors the layered structure used by production catalog filepaths:
      <root>/<base_dataset_version>/[train_variants/<train_variant_id>/]<name>.parquet
    """
    if dataset_name not in _CACHE_PATH_LAYOUT:
        raise ValueError(f"unknown dataset for cache path: {dataset_name!r}")
    cache_cfg = parameters.get("cache", {})
    root = Path(cache_cfg.get("root", "/tmp/recsys_cache"))
    parts = [root]
    for token in _CACHE_PATH_LAYOUT[dataset_name]:
        if token in ("train_variants", "calibration_variants"):
            parts.append(Path(token))
        else:
            value = parameters[token]
            parts.append(Path(value))
    parts.append(Path(f"{dataset_name}.parquet"))
    full = parts[0]
    for p in parts[1:]:
        full = full / p
    return str(full)


def _materialize_parquet_handle(
    df, dataset_name: str, parameters: dict
) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for a single model_input.

    Behaviour:
      - df is not a Spark DataFrame  → TypeError (pandas-passthrough removed)
      - target path has _SUCCESS  → return ParquetHandle pointing at it
      - target path exists but no _SUCCESS  → rmtree and rebuild
      - cache miss  → hadoop fs copyToLocal HDFS subtree to driver-local;
                      touch _SUCCESS; return ParquetHandle
    """
    if not hasattr(df, "sql_ctx"):
        raise TypeError(
            f"{dataset_name} input must be a Spark DataFrame; got "
            f"{type(df).__name__}. cache.enabled=false passthrough has been "
            "removed; all environments (including dev/test) must use a "
            "writable cache.root."
        )

    local_path = _resolve_cache_path(dataset_name, parameters)
    success_marker = Path(local_path) / "_SUCCESS"

    if Path(local_path).exists() and not success_marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    if not success_marker.exists():
        spark = df.sql_ctx.sparkSession
        logger.info("cache_miss name=%s path=%s", dataset_name, local_path)
        _populate_cache_from_hive(spark, dataset_name, parameters, local_path)
        success_marker.touch()
    else:
        logger.info("cache_hit name=%s path=%s", dataset_name, local_path)

    return ParquetHandle(path=local_path)


# ---------------------------------------------------------------------------
# Cache nodes
# ---------------------------------------------------------------------------

def cache_train_model_input(train_model_input, parameters: dict) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for train_model_input."""
    return _materialize_parquet_handle(train_model_input, "train_model_input", parameters)


def cache_train_dev_model_input(train_dev_model_input, parameters: dict) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for train_dev_model_input."""
    return _materialize_parquet_handle(
        train_dev_model_input, "train_dev_model_input", parameters
    )


def cache_val_model_input(val_model_input, parameters: dict) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for val_model_input."""
    return _materialize_parquet_handle(val_model_input, "val_model_input", parameters)


def cache_test_model_input(test_model_input, parameters: dict) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for test_model_input."""
    return _materialize_parquet_handle(test_model_input, "test_model_input", parameters)


def cache_calibration_model_input(calibration_model_input, parameters: dict) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for calibration_model_input."""
    return _materialize_parquet_handle(
        calibration_model_input, "calibration_model_input", parameters
    )


def prepare_lgb_train_inputs(
    train_parquet_handle: ParquetHandle,
    train_dev_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
):
    """Materialize lgb.Dataset binaries for train + train_dev.

    Delegates to the configured ModelAdapter's prepare_train_inputs. The
    cache_dir uses the same train_variant directory as the parquet cache,
    placing 'lgb/' as a sibling of the parquets.
    """
    algorithm = parameters["training"].get("algorithm", "lightgbm")
    adapter = get_adapter(algorithm)

    cache_root = parameters["cache"]["root"]
    base_v = parameters["base_dataset_version"]
    train_v = parameters["train_variant_id"]
    cache_dir = Path(cache_root) / base_v / "train_variants" / train_v

    return adapter.prepare_train_inputs(
        train_parquet_handle,
        train_dev_parquet_handle,
        preprocessor_metadata,
        parameters,
        str(cache_dir),
    )


# ---------------------------------------------------------------------------
# Pipeline nodes
# ---------------------------------------------------------------------------

HPO_OBJECTIVES = ("mean_ap", "macro_per_item_map")


def _hpo_score(
    objective_name: str,
    groups: np.ndarray,
    items: Optional[np.ndarray],
    y_true: np.ndarray,
    y_score: np.ndarray,
) -> float:
    """Score val predictions for one HPO trial under the chosen objective.

    ``mean_ap``            — per-query mAP (``items`` unused).
    ``macro_per_item_map`` — macro average of per-item attributed mAP.

    Unknown ``objective_name`` raises ``ValueError`` (fail-loud).
    """
    if objective_name == "mean_ap":
        return compute_mean_ap(groups, y_true, y_score)
    if objective_name == "macro_per_item_map":
        return compute_macro_per_item_map(groups, items, y_true, y_score)
    raise ValueError(
        f"unknown training.hpo_objective {objective_name!r}; "
        f"allowed: {', '.join(HPO_OBJECTIVES)}"
    )


def tune_hyperparameters(
    train_lgb_handle,
    train_dev_lgb_handle,
    val_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[dict, int, ModelAdapter]:
    """Search for optimal hyperparameters using Optuna and return best trial's model.

    train + train_dev consumed as pre-built lgb.Dataset binaries (no rebinning
    across trials). val read fresh from parquet inside this scope so its pandas
    DataFrame is freed when the function returns.

    Returns (best_params, best_iteration, best_model). best_iteration is the
    booster's best_iteration on the winning trial (the early-stopping pick when
    triggered, otherwise the iteration with the lowest val loss within
    num_iterations). It is consumed by `finalize_model` under the
    `refit_on_full` strategy as the fixed iteration count for the no-val refit.
    """
    from recsys_tfb.io.extract import extract_Xy_with_groups

    training_params = parameters["training"]
    n_trials = training_params["n_trials"]
    search_space = training_params["search_space"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")

    hpo_objective = training_params.get("hpo_objective", "mean_ap")
    if hpo_objective not in HPO_OBJECTIVES:
        raise ValueError(
            f"unknown training.hpo_objective {hpo_objective!r}; "
            f"allowed: {', '.join(HPO_OBJECTIVES)}"
        )

    from recsys_tfb.core.group_utils import default_metric_for_objective
    from recsys_tfb.pipelines.training.search_space import build_trial_params

    # Local copy: defaulting the ranking metric must not mutate the shared
    # `parameters` dict (it is still written verbatim to manifest.json).
    algorithm_params = dict(training_params.get("algorithm_params", {}))
    _metric = default_metric_for_objective(
        algorithm_params.get("objective"), algorithm_params.get("metric")
    )
    if _metric:
        algorithm_params["metric"] = _metric

    # val_model_input is already pre-filtered to positive groups by the dataset
    # pipeline (filter_val_model_input node) — no in-pandas re-filter here.
    with log_step(logger, "extract_features"):
        if hpo_objective == "macro_per_item_map":
            X_v, y_v, groups_v, items_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
                with_items=True,
            )
        else:
            X_v, y_v, groups_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
            )
            items_v = None

    best_state: dict = {"score": -1.0, "model": None, "iteration": 0}

    def objective(trial: optuna.Trial) -> float:
        trial_idx = trial.number
        trial_params = build_trial_params(trial, search_space)

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        logger.info(
            "tune_hyperparameters: trial=%d/%d start params=%s",
            trial_idx, n_trials, trial_params,
        )
        t0 = time.monotonic()

        adapter = get_adapter(algorithm)

        # feature_pre_filter=False must match the construct_params used when
        # writing the .bin (LightGBMAdapter.prepare_train_inputs); otherwise
        # lgb.train hits "Cannot change feature_pre_filter after constructed".
        # Required because min_child_samples is in the HPO search space and a
        # pre-filtered Dataset would be tied to one specific value.
        construct_params = {"feature_pre_filter": False}
        with log_step(logger, "prepare_datasets"):
            ds_train = train_lgb_handle.load(params=construct_params).construct()
            ds_dev = train_dev_lgb_handle.load(
                reference=ds_train, params=construct_params
            ).construct()
        log_data_volume(logger, "tune.ds_train", ds_train)
        log_data_volume(logger, "tune.ds_dev", ds_dev)

        with log_step(logger, "train"):
            adapter.train(
                X_train=None, y_train=None, X_val=None, y_val=None,
                params=params,
                train_dataset=ds_train, val_dataset=ds_dev,
            )

        with log_step(logger, "predict"):
            y_pred = adapter.predict(X_v)

        with log_step(logger, "score"):
            score = _hpo_score(hpo_objective, groups_v, items_v, y_v, y_pred)

        if score > best_state["score"]:
            best_state["score"] = score
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed score=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, n_trials, score,
            adapter.booster.best_iteration, duration, best_state["score"],
        )

        return score

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    with log_step(logger, "optuna_optimize"):
        study.optimize(objective, n_trials=n_trials)

    best_params = study.best_params
    best_model = best_state["model"]
    best_iteration = best_state["iteration"]
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, study.best_value, best_iteration, best_params,
    )
    return best_params, best_iteration, best_model


def finalize_model(
    train_parquet_handle,
    train_dev_parquet_handle,
    hpo_best_model: ModelAdapter,
    best_params: dict,
    best_iteration: int,
    preprocessor_metadata: dict,
    parameters: dict,
) -> ModelAdapter:
    """Produce the final model based on `training.final_model_strategy`.

    Strategies:
      hpo_best (default): pass the HPO best-trial adapter through unchanged.
        Cheapest path; identical to Phase 1 behavior. Best-iteration value is
        whatever the early-stopping callback selected during HPO.

      refit_on_full: retrain on train + train_dev concatenated, with
        num_iterations = best_iteration (HPO winner's stopping point) and no
        early-stopping. Trades the HPO val signal for ~25% more training data
        (train_dev_ratio=0.2 default). Same hyperparameters; deterministic
        given (best_params, best_iteration, seed).
    """
    strategy = parameters.get("training", {}).get("final_model_strategy", "hpo_best")

    if strategy == "hpo_best":
        logger.info("final_model_strategy=hpo_best (passthrough; best_iteration=%d)", best_iteration)
        return hpo_best_model

    if strategy != "refit_on_full":
        raise ValueError(
            f"Unknown training.final_model_strategy={strategy!r}. "
            "Expected 'hpo_best' or 'refit_on_full'."
        )

    import lightgbm as lgb
    import numpy as np

    from recsys_tfb.core.group_utils import (
        default_metric_for_objective,
        is_ranking_objective,
        to_contiguous_groups,
    )

    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = dict(training_params.get("algorithm_params", {}))
    objective = algorithm_params.get("objective")
    _metric = default_metric_for_objective(
        objective, algorithm_params.get("metric")
    )
    if _metric:
        algorithm_params["metric"] = _metric

    logger.info(
        "final_model_strategy=refit_on_full (num_iterations=%d, no early stopping)",
        best_iteration,
    )

    feat_cols = preprocessor_metadata["feature_columns"]
    cat_cols = preprocessor_metadata.get("categorical_columns", [])
    cat_idx = [feat_cols.index(c) for c in cat_cols if c in feat_cols] or None

    if is_ranking_objective(objective):
        from recsys_tfb.io.extract import extract_Xy_with_groups

        with log_step(logger, "extract_features"):
            X_tr, y_tr, gid_tr, w_tr = extract_Xy_with_groups(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            X_dv, y_dv, gid_dv, w_dv = extract_Xy_with_groups(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        # train / train_dev are customer-disjoint by sampling design, so a
        # query group never spans both splits — offset dev ids past train's
        # max to keep them distinct after concatenation.
        offset = (int(gid_tr.max()) + 1) if len(gid_tr) else 0
        X_full = np.concatenate([X_tr, X_dv], axis=0)
        y_full = np.concatenate([y_tr, y_dv], axis=0)
        w_full = np.concatenate([w_tr, w_dv])
        gid_full = np.concatenate([gid_tr, gid_dv + offset])
        log_data_volume(logger, "finalize.X_full", X_full)
        log_data_volume(logger, "finalize.y_full", y_full)
        del X_tr, y_tr, X_dv, y_dv, gid_tr, gid_dv, w_tr, w_dv

        perm, grp = to_contiguous_groups(gid_full)
        # feature_pre_filter=False: matches HPO's lgb.Dataset binaries (binned
        # with the same construct param) so refit's splits use the same feature
        # set. group= makes this a ranking refit consistent with the objective.
        ds_full = lgb.Dataset(
            X_full[perm],
            label=y_full[perm],
            weight=w_full[perm],
            group=grp,
            categorical_feature=cat_idx,
            params={"feature_pre_filter": False},
            free_raw_data=True,
        )
    else:
        from recsys_tfb.io.extract import extract_Xy

        with log_step(logger, "extract_features"):
            X_tr, y_tr, w_tr = extract_Xy(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            X_dv, y_dv, w_dv = extract_Xy(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        X_full = np.concatenate([X_tr, X_dv], axis=0)
        y_full = np.concatenate([y_tr, y_dv], axis=0)
        w_full = np.concatenate([w_tr, w_dv])
        log_data_volume(logger, "finalize.X_full", X_full)
        log_data_volume(logger, "finalize.y_full", y_full)
        del X_tr, y_tr, X_dv, y_dv, w_tr, w_dv

        # feature_pre_filter=False: matches HPO's lgb.Dataset binaries (binned
        # with the same construct param) so refit's tree splits use the same
        # feature set.
        ds_full = lgb.Dataset(
            X_full,
            label=y_full,
            weight=w_full,
            categorical_feature=cat_idx,
            params={"feature_pre_filter": False},
            free_raw_data=True,
        )

    params = {
        **algorithm_params,
        "seed": seed,
        "feature_pre_filter": False,
        **best_params,
        "num_iterations": best_iteration,
        "early_stopping_rounds": 0,
    }

    with log_step(logger, "model_refit"):
        adapter = get_adapter(algorithm)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_full,
        )

    logger.info(
        "Refitted on full train+train_dev (n=%d, iterations=%d)",
        len(y_full), best_iteration,
    )
    return adapter


def calibrate_model(
    model: ModelAdapter,
    calibration_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> ModelAdapter:
    """Wrap model with probability calibration."""
    from recsys_tfb.io.extract import extract_Xy

    method = (
        parameters.get("training", {})
        .get("calibration", {})
        .get("method", "isotonic")
    )

    with log_step(logger, "extract_features"):
        X_cal, y_cal = extract_Xy(
            calibration_parquet_handle, preprocessor_metadata, parameters
        )

    with log_step(logger, "fit_calibrator"):
        calibrated = CalibratedModelAdapter(model, method=method)
        calibrated.fit_calibrator(X_cal, y_cal)

    logger.info(
        "Model calibrated: method=%s, n_samples=%d", method, len(y_cal)
    )
    return calibrated



def predict_and_write_test_predictions(
    model: ModelAdapter,
    test_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    training_eval_predictions,  # HiveTableDataset, supplied via @ runner prefix
) -> dict:
    """Per-partition test prediction + Hive write.

    For each (snap_date, prod_name) partition of the parquet:
        - load only that partition's rows via pyarrow filter
        - slice X via _pdf_to_X; predict; (predict_uncalibrated if Calibrated)
        - build a pandas DataFrame with (cust_id, score, score_uncalibrated,
          label) + partition cols snap_date, prod_name
        - training_eval_predictions.save(df) — exactly one partition's
          rows per save, so dynamic-partition overwrite cleanly overwrites
          a single partition and successive saves don't collide

    test_model_input is pre-filtered upstream (filter_test_model_input node
    in dataset pipeline) so every (snap_date, cust_id) group already has
    at least one positive label.

    Returns:
        manifest dict for downstream compute_test_mAP_spark to depend on
        (DAG ordering — the actual data is read back from Hive there).
    """
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _pdf_to_X

    schema_cfg = get_schema(parameters)
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    item_col = schema_cfg["item"]
    label_col = schema_cfg["label"]
    if len(entity_cols) != 1:
        raise ValueError(
            f"predict_and_write_test_predictions expects single entity column; "
            f"got {entity_cols}."
        )
    cust_id_col = entity_cols[0]
    model_version = parameters["model_version"]

    # partitioning="hive" tells pyarrow to reconstruct (snap_date, prod_name)
    # columns from the snap_date=*/prod_name=* directory tree produced by
    # HiveTableDataset.save() (and by the test fixture's pq.write_to_dataset).
    ds = pads.dataset(test_parquet_handle.path, format="parquet", partitioning="hive")

    # Enumerate distinct (snap_date, prod_name) values by projecting just the
    # two partition columns and de-duplicating. Note: select-on-partition-cols
    # in pyarrow still materializes one row per data row (the values are filled
    # from directory names per fragment), so this is two-string-columns-wide,
    # not zero I/O. At production scale (~220M rows × 2 short strings) the
    # transient DataFrame fits comfortably on the 128GB driver — much cheaper
    # than reading any feature columns — and drop_duplicates collapses it to
    # n_snap_dates * n_prods rows immediately.
    partition_table = ds.to_table(columns=[time_col, item_col])
    log_data_volume(logger, "predict.partition_table", partition_table)
    partition_pdf = partition_table.to_pandas()
    log_data_volume(logger, "predict.partition_pdf", partition_pdf, deep=False)
    partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])
    log_data_volume(logger, "predict.partition_pdf_unique", partition_pdf, deep=False)

    snap_dates_seen: set[str] = set()
    prods_seen: set[str] = set()
    n_rows_written = 0
    is_calibrated = isinstance(model, CalibratedModelAdapter)

    for _, row in partition_pdf.iterrows():
        snap_date = str(row[time_col])
        prod_name = str(row[item_col])

        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
            part_table = ds.to_table(
                filter=(pads.field(time_col) == snap_date)
                & (pads.field(item_col) == prod_name)
            )
            log_data_volume(
                logger, f"predict.part_table[{snap_date}/{prod_name}]", part_table
            )
            part_pdf = part_table.to_pandas()
            log_data_volume(
                logger, f"predict.part_pdf[{snap_date}/{prod_name}]",
                part_pdf, deep=True,
            )

            snap_dates_seen.add(snap_date)
            prods_seen.add(prod_name)

            X = _pdf_to_X(part_pdf, preprocessor_metadata, parameters)
            y_score = model.predict(X)
            score_uncalibrated = (
                model.predict_uncalibrated(X) if is_calibrated else y_score
            )

            out_pdf = pd.DataFrame({
                cust_id_col: part_pdf[cust_id_col].astype(str).values,
                "score": y_score,
                "score_uncalibrated": score_uncalibrated,
                label_col: part_pdf[label_col].values,
                time_col: snap_date,
                item_col: prod_name,
            })

            training_eval_predictions.save(out_pdf)
            n_rows_written += len(out_pdf)

    manifest = {
        "snap_dates": sorted(snap_dates_seen),
        "prods": sorted(prods_seen),
        "model_version": model_version,
        "n_rows_written": n_rows_written,
    }
    logger.info(
        "predict_and_write_test_predictions: done — "
        "snap_dates=%d prods=%d n_rows_written=%d model_version=%s",
        len(manifest["snap_dates"]), len(manifest["prods"]),
        manifest["n_rows_written"], manifest["model_version"],
    )
    return manifest


def log_experiment(
    model: ModelAdapter,
    best_params: dict,
    best_iteration: int,
    evaluation_results: dict,
    feature_statistics: dict,
    feature_importance: dict,
    shap_diagnostics: dict,
    parameters: dict,
) -> None:
    """Log training results to MLflow."""
    from recsys_tfb.pipelines.training.diagnostics import diagnostics_dir
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")
    # MLflow logging 是 best-effort 的 sink node（DAG 終端、無下游依賴）。
    # tracking server 不可用或版本不相容（例如 client 3.x 對舊 server 呼叫
    # /api/2.0/mlflow/logged-models 收到 404）時，預設記 warning 後讓 pipeline
    # 跑完，不讓 experiment logging 拖垮整個 training。需硬失敗時設 strict: true。
    strict = mlflow_params.get("strict", False)
    training_cfg = parameters.get("training", {})
    algorithm = training_cfg.get("algorithm", "lightgbm")
    final_model_strategy = training_cfg.get("final_model_strategy", "hpo_best")

    try:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        with log_step(logger, "mlflow_log"):
            with mlflow.start_run():
                mlflow.log_params(best_params)
                mlflow.log_param("algorithm", algorithm)
                mlflow.log_param("final_model_strategy", final_model_strategy)
                mlflow.log_metric("best_iteration", best_iteration)
                mlflow.log_metric("overall_map", evaluation_results["overall_map"])

                for item, attr in evaluation_results.get("per_item_map_attr", {}).items():
                    mlflow.log_metric(f"map_attr_{item}", attr)

                mlflow.log_metric("n_queries", evaluation_results["n_queries"])
                mlflow.log_metric("n_excluded_queries", evaluation_results["n_excluded_queries"])

                # Calibration info
                if "uncalibrated" in evaluation_results:
                    mlflow.log_param("calibrated", True)
                    mlflow.log_param("calibration_method", evaluation_results["calibration_method"])
                    mlflow.log_metric(
                        "uncalibrated_overall_map",
                        evaluation_results["uncalibrated"]["overall_map"],
                    )
                else:
                    mlflow.log_param("calibrated", False)

                model.log_to_mlflow()

                # --- diagnostics scalar summary ---
                if feature_importance:
                    mlflow.log_metric("n_dead_features", len(feature_importance.get("dead_features", [])))
                if feature_statistics:
                    mlflow.log_metric(
                        "n_single_value_features",
                        sum(1 for s in feature_statistics.values() if s.get("single_value")),
                    )
                    mlflow.log_metric(
                        "n_high_null_features",
                        sum(1 for s in feature_statistics.values() if s.get("high_null")),
                    )

                # --- diagnostics artifacts (JSON written by catalog, PNG by shap node;
                #     upload the whole dir) ---
                diag_dir = diagnostics_dir(parameters)
                if diag_dir.exists():
                    mlflow.log_artifacts(str(diag_dir))

        logger.info("MLflow experiment logged: %s", experiment_name)
    except Exception:
        if strict:
            raise
        logger.warning(
            "MLflow logging failed; training pipeline continues without "
            "experiment logging (set mlflow.strict=true to fail hard). "
            "tracking_uri=%s experiment=%s",
            tracking_uri,
            experiment_name,
            exc_info=True,
        )


def compute_test_mAP_spark(
    training_eval_predictions,  # Spark DataFrame, loaded by catalog (filtered to current model_version)
    predict_manifest: dict,
    parameters: dict,
) -> dict:
    """Spark-native mAP over training_eval_predictions; emits the dict
    shape consumed by log_experiment.

    Keys (post metrics-spark redesign):
        overall_map        per-query mAP@n_products averaged across queries
                           (mean of per-query AP@all)
        per_item_map_attr  {item: mean(ap_contrib@all) over item-positive rows}
                           — replaces the old per_product_ap; carries the same
                           interpretation when n_products dimension is full.
        n_queries / n_excluded_queries
        uncalibrated       (only when score != score_uncalibrated) sub-dict with
                           overall_map / per_item_map_attr in the same shape
        calibration_method (only when calibration was applied)

    predict_manifest is an in-DAG dependency only — its content is logged
    for observability but the actual data is read back from
    training_eval_predictions (Spark-loaded via the catalog).
    """
    from pyspark.sql import functions as F

    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]

    n_prods = training_eval_predictions.select(item_col).distinct().count()
    overall_map_key = f"map@{n_prods}"
    item_map_attr_key = f"map_attr@{n_prods}"

    logger.info(
        "compute_test_mAP_spark: starting — n_prods=%d overall_key=%s item_key=%s manifest=%s",
        n_prods, overall_map_key, item_map_attr_key, predict_manifest,
    )

    calibration_applied = (
        training_eval_predictions.filter(
            F.col("score") != F.col("score_uncalibrated")
        )
        .limit(1)
        .count()
        > 0
    )

    cal = compute_all_metrics(training_eval_predictions, parameters)
    result = {
        "overall_map": float(cal["overall"].get(overall_map_key, 0.0)),
        "per_item_map_attr": {
            p: float(v.get(item_map_attr_key, 0.0))
            for p, v in cal["per_item"].items()
        },
        "n_queries": cal["n_queries"],
        "n_excluded_queries": cal["n_excluded_queries"],
    }

    if calibration_applied:
        uncal_df = (
            training_eval_predictions
            .withColumnRenamed("score", "_score_calibrated")
            .withColumnRenamed("score_uncalibrated", "score")
        )
        uncal = compute_all_metrics(uncal_df, parameters)
        result["uncalibrated"] = {
            "overall_map": float(uncal["overall"].get(overall_map_key, 0.0)),
            "per_item_map_attr": {
                p: float(v.get(item_map_attr_key, 0.0))
                for p, v in uncal["per_item"].items()
            },
        }
        result["calibration_method"] = (
            parameters.get("training", {}).get("calibration", {}).get("method", "isotonic")
        )
        logger.info(
            "compute_test_mAP_spark: calibrated=%.4f uncalibrated=%.4f",
            result["overall_map"], result["uncalibrated"]["overall_map"],
        )
    else:
        logger.info(
            "compute_test_mAP_spark: mAP=%.4f items=%d excluded_queries=%d",
            result["overall_map"],
            len(result["per_item_map_attr"]),
            result["n_excluded_queries"],
        )

    return result

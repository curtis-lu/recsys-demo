"""Pure functions for the training pipeline."""

import logging
import shutil
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import compute_all_metrics, compute_ap
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.utils.hdfs import copy_hdfs_to_local, get_hive_table_location
from recsys_tfb.utils.spark import get_or_create_spark_session

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
         names (e.g. company prod 'recsys_prod_train_model_input').
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
    from recsys_tfb.io.extract import extract_Xy

    training_params = parameters["training"]
    n_trials = training_params["n_trials"]
    search_space = training_params["search_space"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    with log_step(logger, "extract_features"):
        X_v, y_v = extract_Xy(val_parquet_handle, preprocessor_metadata, parameters)

    best_state: dict = {"ap": -1.0, "model": None, "iteration": 0}

    def objective(trial: optuna.Trial) -> float:
        trial_params = {
            "learning_rate": trial.suggest_float(
                "learning_rate",
                search_space["learning_rate"]["low"],
                search_space["learning_rate"]["high"],
                log=True,
            ),
            "num_leaves": trial.suggest_int(
                "num_leaves",
                search_space["num_leaves"]["low"],
                search_space["num_leaves"]["high"],
            ),
            "max_depth": trial.suggest_int(
                "max_depth",
                search_space["max_depth"]["low"],
                search_space["max_depth"]["high"],
            ),
            "min_child_samples": trial.suggest_int(
                "min_child_samples",
                search_space["min_child_samples"]["low"],
                search_space["min_child_samples"]["high"],
            ),
            "subsample": trial.suggest_float(
                "subsample",
                search_space["subsample"]["low"],
                search_space["subsample"]["high"],
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                search_space["colsample_bytree"]["low"],
                search_space["colsample_bytree"]["high"],
            ),
        }

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
        ap = ap if ap is not None else 0.0

        if ap > best_state["ap"]:
            best_state["ap"] = ap
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        return ap

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    with log_step(logger, "optuna_optimize"):
        study.optimize(objective, n_trials=n_trials)

    best_params = study.best_params
    best_model = best_state["model"]
    best_iteration = best_state["iteration"]
    logger.info(
        "Best trial mAP: %.4f, best_iteration: %d, params: %s",
        study.best_value, best_iteration, best_params,
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
    from recsys_tfb.io.extract import extract_Xy

    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    logger.info(
        "final_model_strategy=refit_on_full (num_iterations=%d, no early stopping)",
        best_iteration,
    )

    with log_step(logger, "extract_features"):
        X_tr, y_tr = extract_Xy(train_parquet_handle, preprocessor_metadata, parameters)
        X_dv, y_dv = extract_Xy(train_dev_parquet_handle, preprocessor_metadata, parameters)
    X_full = np.concatenate([X_tr, X_dv], axis=0)
    y_full = np.concatenate([y_tr, y_dv], axis=0)
    del X_tr, y_tr, X_dv, y_dv

    feat_cols = preprocessor_metadata["feature_columns"]
    cat_cols = preprocessor_metadata.get("categorical_columns", [])
    cat_idx = [feat_cols.index(c) for c in cat_cols if c in feat_cols] or None

    # feature_pre_filter=False: matches HPO's lgb.Dataset binaries (binned with
    # the same construct param) so refit's tree splits use the same feature set.
    ds_full = lgb.Dataset(
        X_full,
        label=y_full,
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


def evaluate_model(
    model: ModelAdapter,
    eval_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Predict on the test set and rank within each query group.

    Returns:
        (predictions_pdf, labels_pdf):
          predictions_pdf — identity_columns + [score, score_uncalibrated, rank]
          labels_pdf      — identity_columns + [label]

    Downstream nodes consume the predictions/labels separately:
      - write_test_predictions persists predictions to Hive
      - compute_test_mAP computes the dict consumed by MLflow

    score_uncalibrated semantics: always the raw model output. For calibrated
    runs it differs from score; for non-calibrated runs it equals score.
    """
    from recsys_tfb.io.extract import extract_Xy

    schema_cfg = get_schema(parameters)
    score_col = schema_cfg["score"]
    rank_col = schema_cfg["rank"]
    label_col = schema_cfg["label"]
    identity_cols = schema_cfg["identity_columns"]
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    group_cols = [time_col] + entity_cols

    eval_pdf = eval_parquet_handle.to_pandas()

    with log_step(logger, "extract_features"):
        X, _ = extract_Xy(eval_parquet_handle, preprocessor_metadata, parameters)

    with log_step(logger, "predict"):
        y_score = model.predict(X)

    predictions_pdf = eval_pdf[identity_cols].reset_index(drop=True).copy()
    predictions_pdf[score_col] = y_score
    predictions_pdf[rank_col] = (
        predictions_pdf.groupby(group_cols)[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    # score_uncalibrated: always the raw model output, regardless of calibration.
    if isinstance(model, CalibratedModelAdapter):
        with log_step(logger, "predict_uncalibrated"):
            predictions_pdf["score_uncalibrated"] = model.predict_uncalibrated(X)
    else:
        predictions_pdf["score_uncalibrated"] = y_score

    labels_pdf = eval_pdf[identity_cols + [label_col]].reset_index(drop=True).copy()

    logger.info(
        "evaluate_model: predicted %d rows, %d queries, calibrated=%s",
        len(predictions_pdf),
        predictions_pdf[group_cols].drop_duplicates().shape[0],
        isinstance(model, CalibratedModelAdapter),
    )
    return predictions_pdf, labels_pdf


def compute_test_mAP(
    test_predictions_pdf: pd.DataFrame,
    test_labels_pdf: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking-aware mAP from test-set predictions; feed log_experiment.

    test_predictions_pdf must contain identity_columns + [score, score_uncalibrated, rank].
    test_labels_pdf must contain identity_columns + [label].

    When score and score_uncalibrated differ (i.e., calibration applied), emits
    an additional ``uncalibrated`` sub-dict for MLflow comparison.
    """
    schema_cfg = get_schema(parameters)
    score_col = schema_cfg["score"]
    label_col = schema_cfg["label"]
    item_col = schema_cfg["item"]
    identity_cols = schema_cfg["identity_columns"]

    merged = test_predictions_pdf.merge(test_labels_pdf, on=identity_cols, how="inner")

    def _calc_metrics(score_column_name: str) -> dict:
        preds = merged[identity_cols + [score_column_name]].rename(
            columns={score_column_name: score_col}
        )
        labs = merged[identity_cols + [label_col]]
        m = compute_all_metrics(preds, labs, k_values=["all"])
        n_products = preds[item_col].nunique()
        map_key = f"map@{n_products}"
        return {
            "overall_map": m["overall"].get(map_key, 0.0),
            "per_product_ap": {
                p: v.get(map_key, 0.0) for p, v in m["per_product"].items()
            },
            "n_queries": m["n_queries"],
            "n_excluded_queries": m["n_excluded_queries"],
        }

    cal = _calc_metrics(score_col)
    evaluation_results = {
        "overall_map": cal["overall_map"],
        "per_product_ap": cal["per_product_ap"],
        "n_queries": cal["n_queries"],
        "n_excluded_queries": cal["n_excluded_queries"],
    }

    # score_uncalibrated is always present (per evaluate_model contract).
    # Only emit the uncalibrated comparison subdict when calibration was
    # actually applied — i.e. when the two columns differ.
    calibration_applied = (
        "score_uncalibrated" in test_predictions_pdf.columns
        and not (
            test_predictions_pdf[score_col]
            == test_predictions_pdf["score_uncalibrated"]
        ).all()
    )
    if calibration_applied:
        uncal = _calc_metrics("score_uncalibrated")
        evaluation_results["uncalibrated"] = {
            "overall_map": uncal["overall_map"],
            "per_product_ap": uncal["per_product_ap"],
        }
        logger.info(
            "compute_test_mAP: uncalibrated mAP=%.4f vs calibrated mAP=%.4f",
            uncal["overall_map"], cal["overall_map"],
        )

    logger.info(
        "compute_test_mAP: mAP=%.4f, products=%d, excluded_queries=%d",
        cal["overall_map"],
        len(cal["per_product_ap"]),
        cal["n_excluded_queries"],
    )
    return evaluation_results


def _build_training_eval_predictions_ddl(table_fqn: str) -> str:
    """CREATE TABLE IF NOT EXISTS DDL — schema matches catalog declaration.

    score_uncalibrated semantics: always raw model output. For calibrated runs
    it differs from score; for non-calibrated runs it equals score.
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
        cust_id STRING,
        score DOUBLE,
        score_uncalibrated DOUBLE,
        `rank` BIGINT
    )
    PARTITIONED BY (snap_date STRING, prod_name STRING, model_version STRING)
    STORED AS PARQUET
    """.strip()


def write_test_predictions(
    test_predictions_pdf: pd.DataFrame,
    parameters: dict,
) -> None:
    """Write test-set predictions to Hive, iterating per prod_name for memory control.

    Bypasses catalog auto-save: outputs=None at DAG level. The catalog entry
    `training_eval_predictions` declares the table for downstream evaluation reads;
    this function owns the writes (DDL bootstrap + per-prod insertInto).
    """
    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]
    score_col = schema_cfg["score"]
    rank_col = schema_cfg["rank"]
    time_col = schema_cfg["time"]
    identity_cols = schema_cfg["identity_columns"]
    spark = get_or_create_spark_session()
    model_version = parameters["model_version"]
    hive_db = parameters["hive"]["db"]
    table_fqn = f"{hive_db}.training_eval_predictions"

    if "score_uncalibrated" not in test_predictions_pdf.columns:
        raise RuntimeError(
            "test_predictions_pdf missing 'score_uncalibrated' column. "
            "evaluate_model must populate it (= score for non-calibrated runs)."
        )

    # Extract cust_id (entity col) from identity_cols = [time, *entity, item].
    cust_id_col = next(c for c in identity_cols if c not in {item_col, time_col})
    # Column order matches Hive table: non-partition cols first, then partition
    # cols (snap_date, prod_name) and finally model_version. Dynamic-partition
    # insertInto uses positional column mapping.
    write_cols = [
        cust_id_col,
        score_col,
        "score_uncalibrated",
        rank_col,
        time_col,
        item_col,
    ]
    pdf = test_predictions_pdf[write_cols].copy()

    spark.sql(_build_training_eval_predictions_ddl(table_fqn))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    distinct_prods = sorted(pdf[item_col].unique())
    logger.info(
        "write_test_predictions: %d prods x ~%d rows = %d total -> %s",
        len(distinct_prods),
        len(pdf) // max(len(distinct_prods), 1),
        len(pdf),
        table_fqn,
    )

    for prod in distinct_prods:
        chunk_pdf = pdf[pdf[item_col] == prod].assign(model_version=model_version)
        chunk_sdf = spark.createDataFrame(chunk_pdf)
        chunk_sdf.write.insertInto(table_fqn, overwrite=True)
        logger.info(
            "write_test_predictions: wrote prod=%s rows=%d",
            prod,
            len(chunk_pdf),
        )


def log_experiment(
    model: ModelAdapter,
    best_params: dict,
    best_iteration: int,
    evaluation_results: dict,
    parameters: dict,
) -> None:
    """Log training results to MLflow."""
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")
    training_cfg = parameters.get("training", {})
    algorithm = training_cfg.get("algorithm", "lightgbm")
    final_model_strategy = training_cfg.get("final_model_strategy", "hpo_best")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    with log_step(logger, "mlflow_log"):
        with mlflow.start_run():
            mlflow.log_params(best_params)
            mlflow.log_param("algorithm", algorithm)
            mlflow.log_param("final_model_strategy", final_model_strategy)
            mlflow.log_metric("best_iteration", best_iteration)
            mlflow.log_metric("overall_map", evaluation_results["overall_map"])

            for prod, ap in evaluation_results.get("per_product_ap", {}).items():
                mlflow.log_metric(f"ap_{prod}", ap)

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

    logger.info("MLflow experiment logged: %s", experiment_name)

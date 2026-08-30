"""Pure functions for the training pipeline."""

import logging
import shutil
from pathlib import Path
from typing import Iterable, NamedTuple

import mlflow
import numpy as np
import optuna
import pandas as pd

from recsys_tfb.core.consistency import HPO_OBJECTIVES, REBUILD_SNAP_DATES_KEY
from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle, handle_paths, open_parquet_dataset
from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.pipelines.training.steps.hpo_scoring import TrialScorer
from recsys_tfb.pipelines.training.steps.local_cache import (
    CACHE_SOURCE_TABLES,
    populate_cache_from_hive,
    require_spark_input,
    resolve_cache_path,
    success_marker,
)
from recsys_tfb.utils.spark import release_spark_session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_weight_diagnostics(
    train_handle, parameters: dict, preprocessor_metadata: dict,
) -> dict:
    """Data-driven sample_weight diagnostic for the model manifest.

    Reports configured sample_weights entries that match zero train rows
    (``unmatched_keys``) — covers label / identity / feature / encoding
    mismatch + unknown-category typos. Reads only the weight-key columns of the
    train parquet (cheap distinct).
    """
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _composite_key_series, _translate_weight_table

    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]
    diag = {"enabled": bool(sw), "weight_keys": list(weight_keys),
            "n_weight_entries": len(sw), "unmatched_keys": []}
    if not sw:
        return diag

    category_mappings = (preprocessor_metadata or {}).get("category_mappings", {}) or {}
    identity_cols = get_schema(parameters)["identity_columns"]

    ds = pads.dataset(train_handle.path, format="parquet")
    if any(k not in ds.schema.names for k in weight_keys):
        diag["unmatched_keys"] = sorted(str(k) for k in sw)
        return diag
    pdf = ds.to_table(columns=list(weight_keys)).to_pandas().drop_duplicates()
    present = set(_composite_key_series(pdf, weight_keys).tolist())

    unmatched = []
    for key in sw:
        one, _ = _translate_weight_table(
            {key: sw[key]}, weight_keys, category_mappings, identity_cols)
        if not one or next(iter(one)) not in present:
            unmatched.append(str(key))
    diag["unmatched_keys"] = sorted(unmatched)
    return diag


def persist_sample_weight_report(
    train_parquet_handle, preprocessor_metadata: dict, parameters: dict,
) -> dict:
    """Compute the sample_weight diagnostic; the catalog persists it.

    Always runs (not gated by the lgb .bin cache) so the report reflects the
    current config every run.

    The node does not write the file: ``sample_weight_report`` is a catalog
    entry pointing at ``data/models/<model_version>/sample_weight_report.json``,
    which is what keeps the report in the manifest's artifacts list *and* in
    ``extra_metadata.sample_weight``. Nobody downstream consumes it -- the entry
    exists so the report can be fetched and read on its own.
    """
    diag = resolve_weight_diagnostics(
        train_parquet_handle, parameters, preprocessor_metadata)
    logger.info(
        "sample_weight report: enabled=%s unmatched=%d",
        diag["enabled"], len(diag["unmatched_keys"]),
    )
    return diag


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _test_month_dir(snap_date: str) -> str:
    """Directory-name form of a test month (``2026-01-31`` → ``20260131``).

    Single definition shared by the path builder, the caller that dedupes
    configured months, and the predict-side month matching, so they cannot
    drift apart. Doubling as the comparison key means a month is the same month
    however it was spelled — config value, cache directory name, Hive partition
    value — which is what the incremental decisions compare.

    Kept here rather than in ``steps/local_cache.py`` with the rest of the cache
    mechanism: issue #231 moves it into ``steps/predict_months.py``, a module
    that has to stay importable with zero project dependencies, and a copy in
    the cache module would have to be imported back. ``resolve_cache_path``
    takes the directory name as an argument for exactly this reason.
    """
    return str(snap_date).strip().replace("-", "")


def inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Auto-derive cache source_tables from catalog_config and write into parameters.

    Mutates `parameters` to add `_cache_source_tables` mapping (cache logical
    name → actual Hive table name). Cache nodes read this in
    steps.local_cache.populate_cache_from_hive.

    For each known cache name in CACHE_SOURCE_TABLES, look up the catalog entry.
    If present and `type: HiveTableDataset`, take its `table` field. Skips
    entries that aren't HiveTableDataset and missing entries.

    Operates on raw catalog_config dict (not DataCatalog instance) — the yaml
    schema is the public contract; we don't access dataset instance internals.

    No-op (does not write the key) when no cache entries match.

    Called by __main__.py:_run_pipeline before DataCatalog construction so the
    cache nodes see the auto-derived mapping at runtime.
    """
    auto: dict[str, str] = {}
    for cache_name in CACHE_SOURCE_TABLES:
        entry = catalog_config.get(cache_name)
        if entry and entry.get("type") == "HiveTableDataset":
            table = entry.get("table")
            if table:
                auto[cache_name] = table
    if auto:
        parameters["_cache_source_tables"] = auto


# ---------------------------------------------------------------------------
# Cache nodes
# ---------------------------------------------------------------------------
#
# Five nodes, each writing out its own cache decisions rather than calling one
# shared ``_cache(split_name, parameters)``. That helper is the shape ADR-0008
# §2 forbids and ADR-0014 decision 1 re-affirms: it held four decisions, so
# reading any of the five nodes above it told you nothing about what the cache
# had decided. The duplication below is the point; what is shared is the
# mechanism, in ``steps/local_cache.py``.
#
# The ``shutil.rmtree`` calls stay in this module on purpose — see that module's
# docstring for which audit stops seeing them if they move.


def cache_train_model_input(train_model_input, parameters: dict) -> ParquetHandle:
    """Driver-local parquet copy of the train split, keyed by its sampling variant.

    The cache directory carries ``train_variant_id``, so changing the sampling
    settings writes a *new* directory rather than overwriting the old one — which
    is what makes a sweep over those settings resumable. Keyed by
    ``base_dataset_version`` alone, the second variant would find the first one's
    ``_SUCCESS``, read its bytes, and train on the wrong draw without a word.
    """
    require_spark_input(train_model_input, "train_model_input")
    local_path = resolve_cache_path("train_model_input", parameters)
    marker = success_marker(local_path)

    # Decision — a directory with no marker is an interrupted copy, not a cache
    # entry: drop it and copy again. Reading it is the dangerous alternative —
    # pyarrow opens whatever fragments landed and every number downstream is
    # computed over a silent subset. Rebuilding is only right here because the
    # Hive source is still in reach; a consumer holding nothing but the handle
    # cannot rebuild, which is why io.handles.require_complete_cache refuses
    # instead of recovering.
    if Path(local_path).exists() and not marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Checking
    # freshness would cost a Hive metadata query per split per run; the price of
    # the marker rule is that an upstream backfill leaves a stale-but-complete
    # copy in place and nothing warns. No escape hatch reaches this split:
    # ``--rebuild-dates`` is constrained to ``dataset.test_snap_dates`` (A21), so
    # clearing this one after a backfill is a manual ``rm -rf`` (see
    # ``docs/operations/user-guides/pipeline-slicing.md``).
    if marker.exists():
        logger.info("cache_hit name=%s path=%s", "train_model_input", local_path)
        return ParquetHandle(path=local_path)

    logger.info("cache_miss name=%s path=%s", "train_model_input", local_path)
    populate_cache_from_hive(
        train_model_input.sql_ctx.sparkSession,
        "train_model_input", parameters, local_path,
    )
    marker.touch()
    return ParquetHandle(path=local_path)


def cache_train_dev_model_input(train_dev_model_input, parameters: dict) -> ParquetHandle:
    """Driver-local parquet copy of the early-stopping split, beside its train split.

    Same ``train_variants/<train_variant_id>`` level as ``train_model_input``,
    because one draw produced both: change the sampling settings and the pair
    retires together. Drop the variant level from this path and re-sampling train
    would leave train_dev on the *old* draw — every HPO trial would early-stop
    against rows from a different sample than the one it was fit on, and the only
    symptom would be scores that look slightly off.
    """
    require_spark_input(train_dev_model_input, "train_dev_model_input")
    local_path = resolve_cache_path("train_dev_model_input", parameters)
    marker = success_marker(local_path)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than read a silent subset of the split. Safe here only
    # because Hive can still be re-read; the consumer-side guard
    # (io.handles.require_complete_cache) refuses instead, having no source.
    if Path(local_path).exists() and not marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Same trade as
    # the train split, and with the same gap: ``train_variant_id`` is derived from
    # the sampling config, not from the rows, so a backfill that adds rows under
    # an unchanged config leaves this copy stale-but-complete. Only test months
    # have an escape hatch (``--rebuild-dates``).
    if marker.exists():
        logger.info("cache_hit name=%s path=%s", "train_dev_model_input", local_path)
        return ParquetHandle(path=local_path)

    logger.info("cache_miss name=%s path=%s", "train_dev_model_input", local_path)
    populate_cache_from_hive(
        train_dev_model_input.sql_ctx.sparkSession,
        "train_dev_model_input", parameters, local_path,
    )
    marker.touch()
    return ParquetHandle(path=local_path)


def cache_val_model_input(val_model_input, parameters: dict) -> ParquetHandle:
    """Driver-local parquet copy of the val split, keyed by dataset version only.

    No variant level in the path, deliberately: val is the yardstick every train
    variant is scored against, so it must *not* move when the train draw changes.
    Put ``train_variant_id`` in this path and each variant would be measured on
    its own val rows — the comparison that picks a winner would be between two
    numbers computed on different data, and it would look perfectly normal.
    """
    require_spark_input(val_model_input, "val_model_input")
    local_path = resolve_cache_path("val_model_input", parameters)
    marker = success_marker(local_path)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than read a silent subset. Recovery is available here
    # because Hive is still in reach; a consumer handed only the handle cannot
    # rebuild, so io.handles.require_complete_cache fails instead.
    if Path(local_path).exists() and not marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. This split's
    # copy is the longest-lived of the five (nothing but a new
    # ``base_dataset_version`` retires it), which is exactly what makes a stale
    # copy after an upstream backfill worth knowing about: nothing warns.
    if marker.exists():
        logger.info("cache_hit name=%s path=%s", "val_model_input", local_path)
        return ParquetHandle(path=local_path)

    logger.info("cache_miss name=%s path=%s", "val_model_input", local_path)
    populate_cache_from_hive(
        val_model_input.sql_ctx.sparkSession,
        "val_model_input", parameters, local_path,
    )
    marker.touch()
    return ParquetHandle(path=local_path)


def cache_test_model_input(
    test_model_input, parameters: dict
) -> dict[str, ParquetHandle]:
    """Driver-local parquet copy of the test split, one directory per month.

    Returns ``{snap_date: handle}`` keyed by the **verbatim**
    ``dataset.test_snap_dates`` values (no format conversion), sorted so the
    mapping is deterministic. One month per directory is what lets each month be
    cached and invalidated on its own: adding a month copies only that month, and
    a month whose copy was interrupted is rebuilt without disturbing its siblings.

    This is the only one of the five that can be told to drop a *complete* copy.
    Not because the other four never go stale — a backfill under an unchanged
    config leaves any of them stale-but-complete — but because ``--rebuild-dates``
    is constrained to ``dataset.test_snap_dates`` (A21). Clearing the other four
    is a manual ``rm -rf``.

    The input type check runs once here rather than once per month, so a
    misconfigured environment is rejected even when no months are configured —
    the one behavioural difference from the per-month helper this replaced, and
    it only tightens a path that could not have produced a usable handle anyway.
    """
    require_spark_input(test_model_input, "test_model_input")

    configured = (parameters.get("dataset") or {}).get("test_snap_dates") or []
    rebuild = {
        _test_month_dir(d) for d in (parameters.get(REBUILD_SNAP_DATES_KEY) or [])
    }

    # Decision — what counts as one month. Dedupe on the *directory* form, not
    # the raw string: the same month is the same cache entry however it was
    # spelled. Two DIFFERENT spellings of one month would yield two keys pointing
    # at one directory (handle_paths would hand the same root to pyarrow twice
    # and silently double every row) — that config is rejected at CLI entry by
    # A26, so by the time this runs a key can only be carrying repeats of one
    # literal.
    by_dir: dict[str, str] = {}
    for raw in configured:
        by_dir.setdefault(_test_month_dir(str(raw)), str(raw))

    handles: dict[str, ParquetHandle] = {}
    for month in sorted(by_dir.values()):
        month_dir = _test_month_dir(month)
        local_path = resolve_cache_path("test_model_input", parameters, month_dir)
        marker = success_marker(local_path)

        # Decision — a month named by ``--rebuild-dates`` is dropped even on a
        # hit. Cache hits never look at freshness, so after an upstream backfill
        # the month's cached parquet is stale but complete; without this the
        # escape hatch would run, re-predict from the pre-backfill rows, and
        # produce byte-identical numbers.
        if month_dir in rebuild and Path(local_path).exists():
            logger.info(
                "cache_rebuild name=%s path=%s — named by --rebuild-dates, "
                "dropping the cached copy so the refreshed source is re-read",
                "test_model_input", local_path,
            )
            shutil.rmtree(local_path, ignore_errors=True)
            # Decision — a drop that did not take is fatal, not a warning.
            # ``ignore_errors`` is right for the partial-cache branch below (that
            # copy is unusable either way) but not here: a surviving marker would
            # be read as a hit three lines down, and the rebuild would degrade
            # into the exact stale-cache re-run it was invoked to prevent.
            if marker.exists():
                raise RuntimeError(
                    f"could not clear the cached month at {local_path} "
                    "(--rebuild-dates named it). Refusing to continue: the "
                    "surviving _SUCCESS would be taken as a cache hit and this "
                    "month would be re-predicted from the pre-backfill rows, "
                    "producing identical numbers. Remove the directory by hand "
                    "and re-run."
                )

        # Decision — a directory with no marker is an interrupted copy: drop it
        # and copy that month again. A failed copy leaves an empty directory
        # behind (``populate_cache_from_hive`` says so in its own docstring), and
        # reading it would put a month's worth of missing rows into the test
        # metric without an error. Rebuilding is right only while Hive is in
        # reach — a diagnosis node handed the finished handle cannot rebuild, so
        # io.handles.require_complete_cache refuses there instead.
        if Path(local_path).exists() and not marker.exists():
            logger.warning(
                "Partial cache detected at %s, clearing before retry", local_path
            )
            shutil.rmtree(local_path, ignore_errors=True)

        # Decision — a hit is "the marker is present", never freshness; the
        # months this misses are exactly the ones ``--rebuild-dates`` names.
        if marker.exists():
            logger.info("cache_hit name=%s path=%s", "test_model_input", local_path)
        else:
            logger.info("cache_miss name=%s path=%s", "test_model_input", local_path)
            populate_cache_from_hive(
                test_model_input.sql_ctx.sparkSession,
                "test_model_input", parameters, local_path, snap_date=month,
            )
            marker.touch()

        handles[month] = ParquetHandle(path=local_path)

    return handles


def cache_calibration_model_input(calibration_model_input, parameters: dict) -> ParquetHandle:
    """Driver-local parquet copy of the calibration split, keyed by its own variant.

    ``calibration_variant_id``, not ``train_variant_id``: the calibration draw
    has its own ratio and its own sampling site, so it retires on its own
    schedule. Share the train variant's directory and re-sampling calibration
    alone would keep hitting the old copy — the calibrator would be fitted on the
    draw the config no longer asks for, and every downstream probability would be
    quietly calibrated against it.
    """
    require_spark_input(calibration_model_input, "calibration_model_input")
    local_path = resolve_cache_path("calibration_model_input", parameters)
    marker = success_marker(local_path)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than fit a calibrator on a silent subset. Rebuilding is
    # the right move only while Hive is reachable; the consumer-side guard
    # (io.handles.require_complete_cache) refuses instead.
    if Path(local_path).exists() and not marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Same trade as
    # the other splits: no metadata query per run, at the price of a
    # stale-but-complete copy surviving an upstream backfill unannounced.
    if marker.exists():
        logger.info("cache_hit name=%s path=%s", "calibration_model_input", local_path)
        return ParquetHandle(path=local_path)

    logger.info("cache_miss name=%s path=%s", "calibration_model_input", local_path)
    populate_cache_from_hive(
        calibration_model_input.sql_ctx.sparkSession,
        "calibration_model_input", parameters, local_path,
    )
    marker.touch()
    return ParquetHandle(path=local_path)


def select_features(preprocessor_metadata: dict, parameters: dict) -> dict:
    """Apply training-stage feature selection, returning a preprocessor view.

    Single chokepoint for the training pipeline: every model-touching node
    consumes this (possibly subset) view instead of the raw dataset-built
    ``preprocessor``, so ``training.feature_selection.exclude`` is applied
    exactly once and stays consistent across bin-build, HPO, finalize,
    calibration, test scoring, and diagnostics. Empty/absent selection returns
    the input unchanged, so non-selection runs are byte-identical.
    """
    from recsys_tfb.models.feature_selection import apply_feature_selection

    return apply_feature_selection(preprocessor_metadata, parameters)


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

def _resolve_search_id(parameters: dict) -> str:
    """HPO search_id：production 由 __main__ 注入；單測/直呼則就地計算。"""
    sid = parameters.get("search_id")
    if sid:
        return str(sid)
    from recsys_tfb.core.versioning import compute_search_id

    cvi = parameters.get("calibration_variant_id")
    if not isinstance(cvi, str) or cvi.startswith("__"):  # "__none__" placeholder
        cvi = None
    return compute_search_id(
        parameters,
        str(parameters.get("base_dataset_version", "")),
        str(parameters.get("train_variant_id", "")),
        cvi,
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
    # HPO 與其後的 finalize/calibrate 全是 driver-local:Spark 從這裡到
    # predict_and_write_test_predictions 完全閒置,可能數小時。閒置的 application
    # 會被叢集端回收,context 在 JVM 端死掉,之後寫 Hive 就撞 IllegalStateException。
    # 主動釋放,由 predict 節點依 canonical configs 重建。
    #
    # 放在函式體第一行(而非新增 DAG 節點):Runner 循序執行,「第一行」在構造上就等於
    # 「前面的節點都跑完」;零入度節點的排序則取決於宣告位置,靠不住。
    #
    # 注意:fb0d4c4 也曾在此 stop 過 session,並在 85b28699 被移除——那次是誤診效能
    # 問題(真因是 OMP thread oversubscription),且當時的重建路徑無法處理 JVM 端
    # 死亡(只處理 Python 端 stop)。這次不同:釋放是目的,且重建對兩種死法都有效。
    release_spark_session(parameters)

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

    from recsys_tfb.pipelines.training import hpo_resume

    checkpointing = parameters.get("hpo_checkpointing", True)
    search_id = _resolve_search_id(parameters)

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    ckpt = None
    if checkpointing:
        study_dir = hpo_resume.hpo_study_dir(search_id)
        if parameters.get("_fresh_hpo", False):
            n_prev, prev_best = 0, float("nan")
            if study_dir.exists():
                try:
                    _tmp = hpo_resume.open_study(study_dir, search_id, seed)
                    n_prev = hpo_resume.count_completed(_tmp)
                    prev_best = _tmp.best_value if n_prev else float("nan")
                except Exception:  # pragma: no cover - defensive
                    pass
            logger.warning(
                "--fresh-hpo: clearing %s (discarding %d completed trial(s), prev best=%.4f)",
                study_dir, n_prev, prev_best,
            )
            hpo_resume.clear_study_dir(study_dir)

        study = hpo_resume.open_study(study_dir, search_id, seed)
        done = hpo_resume.count_completed(study)
        ckpt = hpo_resume.load_checkpoint(study_dir, algorithm)
        if ckpt is not None:
            logger.info(
                "HPO resume: %d completed trial(s) found; best so far score=%.4f "
                "(trial #%d); running %d more (target=%d)",
                done, ckpt["score"], ckpt["trial_number"],
                max(0, n_trials - done), n_trials,
            )
        remaining = max(0, n_trials - done)
    else:
        study_dir = None
        study = optuna.create_study(
            direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed)
        )
        remaining = n_trials

    # Optuna only ever sees the float a trial returns, so the winning model has
    # to be kept on the callable itself — that is what `scorer.best` is for.
    # Built here rather than above the branch because `study_dir` is one of its
    # arguments: `None` means "do not checkpoint", and the checkpointing branch
    # is the only one that sets it.
    scorer = TrialScorer(
        train_lgb_handle=train_lgb_handle,
        train_dev_lgb_handle=train_dev_lgb_handle,
        X_val=X_v, y_val=y_v, groups_val=groups_v, items_val=items_v,
        algorithm=algorithm,
        algorithm_params=algorithm_params,
        search_space=search_space,
        hpo_objective=hpo_objective,
        seed=seed,
        num_iterations=num_iterations,
        early_stopping_rounds=early_stopping_rounds,
        n_trials=n_trials,
        search_id=search_id,
        study_dir=study_dir,
    )

    # Decision — a resumed search inherits the previous run's winner before it
    # scores anything. Skip this and the first new trial wins by default
    # (best-so-far starts at -1.0), silently shipping a worse model and
    # checkpointing over the better one.
    if ckpt is not None:
        scorer.adopt_checkpoint(ckpt)

    if remaining > 0:
        with log_step(logger, "optuna_optimize"):
            study.optimize(scorer, n_trials=remaining)
    else:
        logger.info("HPO target already met (done>=%d); skipping optimize", n_trials)

    # last-resort: study has trials but no usable checkpoint model — refit best_params once.
    if scorer.best["model"] is None:
        logger.warning(
            "No usable best model from memory/checkpoint; "
            "refitting study.best_params once (last-resort recovery)"
        )
        study.enqueue_trial(study.best_params)
        with log_step(logger, "last_resort_refit"):
            study.optimize(scorer, n_trials=1)

    best_params = scorer.best["params"] or study.best_params
    best_model = scorer.best["model"]
    best_iteration = scorer.best["iteration"]
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, scorer.best["score"], best_iteration, best_params,
    )

    # HPO 搜尋診斷：best-effort 側輸出，衍生自本地 study。失敗只 warning、絕不影響
    # 回傳（診斷 bug 不得逼你重跑 HPO）。不新增 DAG node、不改本函式 outputs → 對
    # RESUME_CONTRACTS 隱形。產物寫進 diagnostics_dir/hpo/，由 log_experiment 的
    # log_artifacts 撿走。見 docs/superpowers/specs/2026-07-15-hpo-search-diagnostics-design.md
    try:
        from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics

        write_hpo_diagnostics(
            study, search_space, parameters,
            search_id=search_id, hpo_objective=hpo_objective, seed=seed,
            n_trials_target=n_trials, best_iteration=best_iteration,
        )
    except Exception:  # pragma: no cover - best-effort guard
        logger.warning("HPO diagnostics failed; training continues", exc_info=True)

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

    # refit_on_full — the only other value A25 admits, so there is nothing left
    # to reject here. The domain check lives at CLI entry on purpose: this node
    # runs after the whole HPO search, and a typo used to cost that search.
    # A25 rejects an explicit `final_model_strategy:` (yaml null) too, which
    # matters here: .get would hand this line None, not "hpo_best", and None
    # would fall through to a silent full refit.

    import lightgbm as lgb

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
            feature_name=feat_cols,
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
            feature_name=feat_cols,
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



#: Hive's stand-in for a NULL partition value.
_HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class _PredictMonthPlan(NamedTuple):
    """Which test months this predict run will write, and which it will not.

    ``to_process`` and ``skipped`` partition the configured months (disjoint,
    union == configured). ``rebuilt`` is the subset of ``to_process`` that was
    already complete and is being redone only because ``--rebuild-dates``
    named it.
    """

    to_process: list[str]
    skipped: list[str]
    rebuilt: list[str]


def _plan_predict_months(
    configured: Iterable,
    cache_items: dict[str, set[str]],
    written_items: dict[str, set[str]],
    rebuild: set[str],
) -> _PredictMonthPlan:
    """Decide, per configured month, whether its predictions still need writing.

    A ``(model_version, snap_date)`` prediction set is an immutable product:
    ``model_version`` hashes everything that defines the model, so the same
    model over the same month's model_input predicts bit-identically. Recomputing
    it buys nothing — hence skipping, and hence ``rebuild`` as the price of that
    (an upstream backfill changes the input without changing any version).

    Completeness is "the item partitions written for this month are exactly the
    distinct items the month's cache holds", not "some partition exists". The
    weaker test would call a run that died halfway complete and leave the
    missing items absent forever, and would miss a newly added item entirely.

    Args:
        configured: ``dataset.test_snap_dates`` — the authority on which months
            exist. The cache is only a data source; a month lingering there
            after being dropped from config must not be predicted.
        cache_items: month key → distinct items present in that month's cache.
        written_items: month key → item partitions already written for this
            model_version.
        rebuild: month keys named by ``--rebuild-dates``.

    Raises:
        ValueError: a configured month has no rows in the cache — dataset never
            produced it. Calling it complete (∅ == ∅) would skip it silently
            and hand evaluation an empty report for that month.
    """
    to_process: list[str] = []
    skipped: list[str] = []
    rebuilt: list[str] = []

    by_key: dict[str, str] = {}
    for raw in configured:
        by_key.setdefault(_test_month_dir(raw), str(raw).strip())

    for key in sorted(by_key):
        label = by_key[key]
        if key not in cache_items:
            raise ValueError(
                f"test month {label!r} is in dataset.test_snap_dates but has no "
                "rows in the test cache. Run the dataset pipeline for that "
                "month first (predict cannot invent it, and treating it as "
                "already-done would silently produce an empty report)."
            )
        already = written_items.get(key, set())
        if key in rebuild:
            rebuilt.append(label)
            to_process.append(label)
            continue
        if already == cache_items[key]:
            skipped.append(label)
            continue
        surplus = already - cache_items[key]
        if surplus:
            # Set difference, not a superset test: an item renamed between runs
            # leaves both a surplus and a missing partition, and a superset test
            # sees neither. Re-predicting cannot delete the surplus one, so it
            # survives every run — and compute_test_mAP_spark reads the whole
            # model_version, so a stale item's rows keep landing in the metric.
            logger.warning(
                "[months] predict: %s has prediction partitions for items that "
                "are not in the cache (%s). Re-predicting cannot remove them, "
                "so they will keep contributing rows to this model_version's "
                "metrics until they are dropped by hand, and this month will "
                "be re-predicted on every run.",
                label, sorted(surplus),
            )
        to_process.append(label)

    return _PredictMonthPlan(
        to_process=to_process, skipped=skipped, rebuilt=rebuilt
    )


def _written_prediction_partitions(
    predictions_dataset, time_col: str, item_col: str
) -> dict[str, set[str]]:
    """Item partitions already written per month, from the catalog dataset.

    predict never receives a SparkSession — its inputs are the model, the cache
    handles, the preprocessor, parameters and the predictions dataset object —
    so that object is the only route to the metastore. It already scopes itself
    to this ``model_version`` through its ``partition_filter``, which is exactly
    the scope the completeness question is asked in.

    A dataset type that cannot list partitions makes every month look
    incomplete: that re-predicts (wasteful) rather than skips (silently stale),
    which is the direction this decision must fail in.
    """
    lister = getattr(predictions_dataset, "existing_partition_values", None)
    if lister is None:
        logger.warning(
            "[months] predict: %s cannot list partitions, so no month can be "
            "shown complete; every configured month will be predicted.",
            type(predictions_dataset).__name__,
        )
        return {}

    written: dict[str, set[str]] = {}
    for spec in lister():
        month, item = spec.get(time_col), spec.get(item_col)
        if month is None or item is None:
            continue
        if _HIVE_NULL_PARTITION in (month, item):
            # Hive writes a NULL partition value as this literal, while the
            # parquet side reconstructs it as None -> "None"; the two spellings
            # would never match, so this month would look permanently
            # incomplete. Drop it and say so: dropping means "not written yet",
            # which re-predicts rather than skips. Mirrors the same guard on the
            # dataset side (pipelines/dataset/month_plans.py).
            logger.warning(
                "[months] predict: ignoring prediction partition with a NULL "
                "value (%s=%r, %s=%r); that month will be treated as not yet "
                "written.", time_col, month, item_col, item,
            )
            continue
        written.setdefault(_test_month_dir(month), set()).add(str(item))
    return written


def predict_and_write_test_predictions(
    model: ModelAdapter,
    test_parquet_handle: dict[str, ParquetHandle],
    preprocessor_metadata: dict,
    parameters: dict,
    training_eval_predictions,  # HiveTableDataset, supplied via Node(writes=...)
) -> dict:
    """Per-partition test prediction + Hive write, one month at a time.

    Months whose predictions are already complete are skipped (see
    :func:`_plan_predict_months`), so adding a test month costs one month of
    prediction rather than re-predicting every accumulated month. The manifest
    names what was processed, skipped and rebuilt: a node that decides to do
    less work has to say what it decided not to do, or a silently stale month
    is indistinguishable from a correctly skipped one.

    For each (snap_date, prod_name) partition of the months being processed:
        - load only that partition's rows via pyarrow filter
        - slice X via _pdf_to_X; predict; (predict_uncalibrated if Calibrated)
        - build a pandas DataFrame with (every schema.entity column, score,
          score_uncalibrated, label) + partition cols snap_date, prod_name
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
    model_version = parameters["model_version"]

    # partitioning="hive" tells pyarrow to reconstruct (snap_date, prod_name)
    # columns from the snap_date=*/prod_name=* directory tree produced by
    # HiveTableDataset.save() (and by the test fixture's pq.write_to_dataset).
    ds = open_parquet_dataset(handle_paths(test_parquet_handle))

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

    cache_items: dict[str, set[str]] = {}
    for _, row in partition_pdf.iterrows():
        cache_items.setdefault(_test_month_dir(row[time_col]), set()).add(
            str(row[item_col])
        )

    # The config is the authority on which months exist; the cache is only
    # where their rows come from. Deliberately no "fall back to whatever the
    # cache holds": that would resurrect a month dropped from the config, which
    # is the one thing the authority rule exists to prevent.
    configured = (parameters.get("dataset") or {}).get("test_snap_dates") or []
    plan = _plan_predict_months(
        configured,
        cache_items,
        _written_prediction_partitions(
            training_eval_predictions, time_col, item_col
        ),
        {
            _test_month_dir(d)
            for d in (parameters.get(REBUILD_SNAP_DATES_KEY) or [])
        },
    )
    logger.info(
        "[months] predict: processed=%s skipped=%s rebuilt=%s",
        ",".join(plan.to_process) or "-",
        ",".join(plan.skipped) or "-",
        ",".join(plan.rebuilt) or "-",
    )

    process_keys = {_test_month_dir(m) for m in plan.to_process}
    # .map keeps this a row mask even when the frame is empty; a list
    # comprehension would degrade into `pdf[[]]`, which pandas reads as
    # "select these zero *columns*".
    partition_pdf = partition_pdf[
        partition_pdf[time_col].map(lambda v: _test_month_dir(v) in process_keys)
    ]

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
                # Every entity column, not just the first: the identity of a
                # scored row is the whole tuple. `str` for all of them matches
                # what the ranking side compares on. That the write target
                # declares all of them is A28, checked at CLI entry — a column
                # it never declared is dropped by `save` in silence.
                **{c: part_pdf[c].astype(str).values for c in entity_cols},
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
        # What this run decided about every configured month, not just the ones
        # it touched: `snap_dates` above cannot distinguish "skipped because
        # complete" from "never knew about it".
        "months_processed": plan.to_process,
        "months_skipped": plan.skipped,
        "months_rebuilt": plan.rebuilt,
    }
    logger.info(
        "predict_and_write_test_predictions: done — "
        "snap_dates=%d prods=%d n_rows_written=%d model_version=%s "
        "months_processed=%d months_skipped=%d months_rebuilt=%d",
        len(manifest["snap_dates"]), len(manifest["prods"]),
        manifest["n_rows_written"], manifest["model_version"],
        len(plan.to_process), len(plan.skipped), len(plan.rebuilt),
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
    quadrant_profiles: dict = None,
    cases_manifest: dict = None,
) -> None:
    """Log training results to MLflow."""
    from recsys_tfb.diagnosis.model import diagnostics_dir
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
                if quadrant_profiles:
                    n_cells = sum(len(v) for v in quadrant_profiles.values())
                    mlflow.log_metric("n_quadrant_cells", n_cells)
                if cases_manifest:
                    n_cases = sum(
                        1 for it in cases_manifest.values() for cell in it.values()
                        for r in cell.values() if r.get("rendered")
                    )
                    mlflow.log_metric("n_cases_rendered", n_cases)

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

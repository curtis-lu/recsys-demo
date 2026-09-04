"""Fourteen of the training pipeline's twenty-one nodes; the other seven below.

This module is the home of the pipeline's ML story: a reader who opens it sees
each decision this pipeline makes about the data, without jumping files. The
mechanisms those decisions are expressed in live in ``steps/``, one module per
concern (``local_cache``, ``predict_months``, ``search_space``, ``hpo_resume``,
``hpo_scoring``, ``refit``, ``sample_weights``, ``experiment_log``).
``cache_sources`` sits beside this file instead, because ``__main__.py`` reads
it before the pipeline starts. ADR-0014 draws both lines and
``docs/agents/pipeline-node-design.md`` is where the placement criterion and
the node-body shape are written down.

**Nothing in here is a pure function**, and the import list is the tell: these
nodes delete cache directories (``shutil.rmtree``), copy Hive partitions onto
driver-local disk, stop the SparkSession in the middle of the DAG, write Hive
one partition at a time, and open an MLflow run. Each of those is argued at its
own call site. What the module promises is not purity but legibility: the
*decision* behind every side effect is readable here rather than buried in a
helper.

Where the other seven nodes are
-------------------------------
``pipeline.py`` registers 21 nodes. Fourteen are ``def``-ed in this file. The
seven diagnosis nodes are ``def``-ed under ``recsys_tfb.diagnosis.model``:

- ``compute_feature_statistics``  -> ``diagnosis/model/feature_stats.py``
- ``compute_feature_importance``  -> ``diagnosis/model/importance.py``
- ``compute_gain_ledger``         -> ``diagnosis/model/gain_ledger.py``
- ``compute_shap_diagnostics``    -> ``diagnosis/model/shap_per_item.py``
- ``select_shap_population``      -> ``diagnosis/model/population_spark.py``
- ``compute_quadrant_profiles``   -> ``diagnosis/model/shap_cases.py``
- ``compute_quadrant_cases``      -> ``diagnosis/model/shap_cases.py``

They stay there deliberately, and this list is the price of that: the usual
rule -- one pipeline, one ``nodes.py`` -- does not hold here, so the way back
has to be written down. Three reasons they are not moved (ADR-0014 decision
6). Their home is undecided: splitting diagnosis into a pipeline of its own
is an open question, and a move now would be undone by it. Moving them means
seven pass-through shells over ~1400 lines of helper, which is precisely the
shape rule 3 exists to forbid; making them real nodes instead would mean
floating that module's decisions up first, a separate piece of work. And a
shell is one more file to open when chasing a bug — the cost this list is
meant to pay off, not to add to.
"""

import logging
import shutil
from pathlib import Path

import mlflow
import optuna
import pandas as pd
import pyarrow.dataset as pads
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import HPO_OBJECTIVES, REBUILD_SNAP_DATES_KEY
from recsys_tfb.core.group_utils import (
    default_metric_for_objective,
    is_ranking_objective,
    to_contiguous_groups,
)
from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.core.versioning import compute_search_id
from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics
from recsys_tfb.diagnosis.model import diagnostics_dir
from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
from recsys_tfb.io.extract import extract_Xy, extract_Xy_with_groups, pdf_to_X
from recsys_tfb.io.handles import ParquetHandle, handle_paths, open_parquet_dataset
from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.models.feature_selection import apply_feature_selection
from recsys_tfb.pipelines.training.steps import (
    experiment_log,
    hpo_resume,
    refit,
    sample_weights,
)
from recsys_tfb.pipelines.training.steps.hpo_scoring import TrialScorer
from recsys_tfb.pipelines.training.steps.local_cache import (
    cache_exists,
    cache_is_complete,
    is_partial_cache,
    log_cache_dropped_for_rebuild,
    log_cache_hit,
    log_cache_miss,
    log_partial_cache_cleared,
    mark_cache_complete,
    populate_cache_from_hive,
    require_spark_input,
    resolve_cache_path,
)
from recsys_tfb.pipelines.training.steps.predict_months import (
    configured_months,
    month_dir,
    months_already_written,
    plan_predict_months,
    require_months_are_cached,
    warn_about_surplus_partitions,
    written_prediction_partitions,
)
from recsys_tfb.utils.spark import release_spark_session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def persist_sample_weight_report(
    train_parquet_handle, preprocessor_metadata: dict, parameters: dict,
) -> dict:
    """Report which configured sample_weights entries matched zero train rows.

    A weight that matches nothing is the failure this node exists to surface:
    nothing raises, nothing logs, the model just trains as if the weight had
    never been configured. Label / identity / feature / encoding mismatches and
    unknown-category typos all end up looking the same from the outside, so the
    report names the entries rather than diagnosing the cause.

    Always runs (not gated by the lgb .bin cache) so the report reflects the
    current config every run.

    The node does not write the file: ``sample_weight_report`` is a catalog
    entry pointing at ``data/models/<model_version>/sample_weight_report.json``,
    which is what keeps the report in the manifest's artifacts list *and* in
    ``extra_metadata.sample_weight``. Nobody downstream consumes it -- the entry
    exists so the report can be fetched and read on its own.
    """
    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}

    # Decision — what a weight key is made of: the configured
    # `training.sample_weight_keys`, or the item column alone when unset. It has
    # to be the same tuple io/extract.py looks weights up by; a report built on
    # a different tuple vouches for lookups the trainer never made.
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]

    diag = {"enabled": bool(sw), "weight_keys": list(weight_keys),
            "n_weight_entries": len(sw), "unmatched_keys": []}
    if not sw:
        return diag

    present = sample_weights.distinct_weight_keys(train_parquet_handle, weight_keys)

    # Decision — what counts as unmatched, i.e. a weight that did nothing. Two
    # ways in: the train parquet carries no column for the key at all, which
    # condemns every entry at once (the weight key names a column model_input
    # does not have); or this one entry's key is absent from the rows read,
    # including the case where its value is not in the encoding at all.
    if present is None:
        unmatched = [str(key) for key in sw]
    else:
        category_mappings = (preprocessor_metadata or {}).get("category_mappings", {}) or {}
        identity_cols = get_schema(parameters)["identity_columns"]
        unmatched = []
        for key in sw:
            encoded = sample_weights.encoded_key(
                key, sw[key], weight_keys, category_mappings, identity_cols)
            if encoded is None or encoded not in present:
                unmatched.append(str(key))
    diag["unmatched_keys"] = sorted(unmatched)

    logger.info(
        "sample_weight report: enabled=%s unmatched=%d",
        diag["enabled"], len(diag["unmatched_keys"]),
    )
    return diag


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
    # Pre-check — a non-Spark input is a misconfigured environment, not a cache
    # problem. Say so before a path is composed for it.
    require_spark_input(train_model_input, "train_model_input")
    local_path = resolve_cache_path("train_model_input", parameters)

    # Decision — a directory with no marker is an interrupted copy, not a cache
    # entry: drop it and copy again. Reading it is the dangerous alternative —
    # pyarrow opens whatever fragments landed and every number downstream is
    # computed over a silent subset. Rebuilding is only right here because the
    # Hive source is still in reach; a consumer holding nothing but the handle
    # cannot rebuild, which is why io.handles.require_complete_cache refuses
    # instead of recovering.
    if is_partial_cache(local_path):
        log_partial_cache_cleared(local_path)
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Checking
    # freshness would cost a Hive metadata query per split per run; the price of
    # the marker rule is that an upstream backfill leaves a stale-but-complete
    # copy in place and nothing warns. No escape hatch reaches this split:
    # ``--rebuild-dates`` is constrained to ``dataset.test_snap_dates`` (A21), so
    # clearing this one after a backfill is a manual ``rm -rf`` (see
    # ``docs/operations/user-guides/pipeline-slicing.md``).
    if cache_is_complete(local_path):
        log_cache_hit("train_model_input", local_path)
        return ParquetHandle(path=local_path)

    log_cache_miss("train_model_input", local_path)
    populate_cache_from_hive(
        train_model_input.sql_ctx.sparkSession,
        "train_model_input", parameters, local_path,
    )
    mark_cache_complete(local_path)
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
    # Pre-check — a non-Spark input is a misconfigured environment, not a cache
    # problem. Say so before a path is composed for it.
    require_spark_input(train_dev_model_input, "train_dev_model_input")
    local_path = resolve_cache_path("train_dev_model_input", parameters)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than read a silent subset of the split. Safe here only
    # because Hive can still be re-read; the consumer-side guard
    # (io.handles.require_complete_cache) refuses instead, having no source.
    if is_partial_cache(local_path):
        log_partial_cache_cleared(local_path)
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Same trade as
    # the train split, and with the same gap: ``train_variant_id`` is derived from
    # the sampling config, not from the rows, so a backfill that adds rows under
    # an unchanged config leaves this copy stale-but-complete. Only test months
    # have an escape hatch (``--rebuild-dates``).
    if cache_is_complete(local_path):
        log_cache_hit("train_dev_model_input", local_path)
        return ParquetHandle(path=local_path)

    log_cache_miss("train_dev_model_input", local_path)
    populate_cache_from_hive(
        train_dev_model_input.sql_ctx.sparkSession,
        "train_dev_model_input", parameters, local_path,
    )
    mark_cache_complete(local_path)
    return ParquetHandle(path=local_path)


def cache_val_model_input(val_model_input, parameters: dict) -> ParquetHandle:
    """Driver-local parquet copy of the val split, keyed by dataset version only.

    No variant level in the path, deliberately: val is the yardstick every train
    variant is scored against, so it must *not* move when the train draw changes.
    Put ``train_variant_id`` in this path and each variant would be measured on
    its own val rows — the comparison that picks a winner would be between two
    numbers computed on different data, and it would look perfectly normal.
    """
    # Pre-check — a non-Spark input is a misconfigured environment, not a cache
    # problem. Say so before a path is composed for it.
    require_spark_input(val_model_input, "val_model_input")
    local_path = resolve_cache_path("val_model_input", parameters)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than read a silent subset. Recovery is available here
    # because Hive is still in reach; a consumer handed only the handle cannot
    # rebuild, so io.handles.require_complete_cache fails instead.
    if is_partial_cache(local_path):
        log_partial_cache_cleared(local_path)
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. This split's
    # copy is the longest-lived of the five (nothing but a new
    # ``base_dataset_version`` retires it), which is exactly what makes a stale
    # copy after an upstream backfill worth knowing about: nothing warns.
    if cache_is_complete(local_path):
        log_cache_hit("val_model_input", local_path)
        return ParquetHandle(path=local_path)

    log_cache_miss("val_model_input", local_path)
    populate_cache_from_hive(
        val_model_input.sql_ctx.sparkSession,
        "val_model_input", parameters, local_path,
    )
    mark_cache_complete(local_path)
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

    The ``raise`` in the body is a **post-condition**, not a pre-check: it runs
    after this node has removed a directory and asks whether the removal took.
    Nothing upstream can be at fault for it, so it is not a "the producer did
    not run" report — a surviving ``_SUCCESS`` means the drop this node just
    performed did not happen, and the person to find owns the local disk, not
    the input.
    """
    # Pre-check — a non-Spark input is a misconfigured environment, not a cache
    # problem. Say so before a path is composed for it.
    require_spark_input(test_model_input, "test_model_input")

    configured = (parameters.get("dataset") or {}).get("test_snap_dates") or []
    rebuild = {
        month_dir(d) for d in (parameters.get(REBUILD_SNAP_DATES_KEY) or [])
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
        by_dir.setdefault(month_dir(str(raw)), str(raw))

    handles: dict[str, ParquetHandle] = {}
    for month in sorted(by_dir.values()):
        month_key = month_dir(month)
        local_path = resolve_cache_path("test_model_input", parameters, month)

        # Decision — a month named by ``--rebuild-dates`` is dropped even on a
        # hit. Cache hits never look at freshness, so after an upstream backfill
        # the month's cached parquet is stale but complete; without this the
        # escape hatch would run, re-predict from the pre-backfill rows, and
        # produce byte-identical numbers.
        if month_key in rebuild and cache_exists(local_path):
            log_cache_dropped_for_rebuild("test_model_input", local_path)
            shutil.rmtree(local_path, ignore_errors=True)
            # Decision — a drop that did not take is fatal, not a warning.
            # ``ignore_errors`` is right for the partial-cache branch below (that
            # copy is unusable either way) but not here: a surviving marker would
            # be read as a hit three lines down, and the rebuild would degrade
            # into the exact stale-cache re-run it was invoked to prevent.
            if cache_is_complete(local_path):
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
        if is_partial_cache(local_path):
            log_partial_cache_cleared(local_path)
            shutil.rmtree(local_path, ignore_errors=True)

        # Decision — a hit is "the marker is present", never freshness; the
        # months this misses are exactly the ones ``--rebuild-dates`` names.
        if cache_is_complete(local_path):
            log_cache_hit("test_model_input", local_path)
        else:
            log_cache_miss("test_model_input", local_path)
            populate_cache_from_hive(
                test_model_input.sql_ctx.sparkSession,
                "test_model_input", parameters, local_path, snap_date=month,
            )
            mark_cache_complete(local_path)

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
    # Pre-check — a non-Spark input is a misconfigured environment, not a cache
    # problem. Say so before a path is composed for it.
    require_spark_input(calibration_model_input, "calibration_model_input")
    local_path = resolve_cache_path("calibration_model_input", parameters)

    # Decision — a directory with no marker is an interrupted copy: drop it and
    # copy again rather than fit a calibrator on a silent subset. Rebuilding is
    # the right move only while Hive is reachable; the consumer-side guard
    # (io.handles.require_complete_cache) refuses instead.
    if is_partial_cache(local_path):
        log_partial_cache_cleared(local_path)
        shutil.rmtree(local_path, ignore_errors=True)

    # Decision — a hit is "the marker is present", never freshness. Same trade as
    # the other splits: no metadata query per run, at the price of a
    # stale-but-complete copy surviving an upstream backfill unannounced.
    if cache_is_complete(local_path):
        log_cache_hit("calibration_model_input", local_path)
        return ParquetHandle(path=local_path)

    log_cache_miss("calibration_model_input", local_path)
    populate_cache_from_hive(
        calibration_model_input.sql_ctx.sparkSession,
        "calibration_model_input", parameters, local_path,
    )
    mark_cache_complete(local_path)
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
    """The HPO ``search_id``: injected by ``__main__`` in production.

    Computed here only for unit tests and direct calls, which have no
    ``__main__`` to inject it.
    """
    sid = parameters.get("search_id")
    if sid:
        return str(sid)
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

    The ``raise`` in the body is a **pre-check** on config: ``hpo_objective``
    has to name a score this pipeline can compute. A25 rejects the same value
    at CLI entry, so a run that reaches this line built ``parameters`` without
    passing that gate (tests, direct calls). It is a runtime backstop, and the
    person to find is whoever wrote the config, not whoever produced the data.
    """
    # HPO and everything after it (finalize / calibrate) is driver-local: Spark
    # sits completely idle from here until predict_and_write_test_predictions,
    # possibly for hours. An idle application gets reclaimed by the cluster, the
    # context dies on the JVM side, and the Hive write that comes later hits
    # IllegalStateException. Release it deliberately; the predict node rebuilds
    # it from the canonical configs.
    #
    # On the first line of the function body (rather than as a new DAG node):
    # the Runner runs sequentially, so "first line" is structurally the same as
    # "every preceding node has finished"; the ordering among zero-in-degree
    # nodes depends on declaration position and cannot be relied on.
    #
    # Note: fb0d4c4 also stopped the session here, and 85b28699 removed it —
    # that time was a misdiagnosed performance problem (the real cause was OMP
    # thread oversubscription), and the rebuild path of the day could not handle
    # a JVM-side death (it only handled a Python-side stop). This time is
    # different: releasing is the point, and the rebuild works for both ways of
    # dying.
    release_spark_session(parameters)

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
    #
    # Decision — the val matrix is mapped from disk, not held
    # on the heap. This is the one caller that keeps a matrix for the whole
    # search rather than for one fit, and in production it is 37-89 GiB on a
    # 128 GiB driver; mapped, the search's resident memory stops tracking the
    # val row count and the pages the current predict batch is not touching
    # are the OS's to reclaim. Unconditional on purpose — a "small enough for
    # RAM" branch would only ever run at the sizes nobody tests. The file is
    # unlinked as soon as it is mapped, so cleanup needs nothing from this
    # node; see `io/disk_matrix.py`, including why a full disk here would
    # otherwise corrupt the matrix in silence.
    with log_step(logger, "extract_features"):
        if hpo_objective == "macro_per_item_map":
            X_v, y_v, groups_v, items_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
                with_items=True, on_disk_label="hpo_val_matrix",
            )
        else:
            X_v, y_v, groups_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
                on_disk_label="hpo_val_matrix",
            )
            items_v = None

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

    # HPO search diagnostics: a best-effort side output derived from the local
    # study. Every failure of the call below only warns and never touches the
    # return value — a bug in the diagnostics must not be able to force an HPO
    # re-run. What the guard does NOT cover is importing the subtree: that
    # import sits at module level (see the header), so an ImportError there
    # stops the run before the first trial instead of after the whole search —
    # the better side to fail on, and free today: recsys_tfb.diagnosis.hpo
    # imports optuna (already imported here), the stdlib, and one internal
    # paths module, and defers its plotting import into a function body.
    # It adds no DAG node and does not change this function's outputs, so it
    # is invisible to RESUME_CONTRACTS. The artifacts land in
    # diagnostics_dir/hpo/ and are picked up by log_experiment's log_artifacts.
    # See
    # docs/superpowers/specs/2026-07-15-hpo-search-diagnostics-design.md
    try:
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
        with log_step(logger, "extract_features"):
            X_tr, y_tr, gid_tr, w_tr = extract_Xy_with_groups(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            X_dv, y_dv, gid_dv, w_dv = extract_Xy_with_groups(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        X_full, y_full, w_full = refit.stack_splits(
            (X_tr, y_tr, w_tr), (X_dv, y_dv, w_dv))
        # Decision — train / train_dev are customer-disjoint by sampling
        # design, so a query group never spans both splits: dev ids are offset
        # past train's max to keep them distinct after concatenation.
        gid_full = refit.offset_dev_group_ids(gid_tr, gid_dv)
        del X_tr, y_tr, X_dv, y_dv, gid_tr, gid_dv, w_tr, w_dv

        # Decision — group= makes this a ranking refit consistent with the
        # objective, and the row order follows the groups: the permutation
        # to_contiguous_groups returns has to be applied to X / y / weight too,
        # or the labels no longer belong to the rows they came from.
        perm, grp = to_contiguous_groups(gid_full)
        ds_full = refit.build_dataset(
            X_full[perm], y_full[perm], w_full[perm],
            feat_cols, cat_idx, group=grp,
        )
    else:
        with log_step(logger, "extract_features"):
            X_tr, y_tr, w_tr = extract_Xy(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            X_dv, y_dv, w_dv = extract_Xy(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        X_full, y_full, w_full = refit.stack_splits(
            (X_tr, y_tr, w_tr), (X_dv, y_dv, w_dv))
        del X_tr, y_tr, X_dv, y_dv, w_tr, w_dv

        # Decision — no group=: a non-ranking objective scores each row on its
        # own, so there is no query grouping to carry and no reordering to do.
        ds_full = refit.build_dataset(
            X_full, y_full, w_full, feat_cols, cat_idx)

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
    test_parquet_handle: dict[str, ParquetHandle],
    preprocessor_metadata: dict,
    parameters: dict,
    training_eval_predictions,  # HiveTableDataset, supplied via Node(writes=...)
) -> dict:
    """Per-partition test prediction + Hive write, one month at a time.

    Months whose predictions are already complete are skipped (the five month
    decisions are written out in the body, under "Which months this run
    writes"), so adding a test month costs one month of prediction rather than
    re-predicting every accumulated month. The manifest
    names what was processed, skipped and rebuilt: a node that decides to do
    less work has to say what it decided not to do, or a silently stale month
    is indistinguishable from a correctly skipped one.

    For each (snap_date, prod_name) partition of the months being processed:
        - load only that partition's rows via pyarrow filter
        - slice X via pdf_to_X; predict; (predict_uncalibrated if Calibrated)
        - build a pandas DataFrame with (every schema.entity column, score,
          score_uncalibrated, label) + partition cols snap_date, prod_name
        - training_eval_predictions.save(df) — exactly one partition's
          rows per save, so dynamic-partition overwrite cleanly overwrites
          a single partition and successive saves don't collide

    test_model_input is pre-filtered upstream (filter_test_model_input node
    in dataset pipeline) so every (snap_date, cust_id) group already has
    at least one positive label.

    Returns:
        The manifest. It has a catalog entry (issue #233), so it lands at
        ``data/models/<model_version>/predict_manifest.json`` and the three
        month lists stay answerable once the run is over. Downstream it is a
        DAG-ordering dependency only — the predictions themselves are read
        back from Hive — and landing it is also what lets a diagnosis-only
        resume skip this node rather than pay its partition listing again.
    """
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
        cache_items.setdefault(month_dir(row[time_col]), set()).add(
            str(row[item_col])
        )

    # ---- Which months this run writes -------------------------------------
    # One `# Decision —` per call below. Everything they call is in
    # steps/predict_months.py, which holds no decision of its own and touches
    # no SparkSession — that is what lets these judgements be tested in
    # milliseconds, and they are the ones where being wrong is silent.

    # Decision — the config is the authority on which months exist; the cache is
    # only where their rows come from. Deliberately no "fall back to whatever
    # the cache holds": that would resurrect a month dropped from the config,
    # which is the one thing the authority rule exists to prevent.
    months = configured_months(
        (parameters.get("dataset") or {}).get("test_snap_dates") or []
    )

    # Decision — a configured month with no rows in the cache stops the run.
    # Pre-check: what it compares against exists only once the cache has been
    # read, so it cannot move to core/consistency.py. Letting it through would
    # read as ∅ == ∅ two decisions down, i.e. "already complete", and hand
    # evaluation an empty report for a month the operator asked for. It runs
    # before the partition listing below so a config error costs no metastore
    # round trip — the one ordering difference from the arrangement this
    # replaced, where the listing was evaluated as an argument.
    require_months_are_cached(months, cache_items)

    # Decision — which way to fail when the metastore cannot answer cleanly. A
    # dataset type that cannot list partitions at all, and a Hive NULL partition
    # value (the parquet side spells it None, so the two would never match),
    # both count as "not written yet". That re-predicts, which is wasteful, in
    # preference to skipping, which is silently stale.
    written_items = written_prediction_partitions(
        training_eval_predictions, time_col, item_col
    )

    # Decision — what "already done" means: the item partitions written for this
    # model_version are *exactly* the distinct items the month's cache holds.
    # The weaker "some partition exists" test would call a run that died halfway
    # complete, leaving its missing items absent forever, and would not notice a
    # month that gained an item after it was first predicted.
    done = months_already_written(months, cache_items, written_items)

    # Decision — --rebuild-dates overrides completeness. Skipping is safe only
    # because a (model_version, snap_date) prediction set is immutable: the same
    # model over the same month's rows predicts bit-identically. An upstream
    # backfill changes those rows without changing either version, and this flag
    # is the operator's only way to say so.
    rebuild = {
        month_dir(d) for d in (parameters.get(REBUILD_SNAP_DATES_KEY) or [])
    }

    # Decision — a month holding prediction partitions for items the cache no
    # longer has is re-predicted and warned about, never repaired. Re-predicting
    # writes the items that are in the cache and cannot delete one that is not,
    # so the surplus survives every run — and compute_test_mAP_spark reads the
    # whole model_version, so a stale item keeps contributing rows to the metric.
    warn_about_surplus_partitions(
        months, cache_items, written_items, exclude=rebuild
    )

    plan = plan_predict_months(months, done=done, rebuild=rebuild)
    logger.info(
        "[months] predict: processed=%s skipped=%s rebuilt=%s",
        ",".join(plan.to_process) or "-",
        ",".join(plan.skipped) or "-",
        ",".join(plan.rebuilt) or "-",
    )

    process_keys = {month_dir(m) for m in plan.to_process}
    # .map keeps this a row mask even when the frame is empty; a list
    # comprehension would degrade into `pdf[[]]`, which pandas reads as
    # "select these zero *columns*".
    partition_pdf = partition_pdf[
        partition_pdf[time_col].map(lambda v: month_dir(v) in process_keys)
    ]

    snap_dates_seen: set[str] = set()
    prods_seen: set[str] = set()
    n_rows_written = 0
    is_calibrated = isinstance(model, CalibratedModelAdapter)

    for _, row in partition_pdf.iterrows():
        snap_date = str(row[time_col])
        prod_name = str(row[item_col])

        # A step name built from the data gives the log aggregator one name
        # per (month, item) pair; the values travel as structured fields
        # instead, and the console message still carries them. The field is
        # keyed on the schema role (`item_name`), not on the local variable,
        # which still carries this repo's default column name.
        with log_step(
            logger, "predict_partition",
            snap_date=snap_date, item_name=prod_name,
        ):
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

            X = pdf_to_X(part_pdf, preprocessor_metadata, parameters)
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
    """Record this run in MLflow. The DAG's terminal sink.

    Nothing downstream reads the run, so its whole value is being comparable to
    other runs later — which is why a tracking failure is not allowed to take
    the training with it (see the ``strict`` comment below for the trade).

    What each field is *called* lives in ``steps/experiment_log.py``. Those
    names are read by people and dashboards outside this repo, and renaming one
    fails silently: the run still succeeds and a chart just stops having a line.
    """
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")
    # MLflow logging is a best-effort sink node (terminal in the DAG, nothing
    # downstream depends on it). When the tracking server is unavailable or
    # version-incompatible — a 3.x client calling /api/2.0/mlflow/logged-models
    # on an older server gets a 404 — the default is to warn and let the
    # pipeline finish, rather than let experiment logging take the whole
    # training down with it. Set strict: true to fail hard instead.
    strict = mlflow_params.get("strict", False)
    training_cfg = parameters.get("training", {})
    algorithm = training_cfg.get("algorithm", "lightgbm")
    final_model_strategy = training_cfg.get("final_model_strategy", "hpo_best")

    try:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        with log_step(logger, "mlflow_log"):
            with mlflow.start_run():
                # Decision — the run is keyed by what produced the model, not
                # just its hyperparameters: `algorithm` and
                # `final_model_strategy` are what let two runs with identical
                # best_params still be told apart months later.
                experiment_log.log_run_params(
                    best_params, algorithm, final_model_strategy, best_iteration)

                # Decision — the scores recorded are the evaluation node's, not
                # a re-derivation. Recomputing here would make MLflow and the
                # evaluation report able to disagree with no way to tell which
                # is right.
                experiment_log.log_evaluation_metrics(evaluation_results)

                # Decision — a calibrated run says so, and carries the
                # uncalibrated score beside it. Without the pair, "did
                # calibration help" is unanswerable from the run alone.
                experiment_log.log_calibration_outcome(evaluation_results)

                # The adapter logs its own artifact: only it knows the flavour.
                model.log_to_mlflow()

                # Decision — diagnostics go in twice, as scalars and as files.
                # The scalars are what makes runs comparable in the UI; the
                # files are what someone opens once a scalar looks wrong.
                experiment_log.log_diagnostics_summary(
                    feature_statistics, feature_importance,
                    quadrant_profiles, cases_manifest,
                )

                # --- diagnostics artifacts (JSON written by catalog, PNG by shap node;
                #     upload the whole dir) ---
                # This one write stays in nodes.py rather than moving to
                # steps/experiment_log.py: the architecture audit only scans
                # nodes*.py, so a write that moves out of this file stops being
                # registered. Same call ADR-0014 decision 1 made for shutil.rmtree.
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

    That it is only logged, never computed from, is load-bearing: the manifest
    has a catalog entry, so a ``--from-node`` resume starting here loads the
    *previous* run's copy rather than re-running predict. The trade is argued
    once, at that entry in ``conf/base/catalog.yaml``.
    """
    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]

    # Every block below reads the *whole* test prediction table — all months,
    # all items — which makes this node one of the likelier places for the
    # tail of the pipeline to get slow. Without these it has no timing at all.
    with log_step(logger, "count_distinct_items"):
        n_prods = training_eval_predictions.select(item_col).distinct().count()
    overall_map_key = f"map@{n_prods}"
    item_map_attr_key = f"map_attr@{n_prods}"

    logger.info(
        "compute_test_mAP_spark: starting — n_prods=%d overall_key=%s item_key=%s manifest=%s",
        n_prods, overall_map_key, item_map_attr_key, predict_manifest,
    )

    with log_step(logger, "detect_calibration"):
        calibration_applied = (
            training_eval_predictions.filter(
                F.col("score") != F.col("score_uncalibrated")
            )
            .limit(1)
            .count()
            > 0
        )

    # The action is not on this line: compute_all_metrics counts and collects
    # several times inside evaluation/metrics_spark.py. Rule 10's "follow one
    # level" applies — this is the expensive block, not a lazy plan.
    with log_step(logger, "compute_metrics"):
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
        # The renames are lazy, so they stay outside the timed block.
        uncal_df = (
            training_eval_predictions
            .withColumnRenamed("score", "_score_calibrated")
            .withColumnRenamed("score_uncalibrated", "score")
        )
        with log_step(logger, "compute_metrics_uncalibrated"):
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

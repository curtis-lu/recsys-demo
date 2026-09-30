"""The training pipeline's node functions, all twenty-one of them.

This module is the home of the pipeline's ML story: a reader who opens it sees
each decision this pipeline makes about the data, without jumping files. The
mechanisms those decisions are expressed in live in ``steps/``, one module per
concern (``local_cache``, ``train_data_cache``, ``predict_months``,
``predict_partitions``, ``scored_months``, ``search_space``, ``hpo_resume``,
``hpo_scoring``, ``fit_params``, ``refit``, ``sample_weights``,
``experiment_log``, and for the diagnoses ``bounded_reads``,
``item_sampling``, ``attribution_profiles``, ``gain_ledger``,
``quadrant_population``, ``quadrant_cases``, ``figures``,
``diagnosis_artifacts``). ``cache_sources`` and ``run_contract`` sit beside
this file instead, because ``__main__.py`` asks them before the pipeline
starts. ADR-0014 draws both lines and ``docs/agents/pipeline-node-design.md``
is where the placement criterion and the node-body shape are written down.

**Nothing in here is a pure function**, and the import list is the tell: these
nodes delete cache directories (``shutil.rmtree``), copy Hive partitions onto
driver-local disk, stop the SparkSession in the middle of the DAG, write Hive
one partition at a time, and open an MLflow run. Each of those is argued at its
own call site. What the module promises is not purity but legibility: the
*decision* behind every side effect is readable here rather than buried in a
helper.

How the nodes are laid out
--------------------------
``pipeline.py`` wires 20 nodes, or 19 when ``training.hpo_enabled`` is false:
that mode drops the val copy and puts ``train_with_fixed_params`` where
``tune_hyperparameters`` was (ADR-0030 decision 11). Every one of the 21
functions is ``def``-ed here, in four sections: the two report nodes; the
cache nodes, with ``select_features`` and ``prepare_train_inputs``; the
pipeline nodes, from choosing the hyperparameters to MLflow and the test
metrics; and the seven diagnosis nodes last.

The diagnosis nodes were ``def``-ed under ``recsys_tfb.diagnosis.model``
until ADR-0030 decision 6 brought them here, their decisions floated up into
their bodies and their mechanisms moved into ``steps/``. Only
``diagnostics_dir`` stayed in that library: HPO's search diagnostics
(``diagnosis/hpo``) write under the same directory, and a library may not
import a pipeline.
"""

import logging
import shutil
from functools import partial
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd
import pyarrow.dataset as pads
from pyspark.storagelevel import StorageLevel

from recsys_tfb.core.consistency import (
    DATASET_TEST_RATIO_KEY,
    REBUILD_SNAP_DATES_KEY,
    binary_test_metrics_verdict,
    scoring_snap_dates,
)
from recsys_tfb.core.group_utils import (
    drop_zero_positive_groups,
    to_contiguous_groups,
)
from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.consistency import (
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    DataConsistencyError,
    item_list_counted_from_data,
    optional_role_columns,
    resolved_zero_positive_group_ratio,
    test_carries_zero_positive_group_weight,
    weight_unknown_item_errors,
)
from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.core.schema import get_schema
from recsys_tfb.preprocessing import preprocessor_item_values
from recsys_tfb.core.versioning import (
    TRAINING_PREDICTION_FORMAT_VERSION,
    compute_search_id,
)
from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics
from recsys_tfb.diagnosis.model import diagnostics_dir
from recsys_tfb.evaluation.metric_registry import (
    BINARY_PREDICTION_METRICS,
    METRIC_NAMES,
    RANKING_PASS,
    effective_hpo_objective,
    passes_for,
    read_test_values,
    requested_test_metrics,
    run_test_pass,
    selection_metric,
)
from recsys_tfb.evaluation.metrics_spark import count_query_groups_by_time
from recsys_tfb.io.extract import (
    extract_X_rows,
    extract_Xy,
    extract_Xy_with_groups,
    extract_y,
    extract_y_with_groups,
    pdf_to_X,
    weight_key_columns,
    weight_key_decode_map_from_config,
)
from recsys_tfb.io.handles import (
    ParquetHandle,
    handle_paths,
    open_parquet_dataset,
    require_complete_cache,
    write_group_filter_counts,
    write_weight_keys_sidecar,
)
from recsys_tfb.models.base import (
    ModelAdapter,
    UnsupportedCapability,
    configured_algorithm,
    get_adapter,
)
from recsys_tfb.models.feature_selection import apply_feature_selection
from recsys_tfb.models.feature_view import model_feature_columns, model_feature_view
from recsys_tfb.pipelines.training.steps import (
    bounded_reads,
    experiment_log,
    hpo_resume,
    refit,
    sample_weights,
    scored_months,
    train_data_cache,
)
from recsys_tfb.pipelines.training.steps.attribution_profiles import (
    divergence,
    signed_profile,
)
from recsys_tfb.pipelines.training.steps.diagnosis_artifacts import (
    to_native,
    unsupported_artifact,
)
from recsys_tfb.pipelines.training.steps.figures import beeswarm, safe_name
from recsys_tfb.pipelines.training.steps.fit_params import fit_params
from recsys_tfb.pipelines.training.steps.gain_ledger import (
    coarse_ledger,
    ledger_from_trees,
)
from recsys_tfb.pipelines.training.steps.hpo_scoring import (
    TrialScorer,
    fit_stopping_on_train_dev,
    item_support,
    val_composition,
)
from recsys_tfb.pipelines.training.steps.item_sampling import (
    per_item_background,
    positive_item_sample,
    stratified_item_sample,
)
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
    PREDICTION_FORMATS_FIELD,
    configured_months,
    month_dir,
    months_already_written,
    months_in_another_format,
    plan_predict_months,
    rebuild_month_keys,
    recorded_prediction_formats,
    require_months_are_cached,
    warn_about_months_in_another_format,
    warn_about_surplus_partitions,
    written_prediction_partitions,
)
from recsys_tfb.pipelines.training.steps.predict_partitions import (
    partitions_from_directory_names,
)
from recsys_tfb.pipelines.training.steps.quadrant_cases import case_chart, row_identity
from recsys_tfb.pipelines.training.steps.quadrant_population import (
    QUADRANTS,
    extremes_of_each_cell,
    label_quadrants,
    sample_each_cell,
)
from recsys_tfb.score_output import (
    ScoredFrameLayout,
    require_scored_chunk,
    require_single_partition,
)
from recsys_tfb.utils.ranking import item_sort_codes, rank_by_score_then_item
from recsys_tfb.utils.spark import release_spark_session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


#: Query-group floor below which a split is called out in the log. A rule of
#: thumb, not a derived bound -- early stopping reads a per-group mean off
#: train_dev, and there is no threshold at which that mean stops being
#: meaningful, only a range where it gets noisy. Chosen so the local synthetic
#: train_dev (492 groups once filtered) trips it: production entity
#: populations are millions, so a production run that trips it is reporting
#: something wrong upstream rather than a thin fixture.
THIN_QUERY_GROUPS = 1000


# ---------------------------------------------------------------------------
# Report nodes
# ---------------------------------------------------------------------------
#
# Both return a dict and write nothing: the catalog entry of the same name is
# what lands it in the model version directory (pipeline-node-design.md
# rule 6 — before #483 both were called ``persist_*``, which they had stopped
# doing).


def compute_group_filter_report(train_lgb_handle, parameters: dict) -> dict:
    """Report how many zero-positive query groups training dropped.

    Under ``objective: lambdarank`` the training matrix is not the train /
    train_dev tables: groups with no positive are left out, because their
    lambdarank gradient contribution is exactly zero (which objectives drop
    them is the algorithm's rule, ``ModelAdapter.rules``; LightGBM's reasons
    are at ``models/lightgbm_adapter.LIGHTGBM_RULES``). Row counts
    stop matching the tables, and this is what says why -- comparing two MLflow
    runs on training-set size otherwise gives no way to tell a filter from a
    dataset change.

    Reads the counts through the handle rather than recomputing them: the
    filter runs while the .bin is built (``prepare_train_inputs``), and a run
    that hits that cache never reads a parquet row. Always runs, so the report
    reflects every run including the cache-hit ones.

    ``enabled: False`` for every other objective. That is a finding too --
    an absent report reads the same as a report that failed to run, and
    ``rank_xendcg`` keeping every row is a deliberate difference from
    lambdarank, not an omission.

    The node does not write the file: ``group_filter_report`` is a catalog
    entry pointing at ``data/models/<model_version>/group_filter_report.json``,
    which is what keeps it in the manifest's artifacts list *and* in
    ``extra_metadata.group_filter``.
    """
    objective = (
        (parameters.get("training") or {})
        .get("algorithm_params", {})
        .get("objective")
    )
    report = train_lgb_handle.group_filter_counts()
    if report is None:
        return {"enabled": False, "objective": objective}

    diag = {"enabled": True, **report}
    # Decision -- which splits are too thin to read a per-group metric off.
    # Named rather than raised: how few groups is too few has no derived
    # answer (see THIN_QUERY_GROUPS), so the call belongs to the person
    # reading the log, and stopping the run on a guess would be worse.
    diag["thin_splits"] = sorted(
        split for split in ("train", "train_dev")
        if split in report and report[split]["groups_kept"] < THIN_QUERY_GROUPS
    )
    for split in ("train", "train_dev"):
        if split in report:
            counts = report[split]
            logger.info(
                "zero-positive group filter [%s]: %d of %d groups kept "
                "(%d of %d rows); objective=%s",
                split, counts["groups_kept"], counts["groups_total"],
                counts["rows_kept"], counts["rows_total"], report["objective"],
            )
    for split in diag["thin_splits"]:
        logger.warning(
            "Only %d query groups left in %s after dropping zero-positive "
            "groups (from %d). Below ~%d groups a per-group metric read off "
            "this split is noisy — early stopping on it may be picking noise. "
            "Check the split is as large as you expect before trusting the "
            "result.",
            report[split]["groups_kept"], split,
            report[split]["groups_total"], THIN_QUERY_GROUPS,
        )
    return diag


def compute_sample_weight_report(
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
    # `training.sample_weight_keys`, or the item column alone when unset. Read
    # through the same functions the trainer resolves weights with, so this
    # report describes the lookups training actually makes.
    weight_keys = weight_key_columns(parameters)

    diag = {"enabled": bool(sw), "weight_keys": list(weight_keys),
            "n_weight_entries": len(sw), "unmatched_keys": []}
    if not sw:
        return diag

    decode_map = weight_key_decode_map_from_config(parameters, preprocessor_metadata)

    present = sample_weights.distinct_weight_keys(
        train_parquet_handle, weight_keys, decode_map)

    # Decision — what counts as unmatched, i.e. a weight that did nothing. Two
    # ways in: the train parquet carries no column for the key at all, which
    # condemns every entry at once (the weight key names a column model_input
    # does not have); or this one entry's key is absent from the rows read,
    # including the case where its value is not in the encoding at all.
    if present is None:
        unmatched = [str(key) for key in sw]
    else:
        unmatched = []
        for key in sw:
            nameable = sample_weights.nameable_key(
                key, sw[key], weight_keys, decode_map)
            if nameable is None or nameable not in present:
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
# Four nodes, each writing out its own cache decisions rather than calling one
# shared ``_cache(split_name, parameters)``. That helper is the shape ADR-0008
# §2 forbids and ADR-0014 decision 1 re-affirms: it held four decisions, so
# reading any of the nodes above it told you nothing about what the cache had
# decided. The duplication below is the point; what is shared is the
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
    # copy is the longest-lived of the four (nothing but a new
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

    Returns ``{snap_date: handle}`` keyed by the ``dataset.test_snap_dates``
    values as configured — ``str()`` of each with surrounding whitespace
    stripped (``configured_months``), otherwise not reformatted — sorted so the
    mapping is deterministic. One month per directory is what lets each month be
    cached and invalidated on its own: adding a month copies only that month, and
    a month whose copy was interrupted is rebuilt without disturbing its siblings.

    This is the only one of the four that can be told to drop a *complete* copy.
    Not because the other three never go stale — a backfill under an unchanged
    config leaves any of them stale-but-complete — but because ``--rebuild-dates``
    is constrained to ``dataset.test_snap_dates`` (A21). Clearing the other three
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

    rebuild = rebuild_month_keys(parameters.get(REBUILD_SNAP_DATES_KEY) or [])

    # Decision — what counts as one month. Dedupe on the *directory* form, not
    # the raw string: the same month is the same cache entry however it was
    # spelled. Two DIFFERENT spellings of one month would yield two keys pointing
    # at one directory (handle_paths would hand the same root to pyarrow twice
    # and silently double every row) — that config is rejected at CLI entry by
    # A26, so by the time this runs a key can only be carrying repeats of one
    # literal. The same function predict counts months with, so the two nodes
    # cannot disagree about which months exist.
    months = configured_months(
        (parameters.get("dataset") or {}).get("test_snap_dates") or []
    )

    handles: dict[str, ParquetHandle] = {}
    for month in sorted(months.values()):
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


def select_features(preprocessor_metadata: dict, parameters: dict) -> dict:
    """Apply training-stage feature selection, returning a preprocessor view.

    Single chokepoint for the training pipeline: every model-touching node
    consumes this (possibly subset) view instead of the raw dataset-built
    ``preprocessor``, so ``training.feature_selection.exclude`` is applied
    exactly once and stays consistent across bin-build, HPO, finalize,
    test scoring, and diagnostics. Empty/absent selection returns
    the input unchanged, so non-selection runs are byte-identical.

    Pre-check (runtime A9c, #379): with the item list counted from the data,
    no ``training.sample_weights`` key names an item the preprocessor's list
    lacks. The CLI entry had no list to check against. Here, because this is
    the first node holding the preprocessor and its output is memory-only, so
    every training slice runs it again.
    """
    if item_list_counted_from_data(parameters):
        unknown = weight_unknown_item_errors(
            parameters,
            items=preprocessor_item_values(
                preprocessor_metadata, get_schema(parameters)["item"]),
        )
        if unknown:
            raise DataConsistencyError("\n".join(unknown))
    return apply_feature_selection(preprocessor_metadata, parameters)


def prepare_train_inputs(
    train_parquet_handle: ParquetHandle,
    train_dev_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
):
    """train + train_dev as the configured algorithm's native training data on disk.

    Every HPO trial reads these files (``train.bin`` / ``train_dev.bin``)
    instead of the parquet, so they are built once per content and cached
    beside the parquet copies. The adapter only turns arrays into its own
    format and saves it (ADR-0030 decision 1); what goes into the arrays is
    decided here.

    The matrix is streamed straight into the order the binary holds — groups
    dropped, groups contiguous — so the build holds one matrix, not the matrix
    and a reordered copy of it (ADR-0030 decision 12, item 3).
    """
    algorithm = configured_algorithm(parameters)
    adapter = get_adapter(algorithm)
    objective = (
        (parameters.get("training") or {}).get("algorithm_params", {}).get("objective")
    )
    ranking = adapter.rules.is_ranking_objective(objective)
    feature_columns = list(preprocessor_metadata["feature_columns"])
    categorical_columns = list(preprocessor_metadata.get("categorical_columns", []))
    weight_keys = weight_key_columns(parameters)

    # Decision — the cache is usable exactly when its directory holds
    # _SUCCESS, and nothing inside a directory is checked. That is only safe
    # because everything that decides what the binaries hold is a segment of
    # the path: dataset version and train variant (the rows), algorithm (the
    # file format), objective (grouping and dropped groups), the feature
    # columns, the weight-key columns the sidecar carries, the cache format
    # version for the code itself (ADR-0030 decision 10), and the training
    # model format version (decision 9) — not a version of these files, but a
    # bump of it for a change to how they are built then rebuilds them even
    # if the cache's own number was forgotten. Change any of them and the run
    # looks in a directory that does not exist yet. A change to what this
    # node writes that none of them names is a bump of
    # TRAIN_DATA_CACHE_FORMAT_VERSION; without it the old files are served.
    # Row-wise objectives all build the same rows, no groups, so they share
    # one segment; each ranking objective gets its own, because lambdarank
    # drops groups that rank_xendcg keeps.
    cache_dir = train_data_cache.cache_dir(
        parameters,
        algorithm=algorithm,
        objective_segment=objective if ranking else "binary",
        feature_columns=feature_columns,
        weight_keys=weight_keys,
    )

    # Decision — a directory with no marker is an interrupted build: drop it
    # and build again. Its files are whatever landed before the build died,
    # and a trial reading them would train on part of a split.
    if is_partial_cache(cache_dir):
        log_partial_cache_cleared(cache_dir)
        shutil.rmtree(cache_dir, ignore_errors=True)

    if cache_is_complete(cache_dir):
        log_cache_hit("train_data", cache_dir)
        train_handle, dev_handle = train_data_cache.handles(cache_dir)
        log_data_volume(logger, "prepare.train.bin", train_handle.bin_path)
        log_data_volume(logger, "prepare.train_dev.bin", dev_handle.bin_path)
        return train_handle, dev_handle

    log_cache_miss("train_data", cache_dir)
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    # Decision — train and train_dev take the same rules. train_dev is the
    # early-stopping set in the search, but `final_model_strategy:
    # refit_on_full` trains on it too, so a rule applied to one split only
    # would be wrong under one of the two strategies.
    drops_zero_positive = adapter.rules.objective_drops_zero_positive_groups(objective)
    filter_counts: dict = {"objective": objective} if drops_zero_positive else {}
    reference = None
    for split, parquet_handle in (
        ("train", train_parquet_handle), ("train_dev", train_dev_parquet_handle),
    ):
        if ranking:
            y, group_ids, weight_key_rows = extract_y_with_groups(
                parquet_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            rows = np.arange(len(y))
            # Decision — under an objective whose rules drop zero-positive
            # query groups (LightGBM: lambdarank), those groups are left out
            # of the binary: their gradient is exactly zero, so they cost
            # time and teach nothing. The algorithm says which objectives,
            # with its reasons (ModelAdapter.rules); the counts are kept
            # beside the binary so a cache hit can still report them.
            if drops_zero_positive:
                (y, group_ids, rows), counts = drop_zero_positive_groups(
                    y, group_ids, rows)
                filter_counts[split] = counts
                train_data_cache.log_group_filter(split, counts)
            train_data_cache.log_single_label_groups(split, objective, y, group_ids)
            # Decision — a ranking objective reads each query group as one
            # consecutive block with a per-group row count, so the rows are
            # put in group order. The same order has to reach the labels, the
            # matrix and the weight keys, or a label ends up on another row
            # with nothing raised.
            perm, group = to_contiguous_groups(group_ids)
            rows, y = rows[perm], y[perm]
            X = extract_X_rows(
                parquet_handle, preprocessor_metadata, parameters,
                rows=rows, labels=y,
            )
            weight_key_rows = weight_key_rows.take(rows)
        else:
            # Decision — a row-wise objective keeps every row in file order:
            # no groups to drop, none to keep together, so there are no rows
            # to choose before the matrix is read and one read does it.
            X, y, weight_key_rows = extract_Xy(
                parquet_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            group = None

        data = adapter.build_train_data(
            X, y, group=group, reference=reference,
            feature_names=feature_columns, categorical_features=categorical_columns,
        )
        del X
        path = train_data_cache.bin_path(cache_dir, split)
        # Decision — sample weights are not saved into the binary: the cache
        # path does not name `training.sample_weights`, so a saved vector would
        # be served to a later run configured with other weights, and nothing
        # would say so (#318). What goes beside the binary is the rows'
        # weight-key columns, in the binary's own row order; each run resolves
        # its own weight table against them when it reads the file
        # (LgbDatasetHandle.sample_weights). The adapter refuses a weighted save.
        adapter.save_train_data(data, path)
        write_weight_keys_sidecar(weight_key_rows, path)
        # The four prepare.* volume record names are kept from the adapter this
        # build moved out of: they are a monitoring interface. Their logger
        # field is now this module's (it was models.lightgbm_adapter's) — a
        # filter on the logger name, rather than the record name, needs updating.
        log_data_volume(
            logger, "prepare.ds_train" if split == "train" else "prepare.ds_dev", data)
        log_data_volume(logger, f"prepare.{split}.bin", path)
        if split == "train":
            # Decision — train_dev is binned against train's bin edges
            # (`reference`), so an early-stopping score on it is computed on
            # the features the trial's trees split on; binned on its own, its
            # bins would not line up with the model's thresholds.
            reference = data

    if filter_counts:
        write_group_filter_counts(cache_dir, filter_counts)
    # Last, after every file above: a build that died partway leaves no
    # marker, and the next run clears it instead of serving it.
    mark_cache_complete(cache_dir)
    logger.info("train data cache written: %s", cache_dir)
    return train_data_cache.handles(cache_dir)


# ---------------------------------------------------------------------------
# Pipeline nodes
# ---------------------------------------------------------------------------

def _search_id(parameters: dict) -> str:
    """The HPO ``search_id``, computed here rather than handed in by the CLI.

    Everything it hashes is already in ``parameters`` — the ``training:``
    block and the two dataset version IDs the CLI puts there to resolve the
    catalog — so an injected copy would only be a second answer that could
    disagree with this one (node-design rule 15, ADR-0030 decision 13).
    """
    return compute_search_id(
        parameters,
        str(parameters.get("base_dataset_version", "")),
        str(parameters.get("train_variant_id", "")),
    )


def tune_hyperparameters(
    train_lgb_handle,
    train_dev_lgb_handle,
    val_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[dict, int, ModelAdapter]:
    """Search for optimal hyperparameters using Optuna and return best trial's model.

    train + train_dev consumed as the adapter's cached native training data (no
    rebinning across trials). val is read from parquet into a matrix mapped
    from disk (``io/disk_matrix.py``) — and only when some trial will score on
    it: a search already finished whose checkpoint loads back reads none.

    Returns (best_params, best_iteration, best_model). best_iteration is the
    winning trial's ``ModelAdapter.best_iteration``: the round its train_dev
    early stopping picked. It is consumed by `finalize_model` under the
    `refit_on_full` strategy as the fixed iteration count for the no-val refit.

    The first ``raise`` in the body is a **pre-check** on config:
    ``hpo_objective`` has to name a score this pipeline can compute. A25
    rejects the same value at CLI entry, so a run that reaches this line built
    ``parameters`` without passing that gate (tests, direct calls). It is a
    runtime backstop, and the person to find is whoever wrote the config, not
    whoever produced the data.

    The second is a **pre-check** on the val data, before the first trial: an
    objective in ``metric_registry.BINARY_PREDICTION_METRICS`` over a val set
    holding no positive row. Average precision is undefined there, and any constant
    stood in for it would score every trial alike — the first trial wins and
    the search runs to the end without a word (#430). Only the data can tell,
    so it is checked here rather than at CLI entry; the person to find is
    whoever chose the val window or the ratio. It runs with the val read, so a
    finished search that reads no val does not check it: nothing is scored.
    """
    # HPO and everything after it (finalize_model) is driver-local: Spark
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
    algorithm = configured_algorithm(parameters)

    # Decision — say so when training.fixed_params is written but a search
    # runs. No trial reads it, yet its name reads as "pin these while
    # searching", and it still moves model_version, which looks like it did
    # something.
    if training_params.get("fixed_params"):
        logger.warning(
            "tune_hyperparameters: training.fixed_params is set but not used: "
            "HPO is on, so every trial samples search_space. fixed_params "
            "applies only with training.hpo_enabled: false",
        )

    hpo_objective = effective_hpo_objective(parameters)
    if hpo_objective not in METRIC_NAMES:
        raise ValueError(
            f"unknown training.hpo_objective {hpo_objective!r}; "
            f"allowed: {', '.join(METRIC_NAMES)}"
        )

    checkpointing = parameters.get("hpo_checkpointing", True)
    search_id = _search_id(parameters)
    logger.info("search_id: %s", search_id)

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

    # Decision — val is read only when some trial will score on it (ADR-0030
    # decision 12, item 1). A search that already ran every trial and whose
    # best model loads back from the checkpoint scores nothing: its winner is
    # the checkpoint's. Every other path reads val, the last resort below
    # included — it re-runs the best trial, and that trial is scored like any
    # other. This is the node's largest read (a 37-89 GiB mapped matrix in
    # production), and the run that skips it is an ordinary one: a finished
    # search run again because a later node failed.
    if ckpt is not None and remaining == 0:
        logger.info(
            "HPO target already met (done>=%d) and the checkpoint loads; "
            "val is not read", n_trials,
        )
        best = ckpt  # the keys TrialScorer.best keeps: score/model/iteration/params
    else:
        # val_model_input holds every query group with a positive plus the
        # share of the ones without that dataset.val_zero_positive_group_ratio
        # keeps (none at the default 0; filter_val_keys). No in-pandas
        # re-filter here: the two ranking objectives skip a group without a
        # positive by construction, so kept zero-positive groups leave them
        # unchanged (ADR-0025 decision 3); the two binary-prediction objectives
        # need exactly those groups, weighted by zero_positive_group_weight
        # (#430, A48).
        #
        # Decision — the val matrix is mapped from disk, not held on the heap.
        # This is the one caller that keeps a matrix for the whole search
        # rather than for one fit, and in production it is 37-89 GiB on a 128
        # GiB driver; mapped, the search's resident memory stops tracking the
        # val row count and the pages the current predict batch is not touching
        # are the OS's to reclaim. Unconditional on purpose — a "small enough
        # for RAM" branch would only ever run at the sizes nobody tests. The
        # file is unlinked as soon as it is mapped, so cleanup needs nothing
        # from this node; see `io/disk_matrix.py`, including why a full disk
        # here would otherwise corrupt the matrix in silence.
        #
        # Decision — both objectives read the item column. Tied scores rank by
        # item, the rule the evaluation metrics use (`utils/ranking.py`, #355),
        # so a trial's score does not depend on the order the val rows were
        # read in. The raw values become order-preserving codes once, here:
        # every trial ranks the same items, and sorting a string per row on
        # each trial is work the search would repeat for nothing.
        #
        # Decision — only a binary-prediction objective reads val's
        # zero_positive_group_weight. The ranking objectives never weigh a row,
        # and the column exists only when the val ratio is above 0, which A48
        # guarantees for these objectives alone.
        binary_objective = hpo_objective in BINARY_PREDICTION_METRICS
        with log_step(logger, "extract_features"):
            extracted = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
                with_items=True, with_event=True,
                with_zero_positive_group_weight=binary_objective,
                on_disk_label="hpo_val_matrix",
            )
        X_v, y_v, groups_v, items_v, event_keys_v = extracted[:5]
        weights_v = extracted[5] if binary_objective else None
        items_v = item_sort_codes(items_v)
        # Same pre-coding, same reason, for each `event` column — the list is
        # empty unless the deployment declares the role, so a deployment
        # without one pays nothing and ranks exactly as it did.
        event_keys_v = [item_sort_codes(k) for k in event_keys_v]

        # Decision — say what a binary-prediction objective will average over,
        # before the search spends hours on it (#430). The kept group count and
        # the weight they carry are what a reader needs to judge how steady a
        # score weighted by 1/r is (ADR-0025 decision 3); the row counts are
        # the cost lever — every trial predicts every row, and the kept groups
        # are what r adds. The weight is read off the data, beside the config's
        # r: training reads the dataset version on disk, which can predate the
        # config.
        if binary_objective:
            val = val_composition(groups_v, y_v, weights_v)
            logger.info(
                "tune_hyperparameters: %s scores every val row; query groups "
                "holding a positive=%d (rows=%d); kept query groups holding "
                "none=%d (rows=%d); dataset.val_zero_positive_group_ratio=%g in "
                "the config; zero_positive_group_weight on kept groups=%s in the "
                "val read",
                hpo_objective, val.groups_with_positive, val.rows_with_positive,
                val.groups_without, val.rows_without,
                resolved_zero_positive_group_ratio(parameters, "val"),
                "/".join(f"{w:g}" for w in val.kept_group_weights) or "none",
            )
            if val.rows_with_positive == 0:
                raise ValueError(
                    f"{hpo_objective}: val holds no positive row, so average "
                    f"precision is undefined and every trial would score alike. "
                    f"Check the val window (dataset.val_snap_dates) and the label "
                    f"source; no trial was run."
                )
        # Decision — for the per-item mean, also say which items it covers:
        # only items with a positive in val enter it, and each weighs the same
        # however few positives it has (#430). Many items on one or two
        # positives means the mean is noisy; the docs point such a deployment
        # at the pooled one.
        if hpo_objective == "macro_per_item_average_precision":
            support = item_support(items_v, y_v)
            logger.info(
                "tune_hyperparameters: items entering the mean=%d of %d; "
                "positives per entering item: min=%d median=%g",
                support.entering, support.all, support.fewest, support.median,
            )

        # Optuna only ever sees the float a trial returns, so the winning model
        # has to be kept on the callable itself — that is what `scorer.best` is
        # for. Built after the checkpointing branch because `study_dir` is one
        # of its arguments: `None` means "do not checkpoint", and that branch
        # is the only one that sets it.
        scorer = TrialScorer(
            train_lgb_handle=train_lgb_handle,
            train_dev_lgb_handle=train_dev_lgb_handle,
            # Resolved here, once, against the rows each .bin actually holds.
            # The binaries carry no weights — their cache path says nothing
            # about `training.sample_weights`, so a baked vector would outlive
            # the config that produced it (#318) — and the resolution is the
            # same for every trial, so it does not belong inside the search
            # loop.
            train_weights=train_lgb_handle.sample_weights(
                parameters, preprocessor_metadata),
            train_dev_weights=train_dev_lgb_handle.sample_weights(
                parameters, preprocessor_metadata),
            X_val=X_v, y_val=y_v, groups_val=groups_v, items_val=items_v,
            event_keys_val=event_keys_v,
            zero_positive_group_weight_val=weights_v,
            algorithm=algorithm,
            # Decision — a trial trains under the stacking the refit uses:
            # algorithm_params (ranking metric defaulted by the algorithm's
            # rules), then the seed, then the trial's own sample. One function
            # for both, so the hyperparameters reported for the winner are the
            # ones the final model is trained with.
            params_for_trial=partial(
                fit_params, parameters, get_adapter(algorithm).rules),
            search_space=search_space,
            hpo_objective=hpo_objective,
            num_iterations=num_iterations,
            early_stopping_rounds=early_stopping_rounds,
            n_trials=n_trials,
            search_id=search_id,
            study_dir=study_dir,
        )

        # Decision — a resumed search inherits the previous run's winner before
        # it scores anything. Skip this and the first new trial wins by default
        # (best-so-far starts at -1.0), silently shipping a worse model and
        # checkpointing over the better one.
        if ckpt is not None:
            scorer.adopt_checkpoint(ckpt)

        if remaining > 0:
            with log_step(logger, "optuna_optimize"):
                study.optimize(scorer, n_trials=remaining)
        else:
            logger.info(
                "HPO target already met (done>=%d); skipping optimize",
                n_trials,
            )

        # last-resort: study has trials but no usable checkpoint model — refit
        # best_params once.
        if scorer.best["model"] is None:
            logger.warning(
                "No usable best model from memory/checkpoint; "
                "refitting study.best_params once (last-resort recovery)"
            )
            study.enqueue_trial(study.best_params)
            with log_step(logger, "last_resort_refit"):
                study.optimize(scorer, n_trials=1)

        best = scorer.best

    best_params = best["params"] or study.best_params
    best_model = best["model"]
    best_iteration = best["iteration"]
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, best["score"], best_iteration, best_params,
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


def train_with_fixed_params(
    train_lgb_handle,
    train_dev_lgb_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[dict, int, ModelAdapter]:
    """Train one model on ``training.fixed_params``: what the DAG runs in
    place of ``tune_hyperparameters`` when ``training.hpo_enabled`` is false
    (ADR-0030 decision 11).

    Hands on the same three things the search does — ``best_params`` (the
    fixed params, where the search hands on the winning trial's sample),
    ``best_iteration`` and the fitted model — so ``finalize_model`` and
    everything after it run unchanged, both ``final_model_strategy`` values
    included: the refit stacks ``best_params`` exactly as it stacks a trial's.

    The fit is one trial's in everything but the choosing, so empty
    ``fixed_params`` means "nothing beyond ``algorithm_params``", not
    "LightGBM's defaults": the seed, the ranking metric the algorithm's rules
    fill in, the round cap, early stopping on train_dev and this run's sample
    weights all still apply. A reserved key in ``fixed_params`` (``seed``,
    ``objective``, ...) is refused before Spark starts (A58); written here it
    would be overwritten by, or overwrite, the framework's value in silence.

    Reads no val — the DAG of this mode copies none — and writes nothing: no
    study, no checkpoint, no search diagnostics. Its three outputs have
    catalog entries, so ``--from-node finalize_model`` does not train again.
    """
    # Same reason, same place as in tune_hyperparameters: from here to
    # predict_and_write_test_predictions everything is driver-local, and an
    # idle application gets reclaimed by the cluster.
    release_spark_session(parameters)

    training_params = parameters["training"]
    # Decision — say which settings this mode leaves unread. Both still sit in
    # the config and still move model_version (hashing them is the safe side:
    # a version rule with conditions is where keys get missed), so a reader
    # comparing two runs needs to know they did nothing here.
    logger.info(
        "train_with_fixed_params: training.hpo_enabled is false, so "
        "training.search_space and training.n_trials are not used and val is "
        "not read",
    )
    fixed_params = dict(training_params.get("fixed_params") or {})
    logger.info("train_with_fixed_params: fixed_params=%s", fixed_params)

    adapter = get_adapter(configured_algorithm(parameters))
    # Decision — one fit, trained the way one trial is: algorithm_params
    # (ranking metric defaulted by the algorithm's rules), then the seed, then
    # fixed_params where the trial's sample would be; training.num_iterations
    # caps the rounds and early stopping reads train_dev. One function for the
    # trial and for this, so a config switched between the two modes differs
    # only in how the hyperparameters were chosen.
    adapter = fit_stopping_on_train_dev(
        adapter, train_lgb_handle, train_dev_lgb_handle,
        train_weights=train_lgb_handle.sample_weights(
            parameters, preprocessor_metadata),
        train_dev_weights=train_dev_lgb_handle.sample_weights(
            parameters, preprocessor_metadata),
        params=fit_params(parameters, adapter.rules, fixed_params),
        num_iterations=training_params.get("num_iterations", 500),
        early_stopping_rounds=training_params.get("early_stopping_rounds", 50),
    )
    logger.info(
        "train_with_fixed_params: best_iteration=%d", adapter.best_iteration)
    return fixed_params, adapter.best_iteration, adapter


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

    adapter = get_adapter(configured_algorithm(parameters))
    objective = parameters["training"].get("algorithm_params", {}).get("objective")

    logger.info(
        "final_model_strategy=refit_on_full (num_iterations=%d, no early stopping)",
        best_iteration,
    )

    feat_cols = list(preprocessor_metadata["feature_columns"])
    cat_cols = list(preprocessor_metadata.get("categorical_columns", []))
    splits = (train_parquet_handle, train_dev_parquet_handle)

    # The rows are chosen from the labels and group ids first, and the matrix
    # is then streamed straight into that order across both splits
    # (extract_X_rows): stacking two finished matrices and reordering the
    # result held two full copies at the peak (ADR-0030 decision 12, item 3).
    if adapter.rules.is_ranking_objective(objective):
        with log_step(logger, "extract_features"):
            y_tr, gid_tr, w_tr = extract_y_with_groups(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            y_dv, gid_dv, w_dv = extract_y_with_groups(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        rows_tr, rows_dv = refit.stacked_row_numbers(len(y_tr), len(y_dv))
        if adapter.rules.objective_drops_zero_positive_groups(objective):
            # Decision — the refit trains on the rows the search trained on.
            # HPO reads the cached .bin, which prepare_train_inputs already
            # stripped of zero-positive query groups; this branch re-reads the
            # parquet, so without the same rule the final model would be fit on
            # a matrix the search never saw and still be reported under the
            # search's hyperparameters. Applied per split, before stacking, for
            # the same reason it is applied per split there: the two are one
            # rule, and a reader comparing them should not have to check.
            (y_tr, gid_tr, w_tr, rows_tr), counts_tr = drop_zero_positive_groups(
                y_tr, gid_tr, w_tr, rows_tr)
            (y_dv, gid_dv, w_dv, rows_dv), counts_dv = drop_zero_positive_groups(
                y_dv, gid_dv, w_dv, rows_dv)
            logger.info(
                "refit zero-positive filter: train %d -> %d rows, "
                "train_dev %d -> %d rows",
                counts_tr["rows_total"], counts_tr["rows_kept"],
                counts_dv["rows_total"], counts_dv["rows_kept"],
            )
        y_full, w_full, rows_full = refit.stack_splits(
            (y_tr, w_tr, rows_tr), (y_dv, w_dv, rows_dv))
        # Decision — train / train_dev are customer-disjoint by sampling
        # design, so a query group never spans both splits: dev ids are offset
        # past train's max to keep them distinct after stacking.
        gid_full = refit.offset_dev_group_ids(gid_tr, gid_dv)
        del y_tr, y_dv, gid_tr, gid_dv, w_tr, w_dv, rows_tr, rows_dv

        # Decision — group= makes this a ranking refit consistent with the
        # objective, and the row order follows the groups: the permutation
        # to_contiguous_groups returns has to reach the matrix rows, the labels
        # and the weights alike, or the labels no longer belong to the rows
        # they came from.
        perm, grp = to_contiguous_groups(gid_full)
        y_full, w_full = y_full[perm], w_full[perm]
        X_full = refit.stacked_matrix(
            splits, preprocessor_metadata, parameters,
            rows=rows_full[perm], labels=y_full,
        )
        ds_full = adapter.build_train_data(
            X_full, y_full, weight=w_full, group=grp,
            feature_names=feat_cols, categorical_features=cat_cols,
        )
    else:
        with log_step(logger, "extract_features"):
            y_tr, w_tr = extract_y(
                train_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            y_dv, w_dv = extract_y(
                train_dev_parquet_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
        y_full, w_full = refit.stack_splits((y_tr, w_tr), (y_dv, w_dv))
        del y_tr, y_dv, w_tr, w_dv
        X_full = refit.stacked_matrix(
            splits, preprocessor_metadata, parameters, rows=None, labels=y_full,
        )

        # Decision — no group=: a non-ranking objective scores each row on its
        # own, so there is no query grouping to carry and no reordering to do.
        ds_full = adapter.build_train_data(
            X_full, y_full, weight=w_full,
            feature_names=feat_cols, categorical_features=cat_cols,
        )
    del X_full

    # Decision — the refit trains under the search's stacking with the winning
    # trial's hyperparameters on top, for exactly best_iteration rounds: there
    # is no validation split left to stop early on.
    with log_step(logger, "model_refit"):
        adapter.train(
            ds_full, fit_params(parameters, adapter.rules, best_params),
            num_iterations=best_iteration, early_stopping_rounds=0,
        )

    logger.info(
        "Refitted on full train+train_dev (n=%d, iterations=%d)",
        len(y_full), best_iteration,
    )
    return adapter


def predict_and_write_test_predictions(
    model: ModelAdapter,
    test_parquet_handle: dict[str, ParquetHandle],
    preprocessor_metadata: dict,
    parameters: dict,
    predict_manifest_on_disk: dict | None,
    training_eval_predictions,  # HiveTableDataset, supplied via Node(writes=...)
) -> dict:
    """Per-partition test prediction + Hive write, one month at a time.

    Months whose predictions are already complete, in this code's prediction
    format, are skipped (the nine month decisions are written out in the body,
    under "Which months this run writes"), so adding a test month costs one
    month of prediction rather than re-predicting every accumulated month.
    ``predict_manifest_on_disk`` is the manifest the last completed run landed
    (``None`` before one has), read for the format each month is in: this
    node's own output cannot also be its input (A6), so the CLI derives a
    second name for the same file (A56). Its one blind spot is a rollback: a
    newer code that died partway leaves the older record, so after rolling
    back, name every configured month in ``--rebuild-dates``. The manifest
    names what was processed, skipped and rebuilt: a node that decides to do
    less work has to say what it decided not to do, or a silently stale month
    is indistinguishable from a correctly skipped one.

    For each (snap_date, prod_name) partition of the months being processed:
        - read that partition's rows, and of its columns only the ones the
          output frame carries and the ones the model scores from
        - score them through the adapter (``ModelAdapter.score``) — the entry
          inference scores through too
        - lay out the frame (``score_output.ScoredFrameLayout``): every
          schema.entity column, the score under ``schema.score``,
          ``score_uncalibrated`` (deprecated, equal to the score, kept only so
          the landed table's shape does not change; #412 removes it), the
          optional-role columns, the label, the zero-positive group weight
          when there is one, and the partition columns
        - check before the write, two kinds (``pipeline-node-design.md``
          rule 11). **Post-conditions** on the frame this node built: one
          partition (``ValueError``), one row out per row read and no NULL
          score (``ScoredChunkError``). **Pre-checks** on the rows it read:
          no NULL entity and no row repeating ``identity_columns``
          (``ScoredChunkError``) — nothing this node does can null an entity
          or repeat a row, so these fail only on a wrong test_model_input,
          and the place to look is dataset. Any failure stops the run before
          that partition is saved.
        - training_eval_predictions.save(df): exactly one partition's rows per
          save, so dynamic-partition overwrite cleanly overwrites a single
          partition and successive saves don't collide

    Which (snap_date, prod_name) pairs exist is read off the cache's partition
    directory names, not its rows (``steps/predict_partitions.py``).

    test_model_input is filtered upstream (filter_test_keys in the
    dataset pipeline): every query group holding a positive, plus the share
    ``dataset.test_zero_positive_group_ratio`` keeps of the ones holding none
    — none at the default 0. Above 0 the rows carry the zero-positive group
    weight, and it is written here too.

    Returns:
        The manifest. It has a catalog entry (issue #233), so it lands at
        ``data/models/<model_version>/predict_manifest.json`` and the three
        month lists stay answerable once the run is over. Downstream it is a
        DAG-ordering dependency only — the predictions themselves are read
        back from Hive — and landing it is also what lets a diagnosis-only
        resume skip this node rather than pay its partition listing again.
        Its one reader that takes a value from it is this node's next run,
        through ``predict_manifest_on_disk``.
    """
    schema_cfg = get_schema(parameters)
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    item_col = schema_cfg["item"]
    label_col = schema_cfg["label"]
    score_col = schema_cfg["score"]
    # Empty unless the deployment declares an optional role; resolved through
    # the shared predicate so the write and the A39 gate that checks it can
    # never disagree about which columns those are.
    optional_role_cols = optional_role_columns(parameters)
    # Decision — the zero-positive group weight is read from the test table
    # only when test kept some of those groups (ADR-0025 decision 3); decided
    # from the config, not from the cached parquet, which is `columns: "auto"`
    # and so holds the column as NULL in a partition written under ratio 0
    # once any run added it. A45 makes the write target declare it.
    #
    # Decision — a write target that declares the column always gets it, NULL
    # when test kept no zero-positive group: a Hive save selects every declared
    # column, so leaving it out would fail there the day the ratio goes back to
    # 0. NULL rather than 1.0 because no design weight applies, and evaluation
    # refuses a NULL weight under a positive ratio (conf and --model-version
    # disagree) instead of reading it as a weight. Undeclared and ratio 0: the
    # frame every existing deployment writes, unchanged.
    carries_weight = test_carries_zero_positive_group_weight(parameters)
    declared = getattr(training_eval_predictions, "declared_columns", None)
    write_weight = carries_weight or (
        isinstance(declared, (list, tuple))
        and ZERO_POSITIVE_GROUP_WEIGHT_COL in declared
    )
    model_version = parameters["model_version"]

    # Decision — what the written frame holds.
    # - Every entity column, not just the first: the identity of a scored row
    #   is the whole tuple, written as `str` because that is what the ranking
    #   side compares on. That the write target declares all of them is A28,
    #   checked at CLI entry — a column it never declared is dropped by `save`
    #   in silence.
    # - Every declared optional-role column, for the entity columns' reason:
    #   with `event` declared the identity of a scored row includes it, and
    #   without it the published table holds several rows per item that
    #   nothing can tell apart — evaluation's duplicate check then raises on a
    #   table that was correct when written. Empty for every deployment that
    #   declares no optional role, so the frame is unchanged there. That the
    #   write target declares them is A39, checked at CLI entry beside A28.
    #   Carried, NOT stringified: `event` may be a timestamp, and the tie-break
    #   compares it by its own type (`utils/ranking.py`), so `"10" < "2"` would
    #   reorder ranks.
    # - The label, which evaluation scores the predictions against.
    # - The zero-positive group weight: 1 on a group holding a positive, 1/r
    #   on a kept zero-positive group, NULL when test kept none; absent unless
    #   test kept any or the write target declares it (the two decisions
    #   above).
    # - The score under `schema.score`: a deployment that renames it declares
    #   the new name in the catalog, and a hardcoded name would land the
    #   scores in an undeclared column.
    layout = ScoredFrameLayout(
        entity_cols=entity_cols,
        score_col=score_col,
        carried_cols=[
            *optional_role_cols,
            label_col,
            *([ZERO_POSITIVE_GROUP_WEIGHT_COL] if carries_weight else []),
        ],
        null_cols=(
            [ZERO_POSITIVE_GROUP_WEIGHT_COL]
            if write_weight and not carries_weight else []
        ),
    )

    # Decision — each partition reads the columns the frame carries and the
    # columns the model scores from, and nothing else. Both are asked of their
    # owners rather than listed here: the frame reads the weight only under a
    # positive ratio, and a composite model reads a group key that need not be
    # a feature, so a fixed list would miss whichever came last (ADR-0030
    # decision 12.2). The replaced read took every column of the partition.
    read_cols = list(dict.fromkeys(
        layout.source_columns() + model.scoring_columns(preprocessor_metadata)
    ))

    # Decision — what counts as a duplicate row, checked before each write:
    # this pipeline's own `identity_columns`, optional roles included. Under
    # `occasion` or `event` one (time, entity, item) legitimately holds several
    # rows — distinct candidates with their own labels — so the narrower key
    # inference checks (it ignores the optional roles, ADR-0025 decision 1)
    # would report every one of them (ADR-0030 decision 5).
    identity_cols = schema_cfg["identity_columns"]

    # Decision — which output columns must not be NULL: the score, the one
    # column this node computes. The two partition values are not listed:
    # each is `str(...)` of a directory name, which is never NULL (a Hive
    # NULL partition is refused when the partitions are listed), so a check
    # on them could never fire — the decorative shape ADR-0011 removes. The
    # entity columns are checked on the rows they were read from, since `str`
    # turns a NULL into "None". The carried columns are not listed: they
    # arrive from dataset unchanged, and whether a label or an event may be
    # NULL is dataset's contract, not something this node vouches for.
    not_null_cols = [score_col]

    # partitioning="hive" tells pyarrow to reconstruct (snap_date, prod_name)
    # columns from the snap_date=*/prod_name=* directory tree produced by
    # HiveTableDataset.save() (and by the test fixture's pq.write_to_dataset).
    ds = open_parquet_dataset(handle_paths(test_parquet_handle))

    # The (snap_date, prod_name) pairs the cache holds, from its directory
    # names alone — a row read of the two partition columns materialises one
    # row per data row, every month, before any month is known to need work.
    cache_partitions = [
        (str(snap_date), str(prod_name))
        for snap_date, prod_name in partitions_from_directory_names(
            ds, time_col, item_col)
    ]
    cache_items: dict[str, set[str]] = {}
    for snap_date, prod_name in cache_partitions:
        cache_items.setdefault(month_dir(snap_date), set()).add(prod_name)

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
    complete = months_already_written(months, cache_items, written_items)

    # Decision — which record of the prediction format counts: the manifest
    # the last completed run of this model_version landed, and nothing else.
    # Read from the manifest rather than the table, which holds no such
    # column. A manifest another model_version wrote (a catalog path without
    # the version in it) is no record: its months could name this format
    # while these partitions were written in an older one.
    recorded_formats = recorded_prediction_formats(
        predict_manifest_on_disk, model_version
    )

    # Decision — a complete month counts as done only when that record has it
    # in this code's prediction format (ADR-0030 decision 9): skipping rests
    # on "same model_version, same predictions", which a code change to
    # scoring breaks without moving model_version. No record counts as
    # another format — no manifest yet, one from before the field, a month the
    # last run did not configure (dropped, then configured again) — which
    # re-predicts rather than skips, the direction every decision here fails
    # in. One record per month rather than one number for the manifest, so a
    # month the last run did not configure is not taken to be in its format.
    # Two prices, both of the record being written only when this node
    # finishes: the first run of a model_version that dies partway leaves no
    # record, so its rerun re-predicts the months it had finished too; and a
    # newer code that dies partway leaves the older record, so rolling back to
    # the older code skips months the newer one re-wrote — the rollback must
    # name every month in --rebuild-dates.
    stale_format = months_in_another_format(
        months, recorded_formats, TRAINING_PREDICTION_FORMAT_VERSION
    )
    done = complete - stale_format

    # Decision — the months re-predicted for their format are named, but only
    # the complete ones: a month with nothing written has no record either,
    # and naming it would warn on every first run of a model_version.
    warn_about_months_in_another_format(
        months, complete, stale_format, recorded_formats,
        TRAINING_PREDICTION_FORMAT_VERSION,
    )

    # Decision — --rebuild-dates overrides completeness. Skipping is safe only
    # because a (model_version, snap_date) prediction set is immutable: the same
    # model over the same month's rows predicts bit-identically. An upstream
    # backfill changes those rows without changing either version, and this flag
    # is the operator's only way to say so.
    rebuild = rebuild_month_keys(parameters.get(REBUILD_SNAP_DATES_KEY) or [])

    # Decision — a month holding prediction partitions for items the cache no
    # longer has is re-predicted and warned about, never repaired. Re-predicting
    # writes the items that are in the cache and cannot delete one that is not,
    # so the surplus survives every run — and compute_test_metrics reads every
    # item partition of the scored months, so a stale item keeps contributing
    # rows to the metric.
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

    snap_dates_seen: set[str] = set()
    items_seen: set[str] = set()
    n_rows_written = 0

    for snap_date, prod_name in cache_partitions:
        if month_dir(snap_date) not in process_keys:
            continue

        # A step name built from the data gives the log aggregator one name
        # per (month, item) pair; the values travel as structured fields
        # instead, and the console message still carries them. Both fields are
        # keyed on the schema *role*, not on the local variables, which still
        # carry this repo's default column names (`snap_date`, `prod_name`).
        with log_step(
            logger, "predict_partition",
            time_value=snap_date, item_name=prod_name,
        ):
            part_table = ds.to_table(
                filter=(pads.field(time_col) == snap_date)
                & (pads.field(item_col) == prod_name),
                columns=read_cols,
            )
            # Same reasoning as the step name above, one key over: a
            # `volume.name` built from the data is `n_months * n_items`
            # buckets. `log_data_volume` merges `**fields` into the `volume`
            # dict, so the identity survives without splitting the name.
            log_data_volume(
                logger, "predict.part_table", part_table,
                time_value=snap_date, item_name=prod_name,
            )
            part_pdf = part_table.to_pandas()
            log_data_volume(
                logger, "predict.part_pdf", part_pdf, deep=True,
                time_value=snap_date, item_name=prod_name,
            )

            snap_dates_seen.add(snap_date)
            items_seen.add(prod_name)

            # Decision — the score is the model's own answer for these rows,
            # asked through the adapter's scoring entry (ADR-0030 decision 2):
            # the one inference scores through, so a row cannot be encoded one
            # way for the test metric and another way for publication.
            y_score = model.score(part_pdf, preprocessor_metadata, parameters)
            out_pdf = layout.build(
                part_pdf, y_score, {time_col: snap_date, item_col: prod_name},
            )

            # Before the write, two kinds of check (pipeline-node-design.md
            # rule 11), each with this node's own answers from above:
            # - post-conditions on the frame just built: one partition per
            #   save (a frame spanning two would have the second save delete
            #   the first one's rows), one row out per row read, no NULL score;
            # - pre-checks on the rows read: no NULL entity, no row repeating
            #   the identity. Nothing above can null an entity or repeat a
            #   row, so these fail only on a wrong test_model_input — dataset
            #   is where to look.
            # The last four are the per-chunk checks inference also runs, in
            # one call.
            require_single_partition(out_pdf, [time_col, item_col])
            require_scored_chunk(
                out_pdf, part_pdf,
                entity_cols=entity_cols,
                identity_cols=identity_cols,
                not_null_cols=not_null_cols,
            )

            training_eval_predictions.save(out_pdf)
            n_rows_written += len(out_pdf)

    manifest = {
        "snap_dates": sorted(snap_dates_seen),
        "items": sorted(items_seen),
        "model_version": model_version,
        "n_rows_written": n_rows_written,
        # What this run decided about every configured month, not just the ones
        # it touched: `snap_dates` above cannot distinguish "skipped because
        # complete" from "never knew about it".
        "months_processed": plan.to_process,
        "months_skipped": plan.skipped,
        "months_rebuilt": plan.rebuilt,
        # Every configured month, as configured: processed ones were written
        # just now and skipped ones were recorded in this format already. A
        # month not configured is left out, so configuring it again finds no
        # record and re-predicts it (the decision above).
        PREDICTION_FORMATS_FIELD: {
            months[key]: TRAINING_PREDICTION_FORMAT_VERSION
            for key in sorted(months)
        },
    }
    logger.info(
        "predict_and_write_test_predictions: done — "
        "snap_dates=%d items=%d n_rows_written=%d model_version=%s "
        "months_processed=%d months_skipped=%d months_rebuilt=%d",
        len(manifest["snap_dates"]), len(manifest["items"]),
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
    gain_ledger: dict,
    shap_diagnostics: dict,
    quadrant_profiles: dict,
    cases_manifest: dict,
    parameters: dict,
) -> None:
    """Record this run in MLflow. The DAG's terminal sink.

    Nothing downstream reads the run, so its whole value is being comparable to
    other runs later — which is why a tracking failure is not allowed to take
    the training with it (see the ``strict`` comment below for the trade).

    What each field is *called* lives in ``steps/experiment_log.py``. Those
    names are read by people and dashboards outside this repo, and renaming one
    fails silently: the run still succeeds and a chart just stops having a line.

    Every diagnosis comes in as an input, wired by name (ADR-0030 decision 8),
    including ``gain_ledger``, which nothing here reads: taking it is what puts
    ``gain_ledger.json`` on disk before the upload below, by an edge rather
    than by where the sort happened to place its producer. A new diagnosis
    that lands a file in ``diagnostics/`` joins the same way — one parameter
    here, one line in ``pipeline.py``. None has a default, so a parameter left
    unwired fails when the node is called instead of arriving as ``None``.
    The HPO search diagnostics are not an input: ``tune_hyperparameters``
    writes them itself, already upstream through ``best_params``, and without
    a search there are none — the upload takes whatever the directory holds.
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
    algorithm = configured_algorithm(parameters)
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

                # The adapter logs its own artifact: only it knows the flavour.
                model.log_to_mlflow()

                # Decision — diagnostics go in twice, as scalars and as files.
                # The scalars are what makes runs comparable in the UI; the
                # files are what someone opens once a scalar looks wrong.
                experiment_log.log_diagnostics_summary(
                    feature_statistics, feature_importance,
                    quadrant_profiles, cases_manifest,
                )

                # --- diagnostics artifacts (JSON and PNG both written by the
                #     catalog; upload the whole dir) ---
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


def compute_test_metrics(
    training_eval_predictions,  # Spark DataFrame, loaded by catalog (filtered to current model_version)
    predict_manifest: dict,
    parameters: dict,
) -> dict:
    """Score the model on test: the metrics promote and MLflow read, over the
    scored months (ADR-0028).

    Keys:
        overall_map        mean over query groups of each group's AP, no
                           truncation (``mean_ap``)
        per_item_map_attr  {item: mean AP contribution of its positive rows}
        n_queries / n_excluded_queries
                           query groups scored / of those, holding no positive
        snap_dates         the time values scored, as configured
        metrics            {metric: value} for every metric computed on test
        metrics_not_computed
                           {metric: reason} for the ones asked for with no
                           value: the binary-prediction HPO objective when
                           test cannot score it (A54)
        selection_metric / hpo_objective
                           the names this run scored under

    The first four are the ones this node always wrote, value for value when
    the scored months are the table's months: the same building blocks as the
    report's ``map@all`` (``metrics_spark.compute_untruncated_ap``), minus the
    K ``"all"`` never truncated at. So nothing under ``evaluation.*`` is read —
    ``k_values`` without ``"all"`` used to log 0.0 for two of them — and the
    three Spark actions are all the four values and both ranking metrics cost.
    Each binary-prediction metric asked for costs a pass of its own, exact and
    weighted as HPO's val score is (``metrics_spark``; ADR-0028 decision 4).

    predict_manifest is an in-DAG dependency only — its content is logged
    for observability but the actual data is read back from
    training_eval_predictions (Spark-loaded via the catalog).

    That it is only logged, never computed from, is load-bearing: the manifest
    has a catalog entry, so a ``--from-node`` resume starting here loads the
    *previous* run's copy rather than re-running predict. The trade is argued
    once, at that entry in ``conf/base/catalog.yaml``.

    The first two ``raise`` are **runtime backstops**: for A36 / A53, which
    stop the command before it when no month would be scored, and for A54,
    which stops it when a binary-prediction metric the run must record cannot
    be scored on this test. The third is a **pre-check** on the table: a
    scored month with no prediction.
    """
    logger.info("compute_test_metrics: starting — manifest=%s", predict_manifest)
    time_col = get_schema(parameters)["time"]

    # Decision — which months: the scored months (test_metrics.snap_date, or
    # every test_snap_dates month), never whatever the table holds. The table
    # keeps every month this model_version was ever predicted on, so scoring
    # all of it let the score move with the table and let promote rank
    # versions scored on different months (ADR-0028 background three).
    months = scoring_snap_dates(parameters)
    if not months:
        raise ValueError(
            "compute_test_metrics: no scored month — test_metrics.snap_date "
            "and dataset.test_snap_dates are both unset or empty."
        )
    frame = scored_months.restrict_to_scored_months(
        training_eval_predictions, time_col, months)

    # Decision — say which months the table holds but this run does not
    # score, off the partition listing: no Spark job, and those rows are
    # never read. A month left out of test_snap_dates, or out of the scored
    # months on purpose, is named rather than silently dropped.
    in_table = scored_months.partition_months(training_eval_predictions, time_col)
    if in_table is None:
        logger.info(
            "compute_test_metrics: %s is not read from files partitioned by "
            "%s, so the months it holds beyond %s cannot be listed",
            type(training_eval_predictions).__name__, time_col, months,
        )
    else:
        unscored = [m for m in in_table if m not in months]
        if unscored:
            logger.info(
                "compute_test_metrics: %s in the prediction table, not scored "
                "(scored months: %s)", unscored, months,
            )

    # Decision — what to score: both ranking metrics always, plus the
    # selection metric, the HPO objective and test_metrics.metrics (ADR-0028
    # decision 1). Printed before the work, so a reader sees what was asked
    # for even when some of it has no value at the end.
    wanted = requested_test_metrics(parameters)
    selected_by = selection_metric(parameters)
    hpo_objective = effective_hpo_objective(parameters)
    logger.info(
        "compute_test_metrics: scoring %s on %s (selection metric %s, HPO "
        "objective %s)", wanted, months, selected_by, hpo_objective,
    )

    # Decision — a binary-prediction metric is scored only when test kept the
    # query groups holding no positive, in the config and in the dataset
    # version this run read (the CLI passes that version's ratio): otherwise
    # its value would be another population's than the val score HPO chose by.
    # The HPO objective alone is then withheld with the reason; the selection
    # metric or a name in test_metrics.metrics stops the run — A54 already
    # stopped it at the command's entry, so this is the backstop.
    verdict = binary_test_metrics_verdict(
        parameters,
        dataset_version=parameters.get("base_dataset_version"),
        dataset_test_ratio=parameters.get(DATASET_TEST_RATIO_KEY),
    )
    if verdict.errors:
        raise ValueError("compute_test_metrics: " + " ".join(verdict.errors))
    for name, reason in verdict.withheld.items():
        logger.warning(
            "compute_test_metrics: %s has no value on test: %s", name, reason)
    scored = [name for name in wanted if name not in verdict.withheld]

    with log_step(logger, "count_query_groups"):
        counts = count_query_groups_by_time(frame, parameters)

    # Decision — a scored month with no prediction stops the run: a score over
    # the months that happen to be there would be recorded as the score over
    # the months asked for, and promote compares the recorded months. Checked
    # off the count above, not by an action of its own. The table's own
    # months go into the message: a month the data spells otherwise than the
    # config (the filter compares text) shows up there, not as "not predicted".
    missing = [m for m in months if m not in counts.times]
    if missing:
        held = (
            f"the table holds {in_table}" if in_table is not None
            else f"of the scored months it holds {sorted(counts.times)}"
        )
        raise ValueError(
            f"compute_test_metrics: scored month(s) {missing} have no rows in "
            f"training_eval_predictions for this model_version ({held}). Run "
            f"predict for them first — training, or --from-node "
            f"predict_and_write_test_predictions — or leave them out of "
            f"test_metrics.snap_date. A month the table holds under another "
            f"spelling is matched by text only: spell it as the table does."
        )

    # The expensive blocks: each pass the scored metrics need, once, and none
    # nobody asked for. The actions are inside metrics_spark (rule 10's
    # "follow one level"); one step per pass, so each pass's time is its own.
    pass_results = {}
    for name in passes_for(scored):
        with log_step(logger, "compute_metrics", test_pass=name):
            pass_results[name] = run_test_pass(name, frame, parameters)
    values = read_test_values(scored, pass_results)

    # Always there: requested_test_metrics puts both ranking metrics first,
    # which is also what keeps the four keys below present in every file.
    ranking = pass_results[RANKING_PASS]
    result = {
        "overall_map": ranking.overall_map,
        "per_item_map_attr": ranking.per_item_map_attr,
        "n_queries": counts.n_queries,
        "n_excluded_queries": counts.n_queries - counts.n_with_positive,
        "snap_dates": months,
        "metrics": values,
        "metrics_not_computed": verdict.withheld,
        "selection_metric": selected_by,
        "hpo_objective": hpo_objective,
    }

    logger.info(
        "compute_test_metrics: mAP=%.4f items=%d excluded_queries=%d "
        "metrics=%s",
        result["overall_map"],
        len(result["per_item_map_attr"]),
        result["n_excluded_queries"],
        values,
    )

    return result


# ---------------------------------------------------------------------------
# Diagnosis nodes
# ---------------------------------------------------------------------------
#
# Seven nodes that describe the finished model; nothing downstream in this
# pipeline reads them except log_experiment. Each returns its result and
# writes nothing: the JSON and the figures are landed by their catalog entries
# (ADR-0030 decision 7). The model is reached only through the adapter. A
# model that cannot answer a diagnosis — UnsupportedCapability — skips it with
# a warning and lands the "model cannot" shape; any other exception stops the
# run (ADR-0030 decision 4). The mechanisms are in steps/ (bounded_reads,
# item_sampling, attribution_profiles, gain_ledger, quadrant_population,
# quadrant_cases, figures, diagnosis_artifacts).


def compute_feature_statistics(
    train_parquet_handle, model, preprocessor: dict, parameters: dict,
) -> dict:
    """Per-feature null_rate / mean,std,min,max (numeric) / n_distinct, plus the
    ``single_value`` and ``high_null`` flags.

    Memory: the row count comes from parquet metadata, then only the sampled
    ``sample_rows`` rows are read (bounded take) instead of loading the whole
    train split and down-sampling afterwards. The sampled indices are unchanged
    (``RandomState(42).choice``), so the output stays bit-for-bit identical.

    Takes ``model`` although this is a *data*-layer diagnosis — the null rate and
    mean of a training feature owe nothing to a booster. The model is here purely
    as the authority on *which* columns to summarize (ADR-0014 decision 7).

    Not because the alternative is unsafe. Re-deriving the column set from
    ``training.feature_selection`` cannot silently drift: that key lives in the
    ``training:`` block, so editing it bumps ``model_version``, the model's
    catalog path moves, and the whole training chain is pulled back. ADR-0014 is
    explicit that this is interface work, not a bug fix. The reason is that
    ``preprocessor_view`` is memory-only, so reading it forces ``select_features``
    into any slice that wants this node — while ``model`` and ``preprocessor``
    both have catalog entries.

    The coupling is accepted because ``feature_statistics`` already lands under
    ``data/models/${model_version}/``: computing it for a model that does not
    exist was never meaningful.

    The edge is not only a cost. It also orders this node after the one that
    produces the model, where it always belonged: without it the topological sort
    put a diagnosis of ``data/models/${model_version}/`` *ahead* of HPO, so
    ``--from-node compute_feature_statistics`` re-ran HPO and the final fit to
    regenerate this JSON (18 nodes, now 13). What it does cost is
    ``--only-node compute_feature_statistics``, which now needs a
    ``model_version``-scoped input rather than only ``base_dataset_version`` ones,
    and ``--from-node finalize_model``, which picks up this node's train handle.
    Both slices are pinned in ``tests/test_pipelines/test_resume_contracts.py``.
    """
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    # Decision — which features get summarized: the model, not
    # apply_feature_selection(preprocessor, parameters). Pick the config and the
    # stats still come out, just over whatever column set the *current* config
    # names; the docstring argues why that is a worse authority than the model
    # even though the version mechanism keeps it from being an outright bug.
    feature_cols = model_feature_columns(model, preprocessor)

    # Pre-check (input) — an interrupted copy reads as a smaller split rather
    # than as an error, so count_rows would return a number and every statistic
    # below would describe an unknown fraction of train (ADR-0014 decision 7).
    # The cache node's opposite behaviour on the same marker — clear and rebuild
    # from Hive — is the right one there and is not touched.
    require_complete_cache(train_parquet_handle)

    # Decision — which rows: at most sample_rows of train, drawn uniformly
    # without replacement under a fixed seed (RandomState(42)), and all of
    # train when it is no bigger. Every number below describes that sample —
    # n_distinct is capped by it — and the same seed on the same train gives
    # the same numbers on every run.
    path = train_parquet_handle.path
    n = bounded_reads.count_rows(path)
    if n > sample_rows:
        idx = np.sort(np.random.RandomState(42).choice(n, size=sample_rows, replace=False))
        logger.info("feature_statistics: bounded take %d of %d rows", sample_rows, n)
    else:
        idx = np.arange(n, dtype=np.int64)
        logger.info("feature_statistics: reading all %d rows (<= sample_rows)", n)
    pdf = bounded_reads.take_rows(path, idx, columns=feature_cols)
    log_data_volume(logger, "feature_statistics.sample", pdf, deep=True)

    # Decision — what summarizes a feature: its null rate and distinct count
    # for every dtype, mean / std / min / max for numeric ones only.
    # single_value flags at most one distinct non-null value; high_null a null
    # rate at or above high_null_threshold. A NaN statistic lands as null.
    stats: dict = {}
    for col in feature_cols:
        s = pdf[col]
        null_rate = float(s.isna().mean())
        n_distinct = int(s.nunique(dropna=True))
        entry = {
            "null_rate": null_rate,
            "n_distinct": n_distinct,
            "single_value": n_distinct <= 1,
            "high_null": null_rate >= high_null_threshold,
        }
        if pd.api.types.is_numeric_dtype(s):
            entry["mean"] = to_native(s.mean())
            entry["std"] = to_native(s.std())
            entry["min"] = to_native(s.min())
            entry["max"] = to_native(s.max())
        stats[col] = entry
    logger.info("feature_statistics: %d features summarized", len(stats))
    return stats


def compute_feature_importance(model, parameters: dict) -> dict:
    """Split + gain importance, ranked by gain, with the features no split uses.

    A model that keeps no such counts (``UnsupportedCapability``) lands the
    "model cannot" shape and the run goes on; anything else stops it
    (ADR-0030 decision 4).
    """
    cfg = parameters.get("diagnostics", {}).get("feature_importance", {})
    if not cfg.get("enabled", True):
        return {}
    # Decision — a model without split / gain bookkeeping skips this and says
    # so, rather than stopping a run whose model is fine.
    try:
        split = model.feature_importance(kind="split")
        gain = model.feature_importance(kind="gain")
    except UnsupportedCapability as exc:
        logger.warning("feature_importance: skipped, the model cannot provide it: %s", exc)
        return unsupported_artifact(exc)
    # Decision — ranked by gain: a split count says how often a feature is
    # used, not how much it contributes. "Dead" is a split count of 0 — no
    # split in any tree uses the feature — not a low gain, so it is a fact
    # about the trees rather than a cut-off someone chose.
    ranked = sorted(
        ({"feature": f, "split": float(split[f]), "gain": float(gain[f])} for f in split),
        key=lambda r: r["gain"],
        reverse=True,
    )
    dead = sorted(f for f, v in split.items() if v == 0)
    logger.info("feature_importance: %d features, %d dead", len(ranked), len(dead))
    return {"ranked": ranked, "dead_features": dead}


def compute_gain_ledger(model, preprocessor: dict, parameters: dict) -> dict:
    """The model's split gain accounted per item: what isolating each item
    costs and how much gain is spent on it afterwards. What each account
    counts is written in ``steps/gain_ledger.py``; evaluation's
    ``model_capacity`` reads the result.

    ``diagnostics.gain_ledger.enabled`` (default true) switched off returns
    ``{"enabled": False}`` without touching the model.

    A model with no tree structure (``UnsupportedCapability``) lands
    ``{"enabled": True, "supported": False, "reason": ...}`` and the run goes
    on. Any other exception is a bug and stops it (ADR-0030 decision 4).

    ``preprocessor`` is the dataset-built artifact, not the training-stage view.
    Unlike the other diagnosis nodes this one never slices X, so it needs only
    the *encoding* half of the artifact: ``category_mappings`` is the code-to-item
    lookup the tree table's integer category codes have to be read through, and
    feature selection passes it through untouched either way (ADR-0014
    decision 7).
    """
    cfg = (parameters.get("diagnostics", {}) or {}).get("gain_ledger", {}) or {}
    if not cfg.get("enabled", True):
        return {"enabled": False}

    item_col = get_schema(parameters)["item"]
    # Decision — a model without trees skips the ledger, warns and says so in
    # the artifact; evaluation reads that as its own reason, not as "turned
    # off" or "never ran".
    try:
        trees = model.tree_structure()
    except UnsupportedCapability as exc:
        logger.warning("gain_ledger: skipped, the model cannot provide it: %s", exc)
        return unsupported_artifact(exc)
    n_trees = int(trees["tree_index"].nunique())

    # Decision — without the preprocessor's code-to-item mapping for the item
    # column, land the coarse ledger (the item-id account alone, fallback:
    # True, and a note saying why) rather than stop. The trees' category
    # codes cannot be read as items, so the per-item and context accounts
    # cannot be built, but the item-id account needs no codes; evaluation's
    # model_capacity recognises the fallback and says so.
    categories = (preprocessor or {}).get("category_mappings", {}).get(item_col)
    if not categories:
        logger.warning(
            "gain_ledger: preprocessor 缺 category_mappings[%s]，降級為粗帳本", item_col
        )
        return coarse_ledger(trees, item_col, n_trees)

    return ledger_from_trees(trees, item_col, list(categories))


def compute_shap_diagnostics(
    model, test_parquet_handle, preprocessor: dict, parameters: dict,
) -> tuple[dict, dict]:
    """SHAP over a sample of test: the global profile, a profile per item (a
    population-representative sample), and how far each item's ranking of
    the features is from the global one. One attribution pass over the sample
    serves both the global and the per-item profiles; the positive profiles
    take a second pass over a sample of their own.

    Returns ``(shap_diagnostics, shap_summary_figures)``: the JSON result, and
    ``{path under diagnostics/summary/: draw}`` for the beeswarm plots. Both
    empty when disabled. A model that cannot attribute lands the "model
    cannot" shape and no figures; every other exception stops the run
    (ADR-0030 decision 4).

    The model is reached only through the adapter (ADR-0030 decisions 1 and
    4): ``attribution_cost`` for the budget guard, ``predict`` for each item's
    score range, ``feature_attributions`` for the SHAP values. The ``notes``
    written under ``background: per_item`` are part of the JSON and are kept
    word for word.
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("enabled", True):
        return {}, {}

    top_k = int(cfg.get("top_k", 30))
    min_per_item = int(cfg.get("min_rows_per_item", 30))
    sample_rows = int(cfg.get("sample_rows", 2000))
    max_budget = int(cfg.get("max_budget", 4_000_000))
    positive_min_rows = int(cfg.get("positive_min_rows", 20))
    positive_sample_per_item = int(cfg.get("positive_sample_per_item", 30))
    divergence_metric = str(cfg.get("divergence_metric", "jaccard_topk"))
    # Usually smaller than top_k; it only sizes the top-k sets compared (the
    # Jaccard metric's, and the idiosyncratic features).
    divergence_top_k = int(cfg.get("divergence_top_k", 15))
    profile_positive = bool(cfg.get("profile_positive", True))
    background_mode = str(cfg.get("background", "global"))

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    # Decision — which features, and in what order: ask the model, not
    # apply_feature_selection(preprocessor, parameters). This is not a drift fix:
    # the exclude list lives in the `training:` block, so editing it bumps
    # model_version, the model's catalog path moves with it, and the whole
    # training chain is pulled back — ADR-0014 decision 7 is explicit that the
    # version mechanism already blocks that, and that this is interface work, not
    # a bug fix. What it buys is addressability: model and preprocessor both have
    # catalog entries, while the config-derived view is memory-only and drags
    # select_features into every diagnosis-only slice.
    model_view = model_feature_view(model, preprocessor)
    feature_cols = list(model_view["feature_columns"])

    # Pre-check (input) — same contract as compute_feature_statistics: a
    # half-copied month reads as a smaller month, so the stratified sample would
    # be drawn from a split nobody knows the size of (ADR-0014 decision 7).
    require_complete_cache(test_parquet_handle)

    # Decision — which months: every month the test handle holds (the
    # configured test months) read as one population, the same rows and the
    # same strata as before test was cached a month per directory. A
    # diagnosis per month is a separate question (issue #128, out of scope).
    path = handle_paths(test_parquet_handle)

    # Decision — a model that cannot attribute at all skips this diagnosis with
    # a warning and says so in the artifact. Asked twice, here and at the
    # first attribution below: cost is the cheap question, the first real
    # attribution the one that settles it.
    try:
        n_trees = model.attribution_cost()
    except UnsupportedCapability as exc:
        logger.warning("shap diagnostics: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc), {}
    # Decision — the budget guard: an attribution pass costs about rows ×
    # attribution_cost (the trees), so when sample_rows × that exceeds
    # max_budget the sample shrinks to fit — never below min_rows_per_item —
    # and a warning says by how much. Without it the cost of this node grows
    # with the size of the model, and nothing bounds it.
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d -> reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    # Decision — the sample is population-representative, stratified by
    # item: each item gets max(min_rows_per_item, the sample size // the
    # number of items) rows drawn at random under a fixed seed, all of its
    # rows when it has fewer. Only the item column is read to draw it; the
    # test split is never materialized.
    item_values = bounded_reads.read_column(path, item_col)
    idx = stratified_item_sample(item_values, eff_sample, min_per_item, seed=42)
    if len(idx) == 0:
        logger.warning("shap diagnostics: empty sample after stratification; skipping")
        return {}, {}

    # Only the drawn rows × (the feature columns + the item and label columns).
    # In production the item column is usually a categorical feature already
    # in feature_cols, but a diagnosis fixture or cache layout need not be;
    # the per-item grouping below reads sample_pdf[item_col], so item and
    # label are put in take_cols explicitly.
    names = bounded_reads.schema_names(path)
    take_cols = list(feature_cols)
    for col in (item_col, label_col):
        if col in names and col not in take_cols:
            take_cols.append(col)
    sample_pdf = bounded_reads.take_rows(path, idx, columns=take_cols).reset_index(drop=True)
    logger.info("shap diagnostics: n_total=%d n_sampled=%d n_cols=%d",
                len(item_values), len(sample_pdf), len(take_cols))
    log_data_volume(logger, "shap.sample_pdf", sample_pdf, deep=True)

    X = pdf_to_X(sample_pdf, model_view, parameters)
    scores = model.predict(X)

    try:
        with log_step(logger, "shap_values"):
            shap_values = model.feature_attributions(X)
    except UnsupportedCapability as exc:
        logger.warning("shap diagnostics: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc), {}
    items = sample_pdf[item_col].values

    # Decision — per_item attributes each item's rows against that item's own
    # rows as background (its sampled rows, capped). A model that cannot
    # attribute against a background — for any item — degrades the whole
    # option to global with a note, and only that exception does; the global
    # attributions above stay. Every item is attributed here, before anything
    # is built from them, so an item that fails half way cannot leave a half
    # per_item result. Today every LightGBM model degrades: shap 0.42.1
    # cannot build an interventional explainer over the categorical splits
    # every model here has on the item (the adapter's feature_attributions
    # docstring has the evidence). Review fix 2026-07-08.
    requested_background = background_mode
    degrade_note = None
    per_item_values = {}
    if background_mode == "per_item":
        try:
            for item in pd.unique(items):
                X_item = X[items == item]
                bg = per_item_background(X_item, seed=42)
                with log_step(logger, "shap_values_per_item"):
                    per_item_values[item] = model.feature_attributions(
                        X_item, background=bg)
        except UnsupportedCapability as exc:
            background_mode = "global"
            per_item_values = {}
            # The library's own error type is the informative one, as before
            # the adapter wrapped it.
            cause = exc.__cause__ or exc
            degrade_note = (
                "per_item 背景已降級為 global：interventional TreeSHAP 在目前"
                f"版本組合下無法解析類別切分（{type(cause).__name__}）。"
                "條件化背景不可行，見手冊已知限制。"
            )
            logger.warning("shap background=per_item 不可行，降級 global：%s", exc)

    global_top, mean_abs = signed_profile(shap_values, feature_cols, top_k)

    # Decision — positive profiles come from a sample of their own: at most
    # positive_sample_per_item label == 1 rows per item, drawn apart from the
    # stratified sample and attributed in a second pass, so an item whose
    # positives are rare still gets enough of them to profile. An item with
    # fewer than positive_min_rows gets no profile and is flagged low
    # coverage. Skipped when profile_positive is off or the test data has no
    # label column, and under the per_item background, which takes each
    # item's label == 1 rows out of its own attributions instead (below). An
    # UnsupportedCapability out of this pass is not caught: the model has
    # just attributed the main sample the same way, so it would mean an
    # adapter that can and cannot at once — a bug, which stops the run.
    positive_profiles = {}
    if (background_mode != "per_item" and profile_positive
            and label_col in bounded_reads.schema_names(path)):
        all_labels = bounded_reads.read_column(path, label_col)
        pos_idx = positive_item_sample(
            item_values, all_labels, positive_sample_per_item, seed=42)
        if len(pos_idx) > 0:
            pos_pdf = bounded_reads.take_rows(
                path, pos_idx, columns=take_cols).reset_index(drop=True)
            log_data_volume(logger, "shap.positive_sample_pdf", pos_pdf, deep=True)
            X_pos = pdf_to_X(pos_pdf, model_view, parameters)
            with log_step(logger, "shap_values_positive"):
                shap_pos = model.feature_attributions(X_pos)
            pos_items = pos_pdf[item_col].values
            for item in pd.unique(pos_items):
                m = pos_items == item
                n = int(m.sum())
                if n >= positive_min_rows:
                    prof, _ = signed_profile(shap_pos[m], feature_cols, top_k)
                    positive_profiles[str(item)] = (prof, n, False)
                else:
                    positive_profiles[str(item)] = (None, n, True)

    label_present = background_mode == "per_item" and label_col in sample_pdf.columns
    labels = sample_pdf[label_col].values if label_present else None
    per_item = {}
    for item in pd.unique(items):
        mask = items == item
        # Decision — an item's profile is over its own rows: their per-item
        # background attributions under per_item, their slice of the global
        # attributions otherwise. The global vector its divergence is measured
        # against stays the global-background one either way, so under
        # per_item the divergence mixes in the change of background (the
        # note below says so; the handbook's §12 has how to read it).
        if background_mode == "per_item":
            sv_item = per_item_values[item]
            prof_all, ai = signed_profile(sv_item, feature_cols, top_k)
        else:
            sv_item = None
            prof_all, ai = signed_profile(shap_values[mask], feature_cols, top_k)
        sc = scores[mask]
        div, idio = divergence(ai, mean_abs, divergence_metric, divergence_top_k, feature_cols)
        # Decision — under per_item the positive profile is the item's
        # label == 1 rows of its own per-item attributions: no extra draw, so
        # its coverage is whatever the foreground sample holds, not the
        # targeted oversampling above.
        if background_mode == "per_item" and profile_positive and label_present:
            pos_mask = labels[mask] == 1
            n_pos = int(pos_mask.sum())
            if n_pos >= positive_min_rows:
                prof_pos, _ = signed_profile(sv_item[pos_mask], feature_cols, top_k)
                pos_low = False
            else:
                prof_pos, pos_low = None, True
        else:
            prof_pos, n_pos, pos_low = positive_profiles.get(
                str(item), (None, 0, bool(profile_positive)))
        # Decision — low_coverage flags an item with fewer sampled rows than
        # min_rows_per_item: it was taken whole and is still small, so its
        # profile rests on few rows. Flagged, not dropped.
        per_item[str(item)] = {
            "top_features": prof_all,
            "n_sampled": int(mask.sum()),
            "n_positive": n_pos,
            "score_min": float(sc.min()), "score_max": float(sc.max()),
            "score_mean": float(sc.mean()),
            "low_coverage": bool(mask.sum() < min_per_item),
            "top_features_positive": prof_pos,
            "positive_low_coverage": bool(pos_low),
            "divergence_from_global": to_native(div),
            "idiosyncratic_features": idio,
        }

    item_idiosyncrasy = sorted(
        ({"item": k,
          "divergence_from_global": v["divergence_from_global"],
          "idiosyncratic_features": v["idiosyncratic_features"]}
         for k, v in per_item.items()),
        key=lambda r: r["divergence_from_global"],
        reverse=True,
    )

    # Decision — the beeswarms plot the global-background attributions,
    # the per-item ones too (the item's rows of them), whichever background
    # was asked for. Drawn by the shap_summary_figures catalog entry when it
    # saves, one at a time; a figure that fails is its warning, not this
    # node's.
    figures = {"shap_summary_global.png": beeswarm(shap_values, X, feature_cols)}
    if cfg.get("per_item_beeswarm", True):
        for item in pd.unique(items):
            figures[f"per_item/shap_summary__{safe_name(item)}.png"] = beeswarm(
                shap_values, X, feature_cols, rows=items == item)

    logger.info("shap diagnostics: n_sample=%d n_trees=%d items=%d",
                len(idx), n_trees, len(per_item))
    out = {"global": {"top_features": global_top}, "per_item": per_item,
           "item_idiosyncrasy": item_idiosyncrasy}
    # Decision — notes appear only when per_item was asked for, including
    # when it degraded to global (the note then says so). A global run's
    # output has no notes key, as it never had.
    if requested_background == "per_item":
        out["notes"] = [degrade_note] if degrade_note else [
            "shap background=per_item（interventional，背景=各 item 子母體，上限 128 列）；"
            "divergence 的全域向量仍為 global 背景——占比混入背景效應，判讀見手冊 §12"
        ]
    return out, figures


def select_shap_population(
    training_eval_predictions, test_model_input, parameters, predict_manifest=None
):
    """Returns ``(shap_population, case_rows)``: who the quadrant diagnoses
    attribute.

    ``shap_population``: a deterministic sample of at most
    ``quadrant_sample_per_cell`` candidates per (item × quadrant), with their
    features, for ``compute_quadrant_profiles``. ``case_rows``: every
    (item × quadrant) cell's highest- and lowest-scored candidate
    (``role`` ``high`` / ``low``), carrying ``quadrant`` / ``role`` / ``rank``
    / ``score`` / ``label``, the identity columns and the features, for
    ``compute_quadrant_cases`` to chart one row at a time. Ranking, quadrants,
    sampling and the joins all run on Spark (the executors); the driver only
    collects the two small results.

    ``quadrant_enabled: false`` → ``(None, None)``. ``predict_manifest`` is an
    in-DAG ordering dependency only (the same convention as
    ``compute_test_metrics``): none of the three data inputs has a node
    producer, so without it the topological sort could run this before
    predict and read predictions not yet written.

    Reads ``dataset.test_snap_dates``' months only, from both tables, and ranks
    with ``utils.ranking.rank_by_score_then_item`` on ``schema``'s score column
    (ADR-0030 decisions 5 and 12). The ``score`` column this hands
    ``compute_quadrant_cases`` is a name the two nodes agree on, not the
    prediction table's column, so it stays ``score`` whatever ``schema`` calls
    that one.

    A failure stops the run: this node asks nothing of the model, so nothing
    here is "the model cannot" (ADR-0030 decision 4). The two ``raise`` in the
    body are a **runtime backstop** (no configured test month: A36 stops the
    training command before Spark starts) and a **post-condition** (the
    configured months matched no row: a population read as empty would land
    as ``{}``, the shape "switched off" lands, and the quadrants would go
    missing without a word).
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None, None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    # The rank window is a query group; the two joins back to ``test_model_input``
    # are at candidate grain, so they take identity (ADR-0025 decision 2).
    group_cols = schema["query_group_columns"]
    identity_cols = schema["identity_columns"]

    # Decision — which months: dataset.test_snap_dates, the ones this run
    # predicted and compute_shap_diagnostics describes (node rule 14). Both
    # tables keep every month ever written under their version, so an
    # unfiltered read grows with that history, not with this run. Normalised
    # the way the scored months are (core.date_ranges.as_date_list) and
    # compared as text, the rule compute_test_metrics reads the same
    # prediction table with (steps/scored_months.restrict_to_scored_months,
    # whose docstring says why).
    months = as_date_list((parameters.get("dataset") or {}).get("test_snap_dates") or [])
    # Runtime backstop — A36 rejects this config before Spark starts.
    if not months:
        raise ValueError(
            "select_shap_population: dataset.test_snap_dates is unset or empty, "
            "so there is no month to pick the quadrant population from.")
    training_eval_predictions = scored_months.restrict_to_scored_months(
        training_eval_predictions, schema["time"], months)
    test_model_input = scored_months.restrict_to_scored_months(
        test_model_input, schema["time"], months)

    labeled = None
    try:
        # Decision — rank with the rule evaluation ranks with (score, then
        # item, then each event column), so a declared event cannot make the
        # quadrants' top-1 differ from evaluation's.
        ranked = training_eval_predictions.withColumn(
            "_rank",
            rank_by_score_then_item(
                group_cols, score_col, item_col, schema.get("event", [])),
        )

        # Decision — a candidate's quadrant crosses "ranked within
        # quadrant_top_k_decision of its query group" with "label == 1": TP,
        # FP, FN, TN — the recommendation the model would make against what
        # happened.
        # The two results below are each collected once (two actions);
        # without the persist the rank's shuffle would run twice. The storage
        # level is spelled out rather than left to the default: at production
        # volume this intermediate may not fit in executor memory, and it must
        # spill to disk rather than be dropped and recomputed.
        labeled = label_quadrants(
            ranked, "_rank", label_col, top_k_decision, identity_cols,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        # Decision — the profile population: at most quadrant_sample_per_cell
        # candidates per (item × quadrant), chosen by a hash of their identity
        # (crc32) rather than at random or by score, so a rerun over the same
        # predictions picks the same rows. Only their keys go back to
        # test_model_input for the features.
        keyset = sample_each_cell(labeled, item_col, per_cell).select(
            *identity_cols, "quadrant")
        pop_pdf = keyset.join(
            test_model_input, on=identity_cols, how="inner").toPandas()

        # Decision — the cases: every cell's highest- and lowest-scored
        # candidate, over the whole cell rather than the sample. Ties on score
        # are broken in opposite directions for the two, so a tied cell still
        # gives two rows; only a one-row cell gives the same row as both.
        extremes = extremes_of_each_cell(
            labeled, item_col, "_rank", score_col, label_col, identity_cols)
        # test_model_input has a label column too; it is dropped so the join
        # is not ambiguous (the label is not a feature).
        feats_only = (test_model_input.drop(label_col)
                      if label_col in test_model_input.columns else test_model_input)
        case_pdf = extremes.join(
            feats_only, on=identity_cols, how="inner").toPandas()
    finally:
        # The Runner only releases MemoryDatasets and never touches a Spark
        # DataFrame's storage (neither core/runner.py nor core/catalog.py
        # unpersists). Without this, the cache holds the executors until the
        # SparkSession ends.
        # In a finally, not on the success path: a failure above leaves this
        # function too, on its way to stopping the run.
        if labeled is not None:
            try:
                labeled.unpersist()
            except Exception as release_error:
                # Logged, not raised. Raised from here it would replace the
                # exception the body is propagating (the one that says what
                # went wrong); on the success path the results are already in
                # the driver, and the leaked cache is the whole cost.
                logger.warning(
                    "select_shap_population: unpersist failed: %s", release_error)

    # Post-condition — every configured month was predicted
    # (compute_test_metrics checks that first), so an empty population means
    # the month filter or the join back to test_model_input matched nothing:
    # a spelling that differs between the config and a table, say. Landed as
    # {} it would read as "switched off".
    if len(pop_pdf) == 0:
        raise ValueError(
            f"select_shap_population: no prediction for months {months} joined "
            f"back to test_model_input. Check that {schema['time']} is spelled "
            "in both tables as dataset.test_snap_dates spells it.")
    logger.info(
        "select_shap_population: pop_rows=%d case_rows=%d items=%d per_cell=%d",
        len(pop_pdf), len(case_pdf), pop_pdf[item_col].nunique(), per_cell,
    )
    return pop_pdf, case_pdf


def compute_quadrant_profiles(model, shap_population, preprocessor: dict, parameters: dict) -> dict:
    """The mean signed SHAP profile of every (item × quadrant) cell of the
    population ``select_shap_population`` drew, from one attribution pass.

    Returns ``{"<item>": {"<quadrant>": {"top_features": […], "n_sampled": int,
    "low_coverage": bool}}}``. ``shap_population`` is
    ``select_shap_population``'s small pandas frame (features + item +
    quadrant). ``None``, empty, or ``quadrant_enabled: false`` → ``{}``.

    A model that cannot attribute lands the "model cannot" shape; any other
    failure stops the run (ADR-0030 decision 4).
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if shap_population is None or len(shap_population) == 0:
        logger.warning("quadrant profiles: empty population; skipping")
        return {}

    top_k = int(cfg.get("top_k", 30))
    quadrant_min_rows = int(cfg.get("quadrant_min_rows", 10))
    item_col = get_schema(parameters)["item"]
    # Decision — which features, and in what order: ask the model, not
    # apply_feature_selection(preprocessor, parameters). This is not a drift fix:
    # the exclude list lives in the `training:` block, so editing it bumps
    # model_version, the model's catalog path moves with it, and the whole
    # training chain is pulled back — ADR-0014 decision 7 is explicit that the
    # version mechanism already blocks that, and that this is interface work, not
    # a bug fix. What it buys is addressability: model and preprocessor both have
    # catalog entries, while the config-derived view is memory-only and drags
    # select_features into every diagnosis-only slice.
    model_view = model_feature_view(model, preprocessor)
    feature_cols = list(model_view["feature_columns"])

    pdf = shap_population.reset_index(drop=True)
    X = pdf_to_X(pdf, model_view, parameters)
    log_data_volume(logger, "quadrant.X", X)
    # Decision — a model that cannot attribute skips this with a warning and
    # says so; nothing else is caught (ADR-0030 decision 4).
    try:
        shap_values = model.feature_attributions(X)
    except UnsupportedCapability as exc:
        logger.warning("quadrant profiles: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc)
    # Decision — one profile per cell that has rows; a cell with none is left
    # out rather than listed empty (compute_quadrant_cases lists it).
    # low_coverage flags a cell with fewer than quadrant_min_rows rows: its
    # profile is kept, but it rests on few rows.
    items = pdf[item_col].values
    quads = pdf["quadrant"].values
    out: dict = {}
    for item in pd.unique(items):
        for q in QUADRANTS:
            mask = (items == item) & (quads == q)
            n = int(mask.sum())
            if n == 0:
                continue
            prof, _ = signed_profile(shap_values[mask], feature_cols, top_k)
            out.setdefault(str(item), {})[q] = {
                "top_features": prof,
                "n_sampled": n,
                "low_coverage": bool(n < quadrant_min_rows),
            }
    logger.info("quadrant profiles: items=%d", len(out))
    return out


def compute_quadrant_cases(
    model, case_rows, preprocessor: dict, parameters: dict,
) -> tuple[dict, dict]:
    """Each (item × quadrant) cell's extreme cases as one-row signed SHAP bar
    charts, plus a manifest that accounts for every cell.

    ``case_rows`` is ``select_shap_population``'s second output (a ``high``
    and a ``low`` row per item × quadrant); one attribution pass over those
    few dozen rows. An empty cell records ``reason: empty``; a one-row cell's
    low records ``reason: single_row_same_as_high`` (no duplicate file).

    Returns ``(cases_manifest, case_figures)``: the manifest
    ``{"<item>": {"<quadrant>": {"high"/"low": {rendered, png|reason, <identity
    columns but the item>, rank, score, label}}}}``, and ``{path under
    diagnostics/cases/: draw}`` for the charts. ``({}, {})`` for no case rows
    or ``quadrant_enabled: false``.

    ``rendered: True`` with a ``png`` says a chart for that case was handed to
    the catalog. The catalog draws it when it saves, so a chart that then
    fails to draw is a warning and a missing file (ADR-0030 decisions 4 and
    7); the manifest, written before any chart is drawn, cannot say
    ``render_failed`` any more. A model that cannot attribute → the "model
    cannot" shape and no charts; any other failure stops the run.
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}, {}
    if case_rows is None or len(case_rows) == 0:
        logger.warning("quadrant cases: empty case_rows; skipping")
        return {}, {}

    case_top_k = int(cfg.get("case_top_k", 15))
    schema = get_schema(parameters)
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    # Decision — a case's manifest label says which row its chart is of, so
    # it is the identity; the item is dropped only because it is already the
    # manifest's outer key, and writing it again says nothing new.
    #
    # Deliberately not the base key: the base key does not widen with
    # occasion (ADR-0025 decision 2), which would give two rows of the same
    # entity, the same period and different occasions the very same label —
    # two different inputs mapped to one result. This subtracts one column
    # from the identity, so it widens when the identity does. Today the two
    # spellings agree value for value, so the manifest's content did not
    # change when this was introduced.
    case_label_cols = [c for c in identity_cols if c != item_col]
    # Decision — which features, and in what order: ask the model, not
    # apply_feature_selection(preprocessor, parameters). This is not a drift fix:
    # the exclude list lives in the `training:` block, so editing it bumps
    # model_version, the model's catalog path moves with it, and the whole
    # training chain is pulled back — ADR-0014 decision 7 is explicit that the
    # version mechanism already blocks that, and that this is interface work, not
    # a bug fix. What it buys is addressability: model and preprocessor both have
    # catalog entries, while the config-derived view is memory-only and drags
    # select_features into every diagnosis-only slice.
    model_view = model_feature_view(model, preprocessor)
    feature_cols = list(model_view["feature_columns"])

    pdf = case_rows.reset_index(drop=True)
    X = pdf_to_X(pdf, model_view, parameters)
    log_data_volume(logger, "cases.X", X)
    # Decision — a model that cannot attribute skips this with a warning and
    # says so; nothing else is caught (ADR-0030 decision 4).
    try:
        shap_values = model.feature_attributions(X)
    except UnsupportedCapability as exc:
        logger.warning("quadrant cases: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc), {}

    items = pdf[item_col].values
    quads = pdf["quadrant"].values
    roles = pdf["role"].values

    manifest: dict = {}
    figures: dict = {}
    for item in pd.unique(items):
        item_entry: dict = {}
        for q in QUADRANTS:
            idx = np.where((items == item) & (quads == q))[0]
            # Decision — every cell is accounted for: one with no row records
            # reason "empty" for both roles instead of being left out, so a
            # reader can tell an empty quadrant from a missing one.
            if len(idx) == 0:
                item_entry[q] = {
                    "high": {"rendered": False, "reason": "empty"},
                    "low": {"rendered": False, "reason": "empty"}}
                continue
            by_role = {roles[i]: i for i in idx}
            hi, lo = by_role.get("high"), by_role.get("low")
            cell: dict = {}
            # high — a non-empty cell normally has one; a degenerate input
            # holding only a low is recorded as "empty" rather than raised on.
            if hi is None:
                cell["high"] = {"rendered": False, "reason": "empty"}
            else:
                figure_path, cell["high"], draw = case_chart(
                    pdf.iloc[hi], shap_values[hi], item, q, "high",
                    feature_cols, case_top_k, case_label_cols)
                figures[figure_path] = draw
            # Decision — low: in a one-row cell the low is the high's row, so
            # it records single_row_same_as_high and is not charted twice; a
            # degenerate input holding only a high records "empty".
            if lo is None:
                cell["low"] = {"rendered": False, "reason": "empty"}
            elif hi is not None and (row_identity(pdf.iloc[hi], identity_cols)
                                     == row_identity(pdf.iloc[lo], identity_cols)):
                cell["low"] = {"rendered": False,
                               "reason": "single_row_same_as_high"}
            else:
                figure_path, cell["low"], draw = case_chart(
                    pdf.iloc[lo], shap_values[lo], item, q, "low",
                    feature_cols, case_top_k, case_label_cols)
                figures[figure_path] = draw
            item_entry[q] = cell
        manifest[str(item)] = item_entry

    logger.info("quadrant cases: items=%d charts=%d", len(manifest), len(figures))
    return manifest, figures

"""PySpark implementations for the inference pipeline."""

import itertools
import logging

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY
from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.pipelines.inference.chunk_plans import (
    ScoringChunk,
    plan_scoring_chunks,
)
from recsys_tfb.pipelines.inference.validation import (
    BATCH_CHECKS,
    ValidationError,
    validate_scored_chunk,
)
from recsys_tfb.preprocessing import (
    _cast_feature_floats_to_float32,
    _encode_categoricals,
    encodable_categoricals,
    warn_unknown_encodings,
)
from recsys_tfb.utils.hashing import spark_bucket

logger = logging.getLogger(__name__)

#: Partition column carrying the entity chunk boundary. A mechanism column, not
#: part of any published contract: it exists so one ``save()`` touches exactly
#: one partition (ADR-0010 section 3, constraint C), and ``rank_predictions``
#: drops it on the way to ``ranked_staging``.
ENTITY_BUCKET_COL = "entity_bucket"

#: Salt for the entity bucket hash. Fixed rather than configurable on purpose:
#: the bucket assignment only has to be *stable*, and a knob here would let
#: someone reshuffle every entity between runs, orphaning every partition
#: already written for this ``model_version``.
_BUCKET_SITE = "inference_entity_bucket"
_BUCKET_SEED = 0

#: Hive's literal for a NULL partition value. Same guard as the dataset and
#: training month planners: the parquet side reconstructs it as ``None`` ->
#: ``"None"``, so the two spellings would never match and the chunk would look
#: permanently unwritten. Dropping it means "not written yet", which re-scores
#: rather than skips.
_HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def _iso_snap_dates(parameters: dict) -> list[str]:
    """``inference.snap_dates`` as ``YYYY-MM-DD`` strings.

    The single place that decides what the config's date values mean — see
    :func:`_snap_dates_as_dates` for the other shape, which derives from this
    one. The string form is what gets compared against Hive partition directory
    names, so a ``datetime`` reaching that comparison would never match a
    directory and every chunk would look unwritten.
    """
    return [
        pd.Timestamp(value).date().isoformat()
        for value in (parameters.get("inference", {}) or {}).get("snap_dates", [])
    ]


def _snap_dates_as_dates(parameters: dict) -> list:
    """The same months as ``datetime.date``, for comparing against a date column.

    Two consumers want two shapes and both are load-bearing: a Hive partition
    directory name is a string, while ``isin`` against a cast-to-date column
    wants date objects. Derived from :func:`_iso_snap_dates` rather than parsed
    again, so there is still exactly one place that decides what the config's
    date values mean.
    """
    return [pd.Timestamp(value).date() for value in _iso_snap_dates(parameters)]


def _entity_buckets(parameters: dict) -> int:
    return int(
        (parameters.get("inference", {}) or {}).get("entity_buckets", 10)
    )


def restrict_to_snap_dates(df: DataFrame, parameters: dict) -> DataFrame:
    """Cut a persisted inference table down to the months this run scores.

    ``unranked_predictions`` and ``ranked_staging`` accumulate across months
    while ``inference.snap_dates`` names one run's worth, so a node that reads
    either one back has to say which months it means. Without this cut the
    second month reads back every historical month, re-ranks it, and republishes
    it — silently, because a re-publish of an unchanged month looks exactly like
    a correct one (ADR-0010 section 5).

    The model-version half of the old ``_filter_current_inference_scope`` is
    **gone, not moved**: ``partition_filter: model_version`` makes the catalog's
    load emit ``WHERE model_version = '…'`` and drop the column, so a comparison
    here would be dead code against a column the frame no longer has.

    Both failure modes raise rather than pass the frame through. An empty scope
    that quietly means "keep everything" is the exact behaviour this exists to
    prevent, and it would show up downstream as a republished month rather than
    as an error.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]

    snap_dates = _snap_dates_as_dates(parameters)
    if not snap_dates:
        raise ValueError(
            "inference.snap_dates is empty; refusing to rank or validate an "
            "unrestricted table (every historical month would be republished)"
        )
    if time_col not in df.columns:
        raise ValueError(
            f"cannot restrict to inference.snap_dates: no {time_col!r} column "
            f"in {sorted(df.columns)}"
        )

    return df.filter(F.col(time_col).cast("date").isin(snap_dates))


def build_inference_population_features(
    inference_population: DataFrame,
    feature_table: DataFrame,
    preprocessor: dict,
    parameters: dict,
) -> DataFrame:
    """The scoring population's features at ``(time, entity)`` grain — no item.

    One node where there used to be two (``build_scoring_dataset`` then
    ``apply_preprocessor``). The intermediate between them landed nowhere and
    nobody observed it: no catalog entry, no test read it, no log named it. A
    DAG node whose only consumer is the next box adds topology without adding
    information (ADR-0010 section 4).

    **Deliberately not exploded by item.** The feature vector for
    ``(entity, item)`` is the entity's features plus one categorical scalar, so
    an explosion copies each entity's row once per item and those copies carry
    no information. Landing the un-exploded frame takes the source feature
    table's full scans from one-per-item per month down to one
    (ADR-0010 section 4's cost table: 22 FT -> 3 FT).

    **The stored columns are the preprocessor's full feature set minus the
    item**, never the subset a particular model wants. The subset is sliced per
    chunk from ``model.feature_names()`` inside :func:`predict_and_write_scores`.
    That is the whole reason this table is scoped by ``base_dataset_version``
    and can be reused across ``model_version`` — "optimising" it down to one
    model's columns would silently bind it to that model, and nothing would go
    red (ADR-0010 section 5).

    Membership comes from ``inference_population``; ``feature_table`` is
    enrichment only. Members with no features are kept, not dropped — losing an
    entity here would silently shrink the ranked output — and the per-month
    count of them is logged as the durable record of it.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    join_key = [time_col] + entity_cols

    n_buckets = _entity_buckets(parameters)
    snap_dates = _snap_dates_as_dates(parameters)

    categorical_cols = preprocessor["categorical_columns"]
    category_mappings = preprocessor["category_mappings"]
    drop_cols = preprocessor["drop_columns"]
    # The full set minus the item: the item has no column here because this
    # frame is not exploded by it. It reappears in the driver, per chunk.
    feature_columns = [c for c in preprocessor["feature_columns"] if c != item_col]
    keep_identity = [c for c in identity_cols if c != item_col]

    with log_step(logger, "read_population"):
        customers = (
            inference_population
            .filter(F.col(time_col).cast("date").isin(snap_dates))
            .select(*join_key)
        )
        available_dates = {
            pd.Timestamp(row[time_col]).date()
            for row in customers.select(time_col).distinct().collect()
            if row[time_col] is not None
        }
        missing_dates = sorted(set(snap_dates) - available_dates)
        if missing_dates:
            raise ValueError(
                "inference_population missing inference.snap_dates: "
                f"{[value.isoformat() for value in missing_dates]}"
            )

    with log_step(logger, "feature_coverage_report"):
        # 用窄投影（join_key + 指標）算覆蓋，避免在 wide feature 上做聚合
        ft_keys = (
            feature_table.select(*join_key).distinct()
            .withColumn("_ft_present", F.lit(True))
        )
        presence = customers.join(ft_keys, on=join_key, how="left")
        coverage = (
            presence.groupBy(time_col)
            .agg(
                F.count(F.lit(1)).alias("members"),
                F.sum(
                    F.when(F.col("_ft_present").isNull(), F.lit(1)).otherwise(F.lit(0))
                ).alias("members_missing_features"),
            )
            .collect()
        )
        for row in coverage:
            logger.info(
                "feature coverage %s=%s: members=%d missing_features=%d",
                time_col, row[time_col], row["members"],
                row["members_missing_features"],
            )

    with log_step(logger, "merge_features"):
        result = customers.join(feature_table, on=join_key, how="left")

    with log_step(logger, "assign_entity_buckets"):
        # Hashing the entity columns only, never the time: the ranking group is
        # (time, entity) and the bucket has to be a subset of it, or one
        # entity's items would land in different buckets and the group would be
        # split across partitions that are scored independently.
        result = result.withColumn(
            ENTITY_BUCKET_COL,
            spark_bucket(
                result, entity_cols, _BUCKET_SEED, _BUCKET_SITE, n_buckets,
            ).cast("string"),
        )

    with log_step(logger, "drop_non_feature_columns"):
        cols_to_drop = [
            c for c in drop_cols
            if c in result.columns and c not in identity_cols
        ]
        result = result.drop(*cols_to_drop)

    # Identity categoricals are left alone here, and the item is not even
    # present: encoding an identity column in Spark ships the integer code into
    # a partition directory name (`prod_name=0` instead of
    # `prod_name=exchange_fx`) with every sanity check still green
    # (ADR-0010 section 6). The driver encodes a copy instead, per chunk.
    #
    # This is also where the unknown-category warning got 22x cheaper: it
    # aggregates over the un-exploded frame now, which is the same set of values
    # it was already reporting.
    with log_step(logger, "encode_categoricals"):
        encode_cols = encodable_categoricals(
            categorical_cols, result.columns, identity_cols,
        )
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            warn_unknown_encodings(
                result, encode_cols,
                context="build_inference_population_features",
            )

    with log_step(logger, "select_feature_columns"):
        missing = sorted(set(feature_columns) - set(result.columns))
        if missing:
            raise ValueError(
                f"Missing feature columns in inference population: {missing}"
            )
        result = result.select(
            *keep_identity, *feature_columns, ENTITY_BUCKET_COL,
        )

    with log_step(logger, "cast_features_to_float32"):
        result, casted = _cast_feature_floats_to_float32(result, feature_columns)

    with log_step(logger, "cast_time_to_partition_string"):
        # The partition column is STRING, and letting Spark coerce a date on the
        # way into insertInto makes the directory name depend on that coercion.
        # Spelling it here is what ties the directory to the same
        # `YYYY-MM-DD` string the resume planner compares against.
        result = result.withColumn(
            time_col, F.col(time_col).cast("date").cast("string")
        )

    logger.info(
        "build_inference_population_features: %d columns "
        "(%d features, item excluded), %d float-like cast to float32, "
        "%d entity buckets",
        len(result.columns), len(feature_columns), len(casted), n_buckets,
    )
    if casted:
        logger.debug(
            "build_inference_population_features: casted columns = %s", casted
        )
    return result


def require_ordered_subsequence(
    model_features: list[str],
    artifact_features: list[str],
) -> None:
    """Raise unless ``model_features`` is an order-preserving subsequence.

    The model is the authority on *which* features and in *what order*; the
    preprocessor artifact is the authority on how they are *encoded*
    (ADR-0011 §5). Only the model records which view of the full feature set a
    given training run actually used — ``preprocessor.json`` is the full set and
    cannot tell whether ``training.feature_selection.exclude`` was in play.

    So the artifact is allowed to hold *more* columns, in the same relative
    order, and nothing else. A stale artifact (model has a column the artifact
    lacks) and a mismatched model (same columns, permuted) both raise. No
    automatic realignment: an intersection here would silently slice X by the
    full set and hand it to a booster that expects a subset.
    """
    remaining = iter(artifact_features)
    for name in model_features:
        if name not in remaining:
            raise ValueError(
                "model.feature_names() is not an order-preserving subsequence of "
                f"the preprocessor's feature_columns: stuck at {name!r}. "
                f"model={list(model_features)} artifact={list(artifact_features)}"
            )


def _model_feature_columns(model: ModelAdapter, preprocessor: dict) -> list[str]:
    """Which feature columns to slice, and in what order — the model decides.

    The preprocessor artifact holds the full set and cannot tell whether
    ``training.feature_selection.exclude`` was in play; the model is the only
    artifact that records which view a given training run actually used
    (ADR-0011 section 5).
    """
    artifact_feature_columns = list(preprocessor["feature_columns"])
    feature_names_fn = getattr(model, "feature_names", None)
    model_feature_names = (
        feature_names_fn() if callable(feature_names_fn) else None
    )
    if model_feature_names:
        feature_columns = list(model_feature_names)
        require_ordered_subsequence(feature_columns, artifact_feature_columns)
        return feature_columns
    # No declaration, so there is no second opinion to check the artifact
    # against and the assertion is skipped rather than run against itself.
    # Every adapter in this repo declares one once it holds a fitted model
    # (LightGBMAdapter returns None only before load(); CalibratedModelAdapter
    # forwards to its base), so this branch is doubles in tests, not
    # production — and it is logged rather than silent for that reason.
    logger.info(
        "Model declares no feature_names(); falling back to the "
        "preprocessor's %d feature columns", len(artifact_feature_columns),
    )
    return artifact_feature_columns


def _written_score_partitions(
    predictions_dataset, time_col: str, item_col: str
) -> set[ScoringChunk]:
    """Chunks already scored for this ``model_version``, from the metastore.

    The dataset object reaches this node through ``Node(writes=...)``, and it is
    the only route to the metastore here — this node gets no SparkSession of its
    own. Its ``partition_filter`` scopes the answer to this ``model_version``,
    which is exactly the scope the resume question is asked in; without that
    scoping a previous model's partitions would be counted as this run's work
    already done, and ``completeness`` would pass on the old scores
    (ADR-0010 section 5).

    A dataset type that cannot list partitions makes every chunk look unwritten:
    that re-scores (wasteful) rather than skips (silently stale), which is the
    direction this has to fail in.
    """
    lister = getattr(predictions_dataset, "existing_partition_values", None)
    if lister is None:
        logger.warning(
            "[chunks] predict: %s cannot list partitions, so no chunk can be "
            "shown complete; every configured chunk will be scored.",
            type(predictions_dataset).__name__,
        )
        return set()

    written: set[ScoringChunk] = set()
    for spec in lister():
        snap_date = spec.get(time_col)
        item = spec.get(item_col)
        bucket = spec.get(ENTITY_BUCKET_COL)
        if snap_date is None or item is None or bucket is None:
            continue
        if _HIVE_NULL_PARTITION in (snap_date, item, bucket):
            logger.warning(
                "[chunks] predict: ignoring score partition with a NULL value "
                "(%s=%r, %s=%r, %s=%r); that chunk will be treated as not yet "
                "written.",
                time_col, snap_date, item_col, item, ENTITY_BUCKET_COL, bucket,
            )
            continue
        try:
            bucket_index = int(bucket)
        except (TypeError, ValueError):
            logger.warning(
                "[chunks] predict: ignoring score partition with a "
                "non-numeric %s=%r; that chunk will be treated as not yet "
                "written.", ENTITY_BUCKET_COL, bucket,
            )
            continue
        written.add(ScoringChunk(str(snap_date), bucket_index, str(item)))
    return written


def _populated_buckets(
    features: DataFrame, time_col: str, snap_dates: list[str]
) -> dict[str, set[int]]:
    """Which entity buckets the landed table actually holds rows for, per month.

    Exists to separate two things a zero-row chunk cannot tell apart:

    * a bucket that is **legitimately** empty — a population smaller than the
      bucket count leaves gaps, and no partition is written for it, so it must
      not be counted as a missing partition; and
    * a bucket that **should** have had entities. That one is the silent-data-
      loss failure mode, and it has to raise.

    A partition exists for a bucket exactly when the builder wrote at least one
    row for it, so the table's partition values *are* the answer. One action per
    month over a partition column, so the cost scales with the number of
    partitions rather than the number of rows — and a fully resumed run passes
    an empty month list and pays nothing.

    What this does **not** catch: an ``inference_population_features`` that is
    itself stale (built against an older population, reachable via
    ``--from-node predict_and_write_scores``). Then the missing entities have no
    partition either and both sides agree. That residual is the documented one —
    resume's judgement is "the partition exists", never "the partition is
    fresh"; ``--rebuild-dates`` is the override.
    """
    populated: dict[str, set[int]] = {}
    for snap_date in snap_dates:
        rows = (
            features
            .filter(F.col(time_col) == snap_date)
            .select(ENTITY_BUCKET_COL)
            .distinct()
            .collect()
        )
        populated[snap_date] = {int(row[ENTITY_BUCKET_COL]) for row in rows}
    return populated


def _require_single_partition(pdf: pd.DataFrame, partition_cols: list[str]) -> None:
    """One ``save()``, one partition — constraint C in its testable form.

    ``HiveTableDataset.save()`` is ``insertInto`` under
    ``partitionOverwriteMode=dynamic``, whose semantics are "touch only the
    partitions present in this frame, but *replace* those wholesale". Hand it a
    frame spanning two chunks' partitions and the second save deletes the first
    chunk's rows — 90% of the data gone with no error message
    (ADR-0010 section 3, constraint C).

    Free to check here and nowhere else: the frame is a pandas frame that was
    just built in the driver, so counting its distinct partition values touches
    memory rather than launching a Spark job. The same assertion inside
    ``save()`` would have to act on a lazy plan, which is the second full
    lineage execution ADR-0009 removed.

    Deliberately **not** a before/after diff of
    ``existing_partition_values()``: re-publishing an existing partition makes
    that diff empty by construction, so it is blind to exactly the successive
    overwrite this guards (``io/hive_table_dataset.py`` records the same trap).
    """
    combos = pdf[partition_cols].drop_duplicates()
    if len(combos) != 1:
        raise ValueError(
            f"a single save must cover exactly one partition, got "
            f"{len(combos)} distinct {partition_cols} combination(s): "
            f"{combos.to_dict('records')}. Dynamic-partition overwrite "
            "replaces whole partitions, so successive saves would delete each "
            "other's rows without an error."
        )


def predict_and_write_scores(
    model: ModelAdapter,
    inference_population_features: DataFrame,
    preprocessor: dict,
    parameters: dict,
    unranked_predictions,  # HiveTableDataset, supplied via Node(writes=...)
) -> dict:
    """Score chunk by chunk, writing one partition per chunk as it goes.

    **Outer loop entity bucket, inner loop item.** One bucket's features cross
    into the driver once, and the inner loop overwrites the single item column
    in place to reuse them. Reversing the two loops is functionally identical
    and reads the whole population once per item — the one decision here whose
    mistake costs a factor of ``len(items)`` and breaks nothing
    (ADR-0010 section 4, decision 3).

    Each ``(bucket, item)`` lands the moment it is computed, as exactly one
    partition of ``unranked_predictions``. The previous shape accumulated every
    chunk's predictions in the driver and wrote once at the end: the loop
    existed, the by-chunk write did not.

    Chunks whose partition already exists are skipped, so a run that dies
    two-thirds of the way through costs the remaining third rather than
    everything. The manifest names what was processed, skipped, rebuilt and
    found empty: a node that decides to do less work has to say what it decided
    not to do, or a silently stale chunk is indistinguishable from a correctly
    skipped one.

    Returns:
        manifest dict. It is the DAG edge — the data itself travels through the
        catalog write, so the downstream nodes read it back from Hive. It also
        carries the partition bookkeeping ``validate_predictions`` needs to
        answer "is every chunk present" without scanning anything.
    """
    from recsys_tfb.io.extract import _pdf_to_X

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    score_col = schema["score"]

    items = list(parameters["inference"]["products"])
    snap_dates = _iso_snap_dates(parameters)
    n_buckets = _entity_buckets(parameters)
    partition_cols = [time_col, item_col, ENTITY_BUCKET_COL]

    feature_columns = _model_feature_columns(model, preprocessor)
    # The view _pdf_to_X slices by. Built from the model's declaration, never
    # from apply_feature_selection(preprocessor, parameters): that reads the
    # *current* config's training.feature_selection.exclude, and model_version
    # can point at a model trained under a different one. Same-length excludes
    # over different columns would then misalign X silently (ADR-0011 §5).
    preprocessor_view = {**preprocessor, "feature_columns": feature_columns}

    # The item is not a column of the landed table — it is assigned per inner
    # iteration — so it is excluded from what gets collected.
    keep_identity = [c for c in identity_cols if c != item_col]
    collection_columns = keep_identity + [
        c for c in feature_columns if c not in identity_cols
    ]
    missing_features = sorted(
        set(collection_columns) - set(inference_population_features.columns)
    )
    if missing_features:
        raise ValueError(
            "inference_population_features is missing columns required by the "
            f"model: {missing_features}"
        )

    with log_step(logger, "plan_chunks"):
        plan = plan_scoring_chunks(
            snap_dates,
            items,
            n_buckets,
            _written_score_partitions(unranked_predictions, time_col, item_col),
            parameters.get(REBUILD_SNAP_DATES_KEY) or [],
        )
    logger.info(
        "[chunks] predict: to_process=%d skipped=%d rebuilt=%d surplus=%d "
        "(%d month(s) x %d bucket(s) x %d item(s))",
        len(plan.to_process), len(plan.skipped), len(plan.rebuilt),
        len(plan.surplus), len(set(snap_dates)), n_buckets, len(items),
    )

    use_calibration = parameters.get("inference", {}).get("use_calibration", True)
    use_uncalibrated = not use_calibration and isinstance(model, CalibratedModelAdapter)
    if use_uncalibrated:
        logger.info("Calibration disabled by config, using uncalibrated scores")

    with log_step(logger, "list_populated_buckets"):
        populated = _populated_buckets(
            inference_population_features,
            time_col,
            sorted({chunk.snap_date for chunk in plan.to_process}),
        )

    n_rows_written = 0
    processed: list[ScoringChunk] = []
    empty: list[ScoringChunk] = []

    for (snap_date, bucket), group in itertools.groupby(
        plan.to_process, key=lambda chunk: (chunk.snap_date, chunk.entity_bucket)
    ):
        chunk_items = [chunk.item for chunk in group]
        with log_step(logger, f"read_bucket_{snap_date}_{bucket}"):
            # The one Spark action per bucket. Both partition columns are
            # pinned, so this prunes to a single partition directory rather
            # than scanning the table — and the source feature table is not
            # touched at all, which is audit 1 of ADR-0010.
            bucket_pdf = (
                inference_population_features
                .filter(
                    (F.col(time_col) == snap_date)
                    & (F.col(ENTITY_BUCKET_COL) == str(bucket))
                )
                .select(*collection_columns)
                .toPandas()
            )
        if bucket_pdf.empty:
            if bucket in populated.get(snap_date, set()):
                # The landed table HAS a partition for this bucket, so it holds
                # entities — and the read came back with none. That is the
                # failure this whole design exists to prevent, arriving through
                # a different door: the chunk writes nothing, no partition is
                # expected for it, and `completeness` cannot see the gap either
                # because absent entities form no query groups. Silence here
                # would mean publishing a population short by 1/n_buckets.
                raise ValueError(
                    f"{time_col}={snap_date} {ENTITY_BUCKET_COL}={bucket} has "
                    f"a partition in inference_population_features but the read "
                    f"returned no rows. Those entities would be silently "
                    f"dropped from the published ranking. Check the chunk "
                    f"filter against the table's partition values "
                    f"(populated buckets for this month: "
                    f"{sorted(populated[snap_date])})."
                )
            # No partition for this bucket, so it genuinely holds nobody: a
            # population smaller than the bucket count leaves gaps, and
            # insertInto writes no partition for an empty frame. Recorded rather
            # than passed over — `validate_predictions` compares the partitions
            # that exist against the ones that should, and "should" has to
            # exclude these or every small-population run fails.
            logger.info(
                "[chunks] predict: %s=%s %s=%s holds no entities (no partition "
                "in the landed table either, so the population is simply "
                "smaller than %d buckets); %d item partition(s) will not be "
                "written.",
                time_col, snap_date, ENTITY_BUCKET_COL, bucket, n_buckets,
                len(chunk_items),
            )
            empty.extend(
                ScoringChunk(snap_date, bucket, item) for item in chunk_items
            )
            continue

        for item in chunk_items:
            with log_step(logger, f"score_{snap_date}_{bucket}_{item}"):
                # In place, on the frame already in the driver: this is the
                # reuse the loop order buys. The value written is the raw item
                # name — _pdf_to_X applies the integer code to its own copy, so
                # the name is what reaches the partition column.
                bucket_pdf[item_col] = item
                X = _pdf_to_X(bucket_pdf, preprocessor_view, parameters)
                scores = (
                    model.predict_uncalibrated(X) if use_uncalibrated
                    else model.predict(X)
                )
                out_pdf = pd.DataFrame({
                    **{
                        col: bucket_pdf[col].astype(str).values
                        for col in entity_cols
                    },
                    score_col: scores,
                    time_col: snap_date,
                    item_col: item,
                    ENTITY_BUCKET_COL: str(bucket),
                })
                _require_single_partition(out_pdf, partition_cols)
                # The chunk half of validation, before the write rather than
                # after the whole table is ranked: nulls, duplicates, the row
                # count and the item value domain are all answerable from the
                # two frames already in the driver (ADR-0011 section 3). A bad
                # first chunk stops the run in minutes instead of surviving
                # until `validate_predictions` hours later.
                validate_scored_chunk(
                    out_pdf,
                    bucket_pdf,
                    identity_cols=identity_cols,
                    entity_cols=entity_cols,
                    item_col=item_col,
                    score_col=score_col,
                    known_items=items,
                )
                # No model_version column: `partition_filter` owns it. The
                # catalog's save injects it from the same
                # `parameters["model_version"]` this node would have read
                # (ADR-0010 section 5).
                unranked_predictions.save(out_pdf)
            n_rows_written += len(out_pdf)
            processed.append(ScoringChunk(snap_date, bucket, item))

    if not processed and not plan.skipped:
        raise ValueError(
            "No scoring rows found for inference.snap_dates and "
            "inference.products: every entity bucket came back empty. The "
            "population exists (read_population would have raised otherwise), "
            "so this points at the join to the feature table or at "
            "inference_population_features being stale."
        )

    # What should exist afterwards: what this run wrote, plus what it skipped
    # (a skipped chunk's partition is the evidence it was skipped). Empty
    # buckets are excluded — nothing was written for them.
    expected = set(processed) | set(plan.skipped)
    present = {
        chunk
        for chunk in _written_score_partitions(
            unranked_predictions, time_col, item_col
        )
        if chunk.snap_date in set(snap_dates)
    }

    manifest = {
        "snap_dates": sorted(set(snap_dates)),
        "items": sorted(set(items)),
        "entity_buckets": n_buckets,
        "model_version": parameters.get("model_version"),
        # Observability only. Deliberately NOT the basis of any completeness
        # check: it counts what this run wrote, so a resumed run reports a
        # fraction of the table and any comparison against the table's row
        # count fails every time (ADR-0011 section 3).
        "n_rows_written": n_rows_written,
        "chunks_processed": _as_rows(processed),
        "chunks_skipped": _as_rows(plan.skipped),
        "chunks_rebuilt": _as_rows(plan.rebuilt),
        "chunks_empty": _as_rows(empty),
        "expected_partitions": _as_rows(expected),
        "written_partitions": _as_rows(present),
    }
    logger.info(
        "predict_and_write_scores: done — processed=%d skipped=%d rebuilt=%d "
        "empty=%d n_rows_written=%d model_version=%s",
        len(processed), len(plan.skipped), len(plan.rebuilt), len(empty),
        n_rows_written, manifest["model_version"],
    )
    return manifest


def _as_rows(chunks) -> list[list]:
    """Chunks as sorted plain lists, so the manifest stays JSON-shaped."""
    return sorted(list(chunk) for chunk in chunks)


def rank_predictions(
    unranked_predictions: DataFrame,
    score_manifest: dict,
    parameters: dict,
) -> DataFrame:
    """Rank items by score within each query group.

    ``score_manifest`` is an ordering-only dependency (same convention as
    training's ``predict_manifest``): the scores travel through the catalog
    write inside :func:`predict_and_write_scores`, and ``writes=`` deliberately
    creates no topological edge, so this input is what forces this node to run
    after the writes have happened.
    """
    with log_step(logger, "restrict_to_snap_dates"):
        unranked_predictions = restrict_to_snap_dates(
            unranked_predictions, parameters
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    with log_step(logger, "drop_entity_bucket"):
        # The bucket stops here. It exists so one save touches one partition
        # (ADR-0010 section 3, constraint C) and it is not part of any published
        # contract, so carrying it into `ranked_staging` would leak a purely
        # computational mechanism into the shape downstream consumers query.
        #
        # Dropping it cannot change the ranking: the bucket is hashed from the
        # entity columns alone, and the ranking group is (time, entity), so the
        # bucket is a function of the group — every item of one entity is in the
        # same bucket, and no group is split across buckets.
        ranked = unranked_predictions.drop(ENTITY_BUCKET_COL)

    with log_step(logger, "rank_scores"):
        w = Window.partitionBy(*group_cols).orderBy(F.desc(score_col))
        ranked = ranked.withColumn(rank_col, F.row_number().over(w))

    logger.info("Ranked predictions by %s", group_cols)
    return ranked


def validate_predictions(
    ranked_predictions: DataFrame,
    score_manifest: dict,
    parameters: dict,
) -> DataFrame:
    """The batch layer of validation. Raises ValidationError on failure.

    Only the checks that a single chunk cannot answer live here — one chunk is
    one item, so anything comparing a query group's items against each other has
    to wait for the whole table (ADR-0011 section 3). Nulls, duplicates, the
    per-chunk row count and the item value domain are checked in the driver as
    each chunk is scored (:func:`validate_scored_chunk`), which is both free and
    hours earlier. ``validation.BATCH_CHECKS`` is the register of what belongs
    here.

    **Two Spark actions on the success path**, down from seven — eight counting
    the ``scoring_dataset`` re-scan ADR-0011 §3 measured, which #188 had already
    removed. One grouped aggregation answers ``completeness``,
    ``score_varies_within_group`` and the rank range together, and one windowed
    pass covers the score-versus-rank ordering. ``partition_completeness`` needs
    no action at all — it compares two lists the manifest already carries.

    The staging frame gets restricted because it comes back from Hive holding
    every month this model version has ever published, while the checks are
    about this run.
    """
    with log_step(logger, "restrict_to_snap_dates"):
        ranked_predictions = restrict_to_snap_dates(
            ranked_predictions, parameters
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    products = parameters["inference"]["products"]
    n_products = len(products)
    group_cols = [time_col] + entity_cols

    with log_step(logger, "run_sanity_checks"):
        failures = []

        # 1. partition_completeness — replaces the old row_count_match.
        #
        # That check compared `ranked_staging`'s row count against
        # `scoring_dataset`'s. Both halves are gone: the un-exploded
        # `inference_population_features` is |items| times shorter than the
        # ranked output, so the comparison would fail on every correct run, and
        # the frame it read cost a full re-run of the population-to-feature join
        # every time.
        #
        # Comparing the manifest's *row* count instead would fail on every
        # resume — it counts only what this run wrote, and a resumed run writes
        # a fraction (ADR-0011 section 3). So the comparison is over partition
        # sets: what the scoring node says should be there, against what the
        # metastore says is. Pure Python over two lists the manifest already
        # carries — zero scans — and it catches both directions: a partition
        # that a successive save deleted, and a stale one left behind by an
        # entity_buckets change that re-scoring can never remove.
        # Subscripted, not `.get(..., [])`: two absent keys would compare equal
        # and the check would pass vacuously. A manifest without them means the
        # wiring is wrong, and this is the one check whose silent version is
        # indistinguishable from a green one.
        expected_partitions = {
            tuple(row) for row in score_manifest["expected_partitions"]
        }
        written_partitions = {
            tuple(row) for row in score_manifest["written_partitions"]
        }
        if expected_partitions != written_partitions:
            missing = sorted(expected_partitions - written_partitions)
            stale = sorted(written_partitions - expected_partitions)
            failures.append({
                "check": "partition_completeness",
                "detail": (
                    f"{len(missing)} scored chunk(s) have no partition "
                    f"({missing[:10]}) and {len(stale)} partition(s) belong to "
                    f"no scored chunk ({stale[:10]}); "
                    f"expected {len(expected_partitions)}, "
                    f"found {len(written_partitions)}"
                ),
            })

        # One grouped pass, three checks. `completeness`, the rank range and
        # `score_varies_within_group` all want per-group facts, and the shuffle
        # behind `groupBy` is the expensive part — a min and a max added to an
        # aggregation that is already happening cost nothing (ADR-0011 §4).
        # The previous shape paid a separate action for each, every one of them
        # a fresh read of a Hive table.
        group_stats = ranked_predictions.groupBy(*group_cols).agg(
            F.count(F.lit(1)).alias("_size"),
            F.min(score_col).alias("_min_score"),
            F.max(score_col).alias("_max_score"),
            F.min(rank_col).alias("_min_rank"),
            F.max(rank_col).alias("_max_rank"),
        )
        # Rolled up to one row, so the whole grouped result stays in the
        # cluster: collecting it would pull one row per entity into the driver.
        # The rank bounds are min-of-mins and max-of-maxes, which is exactly the
        # global range the old separate aggregation produced. Deliberately *not*
        # upgraded to a per-group range while passing through: that weakness
        # predates this change and ADR-0011 records it as unsolved, so tightening
        # it here would land an unreviewed behaviour change inside a refactor.
        summary = group_stats.agg(
            F.coalesce(F.sum("_size"), F.lit(0)).alias("n_rows"),
            F.count(F.lit(1)).alias("n_groups"),
            F.coalesce(
                F.sum(F.when(F.col("_size") != n_products, 1).otherwise(0)),
                F.lit(0),
            ).alias("n_incomplete"),
            F.min("_size").alias("min_size"),
            F.max("_size").alias("max_size"),
            F.coalesce(
                F.sum(
                    F.when(
                        # A group of one cannot vary, and a config with a single
                        # item makes every group look constant; group size is
                        # `completeness`'s question, not this one's.
                        (F.col("_size") > 1)
                        & (F.col("_max_score") <= F.col("_min_score")),
                        1,
                    ).otherwise(0)
                ),
                F.lit(0),
            ).alias("n_constant"),
            F.min("_min_rank").alias("min_rank"),
            F.max("_max_rank").alias("max_rank"),
        ).collect()[0]
        n_ranked = summary["n_rows"]

        # 2. completeness
        if summary["n_incomplete"] > 0:
            failures.append({
                "check": "completeness",
                "detail": (
                    f"{summary['n_incomplete']} groups do not have exactly "
                    f"{n_products} products, sizes: min={summary['min_size']}, "
                    f"max={summary['max_size']}"
                ),
            })

        # 3. score_varies_within_group — the data-layer half of the backstop
        # `require_item_is_a_feature` only holds up at the config layer. If the
        # item value fed to the model degenerates to a constant, an entity's
        # items all score identically and the ranking is whatever order
        # `row_number` happened to pick. Every shape check stays green through
        # that: the group is complete, the ranks are 1..N, and the lag check's
        # `>` is false on a tie (ADR-0011 §1, example two).
        if summary["n_constant"] > 0:
            failures.append({
                "check": "score_varies_within_group",
                "detail": (
                    f"{summary['n_constant']} of {summary['n_groups']} groups "
                    f"score every product identically; ranking within them is "
                    f"arbitrary"
                ),
            })

        # 4. rank_consistency — ranks are 1..N and ordered by score desc
        if summary["min_rank"] != 1 or summary["max_rank"] != n_products:
            failures.append({
                "check": "rank_consistency",
                "detail": (
                    f"Rank range [{summary['min_rank']}, {summary['max_rank']}] "
                    f"expected [1, {n_products}]"
                ),
            })
        else:
            w = Window.partitionBy(*group_cols).orderBy(F.col(rank_col))
            with_prev = ranked_predictions.withColumn(
                "_prev_score", F.lag(score_col).over(w)
            )
            violations = with_prev.filter(
                F.col("_prev_score").isNotNull()
                & (F.col(score_col) > F.col("_prev_score"))
            ).count()
            if violations > 0:
                failures.append({
                    "check": "rank_consistency",
                    "detail": f"{violations} rows where score increases with rank",
                })

        if failures:
            logger.error("Validation failed: %s", failures)
            raise ValidationError(failures)

    logger.info(
        "All %d batch sanity checks passed (%d rows); the chunk-layer checks "
        "ran during scoring", len(BATCH_CHECKS), n_ranked,
    )
    return ranked_predictions


def publish_predictions(
    validated_predictions: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Promote validated predictions to the production ``ranked_predictions`` table.

    Reached only after ``validate_predictions`` passes (the DAG edge runs through
    ``validated_predictions``), so a failed sanity check aborts the run before
    anything reaches production. This is the single production write: the
    pre-validation copy lives in ``ranked_staging`` and is left in place for
    post-mortem when validation fails. The write itself is the catalog save of
    this node's ``ranked_predictions`` output.
    """
    model_version = parameters.get("model_version")
    logger.info(
        "Publishing validated predictions to production ranked_predictions "
        "(model_version=%s)",
        model_version,
    )
    return validated_predictions

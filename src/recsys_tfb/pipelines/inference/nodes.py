"""Every node function of the inference pipeline.

This module is the one home of the pipeline's ML story: a reader who opens it
sees each decision this pipeline makes about the data, without jumping files.
The mechanisms those decisions are expressed in live in ``steps/``, one module
per concern (``scoping``, ``population``, ``feature_view``, ``partitions``,
``chunk_plans``, ``validation``) — ``docs/agents/pipeline-node-design.md`` is
where the placement criterion and the node-body shape are written down.

Reading a node here, the decisions are the named steps; the constants and dtype
details those steps are made of (the bucket salt, the unknown-category
sentinel, the float32 cast) stay in the helpers. That is why these functions are
longer than the repo's usual — a node is a sequence of decisions, not a call.

``log_step`` goes only around a block that brings data to the driver or writes.
Everything else here is lazy: the joins, the filters, the column selects, the
casts and the window all return a plan in microseconds, and the computation they
describe runs later, inside ``catalog.save()``. Timing such a block reports a
guaranteed ~0.00s that reads exactly like "this step was fast", and mixing the
two kinds under one event name leaves nobody able to tell which zero means
which. Where a node's *time* actually goes is a question for the Runner's
``load``/``func``/``save`` split (``core/runner.py``), not for this module.
"""

import itertools
import logging

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import (
    REBUILD_SNAP_DATES_KEY,
    resolved_numeric_storage,
)
from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.extract import pdf_to_X
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.models.feature_view import model_feature_view
from recsys_tfb.pipelines.inference.steps.chunk_plans import (
    ScoringChunk,
    as_rows,
    build_chunk_report,
    plan_scoring_chunks,
)
from recsys_tfb.pipelines.inference.steps.feature_view import (
    model_columns_to_collect,
    require_population_has_model_columns,
)
from recsys_tfb.pipelines.inference.steps.partitions import (
    ENTITY_BUCKET_COL,
    populated_buckets,
    require_single_partition,
    written_score_partitions,
)
from recsys_tfb.pipelines.inference.steps.population import (
    drop_excluded_columns_keeping_identity,
    join_features_keeping_all_members,
    population_members,
    report_feature_coverage,
    require_feature_columns_present,
    require_population_covers_snap_dates,
    stored_population_columns,
    with_entity_bucket,
    with_time_as_partition_string,
)
from recsys_tfb.pipelines.inference.steps.scoping import (
    entity_buckets,
    iso_snap_dates,
    restrict_to_snap_dates,
    snap_dates_as_dates,
)
from recsys_tfb.pipelines.inference.steps.validation import (
    BATCH_CHECKS,
    ValidationError,
    completeness_failure,
    partition_completeness_failure,
    rank_consistency_failure,
    score_varies_within_group_failure,
    validate_scored_chunk,
)
from recsys_tfb.preprocessing import (
    cast_numeric_features_to_storage_type,
    encodable_categoricals,
    encode_categoricals,
    warn_unknown_encodings,
)

logger = logging.getLogger(__name__)


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

    Pre-check (input): ``inference_population`` covers every configured month.
    Post-condition: every feature column the preprocessor names survived the
    join.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    join_key = [time_col] + entity_cols

    n_buckets = entity_buckets(parameters)
    snap_dates = snap_dates_as_dates(parameters)

    categorical_cols = preprocessor["categorical_columns"]
    category_mappings = preprocessor["category_mappings"]
    drop_cols = preprocessor["drop_columns"]
    # The full set minus the item: the item has no column here because this
    # frame is not exploded by it. It reappears in the driver, per chunk.
    feature_columns = [c for c in preprocessor["feature_columns"] if c != item_col]
    keep_identity = [c for c in identity_cols if c != item_col]

    # Decision — who gets scored: membership comes from ``inference_population``
    # and from nothing else. Deriving it from ``feature_table`` instead would
    # make the population a side effect of feature coverage, so a month the
    # feature pipeline under-covered would score fewer entities and look fine.
    members = population_members(
        inference_population, time_col, join_key, snap_dates,
    )
    with log_step(logger, "read_population"):
        require_population_covers_snap_dates(members, time_col, snap_dates)

    # Decision — a member with no features is kept, so nothing downstream can
    # count how many there were; this is the durable record of it.
    with log_step(logger, "feature_coverage_report"):
        report_feature_coverage(members, feature_table, join_key, time_col)

    # Decision — features enrich membership, they do not filter it. LEFT with
    # members on the left: an INNER join here would drop members the feature
    # table has no row for, shrinking the published ranking with no error.
    result = join_features_keeping_all_members(members, feature_table, join_key)

    # Decision — where the chunk boundary falls: on the entity, so a query group
    # is never split across two independently scored partitions.
    result = with_entity_bucket(result, entity_cols, n_buckets)

    # Decision — the preprocessor's ``drop_columns`` leave here, except identity
    # columns: dropping one of those would take the ranking key or a partition
    # column with it.
    result = drop_excluded_columns_keeping_identity(
        result, drop_cols, identity_cols,
    )

    # Decision — identity categoricals are left alone here, and the item is not
    # even present: encoding an identity column in Spark ships the integer code
    # into a partition directory name (`prod_name=0` instead of
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
            result = encode_categoricals(result, encode_cols, category_mappings)
            warn_unknown_encodings(
                result, encode_cols,
                context="build_inference_population_features",
            )

    # Decision — what gets stored is the preprocessor's full feature set minus
    # the item, never the subset a particular model wants. The subset is sliced
    # per chunk from ``model.feature_names()`` inside
    # :func:`predict_and_write_scores`. That is the whole reason this table is
    # scoped by ``base_dataset_version`` and can be reused across
    # ``model_version`` — "optimising" it down to one model's columns would
    # silently bind it to that model, and nothing would go red
    # (ADR-0010 section 5).
    require_feature_columns_present(result.columns, feature_columns)
    result = result.select(
        *stored_population_columns(keep_identity, feature_columns)
    )

    # Decision — numeric features converge on one storage type, the one
    # ``dataset.numeric_feature_storage_type`` declares. Read here rather than
    # baked in so the scoring population is stored exactly as the training
    # frames were; which types get cast is the helper's business.
    storage_type, _ = resolved_numeric_storage(parameters)
    result, casted = cast_numeric_features_to_storage_type(
        result, feature_columns, storage_type,
    )

    # Decision — the time partition value is spelled here, not left to Spark's
    # coercion inside insertInto: the resume planner compares directory names
    # against exactly this string.
    result = with_time_as_partition_string(result, time_col)

    logger.info(
        "build_inference_population_features: %d columns "
        "(%d features, item excluded), %d numeric cast to %s, "
        "%d entity buckets",
        len(result.columns), len(feature_columns), len(casted), storage_type,
        n_buckets,
    )
    if casted:
        logger.debug(
            "build_inference_population_features: casted columns = %s", casted
        )
    return result


def predict_and_write_scores(
    model: ModelAdapter,
    inference_population_features: DataFrame,
    preprocessor: dict,
    parameters: dict,
    unranked_predictions,  # HiveTableDataset, supplied via Node(writes=...)
) -> tuple[dict, dict]:
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

    Pre-check (input): the landed population table holds every column the model
    declares.

    Post-conditions, all four on what this node itself computed — a failure
    points at this node's filter or at a stale
    ``inference_population_features``, never at the operator's config:

    * each frame handed to ``save()`` covers exactly one partition;
    * each chunk passes the chunk layer of validation;
    * a bucket the landed table has a partition for reads back non-empty
      (otherwise those entities would silently vanish from the ranking);
    * the run scored *something*, unless every chunk was skipped as already
      present.

    Returns:
        ``(score_manifest, score_chunk_report)``.

        The manifest is the DAG edge — the data itself travels through the
        catalog write, so the downstream nodes read it back from Hive. It also
        carries the partition bookkeeping ``validate_predictions`` needs to
        answer "is every chunk present" without scanning anything. It stays
        memory-only: landing it would let a ``--from-node rank_predictions``
        slice load a previous run's copy instead of re-running this node, and
        the numbers validation compares would then come from another run
        (``docs/pipelines/inference.md`` section 7.4).

        The report is the same bookkeeping in a form that outlives the process
        (issue #195). Nothing consumes it; it exists because after the run the
        log answers only "40 chunks were skipped" and the question people
        actually ask is *which* forty. Sent to the catalog as
        ``score_chunk_report`` — a diagnostic byproduct, same arrangement as
        ``sample_weight_report`` and ``numeric_precision_report``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    score_col = schema["score"]

    items = list(parameters["inference"]["products"])
    snap_dates = iso_snap_dates(parameters)
    n_buckets = entity_buckets(parameters)
    partition_cols = [time_col, item_col, ENTITY_BUCKET_COL]

    # Decision — which features, and in what order: the model decides, never the
    # current config. Building the view from
    # apply_feature_selection(preprocessor, parameters) would read *this* run's
    # training.feature_selection.exclude, and model_version can point at a model
    # trained under a different one; same-length excludes over different columns
    # would then misalign X silently (ADR-0011 §5).
    model_view = model_feature_view(model, preprocessor)
    feature_columns = model_view["feature_columns"]

    # Decision — what crosses into the driver per bucket: the model's columns,
    # not the table's. The item is excluded from both sides — it is assigned per
    # inner iteration rather than read.
    keep_identity = [c for c in identity_cols if c != item_col]
    collection_columns = model_columns_to_collect(
        keep_identity, feature_columns, identity_cols,
    )
    require_population_has_model_columns(
        inference_population_features.columns, collection_columns,
    )

    # Decision — what work this run does: the configured grid minus the chunks
    # whose partition already exists, plus whatever --rebuild-dates forces back
    # in. Timed because listing partitions is a metastore round trip, not
    # because it computes anything.
    with log_step(logger, "plan_chunks"):
        plan = plan_scoring_chunks(
            snap_dates,
            items,
            n_buckets,
            written_score_partitions(unranked_predictions, time_col, item_col),
            parameters.get(REBUILD_SNAP_DATES_KEY) or [],
        )
    logger.info(
        "[chunks] predict: to_process=%d skipped=%d rebuilt=%d surplus=%d "
        "(%d month(s) x %d bucket(s) x %d item(s))",
        len(plan.to_process), len(plan.skipped), len(plan.rebuilt),
        len(plan.surplus), len(set(snap_dates)), n_buckets, len(items),
    )

    # Decision — which scores get written when calibration is switched off: the
    # raw booster output, and only when a calibrator is actually wrapped. An
    # uncalibrated adapter has nothing to bypass.
    use_calibration = parameters.get("inference", {}).get("use_calibration", True)
    use_uncalibrated = not use_calibration and isinstance(model, CalibratedModelAdapter)
    if use_uncalibrated:
        logger.info("Calibration disabled by config, using uncalibrated scores")

    # Decision — which empty buckets are legitimate: the ones with no partition
    # in the landed table. Asked once, up front, so the loop's per-bucket
    # judgement costs nothing.
    with log_step(logger, "list_populated_buckets"):
        populated = populated_buckets(
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
        # ADR-0014 predicted this caller when it fixed the event name in the
        # shared layer: a name built from the data is one bucket per
        # (month, entity bucket), so nothing can be summed across a run. The
        # values travel as fields and still print on the console line.
        with log_step(
            logger, "read_bucket",
            time_value=snap_date, entity_bucket=str(bucket),
        ):
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
            with log_step(
                logger, "score_item",
                time_value=snap_date, entity_bucket=str(bucket),
                item_name=item,
            ):
                # In place, on the frame already in the driver: this is the
                # reuse the loop order buys. The value written is the raw item
                # name — pdf_to_X applies the integer code to its own copy, so
                # the name is what reaches the partition column.
                bucket_pdf[item_col] = item
                X = pdf_to_X(bucket_pdf, model_view, parameters)
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
                require_single_partition(out_pdf, partition_cols)
                # The chunk half of validation, before the write rather than
                # after the whole table is ranked: nulls, duplicates, the row
                # count and the item value domain are all answerable from the
                # two frames already in the driver (ADR-0011 section 3). A bad
                # first chunk stops the run in minutes instead of surviving
                # until `validate_predictions` hours later.
                validate_scored_chunk(
                    out_pdf, bucket_pdf, schema=schema, known_items=items,
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
        for chunk in written_score_partitions(
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
        "chunks_processed": as_rows(processed),
        "chunks_skipped": as_rows(plan.skipped),
        "chunks_rebuilt": as_rows(plan.rebuilt),
        "chunks_empty": as_rows(empty),
        "expected_partitions": as_rows(expected),
        "written_partitions": as_rows(present),
    }
    logger.info(
        "predict_and_write_scores: done — processed=%d skipped=%d rebuilt=%d "
        "empty=%d n_rows_written=%d model_version=%s",
        len(processed), len(plan.skipped), len(plan.rebuilt), len(empty),
        n_rows_written, manifest["model_version"],
    )
    # Built here rather than in a node of its own so that it lands whenever
    # scoring decisions are made. A separate node would be a sibling of
    # `rank_predictions`, not an ancestor, so `--from-node rank_predictions`
    # would skip it — and that slice re-runs *this* node (section 7.4), which
    # is exactly a run whose skip list is worth keeping.
    return manifest, build_chunk_report(
        manifest, plan.surplus, parameters.get("run_id"),
    )


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

    Every step below is lazy — the whole node returns a plan, and the work
    happens in the catalog save of ``ranked_staging``. That is why it carries no
    ``log_step``.
    """
    # Decision — scope: this run's months only. The table read back holds every
    # month this model version ever published, and re-ranking one of those would
    # republish it — indistinguishably from a correct run (ADR-0010 section 5).
    #
    # First, before any other config is read: an empty `inference.snap_dates`
    # has to surface as this function's ValueError, not as whatever the next
    # lookup happens to raise.
    ranked = restrict_to_snap_dates(unranked_predictions, parameters)

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    # Decision — the bucket stops here. It exists so one save touches one
    # partition (ADR-0010 section 3, constraint C) and it is not part of any
    # published contract, so carrying it into `ranked_staging` would leak a
    # purely computational mechanism into the shape downstream consumers query.
    #
    # Dropping it cannot change the ranking: the bucket is hashed from the
    # entity columns alone, and the ranking group is (time, entity), so the
    # bucket is a function of the group — every item of one entity is in the
    # same bucket, and no group is split across buckets.
    ranked = ranked.drop(ENTITY_BUCKET_COL)

    # Decision — what the rank means: position within the query group by
    # descending score, ties broken arbitrarily. Spelled inline rather than
    # behind a helper name because the tie behaviour is the part a reader has to
    # see — `row_number` numbers tied rows 1, 2, 3 in whatever order the
    # shuffle produced, which is what `score_varies_within_group` exists to
    # notice when it becomes the whole table.
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

    Post-condition on the whole ranked table, on the way to publication.

    Only the checks that a single chunk cannot answer live here — one chunk is
    one item, so anything comparing a query group's items against each other has
    to wait for the whole table (ADR-0011 section 3). Nulls, duplicates, the
    per-chunk row count and the item value domain are checked in the driver as
    each chunk is scored (:func:`validate_scored_chunk`), which is both free and
    hours earlier. ``steps.validation.BATCH_CHECKS`` is the register of what
    belongs here, and each name in it resolves to a ``<name>_failure`` predicate
    in that module.

    **Two Spark actions on the success path**, down from seven — eight counting
    the ``scoring_dataset`` re-scan ADR-0011 §3 measured, which #188 had already
    removed. One grouped aggregation answers ``completeness``,
    ``score_varies_within_group`` and the rank range together, and one windowed
    pass covers the score-versus-rank ordering. ``partition_completeness`` needs
    no action at all — it compares two lists the manifest already carries.

    The aggregations stay in this node rather than moving to the predicates:
    they are Spark, the predicates are arithmetic on one already-collected row,
    and the action budget is a property of this function.
    """
    # Decision — scope: the staging frame comes back from Hive holding every
    # month this model version has ever published, while the checks are about
    # this run.
    #
    # First, before any other config is read, for the same reason as in
    # :func:`rank_predictions`: a missing `inference.snap_dates` must fail as
    # this function's ValueError rather than as a KeyError from the next lookup.
    ranked_predictions = restrict_to_snap_dates(ranked_predictions, parameters)

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    products = parameters["inference"]["products"]
    n_products = len(products)
    group_cols = [time_col] + entity_cols

    failures = []

    # Decision — completeness is asked of the *partitions*, not the rows. Both
    # of the row-count comparisons that could stand here fail on a correct run:
    # against the un-exploded population because it is |items| times shorter,
    # against the manifest's own count because a resume writes a fraction.
    # Subscripted, not `.get(..., [])`: two absent keys would compare equal and
    # the check would pass vacuously.
    failures.append(partition_completeness_failure(
        score_manifest["expected_partitions"],
        score_manifest["written_partitions"],
    ))

    with log_step(logger, "run_sanity_checks"):
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
        # A group of one is excluded from the constant count here rather than in
        # the predicate — a config with a single item makes every group look
        # constant, and group size is `completeness`'s question, not that one's.
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

        # Decision — every group holds one row per configured item.
        failures.append(completeness_failure(summary, n_products))

        # Decision — a table where whole groups score identically is published
        # anyway below a measured threshold, because that is what a correct
        # isotonic calibration looks like.
        failures.append(score_varies_within_group_failure(summary))

        # Decision — ranks are 1..N and run in descending score order. The order
        # half is passed as a thunk so its Spark action is spent only when the
        # range already holds; the two questions report as one check.
        #
        # A nested def, which `pipeline-node-design.md` rule 9 otherwise rules
        # out for a *step*. This is not a step, it is an argument: the predicate
        # owns the decision, this owns the Spark. It stays here rather than
        # moving to `steps/` because the only module that would own it —
        # `steps/validation.py` — is deliberately pyspark-free so both layers'
        # tests skip the SparkSession, and a seventh module holding one function
        # reads worse than this does. If this pattern recurs, that is the point
        # to reopen the question.
        def count_score_order_violations() -> int:
            w = Window.partitionBy(*group_cols).orderBy(F.col(rank_col))
            with_prev = ranked_predictions.withColumn(
                "_prev_score", F.lag(score_col).over(w)
            )
            return with_prev.filter(
                F.col("_prev_score").isNotNull()
                & (F.col(score_col) > F.col("_prev_score"))
            ).count()

        failures.append(rank_consistency_failure(
            summary, n_products, count_score_order_violations,
        ))

        failures = [failure for failure in failures if failure is not None]
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

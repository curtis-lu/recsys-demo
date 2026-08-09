"""PySpark implementations for the inference pipeline."""

import logging

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.pipelines.inference.validation import ValidationError
from recsys_tfb.preprocessing import (
    _cast_feature_floats_to_float32,
    _encode_categoricals,
    encodable_categoricals,
    warn_unknown_encodings,
)

logger = logging.getLogger(__name__)


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

    snap_dates = [
        pd.Timestamp(value).date()
        for value in (parameters.get("inference", {}) or {}).get("snap_dates", [])
    ]
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


def build_scoring_dataset(
    inference_population: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """以 inference_population 為母體建立評分資料；feature_table 僅作 enrichment。

    母體 grain (time, entity) 由 source_etl 保證唯一，故不需 dropDuplicates。
    缺特徵的母體成員保留，以 feature_present=false 標記（in-memory + log，不下推）。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    join_key = [time_col] + entity_cols

    snap_dates = [
        pd.Timestamp(value).date()
        for value in parameters["inference"]["snap_dates"]
    ]
    products = parameters["inference"]["products"]
    spark = feature_table.sparkSession

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

    with log_step(logger, "cross_join"):
        products_df = spark.createDataFrame([(p,) for p in products], [item_col])
        scoring = customers.crossJoin(products_df)

    with log_step(logger, "merge_features"):
        ft = feature_table.withColumn("_ft_present", F.lit(True))
        scoring = scoring.join(ft, on=join_key, how="left")
        scoring = scoring.withColumn(
            "feature_present", F.col("_ft_present").isNotNull()
        ).drop("_ft_present")

    logger.info(
        "Built scoring dataset for %d products x %d snap_dates",
        len(products),
        len(snap_dates),
    )
    return scoring


def apply_preprocessor(
    scoring_dataset: DataFrame,
    preprocessor: dict,
    parameters: dict,
) -> DataFrame:
    """Apply training preprocessor to scoring dataset, preserving identity columns.

    Reads all four ``PreprocessorMetadata`` keys off the landed artifact and
    touches ``parameters`` only for the schema — this is the reader half of that
    contract, the writer being the dataset pipeline's fit node.

    Returns identity + feature columns for model prediction. ``schema.item``
    appears in both blocks and stays a **raw string in both**: the integer code
    it needs as a feature is applied in the driver, per chunk, by
    :func:`predict_scores`. See ADR-0010 section 4.
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor["feature_columns"]
    categorical_cols = preprocessor["categorical_columns"]
    category_mappings = preprocessor["category_mappings"]
    drop_cols = preprocessor["drop_columns"]

    # Drop non-feature columns (except identity and categorical)
    cols_to_drop = [
        c for c in drop_cols
        if c in scoring_dataset.columns and c not in identity_cols
    ]
    result = scoring_dataset.drop(*cols_to_drop)

    # Decision — the item reaches the model as a code and the output as a name,
    # so the encoding of identity categoricals waits for the driver, where it can
    # be applied to a copy. Doing it here writes the code into the identity
    # position as well, and that position becomes the partition directory name of
    # all three inference tables: `prod_name=0` instead of `prod_name=exchange_fx`,
    # with every sanity check still green (ADR-0010 §6).
    #
    # Cost, for ticket D: this frame is already exploded by |products|, so the
    # warning's aggregation scans ~22x more rows than the values it reports need.
    # The un-exploded frame does not exist yet at this point in the pipeline; when
    # `inference_population_features` lands, this call moves onto it.
    with log_step(logger, "encode_categoricals"):
        encode_cols = encodable_categoricals(
            categorical_cols, result.columns, identity_cols,
        )
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            warn_unknown_encodings(
                result, encode_cols, context="apply_preprocessor",
            )

    with log_step(logger, "select_feature_columns"):
        missing = set(feature_columns) - set(result.columns)
        if missing:
            raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")
        result = result.select(*identity_cols, *feature_columns)

    with log_step(logger, "cast_features_to_float32"):
        result, casted = _cast_feature_floats_to_float32(result, feature_columns)
    logger.info(
        "apply_preprocessor: %d columns, cast %d float-like feature columns to float32",
        len(result.columns), len(casted),
    )
    if casted:
        logger.debug("apply_preprocessor: casted columns = %s", casted)
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


def predict_scores(
    model: ModelAdapter,
    X_score: DataFrame,
    scoring_dataset: DataFrame,
    preprocessor: dict,
    parameters: dict,
) -> DataFrame:
    """Predict probability scores, chunked by (snap_date, prod_name) to control memory.

    Assembles each chunk's feature matrix through :func:`_pdf_to_X`, the same
    slicing helper training's per-partition prediction uses, so the identity
    categoricals ``apply_preprocessor`` deliberately left as raw strings get
    their integer codes here — in a copy, leaving the identity columns that
    become partition values alone (ADR-0010 §4).
    """
    from recsys_tfb.io.extract import _pdf_to_X

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    score_col = schema["score"]

    artifact_feature_columns = list(preprocessor["feature_columns"])
    feature_names_fn = getattr(model, "feature_names", None)
    model_feature_names = (
        feature_names_fn() if callable(feature_names_fn) else None
    )
    if model_feature_names:
        feature_columns = list(model_feature_names)
        require_ordered_subsequence(feature_columns, artifact_feature_columns)
    else:
        # No declaration, so there is no second opinion to check the artifact
        # against and the assertion is skipped rather than run against itself.
        # Every adapter in this repo declares one once it holds a fitted model
        # (LightGBMAdapter returns None only before load(); CalibratedModelAdapter
        # forwards to its base), so this branch is doubles in tests, not
        # production — and it is logged rather than silent for that reason.
        feature_columns = list(artifact_feature_columns)
        logger.info(
            "Model declares no feature_names(); falling back to the "
            "preprocessor's %d feature columns", len(feature_columns),
        )
    missing_features = sorted(set(feature_columns) - set(X_score.columns))
    if missing_features:
        raise ValueError(
            "Scoring data is missing feature columns required by the model: "
            f"{missing_features}"
        )

    # The view _pdf_to_X slices by. Built from the model's declaration, never
    # from apply_feature_selection(preprocessor, parameters): that reads the
    # *current* config's training.feature_selection.exclude, and model_version
    # can point at a model trained under a different one. Same-length excludes
    # over different columns would then misalign X silently (ADR-0011 §5).
    preprocessor_view = {**preprocessor, "feature_columns": feature_columns}

    spark = X_score.sparkSession

    use_calibration = parameters.get("inference", {}).get("use_calibration", True)
    use_uncalibrated = not use_calibration and isinstance(model, CalibratedModelAdapter)

    if use_uncalibrated:
        logger.info("Calibration disabled by config, using uncalibrated scores")

    with log_step(logger, "model_predict"):
        # Process by (snap_date, prod_name) to minimize per-chunk memory
        chunks = X_score.select(time_col, item_col).distinct().collect()

        all_results = []
        for row in chunks:
            sd, prod = row[time_col], row[item_col]
            chunk = X_score.filter(
                (F.col(time_col) == sd) & (F.col(item_col) == prod)
            )
            collection_columns = identity_cols + [
                column
                for column in feature_columns
                if column not in identity_cols
            ]
            chunk_pdf = chunk.select(*collection_columns).toPandas()
            X = _pdf_to_X(chunk_pdf, preprocessor_view, parameters)
            identity_pdf = chunk_pdf[identity_cols].copy()
            if use_uncalibrated:
                scores = model.predict_uncalibrated(X)
            else:
                scores = model.predict(X)
            identity_pdf[score_col] = scores
            all_results.append(identity_pdf)

        if not all_results:
            raise ValueError(
                "No scoring rows found for inference.snap_dates and products"
            )
        result_pdf = pd.concat(all_results, ignore_index=True)

    logger.info(
        "Predicted %d scores, mean=%.4f",
        len(result_pdf),
        result_pdf[score_col].mean(),
    )
    # No model_version column: `partition_filter` owns it now. The catalog's
    # save injects it from the same `parameters["model_version"]` this node
    # would have read, and injecting it here only makes that injection take the
    # "column already present" branch, which pays a `distinct()` action to check
    # the value it just wrote. training_eval_predictions has always been shaped
    # this way (ADR-0010 section 5).
    return spark.createDataFrame(result_pdf)


def rank_predictions(
    unranked_predictions: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Rank items by score within each query group."""
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

    with log_step(logger, "rank_scores"):
        w = Window.partitionBy(*group_cols).orderBy(F.desc(score_col))
        ranked = unranked_predictions.withColumn(rank_col, F.row_number().over(w))

    logger.info("Ranked predictions by %s", group_cols)
    return ranked


def validate_predictions(
    ranked_predictions: DataFrame,
    scoring_dataset: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Validate inference output with sanity checks. Raises ValidationError on failure.

    Only the staging frame gets restricted: it comes back from Hive holding
    every month this model version has ever published, while ``scoring_dataset``
    is this run's in-memory frame, already built against ``inference.snap_dates``
    by :func:`build_scoring_dataset`. Restricting both would hide a real
    disagreement between them behind a filter applied to each side.
    """
    with log_step(logger, "restrict_to_snap_dates"):
        ranked_predictions = restrict_to_snap_dates(
            ranked_predictions, parameters
        )

    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    products = parameters["inference"]["products"]
    n_products = len(products)
    group_cols = [time_col] + entity_cols

    with log_step(logger, "run_sanity_checks"):
        failures = []

        # 1. row_count_match
        n_ranked = ranked_predictions.count()
        n_scoring = scoring_dataset.count()
        if n_ranked != n_scoring:
            failures.append({
                "check": "row_count_match",
                "detail": f"ranked_predictions has {n_ranked} rows, scoring_dataset has {n_scoring} rows",
            })

        # 2. score_range
        out_of_range = ranked_predictions.filter(
            ~F.col(score_col).between(0.0, 1.0)
        ).count()
        if out_of_range > 0:
            stats = ranked_predictions.agg(
                F.min(score_col).alias("min_score"),
                F.max(score_col).alias("max_score"),
            ).collect()[0]
            failures.append({
                "check": "score_range",
                "detail": (
                    f"{out_of_range} scores outside [0, 1], "
                    f"min={stats['min_score']:.6f}, max={stats['max_score']:.6f}"
                ),
            })

        # 3. no_missing
        check_cols = identity_cols + [score_col, rank_col]
        null_conditions = [F.isnull(F.col(c)) for c in check_cols]
        null_rows = ranked_predictions.filter(
            null_conditions[0] if len(null_conditions) == 1
            else null_conditions[0].__or__(null_conditions[1])
            if len(null_conditions) == 2
            else F.greatest(*[F.when(cond, F.lit(1)).otherwise(F.lit(0)) for cond in null_conditions]) > 0
        ).count()
        if null_rows > 0:
            null_exprs = [
                F.sum(F.when(F.isnull(F.col(c)), 1).otherwise(0)).alias(c)
                for c in check_cols
            ]
            null_counts = ranked_predictions.agg(*null_exprs).collect()[0]
            cols_with_nulls = {c: null_counts[c] for c in check_cols if null_counts[c] > 0}
            failures.append({
                "check": "no_missing",
                "detail": f"NaN values found: {cols_with_nulls}",
            })

        # 4. completeness
        group_counts = ranked_predictions.groupBy(*group_cols).count()
        incomplete = group_counts.filter(F.col("count") != n_products)
        n_incomplete = incomplete.count()
        if n_incomplete > 0:
            stats = group_counts.agg(
                F.min("count").alias("min_size"),
                F.max("count").alias("max_size"),
            ).collect()[0]
            failures.append({
                "check": "completeness",
                "detail": (
                    f"{n_incomplete} groups do not have exactly {n_products} products, "
                    f"sizes: min={stats['min_size']}, max={stats['max_size']}"
                ),
            })

        # 5. rank_consistency — check ranks are 1..N and ordered by score desc
        rank_stats = ranked_predictions.agg(
            F.min(rank_col).alias("min_rank"),
            F.max(rank_col).alias("max_rank"),
        ).collect()[0]
        if rank_stats["min_rank"] != 1 or rank_stats["max_rank"] != n_products:
            failures.append({
                "check": "rank_consistency",
                "detail": (
                    f"Rank range [{rank_stats['min_rank']}, {rank_stats['max_rank']}] "
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

        # 6. no_duplicates
        n_total = n_ranked
        n_distinct = ranked_predictions.dropDuplicates(identity_cols).count()
        n_dupes = n_total - n_distinct
        if n_dupes > 0:
            failures.append({
                "check": "no_duplicates",
                "detail": f"{n_dupes} duplicate rows on {identity_cols}",
            })

        if failures:
            logger.error("Validation failed: %s", failures)
            raise ValidationError(failures)

    logger.info("All %d sanity checks passed (%d rows)", 6, n_ranked)
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

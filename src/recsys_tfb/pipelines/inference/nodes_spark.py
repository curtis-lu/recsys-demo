"""PySpark implementations for the inference pipeline."""

import logging

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.pipelines.inference.validation import ValidationError
from recsys_tfb.pipelines.preprocessing import apply_preprocessor_spark

logger = logging.getLogger(__name__)


def build_scoring_dataset(
    feature_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Build scoring dataset by cross-joining customers with all products."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    join_key = [time_col] + entity_cols

    snap_dates = [pd.Timestamp(d) for d in parameters["inference"]["snap_dates"]]
    products = parameters["inference"]["products"]

    # Filter to target snap_dates and get unique identity keys
    customers = (
        feature_table.filter(F.col(time_col).isin(snap_dates))
        .select(*join_key)
        .dropDuplicates()
    )

    # Cross-join with products
    spark = feature_table.sparkSession
    products_df = spark.createDataFrame([(p,) for p in products], [item_col])
    scoring = customers.crossJoin(products_df)

    # Left-join features
    scoring = scoring.join(feature_table, on=join_key, how="left")

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
    """Apply training preprocessor to scoring dataset, preserving identity columns."""
    return apply_preprocessor_spark(scoring_dataset, preprocessor, parameters)


def predict_scores(
    model: ModelAdapter,
    X_score: DataFrame,
    scoring_dataset: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Predict probability scores, chunked by (snap_date, prod_name) to control memory."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    score_col = schema["score"]

    feature_columns = [
        c for c in X_score.columns if c not in identity_cols
    ]
    spark = X_score.sparkSession

    use_calibration = parameters.get("inference", {}).get("use_calibration", True)
    use_uncalibrated = not use_calibration and isinstance(model, CalibratedModelAdapter)

    if use_uncalibrated:
        logger.info("Calibration disabled by config, using uncalibrated scores")

    # Process by (snap_date, prod_name) to minimize per-chunk memory
    chunks = X_score.select(time_col, item_col).distinct().collect()

    all_results = []
    for row in chunks:
        sd, prod = row[time_col], row[item_col]
        chunk = X_score.filter(
            (F.col(time_col) == sd) & (F.col(item_col) == prod)
        )
        features_pdf = chunk.select(*feature_columns).toPandas()
        identity_pdf = chunk.select(*identity_cols).toPandas()
        if use_uncalibrated:
            scores = model.predict_uncalibrated(features_pdf)
        else:
            scores = model.predict(features_pdf)
        identity_pdf[score_col] = scores
        all_results.append(identity_pdf)

    result_pdf = pd.concat(all_results, ignore_index=True)

    logger.info(
        "Predicted %d scores, mean=%.4f",
        len(result_pdf),
        result_pdf[score_col].mean(),
    )
    result = spark.createDataFrame(result_pdf)

    # Inject model_version for partitioned output in production
    model_version = parameters.get("model_version")
    if model_version:
        result = result.withColumn("model_version", F.lit(model_version))

    return result


def rank_predictions(
    score_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Rank products by score within each query group."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    w = Window.partitionBy(*group_cols).orderBy(F.desc(score_col))
    ranked = score_table.withColumn(rank_col, F.row_number().over(w))

    logger.info("Ranked predictions by %s", group_cols)
    return ranked


def validate_predictions(
    ranked_predictions: DataFrame,
    scoring_dataset: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Validate inference output with sanity checks. Raises ValidationError on failure."""
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    products = parameters["inference"]["products"]
    n_products = len(products)
    group_cols = [time_col] + entity_cols

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
        # Get per-column null counts
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
    # Check rank range
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
        # Check score ordering: for each group, rank i should have score >= rank i+1
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

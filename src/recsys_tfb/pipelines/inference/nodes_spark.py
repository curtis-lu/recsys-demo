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
from recsys_tfb.preprocessing._spark import apply_preprocessor as _apply_preprocessor

logger = logging.getLogger(__name__)


def _filter_current_inference_scope(
    df: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Limit a persisted inference table to the model and dates of this run."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    inference = parameters.get("inference", {})

    model_version = parameters.get("model_version")
    if not model_version:
        return df

    if "model_version" in df.columns:
        df = df.filter(F.col("model_version") == model_version)

    snap_dates = [
        pd.Timestamp(value).date()
        for value in inference.get("snap_dates", [])
    ]
    if snap_dates and time_col in df.columns:
        df = df.filter(F.col(time_col).cast("date").isin(snap_dates))

    return df


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
    """Apply training preprocessor to scoring dataset, preserving identity columns."""
    return _apply_preprocessor(scoring_dataset, preprocessor, parameters)


def _predict_chunk_staged(model, X, features_pdf):
    """Staged inference chunk: route, skip missing groups, count them.

    Returns (scores, keep_mask, missing_stats). ``scores`` is the full,
    per-row array straight from predict_routed (NaN at skipped rows) —
    callers apply ``keep_mask`` themselves to align with other per-row
    frames (e.g. identity columns) before combining. Inference path uses
    on_missing="skip" (spec D11 分流): new partition values are a natural
    event for inference_population — drop, WARN, and report.
    """
    from recsys_tfb.models.staged.partition import routing_keys

    keys = routing_keys(features_pdf, model.partition_keys)
    scores, keep = model.predict_routed(X, keys, on_missing="skip")
    return scores, keep, dict(model.last_missing_stats)


def _raise_if_all_rows_skipped(result_pdf: pd.DataFrame, missing_stats: dict) -> None:
    """Fail loud when the concatenated score table has zero rows.

    ``all_results`` (one entry per (time, item) chunk) can be non-empty
    while every entry contributed zero rows — staged routing's
    on_missing="skip" drops a chunk's rows entirely when no stage-1 model
    covers the group(s) present. The pre-existing ``if not all_results``
    guard never catches this (the list itself is non-empty), so without
    this check ``result_pdf[score_col].mean()`` silently returns NaN and a
    zero-row score table would propagate downstream unnoticed.
    """
    if result_pdf.empty:
        raise ValueError(
            "all rows skipped — no stage-1 model covers any partition "
            "group in the scoring data; missing_groups="
            f"{dict(sorted(missing_stats.items()))}"
        )


def predict_scores(
    model: ModelAdapter,
    X_score: DataFrame,
    scoring_dataset: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, dict]:
    """Predict probability scores, chunked by (snap_date, prod_name) to control memory.

    Returns (score_table, staged_missing_groups_report). The report is a
    no-op summary ({"model_structure": "shared", "missing_groups": {}, ...})
    for shared models; only StagedModelAdapter can skip rows (D11 分流：
    inference 用 on_missing="skip"，見 _predict_chunk_staged)。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    score_col = schema["score"]

    available_feature_columns = [
        c for c in X_score.columns if c not in identity_cols
    ]
    feature_names_fn = getattr(model, "feature_names", None)
    model_feature_names = (
        feature_names_fn() if callable(feature_names_fn) else None
    )
    feature_columns = (
        list(model_feature_names)
        if model_feature_names
        else available_feature_columns
    )
    missing_features = sorted(set(feature_columns) - set(X_score.columns))
    if missing_features:
        raise ValueError(
            "Scoring data is missing feature columns required by the model: "
            f"{missing_features}"
        )

    spark = X_score.sparkSession

    from recsys_tfb.models.staged.adapter import StagedModelAdapter

    is_staged = isinstance(model, StagedModelAdapter)

    use_calibration = parameters.get("inference", {}).get("use_calibration", True)
    use_uncalibrated = not use_calibration and isinstance(model, CalibratedModelAdapter)

    if use_uncalibrated:
        logger.info("Calibration disabled by config, using uncalibrated scores")

    missing_stats: dict = {}
    total_rows = 0

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
            features_pdf = chunk_pdf[feature_columns]
            identity_pdf = chunk_pdf[identity_cols].copy()
            total_rows += len(chunk_pdf)
            if is_staged:
                chunk_scores, keep_mask, chunk_missing = _predict_chunk_staged(
                    model, features_pdf.values, chunk_pdf)
                scores = chunk_scores[keep_mask]
                if not keep_mask.all():
                    identity_pdf = identity_pdf.iloc[keep_mask.nonzero()[0]]
                for g, n in chunk_missing.items():
                    missing_stats[g] = missing_stats.get(g, 0) + n
            elif use_uncalibrated:
                scores = model.predict_uncalibrated(features_pdf)
            else:
                scores = model.predict(features_pdf)
            identity_pdf[score_col] = scores
            all_results.append(identity_pdf)

        if not all_results:
            raise ValueError(
                "No scoring rows found for inference.snap_dates and products"
            )
        result_pdf = pd.concat(all_results, ignore_index=True)
        _raise_if_all_rows_skipped(result_pdf, missing_stats)

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

    report = {
        "model_structure": "staged" if is_staged else "shared",
        "missing_groups": missing_stats,
        "rows_skipped": int(sum(missing_stats.values())),
        "rows_total": int(total_rows),
    }
    if missing_stats:
        logger.warning(
            "predict_scores: %d group(s) had no stage-1 model — skipped %d/%d "
            "row(s): %s — the candidate universe SHRANK for affected "
            "entities; retrain to cover new groups",
            len(missing_stats), report["rows_skipped"], report["rows_total"],
            dict(sorted(missing_stats.items())),
        )

    return result, report


def rank_predictions(
    score_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Rank products by score within each query group."""
    score_table = _filter_current_inference_scope(score_table, parameters)

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    with log_step(logger, "rank_scores"):
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
    ranked_predictions = _filter_current_inference_scope(
        ranked_predictions, parameters
    )
    scoring_dataset = _filter_current_inference_scope(
        scoring_dataset, parameters
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

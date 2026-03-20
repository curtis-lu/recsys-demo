"""PySpark implementations for the inference pipeline."""

import logging

import lightgbm as lgb
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def build_scoring_dataset(
    feature_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Build scoring dataset by cross-joining customers with all products."""
    snap_dates = [pd.Timestamp(d) for d in parameters["inference"]["snap_dates"]]
    products = parameters["inference"]["products"]

    # Filter to target snap_dates and get unique (snap_date, cust_id)
    customers = (
        feature_table.filter(F.col("snap_date").isin(snap_dates))
        .select("snap_date", "cust_id")
        .dropDuplicates()
    )

    # Cross-join with products
    spark = feature_table.sparkSession
    products_df = spark.createDataFrame([(p,) for p in products], ["prod_name"])
    scoring = customers.crossJoin(products_df)

    # Left-join features
    scoring = scoring.join(feature_table, on=["snap_date", "cust_id"], how="left")

    n_customers = customers.count()
    logger.info(
        "Scoring dataset: %d rows (%d customers x %d products x %d snap_dates)",
        scoring.count(),
        n_customers,
        len(products),
        len(snap_dates),
    )
    return scoring


def apply_preprocessor(
    scoring_dataset: DataFrame,
    preprocessor: dict,
) -> DataFrame:
    """Apply training preprocessor to scoring dataset, preserving identity columns."""
    drop_cols = preprocessor["drop_columns"]
    category_mappings = preprocessor["category_mappings"]
    categorical_cols = preprocessor["categorical_columns"]
    feature_columns = preprocessor["feature_columns"]

    spark = scoring_dataset.sparkSession
    identity_cols = ["snap_date", "cust_id", "prod_name"]

    # Drop non-feature columns (except identity and categorical)
    cols_to_drop = [c for c in drop_cols if c in scoring_dataset.columns and c not in identity_cols]
    result = scoring_dataset.drop(*cols_to_drop)

    # Encode categoricals via broadcast join
    for col in categorical_cols:
        categories = category_mappings[col]
        mapping_rows = [(cat, idx) for idx, cat in enumerate(categories)]
        mapping_df = spark.createDataFrame(mapping_rows, [col, f"{col}_code"])
        result = result.join(F.broadcast(mapping_df), on=col, how="left")
        result = result.drop(col).withColumnRenamed(f"{col}_code", col)

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Select identity + feature columns in correct order
    result = result.select(*identity_cols, *feature_columns)

    logger.info("Preprocessed scoring data: %d columns", len(result.columns))
    return result


def predict_scores(
    model: lgb.Booster,
    X_score: DataFrame,
    scoring_dataset: DataFrame,
) -> DataFrame:
    """Predict probability scores for each customer-product pair, chunked by snap_date."""
    feature_columns = [
        c for c in X_score.columns if c not in ("snap_date", "cust_id", "prod_name")
    ]
    spark = X_score.sparkSession

    # Process by snap_date to control memory
    snap_dates = [row.snap_date for row in X_score.select("snap_date").distinct().collect()]

    all_results = []
    for sd in snap_dates:
        chunk = X_score.filter(F.col("snap_date") == sd).toPandas()
        X_chunk = chunk[feature_columns]
        scores = model.predict(X_chunk)

        chunk_dedup = chunk.loc[:, ~chunk.columns.duplicated()]
        chunk_result = chunk_dedup[["snap_date", "cust_id", "prod_name"]].copy()
        chunk_result["score"] = scores

        all_results.append(chunk_result)

    result_pdf = pd.concat(all_results, ignore_index=True)

    logger.info(
        "Predicted %d scores, mean=%.4f",
        len(result_pdf),
        result_pdf["score"].mean(),
    )
    return spark.createDataFrame(result_pdf)


def rank_predictions(
    score_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Rank products by score within each (snap_date, cust_id) group."""
    w = Window.partitionBy("snap_date", "cust_id").orderBy(F.desc("score"))
    ranked = score_table.withColumn("rank", F.row_number().over(w))

    n_groups = ranked.select("snap_date", "cust_id").dropDuplicates().count()
    logger.info("Ranked predictions: %d rows, %d groups", ranked.count(), n_groups)
    return ranked

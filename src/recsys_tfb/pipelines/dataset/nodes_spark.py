"""PySpark implementations for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys
from recsys_tfb.pipelines.dataset.nodes_shared import validate_date_splits
from recsys_tfb.preprocessing._spark import (
    apply_preprocessor_to_features as _apply_preprocessor_to_features,
    build_model_input as _build_model_input,
    fit_preprocessor_metadata as _fit_preprocessor_metadata,
)

logger = logging.getLogger(__name__)



def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys using date range from parameters."""
    validate_date_splits(parameters)

    ds = parameters["dataset"]
    time_col = get_schema(parameters)["time"]
    start = pd.Timestamp(ds["train_snap_date_start"])
    end = pd.Timestamp(ds["train_snap_date_end"])

    # Filter sample_pool to train date range directly
    pool = sample_pool.filter(
        (F.col(time_col) >= F.lit(start)) & (F.col(time_col) <= F.lit(end))
    )

    # Collect unique dates for passing to select_keys
    train_dates_rows = pool.select(time_col).distinct().collect()
    train_dates = [row[time_col] for row in train_dates_rows]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(sample_pool, parameters, train_dates, ds["sample_ratio"], overrides)


def select_calibration_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(sample_pool, parameters, cal_dates, cal_ratio, cal_overrides)


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by cust_id ratio.

    All rows for a given cust_id are assigned to the same split.
    No .count() action triggered for logging.
    """
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    cust_col = entity_cols[0]

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Assign random value per cust_id, split by threshold
    cust_df = sample_keys.select(cust_col).distinct()
    cust_df = cust_df.withColumn("_rand", F.rand(seed))

    # cust_ids with _rand < train_dev_ratio → train-dev
    dev_custs = cust_df.filter(F.col("_rand") < F.lit(train_dev_ratio)).select(cust_col)
    train_custs = cust_df.filter(F.col("_rand") >= F.lit(train_dev_ratio)).select(cust_col)

    train_keys = sample_keys.join(train_custs, on=cust_col, how="inner")
    train_dev_keys = sample_keys.join(dev_custs, on=cust_col, how="inner")

    logger.info(
        "Split train keys (ratio=%.2f)",
        train_dev_ratio,
    )
    return train_keys, train_dev_keys


def select_val_keys(
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select validation identity keys (full population, optional random cust_id sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols
    cust_col = entity_cols[0]

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    val_labels = label_table.filter(F.col(time_col).isin(val_dates))
    all_keys = val_labels.select(*identity_key).dropDuplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys (full population)")
        return all_keys

    # Pure random cust_id sampling
    custs = all_keys.select(cust_col).distinct()
    sampled_custs = custs.withColumn("_rand", F.rand(seed)).filter(
        F.col("_rand") < F.lit(val_sample_ratio)
    ).select(cust_col)

    sampled = all_keys.join(sampled_custs, on=cust_col, how="inner")
    logger.info("Val keys (ratio=%.2f)", val_sample_ratio)
    return sampled


def select_test_keys(
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select test identity keys (full population, no sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    ds = parameters["dataset"]
    test_dates = [pd.Timestamp(d) for d in ds.get("test_snap_dates", [])]

    test_labels = label_table.filter(F.col(time_col).isin(test_dates))
    all_keys = test_labels.select(*identity_key).dropDuplicates()

    logger.info("Test keys (full population)")
    return all_keys


def fit_preprocessor_metadata(
    feature_table: DataFrame,
    train_keys: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Fit preprocessor on Spark feature_table restricted to train customer-months.

    Only collects small metadata (distinct category values) to driver.
    """
    return _fit_preprocessor_metadata(feature_table, train_keys, parameters)


def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table once for all splits."""
    return _apply_preprocessor_to_features(feature_table, preprocessor_metadata, parameters)


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Merge Spark keys + labels + encoded features into model_input for a split."""
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )

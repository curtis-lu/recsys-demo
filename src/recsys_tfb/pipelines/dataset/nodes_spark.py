"""PySpark implementations for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def select_sample_keys(label_table: DataFrame, parameters: dict) -> DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    sample_ratio = parameters["dataset"]["sample_ratio"]
    seed = parameters.get("random_seed", 42)
    group_keys = parameters["dataset"].get("sample_group_keys", [time_col])

    # Extract unique identity keys with group columns
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = label_table.select(*extract_cols).dropDuplicates(identity_key)

    if sample_ratio >= 1.0:
        sampled = keys.select(*identity_key)
        total = sampled.count()
        logger.info("Sampled %d keys (ratio=1.0, no sampling)", total)
        return sampled

    # Stratified sampling via Window functions
    w = Window.partitionBy(*group_keys).orderBy(F.rand(seed))
    keys_ranked = keys.withColumn("_rn", F.row_number().over(w))
    keys_counted = keys_ranked.withColumn(
        "_cnt", F.count("*").over(Window.partitionBy(*group_keys))
    )
    sampled = keys_counted.filter(
        F.col("_rn") <= F.round(F.col("_cnt") * F.lit(sample_ratio))
    ).select(*identity_key)

    total_before = keys.count()
    total_after = sampled.count()
    logger.info(
        "Sampled %d keys from %d (ratio=%.2f, group_keys=%s)",
        total_after,
        total_before,
        sample_ratio,
        group_keys,
    )
    return sampled


def split_keys(
    sample_keys: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split keys into train (in-time sampled), train_dev (out-of-time sampled), val (out-of-time full)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    train_dev_dates = [
        pd.Timestamp(d) for d in parameters["dataset"]["train_dev_snap_dates"]
    ]
    val_dates = [pd.Timestamp(d) for d in parameters["dataset"]["val_snap_dates"]]
    excluded_dates = train_dev_dates + val_dates

    # Train: sampled keys not in train_dev or val dates
    train_keys = sample_keys.filter(~F.col(time_col).isin(excluded_dates))

    # Train-dev: sampled keys in train_dev dates
    train_dev_keys = sample_keys.filter(F.col(time_col).isin(train_dev_dates))

    # Val: full (unsampled) population for val dates
    all_keys = label_table.select(*identity_key).dropDuplicates()
    val_keys = all_keys.filter(F.col(time_col).isin(val_dates))

    logger.info(
        "Split: train=%d, train_dev=%d (sampled), val=%d (full)",
        train_keys.count(),
        train_dev_keys.count(),
        val_keys.count(),
    )
    return train_keys, train_dev_keys, val_keys


def build_dataset(
    keys: DataFrame,
    feature_table: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Join keys with labels and features to build a complete dataset."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    join_key = [time_col] + entity_cols

    # First join keys with label_table to get all product rows for sampled customers
    dataset = keys.join(label_table, on=join_key, how="inner")

    # Then join with features
    dataset = dataset.join(feature_table, on=join_key, how="left")

    logger.info(
        "Built dataset: %d rows, %d columns", dataset.count(), len(dataset.columns)
    )
    return dataset


def prepare_model_input(
    train_set: DataFrame,
    train_dev_set: DataFrame,
    val_set: DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, dict]:
    """Convert Spark DataFrames to model-ready pandas DataFrames with categorical encoding."""
    schema = get_schema(parameters)
    label_col = schema["label"]

    # Convert to pandas — data has been sampled and split, should fit in memory
    train_pdf = train_set.toPandas()
    train_dev_pdf = train_dev_set.toPandas()
    val_pdf = val_set.toPandas()

    pmi_config = parameters.get("dataset", {}).get("prepare_model_input", {})
    drop_cols = pmi_config.get("drop_columns", [
        schema["time"], *schema["entity"], label_col,
        "apply_start_date", "apply_end_date", "cust_segment_typ",
    ])
    categorical_cols = pmi_config.get("categorical_columns", [schema["item"]])

    # Build category mapping from train set only
    category_mappings = {}
    for col in categorical_cols:
        cat = pd.CategoricalDtype(categories=sorted(train_pdf[col].unique()))
        category_mappings[col] = list(cat.categories)

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop(columns=drop_cols, errors="ignore").copy()
        for col in categorical_cols:
            known = category_mappings[col]
            result[col] = pd.Categorical(result[col], categories=known).codes
        return result

    X_train = _transform(train_pdf)
    y_train = train_pdf[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_pdf)
    y_train_dev = train_dev_pdf[[label_col]].reset_index(drop=True)
    X_val = _transform(val_pdf)
    y_val = val_pdf[[label_col]].reset_index(drop=True)

    feature_columns = list(X_train.columns)

    preprocessor = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Model input: X_train=%s, X_train_dev=%s, X_val=%s, features=%d",
        X_train.shape,
        X_train_dev.shape,
        X_val.shape,
        len(feature_columns),
    )
    return X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, category_mappings

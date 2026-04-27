"""Pure functions for the dataset building pipeline (pandas backend)."""

import logging

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset._hashing import pandas_bucket, ratio_to_threshold
from recsys_tfb.pipelines.dataset.helpers_pandas import select_keys
from recsys_tfb.pipelines.dataset.nodes_shared import validate_date_splits
from recsys_tfb.preprocessing._pandas import (
    apply_preprocessor_to_features as _apply_preprocessor_to_features,
    build_model_input as _build_model_input,
    fit_preprocessor_metadata as _fit_preprocessor_metadata,
)

logger = logging.getLogger(__name__)



def select_train_keys(sample_pool: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Select train identity keys using date range from parameters."""
    validate_date_splits(parameters)

    ds = parameters["dataset"]
    time_col = get_schema(parameters)["time"]
    start = pd.Timestamp(ds["train_snap_date_start"])
    end = pd.Timestamp(ds["train_snap_date_end"])

    # Get unique snap_dates within the train range from sample_pool
    all_dates = sample_pool[time_col].unique()
    train_dates = [d for d in all_dates if start <= pd.Timestamp(d) <= end]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(
        sample_pool, parameters, train_dates, ds["sample_ratio"], overrides,
        site="sample_keys",
    )


def select_calibration_keys(sample_pool: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(
        sample_pool, parameters, cal_dates, cal_ratio, cal_overrides,
        site="calibration_keys",
    )


def split_train_keys(
    sample_keys: pd.DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split sampled keys into train and train-dev by cust_id ratio.

    All rows for a given cust_id are assigned to the same split.
    """
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    # Use first entity col (cust_id) for splitting
    cust_col = entity_cols[0]

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Deterministic per-cust_id bucket; shared with the Spark backend.
    unique_custs_df = pd.DataFrame({cust_col: sample_keys[cust_col].unique()})
    unique_custs_df["_bucket"] = pandas_bucket(
        unique_custs_df, [cust_col], seed, site="split_train_dev",
    )
    threshold = ratio_to_threshold(train_dev_ratio)
    dev_custs = set(unique_custs_df.loc[unique_custs_df["_bucket"] < threshold, cust_col])

    train_dev_mask = sample_keys[cust_col].isin(dev_custs)
    train_keys = sample_keys[~train_dev_mask].reset_index(drop=True)
    train_dev_keys = sample_keys[train_dev_mask].reset_index(drop=True)

    logger.info(
        "Split train keys: train=%d, train_dev=%d (ratio=%.2f, %d/%d cust_ids)",
        len(train_keys),
        len(train_dev_keys),
        train_dev_ratio,
        len(dev_custs),
        len(unique_custs_df),
    )
    return train_keys, train_dev_keys


def select_val_keys(
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Select validation identity keys (full population, optional random cust_id sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols
    cust_col = entity_cols[0]

    ds = parameters["dataset"]
    val_dates = set(pd.to_datetime(ds.get("val_snap_dates", [])))
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    # Filter to val dates and get unique identity keys
    val_labels = label_table[label_table[time_col].isin(val_dates)]
    all_keys = val_labels[identity_key].drop_duplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys: %d (full population)", len(all_keys))
        return all_keys.reset_index(drop=True)

    # Deterministic cust_id sampling; shared with the Spark backend.
    unique_custs_df = pd.DataFrame({cust_col: all_keys[cust_col].unique()})
    unique_custs_df["_bucket"] = pandas_bucket(
        unique_custs_df, [cust_col], seed, site="val_keys",
    )
    threshold = ratio_to_threshold(val_sample_ratio)
    sampled_custs = set(unique_custs_df.loc[unique_custs_df["_bucket"] < threshold, cust_col])

    sampled = all_keys[all_keys[cust_col].isin(sampled_custs)].reset_index(drop=True)
    logger.info(
        "Val keys: %d from %d (ratio=%.2f, %d/%d cust_ids)",
        len(sampled),
        len(all_keys),
        val_sample_ratio,
        len(sampled_custs),
        len(unique_custs_df),
    )
    return sampled


def select_test_keys(
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Select test identity keys (full population, no sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    ds = parameters["dataset"]
    test_dates = set(pd.to_datetime(ds.get("test_snap_dates", [])))

    test_labels = label_table[label_table[time_col].isin(test_dates)]
    all_keys = test_labels[identity_key].drop_duplicates().reset_index(drop=True)

    logger.info("Test keys: %d (full population)", len(all_keys))
    return all_keys


def fit_preprocessor_metadata(
    feature_table: pd.DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Fit preprocessor at customer-month granularity, decoupled from sampling."""
    return _fit_preprocessor_metadata(feature_table, parameters)


def apply_preprocessor_to_features(
    feature_table: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Encode non-identity categoricals in feature_table once for all splits."""
    return _apply_preprocessor_to_features(feature_table, preprocessor_metadata, parameters)


def build_model_input(
    keys: pd.DataFrame,
    preprocessed_feature_table: pd.DataFrame,
    label_table: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Merge keys + labels + encoded features into model_input for a split."""
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )

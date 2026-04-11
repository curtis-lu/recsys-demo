"""Pure functions for the dataset building pipeline (pandas backend)."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.helpers_pandas import select_keys
from recsys_tfb.pipelines.dataset.nodes_shared import validate_date_splits
from recsys_tfb.pipelines.preprocessing import (
    fit_preprocessor_metadata_pandas,
    transform_to_model_input_pandas,
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
    return select_keys(sample_pool, parameters, train_dates, ds["sample_ratio"], overrides)


def select_calibration_keys(sample_pool: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(sample_pool, parameters, cal_dates, cal_ratio, cal_overrides)


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

    # Get unique cust_ids and split
    unique_custs = sample_keys[cust_col].unique()
    rng = np.random.RandomState(seed)
    rng.shuffle(unique_custs)
    n_dev = max(1, int(len(unique_custs) * train_dev_ratio))
    dev_custs = set(unique_custs[:n_dev])

    train_dev_mask = sample_keys[cust_col].isin(dev_custs)
    train_keys = sample_keys[~train_dev_mask].reset_index(drop=True)
    train_dev_keys = sample_keys[train_dev_mask].reset_index(drop=True)

    logger.info(
        "Split train keys: train=%d, train_dev=%d (ratio=%.2f, %d/%d cust_ids)",
        len(train_keys),
        len(train_dev_keys),
        train_dev_ratio,
        len(dev_custs),
        len(unique_custs),
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

    # Pure random cust_id sampling (not stratified)
    unique_custs = all_keys[cust_col].unique()
    rng = np.random.RandomState(seed)
    n_sample = max(1, int(len(unique_custs) * val_sample_ratio))
    sampled_custs = set(rng.choice(unique_custs, size=n_sample, replace=False))

    sampled = all_keys[all_keys[cust_col].isin(sampled_custs)].reset_index(drop=True)
    logger.info(
        "Val keys: %d from %d (ratio=%.2f, %d/%d cust_ids)",
        len(sampled),
        len(all_keys),
        val_sample_ratio,
        n_sample,
        len(unique_custs),
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


def build_dataset(
    keys: pd.DataFrame,
    feature_table: pd.DataFrame,
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Join keys with labels and features to build a complete dataset.

    Dynamically determines the label_table join key based on whether keys
    contains the item column (prod_name). Feature_table join always uses
    (time_col + entity_cols).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    base_key = [time_col] + entity_cols

    # Dynamic label join key: include item_col if present in keys
    label_join_key = base_key + [item_col] if item_col in keys.columns else base_key

    # Join keys with label_table
    dataset = keys.merge(label_table, on=label_join_key, how="inner")

    # Join with features on base key (snap_date, cust_id)
    dataset = dataset.merge(feature_table, on=base_key, how="left")

    logger.info("Built dataset: %d rows, %d columns", len(dataset), len(dataset.columns))
    return dataset


def fit_preprocessor_metadata(
    train_set: pd.DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata and category mappings from train_set only."""
    return fit_preprocessor_metadata_pandas(train_set, parameters)


def transform_to_model_input(
    split_set: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Transform a single split to model_input (identity + label + encoded features)."""
    return transform_to_model_input_pandas(split_set, preprocessor_metadata, parameters)

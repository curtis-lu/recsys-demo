"""Pure functions for the dataset building pipeline (pandas backend)."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.preprocessing import (
    fit_preprocessor_metadata_pandas,
    transform_to_model_input_pandas,
)

logger = logging.getLogger(__name__)


def _validate_date_splits(parameters: dict) -> None:
    """Validate that train, calibration, val, and test snap_dates are mutually non-overlapping."""
    ds = parameters.get("dataset", {})

    # Build train date set from start/end range
    train_start = ds.get("train_snap_date_start")
    train_end = ds.get("train_snap_date_end")
    if train_start and train_end:
        train_start_ts = pd.Timestamp(train_start)
        train_end_ts = pd.Timestamp(train_end)
        if train_start_ts > train_end_ts:
            raise ValueError(
                f"train_snap_date_start ({train_start}) > train_snap_date_end ({train_end})"
            )

    calibration_dates = set(str(d) for d in ds.get("calibration_snap_dates", []))
    val_dates = set(str(d) for d in ds.get("val_snap_dates", []))
    test_dates = set(str(d) for d in ds.get("test_snap_dates", []))

    overlaps = []
    cal_val = calibration_dates & val_dates
    if cal_val:
        overlaps.append(f"calibration & val: {sorted(cal_val)}")
    cal_test = calibration_dates & test_dates
    if cal_test:
        overlaps.append(f"calibration & test: {sorted(cal_test)}")
    val_test = val_dates & test_dates
    if val_test:
        overlaps.append(f"val & test: {sorted(val_test)}")

    # Validate train range doesn't overlap with cal/val/test
    if train_start and train_end:
        train_start_ts = pd.Timestamp(train_start)
        train_end_ts = pd.Timestamp(train_end)
        for name, date_set in [("calibration", calibration_dates), ("val", val_dates), ("test", test_dates)]:
            for d in date_set:
                d_ts = pd.Timestamp(d)
                if train_start_ts <= d_ts <= train_end_ts:
                    overlaps.append(f"train & {name}: [{d}]")

    if overlaps:
        raise ValueError(f"Date splits overlap: {'; '.join(overlaps)}")


def _compute_effective_ratio(
    row_group_key: str,
    sample_ratio: float,
    sample_ratio_overrides: dict,
) -> float:
    """Look up effective sampling ratio for a group key, falling back to default."""
    return sample_ratio_overrides.get(row_group_key, sample_ratio)


def select_keys(
    sample_pool: pd.DataFrame,
    parameters: dict,
    snap_dates: list,
    sample_ratio: float,
    sample_ratio_overrides: dict | None = None,
) -> pd.DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys.

    Filters sample_pool to the given snap_dates and applies stratified sampling
    with per-group ratio overrides. Identity key is (snap_date, cust_id, prod_name).

    Args:
        sample_pool: Full sample pool at customer-month-product granularity.
        parameters: Full parameters dict.
        snap_dates: List of snap_dates to filter to.
        sample_ratio: Default sampling ratio for this split.
        sample_ratio_overrides: Per-group ratio overrides. If None, falls back to
            parameters["dataset"]["sample_ratio_overrides"].
    """
    schema = get_schema(parameters)
    identity_key = schema["identity_columns"]  # [snap_date, cust_id, prod_name]
    time_col = schema["time"]

    ds = parameters["dataset"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    if sample_ratio_overrides is None:
        sample_ratio_overrides = ds.get("sample_ratio_overrides", {})

    # Filter to specified snap_dates
    target_dates = set(pd.to_datetime(snap_dates))
    pool = sample_pool[sample_pool[time_col].isin(target_dates)]

    # Extract group keys + identity keys, dedup on identity
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = pool[extract_cols].drop_duplicates(subset=identity_key)

    if sample_ratio >= 1.0 and not sample_ratio_overrides:
        sampled = keys[identity_key].reset_index(drop=True)
        logger.info("Sampled %d keys (ratio=1.0, no sampling)", len(sampled))
        return sampled

    # Compute effective ratio per row via overrides
    rng = np.random.RandomState(seed)

    def _serialize_group_key(row):
        return "|".join(str(row[k]) for k in group_keys)

    keys = keys.copy()
    keys["_group_key"] = keys.apply(_serialize_group_key, axis=1)
    keys["_effective_ratio"] = keys["_group_key"].map(
        lambda gk: _compute_effective_ratio(gk, sample_ratio, sample_ratio_overrides)
    )
    keys["_rand"] = rng.random(len(keys))
    sampled = keys[keys["_rand"] < keys["_effective_ratio"]][identity_key].reset_index(drop=True)

    logger.info(
        "Sampled %d keys from %d (ratio=%.2f, group_keys=%s, overrides=%s)",
        len(sampled),
        len(keys),
        sample_ratio,
        group_keys,
        sample_ratio_overrides,
    )
    return sampled


def select_train_keys(sample_pool: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Select train identity keys using date range from parameters."""
    _validate_date_splits(parameters)

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

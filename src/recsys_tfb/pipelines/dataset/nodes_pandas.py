"""Pure functions for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def _validate_date_splits(parameters: dict) -> None:
    """Validate that calibration, val, and test snap_dates are mutually non-overlapping."""
    ds = parameters.get("dataset", {})
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

    if overlaps:
        raise ValueError(f"Date splits overlap: {'; '.join(overlaps)}")


def _compute_effective_ratio(
    row_group_key: str,
    sample_ratio: float,
    sample_ratio_overrides: dict,
) -> float:
    """Look up effective sampling ratio for a group key, falling back to default."""
    return sample_ratio_overrides.get(row_group_key, sample_ratio)


def select_sample_keys(sample_pool: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys.

    Filters to train dates (excludes calibration/val/test dates) and supports
    sample_ratio_overrides for per-group custom ratios.
    """
    _validate_date_splits(parameters)

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    ds = parameters["dataset"]
    sample_ratio = ds["sample_ratio"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    sample_ratio_overrides = ds.get("sample_ratio_overrides", {})

    # Filter to train dates (exclude calibration/val/test)
    excluded_dates = set()
    for key in ("calibration_snap_dates", "val_snap_dates", "test_snap_dates"):
        excluded_dates.update(pd.to_datetime(ds.get(key, [])))
    pool = sample_pool[~sample_pool[time_col].isin(excluded_dates)]

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


def select_calibration_keys(
    sample_pool: pd.DataFrame,
    label_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Select calibration identity keys with stratified sampling."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    ds = parameters["dataset"]
    calibration_dates = set(pd.to_datetime(ds.get("calibration_snap_dates", [])))
    calibration_ratio = ds.get("calibration_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    sample_ratio_overrides = ds.get("sample_ratio_overrides", {})

    # Filter label_table to calibration dates, get unique identity keys
    cal_labels = label_table[label_table[time_col].isin(calibration_dates)]
    all_keys = cal_labels[identity_key].drop_duplicates()

    if calibration_ratio >= 1.0 and not sample_ratio_overrides:
        logger.info("Calibration keys: %d (full population)", len(all_keys))
        return all_keys.reset_index(drop=True)

    # Need group keys from sample_pool for stratified sampling
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    # Get group info from sample_pool (which has segment cols)
    pool_filtered = sample_pool[sample_pool[time_col].isin(calibration_dates)]
    keys_with_groups = pool_filtered[extract_cols].drop_duplicates(subset=identity_key)

    # Merge to get group info for all identity keys
    keys = all_keys.merge(keys_with_groups, on=identity_key, how="left")

    rng = np.random.RandomState(seed)

    def _serialize_group_key(row):
        return "|".join(str(row[k]) for k in group_keys)

    keys = keys.copy()
    keys["_group_key"] = keys.apply(_serialize_group_key, axis=1)
    keys["_effective_ratio"] = keys["_group_key"].map(
        lambda gk: _compute_effective_ratio(gk, calibration_ratio, sample_ratio_overrides)
    )
    keys["_rand"] = rng.random(len(keys))
    sampled = keys[keys["_rand"] < keys["_effective_ratio"]][identity_key].reset_index(drop=True)

    logger.info(
        "Calibration keys: %d from %d (ratio=%.2f)",
        len(sampled),
        len(keys),
        calibration_ratio,
    )
    return sampled


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
    """Join keys with labels and features to build a complete dataset."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    join_key = [time_col] + entity_cols

    # First join keys with label_table to get all product rows for sampled customers
    dataset = keys.merge(label_table, on=join_key, how="inner")

    # Then join with features
    dataset = dataset.merge(
        feature_table, on=join_key, how="left"
    )

    logger.info("Built dataset: %d rows, %d columns", len(dataset), len(dataset.columns))
    return dataset


def _prepare_transform(train_set: pd.DataFrame, parameters: dict):
    """Build category_mappings and _transform helper from train_set. Returns (preprocessor, category_mappings, _transform)."""
    schema = get_schema(parameters)
    label_col = schema["label"]

    pmi_config = parameters.get("dataset", {}).get("prepare_model_input", {})
    drop_cols = pmi_config.get("drop_columns", [
        schema["time"], *schema["entity"], label_col,
        "apply_start_date", "apply_end_date", "cust_segment_typ",
    ])
    categorical_cols = pmi_config.get("categorical_columns", [schema["item"]])

    # Build category mapping from train set only
    category_mappings = {}
    for col in categorical_cols:
        cat = pd.CategoricalDtype(categories=sorted(train_set[col].unique()))
        category_mappings[col] = list(cat.categories)

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop(columns=drop_cols, errors="ignore").copy()
        for col in categorical_cols:
            known = category_mappings[col]
            result[col] = pd.Categorical(result[col], categories=known).codes
        return result

    feature_columns = list(_transform(train_set).columns)

    preprocessor = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    return preprocessor, category_mappings, _transform


def prepare_model_input(
    train_set: pd.DataFrame,
    train_dev_set: pd.DataFrame,
    val_set: pd.DataFrame,
    test_set: pd.DataFrame,
    parameters: dict,
) -> tuple:
    """Convert 4 DataFrames to model-ready arrays (without calibration).

    Returns: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val,
             X_test, y_test, preprocessor, category_mappings (10 outputs).
    """
    schema = get_schema(parameters)
    label_col = schema["label"]

    preprocessor, category_mappings, _transform = _prepare_transform(train_set, parameters)

    X_train = _transform(train_set)
    y_train = train_set[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_set)
    y_train_dev = train_dev_set[[label_col]].reset_index(drop=True)
    X_val = _transform(val_set)
    y_val = val_set[[label_col]].reset_index(drop=True)
    X_test = _transform(test_set)
    y_test = test_set[[label_col]].reset_index(drop=True)

    logger.info(
        "Model input: X_train=%s, X_train_dev=%s, X_val=%s, X_test=%s, features=%d",
        X_train.shape,
        X_train_dev.shape,
        X_val.shape,
        X_test.shape,
        len(preprocessor["feature_columns"]),
    )
    return (
        X_train, y_train, X_train_dev, y_train_dev,
        X_val, y_val, X_test, y_test,
        preprocessor, category_mappings,
    )


def prepare_model_input_with_calibration(
    train_set: pd.DataFrame,
    train_dev_set: pd.DataFrame,
    calibration_set: pd.DataFrame,
    val_set: pd.DataFrame,
    test_set: pd.DataFrame,
    parameters: dict,
) -> tuple:
    """Convert 5 DataFrames to model-ready arrays (with calibration).

    Returns: X_train, y_train, X_train_dev, y_train_dev,
             X_calibration, y_calibration, X_val, y_val,
             X_test, y_test, preprocessor, category_mappings (12 outputs).
    """
    schema = get_schema(parameters)
    label_col = schema["label"]

    preprocessor, category_mappings, _transform = _prepare_transform(train_set, parameters)

    X_train = _transform(train_set)
    y_train = train_set[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_set)
    y_train_dev = train_dev_set[[label_col]].reset_index(drop=True)
    X_calibration = _transform(calibration_set)
    y_calibration = calibration_set[[label_col]].reset_index(drop=True)
    X_val = _transform(val_set)
    y_val = val_set[[label_col]].reset_index(drop=True)
    X_test = _transform(test_set)
    y_test = test_set[[label_col]].reset_index(drop=True)

    logger.info(
        "Model input (with calibration): X_train=%s, X_train_dev=%s, X_cal=%s, X_val=%s, X_test=%s, features=%d",
        X_train.shape,
        X_train_dev.shape,
        X_calibration.shape,
        X_val.shape,
        X_test.shape,
        len(preprocessor["feature_columns"]),
    )
    return (
        X_train, y_train, X_train_dev, y_train_dev,
        X_calibration, y_calibration, X_val, y_val,
        X_test, y_test, preprocessor, category_mappings,
    )

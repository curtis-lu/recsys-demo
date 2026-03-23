"""Pure functions for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def select_sample_keys(label_table: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    sample_ratio = parameters["dataset"]["sample_ratio"]
    seed = parameters.get("random_seed", 42)
    group_keys = parameters["dataset"].get("sample_group_keys", [time_col])

    # Extract group keys + identity keys, dedup on identity
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = label_table[extract_cols].drop_duplicates(subset=identity_key)

    sampled = keys.groupby(group_keys, group_keys=False).sample(
        frac=sample_ratio, random_state=seed
    )
    sampled = sampled[identity_key].reset_index(drop=True)

    logger.info(
        "Sampled %d keys from %d (ratio=%.2f, group_keys=%s)",
        len(sampled),
        len(keys),
        sample_ratio,
        group_keys,
    )
    return sampled


def split_keys(
    sample_keys: pd.DataFrame,
    label_table: pd.DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split keys into train (in-time sampled), train_dev (out-of-time sampled), val (out-of-time full)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    train_dev_dates = set(pd.to_datetime(parameters["dataset"]["train_dev_snap_dates"]))
    val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))

    # Train: sampled keys not in train_dev or val dates
    excluded_dates = train_dev_dates | val_dates
    train_mask = ~sample_keys[time_col].isin(excluded_dates)
    train_keys = sample_keys[train_mask].reset_index(drop=True)

    # Train-dev: sampled keys in train_dev dates
    train_dev_mask = sample_keys[time_col].isin(train_dev_dates)
    train_dev_keys = sample_keys[train_dev_mask].reset_index(drop=True)

    # Val: full (unsampled) population for val dates
    all_keys = label_table[identity_key].drop_duplicates()
    val_keys = all_keys[all_keys[time_col].isin(val_dates)].reset_index(drop=True)

    logger.info(
        "Split: train=%d, train_dev=%d (sampled), val=%d (full)",
        len(train_keys),
        len(train_dev_keys),
        len(val_keys),
    )
    return train_keys, train_dev_keys, val_keys


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


def prepare_model_input(
    train_set: pd.DataFrame,
    train_dev_set: pd.DataFrame,
    val_set: pd.DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, dict]:
    """Convert DataFrames to model-ready arrays with categorical encoding."""
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

    X_train = _transform(train_set)
    y_train = train_set[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_set)
    y_train_dev = train_dev_set[[label_col]].reset_index(drop=True)
    X_val = _transform(val_set)
    y_val = val_set[[label_col]].reset_index(drop=True)

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

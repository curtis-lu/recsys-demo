"""Pure functions for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def select_sample_keys(label_table: pd.DataFrame, parameters: dict) -> pd.DataFrame:
    """Stratified sampling by snap_date, returning unique (snap_date, cust_id) keys."""
    sample_ratio = parameters["dataset"]["sample_ratio"]
    seed = parameters.get("random_seed", 42)

    keys = label_table[["snap_date", "cust_id"]].drop_duplicates()

    sampled = keys.groupby("snap_date", group_keys=False).sample(
        frac=sample_ratio, random_state=seed
    )
    sampled = sampled.reset_index(drop=True)

    logger.info(
        "Sampled %d keys from %d (ratio=%.2f)",
        len(sampled),
        len(keys),
        sample_ratio,
    )
    return sampled


def split_keys(
    sample_keys: pd.DataFrame, parameters: dict
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split keys by temporal boundary into train and validation sets."""
    val_snap_dates = parameters["dataset"]["val_snap_dates"]
    val_dates = set(pd.to_datetime(val_snap_dates))

    mask = sample_keys["snap_date"].isin(val_dates)
    train_keys = sample_keys[~mask].reset_index(drop=True)
    val_keys = sample_keys[mask].reset_index(drop=True)

    logger.info(
        "Split: train=%d keys, val=%d keys", len(train_keys), len(val_keys)
    )
    return train_keys, val_keys


def build_dataset(
    keys: pd.DataFrame,
    feature_table: pd.DataFrame,
    label_table: pd.DataFrame,
) -> pd.DataFrame:
    """Join keys with labels and features to build a complete dataset."""
    # First join keys with label_table to get all product rows for sampled customers
    dataset = keys.merge(label_table, on=["snap_date", "cust_id"], how="inner")

    # Then join with features
    dataset = dataset.merge(
        feature_table, on=["snap_date", "cust_id"], how="left"
    )

    logger.info("Built dataset: %d rows, %d columns", len(dataset), len(dataset.columns))
    return dataset


def prepare_model_input(
    train_set: pd.DataFrame,
    val_set: pd.DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, np.ndarray, pd.DataFrame, np.ndarray, dict]:
    """Convert DataFrames to model-ready arrays with categorical encoding."""
    drop_cols = ["snap_date", "cust_id", "label", "apply_start_date", "apply_end_date"]
    categorical_cols = ["prod_name"]

    # Build category mapping from train set
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
    y_train = train_set["label"].values
    X_val = _transform(val_set)
    y_val = val_set["label"].values

    feature_columns = list(X_train.columns)

    preprocessor = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Model input: X_train=%s, X_val=%s, features=%d",
        X_train.shape,
        X_val.shape,
        len(feature_columns),
    )
    return X_train, y_train, X_val, y_val, preprocessor

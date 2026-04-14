"""Pandas backend for preprocessing: fit, transform, and apply."""

from __future__ import annotations

import logging

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.preprocessing._common import (
    _get_preprocessing_config,
    _validate_columns,
    _warn_missing_drop_columns,
)

logger = logging.getLogger(__name__)


def _encode_categoricals(
    df: pd.DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> pd.DataFrame:
    """Encode categorical columns to integer codes. Unknown values -> -1."""
    for col in categorical_cols:
        known = category_mappings[col]
        df[col] = pd.Categorical(df[col], categories=known).codes
    return df


def fit_preprocessor_metadata(
    train_set: pd.DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata and category mappings from train_set.

    Only small metadata is extracted; no large data copies are made.

    Returns:
        (preprocessor_metadata, category_mappings)
    """
    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    # Validate categorical columns exist in train_set
    _validate_columns(train_set.columns.tolist(), categorical_cols, "train_set (categorical)")

    # Build category mapping from train set only
    category_mappings = {}
    for col in categorical_cols:
        category_mappings[col] = sorted(train_set[col].dropna().unique().tolist())

    # Determine feature columns: all columns except drop_cols, label, and
    # identity columns that are NOT categorical features.
    # Categorical identity columns (e.g., prod_name) ARE features.
    all_cols = train_set.columns.tolist()
    non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
    feature_columns = [c for c in all_cols if c not in non_feature]

    preprocessor_metadata = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Fit preprocessor: %d features, %d categorical, %d drop",
        len(feature_columns), len(categorical_cols), len(drop_cols),
    )
    return preprocessor_metadata, category_mappings


def transform_to_model_input(
    split_set: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Transform a split dataset into model_input (identity + label + features).

    The output contains identity columns, label column, and feature columns.
    Categorical features that are NOT identity columns are encoded to int codes.
    Categorical features that ARE identity columns (e.g., prod_name) are kept
    as original values — encoding is deferred to the training pipeline's
    _extract_Xy so that evaluate_model can use original values for metrics.
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    # Validate required columns
    required = list(set(identity_cols + [label_col] + feature_columns))
    _validate_columns(split_set.columns.tolist(), required, "split_set")
    _warn_missing_drop_columns(split_set.columns.tolist(), drop_cols, "split_set")

    # Select only the columns we need: identity + label + features
    # Use dict.fromkeys to deduplicate while preserving order
    keep_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
    result = split_set[keep_cols].copy()

    # Encode categoricals EXCEPT identity columns (deferred to training)
    encode_cols = [c for c in categorical_cols if c not in identity_cols]
    if encode_cols:
        result = _encode_categoricals(result, encode_cols, category_mappings)

        # Check for unknown categorical values (encoded as -1)
        for col in encode_cols:
            n_unknown = (result[col] == -1).sum()
            if n_unknown > 0:
                logger.warning(
                    "transform: %d unknown values in column '%s' (encoded as -1)",
                    n_unknown, col,
                )

    # Log stats
    n_label_null = result[label_col].isnull().sum()
    if n_label_null > 0:
        logger.warning("transform: %d null labels in split", n_label_null)

    logger.info(
        "Model input: %d rows, %d features, label_nulls=%d",
        len(result), len(feature_columns), n_label_null,
    )
    return result


def apply_preprocessor(
    scoring_dataset: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Apply preprocessor to inference scoring dataset.

    Returns only feature columns (no identity/label) for model prediction.
    """
    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    result = scoring_dataset.drop(columns=drop_cols, errors="ignore").copy()

    # Encode categoricals
    result = _encode_categoricals(result, categorical_cols, category_mappings)

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Select only feature columns in correct order
    result = result[feature_columns]

    logger.info("Preprocessed scoring data: %s", result.shape)
    return result

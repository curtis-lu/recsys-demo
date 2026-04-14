"""Pandas backend for preprocessing: fit, apply-to-features, build-model-input, apply."""

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


def _compute_feature_columns(
    feature_table_cols: list[str],
    identity_cols: list[str],
    categorical_cols: list[str],
    drop_cols: list[str],
    label_col: str,
) -> list[str]:
    """Compute feature_columns list preserving original post-join column order.

    Order: identity categoricals first (in identity_cols order),
    then feature_table columns minus drops / non-categorical identity / label.
    """
    non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
    feature_columns: list[str] = []
    for c in identity_cols:
        if c in categorical_cols and c not in feature_columns:
            feature_columns.append(c)
    for c in feature_table_cols:
        if c in non_feature or c in feature_columns:
            continue
        feature_columns.append(c)
    return feature_columns


def fit_preprocessor_metadata(
    feature_table: pd.DataFrame,
    train_keys: pd.DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata from feature_table restricted to train customer-months.

    Fit runs at customer-month granularity — *before* the customer-month-product
    fan-out done by ``build_model_input`` — so that future statistical transforms
    (mean/std/quantile) will not be distorted by product duplication.

    Categorical distinct values are collected from the correct source:
    - columns that live in ``feature_table``: from train-restricted feature rows
    - identity categoricals (e.g., ``prod_name``) that live only in ``train_keys``:
      from ``train_keys`` directly

    Returns:
        (preprocessor_metadata, category_mappings)
    """
    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    label_col = schema["label"]

    cm_key = [time_col] + entity_cols
    train_cm = train_keys[cm_key].drop_duplicates()
    train_features = feature_table.merge(train_cm, on=cm_key, how="inner")

    ft_cols = set(feature_table.columns)
    feature_cat_cols = [c for c in categorical_cols if c in ft_cols]
    identity_cat_cols = [c for c in categorical_cols if c not in ft_cols]
    missing_cats = [c for c in identity_cat_cols if c not in train_keys.columns]
    if missing_cats:
        raise ValueError(
            "Categorical columns not found in feature_table or train_keys: "
            f"{missing_cats}"
        )

    category_mappings: dict[str, list] = {}
    for col in feature_cat_cols:
        category_mappings[col] = sorted(train_features[col].dropna().unique().tolist())
    for col in identity_cat_cols:
        category_mappings[col] = sorted(train_keys[col].dropna().unique().tolist())

    feature_columns = _compute_feature_columns(
        feature_table.columns.tolist(),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )

    preprocessor_metadata = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Fit preprocessor: %d features, %d categorical, %d drop (train cust-months=%d)",
        len(feature_columns), len(categorical_cols), len(drop_cols), len(train_cm),
    )
    return preprocessor_metadata, category_mappings


def apply_preprocessor_to_features(
    feature_table: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Encode non-identity categoricals in feature_table at customer-month granularity.

    Returns (time + entity) + feature_columns that live in feature_table, with
    non-identity categoricals encoded to int codes. Identity categoricals
    (e.g., ``prod_name``) are not in feature_table and stay raw until
    ``build_model_input`` / training.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    base_key = [time_col] + entity_cols
    ft_feature_cols = [c for c in feature_columns if c in feature_table.columns]
    keep_cols = list(dict.fromkeys(base_key + ft_feature_cols))
    missing_base = [c for c in base_key if c not in feature_table.columns]
    if missing_base:
        raise ValueError(f"feature_table missing base-key columns: {missing_base}")
    _warn_missing_drop_columns(feature_table.columns.tolist(), drop_cols, "feature_table")

    result = feature_table[keep_cols].copy()

    encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
    if encode_cols:
        result = _encode_categoricals(result, encode_cols, category_mappings)
        for col in encode_cols:
            n_unknown = (result[col] == -1).sum()
            if n_unknown > 0:
                logger.warning(
                    "apply_preprocessor_to_features: %d unknowns in column '%s'",
                    n_unknown, col,
                )

    logger.info(
        "Preprocessed feature_table: %d rows, %d cols (encoded=%d)",
        len(result), len(result.columns), len(encode_cols),
    )
    return result


def build_model_input(
    keys: pd.DataFrame,
    preprocessed_feature_table: pd.DataFrame,
    label_table: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Merge keys, labels, and pre-encoded features into model_input.

    Equivalent to the old (build_dataset + transform_to_model_input) pair, but
    sources feature values from the already-encoded ``preprocessed_feature_table``
    so encoding work is not duplicated across splits.

    Identity-categorical columns (e.g., ``prod_name``) stay raw here — encoding
    is deferred to the training pipeline's ``_extract_Xy`` so that evaluation
    can use original values for metrics.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    base_key = [time_col] + entity_cols

    feature_columns = preprocessor_metadata["feature_columns"]

    label_join_key = base_key + [item_col] if item_col in keys.columns else base_key
    dataset = keys.merge(label_table, on=label_join_key, how="inner")
    dataset = dataset.merge(preprocessed_feature_table, on=base_key, how="left")

    required = list(set(identity_cols + [label_col] + feature_columns))
    _validate_columns(dataset.columns.tolist(), required, "build_model_input")

    keep_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
    result = dataset[keep_cols].copy()

    n_label_null = result[label_col].isnull().sum()
    if n_label_null > 0:
        logger.warning("build_model_input: %d null labels", n_label_null)

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

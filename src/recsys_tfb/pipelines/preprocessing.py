"""Shared preprocessing logic for dataset, training, and inference pipelines.

Provides fit/transform/apply functions for both pandas and Spark backends.
The fit step builds preprocessor metadata from train_set only (no leakage).
The transform step applies encoding to any split, producing model_input.
The apply step is inference-specific (feature columns only, no label/identity).
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_preprocessing_config(parameters: dict) -> tuple[list[str], list[str]]:
    """Extract drop_columns and categorical_columns from parameters.

    Returns:
        (drop_columns, categorical_columns)
    """
    schema = get_schema(parameters)
    pmi_config = parameters.get("dataset", {}).get("prepare_model_input", {})

    drop_cols = pmi_config.get("drop_columns", [
        schema["time"], *schema["entity"], schema["label"],
        "apply_start_date", "apply_end_date", "cust_segment_typ",
    ])
    categorical_cols = pmi_config.get("categorical_columns", [schema["item"]])

    return drop_cols, categorical_cols


def _encode_categoricals_pandas(
    df: pd.DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> pd.DataFrame:
    """Encode categorical columns to integer codes. Unknown values -> -1."""
    for col in categorical_cols:
        known = category_mappings[col]
        df[col] = pd.Categorical(df[col], categories=known).codes
    return df


def _validate_columns(
    columns: list[str],
    required: list[str],
    context: str,
) -> None:
    """Check that all required columns exist. Raises ValueError if missing."""
    missing = set(required) - set(columns)
    if missing:
        raise ValueError(f"Missing columns in {context}: {sorted(missing)}")


def _warn_missing_drop_columns(
    columns: list[str],
    drop_cols: list[str],
    context: str,
) -> None:
    """Log warning for drop_columns that don't exist in the DataFrame."""
    missing = [c for c in drop_cols if c not in columns]
    if missing:
        logger.warning(
            "drop_columns not found in %s (will be ignored): %s",
            context, missing,
        )


# ---------------------------------------------------------------------------
# pandas backend
# ---------------------------------------------------------------------------

def fit_preprocessor_metadata_pandas(
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


def transform_to_model_input_pandas(
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
        result = _encode_categoricals_pandas(result, encode_cols, category_mappings)

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


def apply_preprocessor_pandas(
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
    result = _encode_categoricals_pandas(result, categorical_cols, category_mappings)

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Select only feature columns in correct order
    result = result[feature_columns]

    logger.info("Preprocessed scoring data: %s", result.shape)
    return result


# ---------------------------------------------------------------------------
# Spark backend
# ---------------------------------------------------------------------------

def fit_preprocessor_metadata_spark(
    train_set: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata from Spark train_set.

    Only collects small metadata (distinct category values) to driver.
    No toPandas() on the full dataset.

    Returns:
        (preprocessor_metadata, category_mappings)
    """
    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    # Validate categorical columns exist
    _validate_columns(train_set.columns, categorical_cols, "train_set (categorical)")

    # Collect distinct category values per column (small metadata)
    category_mappings = {}
    for col in categorical_cols:
        distinct_rows = (
            train_set.select(col)
            .filter(F.col(col).isNotNull())
            .distinct()
            .orderBy(col)
            .collect()
        )
        category_mappings[col] = [row[col] for row in distinct_rows]

    # Determine feature columns: categorical identity columns ARE features
    all_cols = train_set.columns
    non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
    feature_columns = [c for c in all_cols if c not in non_feature]

    preprocessor_metadata = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Fit preprocessor (Spark): %d features, %d categorical, %d drop",
        len(feature_columns), len(categorical_cols), len(drop_cols),
    )
    return preprocessor_metadata, category_mappings


def _encode_categoricals_spark(
    df: DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> DataFrame:
    """Encode categorical columns via broadcast join. Unknown values -> null (cast to -1)."""
    spark = df.sparkSession
    result = df

    for col in categorical_cols:
        categories = category_mappings[col]
        mapping_rows = [(cat, idx) for idx, cat in enumerate(categories)]
        mapping_df = spark.createDataFrame(mapping_rows, [col, f"{col}_code"])
        result = result.join(F.broadcast(mapping_df), on=col, how="left")
        # Replace null codes (unknown categories) with -1
        result = result.withColumn(
            f"{col}_code",
            F.coalesce(F.col(f"{col}_code"), F.lit(-1)),
        )
        result = result.drop(col).withColumnRenamed(f"{col}_code", col)

    return result


def transform_to_model_input_spark(
    split_set: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Transform a Spark split dataset into model_input (identity + label + features).

    Categorical features that are NOT identity columns are encoded in Spark.
    Categorical identity columns (e.g., prod_name) keep original values —
    encoding is deferred to training pipeline's _extract_Xy.
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
    _validate_columns(split_set.columns, required, "split_set")
    _warn_missing_drop_columns(split_set.columns, drop_cols, "split_set")

    # Select only the columns we need (deduplicated)
    keep_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
    result = split_set.select(*keep_cols)

    # Encode categoricals EXCEPT identity columns (deferred to training)
    encode_cols = [c for c in categorical_cols if c not in identity_cols]
    if encode_cols:
        result = _encode_categoricals_spark(result, encode_cols, category_mappings)

        # Check for unknown values (-1 from encoding)
        for col in encode_cols:
            n_unknown = result.filter(F.col(col) == -1).count()
            if n_unknown > 0:
                logger.warning(
                    "transform: %d unknown values in column '%s' (encoded as -1)",
                    n_unknown, col,
                )

    # Validate label nulls
    n_label_null = result.filter(F.col(label_col).isNull()).count()
    if n_label_null > 0:
        logger.warning("transform: %d null labels in split", n_label_null)

    # Ensure consistent column ordering (deduplicated)
    output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
    result = result.select(*output_cols)

    logger.info(
        "Model input (Spark): %d features, label_nulls=%d",
        len(feature_columns), n_label_null,
    )
    return result


def apply_preprocessor_spark(
    scoring_dataset: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Apply preprocessor to Spark inference scoring dataset.

    Returns identity + feature columns for model prediction.
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    # Drop non-feature columns (except identity and categorical)
    cols_to_drop = [
        c for c in drop_cols
        if c in scoring_dataset.columns and c not in identity_cols
    ]
    result = scoring_dataset.drop(*cols_to_drop)

    # Encode categoricals via broadcast join
    result = _encode_categoricals_spark(result, categorical_cols, category_mappings)

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Select identity + feature columns in correct order
    result = result.select(*identity_cols, *feature_columns)

    logger.info("Preprocessed scoring data (Spark): %d columns", len(result.columns))
    return result

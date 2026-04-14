"""Spark backend for preprocessing: fit, apply-to-features, build-model-input, apply."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.preprocessing._common import (
    _get_preprocessing_config,
    _validate_columns,
    _warn_missing_drop_columns,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _encode_categoricals(
    df: DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> DataFrame:
    """Encode categorical columns via broadcast join. Unknown values -> -1."""
    spark = df.sparkSession
    result = df

    for col in categorical_cols:
        categories = category_mappings[col]
        mapping_rows = [(cat, idx) for idx, cat in enumerate(categories)]
        mapping_df = spark.createDataFrame(mapping_rows, [col, f"{col}_code"])
        result = result.join(F.broadcast(mapping_df), on=col, how="left")
        result = result.withColumn(
            f"{col}_code",
            F.coalesce(F.col(f"{col}_code"), F.lit(-1)),
        )
        result = result.drop(col).withColumnRenamed(f"{col}_code", col)

    return result


def _compute_feature_columns(
    feature_table_cols: list[str],
    identity_cols: list[str],
    categorical_cols: list[str],
    drop_cols: list[str],
    label_col: str,
) -> list[str]:
    """Compute feature_columns list preserving original post-join column order.

    Order: identity categoricals first (in identity_cols order), then
    feature_table columns minus drops / non-categorical identity / label.
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
    feature_table: DataFrame,
    train_keys: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata from Spark feature_table restricted to train custmers-months.

    Only small metadata (distinct category values) is collected to driver; no
    ``toPandas()`` on full data. Fit happens at customer-month granularity so
    future statistical transforms are not distorted by product fan-out.

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
    train_cm = train_keys.select(*cm_key).dropDuplicates()
    train_features = feature_table.join(F.broadcast(train_cm), on=cm_key, how="inner")

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
        distinct_rows = (
            train_features.select(col)
            .filter(F.col(col).isNotNull())
            .distinct()
            .orderBy(col)
            .collect()
        )
        category_mappings[col] = [row[col] for row in distinct_rows]
    for col in identity_cat_cols:
        distinct_rows = (
            train_keys.select(col)
            .filter(F.col(col).isNotNull())
            .distinct()
            .orderBy(col)
            .collect()
        )
        category_mappings[col] = [row[col] for row in distinct_rows]

    feature_columns = _compute_feature_columns(
        feature_table.columns,
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
        "Fit preprocessor (Spark): %d features, %d categorical, %d drop",
        len(feature_columns), len(categorical_cols), len(drop_cols),
    )
    return preprocessor_metadata, category_mappings


def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table at customer-month granularity.

    Output: (time + entity) + feature_columns that live in feature_table.
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
    _warn_missing_drop_columns(feature_table.columns, drop_cols, "feature_table")

    result = feature_table.select(*keep_cols)

    encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
    if encode_cols:
        result = _encode_categoricals(result, encode_cols, category_mappings)
        for col in encode_cols:
            n_unknown = result.filter(F.col(col) == -1).count()
            if n_unknown > 0:
                logger.warning(
                    "apply_preprocessor_to_features: %d unknowns in column '%s'",
                    n_unknown, col,
                )

    logger.info(
        "Preprocessed feature_table (Spark): %d cols (encoded=%d)",
        len(result.columns), len(encode_cols),
    )
    return result


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Merge Spark keys + labels + pre-encoded features into model_input.

    Equivalent to (build_dataset + transform_to_model_input) but encoding is
    already applied to feature_table once, so splits share the work.
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
    dataset = keys.join(label_table, on=label_join_key, how="inner")
    dataset = dataset.join(preprocessed_feature_table, on=base_key, how="left")

    required = list(set(identity_cols + [label_col] + feature_columns))
    _validate_columns(dataset.columns, required, "build_model_input")

    output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
    result = dataset.select(*output_cols)

    n_label_null = result.filter(F.col(label_col).isNull()).count()
    if n_label_null > 0:
        logger.warning("build_model_input: %d null labels", n_label_null)

    logger.info(
        "Model input (Spark): %d features, label_nulls=%d",
        len(feature_columns), n_label_null,
    )
    return result


def apply_preprocessor(
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
    result = _encode_categoricals(result, categorical_cols, category_mappings)

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Select identity + feature columns in correct order
    result = result.select(*identity_cols, *feature_columns)

    logger.info("Preprocessed scoring data (Spark): %d columns", len(result.columns))
    return result

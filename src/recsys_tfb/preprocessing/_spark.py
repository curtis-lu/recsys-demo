"""Spark backend for preprocessing: fit, apply-to-features, build-model-input, apply."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql import types as T

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.nodes_shared import collect_dataset_snap_dates
from recsys_tfb.preprocessing._common import (
    _get_preprocessing_config,
    _validate_columns,
    _warn_missing_drop_columns,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _cast_feature_decimals_to_double(
    df: DataFrame,
    feature_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Cast all DecimalType columns within feature_cols to double.

    pandas/pyarrow materializes decimal128 as Python decimal.Decimal objects
    (~70 bytes/value vs 8 bytes/float64), inflating peak memory ~10x and
    OOM-killing extract_Xy in prod. LightGBM consumes float anyway, so cast
    at write time and bake the smaller representation into model_input.

    Identity and label columns are intentionally NOT cast - they should not
    be decimal to begin with, and silent coercion of primary keys / label
    dtype would mask a real schema bug.
    """
    feature_set = set(feature_cols)
    decimal_feature_cols = [
        f.name for f in df.schema.fields
        if f.name in feature_set and isinstance(f.dataType, T.DecimalType)
    ]
    for col in decimal_feature_cols:
        df = df.withColumn(col, F.col(col).cast("double"))
    return df, decimal_feature_cols


def _encode_categoricals(
    df: DataFrame,
    categorical_cols: list[str],
    category_mappings: dict[str, list],
) -> DataFrame:
    """Encode categorical columns via Spark SQL map literal. Unknown values -> -1.

    Uses F.create_map (JVM-side) instead of createDataFrame(list) + broadcast join
    to avoid sc.parallelize(), which would pickle data with the driver's protocol
    (5 on Python 3.10) and fail on Python 3.6 workers.
    """
    result = df
    for col in categorical_cols:
        categories = category_mappings[col]
        if not categories:
            result = result.withColumn(col, F.lit(-1).cast("integer"))
            continue
        pairs: list = []
        for idx, cat in enumerate(categories):
            pairs.extend([F.lit(cat), F.lit(idx)])
        map_col = F.create_map(*pairs)
        result = result.withColumn(
            col, F.coalesce(map_col[F.col(col)], F.lit(-1)).cast("integer")
        )
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
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata at customer-month granularity, decoupled from sampling.

    Feature-categorical distinct values come from feature_table rows whose
    ``time`` falls in ``train_snap_dates``. Identity categoricals (not present
    in feature_table) come from ``parameters["schema"]["categorical_values"][col]``;
    missing declarations raise ``ValueError``.

    Raises ``ValueError`` if feature_table is missing any required train_snap_date
    (fail-loud principle: dataset must be reproducible from feature_table).

    Only small metadata (distinct category values) is collected to driver.

    Returns:
        (preprocessor_metadata, category_mappings)
    """
    import pandas as pd

    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {})
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Fail-loud if feature_table is missing any required train_snap_date.
    # Cardinality is small (typically 12-52 dates); .distinct().collect() is cheap.
    ft_dates = {
        row[time_col]
        for row in feature_table.select(time_col).distinct().collect()
    }
    ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
    missing = sorted(set(train_dates) - ft_dates)
    if missing:
        raise ValueError(
            "feature_table missing required train_snap_dates: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

    with log_step(logger, "filter_train_window"):
        train_features = feature_table.filter(F.col(time_col).isin(train_dates))

    ft_cols = set(feature_table.columns)
    feature_cat_cols = [c for c in categorical_cols if c in ft_cols]
    identity_cat_cols = [c for c in categorical_cols if c not in ft_cols]

    cat_values = schema.get("categorical_values", {})
    missing_cats = [c for c in identity_cat_cols if c not in cat_values]
    if missing_cats:
        raise ValueError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing_cats}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )

    with log_step(logger, "collect_category_mappings"):
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
            category_mappings[col] = list(cat_values[col])

    with log_step(logger, "compute_feature_columns"):
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

    Filters feature_table to the union of all dataset snap_dates (train ∪ cal ∪ val ∪ test).
    Raises ``ValueError`` if any required snap_date is missing from feature_table.

    Output: (time + entity) + feature_columns that live in feature_table.
    """
    import pandas as pd

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

    needed_dates = collect_dataset_snap_dates(parameters)

    # Fail-loud if feature_table is missing any required snap_date.
    ft_dates = {
        row[time_col]
        for row in feature_table.select(time_col).distinct().collect()
    }
    ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
    missing = sorted(set(needed_dates) - ft_dates)
    if missing:
        raise ValueError(
            "feature_table missing required snap_dates: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

    with log_step(logger, "select_columns"):
        result = (
            feature_table.filter(F.col(time_col).isin(needed_dates))
            .select(*keep_cols)
        )

    with log_step(logger, "encode_categoricals"):
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
    with log_step(logger, "merge_labels"):
        dataset = keys.join(label_table, on=label_join_key, how="left")
        # sample_pool 是 dense (cust × prod 全展開)，label_table 是 sparse
        # (只含有大類交易的 cust)。join miss 的 row 視為 negative。
        dataset = dataset.withColumn(label_col, F.coalesce(F.col(label_col), F.lit(0)))
    with log_step(logger, "merge_features"):
        dataset = dataset.join(preprocessed_feature_table, on=base_key, how="left")

    with log_step(logger, "select_output_columns"):
        required = list(set(identity_cols + [label_col] + feature_columns))
        _validate_columns(dataset.columns, required, "build_model_input")

        output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
        result = dataset.select(*output_cols)

    with log_step(logger, "cast_decimals_to_double"):
        result, casted = _cast_feature_decimals_to_double(result, feature_columns)
    logger.info(
        "build_model_input: %d features, cast %d decimal feature columns to double",
        len(feature_columns), len(casted),
    )
    if casted:
        logger.debug("build_model_input: casted columns = %s", casted)
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

    with log_step(logger, "encode_categoricals"):
        result = _encode_categoricals(result, categorical_cols, category_mappings)

    with log_step(logger, "select_feature_columns"):
        missing = set(feature_columns) - set(result.columns)
        if missing:
            raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")
        result = result.select(*identity_cols, *feature_columns)

    with log_step(logger, "cast_decimals_to_double"):
        result, casted = _cast_feature_decimals_to_double(result, feature_columns)
    logger.info(
        "apply_preprocessor: %d columns, cast %d decimal feature columns to double",
        len(result.columns), len(casted),
    )
    if casted:
        logger.debug("apply_preprocessor: casted columns = %s", casted)
    return result

"""Spark backend for preprocessing: fit, apply-to-features, build-model-input, apply."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import Window
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


def filter_groups_with_positives(
    df: DataFrame,
    group_cols: list[str],
    label_col: str,
) -> DataFrame:
    """Drop rows whose ``group_cols`` partition has ``sum(label_col) == 0``.

    A query group is ``(time, *entity)``; pushing this filter to Spark
    write-time keeps val_model_input / test_model_input Hive tables tight —
    customers with no positives in a snap_date contribute nothing to mAP
    (metrics_spark filters them again) and only waste predict time.
    """
    w = Window.partitionBy(*group_cols)
    return (
        df.withColumn("__grp_pos", F.sum(F.col(label_col)).over(w))
          .filter(F.col("__grp_pos") > 0)
          .drop("__grp_pos")
    )


def _cast_feature_floats_to_float32(
    df: DataFrame,
    feature_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Cast DecimalType and DoubleType columns within feature_cols to float (float32).

    Invariant: model_input's numeric feature columns are stored as float32.

    LightGBM is histogram-based GBT (max_bin=256, so split decisions resolve
    at log2(256)=8-bit granularity). float32's ~7-digit decimal precision is
    far beyond what binning can use, making float64 / decimal128 pure waste:

    - decimal128 is the disaster case: pandas/pyarrow materializes it as
      Python ``decimal.Decimal`` objects (~70 B/value vs 4 B/float32), so
      extract_Xy peak memory explodes (originally OOM-killed the val read).
    - DoubleType is the silent case: ~2x the memory of float32 (8 vs 4 B)
      and ~2x slower on SIMD-vectorized pandas ops, with zero compensating
      benefit for the model.

    Identity and label columns are intentionally NOT cast — they should not
    be a numeric float type to begin with, and silent coercion of primary
    keys / label dtype would mask a real schema bug.

    Returns:
        (df, casted_cols) where ``casted_cols`` is the subset of
        ``feature_cols`` that were DecimalType or DoubleType.
    """
    feature_set = set(feature_cols)
    casted_feature_cols = [
        f.name for f in df.schema.fields
        if f.name in feature_set
        and isinstance(f.dataType, (T.DecimalType, T.DoubleType))
    ]
    for col in casted_feature_cols:
        df = df.withColumn(col, F.col(col).cast("float"))
    return df, casted_feature_cols


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

    # Local import: keep this lazy to avoid an import cycle
    # (_spark -> core.schema -> core.consistency). Do not hoist to module level.
    from recsys_tfb.core.consistency import DataConsistencyError

    cat_values = schema.get("categorical_values", {})
    missing_cats = [c for c in identity_cat_cols if c not in cat_values]
    if missing_cats:
        raise DataConsistencyError(
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

    # Ranking-task invariant: schema.item must end up in feature_columns. The
    # most common way to lose it is omitting it from
    # `dataset.prepare_model_input.categorical_columns` in yaml — silently
    # makes X miss the item dimension, predictions collapse to constant within
    # each query group, and HPO reports a flat mAP across every trial.
    item_col = schema.get("item")
    if item_col and item_col not in feature_columns:
        raise DataConsistencyError(
            f"schema.item='{item_col}' is missing from derived feature_columns. "
            f"For a ranking task the item column must be a model feature; "
            f"otherwise the booster cannot differentiate items within a query "
            f"group and HPO mAP collapses to a constant across trials. "
            f"Fix: add '{item_col}' to "
            f"dataset.prepare_model_input.categorical_columns in "
            f"parameters_dataset.yaml. "
            f"(current categorical_columns={categorical_cols})"
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


def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> None:
    """Layer-2 data gate (B1 + B5). Side-effect only: raises
    ``DataConsistencyError`` on violation, returns ``None`` on success. Wired
    as the first node of the dataset pipeline.

    B1 — item values are checked on sample_pool (set-equality vs declared, both
    directions) and label_table (only data-has-unknown), restricted to the
    configured snap_date windows the pipeline actually uses.

    B5 — a column declared in ``categorical_columns`` must not be a
    continuous-numeric type (decimal/double/float) in feature_table. Read off
    ``feature_table.dtypes`` (metastore metadata, no scan) so the opaque
    "Decimal is not JSON serializable" crash inside fit_preprocessor_metadata
    is caught up-front at the first node instead of after the full distinct pass.

    All errors are collected and raised once so a single fix pass clears them.
    """
    # Local import: keep lazy to avoid an import cycle
    # (_spark -> core.schema -> core.consistency). Matches the existing
    # local-import pattern inside fit_preprocessor_metadata.
    from recsys_tfb.core.consistency import (
        DataConsistencyError,
        categorical_dtype_errors,
        item_coverage_errors,
        nonnumeric_feature_errors,
        resolved_item_values,
        spark_dtype_is_numeric,
    )

    schema = get_schema(parameters)
    item = schema["item"]
    time_col = schema["time"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    windows = collect_dataset_snap_dates(parameters)

    def _distinct_items(df: DataFrame) -> set:
        rows = (
            df.filter(F.col(time_col).isin(windows))
            .select(item)
            .distinct()
            .collect()
        )
        return {r[item] for r in rows if r[item] is not None}

    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    ft_dtypes = dict(feature_table.dtypes)
    feature_cols = _compute_feature_columns(
        list(feature_table.columns),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )
    # Only feature_table-sourced columns have a dtype here; identity categoricals
    # (e.g. prod_name) come from schema.categorical_values, are absent from
    # feature_table.dtypes, and are validated by A3.
    feature_kinds = {
        c: ("numeric" if spark_dtype_is_numeric(ft_dtypes[c]) else "nonnumeric")
        for c in feature_cols
        if c in ft_dtypes
    }
    errors = (
        item_coverage_errors(
            item,
            resolved_item_values(parameters),
            _distinct_items(sample_pool),
            _distinct_items(label_table),
        )
        + categorical_dtype_errors(categorical_cols, ft_dtypes)
        + nonnumeric_feature_errors(feature_kinds, set(categorical_cols))
    )
    if errors:
        raise DataConsistencyError(
            "Data consistency check failed ("
            + str(len(errors))
            + " issue(s)):\n- "
            + "\n- ".join(errors)
        )


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
            # Single pass: one aggregation returns the unknown (-1) count for
            # every encoded column at once. The previous per-column .count()
            # re-scanned the full multi-month feature_table once per categorical
            # (N actions); this collapses it to a single scan.
            unknown_counts = result.agg(*[
                F.sum(F.when(F.col(c) == -1, 1).otherwise(0)).alias(c)
                for c in encode_cols
            ]).collect()[0]
            for col in encode_cols:
                n_unknown = unknown_counts[col] or 0
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
        # sample_pool is dense (cust × prod fully expanded); label_table is
        # sparse (only customers with category transactions). Join misses are
        # treated as negatives.
        dataset = dataset.withColumn(label_col, F.coalesce(F.col(label_col), F.lit(0)))
    with log_step(logger, "merge_features"):
        dataset = dataset.join(preprocessed_feature_table, on=base_key, how="left")

    with log_step(logger, "select_output_columns"):
        required = list(set(identity_cols + [label_col] + feature_columns))
        _validate_columns(dataset.columns, required, "build_model_input")

        carry_present = [
            c for c in keys.columns
            if c not in identity_cols and c not in feature_columns
            and c != label_col and c in dataset.columns
        ]
        output_cols = list(dict.fromkeys(
            identity_cols + [label_col] + feature_columns + carry_present
        ))
        result = dataset.select(*output_cols)

    with log_step(logger, "cast_features_to_float32"):
        result, casted = _cast_feature_floats_to_float32(result, feature_columns)
    logger.info(
        "build_model_input: %d features, cast %d float-like feature columns to float32",
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

    with log_step(logger, "cast_features_to_float32"):
        result, casted = _cast_feature_floats_to_float32(result, feature_columns)
    logger.info(
        "apply_preprocessor: %d columns, cast %d float-like feature columns to float32",
        len(result.columns), len(casted),
    )
    if casted:
        logger.debug("apply_preprocessor: casted columns = %s", casted)
    return result

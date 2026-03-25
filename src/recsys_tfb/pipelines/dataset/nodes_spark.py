"""PySpark implementations for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

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


def select_keys(
    sample_pool: DataFrame,
    parameters: dict,
    snap_dates: list,
    sample_ratio: float,
    sample_ratio_overrides: dict | None = None,
) -> DataFrame:
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
    target_dates = [pd.Timestamp(d) for d in snap_dates]
    if target_dates:
        pool = sample_pool.filter(F.col(time_col).isin(target_dates))
    else:
        pool = sample_pool

    # Extract unique identity keys with group columns
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = pool.select(*extract_cols).dropDuplicates(identity_key)

    if sample_ratio >= 1.0 and not sample_ratio_overrides:
        sampled = keys.select(*identity_key)
        logger.info("Sampled keys (ratio=1.0, no sampling)")
        return sampled

    # Build override mapping as a UDF-free approach using when/otherwise
    if sample_ratio_overrides:
        # Construct group key column by concatenating with "|"
        if len(group_keys) == 1:
            group_key_col = F.col(group_keys[0]).cast("string")
        else:
            group_key_col = F.concat_ws("|", *[F.col(k).cast("string") for k in group_keys])

        # Build CASE expression for effective ratio
        ratio_expr = F.lit(sample_ratio)
        for gk_val, override_ratio in sample_ratio_overrides.items():
            ratio_expr = F.when(group_key_col == F.lit(str(gk_val)), F.lit(override_ratio)).otherwise(ratio_expr)

        keys = keys.withColumn("_effective_ratio", ratio_expr)
    else:
        keys = keys.withColumn("_effective_ratio", F.lit(sample_ratio))

    # Probabilistic sampling: rand(seed) < effective_ratio
    sampled = keys.filter(
        F.rand(seed) < F.col("_effective_ratio")
    ).select(*identity_key)

    logger.info(
        "Sampled keys (ratio=%.2f, group_keys=%s, overrides=%s)",
        sample_ratio,
        group_keys,
        sample_ratio_overrides,
    )
    return sampled


def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys using date range from parameters."""
    _validate_date_splits(parameters)

    ds = parameters["dataset"]
    time_col = get_schema(parameters)["time"]
    start = pd.Timestamp(ds["train_snap_date_start"])
    end = pd.Timestamp(ds["train_snap_date_end"])

    # Filter sample_pool to train date range directly
    pool = sample_pool.filter(
        (F.col(time_col) >= F.lit(start)) & (F.col(time_col) <= F.lit(end))
    )

    # Collect unique dates for passing to select_keys
    train_dates_rows = pool.select(time_col).distinct().collect()
    train_dates = [row[time_col] for row in train_dates_rows]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(sample_pool, parameters, train_dates, ds["sample_ratio"], overrides)


def select_calibration_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(sample_pool, parameters, cal_dates, cal_ratio, cal_overrides)


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by cust_id ratio.

    All rows for a given cust_id are assigned to the same split.
    No .count() action triggered for logging.
    """
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    cust_col = entity_cols[0]

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Assign random value per cust_id, split by threshold
    cust_df = sample_keys.select(cust_col).distinct()
    cust_df = cust_df.withColumn("_rand", F.rand(seed))

    # cust_ids with _rand < train_dev_ratio → train-dev
    dev_custs = cust_df.filter(F.col("_rand") < F.lit(train_dev_ratio)).select(cust_col)
    train_custs = cust_df.filter(F.col("_rand") >= F.lit(train_dev_ratio)).select(cust_col)

    train_keys = sample_keys.join(train_custs, on=cust_col, how="inner")
    train_dev_keys = sample_keys.join(dev_custs, on=cust_col, how="inner")

    logger.info(
        "Split train keys (ratio=%.2f)",
        train_dev_ratio,
    )
    return train_keys, train_dev_keys


def select_val_keys(
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select validation identity keys (full population, optional random cust_id sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols
    cust_col = entity_cols[0]

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    val_labels = label_table.filter(F.col(time_col).isin(val_dates))
    all_keys = val_labels.select(*identity_key).dropDuplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys (full population)")
        return all_keys

    # Pure random cust_id sampling
    custs = all_keys.select(cust_col).distinct()
    sampled_custs = custs.withColumn("_rand", F.rand(seed)).filter(
        F.col("_rand") < F.lit(val_sample_ratio)
    ).select(cust_col)

    sampled = all_keys.join(sampled_custs, on=cust_col, how="inner")
    logger.info("Val keys (ratio=%.2f)", val_sample_ratio)
    return sampled


def select_test_keys(
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select test identity keys (full population, no sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = [time_col] + entity_cols

    ds = parameters["dataset"]
    test_dates = [pd.Timestamp(d) for d in ds.get("test_snap_dates", [])]

    test_labels = label_table.filter(F.col(time_col).isin(test_dates))
    all_keys = test_labels.select(*identity_key).dropDuplicates()

    logger.info("Test keys (full population)")
    return all_keys


def build_dataset(
    keys: DataFrame,
    feature_table: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
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
    dataset = keys.join(label_table, on=label_join_key, how="inner")

    # Join with features on base key (snap_date, cust_id)
    dataset = dataset.join(feature_table, on=base_key, how="left")

    logger.info("Built dataset: %d columns", len(dataset.columns))
    return dataset


def _prepare_transform_spark(train_pdf: pd.DataFrame, parameters: dict):
    """Build category_mappings and _transform helper from train pandas DataFrame."""
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
        cat = pd.CategoricalDtype(categories=sorted(train_pdf[col].unique()))
        category_mappings[col] = list(cat.categories)

    def _transform(df: pd.DataFrame) -> pd.DataFrame:
        result = df.drop(columns=drop_cols, errors="ignore").copy()
        for col in categorical_cols:
            known = category_mappings[col]
            result[col] = pd.Categorical(result[col], categories=known).codes
        return result

    feature_columns = list(_transform(train_pdf).columns)

    preprocessor = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    return preprocessor, category_mappings, _transform


def prepare_model_input(
    train_set: DataFrame,
    train_dev_set: DataFrame,
    val_set: DataFrame,
    test_set: DataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict, dict]:
    """Convert 4 Spark DataFrames to model-ready pandas DataFrames (without calibration).

    Returns: X_train, y_train, X_train_dev, y_train_dev, X_val, y_val,
             X_test, y_test, preprocessor, category_mappings (10 outputs).
    """
    schema = get_schema(parameters)
    label_col = schema["label"]

    # Convert to pandas
    train_pdf = train_set.toPandas()
    train_dev_pdf = train_dev_set.toPandas()
    val_pdf = val_set.toPandas()
    test_pdf = test_set.toPandas()

    preprocessor, category_mappings, _transform = _prepare_transform_spark(train_pdf, parameters)

    X_train = _transform(train_pdf)
    y_train = train_pdf[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_pdf)
    y_train_dev = train_dev_pdf[[label_col]].reset_index(drop=True)
    X_val = _transform(val_pdf)
    y_val = val_pdf[[label_col]].reset_index(drop=True)
    X_test = _transform(test_pdf)
    y_test = test_pdf[[label_col]].reset_index(drop=True)

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
    train_set: DataFrame,
    train_dev_set: DataFrame,
    calibration_set: DataFrame,
    val_set: DataFrame,
    test_set: DataFrame,
    parameters: dict,
) -> tuple:
    """Convert 5 Spark DataFrames to model-ready pandas DataFrames (with calibration).

    Returns: X_train, y_train, X_train_dev, y_train_dev,
             X_calibration, y_calibration, X_val, y_val,
             X_test, y_test, preprocessor, category_mappings (12 outputs).
    """
    schema = get_schema(parameters)
    label_col = schema["label"]

    # Convert to pandas
    train_pdf = train_set.toPandas()
    train_dev_pdf = train_dev_set.toPandas()
    calibration_pdf = calibration_set.toPandas()
    val_pdf = val_set.toPandas()
    test_pdf = test_set.toPandas()

    preprocessor, category_mappings, _transform = _prepare_transform_spark(train_pdf, parameters)

    X_train = _transform(train_pdf)
    y_train = train_pdf[[label_col]].reset_index(drop=True)
    X_train_dev = _transform(train_dev_pdf)
    y_train_dev = train_dev_pdf[[label_col]].reset_index(drop=True)
    X_calibration = _transform(calibration_pdf)
    y_calibration = calibration_pdf[[label_col]].reset_index(drop=True)
    X_val = _transform(val_pdf)
    y_val = val_pdf[[label_col]].reset_index(drop=True)
    X_test = _transform(test_pdf)
    y_test = test_pdf[[label_col]].reset_index(drop=True)

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

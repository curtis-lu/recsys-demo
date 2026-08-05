"""PySpark implementations for the dataset building pipeline."""

import logging

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys
from recsys_tfb.pipelines.dataset.nodes_shared import (
    SnapDatePlan,
    validate_date_splits,
)
from recsys_tfb.preprocessing._spark import (
    apply_preprocessor_to_features as _apply_preprocessor_to_features,
    build_model_input as _build_model_input,
    filter_groups_with_positives as _filter_groups_with_positives,
    fit_preprocessor_metadata as _fit_preprocessor_metadata,
)

logger = logging.getLogger(__name__)



def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys using explicit train_snap_dates list."""
    validate_date_splits(parameters)

    ds = parameters["dataset"]
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(
        sample_pool, parameters, train_dates, ds["sample_ratio"], overrides,
        site="sample_keys",
    )


def select_calibration_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(
        sample_pool, parameters, cal_dates, cal_ratio, cal_overrides,
        site="calibration_keys",
    )


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by cust_id ratio.

    All rows for a given cust_id are assigned to the same split.
    Logging still triggers no action; the empty-dev guard below does — one
    ``isEmpty`` always, plus one ``count`` on the failing path only.
    """
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    cust_col = entity_cols[0]

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Deterministic per-cust_id bucket; threshold is computed once so the two
    # filters are guaranteed to be a complete and disjoint partition regardless
    # of how many actions Spark runs against this plan.
    cust_df = sample_keys.select(cust_col).distinct()
    cust_df = cust_df.withColumn(
        "_bucket", spark_bucket(cust_df, [cust_col], seed, site="split_train_dev"),
    )
    threshold = ratio_to_threshold(train_dev_ratio)

    dev_custs = cust_df.filter(F.col("_bucket") < F.lit(threshold)).select(cust_col)
    train_custs = cust_df.filter(F.col("_bucket") >= F.lit(threshold)).select(cust_col)

    train_keys = sample_keys.join(train_custs, on=cust_col, how="inner")
    train_dev_keys = sample_keys.join(dev_custs, on=cust_col, how="inner")

    # An empty train_dev is invisible downstream: it is the early-stopping
    # validation set for every HPO trial (training/nodes.py passes
    # train_dev_lgb_handle as val_dataset), so an empty one means each trial
    # silently runs its full round budget with early stopping never firing —
    # no error, no warning, just worse models and a longer search. Costs one
    # Spark action; see ADR-0005 for the fallback if that ever matters at scale.
    # `!= 0`, not `> 0`: a negative ratio makes ratio_to_threshold return a
    # negative threshold, so `_bucket < threshold` is empty and
    # `_bucket >= threshold` takes everything — the same silent state, reached
    # by one stray minus sign. Only an exact 0 means "no dev split wanted".
    if train_dev_ratio != 0 and train_dev_keys.isEmpty():
        n_entities = cust_df.count()
        if n_entities == 0:
            raise ValueError(
                f"split_train_keys received no sampled keys at all "
                f"(train_dev_ratio={train_dev_ratio}), so train and train-dev "
                f"are both empty. The cause is upstream of the split — check "
                f"dataset.sample_ratio, dataset.train_snap_dates, and any "
                f"partition filter applied when sample_keys was read back."
            )
        raise ValueError(
            f"split_train_keys produced an empty train-dev split: "
            f"train_dev_ratio={train_dev_ratio} applied to "
            f"{n_entities} distinct {cust_col} value(s) puts every entity "
            f"on the train side. train_dev is the early-stopping validation set "
            f"for every HPO trial, so an empty one disables early stopping "
            f"without raising. Raise dataset.train_dev_ratio, or widen the "
            f"sample (dataset.sample_ratio / train_snap_dates)."
        )

    logger.info(
        "Split train keys (ratio=%.2f)",
        train_dev_ratio,
    )
    return train_keys, train_dev_keys


def select_val_keys(
    sample_pool: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select validation identity keys (full population, optional random cust_id sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = schema["identity_columns"]
    cust_col = entity_cols[0]

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    val_labels = sample_pool.filter(F.col(time_col).isin(val_dates))
    all_keys = val_labels.select(*identity_key).dropDuplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys (full population)")
        return all_keys

    # Deterministic cust_id sampling
    custs = all_keys.select(cust_col).distinct()
    sampled_custs = custs.withColumn(
        "_bucket", spark_bucket(custs, [cust_col], seed, site="val_keys"),
    ).filter(
        F.col("_bucket") < F.lit(ratio_to_threshold(val_sample_ratio))
    ).select(cust_col)

    sampled = all_keys.join(sampled_custs, on=cust_col, how="inner")
    logger.info("Val keys (ratio=%.2f)", val_sample_ratio)
    return sampled


def _date_filter(time_col: str, dates: list):
    """``time_col IN dates``, normalised to DATE on both sides.

    ``time_col`` reaches these nodes with two different types depending on where
    the frame came from: a real DATE/TIMESTAMP when read from a source table,
    but a **string** when read back from a Hive table where snap_date is a
    partition column (``partition_cols: {name: snap_date, type: STRING}``) —
    which is what the runner does, since it reloads every node input through the
    catalog. ``F.col(snap_date).isin([pd.Timestamp(...)])`` matches **zero** rows
    against the string form while raising nothing, so the comparison must be
    pinned to DATE on both sides.

    An empty ``dates`` list is a normal state here (every configured month
    already landed) and gets an explicit constant-false predicate: ``isin([])``
    is not a dependable "match nothing".
    """
    if not dates:
        return F.lit(False)
    return F.to_date(F.col(time_col)).isin([pd.Timestamp(d).date() for d in dates])


def select_test_keys(
    sample_pool: DataFrame,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """Select test identity keys (full population, no sampling).

    Restricted to ``month_plan.to_process`` (ADR-0002). Months that already
    landed are left alone: the write is a dynamic partition overwrite, so an
    absent month means "untouched", not "deleted".
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]

    test_labels = sample_pool.filter(_date_filter(time_col, month_plan.to_process))
    all_keys = test_labels.select(*identity_key).dropDuplicates()

    logger.info("Test keys (full population)")
    return all_keys


def build_test_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """build_model_input for the test split, scoped to ``month_plan``.

    The one thing this adds over the shared ``build_model_input``: ``test_keys``
    is a persistent Hive table holding *every* month under this base version, so
    reading it back gives the full history even when ``select_test_keys`` only
    wrote the new month. Without this filter the downstream join would rebuild
    every month — the ∝N cost ADR-0002 exists to remove.
    """
    schema = get_schema(parameters)
    keys = keys.filter(_date_filter(schema["time"], month_plan.to_process))
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )


def fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Fit Spark preprocessor at customer-month granularity, decoupled from sampling.

    Only collects small metadata (distinct category values) to driver.
    """
    return _fit_preprocessor_metadata(feature_table, parameters)


def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table once for all splits.

    Only the months in ``month_plan`` are encoded (ADR-0002). Skipping an
    existing month is safe because a partition's content is
    f(that month's feature_table rows, category_mappings) and the mappings are
    fit on train months only — no cross-month term, so a re-encode would
    reproduce the same bits.

    Passes the month *list* down, not the plan: preprocessing uses it to raise
    when a month it is about to process is missing from ``feature_table``.
    Pre-filtering here would make that check vacuously true — and preprocessing
    stays unaware that "incremental" is a concept.
    """
    return _apply_preprocessor_to_features(
        feature_table, preprocessor_metadata, parameters,
        snap_dates=month_plan.to_process,
    )


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Merge Spark keys + labels + encoded features into model_input for a split."""
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )


def filter_groups_with_positives(
    model_input: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Drop (time, *entity) groups whose label sum is zero.

    Pipeline-node wrapper over preprocessing._spark.filter_groups_with_positives;
    resolves group_cols / label_col from schema. Used for val / test only —
    groups without any positive contribute nothing to mAP (metrics_spark
    re-applies the same filter) and would only waste predict time.
    """
    schema = get_schema(parameters)
    group_cols = [schema["time"]] + schema["entity"]
    label_col = schema["label"]
    return _filter_groups_with_positives(model_input, group_cols, label_col)

"""Helper functions for the dataset building pipeline (Spark backend)."""

import logging

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


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

    # Extract identity + group columns. sample_pool PK = identity_key is enforced
    # by source_etl's max_duplicate_key_ratio check, so no dedup needed here.
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = pool.select(*extract_cols)

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

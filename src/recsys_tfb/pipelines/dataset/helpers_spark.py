"""Helper functions for the dataset building pipeline (Spark backend)."""

import logging

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset._hashing import HASH_BUCKETS, spark_bucket

logger = logging.getLogger(__name__)


def select_keys(
    sample_pool: DataFrame,
    parameters: dict,
    snap_dates: list,
    sample_ratio: float,
    sample_ratio_overrides: dict | None = None,
    *,
    site: str = "sample_keys",
) -> DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys.

    Filters sample_pool to the given snap_dates and applies stratified sampling
    with per-group ratio overrides. Identity key is (snap_date, cust_id, prod_name).

    Sampling is deterministic: a row is kept when
    ``crc32(identity_key | site | seed) % HASH_BUCKETS < ratio * HASH_BUCKETS``.

    Args:
        sample_pool: Full sample pool at customer-month-product granularity.
        parameters: Full parameters dict.
        snap_dates: List of snap_dates to filter to.
        sample_ratio: Default sampling ratio for this split.
        sample_ratio_overrides: Per-group ratio overrides. If None, falls back to
            parameters["dataset"]["sample_ratio_overrides"].
        site: Stable label that namespaces this sampling site so two callers
            sharing the same seed (e.g. train vs calibration) draw independent
            buckets.
    """
    schema = get_schema(parameters)
    identity_key = schema["identity_columns"]  # [snap_date, cust_id, prod_name]
    time_col = schema["time"]

    ds = parameters["dataset"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    if sample_ratio_overrides is None:
        sample_ratio_overrides = ds.get("sample_ratio_overrides", {})

    carry_columns = ds.get("carry_columns", []) or []
    return_cols = identity_key + [c for c in carry_columns if c not in identity_key]

    # Filter to specified snap_dates
    target_dates = [pd.Timestamp(d) for d in snap_dates]
    if target_dates:
        pool = sample_pool.filter(F.col(time_col).isin(target_dates))
    else:
        pool = sample_pool

    # Extract identity + group columns. sample_pool PK = identity_key is enforced
    # by source_etl's max_duplicate_key_ratio check, so no dedup needed here.
    extract_cols = list(dict.fromkeys(group_keys + identity_key + carry_columns))
    keys = pool.select(*extract_cols)

    if sample_ratio >= 1.0 and not sample_ratio_overrides:
        sampled = keys.select(*return_cols)
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

    # Deterministic sampling: bucket(identity_key | site | seed) < threshold
    keys = keys.withColumn("_bucket", spark_bucket(keys, identity_key, seed, site=site))
    threshold_expr = (F.col("_effective_ratio") * F.lit(HASH_BUCKETS)).cast("int")
    sampled = keys.filter(F.col("_bucket") < threshold_expr).select(*return_cols)

    logger.info(
        "Sampled keys (ratio=%.2f, group_keys=%s, overrides=%s, site=%s)",
        sample_ratio,
        group_keys,
        sample_ratio_overrides,
        site,
    )
    return sampled

"""Helper functions for the dataset building pipeline (pandas backend)."""

import logging

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset._hashing import HASH_BUCKETS, pandas_bucket

logger = logging.getLogger(__name__)


def compute_effective_ratio(
    row_group_key: str,
    sample_ratio: float,
    sample_ratio_overrides: dict,
) -> float:
    """Look up effective sampling ratio for a group key, falling back to default."""
    return sample_ratio_overrides.get(row_group_key, sample_ratio)


def select_keys(
    sample_pool: pd.DataFrame,
    parameters: dict,
    snap_dates: list,
    sample_ratio: float,
    sample_ratio_overrides: dict | None = None,
    *,
    site: str = "sample_keys",
) -> pd.DataFrame:
    """Stratified sampling by configurable group keys, returning unique identity keys.

    Filters sample_pool to the given snap_dates and applies stratified sampling
    with per-group ratio overrides. Identity key is (snap_date, cust_id, prod_name).

    Sampling is deterministic via CRC32 of the identity key — see
    :mod:`recsys_tfb.pipelines.dataset._hashing`. Identical to the Spark backend
    so that runs with the same seed produce identical splits in either engine.

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

    # Filter to specified snap_dates
    target_dates = set(pd.to_datetime(snap_dates))
    pool = sample_pool[sample_pool[time_col].isin(target_dates)]

    # Extract identity + group columns. sample_pool PK = identity_key is enforced
    # by source_etl's max_duplicate_key_ratio check, so no dedup needed here.
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = pool[extract_cols]

    if sample_ratio >= 1.0 and not sample_ratio_overrides:
        sampled = keys[identity_key].reset_index(drop=True)
        logger.info("Sampled %d keys (ratio=1.0, no sampling)", len(sampled))
        return sampled

    # Compute effective ratio per row via overrides
    def _serialize_group_key(row):
        return "|".join(str(row[k]) for k in group_keys)

    keys = keys.copy()
    keys["_group_key"] = keys.apply(_serialize_group_key, axis=1)
    keys["_effective_ratio"] = keys["_group_key"].map(
        lambda gk: compute_effective_ratio(gk, sample_ratio, sample_ratio_overrides)
    )
    keys["_bucket"] = pandas_bucket(keys, identity_key, seed, site=site)
    threshold = (keys["_effective_ratio"] * HASH_BUCKETS).round().astype(int)
    sampled = keys[keys["_bucket"] < threshold][identity_key].reset_index(drop=True)

    logger.info(
        "Sampled %d keys from %d (ratio=%.2f, group_keys=%s, overrides=%s, site=%s)",
        len(sampled),
        len(keys),
        sample_ratio,
        group_keys,
        sample_ratio_overrides,
        site,
    )
    return sampled

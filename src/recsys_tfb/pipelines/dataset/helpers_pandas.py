"""Helper functions for the dataset building pipeline (pandas backend)."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema

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
) -> pd.DataFrame:
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
    target_dates = set(pd.to_datetime(snap_dates))
    pool = sample_pool[sample_pool[time_col].isin(target_dates)]

    # Extract group keys + identity keys, dedup on identity
    extract_cols = list(dict.fromkeys(group_keys + identity_key))
    keys = pool[extract_cols].drop_duplicates(subset=identity_key)

    if sample_ratio >= 1.0 and not sample_ratio_overrides:
        sampled = keys[identity_key].reset_index(drop=True)
        logger.info("Sampled %d keys (ratio=1.0, no sampling)", len(sampled))
        return sampled

    # Compute effective ratio per row via overrides
    rng = np.random.RandomState(seed)

    def _serialize_group_key(row):
        return "|".join(str(row[k]) for k in group_keys)

    keys = keys.copy()
    keys["_group_key"] = keys.apply(_serialize_group_key, axis=1)
    keys["_effective_ratio"] = keys["_group_key"].map(
        lambda gk: compute_effective_ratio(gk, sample_ratio, sample_ratio_overrides)
    )
    keys["_rand"] = rng.random(len(keys))
    sampled = keys[keys["_rand"] < keys["_effective_ratio"]][identity_key].reset_index(drop=True)

    logger.info(
        "Sampled %d keys from %d (ratio=%.2f, group_keys=%s, overrides=%s)",
        len(sampled),
        len(keys),
        sample_ratio,
        group_keys,
        sample_ratio_overrides,
    )
    return sampled

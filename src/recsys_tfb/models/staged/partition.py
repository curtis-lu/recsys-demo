"""Stage-1 partitioning: composite group labels, slugs, deterministic seeds.

Group labels reuse the '|'-joined composite-key convention from
io/extract._composite_key_series (same convention as sample_ratio_overrides
keys), so a partition key that equals sample_group_keys produces identical
group identities across sampling and staged training.
"""

import re
import zlib

import numpy as np
import pandas as pd

_SLUG_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def group_labels(pdf: pd.DataFrame, partition_keys: list) -> pd.Series:
    """Per-row group label ('|'-joined partition key values, as str)."""
    missing = [k for k in partition_keys if k not in pdf.columns]
    if missing:
        raise KeyError(
            f"partition key column(s) {missing} not in dataframe columns"
        )
    # lazy import 避免 io↔models 循環（同 lightgbm_adapter 的作法）
    from recsys_tfb.io.extract import _composite_key_series

    return _composite_key_series(pdf, list(partition_keys))


def routing_keys(pdf: pd.DataFrame, partition_keys: list) -> np.ndarray:
    """group_labels as a numpy object array (predict-side routing)."""
    return group_labels(pdf, partition_keys).to_numpy(dtype=object)


def group_slug(group_key: str) -> str:
    """Filesystem-safe, collision-safe directory name for one group."""
    sanitized = _SLUG_UNSAFE.sub("_", group_key)[:40]
    crc = zlib.crc32(group_key.encode("utf-8")) & 0xFFFFFFFF
    return f"{sanitized}_{crc:08x}"


def group_seed(base_seed: int, group_key: str) -> int:
    """Deterministic per-group sampler seed (spec §3.1: derived, distinct)."""
    crc = zlib.crc32(group_key.encode("utf-8")) & 0xFFFFFFFF
    return (int(base_seed) * 1_000_003 + crc) % (2**31 - 1)

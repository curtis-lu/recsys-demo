"""Deterministic CRC32-based hashing utilities for sampling.

PySpark's F.crc32 uses the IEEE 802.3 polynomial. All dataset sampling
routines route through this helper so splits are reproducible across
reruns and partition layouts.

Datetime/date columns are normalized to ``yyyy-MM-dd HH:mm:ss`` before
concatenation to ensure deterministic byte-level input.
"""
from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

HASH_BUCKETS = 100_000
_DATETIME_FMT_SPARK = "yyyy-MM-dd HH:mm:ss"


def ratio_to_threshold(ratio: float) -> int:
    """Convert a [0, 1] sampling ratio into an integer bucket threshold."""
    return int(round(ratio * HASH_BUCKETS))


def _join_token(seed: int, site: str) -> str:
    return f"{site}|{seed}"


def spark_bucket(
    df: DataFrame, cols: Iterable[str], seed: int, site: str,
) -> Column:
    """Build a Spark Column of bucket indices in [0, HASH_BUCKETS).

    Datetime/date columns are formatted as ``yyyy-MM-dd HH:mm:ss`` so the
    string-level concatenation is deterministic across runs.
    """
    schema = df.schema
    parts: list[Column] = []
    for c in cols:
        dtype = schema[c].dataType
        if isinstance(dtype, (DateType, TimestampType)):
            parts.append(F.date_format(F.col(c), _DATETIME_FMT_SPARK))
        else:
            parts.append(F.col(c).cast("string"))
    parts.append(F.lit(_join_token(seed, site)))
    return F.crc32(F.concat_ws("|", *parts)) % F.lit(HASH_BUCKETS)

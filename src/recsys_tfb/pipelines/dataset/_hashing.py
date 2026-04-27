"""Deterministic CRC32-based hashing utilities for sampling.

Both PySpark's F.crc32 and Python's zlib.crc32 use the IEEE 802.3 polynomial,
so identical input bytes yield identical 32-bit unsigned integers in both
backends. All dataset sampling routines should route through these helpers so
that splits are reproducible across reruns, partition layouts, and backends.

To keep the byte-level input identical across backends, datetime/date columns
are normalized to the format ``yyyy-MM-dd HH:mm:ss`` on both sides before
concatenation.
"""
from __future__ import annotations

import zlib
from collections.abc import Iterable

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

HASH_BUCKETS = 100_000
_DATETIME_FMT_SPARK = "yyyy-MM-dd HH:mm:ss"
_DATETIME_FMT_PANDAS = "%Y-%m-%d %H:%M:%S"


def ratio_to_threshold(ratio: float) -> int:
    """Convert a [0, 1] sampling ratio into an integer bucket threshold."""
    return int(round(ratio * HASH_BUCKETS))


def _join_token(seed: int, site: str) -> str:
    return f"{site}|{seed}"


def spark_bucket(
    df: DataFrame, cols: Iterable[str], seed: int, site: str,
) -> Column:
    """Build a Spark Column of bucket indices in [0, HASH_BUCKETS).

    Datetime/date columns are formatted as ``yyyy-MM-dd HH:mm:ss`` so that
    string-level concatenation matches :func:`pandas_bucket` byte-for-byte.
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
    # F.crc32 returns a non-negative BIGINT, so `%` is safe (no need for pmod).
    return F.crc32(F.concat_ws("|", *parts)) % F.lit(HASH_BUCKETS)


def pandas_bucket(
    df: pd.DataFrame, cols: Iterable[str], seed: int, site: str,
) -> np.ndarray:
    """Build a numpy array of bucket indices in [0, HASH_BUCKETS).

    Mirrors :func:`spark_bucket`: datetime columns are normalized to
    ``%Y-%m-%d %H:%M:%S`` before concatenation.
    """
    cols = list(cols)
    token = _join_token(seed, site)
    parts = []
    for c in cols:
        s = df[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            parts.append(s.dt.strftime(_DATETIME_FMT_PANDAS))
        else:
            parts.append(s.astype(str))
    series = pd.concat(parts, axis=1).agg("|".join, axis=1) + "|" + token
    return series.map(lambda s: zlib.crc32(s.encode("utf-8")) % HASH_BUCKETS).to_numpy()

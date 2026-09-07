"""Per-column statistics read out of parquet footers, without scanning data.

Why this exists at all: the dataset pipeline's Layer-2 gates hold a cost
invariant — they establish facts about the data from metadata only, never from
an aggregation over it (ADR-0006). B8 needs one fact the metastore does not
hold, ``max(|value|)`` per column, and parquet already carries it: every row
group's footer records a min and a max per column, written by the writer as it
went. Reading them is a seek to the end of each file.

The read goes through the Spark JVM's Hadoop ``FileSystem`` — the door this repo
already uses for a Hive table's files (``utils/hdfs.copy_hdfs_to_local``), and
not pyarrow, whose own HDFS client needs a second JVM loaded into the Python
process. Why that choice and not the alternatives: ADR-0006's 2026-09-03
amendment.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)


def partition_value(path: str, key: str) -> str | None:
    """The value Hive partition ``key`` takes in ``path``, or None if absent.

    Matches on ``/<key>=`` so a key never matches as the suffix of a longer one
    (``date`` must not be read out of ``snap_date=``): that failure would be
    silent, collapsing every partition under one wrong value rather than raising.
    """
    marker = f"/{key}="
    idx = path.find(marker)
    if idx < 0:
        return None
    rest = path[idx + len(marker):]
    end = rest.find("/")
    return rest if end < 0 else rest[:end]


def group_by_partition(
    paths: Iterable[str], key: str
) -> dict[str, list[str]]:
    """Group file paths by the value of Hive partition ``key``.

    Paths that do not carry the key are dropped rather than grouped under a
    ``None`` bucket: the caller is asking "which files belong to which
    partition", and a file with no answer belongs to none of them.
    """
    grouped: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        value = partition_value(path, key)
        if value is not None:
            grouped[value].append(path)
    return dict(grouped)


def filter_by_partitions(
    paths: Iterable[str], partition_filter: Mapping[str, str]
) -> list[str]:
    """``paths`` narrowed to those whose Hive partition values match every pair.

    Why a caller needs this at all: ``DataFrame.inputFiles()`` answers for the
    whole relation, not for the ``partition_filter`` the catalog put in the
    ``WHERE`` when it loaded the table. A gate that skipped this step would read
    every dataset version's and every variant's files and report on rows this
    run never wrote.

    A path that does not carry one of the keys is dropped rather than kept:
    ``partition_value`` returns ``None`` for it, which is not the declared
    value, and counting a file whose partition cannot be established would be
    the silent half of the same mistake. Returns sorted paths so two runs over
    the same partitions read the same way.
    """
    return sorted(
        p for p in paths
        if all(partition_value(p, k) == v for k, v in partition_filter.items())
    )


def read_row_count(spark, paths: Sequence[str]) -> int:
    """Total rows over ``paths``, summed from footers.

    Every parquet row group's footer records how many rows it holds — the same
    ``getRowCount()`` :func:`read_max_abs_stats` already reads to interpret a
    column chunk's min/max. So a row count costs one seek per file and is
    independent of how many rows the file holds, which is what keeps a caller
    inside the dataset gates' cost invariant (ADR-0006) where ``df.count()``
    would not be.

    No "statistics missing" case exists here, unlike the per-column read: the
    row count is structural in the format rather than optional statistics, so
    every footer has one. An empty ``paths`` is 0 — a caller that needs to tell
    "no rows" from "no files found" must check the paths itself, because from
    here the two are the same number.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    no_filter = jvm.org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader

    total = 0
    for path in paths:
        footer = reader.readFooter(hadoop_conf, hadoop_path(path), no_filter)
        for block in footer.getBlocks():
            total += block.getRowCount()
    return total


def _stat_max_abs(statistics, row_count: int) -> float | None:
    """``max(|value|)`` for one column chunk, or None when unmeasurable.

    Three cases, and telling them apart is the point:

    - statistics present  -> ``max(|min|, |max|)``. Both ends are read because a
      column's largest magnitude is as likely to be its minimum (a negative
      balance, a signed delta); reading ``max`` alone would pass every such
      column silently.
    - all values null     -> ``0.0``. The writer records no min/max but does
      record ``num_nulls``, and ``num_nulls == row_count`` says the column holds
      nothing that could lose precision.
    - anything else       -> ``None``, meaning the writer left no usable
      statistics. The caller decides what that means; this function does not
      guess a value it cannot see.

    Values arrive as strings (``minAsString``) rather than through py4j's
    numeric conversion so BOOLEAN — whose statistics read "true"/"false" — does
    not have to be a second code path.
    """
    if statistics.hasNonNullValue():
        return max(
            abs(_parse_stat(statistics.minAsString())),
            abs(_parse_stat(statistics.maxAsString())),
        )
    if statistics.isNumNullsSet() and statistics.getNumNulls() == row_count:
        return 0.0
    return None


def _parse_stat(raw: str) -> float:
    """Parse one footer statistic. BOOLEAN reads "true"/"false", not a number."""
    text = raw.strip().lower()
    if text == "true":
        return 1.0
    if text == "false":
        return 0.0
    return float(text)


def read_max_abs_stats(
    spark,
    paths: Sequence[str],
    columns: Sequence[str],
) -> dict[str, float | None]:
    """``{column: max(|value|)}`` over ``paths``, from footers alone.

    A column maps to ``None`` when no file left usable statistics for it —
    including the case where it is absent from the files entirely. That is
    reported rather than raised because "unmeasurable" is a fact the caller has
    a policy for (B8's ``numeric_precision_policy``), not an error this reader
    can resolve. Any single unusable chunk poisons the column: a maximum taken
    over the row groups that happened to record one is not the column's maximum.

    Every returned key comes from ``columns``, so a caller can index the result
    without checking membership.
    """
    result: dict[str, float | None] = {c: 0.0 for c in columns}
    seen: dict[str, bool] = {c: False for c in columns}
    wanted = set(columns)

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    no_filter = jvm.org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader

    for path in paths:
        footer = reader.readFooter(hadoop_conf, hadoop_path(path), no_filter)
        for block in footer.getBlocks():
            row_count = block.getRowCount()
            for chunk in block.getColumns():
                name = chunk.getPath().toDotString()
                if name not in wanted:
                    continue
                seen[name] = True
                if result[name] is None:
                    continue
                chunk_max = _stat_max_abs(chunk.getStatistics(), row_count)
                if chunk_max is None:
                    logger.warning(
                        "parquet footer carries no usable statistics for "
                        "column '%s' in %s", name, path,
                    )
                    result[name] = None
                else:
                    result[name] = max(result[name], chunk_max)

    for col in columns:
        if not seen[col]:
            result[col] = None
    return result

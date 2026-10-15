"""Which files this run landed, and what their parquet footers record.

The facts the B10 gate stands on. It holds the dataset pipeline's cost
invariant — facts from metadata, never from an aggregation over the data
(ADR-0006) — and parquet carries what it needs in every file's footer: a row
count per row group, written by the writer as it went. Reading it is a seek to
the end of each file:

* **Which files.** :func:`filter_by_partitions` narrows a file listing to the
  partition scope the table was written under — the dataset version, and for
  the train tables the train variant. It is the one version filter here;
  :func:`landed_partition_files` narrows its result further to the months a
  plan wrote.
* **What the footers say.** :func:`read_row_count` reads them;
  :func:`footer_rows` and :func:`footer_rows_by_month` put both questions
  together for a landed frame.

**Why select by version at all.** What ``DataFrame.inputFiles()`` lists for a
frame the catalog loaded (``HiveTableDataset.load()``, a ``WHERE`` on the
partition filter) depends on two Spark settings. Measured on one table holding
two versions, local ``[*]``, 2026-09-26:

* both at Spark's default (``spark.sql.hive.manageFilesourcePartitions`` and
  ``spark.sql.hive.convertMetastoreParquet`` true) — only the loaded version's
  files. The version filter here changes nothing.
* ``manageFilesourcePartitions`` false — every version's files. The filter is
  what keeps a gate on the rows this run wrote; without it the gate reports on
  another version's parquet, and nothing says so.
* ``convertMetastoreParquet`` false — the table's root directory alone, with
  no partition in the path. Nothing here can select a file from that: B10
  finds none in scope and reports a failure to measure, which stops the run.

This repo sets neither key; a deployment's cluster may. So the filter stays —
a string comparison per path — and
``tests/test_pipelines/test_dataset/test_footer_facts.py`` pins it on a frame
that lists two versions, the second case.

The read goes through the Spark JVM's Hadoop ``FileSystem`` — the door this repo
already uses for a Hive table's files (``utils/hdfs.copy_hdfs_to_local``), and
not pyarrow, whose own HDFS client needs a second JVM loaded into the Python
process. Why that choice and not the alternatives: ADR-0006's 2026-09-03
amendment. Paths are matched as text rather than through Spark filters, which is
also why the only pyspark import here is for type hints.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


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

    The version (and variant) filter every footer fact here is read under; the
    module docstring says under which Spark settings it is what keeps a gate on
    this run's version. Values compare as the partition directory's text: a
    version ID is a hash the pipeline itself wrote, so there is one spelling of
    it.

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


def landed_partition_files(
    paths: Iterable[str],
    *,
    base_version: str,
    time_col: str,
    months: Iterable,
) -> list[str]:
    """The files under ``base_version`` whose partition month is in ``months``.

    Two narrowings, for two different reasons:

    * ``base_version`` — :func:`filter_by_partitions`, the one version filter
      in this module (the module docstring says why it is kept).
    * ``months`` — B10's test pairing is incremental with the build that
      wrote it (ADR-0002): a month that already landed is not re-read.

    Months are compared as calendar days rather than as strings. The partition
    value is whatever the source column held (``20260131`` and ``2026-01-31``
    are both legal spellings of one month — A26 exists because the two can
    disagree), and a string comparison would answer "no files" for a spelling
    mismatch, which reads exactly like "nothing to check".

    A partition value that is not a date at all — Hive's
    ``__HIVE_DEFAULT_PARTITION__`` for null, most obviously — is skipped rather
    than raised on: it is not one of the months the plan asked for, so it is not
    this function's problem to report. Returns sorted paths so two runs over the
    same partitions read the same way.
    """
    wanted = {pd.Timestamp(m).normalize() for m in months}
    if not wanted:
        return []
    scoped = filter_by_partitions(paths, {"base_dataset_version": base_version})
    selected: list[str] = []
    for value, files in group_by_partition(scoped, time_col).items():
        try:
            landed = pd.Timestamp(value).normalize()
        except ValueError:
            continue
        if landed in wanted:
            selected.extend(files)
    return sorted(selected)


def read_row_count(spark, paths: Sequence[str]) -> int:
    """Total rows over ``paths``, summed from footers.

    Every parquet row group's footer records how many rows it holds
    (``getRowCount()``). So a row count costs one seek per file and is
    independent of how many rows the file holds, which is what keeps a caller
    inside the dataset gates' cost invariant (ADR-0006) where ``df.count()``
    would not be.

    No "statistics missing" case exists here: the row count is structural in
    the format rather than optional column statistics, so every footer has
    one. An empty ``paths`` is 0 — a caller that needs to tell
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


def footer_rows(
    df: DataFrame,
    scope: Mapping[str, str],
) -> tuple[int, int, int]:
    """``(rows, files in scope, files the table has)`` — footer arithmetic only.

    For a table compared whole. It counts, it does not judge: whether a table
    that has files but none in ``scope`` is an error is B10's to say
    (``model_input_grain_scope_errors`` in ``core/consistency.py``), and
    returning the two file counts separately is what leaves that to it.
    """
    all_paths = df.inputFiles()
    files = filter_by_partitions(all_paths, scope)
    return read_row_count(df.sparkSession, files), len(files), len(all_paths)


def footer_rows_by_month(
    df: DataFrame,
    *,
    base_version: str,
    time_col: str,
    months: list,
) -> dict[pd.Timestamp, tuple[int, int]]:
    """``{month: (rows, files)}`` under ``base_version`` — footer arithmetic
    only, for a table written one month partition at a time.

    Each month's files are :func:`landed_partition_files`' — so a partition
    value spelled differently from the plan's month is still found. A month
    with no file is ``(0, 0)``: whether that is fine is the caller's decision.
    """
    paths = df.inputFiles()
    counted = {}
    for month in months:
        files = landed_partition_files(
            paths, base_version=base_version, time_col=time_col, months=[month],
        )
        counted[pd.Timestamp(month)] = (
            read_row_count(df.sparkSession, files), len(files),
        )
    return counted

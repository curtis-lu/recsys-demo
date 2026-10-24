"""Which files this run landed, and how B10 counts their rows.

The facts the B10 gate stands on. It holds the I/O half of the dataset
gates' cost invariant (ADR-0006): it reads file listings and parquet footers,
never column data. The other half — a cost independent of how many rows the
data holds — no longer holds for B10 since #521: the executors step through
every row to count it, CPU that grows with the rows and has not been measured
at production scale. Two kinds of fact, from two places:

* **Which files.** :func:`filter_by_partitions` narrows a file listing to the
  partition scope the table was written under — the dataset version, and for
  the train tables the train variant. It is the one version filter here;
  :func:`landed_partition_files` narrows its result further to the months a
  plan wrote. Both are string work on ``DataFrame.inputFiles()``, no read.
* **How many rows.** Spark's ``count()`` on the frame the catalog loaded:
  :func:`plan_table_count` / :func:`table_rows` for a table compared whole,
  :func:`plan_monthly_count` / :func:`monthly_rows` for one compared a month
  at a time. Planning and running are separate calls so that B10's cost
  guard can read the plan in between (:func:`scan_leaves`, judged by
  ``model_input_grain_count_plan_errors`` in ``core/consistency.py``).

**What ``count()`` costs.** On a parquet table Spark plans the count as a
``FileSourceScanExec`` that needs no column, and such a scan reads each file's
footer and no column chunk — the same I/O as reading the footers directly.
The executors still step through the rows to count them: no column data, but
CPU that grows with the row count, unmeasured at production scale. The I/O
claim holds only for that plan, which is what the guard checks — under
``spark.sql.hive.convertMetastoreParquet`` false the same call reads every row
through the Hive SerDe. Until #521 this module
read the footers itself, on the driver, one py4j round trip per file: on a
table split into tens of thousands of small files that loop was B10's whole
cost, and threads did not shorten it. Measurements: ADR-0006's 2026-10-16
amendment.

**Known limit: files Spark skips are not counted, and nothing says so.**
With ``spark.sql.files.ignoreCorruptFiles`` true the count passes over a file
it cannot read and comes out short; the plan is the same, so the guard lets
it through (measured in review: a 1,000-row table counted 873). The footer
read this replaced raised on such a file. ``ignoreMissingFiles`` is inferred
to behave the same way, not measured. One side short reads as a B10
mismatch that looks like a duplicated join key; both sides short can cancel
out and pass. This repo sets neither key.

**Why select by version at all.** The rows are counted on the frame the
catalog loaded (``HiveTableDataset.load()``, a ``WHERE`` on the partition
filter), so they are this version's whatever the listing says — provided
the entry has a ``partition_filter`` at all. Without one the count adds up
every version in the table and the scope pre-check, which reads only the
listing, cannot see it; ``tests/test_pipelines/test_dataset/test_pipeline.py``
pins all eight of B10's entries to the node's scope keys. The listing is
another matter: what ``DataFrame.inputFiles()`` lists for such a frame depends
on two Spark settings. Measured on one table holding two versions, local
``[*]``, 2026-09-26:

* both at Spark's default (``spark.sql.hive.manageFilesourcePartitions`` and
  ``spark.sql.hive.convertMetastoreParquet`` true) — only the loaded version's
  files. The version filter here changes nothing.
* ``manageFilesourcePartitions`` false — every version's files. The filter is
  what keeps B10's scope pre-check, the file counts in its report and the
  test months it finds on this run's version; without it they would describe
  another version's parquet, and nothing would say so.
* ``convertMetastoreParquet`` false — the table's root directory alone, with
  no partition in the path. Nothing here can select a file from that, and the
  count would be a full read: B10's cost guard refuses before counting.

This repo sets neither key; a deployment's cluster may. So the filter stays —
a string comparison per path — and
``tests/test_pipelines/test_dataset/test_footer_facts.py`` pins it on a frame
that lists two versions, the second case.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, NamedTuple
from urllib.parse import unquote

import pandas as pd
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import COUNT_SCAN_NODE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def partition_value(path: str, key: str) -> str | None:
    """The value Hive partition ``key`` takes in ``path``, or None if absent.

    Matches on ``/<key>=`` so a key never matches as the suffix of a longer one
    (``date`` must not be read out of ``snap_date=``): that failure would be
    silent, collapsing every partition under one wrong value rather than raising.

    The value comes back unescaped twice, because ``path`` is a URI as
    ``DataFrame.inputFiles()`` lists it: once for the URI (``%20`` for a
    space), once for the escaping Spark and Hive give a partition directory's
    name (``%3A`` for ``:``, ``%25`` for ``%`` itself, so the two layers never
    blur). A TIMESTAMP time column's month ``2026-01-31 00:00:00`` is listed as
    ``2026-01-31%2000%253A00%253A00``; read as is it was no date at all, and
    B10 compared a test month's 0 files with 0 files and passed (#558).
    """
    marker = f"/{key}="
    idx = path.find(marker)
    if idx < 0:
        return None
    rest = path[idx + len(marker):]
    end = rest.find("/")
    return unquote(unquote(rest if end < 0 else rest[:end]))


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

    The version (and variant) filter every file fact here is read under; the
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


def _as_month(value) -> pd.Timestamp | None:
    """``value`` as a calendar day, or None when it is not a date at all."""
    try:
        return pd.Timestamp(str(value)).normalize()
    except ValueError:
        return None


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
        if _as_month(value) in wanted:
            selected.extend(files)
    return sorted(selected)


class ScanLeaf(NamedTuple):
    """One leaf of a count's physical plan, as B10's cost guard reads it."""

    #: The plan node's class, e.g. ``FileSourceScanExec``.
    node: str
    #: Data columns the scan reads (its ``requiredSchema``); ``None`` where
    #: the node is not a file scan and has no such schema.
    columns_read: int | None


def scan_leaves(counting: DataFrame) -> list[ScanLeaf]:
    """The leaves of ``counting``'s physical plan — where it reads from.

    Read off ``queryExecution().sparkPlan()``: the planner's output before
    ``prepareForExecution``, so before adaptive execution wraps the plan.
    Its leaves are the same scans with AQE on and off (the tests run both);
    the plan AQE executes is only settled while it runs, which is too late
    for a check meant to stop the run first. Builds the plan, reads nothing.
    """
    leaves = counting._jdf.queryExecution().sparkPlan().collectLeaves()
    found = []
    for i in range(leaves.size()):
        leaf = leaves.apply(i)
        node = leaf.getClass().getSimpleName()
        # Only a file scan has a required schema to ask about.
        columns = (
            leaf.requiredSchema().length() if node == COUNT_SCAN_NODE else None
        )
        found.append(ScanLeaf(node, columns))
    return found


class TableCount(NamedTuple):
    """A table compared whole: its row count, planned and not yet run, and
    the file counts B10's scope pre-check reads."""

    #: The aggregate whose one row is the table's row count — what
    #: ``df.count()`` runs, kept as a frame so its plan can be read first.
    counting: DataFrame
    files_in_scope: int
    files_in_table: int


def plan_table_count(df: DataFrame, scope: Mapping[str, str]) -> TableCount:
    """Plan ``df``'s row count and list its files; read nothing.

    The count is the whole frame as the catalog loaded it — its ``WHERE`` is
    already the scope. The two file counts come from the listing, narrowed by
    :func:`filter_by_partitions`: whether a table with files but none in
    ``scope`` is an error is B10's to say (``model_input_grain_scope_errors``
    in ``core/consistency.py``), and returning both is what leaves that to it.
    """
    all_paths = df.inputFiles()
    return TableCount(
        counting=df.groupBy().count(),
        files_in_scope=len(filter_by_partitions(all_paths, scope)),
        files_in_table=len(all_paths),
    )


def table_rows(planned: TableCount) -> int:
    """Run the count :func:`plan_table_count` planned."""
    return planned.counting.collect()[0][0]


class MonthlyCount(NamedTuple):
    """A table compared a month at a time: its per-month row count, planned
    and not yet run, and the files found for each month."""

    #: ``groupBy(<time column>).count()`` over the months found on disk, or
    #: ``None`` when no planned month has a file: there is nothing to count.
    counting: DataFrame | None
    #: Every planned month, as a calendar day, to the files found for it.
    files: dict[pd.Timestamp, int]


def plan_monthly_count(
    df: DataFrame,
    *,
    base_version: str,
    time_col: str,
    months: Iterable,
) -> MonthlyCount:
    """Plan ``df``'s row count per month of ``months``; read nothing.

    Which partitions hold a planned month is :func:`landed_partition_files`'
    answer, so a partition spelled differently from the plan's month is still
    found. The count then filters on those partitions' own values, as text: a
    partition column is whatever type the table declares or Spark infers
    (``20260131`` reads back as an integer, ``2026-01-31`` as a date), and its
    text is the spelling the listing found.

    No planned month with a file builds no count at all rather than one over
    an empty filter: Spark plans that as an empty local relation, not a file
    scan, and the cost guard would refuse a count that reads nothing.
    """
    paths = df.inputFiles()
    files: dict[pd.Timestamp, int] = {}
    values: set[str] = set()
    for month in months:
        landed = landed_partition_files(
            paths, base_version=base_version, time_col=time_col, months=[month],
        )
        files[pd.Timestamp(month).normalize()] = len(landed)
        values.update(partition_value(p, time_col) for p in landed)
    counting = None
    if values:
        counting = (
            df.where(F.col(time_col).cast("string").isin(sorted(values)))
            .groupBy(time_col)
            .count()
        )
    return MonthlyCount(counting, files)


def monthly_rows(planned: MonthlyCount) -> dict[pd.Timestamp, tuple[int, int]]:
    """Run the count :func:`plan_monthly_count` planned: ``{month: (rows,
    files)}`` for every planned month.

    A month with no file is ``(0, 0)``: whether that is fine is the caller's
    decision. Two spellings of one month on disk are summed into it, the way
    their files were both found for it.
    """
    rows = dict.fromkeys(planned.files, 0)
    if planned.counting is not None:
        for value, count in planned.counting.collect():
            rows[_as_month(value)] += count
    return {month: (rows[month], planned.files[month]) for month in planned.files}

"""Mechanisms for the B8 numeric-precision gate.

What lives here is "which files hold the months this run just wrote" — the one
step between a plan (``SnapDatePlan``) and a footer read
(``utils/parquet_stats``). The decision the gate makes about those numbers stays
in the node, and the rule itself stays in ``core/consistency.py``; this module
only turns a month plan into a file list.

Kept apart from ``steps/scoping.py`` because that module is months against
*frames* (Spark filters, Spark type coercion) while this is months against
*paths*, and mixing them would put a pyspark import in front of a rule that
needs none.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from recsys_tfb.utils.parquet_stats import group_by_partition, partition_value


def landed_partition_files(
    paths: Iterable[str],
    *,
    base_version: str,
    time_col: str,
    months: Iterable,
) -> list[str]:
    """The files under ``base_version`` whose partition month is in ``months``.

    Two filters, for two different reasons:

    * ``base_version`` — ``DataFrame.inputFiles()`` answers for the whole
      relation, not for the ``partition_filter`` the catalog applies when it
      loads. Without this the gate would read a different dataset version's
      parquet and report on values this run never wrote.
    * ``months`` — the gate is incremental with the node that wrote them
      (ADR-0002): a month that already landed is not re-read.

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
    scoped = [
        p for p in paths
        if partition_value(p, "base_dataset_version") == base_version
    ]
    selected: list[str] = []
    for value, files in group_by_partition(scoped, time_col).items():
        try:
            landed = pd.Timestamp(value).normalize()
        except ValueError:
            continue
        if landed in wanted:
            selected.extend(files)
    return sorted(selected)

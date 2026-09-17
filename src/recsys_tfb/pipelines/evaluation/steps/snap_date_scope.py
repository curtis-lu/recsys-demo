"""The evaluated dates (one or several): which they are, keeping only their rows,
and whether those rows were written under today's settings.

``enriched_eval_predictions`` holds every month this ``model_version`` has been
evaluated on: ``prepare_eval_data`` writes one time partition per evaluated date,
and the catalog entry's ``partition_filter`` prunes ``model_version`` only. So
every node that reads the table starts by keeping the evaluated dates (ADR-0018
decision 1). A reader that forgets raises nothing and computes over every month;
``tests/test_pipelines/test_evaluation/test_pipeline.py`` fails any node wired to
the table whose body does not call :func:`restrict_to_current_eval_partitions`.

``evaluation.snap_date`` is one date or a list of dates (#374). Several dates are
evaluated together: their query groups (time × entity) go into one set of
metrics and one report, not one per date. One date's partition can therefore be
written by runs with different output directories (March alone, January–March),
so each row carries the settings its partition was written under
(:func:`stamp_partition_fingerprint`) and the readers check it
(:func:`restrict_to_current_eval_partitions`); why the run directory's JSON
cannot, see ``steps/config_fingerprint.py::PARTITION_FINGERPRINT_COLUMN``.

Why not put the dates in the catalog's ``partition_filter``:
``HiveTableDataset.load`` drops filter columns from the frame it returns, and the
time column is half of every query group.

Why the steps take ``parameters`` instead of the dates: the config key is read
in one function, :func:`eval_snap_dates`, so the S6 literal-column registry holds
one entry for it rather than one per reader.
"""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, NamedTuple

from pyspark.sql import functions as F

from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    PARTITION_FINGERPRINT_COLUMN,
    PARTITION_FINGERPRINT_KEYS,
    partition_fingerprint,
    recorded_partition_fingerprint,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def eval_snap_dates(parameters: dict) -> list[str]:
    """``evaluation.snap_date`` as a list of date texts, in configured order.

    One date gives a one-element list. Each is stripped and otherwise not
    reformatted: the time partition is a STRING written from rows selected with
    this same setting, so comparing text with text is exact. Raises when no date
    is configured.
    """
    dates = as_date_list((parameters.get("evaluation") or {}).get("snap_date"))
    if not dates:
        raise ValueError(
            "evaluation.snap_date not configured. Set evaluation.snap_date "
            "(ISO YYYY-MM-DD) in conf/base/parameters_evaluation.yaml."
        )
    return dates


def _on_dates(time_col: str, dates: list[str]) -> Column:
    """The row condition "the time column is one of ``dates``".

    ``cast("string")`` on a STRING partition column is a no-op, so Spark keeps
    pruning partitions. One date compares with ``=``: the form ADR-0018 checked
    in a physical plan (``PartitionFilters: [(snap_date = …)]``, ``DataFilters:
    []``), the comparison ``prepare_eval_data`` has always filtered with, so a
    single-date run's plan is unchanged. Several dates use ``IN``; on Spark 3.3.2
    a partitioned parquet table read that way plans ``PartitionFilters:
    [as_of IN (…)]``, ``DataFilters: []`` and lists only the matching partition
    paths (checked 2026-09-17, local). Not ``to_date``: whether that form still
    prunes a string partition was never measured.
    """
    column = F.col(time_col).cast("string")
    if len(dates) == 1:
        return column == dates[0]
    return column.isin(dates)


def restrict_to_eval_snap_dates(df: DataFrame, parameters: dict) -> DataFrame:
    """``df`` restricted to the evaluated dates."""
    time_col = get_schema(parameters)["time"]
    return df.filter(_on_dates(time_col, eval_snap_dates(parameters)))


def eval_snap_dates_without_rows(df: DataFrame, parameters: dict) -> list[str]:
    """The evaluated dates ``df`` holds no row for, in configured order.

    Checked per date (:func:`_first_row_per_date`), so a missing month is named
    even when the other months have rows: a check on the restricted frame as a
    whole passes as soon as any one date has a row.
    """
    time_col = get_schema(parameters)["time"]
    dates = eval_snap_dates(parameters)
    found = _first_row_per_date(df, time_col, dates, F.lit(True))
    return [date for date in dates if date not in found]


def _first_row_per_date(
    df: DataFrame, time_col: str, dates: list[str], value: Column,
) -> dict:
    """``{date: value}`` from one row of each date's rows in ``df``; a date
    with no row is absent.

    One action and one Spark job whatever the number of dates, each date a
    one-row read of its own partition: per date ``filter → coalesce(1) →
    limit(1)``, the branches unioned and collected once. Nothing aggregates or
    scans a whole column. Measured on Spark 3.3.2 with AQE on (the evaluation
    tests' local session, in-memory and partitioned-parquet sources alike,
    counted with ``SparkContext.statusTracker``): 1 job for one date and for
    three. Without ``coalesce(1)`` the same union took 4 jobs for three dates,
    since each branch's ``limit`` then needs a single-partition shuffle of its
    own; a plain ``limit(1).collect()`` on a frame with several input
    partitions can take 2, scanning them in batches. ``coalesce(1)`` reads a
    date's files in one task, and the limit stops that task at the first row.
    """
    parts = [
        df.filter(_on_dates(time_col, [date]))
        .select(F.lit(date).alias("date"), value.alias("value"))
        .coalesce(1)
        .limit(1)
        for date in dates
    ]
    union = reduce(lambda left, right: left.unionByName(right), parts)
    return {row["date"]: row["value"] for row in union.collect()}


def stamp_partition_fingerprint(
    df: DataFrame, parameters: dict, joined: list[str],
) -> DataFrame:
    """``df`` with :data:`PARTITION_FINGERPRINT_COLUMN` set to
    :func:`partition_fingerprint` of today's settings and ``joined``, the
    segment columns this run actually joined, on every row.
    ``prepare_eval_data`` writes the partition through this."""
    return df.withColumn(
        PARTITION_FINGERPRINT_COLUMN,
        F.lit(partition_fingerprint(parameters, joined)),
    )


class EvalPartitionsNotCurrentError(ValueError):
    """Some evaluated date's partition was not written for what the reader
    reads it as (:func:`restrict_to_current_eval_partitions`).

    Carries ``dates_without_rows`` from the same check, so a caller that also
    refuses dates without rows (the ``--compare-only`` gate) can name both in
    one raise instead of stopping at the first kind.
    """

    def __init__(self, message: str, dates_without_rows: list[str]):
        super().__init__(message)
        self.dates_without_rows = dates_without_rows


class EvalPartitions(NamedTuple):
    """What :func:`restrict_to_current_eval_partitions` read.

    ``frame`` is the table kept to the evaluated dates, fingerprint column
    dropped. ``dates_without_rows`` are the evaluated dates with no row, in
    configured order: found by the same Spark job that read the fingerprints,
    and left to the caller, since no rows is not a partition written under
    other settings (``compute_metrics``' postcondition and the ``--compare-only``
    gate each refuse it with their own message).
    """

    frame: DataFrame
    dates_without_rows: list[str]


def restrict_to_current_eval_partitions(
    df: DataFrame, parameters: dict, segment_columns: dict, *,
    recorded_settings: bool = False,
) -> EvalPartitions:
    """The evaluated dates' rows of ``enriched_eval_predictions``, after
    checking each date's partition was written for what the caller reads it
    as.

    ``segment_columns`` is the ``evaluation_segment_columns`` this run's
    directory holds: the reader groups by its ``joined`` list, so a partition
    must have been written with exactly those columns joined. The settings
    part is today's (``parameters``), except with ``recorded_settings``, which
    only ``--compare-only`` passes: there the settings recorded in that same
    JSON (:func:`recorded_partition_fingerprint`). ``--post-training`` is inert
    on that path, so today's value of it says nothing about which population an
    earlier standard run wrote (the reason that path never checked the JSON
    against today's settings, #352 correction). Everywhere else today's
    settings are what the rows are read for, the ``--compare`` mode included:
    a slice such as ``--compare X --only-node generate_comparison_report``
    reads partitions an earlier run wrote, whose directory JSON says the same
    old settings as they do.

    Pre-check (inputs), ``ValueError``, collect-all: every evaluated date whose
    rows carry another fingerprint (another run rewrote that date since, under
    other settings: another run mode, other segment settings), or none (NULL,
    or no column at all: written before partitions carried one, #374). Fixed by
    rewriting them: ``--from-node prepare_eval_data``, or under
    ``--compare-only`` a standard run of those dates first.

    Cost: every reader of the table pays one extra small action — one Spark
    job reading one row per evaluated date (:func:`_first_row_per_date`); the
    number of dates only adds branches to that action, and no column is
    scanned whole. The ``--compare-only`` gate's own per-date emptiness check
    comes out of the same action (``dates_without_rows``).

    The premise, the same for one date and for several: a partition is written
    by one dynamic overwrite, so every row in it carries the same fingerprint
    and one row answers for all of them. Rows put into a partition any other
    way can hide a second value from this check.

    B side of a comparison: not checked here. A compared model's table carries
    its own run's settings, so ``steps/compare_sources.py`` only drops the
    column.
    """
    time_col = get_schema(parameters)["time"]
    dates = eval_snap_dates(parameters)
    joined = (segment_columns.get("joined")
              if isinstance(segment_columns, dict) else None)
    if not isinstance(joined, list):
        raise ValueError(
            "evaluation_segment_columns (segment_columns.json) has no 'joined' "
            "list, so which segment columns its partitions hold is unknown. "
            "Re-run with --from-node prepare_eval_data."
        )
    if not recorded_settings:
        current = partition_fingerprint(parameters, joined)
        against = ("the current settings and the segment columns this run's "
                   "directory joined")
    else:
        current = recorded_partition_fingerprint(segment_columns)
        against = ("the settings and segment columns recorded in "
                   "evaluation_segment_columns (segment_columns.json) of this "
                   "run's directory")
        if current is None:
            raise ValueError(
                "evaluation_segment_columns (segment_columns.json) has no "
                "config_fingerprint, so the settings its partitions were "
                "written under are unknown: written before fingerprints "
                "existed. Re-run the standard evaluation of these dates."
            )
    fingerprints = _first_row_per_date(
        df, time_col, dates,
        F.col(PARTITION_FINGERPRINT_COLUMN)
        if PARTITION_FINGERPRINT_COLUMN in df.columns
        else F.lit(None).cast("string"),
    )

    stale = []
    for date in dates:
        if date not in fingerprints:
            continue
        found = fingerprints[date]
        if found is None:
            stale.append(
                f"  - {date}: written before partitions carried a settings "
                "fingerprint (#374)"
            )
        elif found != current:
            stale.append(
                f"  - {date}: written under other settings (the settings "
                "changed since, or a later run rewrote this date: one over "
                "other dates that include it, in the other run mode, or when "
                "the population table had other segment columns)"
            )
    dates_without_rows = [d for d in dates if d not in fingerprints]
    if stale:
        message = (
            "enriched_eval_predictions holds evaluated date(s) not written under "
            f"{against} ({', '.join(PARTITION_FINGERPRINT_KEYS)}, joined "
            f"{joined}); reading them would evaluate rows those settings do "
            "not make:\n"
            + "\n".join(stale)
            + "\nRe-run with --from-node prepare_eval_data, which rewrites them "
            "(under --compare-only, which has no prepare_eval_data, re-run the "
            "standard evaluation of these dates first)."
        )
        raise EvalPartitionsNotCurrentError(message, dates_without_rows)
    return EvalPartitions(
        frame=df.filter(_on_dates(time_col, dates))
        .drop(PARTITION_FINGERPRINT_COLUMN),
        dates_without_rows=dates_without_rows,
    )

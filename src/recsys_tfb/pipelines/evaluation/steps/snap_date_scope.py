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

from typing import TYPE_CHECKING, NamedTuple, Optional

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

    One ``isEmpty()`` per date, each on that date's partition alone, so a
    missing month is named even when the other months have rows: a check on the
    restricted frame as a whole passes as soon as any one date has a row. With
    one date this is the single ``isEmpty()`` a single-date run always paid.
    """
    time_col = get_schema(parameters)["time"]
    return [
        date for date in eval_snap_dates(parameters)
        if df.filter(_on_dates(time_col, [date])).isEmpty()
    ]


def stamp_partition_fingerprint(df: DataFrame, parameters: dict) -> DataFrame:
    """``df`` with :data:`PARTITION_FINGERPRINT_COLUMN` set to today's
    :func:`partition_fingerprint` on every row. ``prepare_eval_data`` writes
    the partition through this."""
    return df.withColumn(
        PARTITION_FINGERPRINT_COLUMN, F.lit(partition_fingerprint(parameters))
    )


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
    df: DataFrame, parameters: dict, *, written_with: Optional[dict] = None,
) -> EvalPartitions:
    """The evaluated dates' rows of ``enriched_eval_predictions``, after
    checking each date's partition was written under the settings the caller
    reads it for.

    Those settings are today's (:func:`partition_fingerprint` of
    ``parameters``), or, when ``written_with`` is given, the ones recorded in
    that landed ``evaluation_segment_columns`` payload
    (:func:`recorded_partition_fingerprint`). The comparison readers pass it:
    under ``--compare-only`` the partition is read for what an earlier standard
    run of this directory wrote, and ``post_training`` is inert there, so
    today's value of it says nothing about the partition (the reason that path
    never checked the JSON against today's settings, #352 correction). In the
    other modes the same run wrote that JSON, so the two agree.

    Pre-check (inputs), ``ValueError``, collect-all: every evaluated date whose
    rows carry another fingerprint (another run rewrote that date since, under
    other settings: another run mode, other segment settings), or none (NULL,
    or no column at all: written before partitions carried one, #374). Fixed by
    rewriting them: ``--from-node prepare_eval_data``, or under
    ``--compare-only`` a standard run of those dates first.

    Cost: one Spark action (one ``collect``). With one date, ``limit(1)`` on
    that partition, the size of the ``isEmpty()`` it stands in for: a partition
    is written by one dynamic overwrite, so every row in it carries the same
    fingerprint and one row answers for all (the premise this check rests on;
    rows put into a partition any other way can hide a second value from it).
    With several dates, the distinct ``(date, fingerprint)`` pairs of all of
    them in one collect, not one action per date: a scan of that one column
    over those partitions, where a partition showing more than one value is
    reported as written under other settings.

    B side of a comparison: not checked here. A compared model's table carries
    its own run's settings, so ``steps/compare_sources.py`` only drops the
    column.
    """
    time_col = get_schema(parameters)["time"]
    dates = eval_snap_dates(parameters)
    if written_with is None:
        current, against = partition_fingerprint(parameters), "the current settings"
    else:
        current = recorded_partition_fingerprint(written_with)
        against = ("the settings recorded in evaluation_segment_columns "
                   "(segment_columns.json) of this run's directory")
        if current is None:
            raise ValueError(
                "evaluation_segment_columns (segment_columns.json) has no "
                "config_fingerprint, so the settings its partitions were "
                "written under are unknown: written before fingerprints "
                "existed. Re-run the standard evaluation of these dates."
            )
    kept = df.filter(_on_dates(time_col, dates))
    fingerprints = _partition_fingerprints(kept, time_col, dates)

    stale = []
    for date in dates:
        found = fingerprints.get(date)
        if found is None:
            continue
        if any(value is None for value in found):
            stale.append(
                f"  - {date}: written before partitions carried a settings "
                "fingerprint (#374)"
            )
        elif set(found) != {current}:
            stale.append(
                f"  - {date}: written under other settings (the settings "
                "changed since, or a later run rewrote this date: one over "
                "other dates that include it, or in the other run mode)"
            )
    if stale:
        raise ValueError(
            "enriched_eval_predictions holds evaluated date(s) not written under "
            f"{against} ({', '.join(PARTITION_FINGERPRINT_KEYS)}); reading "
            "them would evaluate rows those settings do not make:\n"
            + "\n".join(stale)
            + "\nRe-run with --from-node prepare_eval_data, which rewrites them "
            "(under --compare-only, which has no prepare_eval_data, re-run the "
            "standard evaluation of these dates first)."
        )
    return EvalPartitions(
        frame=kept.drop(PARTITION_FINGERPRINT_COLUMN),
        dates_without_rows=[d for d in dates if d not in fingerprints],
    )


def _partition_fingerprints(
    kept: DataFrame, time_col: str, dates: list[str],
) -> dict[str, list[Optional[str]]]:
    """``{date: fingerprints seen}`` for the evaluated dates ``kept`` has rows
    for, in one Spark job (see :func:`restrict_to_current_eval_partitions`)."""
    value = (
        F.col(PARTITION_FINGERPRINT_COLUMN)
        if PARTITION_FINGERPRINT_COLUMN in kept.columns
        else F.lit(None).cast("string")
    ).alias("fingerprint")
    if len(dates) == 1:
        rows = kept.select(value).limit(1).collect()
        return {dates[0]: [rows[0]["fingerprint"]]} if rows else {}
    found: dict[str, list[Optional[str]]] = {}
    for row in (
        kept.select(F.col(time_col).cast("string").alias("date"), value)
        .distinct()
        .collect()
    ):
        found.setdefault(row["date"], []).append(row["fingerprint"])
    return found

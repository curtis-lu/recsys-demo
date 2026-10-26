"""The evaluated dates (one or several): which they are, keeping only their rows,
and whether those rows were written under today's settings; and which dates a
frame holds (:func:`snap_dates_in_frame`).

``enriched_eval_predictions`` holds every month this ``model_version`` has been
evaluated on: ``prepare_eval_data`` writes one time partition per evaluated date,
and the catalog entry's ``partition_filter`` prunes ``model_version`` only. So
every node that reads the table starts by keeping the evaluated dates (ADR-0018
decision 1). A reader that forgets raises nothing and computes over every month.

``evaluation.snap_date`` is one date or a list of dates. Several dates are
evaluated together: their query groups (time × entity) go into one set of
metrics and one report, not one per date. One date's partition can therefore be
written by runs with different output directories, so each row carries the
settings its partition was written under
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

from recsys_tfb.core.consistency import DataConsistencyError
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

    Each is stripped and otherwise not reformatted: the time partition is a STRING written from rows selected with
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
    in a physical plan, so a single-date run's plan is unchanged. Several dates
    use ``IN``, which on Spark 3.3.2 still lists only the matching partition
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


def eval_snap_dates_without_rows(
    df: DataFrame, parameters: dict, *, time_partitioned: bool = True,
) -> list[str]:
    """The evaluated dates ``df`` holds no row for, in configured order.

    Checked per date: a check on the restricted frame as a whole passes as soon
    as any one date has a row.

    ``time_partitioned`` says whether ``df`` is read from a table partitioned by
    the time column, as the framework's own tables are. Then one job reads one
    row per date (:func:`_first_row_per_date`). A user's table (the
    ``external_hive`` compare source) may not be: the date filter then prunes
    nothing, and that one-task read would scan the whole table in a single task
    when a date is missing. There each date gets its own ``isEmpty``, which
    Spark scans in parallel: one job per date, bounded by the date count.
    """
    time_col = get_schema(parameters)["time"]
    dates = eval_snap_dates(parameters)
    if not time_partitioned:
        return [
            date for date in dates
            if df.filter(_on_dates(time_col, [date])).isEmpty()
        ]
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
    scans a whole column. Without ``coalesce(1)`` the same union takes more
    jobs (measured on Spark 3.3.2 with AQE on: 4 for three dates, 1 with it),
    since each branch's ``limit`` then needs a single-partition shuffle of its
    own. ``coalesce(1)`` reads a date's files in one task, and the limit stops
    that task at the first row.
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
    segment columns this run actually joined, on every row."""
    return df.withColumn(
        PARTITION_FINGERPRINT_COLUMN,
        F.lit(partition_fingerprint(parameters, joined)),
    )


class EvalPartitionsNotCurrentError(ValueError):
    """Some evaluated date's partition was not written for what the reader
    reads it as (:func:`restrict_to_current_eval_partitions`).

    Carries ``dates_without_rows`` from the same check, so a caller that also
    refuses dates without rows can name both in one raise.
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
    other settings.
    """

    frame: DataFrame
    dates_without_rows: list[str]


def restrict_to_current_eval_partitions(
    df: DataFrame, parameters: dict, segment_columns: dict, *,
    recorded_settings: bool = False, rerun_advice: str | None = None,
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
    earlier standard run wrote. Everywhere else today's
    settings are what the rows are read for, the ``--compare`` mode included:
    a slice such as ``--compare X --from-node compute_compare_metrics``
    reads partitions an earlier run wrote, whose directory JSON says the same
    old settings as they do.

    Pre-check (inputs), ``ValueError``, collect-all: every evaluated date whose
    rows carry another fingerprint (another run rewrote that date since, under
    other settings: another run mode, other segment settings), or none (NULL,
    or no column at all: written before partitions carried one). Fixed by
    rewriting them: ``--from-node prepare_eval_data``, or under
    ``--compare-only`` a standard run of those dates first; a reader in a
    mode with no ``prepare_eval_data`` of its own passes ``rerun_advice``,
    the sentence that ends the message instead.

    Cost: every reader of the table pays one extra small action — one Spark
    job reading one row per evaluated date (:func:`_first_row_per_date`); the
    number of dates only adds branches to that action, and no column is
    scanned whole.

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
                "config_fingerprint in the shape this version writes, so the "
                "settings its partitions were written under are unknown: "
                "written by an older version. Re-run the standard evaluation "
                "of these dates."
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
            + "\n".join(stale) + "\n"
            + (rerun_advice or (
                "Re-run with --from-node prepare_eval_data, which rewrites "
                "them (under --compare-only, which has no prepare_eval_data, "
                "re-run the standard evaluation of these dates first)."))
        )
        raise EvalPartitionsNotCurrentError(message, dates_without_rows)
    return EvalPartitions(
        frame=df.filter(_on_dates(time_col, dates))
        .drop(PARTITION_FINGERPRINT_COLUMN),
        dates_without_rows=dates_without_rows,
    )


def _standard_run_command(parameters: dict) -> str:
    """The standard evaluation run that writes the partitions a reader of
    this mode reads: same model, same run mode. ``--env`` is not in
    ``parameters``, so it stays a placeholder. ``__main__.py`` spells the same
    command itself: the CLI may not import ``steps/`` (S3)."""
    mode = " --post-training" if parameters.get("post_training") else ""
    model_version = parameters.get("model_version") or "<model_version>"
    return (f"python -m recsys_tfb evaluation --env <env> "
            f"--model-version {model_version}{mode}")


def require_partitions_of_the_standard_run(
    df: DataFrame, parameters: dict, segment_columns: dict,
) -> DataFrame:
    """The evaluated dates' rows of ``enriched_eval_predictions`` for a mode
    that writes no partition itself (``--prediction-quality``), after
    checking the standard run of the same run mode wrote every one of them.

    Pre-check (inputs), ``DataConsistencyError``, collect-all: a date with no
    row (no standard run evaluated it yet), and a date whose partition
    another run mode or other settings wrote since
    (:func:`restrict_to_current_eval_partitions`, whose fingerprint holds
    ``post_training``). Both name the standard run to do first
    (:func:`_standard_run_command`), not ``--from-node prepare_eval_data``:
    this mode has no such node. The same one Spark job as that check.
    """
    advice = (f"Run `{_standard_run_command(parameters)}` first, then this "
              "mode again.")
    not_current = None
    try:
        partitions = restrict_to_current_eval_partitions(
            df, parameters, segment_columns, rerun_advice=advice)
        missing = partitions.dates_without_rows
    except EvalPartitionsNotCurrentError as error:
        not_current, missing = error, error.dates_without_rows
    problems = []
    if missing:
        # The not-current message ends with the advice; say it once.
        problems.append(
            f"enriched_eval_predictions holds no row for evaluated date(s) "
            f"{missing}: no standard evaluation of this model in this run "
            f"mode has written them."
            + ("" if not_current is not None else f" {advice}"))
    if not_current is not None:
        problems.append(str(not_current))
    if problems:
        # ``from None``: the cause's text is already in the message.
        raise DataConsistencyError("\n".join(problems)) from None
    return partitions.frame


def snap_dates_in_frame(frame: DataFrame, time_col: str) -> list[str]:
    """The distinct time values ``frame`` holds, as text, in the order Spark
    returns them; one small collect."""
    return [str(r[time_col]) for r in frame.select(time_col).distinct().collect()]

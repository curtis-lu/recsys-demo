"""The evaluated dates (one or several): which they are, and keeping only their rows.

``enriched_eval_predictions`` holds every month this ``model_version`` has been
evaluated on: ``prepare_eval_data`` writes one time partition per evaluated date,
and the catalog entry's ``partition_filter`` prunes ``model_version`` only. So
every node that reads the table starts by keeping the evaluated dates (ADR-0018
decision 1). A reader that forgets raises nothing and computes over every month;
``tests/test_pipelines/test_evaluation/test_pipeline.py`` fails any node wired to
the table whose body does not call :func:`restrict_to_eval_snap_dates`.

``evaluation.snap_date`` is one date or a list of dates (#374). Several dates are
evaluated together: their query groups (time × entity) go into one set of
metrics and one report, not one per date.

Why not put the dates in the catalog's ``partition_filter``:
``HiveTableDataset.load`` drops filter columns from the frame it returns, and the
time column is half of every query group.

Why the steps take ``parameters`` instead of the dates: the config key is read
in one function, :func:`eval_snap_dates`, so the S6 literal-column registry holds
one entry for it rather than one per reader.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.core.schema import get_schema

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

"""Months against frames: restricting a frame to a set of months, and checking
a frame has the months a run is about to read.

Split out from the month-plan module rather than living beside it: everything
here touches Spark, and ``month_plans.py`` is pinned to zero pyspark imports
(S2) so its test module stays off the 2-4 minute Spark cold start. Deriving
*which* months a run touches is pure; asking a frame about them is not.

One restriction form, :func:`months_filter_as_date`, for every read in the
dataset pipeline (ADR-0029 decision 3). There used to be a second one that
compared the column as it came, and which of the two a node used decided
whether a time column stored as STRING matched its months or matched nothing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def months_filter_as_date(time_col: str, dates: list):
    """``time_col IN dates``, normalised to DATE on both sides.

    ``time_col`` reaches these nodes with two different types depending on where
    the frame came from: a real DATE/TIMESTAMP when read from a source table,
    but a **string** when read back from a Hive table where snap_date is a
    partition column (``partition_cols: {name: snap_date, type: STRING}``) —
    which is what the runner does, since it reloads every node input through the
    catalog. ``F.col(snap_date).isin([pd.Timestamp(...)])`` matches **zero** rows
    against the string form while raising nothing, so the comparison must be
    pinned to DATE on both sides.

    An empty ``dates`` list is a normal state here (every configured month
    already landed) and gets an explicit constant-false predicate: ``isin([])``
    is not a dependable "match nothing".
    """
    if not dates:
        return F.lit(False)
    return F.to_date(F.col(time_col)).isin([pd.Timestamp(d).date() for d in dates])


def require_months_present(
    df: DataFrame, time_col: str, months: list, what: str,
    table: str = "feature_table",
) -> None:
    """Pre-check: every month in ``months`` exists in ``df``.

    Fail loud rather than fit / encode a smaller window: a dataset must be
    reproducible from ``feature_table``, so a month that quietly went missing
    upstream would otherwise produce a complete-looking artifact built on less
    data than its config claims.

    ``what`` names the config key in the message (``train_snap_dates`` for the
    preprocessor fit, ``snap_dates`` for the months a run is about to encode);
    ``table`` names the frame, because two feature tables ask it (ADR-0026) and
    "feature_table is missing a month" about the other one would send the
    operator to the wrong table.

    A month is present when a row's time value falls on it, compared as a
    date the way :func:`months_filter_as_date` reads it — so a TIMESTAMP with
    a time of day counts for its day, as the reads it guards would count it.

    Cost: one ``distinct().collect()`` over the time column **of those months
    only** — the filter comes first, so the question is not asked of a table's
    whole history, which only grows (ADR-0029 decision 1). What lands on the
    driver is bounded by ``months``, not by row count.
    """
    present = {
        row[0]
        for row in df.filter(months_filter_as_date(time_col, months))
        .select(F.to_date(F.col(time_col))).distinct().collect()
    }
    require_months_in(present, months, what, table)


def require_months_in(present, months: list, what: str, table: str) -> None:
    """Pre-check on months already collected: every month in ``months`` is in
    ``present``.

    :func:`require_months_present` minus the collect, for a caller whose own
    scan already returned the months it saw (the candidate table's B8 scan).
    One definition of the message either way.
    """
    present = {pd.Timestamp(d) for d in present if d is not None}
    missing = sorted({pd.Timestamp(d) for d in months} - present)
    if missing:
        raise ValueError(
            f"{table} missing required {what}: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}. A time column "
            f"stored as text counts only when written YYYY-MM-DD."
        )


def months_present_and_max_abs(
    df: DataFrame, time_col: str, months: list, columns: list[str],
) -> tuple[set, dict[str, float]]:
    """One aggregation over ``df``'s ``months``: which of them hold any row, and
    each of ``columns``' largest absolute value.

    The facts B8 needs about a table this framework does not write (the
    candidate-level feature table, ADR-0026): there are no footers of its own
    to read, and its format is the deployment's. The month set comes out of the
    same scan so the coverage check costs nothing extra.

    Values are compared as doubles: ``abs`` refuses a boolean, and the bound
    B8 compares against sits far below where a double stops being exact. A
    column with no non-NULL value over those months reports ``0.0`` — the
    ``ColumnPrecision`` convention for a column with nothing to lose.

    Cost: one scan of those months; one row per month reaches the driver.
    """
    rows = (
        df.filter(months_filter_as_date(time_col, months))
        .groupBy(F.to_date(F.col(time_col)).alias("__month"))
        .agg(
            F.count(F.lit(1)).alias("__rows"),
            *[F.max(F.abs(F.col(c).cast("double"))).alias(c) for c in columns],
        )
        .collect()
    )
    present = {row["__month"] for row in rows}
    max_abs = {
        c: max((row[c] for row in rows if row[c] is not None), default=0.0)
        for c in columns
    }
    return present, max_abs

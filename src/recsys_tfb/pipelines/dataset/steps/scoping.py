"""Months against frames: restricting a frame to a set of months, and checking
a frame has the months a run is about to read.

Split out from the month-plan module rather than living beside it: everything
here touches Spark, and ``month_plans.py`` is pinned to zero pyspark imports
(S2) so its test module stays off the 2-4 minute Spark cold start. Deriving
*which* months a run touches is pure; asking a frame about them is not.

Two restriction forms live here, and the difference is what an empty list means:

* :func:`restrict_to_months` — an empty list selects **no rows**.
* :func:`restrict_to_months_or_all` — an empty list leaves the frame **whole**.

They sit adjacent so the choice cannot be made by accident. The second is
sampling's historical shape (``select_keys`` returned the unfiltered pool when a
split had no configured months); #170 preserved it rather than tightening it,
because tightening would be a behaviour change. It is unreachable today — both
callers read a required config key — but "unreachable" is not "safe to change
while refactoring".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _date_filter(time_col: str, dates: list):
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


def restrict_to_months(df: DataFrame, time_col: str, months: list) -> DataFrame:
    """``df`` restricted to ``months``, compared in the source-table form.

    Deliberately *not* :func:`_date_filter`: the callers are the nodes reading
    ``sample_pool`` / ``feature_table``, source tables whose time column is a
    real DATE. Normalising here would silently widen what matches, which is not
    a change a behaviour-preserving refactor may make; the normalised form stays
    where it is needed, on the frames read back from Hive.

    An empty ``months`` list selects no rows — see the module docstring for the
    other form.
    """
    return df.filter(F.col(time_col).isin([pd.Timestamp(d) for d in months]))


def restrict_to_months_or_all(df: DataFrame, time_col: str, months: list) -> DataFrame:
    """``df`` restricted to ``months``, or left whole when ``months`` is empty."""
    if not months:
        return df
    return restrict_to_months(df, time_col, months)


def require_months_present(
    df: DataFrame, time_col: str, months: list, what: str,
) -> None:
    """Pre-check: every month in ``months`` exists in ``df``.

    Fail loud rather than fit / encode a smaller window: a dataset must be
    reproducible from ``feature_table``, so a month that quietly went missing
    upstream would otherwise produce a complete-looking artifact built on less
    data than its config claims.

    ``what`` names the config key in the message (``train_snap_dates`` for the
    preprocessor fit, ``snap_dates`` for the months a run is about to encode);
    both callers ask this of ``feature_table``, which is why the subject is
    fixed in the text.

    Cost: one ``distinct().collect()`` over the time column. What lands on the
    driver is bounded by the month count (typically 12-52), not by row count.
    """
    present = {
        row[time_col]
        for row in df.select(time_col).distinct().collect()
    }
    present = {pd.Timestamp(d) for d in present if d is not None}
    missing = sorted({pd.Timestamp(d) for d in months} - present)
    if missing:
        raise ValueError(
            f"feature_table missing required {what}: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

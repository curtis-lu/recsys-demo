"""Applying a month plan to a frame: the ``time_col IN dates`` predicate.

Split out from the month-plan module rather than living beside it: this returns
a Spark :class:`~pyspark.sql.Column`, and ``month_plans.py`` is pinned to zero
pyspark imports (S2) so its test module stays off the 2-4 minute Spark cold
start. Deriving *which* months a run touches is pure; turning that answer into a
filter is not.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import functions as F


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

"""The popularity baseline's purchase counts as the report reads them (Spark;
what reaches the driver is per item, or per item and month).

Which score ranks the baseline is decided in ``compute_baseline_metrics``; the
counting itself is ``evaluation.baselines``'. This module turns those counts
into the landed ``purchase_counts`` and ``monthly_counts``.
"""

from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.baselines import (
    compute_monthly_purchase_counts,
    compute_monthly_purchase_counts_by_window,
)


def purchase_counts_per_item(
    counts: SparkDataFrame, parameters: dict,
) -> dict[str, int]:
    """``{item: count}``, ``counts`` (``baselines.compute_purchase_counts``)
    summed over the evaluated dates; with one date, that date's count."""
    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    return {
        str(r[item_col]): int(r[score_col])
        for r in counts.groupBy(item_col)
        .agg(F.sum(F.col(score_col)).alias(score_col))
        .collect()
    }


def monthly_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> tuple[dict[str, dict[str, int]], Optional[dict[str, int]]]:
    """``(monthly_counts, window_months_covered)`` over the same lookback
    windows as :func:`purchase_counts_per_item`:

    * ``monthly_counts`` — ``{item: {month: count}}``, summed over the
      windows; summed over months, each item reconciles with
      ``purchase_counts``.
    * ``window_months_covered`` — ``{date: months with label rows}`` per
      evaluated date's window, sorted by date; ``None`` with one date.

    With several dates the counts are kept apart per window in one scan of
    ``label_table``, so one collect gives both. The per-month average divides
    by the sum of those, not by one window's lookback, because
    ``purchase_counts`` is summed over every window
    (``baselines.popularity_window``).
    """
    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    if len(snap_dates) == 1:
        monthly_counts: dict[str, dict[str, int]] = {}
        monthly = compute_monthly_purchase_counts(
            label_table, snap_dates, lookback_months, parameters
        )
        for r in (
            monthly.groupBy("month", item_col)
            .agg(F.sum(F.col(score_col)).alias(score_col))
            .collect()
        ):
            monthly_counts.setdefault(str(r[item_col]), {})[str(r["month"])] = int(
                r[score_col]
            )
        return monthly_counts, None
    months_by_window: dict[str, set] = {s: set() for s in snap_dates}
    summed: dict[str, dict[str, float]] = {}
    for r in compute_monthly_purchase_counts_by_window(
        label_table, snap_dates, lookback_months, parameters
    ).collect():
        month = str(r["month"])
        months_by_window[r["window_date"]].add(month)
        per_item = summed.setdefault(str(r[item_col]), {})
        per_item[month] = per_item.get(month, 0.0) + r[score_col]
    monthly_counts = {
        item: {month: int(v) for month, v in per_month.items()}
        for item, per_month in summed.items()
    }
    window_months_covered = {
        s: len(months_by_window[s]) for s in sorted(snap_dates)
    }
    return monthly_counts, window_months_covered

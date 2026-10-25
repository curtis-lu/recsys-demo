"""The popularity baseline's purchase counts as the report reads them (Spark;
what reaches the driver is per item, or per item and month).

Which score ranks the baseline is decided in ``compute_baseline_metrics``; the
counting itself is ``evaluation.baselines``'. This module turns those counts
into the landed ``purchase_counts`` and ``monthly_counts``, and the rate
mode's period counts into its report numbers (``positive_rate_block``).
"""

from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.baselines import (
    CANDIDATES_COL,
    POSITIVES_COL,
    compute_monthly_candidate_counts_by_window,
    compute_monthly_candidate_counts_in_windows,
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


def positive_rate_block(period_counts, snap_dates, lookback_months, parameters):
    """The rate mode's report numbers, from the period counts.

    Over the periods the evaluated dates' windows cover, **each period once**
    however many windows cover it: summing per window would multiply an
    overlapping period's candidates by the number of dates, and the printed
    denominator is the reader's cue for an unreliable rate. With several
    dates this is their pooled rate, not the one any single date ranked by.
    Both collects are small: the table is one row per period × item.

    * ``rate``: ``positives ÷ candidates``, per item.
    * ``candidates``, ``positives``: per item.
    * ``monthly_positives``: ``positives`` per item and calendar month.
    * ``window_months_covered``: per window, how many of its months have
      candidate rows.
    """
    item_col = get_schema(parameters)["item"]
    candidates: dict[str, int] = {}
    positives: dict[str, int] = {}
    monthly: dict[str, dict[str, int]] = {}
    for r in compute_monthly_candidate_counts_in_windows(
        period_counts, snap_dates, lookback_months, parameters
    ).collect():
        item, month = str(r[item_col]), str(r["month"])
        candidates[item] = candidates.get(item, 0) + int(r[CANDIDATES_COL])
        positives[item] = positives.get(item, 0) + int(r[POSITIVES_COL])
        monthly.setdefault(item, {})[month] = int(r[POSITIVES_COL])
    months_by_window: dict[str, set] = {}
    for r in compute_monthly_candidate_counts_by_window(
        period_counts, snap_dates, lookback_months, parameters
    ).select("window_date", "month").distinct().collect():
        months_by_window.setdefault(r["window_date"], set()).add(str(r["month"]))
    return {
        "rate": {
            item: (positives[item] / c if c else 0.0)
            for item, c in candidates.items()
        },
        "candidates": candidates,
        "positives": positives,
        "monthly_positives": monthly,
        "window_months_covered": {
            w: len(m) for w, m in sorted(months_by_window.items())
        },
    }

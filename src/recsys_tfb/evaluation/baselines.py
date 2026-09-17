"""Popularity baseline for evaluation — Spark.

Replaces each ``eval_predictions`` row's model score with the product's
historical purchase count (sum of positive labels in a pre-snap_date
window), yielding a global-popularity ranking aligned row-for-row with the
model's evaluation set.
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def resolve_lookback_months(parameters: dict) -> int:
    """``evaluation.baseline.lookback_months``, defaulting to 12.

    The single place both the node (``compute_baseline_metrics``) and the
    report (``build_baseline_section``) read this from — before this helper
    existed the two had drifted to two different defaults (node: 12, report:
    ``None``), so a run using the implicit default silently printed no
    lookback sentence at all while the node had in fact used 12 months
    (bug 1, ADR-0020).
    """
    eval_params = parameters.get("evaluation", {}) or {}
    return int(
        (eval_params.get("baseline", {}) or {}).get("lookback_months", 12)
    )


def _window_bounds(snap_date: str, lookback_months: int) -> tuple[str, str]:
    """``(lower, upper)`` of the ``[snap_date - lookback_months, snap_date)``
    window, as ``YYYY-MM-DD``. Shared by every windowing path here so the
    windows cannot drift apart."""
    upper = pd.Timestamp(snap_date)
    lower = upper - pd.DateOffset(months=lookback_months)
    return str(lower.date()), str(upper.date())


def _lookback_window(
    label_table: SparkDataFrame,
    snap_date: str,
    lookback_months: int,
    ts,
) -> SparkDataFrame:
    """``label_table`` rows in ``[snap_date - lookback_months, snap_date)``.

    ``ts`` is the caller's date-typed time expression. Shared by the total and
    monthly count paths so their windowing stays identical.

    Pre-check (input): the window must be non-empty. It used to fall back to
    the full ``label_table`` when empty — which includes the evaluation
    month's own answers, so the popularity baseline would then rank by the
    ground truth it is supposed to be compared against, while the report
    kept printing an unqualified "reranked by N months of history" sentence
    (bug 1). The user ruled baseline is load-bearing information: raise
    rather than degrade to that silently-leaky stub. Only
    ``evaluation.report.sections.baseline: false`` skips this computation
    entirely (``compute_baseline_metrics`` returns before calling in here).
    Not a ``core/consistency.py`` gate: this is the baseline's own scoring
    window, not the B2 feature-leakage invariant.
    """
    lower, upper = _window_bounds(snap_date, lookback_months)
    window = label_table.filter((ts >= F.lit(lower)) & (ts < F.lit(upper)))
    if window.limit(1).count() == 0:
        available = sorted({
            str(r[0])
            for r in label_table.select(
                F.date_format(ts, "yyyy-MM")
            ).distinct().collect()
        })
        raise ValueError(
            f"No label_table history in [{lower}, {upper}) "
            f"for evaluation.snap_date={snap_date!r} (lookback_months="
            f"{lookback_months}). label_table has months: {available}. "
            "The baseline cannot be scored without pre-snap_date history — "
            "backfill label_table further back, lower "
            "evaluation.baseline.lookback_months, or set "
            "evaluation.report.sections.baseline: false to skip the "
            "baseline section entirely."
        )
    return window


def compute_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(snap_date, prod_name)`` historical purchase count.

    For each ``S`` in ``snap_dates``, count ``sum(label)`` grouped by item
    over ``label_table`` rows whose time falls in
    ``[S - lookback_months, S)``. Raises if a window is empty — see
    ``_lookback_window``'s pre-check (bug 1, ADR-0020): it no longer falls
    back to the full table.

    Returns a DataFrame with columns ``(time_col, item_col, score_col)``
    where ``score_col`` holds the count and ``time_col`` is the string ``S``.
    """
    if not snap_dates:
        raise ValueError(
            "compute_purchase_counts requires a non-empty snap_dates list"
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    ts = F.to_date(F.col(time_col))
    per_snap: list[SparkDataFrame] = []
    for s in snap_dates:
        upper = pd.Timestamp(s)
        window = _lookback_window(label_table, s, lookback_months, ts)
        counts = (
            window.groupBy(item_col)
            .agg(F.sum(F.col(label_col)).cast("double").alias(score_col))
            .withColumn(time_col, F.lit(str(upper.date())))
        )
        per_snap.append(counts.select(time_col, item_col, score_col))

    result = per_snap[0]
    for df in per_snap[1:]:
        result = result.unionByName(df)
    return result


def compute_monthly_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(calendar-month, prod_name)`` purchase count within the windows.

    Same windowing (and empty-window raise, bug 1) as
    ``compute_purchase_counts``, but instead of collapsing each
    ``[S - lookback_months, S)`` window into one
    number it breaks the count down by the label row's calendar month
    (``yyyy-MM``). Drives the report's monthly popularity trend.

    Months are unioned across ``snap_dates`` and summed by the caller — for a
    single-snap eval this is exact; overlapping multi-snap windows double-count
    shared months exactly as ``compute_purchase_counts`` does at the window
    level, so a product's summed monthly counts still reconcile with its total.

    Returns a DataFrame with columns ``("month", item_col, score_col)`` where
    ``month`` is the ``yyyy-MM`` string and ``score_col`` holds the count.
    """
    if not snap_dates:
        raise ValueError(
            "compute_monthly_purchase_counts requires a non-empty snap_dates "
            "list"
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    ts = F.to_date(F.col(time_col))
    month_expr = F.date_format(ts, "yyyy-MM")
    per_snap: list[SparkDataFrame] = []
    for s in snap_dates:
        window = _lookback_window(label_table, s, lookback_months, ts)
        counts = (
            window.withColumn("month", month_expr)
            .groupBy("month", item_col)
            .agg(F.sum(F.col(label_col)).cast("double").alias(score_col))
        )
        per_snap.append(counts.select("month", item_col, score_col))

    result = per_snap[0]
    for df in per_snap[1:]:
        result = result.unionByName(df)
    return result


def compute_monthly_purchase_counts_by_window(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(window_date, calendar-month, item)`` purchase count, one scan.

    For several evaluated dates (#374): the counts
    :func:`compute_monthly_purchase_counts` gives, kept apart per window
    (``window_date`` is the evaluated date the window ends at) instead of
    summed. The caller needs both the per-month trend and how many months with
    label rows each window had, and the months of one window cannot be told
    from the summed ones: windows overlap, and a boundary month can hold rows
    for one window and not the other.

    One scan of ``label_table``: it is filtered once to the span every window
    falls in, then joined with the small table of window bounds (one row per
    evaluated date, so the broadcast is bounded by config). A per-window
    filter unioned together would read the table once per date.

    Same window rule as the other paths (:func:`_window_bounds`). It does not
    repeat the empty-window pre-check: a window with no label rows gives no
    rows here, and :func:`compute_purchase_counts`, which the baseline node
    calls first, raises for that window.

    Returns columns ``("window_date", "month", item_col, score_col)``.
    """
    if not snap_dates:
        raise ValueError(
            "compute_monthly_purchase_counts_by_window requires a non-empty "
            "snap_dates list"
        )

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    bounds = [(s, *_window_bounds(s, lookback_months)) for s in snap_dates]
    span_lower = min(lower for _, lower, _ in bounds)
    span_upper = max(upper for _, _, upper in bounds)
    windows = label_table.sparkSession.createDataFrame(
        bounds, "window_date string, window_lower string, window_upper string"
    )
    ts = F.to_date(F.col(time_col))
    rows = label_table.filter(
        (ts >= F.lit(span_lower)) & (ts < F.lit(span_upper))
    ).withColumn("_label_day", ts)
    in_window = (
        (F.col("_label_day") >= F.to_date(F.col("window_lower")))
        & (F.col("_label_day") < F.to_date(F.col("window_upper")))
    )
    return (
        rows.join(F.broadcast(windows), in_window, "inner")
        .withColumn("month", F.date_format(F.col("_label_day"), "yyyy-MM"))
        .groupBy("window_date", "month", item_col)
        .agg(F.sum(F.col(label_col)).cast("double").alias(score_col))
        .select("window_date", "month", item_col, score_col)
    )


def build_baseline_frame(
    eval_predictions: SparkDataFrame,
    purchase_counts: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Replace ``eval_predictions``' model score with the popularity count.

    Drops the model's ``score`` (and ``rank`` / ``model_version`` if present),
    casts ``time_col`` to string for a type-safe join, then left-joins the
    per-``(snap_date, prod_name)`` count as the new ``score``. Products with
    no count get ``score = 0``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    drop_cols = [
        c for c in (score_col, rank_col, "model_version")
        if c in eval_predictions.columns
    ]
    base = eval_predictions.drop(*drop_cols).withColumn(
        time_col, F.to_date(F.col(time_col)).cast("string")
    )
    return base.join(
        F.broadcast(purchase_counts), on=[time_col, item_col], how="left"
    ).fillna(0, subset=[score_col])

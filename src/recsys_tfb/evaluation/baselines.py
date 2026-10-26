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

from recsys_tfb.core.date_ranges import lookback_window_bounds
from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def resolve_lookback_months(parameters: dict) -> int:
    """``evaluation.baseline.lookback_months``, defaulting to 12.

    The single place this is read: a second reader with its own default lets
    the node (``compute_baseline_metrics``) and the report disagree, and an
    implicit default then prints no lookback sentence while the node used 12
    months (bug 1, ADR-0020). The report does not read it; the node lands the
    value it used (:func:`popularity_window`).
    """
    eval_params = parameters.get("evaluation", {}) or {}
    return int(
        (eval_params.get("baseline", {}) or {}).get("lookback_months", 12)
    )


def popularity_window(
    monthly: dict,
    window_months_covered: dict | None,
    *,
    snap_dates: list,
    lookback_months: int,
    score: str,
) -> dict:
    """The lookback window as the report prints it (ADR-0031 principle 1).
    Landed by ``compute_baseline_metrics`` as
    ``result_readers.POPULARITY_WINDOW_KEY``.

    ``monthly`` is the ``{item: {month: count}}`` table the report prints
    (label_table's ``monthly_counts``, or sample_pool's ``monthly_positives``
    in rate mode); it holds only months with rows inside the window, so its
    distinct months are the coverage (``months_covered``).
    ``window_months_covered`` is each evaluated date's months with rows in
    its own window; ``None`` with one evaluated date (``snap_dates``), whose
    window's months are ``monthly``'s.

    bug 1 (ADR-0020), partial window: a window with rows in only some of its
    lookback months raises nothing (``_lookback_window`` refuses only an
    empty one), and dividing by the full lookback understates the per-month
    average. So ``window_partial`` is ``0 < months_covered < lookback`` and
    ``per_month_divisor`` is the covered months when partial, the lookback
    otherwise. With several windows the counts are summed over all of them,
    so the divisor is the window-months behind that sum (each window's
    covered months under the same partial rule) and the window is partial
    when that falls short of ``full_window_months`` (windows × lookback).
    Distinct months of ``monthly`` cannot stand in for those: overlapping
    windows share months, and a boundary month can hold rows for one window
    and not the other.

    ``lookback_months`` and ``score`` (as configured: monitoring with
    ``rate`` still ranks by the count) are the settings it was counted with,
    landed so the report does not read ``parameters``.
    """
    months_covered = len({mo for per in monthly.values() for mo in per})
    if window_months_covered is None:
        if len(snap_dates) != 1:
            raise ValueError(
                f"popularity_window: {len(snap_dates)} evaluated dates but no "
                f"per-window coverage; only one date's window is the monthly "
                f"table's months.")
        window_months_covered = {str(snap_dates[0]): months_covered}
    window_partial = 0 < months_covered < lookback_months
    per_month_divisor = months_covered if window_partial else lookback_months
    full_window_months = len(window_months_covered) * lookback_months
    if len(window_months_covered) > 1:
        per_month_divisor = sum(
            c if 0 < c < lookback_months else lookback_months
            for c in window_months_covered.values()
        )
        window_partial = per_month_divisor < full_window_months
    return {
        "lookback_months": lookback_months,
        "score": score,
        "months_covered": months_covered,
        "window_months_covered": dict(window_months_covered),
        "full_window_months": full_window_months,
        "per_month_divisor": per_month_divisor,
        "window_partial": window_partial,
    }


def _window_bounds(snap_date: str, lookback_months: int) -> tuple[str, str]:
    """``(lower, upper)`` of the ``[snap_date - lookback_months, snap_date)``
    window, as ``YYYY-MM-DD``. Every windowing path here goes through it so the
    windows cannot drift apart; the rule is
    ``core.date_ranges.lookback_window_bounds``, which the ``--rebuild-dates``
    check (A21) uses too."""
    return lookback_window_bounds(snap_date, lookback_months)


def _lookback_window(
    label_table: SparkDataFrame,
    snap_date: str,
    lookback_months: int,
    ts,
) -> SparkDataFrame:
    """``label_table`` rows in ``[snap_date - lookback_months, snap_date)``.

    ``ts`` is the caller's date-typed time expression.

    Pre-check (input): the window must be non-empty. Falling back to the full
    ``label_table`` would include the evaluation month's own answers, so the
    popularity baseline would rank by the ground truth it is compared
    against while the report kept printing an unqualified "reranked by N
    months of history" sentence (bug 1). Raise rather than degrade to that
    silently-leaky stub. Only
    ``evaluation.report.sections.popularity_baseline: false`` skips this computation
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
            "evaluation.report.sections.popularity_baseline: false to skip the "
            "popularity-baseline section entirely."
        )
    return window


def compute_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(snap_date, prod_name)`` historical purchase count.

    For each ``S`` in ``snap_dates``, ``sum(label)`` by item over
    ``label_table`` rows in ``[S - lookback_months, S)``. Raises if a window
    is empty: ``_lookback_window``'s pre-check (bug 1, ADR-0020).

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
    ``compute_purchase_counts``, but broken down by the label row's calendar
    month (``yyyy-MM``) instead of one number per window.

    Months are unioned across ``snap_dates`` and summed by the caller:
    overlapping multi-snap windows double-count shared months exactly as
    ``compute_purchase_counts`` does at the window level, so a product's
    summed monthly counts still reconcile with its total.

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

    The counts :func:`compute_monthly_purchase_counts` gives, kept apart per
    window (``window_date`` is the evaluated date the window ends at) instead
    of summed. The caller needs how many months with label rows each window
    had, and the months of one window cannot be told from the summed ones:
    windows overlap, and a boundary month can hold rows for one window and
    not the other.

    One scan of ``label_table``: filtered once to the span every window falls
    in, then joined with the small table of window bounds (one row per
    evaluated date). A per-window filter unioned together would read the
    table once per date.

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

    ``time_col`` is cast to string for a type-safe join. Products with no
    count get ``score = 0``.
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


# ---------------------------------------------------------------------------
# Positive-rate mode (#397)
# ---------------------------------------------------------------------------
#
# score = positives / times the item was a candidate, both over the lookback
# window. The denominator is counted from sample_pool, whose rows are by
# definition one candidate each (CONTEXT.md: 來源表, 候選), never from
# label_table's row count: a label_table holding positives only makes every
# rate 1.
#
# The counts are kept per time value in ``popularity_period_counts`` so a
# period is joined once: they depend on sample_pool, label_table and the
# schema only, never on the model.
#
# Only under --post-training. A popularity score has to describe the rows it
# is scored on: the test rows are drawn from sample_pool, so a rate over
# sample_pool's candidates matches them. Monitoring scores offline
# inference's full grid (every entity × every item, unshown pairs labelled
# 0), where whether a row is positive depends on how often the item was
# shown as much as on its rate once shown — which is what the positive count
# measures. There a rate would weaken the baseline and inflate the model's
# lead, so monitoring keeps the count (#397 decision 5).

#: Column names of ``popularity_period_counts`` next to schema time and item.
CANDIDATES_COL = "n_candidates"
POSITIVES_COL = "n_positives"


def baseline_score(parameters: dict) -> str:
    """``evaluation.baseline.score``: ``"count"`` (absent) or ``"rate"``.

    The value domain is checked at the evaluation command's entry (A50); this
    reader only supplies the default.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    return (eval_params.get("baseline", {}) or {}).get("score") or "count"


def baseline_scores_by_rate(parameters: dict, *, post_training: bool) -> bool:
    """Whether this run's baseline scores by positive rate.

    Needs ``score: rate`` and ``--post-training`` (monitoring keeps the
    count, see above). Whether a baseline is computed at all is not asked
    here: the evaluation pipeline's requirement list answers it (ADR-0031
    principle 3).
    """
    return post_training and baseline_score(parameters) == "rate"


def list_candidate_periods(
    sample_pool: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> list[str]:
    """The time values sample_pool holds inside any date's lookback window.

    These are the periods ``popularity_period_counts`` must hold for this run,
    and the only ones the baseline sums. Listed from the data because the time
    grain is the deployment's and sample_pool declares no partitions to list.
    One Spark job: a distinct over the time column of the rows inside the
    windows. With sample_pool partitioned by time it reads those partitions'
    time column only; unpartitioned, it scans the whole time column.
    """
    time_col = get_schema(parameters)["time"]
    ts = F.to_date(F.col(time_col))
    in_any = None
    for s in snap_dates:
        lower, upper = _window_bounds(str(s), lookback_months)
        cond = (ts >= F.lit(lower)) & (ts < F.lit(upper))
        in_any = cond if in_any is None else (in_any | cond)
    rows = (
        sample_pool.filter(in_any).select(ts.cast("string").alias("_period"))
        .distinct().collect()
    )
    return sorted(str(r["_period"]) for r in rows)


def compute_period_candidate_counts(
    sample_pool: SparkDataFrame,
    label_table: SparkDataFrame,
    periods: list[str],
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(time value, item)``: candidate rows and the positives among them.

    ``periods`` are ``YYYY-MM-DD`` time values; only their rows are read. The
    candidate rows are sample_pool's identity columns; label_table attaches by
    identity with a LEFT JOIN, a missing label counting as 0 — the rule
    ``prepare_eval_data`` uses. ``n_positives`` is ``sum(label)``, as the
    count mode and ``metrics_spark``'s ``n_positives`` are: with graded labels
    it is a relevance sum and the rate a mean relevance per candidate.

    Pre-check (input): label_table holds at most one row per identity in these
    periods. A duplicated key copies its candidate row in the join, inflating
    numerator and denominator at once with nothing raising; like
    ``prepare_eval_data``'s check, it raises rather than deduplicates.

    Returns columns ``(time_col, item_col, n_candidates, n_positives)`` with
    ``time_col`` the ``YYYY-MM-DD`` string. No periods, no rows.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    def in_periods(frame):
        return frame.filter(
            F.to_date(F.col(time_col)).cast("string").isin(list(periods)))

    candidates = in_periods(sample_pool).select(*identity_cols)
    labels = in_periods(label_table).select(*identity_cols, label_col)
    n_duplicated_keys = (
        labels.groupBy(*identity_cols)
        .agg(F.count(F.lit(1)).alias("_n_label_rows"))
        .filter(F.col("_n_label_rows") > 1)
        .count()
    )
    if n_duplicated_keys:
        raise ValueError(
            f"{n_duplicated_keys} duplicated label_table key(s) on "
            f"{identity_cols} in the popularity lookback periods. Each extra "
            "row would copy its sample_pool row in the join, inflating both "
            "the positive count and the candidate count of the positive-rate "
            "baseline. Deduplicate label_table upstream; evaluation does not "
            "pick one of the rows for you."
        )
    return (
        candidates.join(labels, on=identity_cols, how="left")
        .withColumn(time_col, F.to_date(F.col(time_col)).cast("string"))
        .groupBy(time_col, item_col)
        .agg(
            F.count(F.lit(1)).cast("long").alias(CANDIDATES_COL),
            F.coalesce(F.sum(F.col(label_col)), F.lit(0)).cast("long")
            .alias(POSITIVES_COL),
        )
        .select(time_col, item_col, CANDIDATES_COL, POSITIVES_COL)
    )


def _rate(positives, candidates):
    """0 when there were no candidates, never null."""
    return F.when(candidates > 0, positives / candidates).otherwise(F.lit(0.0))


def restrict_to_periods(
    period_counts: SparkDataFrame, periods, parameters: dict,
) -> SparkDataFrame:
    """``popularity_period_counts`` rows of ``periods`` (``YYYY-MM-DD``).

    The table keeps every period it ever counted; this run sums only the ones
    sample_pool holds today. Without it a period dropped from sample_pool, or
    re-dated, would still be summed into every window covering it, and
    ``--rebuild-dates`` could not name it (A21 refuses a period sample_pool
    lacks).
    """
    time_col = get_schema(parameters)["time"]
    return period_counts.filter(
        F.to_date(F.col(time_col)).cast("string").isin(list(periods)))


def compute_positive_rates(
    period_counts: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(snap_date, item)`` positive rate over ``[S - lookback, S)``.

    Same output shape as :func:`compute_purchase_counts` so
    :func:`build_baseline_frame` takes either.

    Pre-check (input): each window holds candidate rows. Checked here on the
    candidate counts, not on label_table (``_lookback_window``): with
    sample_pool empty in the window every denominator would be 0, every score
    0, and the baseline would silently rank by item name.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]

    ts = F.to_date(F.col(time_col))
    per_snap: list[SparkDataFrame] = []
    for s in snap_dates:
        lower, upper = _window_bounds(str(s), lookback_months)
        window = period_counts.filter((ts >= F.lit(lower)) & (ts < F.lit(upper)))
        if window.limit(1).count() == 0:
            raise ValueError(
                f"No sample_pool candidate rows in [{lower}, {upper}) for "
                f"evaluation.snap_date={s!r} (lookback_months="
                f"{lookback_months}). evaluation.baseline.score: rate divides "
                "by the times each item was a candidate, counted from "
                "sample_pool, so the baseline cannot be scored — backfill "
                "sample_pool further back, lower "
                "evaluation.baseline.lookback_months, or use "
                "evaluation.baseline.score: count."
            )
        per_snap.append(
            window.groupBy(item_col)
            .agg(F.sum(CANDIDATES_COL).alias("_c"), F.sum(POSITIVES_COL).alias("_p"))
            .withColumn(score_col, _rate(F.col("_p"), F.col("_c")).cast("double"))
            .withColumn(time_col, F.lit(str(pd.Timestamp(s).date())))
            .select(time_col, item_col, score_col)
        )
    result = per_snap[0]
    for df in per_snap[1:]:
        result = result.unionByName(df)
    return result


def compute_monthly_candidate_counts_by_window(
    period_counts: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(window_date, calendar month, item)`` candidates and positives.

    The rate mode's counterpart of
    :func:`compute_monthly_purchase_counts_by_window`, read off the period
    counts so the report's numerator, trend and coverage come from the same
    rows its rates do. Returns
    ``("window_date", "month", item_col, n_candidates, n_positives)``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]

    bounds = [(str(pd.Timestamp(s).date()), *_window_bounds(str(s), lookback_months))
              for s in snap_dates]
    windows = period_counts.sparkSession.createDataFrame(
        bounds, "window_date string, window_lower string, window_upper string"
    )
    day = F.to_date(F.col(time_col))
    in_window = (
        (day >= F.to_date(F.col("window_lower")))
        & (day < F.to_date(F.col("window_upper")))
    )
    return (
        period_counts.join(F.broadcast(windows), in_window, "inner")
        .withColumn("month", F.date_format(day, "yyyy-MM"))
        .groupBy("window_date", "month", item_col)
        .agg(
            F.sum(CANDIDATES_COL).cast("long").alias(CANDIDATES_COL),
            F.sum(POSITIVES_COL).cast("long").alias(POSITIVES_COL),
        )
        .select("window_date", "month", item_col, CANDIDATES_COL, POSITIVES_COL)
    )


def compute_monthly_candidate_counts_in_windows(
    period_counts: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(calendar month, item)`` over the periods any window covers.

    Each period counts once, however many windows cover it: summing each
    window's counts instead would count an overlapping period once per
    window, so the printed denominator — the reader's only cue that a rate
    rests on few candidates — would grow with the number of dates. Returns
    ``("month", item_col, n_candidates, n_positives)``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]

    day = F.to_date(F.col(time_col))
    in_any = None
    for s in snap_dates:
        lower, upper = _window_bounds(str(s), lookback_months)
        cond = (day >= F.lit(lower)) & (day < F.lit(upper))
        in_any = cond if in_any is None else (in_any | cond)
    return (
        period_counts.filter(in_any)
        .withColumn("month", F.date_format(day, "yyyy-MM"))
        .groupBy("month", item_col)
        .agg(
            F.sum(CANDIDATES_COL).cast("long").alias(CANDIDATES_COL),
            F.sum(POSITIVES_COL).cast("long").alias(POSITIVES_COL),
        )
        .select("month", item_col, CANDIDATES_COL, POSITIVES_COL)
    )

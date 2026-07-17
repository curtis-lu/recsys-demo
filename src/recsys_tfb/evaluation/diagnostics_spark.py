"""Spark-side aggregation for the evaluation report's diagnostics figures.

Each function reduces the (potentially huge) ``eval_predictions`` DataFrame to a
small pandas frame, so the rendered figures embed *aggregated* values rather
than raw per-row arrays. Output size is bounded by item/bin/rank counts, never
by the number of rows.

No UDFs are used (production constraint): binning is arithmetic
(``floor``/``least``/``greatest``), quartiles use ``percentile_approx``, and the
rest is ``groupBy`` + ``count``/``avg``/``sum``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

# Quartile probabilities (incl. min/max) for boxplot stats.
_PCTS = [0.0, 0.25, 0.5, 0.75, 1.0]


def score_histogram_counts(
    sdf: SparkDataFrame, item_col: str, score_col: str, nbins: int = 50
) -> pd.DataFrame:
    """Per-item histogram bin counts over a single set of global bin edges.

    Returns long-format columns ``[item_col, "bin_center", "count",
    "bin_width"]`` (item x non-empty-bin rows). Shared edges across items make
    the overlay histogram directly comparable.
    """
    bounds = sdf.agg(
        F.min(score_col).alias("lo"), F.max(score_col).alias("hi")
    ).collect()[0]
    lo, hi = bounds["lo"], bounds["hi"]
    cols = [item_col, "bin_center", "count", "bin_width"]
    if lo is None:  # empty input
        return pd.DataFrame(columns=cols)

    width = (hi - lo) / nbins
    if width <= 0:
        # All scores identical -> one bin holding everything per item.
        counts = sdf.groupBy(item_col).count().toPandas()
        counts["bin_center"] = lo
        counts["bin_width"] = 1.0
        return counts[cols]

    raw_bin = F.floor((F.col(score_col) - F.lit(lo)) / F.lit(width))
    bin_idx = F.least(F.lit(nbins - 1), F.greatest(F.lit(0), raw_bin)).cast("int")
    counts = (
        sdf.withColumn("_bin", bin_idx)
        .groupBy(item_col, "_bin")
        .count()
        .toPandas()
    )
    counts["bin_center"] = lo + (counts["_bin"] + 0.5) * width
    counts["bin_width"] = width
    return counts[cols]


def _fences(pcts) -> tuple[float, float, float, float, float]:
    """(q1, median, q3, lowerfence, upperfence) from [min,q1,median,q3,max],
    fences are Tukey 1.5*IQR clamped to the observed data range."""
    dmin, q1, median, q3, dmax = (float(v) for v in pcts)
    iqr = q3 - q1
    lower = max(dmin, q1 - 1.5 * iqr)
    upper = min(dmax, q3 + 1.5 * iqr)
    return q1, median, q3, lower, upper


def _box_stats(rows: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    out = []
    for _, r in rows.iterrows():
        q1, median, q3, lower, upper = _fences(r["p"])
        rec = {k: r[k] for k in key_cols}
        rec.update(
            q1=q1, median=median, q3=q3, lowerfence=lower, upperfence=upper
        )
        out.append(rec)
    return pd.DataFrame(
        out,
        columns=key_cols + ["q1", "median", "q3", "lowerfence", "upperfence"],
    )


def score_box_stats_by_label(
    sdf: SparkDataFrame,
    item_col: str,
    score_col: str,
    label_col: str,
    accuracy: int = 10000,
) -> pd.DataFrame:
    """Boxplot stats per (item, label): one row per (item, label)."""
    rows = (
        sdf.groupBy(item_col, label_col)
        .agg(F.percentile_approx(F.col(score_col), _PCTS, accuracy).alias("p"))
        .toPandas()
    )
    return _box_stats(rows, [item_col, label_col])


def _all_items(sdf: SparkDataFrame, item_col: str) -> list:
    return sorted(
        r[item_col] for r in sdf.select(item_col).distinct().collect()
    )


def _to_matrix(
    long_pd: pd.DataFrame, item_col: str, rank_col: str, value_col: str,
    items: list,
) -> pd.DataFrame:
    """Pivot long (item, rank, value) -> matrix indexed by ``items`` with
    columns = ranks ``1..len(items)`` (full-query rank range), missing = 0."""
    ranks = list(range(1, len(items) + 1))
    if long_pd.empty:
        return pd.DataFrame(0, index=items, columns=ranks)
    mat = long_pd.pivot_table(
        index=item_col, columns=rank_col, values=value_col,
        aggfunc="sum", fill_value=0,
    )
    return mat.reindex(index=items, columns=ranks, fill_value=0)


def rank_count_matrix(
    sdf: SparkDataFrame, item_col: str, rank_col: str
) -> pd.DataFrame:
    """Count of (item, rank) occurrences as an item x rank matrix."""
    items = _all_items(sdf, item_col)
    long = sdf.groupBy(item_col, rank_col).count().toPandas()
    return _to_matrix(long, item_col, rank_col, "count", items)


def positive_rank_count_matrix(
    sdf: SparkDataFrame, item_col: str, rank_col: str, label_col: str
) -> pd.DataFrame:
    """Count of positive-label (item, rank) occurrences. Rows cover all items
    (including those with zero positives)."""
    items = _all_items(sdf, item_col)
    long = (
        sdf.filter(F.col(label_col) == 1)
        .groupBy(item_col, rank_col)
        .count()
        .toPandas()
    )
    return _to_matrix(long, item_col, rank_col, "count", items)


def positive_rate_matrix(
    sdf: SparkDataFrame, item_col: str, rank_col: str, label_col: str
) -> pd.DataFrame:
    """Positive rate = sum(label)/count per (item, rank) as a matrix
    (rate 0 where there are no rows)."""
    items = _all_items(sdf, item_col)
    agg = (
        sdf.groupBy(item_col, rank_col)
        .agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.col(label_col)).alias("pos"),
        )
        .toPandas()
    )
    total = _to_matrix(agg, item_col, rank_col, "total", items)
    pos = _to_matrix(agg, item_col, rank_col, "pos", items)
    denom = np.where(total.values > 0, total.values, 1)
    rate = np.where(total.values > 0, pos.values / denom, 0.0)
    return pd.DataFrame(rate, index=total.index, columns=total.columns)


def calibration_bins(
    sdf: SparkDataFrame,
    item_col: str,
    score_col: str,
    label_col: str,
    n_bins: int = 10,
) -> pd.DataFrame:
    """Per-item calibration points, replicating sklearn ``calibration_curve``
    (strategy="uniform") semantics: uniform bins over ``[0, 1]``, one point per
    non-empty bin with ``prob_pred = mean(score)`` and ``prob_true =
    mean(label)``. An item is skipped when it has fewer than ``n_bins`` rows or
    no positives. Out-of-range scores are clipped into ``[0, 1]`` (more robust
    than sklearn, which raises).

    Returns columns ``[item_col, "bin", "prob_pred", "prob_true"]`` sorted by
    (item, bin).
    """
    cols = [item_col, "bin", "prob_pred", "prob_true"]
    clipped = F.greatest(
        F.lit(0.0), F.least(F.lit(1.0), F.col(score_col).cast("double"))
    )
    raw = F.floor(clipped * F.lit(float(n_bins)))
    bin_idx = F.least(F.lit(n_bins - 1), F.greatest(F.lit(0), raw)).cast("int")
    agg = (
        sdf.withColumn("bin", bin_idx)
        .groupBy(item_col, "bin")
        .agg(
            F.avg(F.col(score_col)).alias("prob_pred"),
            F.avg(F.col(label_col)).alias("prob_true"),
            F.count(F.lit(1)).alias("n"),
        )
        .toPandas()
    )
    if agg.empty:
        return pd.DataFrame(columns=cols)

    agg["_pos"] = agg["prob_true"] * agg["n"]
    per_item = agg.groupby(item_col).agg(
        total=("n", "sum"), pos=("_pos", "sum")
    )
    keep = per_item[
        (per_item["total"] >= n_bins) & (per_item["pos"] > 0)
    ].index
    out = agg[agg[item_col].isin(keep)][cols]
    return out.sort_values([item_col, "bin"]).reset_index(drop=True)

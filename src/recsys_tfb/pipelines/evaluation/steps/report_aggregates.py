"""Spark-side aggregation for the evaluation report's diagnostics figures.

Each function reduces the (potentially huge) ``eval_predictions`` DataFrame to a
small pandas frame: output size is bounded by item/bin/rank counts, never by
the number of rows. No UDFs (production constraint).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

# The landed form of these frames lives with the report's readers, so the
# report reads it back without importing this Spark module.
from recsys_tfb.evaluation.result_readers import (
    FRAME_LONG as _LONG,
    FRAME_MATRIX as _MATRIX,
    frame_to_json,
)

# Quartile probabilities (incl. min/max) for boxplot stats.
_PCTS = [0.0, 0.25, 0.5, 0.75, 1.0]


def score_histogram_counts(
    sdf: SparkDataFrame, item_col: str, score_col: str, nbins: int = 50
) -> pd.DataFrame:
    """Per-item histogram bin counts over a single set of global bin edges.

    Returns long-format columns ``[item_col, "bin_center", "count",
    "bin_width"]`` (item x non-empty-bin rows), sorted by item then bin. Shared
    edges across items make the overlay histogram directly comparable.

    Sorted because ``groupBy`` returns rows in shuffle order, which moves with
    the parallelism; unsorted, the same data lands as a different
    ``report_aggregates.json``.
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
        return counts[cols].sort_values(item_col).reset_index(drop=True)

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
    counts = counts.sort_values([item_col, "_bin"]).reset_index(drop=True)
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
    """Boxplot stats per (item, label): one row per (item, label), sorted by
    item then label (why: :func:`score_histogram_counts`).

    The quartiles are approximate and may move with how the rows are
    partitioned; accepted (#366 item 4). Each value is a score that occurs in
    its group, off by at most ``1/accuracy`` of the group's rows in rank (the
    bound PySpark documents for ``DataFrame.approxQuantile``, the same
    algorithm). Exact quartiles would hold every score of a group in memory,
    which production volumes do not allow.
    """
    rows = (
        sdf.groupBy(item_col, label_col)
        .agg(F.percentile_approx(F.col(score_col), _PCTS, accuracy).alias("p"))
        .toPandas()
        .sort_values([item_col, label_col])
    )
    return _box_stats(rows, [item_col, label_col])


def _all_items(sdf: SparkDataFrame, item_col: str) -> list:
    return sorted(
        r[item_col] for r in sdf.select(item_col).distinct().collect()
    )


def _n_ranks(sdf: SparkDataFrame, rank_col: str, items: list) -> int:
    """Width of the rank axis: the item count, or the deepest rank if deeper.

    Items are unique within a query group only without ``event``; with it,
    ranks run past the item count, and an axis cut there would drop them with
    nothing raised. Read from the whole frame, not the rows a matrix counts, so
    all three matrices share one axis.

    Cost: one ``agg(max)`` action per matrix over the cached frame, each the
    size of the ``_all_items`` scan beside it.
    """
    deepest = sdf.agg(F.max(rank_col).alias("_max_rank")).collect()[0]["_max_rank"]
    return max(len(items), int(deepest or 0))


def _to_matrix(
    long_pd: pd.DataFrame, item_col: str, rank_col: str, value_col: str,
    items: list, n_ranks: int,
) -> pd.DataFrame:
    """Pivot long (item, rank, value) -> matrix indexed by ``items`` with
    columns = ranks ``1..n_ranks`` (``_n_ranks``), missing = 0."""
    ranks = list(range(1, n_ranks + 1))
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
    return _to_matrix(
        long, item_col, rank_col, "count", items, _n_ranks(sdf, rank_col, items))


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
    return _to_matrix(
        long, item_col, rank_col, "count", items, _n_ranks(sdf, rank_col, items))


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
    n_ranks = _n_ranks(sdf, rank_col, items)
    total = _to_matrix(agg, item_col, rank_col, "total", items, n_ranks)
    pos = _to_matrix(agg, item_col, rank_col, "pos", items, n_ranks)
    denom = np.where(total.values > 0, total.values, 1)
    rate = np.where(total.values > 0, pos.values / denom, 0.0)
    return pd.DataFrame(rate, index=total.index, columns=total.columns)


def aggregate_report_diagnostics(
    sdf: SparkDataFrame,
    item_col: str,
    score_col: str,
    rank_col: str,
    label_col: str,
    include_distributions: bool = True,
) -> dict:
    """Every aggregate of the report's diagnostics, computed in one pass and
    returned as a dict that can be landed.

    ``sdf`` must be projected and ``cache()``d by the caller: each family here
    is its own action, so uncached every one is a full scan.

    Score bins (mean score vs actual positive rate) are not here: they are the
    bin table of the prediction-quality family
    (``evaluation/prediction_quality.py``, ADR-0024〈後果〉).

    A family that is off is **left out of the payload**, not emptied: an empty
    one reads as "measured, found nothing".

    ``columns`` travels with the payload: the JSON can be read back after
    ``parameters`` has changed (slice resume), and the reader is not
    guaranteed the same ``parameters``.
    """
    out: dict = {
        "columns": {
            "item": item_col, "score": score_col,
            "rank": rank_col, "label": label_col,
        },
    }
    if include_distributions:
        out["score_histogram"] = frame_to_json(
            score_histogram_counts(sdf, item_col, score_col), _LONG)
        out["score_box_by_label"] = frame_to_json(
            score_box_stats_by_label(sdf, item_col, score_col, label_col),
            _LONG)
        out["rank_counts"] = frame_to_json(
            rank_count_matrix(sdf, item_col, rank_col), _MATRIX)
        out["positive_rank_counts"] = frame_to_json(
            positive_rank_count_matrix(sdf, item_col, rank_col, label_col),
            _MATRIX)
        out["positive_rate"] = frame_to_json(
            positive_rate_matrix(sdf, item_col, rank_col, label_col), _MATRIX)
    return out

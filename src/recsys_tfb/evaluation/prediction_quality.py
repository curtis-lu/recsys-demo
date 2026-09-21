"""The prediction-quality metric family: each evaluated row as one binary
prediction (ADR-0024).

Why a family of its own, and not part of the ranking metrics
============================================================
The ranking metrics (``metrics_spark``) average per query group and drop every
query group without a positive first (``total_rel > 0``): AP is undefined
there. A binary view must see those rows — precision computed only on
requests that someone clicked is systematically larger (ADR-0024 decision 3).
So nothing here filters on query groups, and nothing here reads or changes
the ranking path (decision 2).

One bin table, everything derived from it
=========================================
:func:`aggregate_score_bins` is the only Spark work: one ``groupBy(item, bin)``
with ``sum(weight)``, ``sum(label * weight)`` and ``sum(score * weight)``.
Every number the report shows — the threshold sweep, ``pr_auc``, ``roc_auc``,
the best-F1 threshold and the display bin table — is computed by the pure
functions below from that one table, so the threshold sweep and the bin table
can never disagree.

Bins are **global and equal-width over this data's ``min(score)`` ..
``max(score)``**, never a fixed ``[0, 1]``: at click rates of 0.1%-1% the
scores crowd the low end and ``[0, 1]`` bins put almost every row into the
first few. Equal width, not quantiles, because it keeps two properties the
report relies on: per-item bins add up to the overall bins, and fine bins
merge into display bins (ADR-0024 decision 4). Quantile bins break both
silently; switching needs its own ADR.

What the bin table can and cannot say
=====================================
Only thresholds on a bin edge are exact; no threshold inside a bin is
interpolated. Rows inside one bin are treated as one tied score, so:

* ``roc_auc`` is the exact ROC-AUC of the binned score (a tie counts half).
* ``pr_auc`` is the exact average precision of the binned score:
  ``sum over bins of (positives in the bin / P) x precision at its lower
  edge`` — the step rule ``sklearn.metrics.average_precision_score`` uses,
  applied to the bin index. Not trapezoids in PR space, which overstate the
  area. It still does not equal an average precision computed on the raw
  scores: the order inside a bin is gone.
* The best-F1 threshold can only land on a bin edge; its resolution is the
  bin width.

The weight column
=================
``weight_col`` multiplies every sum, so each count is a weighted count. It is
there for the whole-group sampling of zero-positive query groups (#429), which
writes ``1 / r`` on the kept rows; without it every row weighs 1 and the sums
are plain counts.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

#: Columns of every bin table this module hands out or reads back.
BIN_COLUMNS = ["bin", "n", "n_pos", "score_sum"]


def aggregate_score_bins(
    sdf: SparkDataFrame,
    *,
    item_col: str,
    score_col: str,
    label_col: str,
    n_bins: int,
    top_n: int,
    weight_col: str | None = None,
) -> dict:
    """Bin every row of ``sdf`` by score and sum per (item, bin).

    Returns a dict:

    * ``lo`` / ``hi`` / ``width``: the bin range (this data's min and max
      score) and one bin's width; ``width`` is ``0.0`` when every score is
      the same, and then every row is in bin 0.
    * ``overall``: :data:`BIN_COLUMNS`, one row per non-empty bin, summed
      over every item.
    * ``item_totals``: ``[item_col, "n", "n_pos"]`` for **every** item, most
      rows first.
    * ``listed_items`` / ``per_item``: the ``top_n`` items with the most rows
      (ties by item), and their bins (``[item_col] + BIN_COLUMNS``).

    Why ``top_n``: per-item bins come back to the driver as ``items x bins``
    rows, and in the ad scenario the item count can run to thousands
    (ADR-0024 decision 6). The overall bins and every item's totals are
    computed from all items regardless, in Spark.

    Cost: one aggregate for the range, one ``groupBy(item, bin)`` over the
    rows (the only full pass that shuffles), cached, then three small
    collects over that aggregate.
    """
    bounds = sdf.agg(
        F.min(score_col).alias("lo"), F.max(score_col).alias("hi")
    ).collect()[0]
    lo, hi = bounds["lo"], bounds["hi"]
    empty = pd.DataFrame(columns=BIN_COLUMNS)
    if lo is None:
        return {
            "lo": None, "hi": None, "width": None,
            "overall": empty,
            "item_totals": pd.DataFrame(columns=[item_col, "n", "n_pos"]),
            "listed_items": [],
            "per_item": pd.DataFrame(columns=[item_col] + BIN_COLUMNS),
        }
    lo, hi = float(lo), float(hi)
    width = (hi - lo) / n_bins
    if width > 0:
        raw = F.floor((F.col(score_col) - F.lit(lo)) / F.lit(width))
        # max(score) lands exactly on the top edge: clamp it into the last bin.
        bin_idx = F.least(F.lit(n_bins - 1), F.greatest(F.lit(0), raw))
    else:
        width = 0.0
        bin_idx = F.lit(0)
    weight = F.col(weight_col) if weight_col else F.lit(1)

    agg = (
        sdf.withColumn("_bin", bin_idx.cast("int"))
        .groupBy(item_col, "_bin")
        .agg(
            F.sum(weight).alias("n"),
            F.sum(F.col(label_col) * weight).alias("n_pos"),
            F.sum(F.col(score_col) * weight).alias("score_sum"),
        )
        .withColumnRenamed("_bin", "bin")
        .cache()
    )
    try:
        overall = (
            agg.groupBy("bin")
            .agg(F.sum("n").alias("n"), F.sum("n_pos").alias("n_pos"),
                 F.sum("score_sum").alias("score_sum"))
            .toPandas()
        )
        item_totals = (
            agg.groupBy(item_col)
            .agg(F.sum("n").alias("n"), F.sum("n_pos").alias("n_pos"))
            .toPandas()
        )
        # Most rows first, ties by item: the order the budget keeps from.
        item_totals = item_totals.sort_values(
            ["n", item_col], ascending=[False, True], kind="mergesort"
        ).reset_index(drop=True)
        listed = [str(i) for i in item_totals[item_col].head(max(top_n, 0))]
        if listed:
            per_item = (
                agg.filter(F.col(item_col).isin(listed))
                .select(item_col, *BIN_COLUMNS)
                .toPandas()
            )
        else:
            per_item = pd.DataFrame(columns=[item_col] + BIN_COLUMNS)
    finally:
        agg.unpersist()

    # groupBy hands rows back in shuffle order; sorted, the same data lands
    # as the same JSON (#355).
    overall = overall.sort_values("bin").reset_index(drop=True)[BIN_COLUMNS]
    per_item = per_item.sort_values([item_col, "bin"]).reset_index(drop=True)
    return {
        "lo": lo, "hi": hi, "width": width,
        "overall": overall,
        "item_totals": item_totals[[item_col, "n", "n_pos"]],
        "listed_items": listed,
        "per_item": per_item,
    }


def _ratio(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    """``num / den`` with NaN where ``den`` is 0 (undefined, not zero)."""
    num = np.asarray(num, dtype=float)
    den = np.asarray(den, dtype=float)
    out = np.full(num.shape, np.nan)
    np.divide(num, den, out=out, where=den > 0)
    return out


def threshold_sweep(bins: pd.DataFrame, *, lo: float, width: float) -> pd.DataFrame:
    """Confusion counts and precision / recall / F1 at every bin edge.

    One row per non-empty bin ``j``, highest threshold first. Its threshold
    is ``j``'s lower edge ``lo + j * width``, and "predicted positive" means
    every row in bin ``j`` or above. An empty bin is left out: its edge
    predicts exactly the rows the next non-empty bin's edge does.

    Precision is NaN where nothing is predicted positive; recall and F1 are
    NaN when the data has no positive — undefined, not zero.
    """
    b = bins.sort_values("bin", ascending=False)
    b = b[b["n"] > 0]
    n = b["n"].to_numpy(dtype=float)
    pos = b["n_pos"].to_numpy(dtype=float)
    total_pos, total_neg = pos.sum(), (n - pos).sum()
    tp = np.cumsum(pos)
    fp = np.cumsum(n - pos)
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, np.full(tp.shape, total_pos))
    f1 = _ratio(2 * precision * recall, precision + recall)
    return pd.DataFrame({
        "bin": b["bin"].to_numpy(dtype=int),
        "threshold": lo + b["bin"].to_numpy(dtype=float) * width,
        "tp": tp, "fp": fp,
        "fn": total_pos - tp, "tn": total_neg - fp,
        "precision": precision, "recall": recall, "f1": f1,
    }).reset_index(drop=True)


def _native(value):
    """A JSON-native number, or ``None`` for NaN (the payload lands as JSON)."""
    if value is None:
        return None
    value = float(value)
    if math.isnan(value):
        return None
    return int(value) if value.is_integer() and abs(value) < 2**53 else value


def binary_summary(bins: pd.DataFrame, *, lo: float, width: float) -> dict:
    """The headline numbers of one bin table (overall, or one item's).

    ``pr_auc`` and ``roc_auc`` are the exact values of the binned score (see
    the module docstring); ``best_f1`` is the bin edge with the largest F1,
    the higher threshold on a tie. A metric the data cannot define — no
    positive, or for ``roc_auc`` no negative — is ``None``, never 0.
    """
    sweep = threshold_sweep(bins, lo=lo, width=width)
    n = float(bins["n"].sum()) if len(bins) else 0.0
    n_pos = float(bins["n_pos"].sum()) if len(bins) else 0.0
    n_neg = n - n_pos
    out = {
        "n": _native(n),
        "n_pos": _native(n_pos),
        "positive_rate": float(n_pos / n) if n > 0 else None,
        "pr_auc": None,
        "roc_auc": None,
        "best_f1": None,
    }
    if n_pos <= 0:
        return out

    # Highest threshold first, so each bin's precision already counts every
    # bin above it.
    b = bins.sort_values("bin", ascending=False)
    b = b[b["n"] > 0]
    pos = b["n_pos"].to_numpy(dtype=float)
    neg = b["n"].to_numpy(dtype=float) - pos
    precision = sweep["precision"].to_numpy(dtype=float)
    out["pr_auc"] = float(np.sum(pos / n_pos * precision))
    if n_neg > 0:
        pos_above = np.cumsum(pos) - pos
        out["roc_auc"] = float(np.sum(neg * (pos_above + 0.5 * pos))
                               / (n_pos * n_neg))

    f1 = sweep["f1"].to_numpy(dtype=float)
    if not np.all(np.isnan(f1)):
        # nanargmax returns the first maximum, and the sweep runs from the
        # highest threshold down: a tie keeps the higher threshold.
        i = int(np.nanargmax(f1))
        out["best_f1"] = {
            key: float(sweep[key].iloc[i])
            for key in ("threshold", "precision", "recall", "f1")
        }
    return out


def build_payload(
    aggregated: dict,
    *,
    item_col: str,
    score_col: str,
    label_col: str,
    weight_col: str | None,
    n_bins: int,
    n_display_bins: int,
    top_n: int,
) -> dict:
    """The landed JSON: :func:`aggregate_score_bins`' tables plus the
    :func:`binary_summary` of each.

    The fine bin tables are kept, not only the summaries: the report
    re-derives its threshold sweep and display bin table from them with the
    same functions, and the JSON can be re-drawn elsewhere without the
    ``parameters`` that made it (``columns`` and ``bins`` travel with it, as
    in ``diagnostics_spark.aggregate_report_diagnostics``).
    """
    from recsys_tfb.evaluation.diagnostics_spark import frame_to_json

    lo, width = aggregated["lo"], aggregated["width"]
    overall = aggregated["overall"]
    per_item = aggregated["per_item"]
    listed = aggregated["listed_items"]
    per_item_summary = {}
    if lo is not None:
        for item in listed:
            item_bins = per_item[per_item[item_col].astype(str) == item]
            per_item_summary[item] = binary_summary(
                item_bins[BIN_COLUMNS], lo=lo, width=width)
    return {
        "columns": {"item": item_col, "score": score_col,
                    "label": label_col, "weight": weight_col},
        "bins": {"n_bins": n_bins, "n_display_bins": n_display_bins,
                 "lo": lo, "hi": aggregated["hi"], "width": width},
        "overall": {
            "summary": (binary_summary(overall, lo=lo, width=width)
                        if lo is not None else None),
            "bins": frame_to_json(overall, "long"),
        },
        "per_item": {
            "top_n": top_n,
            "n_items": int(len(aggregated["item_totals"])),
            "listed": list(listed),
            "totals": frame_to_json(aggregated["item_totals"], "long"),
            "summary": per_item_summary,
            "bins": frame_to_json(per_item, "long"),
        },
    }


def bins_from_payload(payload_bins: dict) -> pd.DataFrame:
    """A bin table landed by :func:`build_payload`, as a frame again."""
    from recsys_tfb.evaluation.diagnostics_spark import frame_from_json

    return frame_from_json(payload_bins)


def coarse_bin_table(
    bins: pd.DataFrame,
    *,
    lo: float,
    width: float,
    n_bins: int,
    n_display_bins: int,
) -> pd.DataFrame:
    """The report's bin table: fine bins merged into ``n_display_bins``
    equal-width display bins.

    Each display bin holds ``n_bins / n_display_bins`` consecutive fine bins
    (A42 keeps that an integer). Every display bin is listed, an empty one
    too, with ``mean_score`` (``score_sum / n``) and ``positive_rate``
    (``n_pos / n``) NaN there. ``precision`` / ``recall`` / ``f1`` are the
    threshold sweep read at the display bin's lower edge, which is a fine
    bin edge, so they are exact.
    """
    per = n_bins // n_display_bins
    b = bins.copy()
    b["display"] = b["bin"].astype(int) // per
    grouped = b.groupby("display")[["n", "n_pos", "score_sum"]].sum()
    grouped = grouped.reindex(range(n_display_bins), fill_value=0)
    n = grouped["n"].to_numpy(dtype=float)
    pos = grouped["n_pos"].to_numpy(dtype=float)
    total_pos = pos.sum()
    # "bins >= this display bin's first fine bin" is a suffix sum here.
    tp = np.cumsum(pos[::-1])[::-1]
    predicted = np.cumsum(n[::-1])[::-1]
    precision = _ratio(tp, predicted)
    recall = _ratio(tp, np.full(tp.shape, total_pos))
    edges = lo + np.arange(n_display_bins + 1) * per * width
    return pd.DataFrame({
        "score_from": edges[:-1],
        "score_to": edges[1:],
        "n": grouped["n"].to_numpy(),
        "n_pos": grouped["n_pos"].to_numpy(),
        "mean_score": _ratio(grouped["score_sum"].to_numpy(), n),
        "positive_rate": _ratio(pos, n),
        "precision": precision,
        "recall": recall,
        "f1": _ratio(2 * precision * recall, precision + recall),
    })

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
can never disagree. All of them land with the payload
(:func:`build_payload`); the report reads them.

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
``weight_col`` multiplies every sum, so each count is a weighted count. It
carries the ``1 / r`` that the whole-group sampling of zero-positive query
groups writes on the kept rows; without it every row weighs 1.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.evaluation.metric_catalog import (
    ITEM_EVERY_GROUP,
    SCORE_BINS,
    metrics_of,
)
from recsys_tfb.evaluation.metrics import macro_from_per_item
from recsys_tfb.evaluation.metrics_spark import item_metrics
from recsys_tfb.evaluation.result_readers import (
    DISPLAY_BINS_KEY,
    FRAME_LONG,
    THRESHOLD_SWEEP_KEY,
    frame_to_json,
)

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
      over every item; ``overall_areas``: its :func:`bin_areas`, a dict.
    * ``item_totals``: :func:`bin_areas` of **every** item —
      ``[item_col, "n", "n_pos", "pr_auc", "roc_auc"]`` — most rows first.
    * ``listed_items`` / ``per_item``: the ``top_n`` items with the most rows
      (ties by item), and their bins (``[item_col] + BIN_COLUMNS``).

    Why ``top_n``: per-item bins come back to the driver as ``items x bins``
    rows, and the item count can run to thousands (ADR-0024 decision 6). So
    it only decides which items' bin tables the page draws: every item's
    areas are computed in Spark, from the same bins, and only one row per
    item comes back.

    Cost: one aggregate for the range, one ``groupBy(item, bin)`` over the
    rows (the only full pass that shuffles), cached, then four small
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
            "overall": empty, "overall_areas": None,
            "item_totals": pd.DataFrame(
                columns=[item_col, "n", "n_pos", *_area_names()]),
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
        overall_bins = (
            agg.groupBy("bin")
            .agg(F.sum("n").alias("n"), F.sum("n_pos").alias("n_pos"),
                 F.sum("score_sum").alias("score_sum"))
        )
        overall = overall_bins.toPandas()
        overall_areas = bin_areas(overall_bins, []).collect()[0].asDict()
        item_totals = bin_areas(agg, [item_col]).toPandas()
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
    # as the same JSON.
    overall = overall.sort_values("bin").reset_index(drop=True)[BIN_COLUMNS]
    per_item = per_item.sort_values([item_col, "bin"]).reset_index(drop=True)
    return {
        "lo": lo, "hi": hi, "width": width,
        "overall": overall,
        "overall_areas": {name: overall_areas[name] for name in _area_names()},
        "item_totals": item_totals,
        "listed_items": listed,
        "per_item": per_item,
    }


def _area_names() -> list[str]:
    """The catalog's :data:`~metric_catalog.SCORE_BINS` entries, in order."""
    return [m.name for m in metrics_of(SCORE_BINS)]


#: Each :class:`~metric_catalog.BinArea` curve: its term per bin — from the
#: bin's positives and negatives, ``tp`` (the positives at or above it) and
#: ``seen`` (the rows at or above it) — and what the key's summed terms are
#: divided by, from its positives P and negatives N.
_CURVES = {
    "pr": (lambda pos, neg, tp, seen: pos * tp / seen,
           lambda p, n: p),
    "roc": (lambda pos, neg, tp, seen: neg * (tp - pos + F.lit(0.5) * pos),
            lambda p, n: p * n),
}


def bin_areas(bins: SparkDataFrame, key_cols: list[str]) -> SparkDataFrame:
    """Every :data:`~metric_catalog.SCORE_BINS` entry of ``metric_catalog``
    (``pr_auc``, ``roc_auc``) of each key's bin table, in Spark: one row per
    key, ``[*key_cols, "n", "n_pos", <entry names>]``. No key column gives
    the one row of the whole table.

    ``bins`` holds ``bin``, ``n`` and ``n_pos``, one row per (key, non-empty
    bin). Each key's bins are walked from the highest down (a window over the
    key), so each bin knows ``tp`` and ``seen``; each entry's curve
    (:data:`_CURVES`) sums a term per bin from them:

    * ``"pr"``: n_pos × precision at the bin's lower edge, over P.
    * ``"roc"``: the bin's negatives × (positives above it + half its own),
      over P × N.

    The same areas the module docstring describes. Both are undefined (NULL)
    for a key without a positive or without a negative: without a negative
    PR-AUC would be 1 whatever the order, and a macro would count it as a
    perfect item. Only these few numbers per key leave the executors: the
    window runs over ``items x bins`` rows, never over the evaluated rows.
    """
    walk = (Window.partitionBy(*key_cols) if key_cols else Window).orderBy(
        F.col("bin").desc()).rowsBetween(
            Window.unboundedPreceding, Window.currentRow)
    walked = bins.select(
        *key_cols, "n", "n_pos",
        F.sum("n_pos").over(walk).alias("_tp"),
        F.sum("n").over(walk).alias("_seen"))
    entries = metrics_of(SCORE_BINS)
    pos, neg = F.col("n_pos"), F.col("n") - F.col("n_pos")
    summed = walked.groupBy(*key_cols).agg(
        F.sum("n").alias("n"), F.sum("n_pos").alias("n_pos"),
        *(F.sum(_CURVES[m.formula.curve][0](
            pos, neg, F.col("_tp"), F.col("_seen"))).alias(f"_{m.name}")
          for m in entries))
    defined = (pos > 0) & (neg > 0)
    return summed.select(*key_cols, "n", "n_pos", *(
        F.when(defined, F.col(f"_{m.name}")
               / _CURVES[m.formula.curve][1](pos, neg)).alias(m.name)
        for m in entries))


def _ratio(num: np.ndarray, den: np.ndarray) -> np.ndarray:
    """``num / den`` with NaN where ``den`` is 0 (undefined, not zero)."""
    num = np.asarray(num, dtype=float)
    den = np.asarray(den, dtype=float)
    out = np.full(num.shape, np.nan)
    np.divide(num, den, out=out, where=den > 0)
    return out


def threshold_sweep(
    bins: pd.DataFrame, *, lo: float, width: float, n_bins: int | None = None,
) -> pd.DataFrame:
    """Confusion counts and precision / recall / F1 at every bin edge — the
    one place they are computed; every other function here reads them from
    this frame.

    One row per non-empty bin ``j``, highest threshold first. Its threshold
    is ``j``'s lower edge ``lo + j * width``, and "predicted positive" means
    every row in bin ``j`` or above. ``n`` / ``n_pos`` are bin ``j``'s own
    counts, ``tp`` / ``fp`` the running totals from the top bin down to it.

    An empty bin is left out: its edge predicts exactly the rows the next
    non-empty bin's edge above it does. Pass ``n_bins`` to get a row for
    every edge ``0 .. n_bins - 1`` anyway, empty bins included — for reading
    the sweep at edges chosen elsewhere (:func:`coarse_bin_table`).

    Precision is NaN where nothing is predicted positive; recall and F1 are
    NaN when the data has no positive — undefined, not zero.
    """
    counts = bins.groupby("bin")[["n", "n_pos"]].sum()
    if n_bins is None:
        counts = counts[counts["n"] > 0]
    else:
        counts = counts.reindex(range(n_bins), fill_value=0)
    counts = counts.sort_index(ascending=False)
    n = counts["n"].to_numpy(dtype=float)
    pos = counts["n_pos"].to_numpy(dtype=float)
    total_pos, total_neg = pos.sum(), (n - pos).sum()
    tp = np.cumsum(pos)
    fp = np.cumsum(n - pos)
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, np.full(tp.shape, total_pos))
    f1 = _ratio(2 * precision * recall, precision + recall)
    return pd.DataFrame({
        "bin": counts.index.to_numpy(dtype=int),
        "threshold": lo + counts.index.to_numpy(dtype=float) * width,
        "n": n, "n_pos": pos,
        "tp": tp, "fp": fp,
        "fn": total_pos - tp, "tn": total_neg - fp,
        "precision": precision, "recall": recall, "f1": f1,
    })


def _native(value):
    """A JSON-native number, or ``None`` for NaN (the payload lands as JSON)."""
    if value is None:
        return None
    value = float(value)
    if math.isnan(value):
        return None
    return int(value) if value.is_integer() and abs(value) < 2**53 else value


def binary_summary(
    bins: pd.DataFrame, *, lo: float, width: float, areas: Mapping,
) -> dict:
    """The headline numbers of one bin table (overall, or one item's).

    ``pr_auc`` and ``roc_auc`` are ``areas``' — :func:`bin_areas` of the same
    bins, computed in Spark for every item; this function does not compute
    them again. ``best_f1`` is the bin edge with the largest F1, the higher
    threshold on a tie. A metric the data cannot define — no positive, or for
    ``roc_auc`` no negative — is ``None``, never 0.
    """
    sweep = threshold_sweep(bins, lo=lo, width=width)
    n = float(sweep["n"].sum())
    n_pos = float(sweep["n_pos"].sum())
    out = {
        "n": _native(n),
        "n_pos": _native(n_pos),
        "positive_rate": float(n_pos / n) if n > 0 else None,
        **{name: _area(areas.get(name)) for name in _area_names()},
        "best_f1": None,
    }
    if n_pos <= 0:
        return out

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


def _area(value) -> float | None:
    """An area as a JSON float, ``None`` when undefined (NULL / NaN)."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return float(value)


def macro_over_items(
    table: pd.DataFrame, names, *, min_positives: int,
) -> dict:
    """The macro of each column ``names`` of ``table`` (one row per item,
    with ``n_pos``): the plain mean over the items holding a value whose
    ``n_pos`` reaches ``min_positives`` (``evaluation.metric.min_positives``,
    as the ranking metrics' macro uses it).

    An item without a value — the metric is undefined for it — is left out
    rather than counted as 0. Returns ``{name: {"value", "n_items"}}`` plus
    ``n_items``, every item of ``table``: the page prints "x of y items".
    """
    n_pos = table["n_pos"].to_numpy(dtype=float)
    out: dict = {"n_items": int(len(table)), "min_positives": min_positives}
    for name in names:
        values = pd.to_numeric(table[name], errors="coerce").to_numpy(
            dtype=float)
        has_value = ~np.isnan(values) & (n_pos >= min_positives)
        value = (macro_from_per_item(values[has_value], n_pos[has_value])
                 if has_value.any() else None)
        out[name] = {"value": value, "n_items": int(has_value.sum())}
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
    min_positives: int = 0,
) -> dict:
    """The landed JSON: :func:`aggregate_score_bins`' tables plus the
    :func:`binary_summary` of each, every item's areas and their macro
    (:func:`macro_over_items`, ``per_item.macro``), and the tables the report
    draws: the
    display bin table (:func:`coarse_bin_table`) overall and for every listed
    item, and the threshold sweep (:func:`threshold_sweep`) the report plots
    — under ``result_readers.DISPLAY_BINS_KEY`` / ``THRESHOLD_SWEEP_KEY``,
    ``None`` when there is no row. The report reads them instead of
    re-deriving them (ADR-0031 principle 1); all come from the same fine
    bins, so they cannot disagree.

    The fine bin tables are kept too, so the JSON can be re-drawn elsewhere
    without the ``parameters`` that made it (``columns`` and ``bins`` travel
    with it).
    """
    lo, width = aggregated["lo"], aggregated["width"]
    overall = aggregated["overall"]
    per_item = aggregated["per_item"]
    listed = aggregated["listed_items"]
    # Each item's positive rate beside its areas: PR-AUC's level under a
    # random order, which the page prints next to it.
    totals = aggregated["item_totals"].copy()
    totals.insert(3, "positive_rate", _ratio(totals["n_pos"], totals["n"]))
    areas_of = {str(row[item_col]): row
                for row in totals.to_dict("records")}
    per_item_summary = {}
    overall_display = overall_sweep = item_display = None
    if lo is not None:
        for item in listed:
            per_item_summary[item] = binary_summary(
                bins_of_item(per_item, item_col, item), lo=lo, width=width,
                areas=areas_of[item])
        overall_display = frame_to_json(coarse_bin_table(
            overall, lo=lo, width=width, n_bins=n_bins,
            n_display_bins=n_display_bins), FRAME_LONG)
        overall_sweep = frame_to_json(
            threshold_sweep(overall, lo=lo, width=width), FRAME_LONG)
        item_display = frame_to_json(_listed_display_bins(
            per_item, listed, item_col=item_col, lo=lo, width=width,
            n_bins=n_bins, n_display_bins=n_display_bins), FRAME_LONG)
    return {
        "columns": {"item": item_col, "score": score_col,
                    "label": label_col, "weight": weight_col},
        "bins": {"n_bins": n_bins, "n_display_bins": n_display_bins,
                 "lo": lo, "hi": aggregated["hi"], "width": width},
        "overall": {
            "summary": (binary_summary(overall, lo=lo, width=width,
                                       areas=aggregated["overall_areas"])
                        if lo is not None else None),
            "bins": frame_to_json(overall, FRAME_LONG),
            DISPLAY_BINS_KEY: overall_display,
            THRESHOLD_SWEEP_KEY: overall_sweep,
        },
        "per_item": {
            "top_n": top_n,
            "n_items": int(len(totals)),
            "listed": list(listed),
            "totals": frame_to_json(totals, FRAME_LONG),
            "macro": macro_over_items(totals, _area_names(),
                                      min_positives=min_positives),
            "summary": per_item_summary,
            "bins": frame_to_json(per_item, FRAME_LONG),
            DISPLAY_BINS_KEY: item_display,
        },
    }


def item_top_k_metrics(
    sdf: SparkDataFrame,
    *,
    item_col: str,
    label_col: str,
    rank_col: str,
    k_values: list[int],
    weight_col: str | None = None,
    min_positives: int = 0,
) -> dict:
    """Every :data:`~metric_catalog.ITEM_EVERY_GROUP` entry of
    ``metric_catalog`` (per-item precision@K, F1@K) of each item, over every
    row of ``sdf`` — the query groups without a positive included (ADR-0031
    decision 7, 2026-10-27 revision) — and their macro.

    One ``groupBy(item)`` sums, per item, ``n_pos`` and at each K ``n_top@K``
    (rows ranked within the top K) and ``hits@K`` (those positive), each row
    times ``weight_col`` (1 without it); only these numbers per item reach
    the driver. ``rank_col`` is the rank already on the rows, taken within
    the whole query group. The formulas then divide them
    (``metrics_spark.item_metrics``); a metric that divides by 0 is undefined
    and left out of its item's cell. The macro is
    :func:`macro_over_items`: ``min_positives`` reads ``n_pos``.

    Returns ``{"k_values", "shared_numbers": {item: counts}, "per_item":
    {item: metrics}, "macro"}``, items sorted, counts beside the metrics so
    the page can print them.
    """
    weight = F.col(weight_col).cast("double") if weight_col else F.lit(1.0)
    label = F.col(label_col).cast("double")
    aggs = [("n_pos", F.sum(label * weight))]
    for k in k_values:
        top = F.col(rank_col) <= F.lit(k)
        aggs += [(f"n_top@{k}", F.sum(F.when(top, weight).otherwise(0.0))),
                 (f"hits@{k}", F.sum(F.when(top, label * weight)
                                     .otherwise(0.0)))]
    rows = sdf.groupBy(item_col).agg(
        *(agg.alias(f"_c{i}") for i, (_, agg) in enumerate(aggs))).collect()
    shared = {
        str(r[item_col]): {key: float(r[f"_c{i}"] or 0.0)
                           for i, (key, _) in enumerate(aggs)}
        for r in sorted(rows, key=lambda r: str(r[item_col]))
    }
    per_item = item_metrics(shared, k_values, 1, method=ITEM_EVERY_GROUP)
    names = [m.key(k) for k in k_values for m in metrics_of(ITEM_EVERY_GROUP)]
    table = pd.DataFrame(
        [{"n_pos": cell["n_pos"],
          **{name: per_item.get(item, {}).get(name, np.nan)
             for name in names}}
         for item, cell in shared.items()],
        columns=["n_pos", *names])
    return {
        "k_values": list(k_values),
        "shared_numbers": shared,
        "per_item": {item: {k: v for k, v in cell.items() if k != "n_pos"}
                     for item, cell in per_item.items()},
        "macro": macro_over_items(table, names, min_positives=min_positives),
    }


def count_query_groups_without_positive(
    sdf: SparkDataFrame, *, group_cols: list[str], label_col: str,
) -> int:
    """How many query groups of ``sdf`` hold no positive: ``sum(label) > 0``
    is not true, so a group of NULL labels counts as holding none."""
    total = F.sum(F.col(label_col))
    return sdf.groupBy(*group_cols).agg(total.alias("_total_rel")).filter(
        ~F.coalesce(F.col("_total_rel") > 0, F.lit(False))).count()


#: The columns of :func:`coarse_bin_table`, the report's display bin table.
DISPLAY_BIN_COLUMNS = ["score_from", "score_to", "n", "n_pos", "mean_score",
                       "positive_rate", "precision", "recall", "f1"]


def _listed_display_bins(
    per_item: pd.DataFrame,
    listed: list,
    *,
    item_col: str,
    lo: float,
    width: float,
    n_bins: int,
    n_display_bins: int,
) -> pd.DataFrame:
    """Every listed item's :func:`coarse_bin_table`, in ``listed`` order, as
    one long frame with the item column first."""
    tables = []
    for item in listed:
        tbl = coarse_bin_table(
            bins_of_item(per_item, item_col, item), lo=lo, width=width,
            n_bins=n_bins, n_display_bins=n_display_bins)
        tbl.insert(0, item_col, item)
        tables.append(tbl)
    if not tables:
        return pd.DataFrame(columns=[item_col, *DISPLAY_BIN_COLUMNS])
    return pd.concat(tables, ignore_index=True)


def bins_of_item(per_item: pd.DataFrame, item_col: str, item: str) -> pd.DataFrame:
    """One listed item's rows of the per-item bin table, as
    :data:`BIN_COLUMNS`. Items compare as strings: the payload's ``listed``
    and ``summary`` keys are strings once landed as JSON."""
    return per_item.loc[per_item[item_col].astype(str) == item, BIN_COLUMNS]


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
    :func:`threshold_sweep` read at the display bin's lower edge, which is a
    fine bin edge, so they are exact and cannot disagree with the sweep.
    """
    per = n_bins // n_display_bins
    b = bins.copy()
    b["display"] = b["bin"].astype(int) // per
    grouped = b.groupby("display")[["n", "n_pos", "score_sum"]].sum()
    grouped = grouped.reindex(range(n_display_bins), fill_value=0)
    n = grouped["n"].to_numpy(dtype=float)
    first_fine_bins = [d * per for d in range(n_display_bins)]
    at_edges = threshold_sweep(
        bins, lo=lo, width=width, n_bins=n_bins,
    ).set_index("bin").loc[first_fine_bins]
    edges = lo + np.arange(n_display_bins + 1) * per * width
    return pd.DataFrame({
        "score_from": edges[:-1],
        "score_to": edges[1:],
        "n": grouped["n"].to_numpy(),
        "n_pos": grouped["n_pos"].to_numpy(),
        "mean_score": _ratio(grouped["score_sum"].to_numpy(), n),
        "positive_rate": _ratio(grouped["n_pos"].to_numpy(), n),
        "precision": at_edges["precision"].to_numpy(),
        "recall": at_edges["recall"].to_numpy(),
        "f1": at_edges["f1"].to_numpy(),
    })

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

import math

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


#: :func:`frame_to_json` 支援的兩種形狀。
_LONG = "long"
_MATRIX = "matrix"


def _no_nan(value):
    """NaN → None。理由見 :func:`frame_to_json` 的 docstring。"""
    return None if isinstance(value, float) and math.isnan(value) else value


def frame_to_json(df: pd.DataFrame, kind: str) -> dict:
    """把聚合出來的小 frame 轉成可以落地的 dict。

    ``kind`` 明寫而不是從 index 型別推斷：長格式的 index 是無意義的
    ``RangeIndex``（丟掉），矩陣的 index 是 item 名稱（丟了 heatmap 就沒有
    y 軸標籤）。推斷會在「item 剛好是 0, 1, 2」時猜錯，而那不會有任何測試轉紅。

    ``to_dict("split")`` **已經**把 numpy 純量轉成 Python 原生型別（實測
    ``int64`` → ``int``、object index → ``str``），所以這裡不逐格轉型。但它
    **不處理 NaN**：``JSONDataset`` 用預設 ``allow_nan=True``，NaN 會寫成
    ``NaN`` 這個非合法 JSON 的字面值，Python 讀得回來、別的工具不行——而這些
    檔案的用途正是被拷到別的環境去讀。所以在這裡換成 ``None``。
    """
    if kind not in (_LONG, _MATRIX):
        raise ValueError(
            f"kind must be {_LONG!r} or {_MATRIX!r}, got {kind!r}"
        )
    split = df.to_dict("split")
    out = {
        "kind": kind,
        "columns": list(split["columns"]),
        "data": [[_no_nan(v) for v in row] for row in split["data"]],
    }
    if kind == _MATRIX:
        out["index"] = list(split["index"])
    return out


def frame_from_json(payload: dict) -> pd.DataFrame:
    """:func:`frame_to_json` 的反向。回傳的 frame 直接餵給繪圖函式。"""
    df = pd.DataFrame(payload["data"], columns=payload["columns"])
    if payload.get("kind") == _MATRIX:
        df.index = payload["index"]
    return df


def aggregate_report_diagnostics(
    sdf: SparkDataFrame,
    item_col: str,
    score_col: str,
    rank_col: str,
    label_col: str,
    include_distributions: bool = True,
    include_calibration: bool = True,
    n_calibration_bins: int = 10,
) -> dict:
    """報表診斷區需要的全部聚合，一次算完並轉成可落地的 dict。

    ``sdf`` 必須由呼叫端先投影並 ``cache()``：這裡每個家族各是一次 action，
    不 cache 就是 6 次全掃。

    關掉的家族**不放進 payload**（不是放空的）：空的看起來像「量到了、結果
    什麼都沒有」，那是這次重構要避免的誤讀。

    ``columns`` 跟著 payload 走，好讓這份 JSON 拷到別的環境也能單獨重繪——
    重繪端不保證拿得到同一份 ``parameters``。
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
    if include_calibration:
        out["calibration"] = frame_to_json(
            calibration_bins(sdf, item_col, score_col, label_col,
                             n_bins=n_calibration_bins), _LONG)
    return out

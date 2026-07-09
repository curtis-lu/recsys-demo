"""條件判別力（框架診斷項目 3）：per-item within-item ROC-AUC。

只取 item 自己的列（跨所有 query），算「隨機一正例、一負例，正例分數較高」
的機率——從不跨 item 比較，per-item 常數偏移被整個消掉，因此它是條件判別
力軸的專用儀表、對水準軸完全免疫（框架手冊 Ch2/Ch3）。

演算法＝rank-sum（Mann–Whitney U）＋ midrank（平均秩）：
    AUC = (R⁺ − n⁺(n⁺+1)/2) / (n⁺ · n⁻)
R⁺＝正例 midrank 總和（分數升冪）。平手釘死 midrank——rank-sum 公式只在
midrank 下精確，F.rank（min-rank）直接代入會系統性偏差，而大量平手正是
本框架的核心診斷對象（近常數分數的冷門 item，真值應為 0.5）。
midrank = F.rank() + (同分列數 − 1)/2，全部 window 內建函式、無 UDF。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def within_item_auc(sdf: SparkDataFrame, parameters: dict) -> dict[str, dict]:
    """per item 的 within-item ROC-AUC（midrank rank-sum）。

    回傳 {item: {auc, n_pos, n_neg, n_rows}}；單類 item（無正例或無負例）
    → auc=None＋reason（不炸）。分數欄用 schema["score"]（實際決定上線
    排序的分數；AUC 對嚴格單調變換不變，校準平坦段併出的平手是要量的
    行為、不是要避開的雜訊）。
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    w_rank = Window.partitionBy(item_col).orderBy(F.col(score_col).asc())
    w_tie = Window.partitionBy(item_col, score_col)
    midrank = (
        F.rank().over(w_rank)
        + (F.count(F.lit(1)).over(w_tie) - F.lit(1)) / F.lit(2.0)
    )
    lbl = F.col(label_col).cast("double")
    rows = (
        sdf.withColumn("_midrank", midrank)
        .groupBy(item_col)
        .agg(
            F.sum(F.when(lbl == 1.0, F.col("_midrank"))).alias("r_pos_sum"),
            F.sum(lbl).alias("n_pos"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    out: dict[str, dict] = {}
    for r in rows:
        n_pos = int(r["n_pos"] or 0)
        n_rows = int(r["n_rows"])
        n_neg = n_rows - n_pos
        entry: dict = {"n_pos": n_pos, "n_neg": n_neg, "n_rows": n_rows}
        if n_pos == 0 or n_neg == 0:
            entry["auc"] = None
            entry["reason"] = (
                f"單一類別（n_pos={n_pos}, n_neg={n_neg}）——AUC 未定義"
            )
        else:
            r_pos = float(r["r_pos_sum"])
            entry["auc"] = (
                (r_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
            )
        out[str(r[item_col])] = entry
    return out

"""名次佔據統計（框架診斷項目 5）：水準軸傷害的直接觀測。

寫法沿 evaluation/diagnostics_spark.py 的聚合家族慣例（Spark 聚合、driver
端只收 item 級小結果），但歸屬診斷域，放 evaluation 會造成跨邊界 import。

**孤兒模組**：`quadrant.py` 已刪，本模組目前無 production 呼叫者；預定由
`score_shift/` 的曝光份額 guardrail 取代（見 `docs/superpowers/plans/
diag-redesign/00-shared-context.md` §2.4 附近說明與 `05-plan-4-score-shift.md`）。

rank 欄由 prepare_eval_data 保證存在（缺時已用 rank_within_query 注入）。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def top_slot_share(sdf: SparkDataFrame, parameters: dict, k: int) -> dict:
    """per item 佔據 top-k 的 query 比例，並列該 item 正類率當對照。

    identity＝time×entity×item（每 item 每 query 至多一列），所以
    「佔據 top-k 的 query 數」＝ rank ≤ k 的列數。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    rank_col = schema["rank"]
    query_cols = [schema["time"]] + schema["entity"]

    n_queries = sdf.select(*query_cols).distinct().count()
    rows = (
        sdf.groupBy(item_col)
        .agg(
            F.sum(
                F.when(F.col(rank_col).cast("long") <= k, 1).otherwise(0)
            ).alias("n_top"),
            F.mean(F.col(label_col).cast("double")).alias("y_rate"),
            F.count(F.lit(1)).alias("n_rows"),
        )
        .collect()
    )
    by_item = {
        str(r[item_col]): {
            "top_share": (int(r["n_top"]) / n_queries) if n_queries else None,
            "n_top": int(r["n_top"]),
            "y_rate": float(r["y_rate"]),
            "n_rows": int(r["n_rows"]),
        }
        for r in rows
    }
    return {"k": int(k), "n_queries": n_queries, "by_item": by_item}


def suppression_counts(sdf: SparkDataFrame, parameters: dict) -> dict:
    """per item「以負例身分排在該 query 首位正例上方」的次數。

    首位正例 rank 用 min(when(label=1, rank)) window 取得；無正例的 query
    條件為 null、自然不貢獻。零壓制的 item 不出現在 by_item（呼叫端補 0）。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    rank_col = schema["rank"]
    query_cols = [schema["time"]] + schema["entity"]

    w = Window.partitionBy(*query_cols)
    lbl = F.col(label_col).cast("int")
    rnk = F.col(rank_col).cast("long")
    with_min = sdf.withColumn(
        "_min_pos_rank", F.min(F.when(lbl == 1, rnk)).over(w)
    )
    suppressing = with_min.filter(
        (lbl == 0)
        & F.col("_min_pos_rank").isNotNull()
        & (rnk < F.col("_min_pos_rank"))
    )
    rows = suppressing.groupBy(item_col).count().collect()
    n_pos_queries = sdf.filter(lbl == 1).select(*query_cols).distinct().count()
    by_item = {
        str(r[item_col]): {"suppression_count": int(r["count"])} for r in rows
    }
    return {"n_pos_queries": n_pos_queries, "by_item": by_item}

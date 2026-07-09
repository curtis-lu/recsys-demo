"""交叉購買矩陣（框架診斷項目 10）：P(買 k｜買 j)，label_table 自 join。

label_table 含 label=0 列（欄位 snap_date, cust_id, prod_name, label）——
先濾 label=1 再自 join；join 鍵＝(time, entity)：同一 snap_date 內算共現、
跨 snap_date 加總。矩陣連同 per-item 買家數一起回——P(k|j) 由 10 個買家
估與 10000 個估的可信度不同，讀矩陣必須帶基數。
"""
from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def cross_purchase_matrix(
    label_rows: SparkDataFrame, parameters: dict,
) -> tuple[pd.DataFrame, pd.Series]:
    """回傳 (P(買 k｜買 j) 矩陣, per-item 買家數)。

    矩陣 index=j、columns=k、對角線＝1；無正例 → (空 DataFrame, 空 Series)。
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    key_cols = [schema["time"]] + schema["entity"]

    pos = (
        label_rows.filter(F.col(label_col).cast("int") == 1)
        .select(*key_cols, item_col)
        .distinct()
    )
    a = pos.select(*key_cols, F.col(item_col).alias("_item_j"))
    b = pos.select(*key_cols, F.col(item_col).alias("_item_k"))
    pairs = a.join(b, on=key_cols).groupBy("_item_j", "_item_k").count().toPandas()
    if pairs.empty:
        return pd.DataFrame(), pd.Series(dtype="int64")

    counts = pairs.pivot_table(
        index="_item_j", columns="_item_k", values="count", fill_value=0
    )
    items = sorted(set(counts.index) | set(counts.columns))
    counts = counts.reindex(index=items, columns=items, fill_value=0)
    n_buyers = pd.Series(
        {j: int(counts.loc[j, j]) for j in items}, name="n_buyers"
    )
    prob = counts.div(n_buyers, axis=0)
    prob.index.name = item_col
    prob.columns.name = item_col
    return prob, n_buyers

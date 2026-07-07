"""Bounded driver-side diagnosis sample（spec §2 共用底座）.

抽樣單位＝query（time × entity），只取有正例的 query（指標只由它們定義）。
兩趟設計：pass 1 count 每 item 的正例 query 數；正例 query 少於保底
``min_pos_queries_per_item`` 的 item 整批全取（take-all），其餘 query 用
CRC32 hash-ratio（``utils.hashing``）抽到補滿 ``max_queries``。被抽中的
query 帶回全部候選列（含負例，排序需要），``toPandas()`` 落到 driver 供
numpy 迭代計算（bootstrap／offset sweep／成對帳本）重複使用。

誠實限制：中型 item 經 hash-ratio 抽樣後仍可能低於保底——不硬補，
metadata 回報實際覆蓋＋log WARN；報表必須標示樣本規模，不得讓抽樣估計
冒充全量。
"""
from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket

logger = logging.getLogger(__name__)

_SITE = "diagnosis_sample"


def draw_diagnosis_sample(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, dict]:
    """兩趟診斷抽樣。回傳 (sample_pdf, metadata)。

    sample_pdf 欄位：query cols（time + entity）、item、label、score、
    （存在時）score_uncalibrated。metadata 見模組 docstring。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    query_cols = [time_col] + entity_cols

    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("sample", {}) or {}
    )
    max_queries = int(cfg.get("max_queries", 200000))
    floor = int(cfg.get("min_pos_queries_per_item", 50))
    seed = int(cfg.get("seed", 42))

    keep_cols = [
        c
        for c in [*query_cols, item_col, label_col, score_col,
                  "score_uncalibrated"]
        if c in eval_predictions.columns
    ]
    df = eval_predictions.select(*keep_cols)

    # ---- pass 1：正例 query 全集＋per-item 正例 query 數 ----
    pos_rows = df.filter(F.col(label_col) == 1)
    pos_queries = pos_rows.select(*query_cols).distinct()
    n_pos_total = pos_queries.count()

    item_counts = {
        str(r[item_col]): int(r["cnt"])
        for r in pos_rows.select(*query_cols, item_col)
        .distinct()
        .groupBy(item_col)
        .agg(F.count(F.lit(1)).alias("cnt"))
        .collect()
    }
    take_all_items = sorted(
        it for it, c in item_counts.items() if c < floor
    )

    # ---- pass 2：take-all ∪ hash-ratio ----
    if take_all_items:
        must = (
            pos_rows.filter(F.col(item_col).isin(take_all_items))
            .select(*query_cols)
            .distinct()
        )
        n_must = must.count()
        others = pos_queries.join(must, on=query_cols, how="left_anti")
    else:
        must = None
        n_must = 0
        others = pos_queries
    n_others = n_pos_total - n_must

    budget = max_queries - n_must
    if budget <= 0:
        logger.warning(
            "diagnosis sample: take-all queries (%d) already exceed "
            "max_queries=%d — sample is take-all only",
            n_must, max_queries,
        )
        ratio = 0.0
        sampled = must
    elif n_others == 0:
        ratio = 0.0
        sampled = must if must is not None else pos_queries.limit(0)
    else:
        ratio = min(1.0, budget / n_others)
        threshold = ratio_to_threshold(ratio)
        picked = others.filter(
            spark_bucket(others, query_cols, seed, _SITE) < threshold
        )
        sampled = picked if must is None else picked.unionByName(must)

    sample_pdf = df.join(sampled, on=query_cols, how="inner").toPandas()

    # ---- metadata（報表據此標示「抽樣估計＋樣本規模」）----
    n_sampled = int(
        sample_pdf[query_cols].drop_duplicates().shape[0]
    ) if len(sample_pdf) else 0
    pos_sampled = sample_pdf[sample_pdf[label_col] == 1]
    per_item_sampled = {
        str(k): int(v)
        for k, v in pos_sampled.drop_duplicates([*query_cols, item_col])
        .groupby(item_col)
        .size()
        .items()
    }
    below = {
        it: per_item_sampled.get(it, 0)
        for it in item_counts
        if it not in take_all_items and per_item_sampled.get(it, 0) < floor
    }
    if below:
        logger.warning(
            "diagnosis sample: items below per-item floor after hash "
            "sampling (not topped up by design): %s", below,
        )
    meta = {
        "n_pos_queries_total": int(n_pos_total),
        "n_queries_sampled": n_sampled,
        "sample_ratio": float(ratio),
        "take_all_items": take_all_items,
        "per_item_pos_queries_sampled": per_item_sampled,
        "items_below_floor_after_sampling": below,
        "max_queries": max_queries,
        "min_pos_queries_per_item": floor,
        "seed": seed,
    }
    return sample_pdf, meta

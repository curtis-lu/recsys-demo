"""Bounded driver-side diagnosis sample（spec §2 共用底座）.

抽樣單位＝query（time × entity），只取有正例的 query（指標只由它們定義）。
兩趟設計：pass 1 count 每 item 的正例 query 數；正例 query 少於保底
``min_pos_queries_per_item`` 的 item 整批全取（take-all），其餘 query 用
CRC32 hash-ratio（``utils.hashing``）抽到補滿 ``max_queries``。被抽中的
query 帶回全部候選列（含負例，排序需要），``toPandas()`` 落到 driver 供
numpy 迭代計算（bootstrap／offset sweep／成對帳本）重複使用。

兩層的納入機率不同（take-all π=1、hash-ratio π=ratio），所以樣本**不是**
簡單隨機樣本：``sample_pdf`` 帶出 ``stratum`` 與 ``inclusion_weight``
（＝1/π），下游估計量須加權，否則 ``ratio < 1`` 時 take-all 層會被系統性
高估 1/ratio 倍。

metadata 的 ``sample_ratio`` 有**兩個**權重全為 1 的分支，讀的人容易只記得
第一個：

* ``sample_ratio == 1.0``——有 hash-ratio 層，但正例 query 總數 ≤
  ``max_queries``，該層無須次抽樣，π=1。
* ``sample_ratio == 0.0``——**根本沒有 hash-ratio 層**（全部 take-all，或
  take-all 已吃掉整個 ``max_queries`` 預算）。這裡的 0.0 是「無此層」的
  哨兵值，**不是**「抽了 0%」。

因此下游**不得**對 ``sample_ratio`` 取倒數當權重用（0.0 會 ZeroDivisionError，
且語意也不對）。要判斷分層與權重一律讀 ``strata``——它由實際落地的樣本推導，
只列真的存在的層，每層直接附 ``weight``。

誠實限制：中型 item 經 hash-ratio 抽樣後仍可能低於保底——不硬補，
metadata 回報實際覆蓋＋log WARN；報表必須標示樣本規模，不得讓抽樣估計
冒充全量。

metadata 額外帶一個 ``sampling_description``：人看得懂的一句話（依上述
``sample_ratio`` 的兩個哨兵分支與 ``strata`` 動態組出，見 :func:`_sampling_description`），
之後會跟著每份診斷報表走，不必讓讀者自己回頭讀這份 docstring。
"""
from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket

logger = logging.getLogger(__name__)

_SITE = "diagnosis_sample"

#: 本模組在樣本上自造的欄名。它們不是配置項，但會跟使用者配置的欄位共用
#: 命名空間——撞名時 join 會產出兩個同名欄，錯誤要到很下游才炸
#: （見 ``_guard_reserved_columns``）。
_RESERVED_COLS = ("stratum", "inclusion_weight")


def _sampling_description(n_pos_total: int, ratio: float, strata: dict) -> str:
    """組出人看得懂的一句話，跟著 meta 走進報表——讀者不必自己讀 ``strata``。

    必須動態組字（不能寫死文案），因為 ``ratio`` 每次執行都不同。``ratio``
    有兩個語意不同的分支都用相同數值表達「這裡沒有次抽樣」：

    * ``ratio == 1.0``——有 hash-ratio 層但沒吃到，全部正例 query 都納入。
    * ``ratio == 0.0``——**根本沒有 hash-ratio 層**，不是「抽了 0%」。這裡
      明講「無 hash-ratio 層」，避免讀者以為抽樣率是 0。

    介於兩者之間才是真的分層抽樣，逐層列出實際落地的 query 數與權重，並
    提醒讀者跨 item 的統計量已加權（呼應模組 docstring 的 Horvitz–Thompson
    警語）。
    """
    if ratio == 1.0:
        return f"未抽樣：全部 {n_pos_total:,} 個有正例的 query 都納入。"
    if ratio == 0.0:
        return (
            f"未次抽樣（無 hash-ratio 層）：全部 {n_pos_total:,} 個有正例的 "
            "query 都以 take-all 全取，沒有進一步抽樣。"
        )
    labels = {"take_all": "take-all 層", "hash_ratio": "hash-ratio 層"}
    notes = {"take_all": "稀有 item，權重", "hash_ratio": "權重"}
    parts = []
    for key in ("take_all", "hash_ratio"):
        if key not in strata:
            continue
        info = strata[key]
        parts.append(
            f"{labels[key]} {info['n_queries']:,} query"
            f"（{notes[key]} {round(float(info['weight']), 2)}）"
        )
    body = "、".join(parts)
    return f"分層抽樣：{body}。跨 item 的統計量已依納入機率加權。"


def _guard_reserved_columns(keep_cols: list[str], seg_cols: list[str]) -> None:
    """撞名就 fail-loud——runtime backstop，主閘在 A15。

    **這不是重複的真實來源，兩者驗的輸入不同**：``core.consistency`` 的 A15
    predicate（``diagnosis_metric_param_errors``）驗「config **宣告**了什麼」，
    在 CLI entry 一秒內擋掉，是主閘；本函式驗「實際 DataFrame **有**什麼欄」。
    保留本函式是因為有繞過 Layer-1 的呼叫路徑——``scripts/*_diagnosis.py``
    直接 import 這個函式，不經過 CLI entry 的 config 驗證；schema 角色欄
    撞到保留名也只有這裡看得到。**刪掉任何一個都會留下缺口。**

    為什麼要在這裡擋：``draw_diagnosis_sample`` 會在 ``sampled`` 上自造
    ``stratum`` / ``inclusion_weight``，再 ``df.join(sampled, on=query_cols)``。
    若 ``df`` 已經帶了同名欄（最可能的來源＝使用者把它配進
    ``evaluation.segment_columns``），join 後會有**兩個同名欄**，pandas 的
    ``groupby("stratum")`` 拿到 2-D 切片，實測炸在
    ``ValueError: Grouper for 'stratum' not 1-dimensional``——訊息完全指不到
    根因（沒有一個字提到 segment 撞名）。與其讓人去追那條 traceback，不如在
    抽樣開始前就講清楚是哪個配置鍵用了保留欄名。
    """
    collisions = []
    for name in _RESERVED_COLS:
        if name not in keep_cols:
            continue
        origin = (
            "evaluation.segment_columns" if name in seg_cols
            else "the schema block (schema.columns.*)"
        )
        collisions.append(f"{name!r} (configured via {origin})")
    if not collisions:
        return
    raise ValueError(
        "diagnosis sample: reserved column name collision — "
        + "; ".join(collisions)
        + ". draw_diagnosis_sample adds its own 'stratum' and "
        "'inclusion_weight' columns to the sample, so an input column of "
        "the same name would be duplicated by the query-level join and fail "
        "far downstream with an opaque pandas error (\"Grouper for "
        "'stratum' not 1-dimensional\"). Fix: drop the column from "
        "evaluation.segment_columns (or rename it in the source table)."
    )


def draw_diagnosis_sample(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, dict]:
    """兩趟診斷抽樣。回傳 (sample_pdf, metadata)。

    sample_pdf 欄位：query cols（time + entity）、item、label、score、
    （存在時）score_uncalibrated＋配置的 ``evaluation.segment_columns``
    （存在者，供 by_segment 分組用；未配置或欄不存在則靜默略過），外加
    ``stratum``（``take_all`` / ``hash_ratio``）與 ``inclusion_weight``
    （納入機率的倒數，query 級：同一 query 的所有候選列同權重）。
    metadata 見模組 docstring（特別注意 ``sample_ratio == 0.0`` 的語意）。

    Raises:
        ValueError: 配置的欄位（``evaluation.segment_columns`` 或 schema 角色欄）
            用了保留欄名 ``stratum`` / ``inclusion_weight``，見
            :func:`_guard_reserved_columns`。
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

    seg_cols = list((parameters.get("evaluation", {}) or {})
                    .get("segment_columns", []) or [])
    keep_cols = list(dict.fromkeys(
        c
        for c in [*query_cols, item_col, label_col, score_col,
                  "score_uncalibrated", *seg_cols]
        if c in eval_predictions.columns
    ))
    _guard_reserved_columns(keep_cols, seg_cols)
    df = eval_predictions.select(*keep_cols)

    # ---- pass 1：正例 query 全集＋per-item 正例 query 數 ----
    with log_step(logger, "diagnosis_sample.pass1_count"):
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
    def _tag(queries: SparkDataFrame, stratum: str) -> SparkDataFrame:
        """標記該 query 集合屬於哪一層——納入機率 π 由層決定。"""
        return queries.withColumn("stratum", F.lit(stratum))

    with log_step(logger, "diagnosis_sample.pass2_select"):
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
            sampled = _tag(must, "take_all")
        elif n_others == 0:
            ratio = 0.0
            sampled = (
                _tag(must, "take_all") if must is not None
                else _tag(pos_queries.limit(0), "hash_ratio")
            )
        else:
            ratio = min(1.0, budget / n_others)
            threshold = ratio_to_threshold(ratio)
            picked = _tag(
                others.filter(
                    spark_bucket(others, query_cols, seed, _SITE) < threshold
                ),
                "hash_ratio",
            )
            sampled = (
                picked if must is None
                else picked.unionByName(_tag(must, "take_all"))
            )

        # 納入機率 π 的倒數＝**設計權重**（Horvitz–Thompson）：take-all 層
        # π=1；hash-ratio 層 π=ratio（該層的設計抽樣率）。
        #
        # 這裡刻意用設計值，**不是**實現比例（realized ratio，
        # ＝該層實際抽中數／該層候選數）。誠實標註：小樣本下兩者會明顯偏離
        # ——實測 ratio=0.25 的一層，40 個候選 query 實際抽中 14 個
        # （14/40 = 0.35），權重仍是 4.0（＝1/0.25）而不是 2.86。CRC32 分桶是
        # 無偏的，大樣本下實現比例會收斂到設計值，所以設計權重的估計量無偏；
        # 但單次小樣本的變異數會比自歸一化（Hájek / ratio estimator）大。
        #
        # 下游若要改用自歸一化估計量：自己除以樣本內的權重和，**不要**假設
        # 本模組已經歸一化過——這裡的權重沒有除以任何東西。
        #
        # ratio==0 代表根本沒有 hash-ratio 層（budget<=0 或 n_others==0），
        # 此時不會有列落在 otherwise 分支，權重取 1.0 只為避免除以零。
        hash_weight = 1.0 / ratio if ratio > 0 else 1.0
        sampled = sampled.withColumn(
            "inclusion_weight",
            F.when(F.col("stratum") == F.lit("take_all"), F.lit(1.0))
            .otherwise(F.lit(hash_weight)),
        )

    with log_step(logger, "diagnosis_sample.to_pandas"):
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
    # 分層摘要由實際落地的樣本推導（只列真的存在的層），確保與 sample_pdf 自洽。
    strata: dict[str, dict] = {}
    if len(sample_pdf):
        uniq_queries = sample_pdf[
            [*query_cols, "stratum", "inclusion_weight"]
        ].drop_duplicates(query_cols)
        for name, grp in uniq_queries.groupby("stratum"):
            strata[str(name)] = {
                "n_queries": int(len(grp)),
                "weight": float(grp["inclusion_weight"].iloc[0]),
            }
    meta = {
        "n_pos_queries_total": int(n_pos_total),
        "n_queries_sampled": n_sampled,
        "sample_ratio": float(ratio),
        "take_all_items": take_all_items,
        "strata": strata,
        "per_item_pos_queries_sampled": per_item_sampled,
        "items_below_floor_after_sampling": below,
        "max_queries": max_queries,
        "min_pos_queries_per_item": floor,
        "seed": seed,
        "sampling_description": _sampling_description(
            int(n_pos_total), float(ratio), strata
        ),
    }
    return sample_pdf, meta

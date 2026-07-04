"""Spark-side 選樣:top@1 象限 + 每 (item×象限) 確定性抽樣,交給 pandas SHAP 診斷。

放此(非 diagnostics/ 純 python 子套件)因為需要 Spark。全 native Spark,無 UDF。
P2b-2 已擴為第二輸出 `case_rows`(全格極值案例)。
"""

import logging

logger = logging.getLogger(__name__)


def select_shap_population(
    training_eval_predictions, test_model_input, parameters, predict_manifest=None
):
    """回傳 ``(shap_population, case_rows)``。

    shap_population:每 (item×象限) ``crc32`` 抽樣 profile 樣本(P2b-1,不變)。
    case_rows:每 (item×象限) 全格最高/最低分各一列(``role=high/low``),帶
    ``quadrant/role/rank/score/label`` + group 欄 + 特徵,供 ``compute_quadrant_cases``
    畫單列案例圖。rank/象限/選樣/join 全在 Spark(executor);driver 只 toPandas 小族群。

    ``quadrant_enabled=false`` → ``(None, None)``。best-effort:選樣失敗亦回 ``(None, None)``
    (不中斷訓練)。``predict_manifest`` 僅作 in-DAG 排序依賴(與 ``compute_test_mAP_spark``
    同慣例;三個資料輸入皆無 node producer,不掛此依賴會被 topo-sort 排到 predict 前讀到
    未寫入的預測)。
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from recsys_tfb.core.schema import get_schema

    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None, None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    group_cols = [time_col] + entity_cols

    try:
        # rank:item_col 作 tie-break,讓象限指派在同分時可重現。
        w_rank = Window.partitionBy(*group_cols).orderBy(
            F.col("score").desc(), F.col(item_col))
        ranked = training_eval_predictions.withColumn("_rank", F.row_number().over(w_rank))

        is_top = F.col("_rank") <= F.lit(top_k_decision)
        is_pos = F.col(label_col) == F.lit(1)
        quadrant = (
            F.when(is_top & is_pos, F.lit("TP"))
            .when(is_top & ~is_pos, F.lit("FP"))
            .when(~is_top & is_pos, F.lit("FN"))
            .otherwise(F.lit("TN"))
        )
        ck = F.concat_ws("|", *[F.col(c).cast("string") for c in group_cols + [item_col]])
        labeled = ranked.withColumn("quadrant", quadrant).withColumn("_ck", ck)

        # ---- 輸出 1:profile 抽樣(crc32 每格取 <= per_cell;P2b-1 行為不變)----
        w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
            F.crc32(F.col("_ck")), F.col("_ck"))
        sampled = (
            labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
            .where(F.col("_cell_rn") <= F.lit(per_cell))
        )
        keyset = sampled.select(*group_cols, item_col, "quadrant")
        pop_pdf = keyset.join(
            test_model_input, on=group_cols + [item_col], how="inner").toPandas()

        # ---- 輸出 2:全格極值案例(role=high/low)----
        # 不對稱 tiebreak:同分格 high/low 落不同列;真正單行格才落同一列。
        w_high = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col("score").desc(), F.col("_ck").asc())
        w_low = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col("score").asc(), F.col("_ck").desc())
        highs = (labeled.withColumn("_rn", F.row_number().over(w_high))
                 .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("high")))
        lows = (labeled.withColumn("_rn", F.row_number().over(w_low))
                .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("low")))
        extremes = highs.unionByName(lows).select(
            *group_cols, item_col, "quadrant", "role",
            F.col("_rank").alias("rank"), F.col("score").alias("score"),
            F.col(label_col).alias("label"))
        # test_model_input 也有 label 欄 → drop 以免 join 後 ambiguous(label 非特徵)。
        feats_only = (test_model_input.drop(label_col)
                      if label_col in test_model_input.columns else test_model_input)
        case_pdf = extremes.join(
            feats_only, on=group_cols + [item_col], how="inner").toPandas()
    except Exception as e:  # best-effort:選樣失敗不中斷訓練(spec §12)
        logger.warning("select_shap_population failed: %s", e)
        return None, None

    logger.info(
        "select_shap_population: pop_rows=%d case_rows=%d items=%d per_cell=%d",
        len(pop_pdf), len(case_pdf),
        pop_pdf[item_col].nunique() if len(pop_pdf) else 0, per_cell,
    )
    return pop_pdf, case_pdf

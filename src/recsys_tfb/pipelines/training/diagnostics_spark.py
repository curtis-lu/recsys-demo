"""Spark-side 選樣:top@1 象限 + 每 (item×象限) 確定性抽樣,交給 pandas SHAP 診斷。

放此(非 diagnostics/ 純 python 子套件)因為需要 Spark。全 native Spark,無 UDF。
P2b-2 會擴充為也標記每格 max/min 極值案例(role=high/low)。
"""

import logging

logger = logging.getLogger(__name__)


def select_shap_population(training_eval_predictions, test_model_input, parameters):
    """回傳每 (item×象限) 抽樣的小 pandas(特徵 + item + quadrant),供 per_quadrant SHAP。

    ``quadrant_enabled=false`` → None。rank/象限/抽樣/join 全在 Spark(executor);
    driver 只 toPandas 小族群。
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from recsys_tfb.core.schema import get_schema

    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    group_cols = [time_col] + entity_cols

    w_rank = Window.partitionBy(*group_cols).orderBy(F.col("score").desc())
    ranked = training_eval_predictions.withColumn("_rank", F.row_number().over(w_rank))

    is_top = F.col("_rank") <= F.lit(top_k_decision)
    is_pos = F.col(label_col) == F.lit(1)
    quadrant = (
        F.when(is_top & is_pos, F.lit("TP"))
        .when(is_top & ~is_pos, F.lit("FP"))
        .when(~is_top & is_pos, F.lit("FN"))
        .otherwise(F.lit("TN"))
    )
    labeled = ranked.withColumn("quadrant", quadrant)

    # 確定性每格抽樣:crc32(key) 排序(key 為 tiebreaker),取 <= per_cell
    ck = F.concat_ws("|", *[F.col(c).cast("string") for c in group_cols + [item_col]])
    labeled = labeled.withColumn("_ck", ck)
    w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
        F.crc32(F.col("_ck")), F.col("_ck"))
    sampled = (
        labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
        .where(F.col("_cell_rn") <= F.lit(per_cell))
    )
    keyset = sampled.select(*group_cols, item_col, "quadrant")

    joined = keyset.join(test_model_input, on=group_cols + [item_col], how="inner")
    pdf = joined.toPandas()
    logger.info(
        "select_shap_population: rows=%d items=%d per_cell=%d",
        len(pdf), pdf[item_col].nunique() if len(pdf) else 0, per_cell,
    )
    return pdf

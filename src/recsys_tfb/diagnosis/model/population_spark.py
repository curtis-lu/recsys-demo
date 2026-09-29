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

    ``quadrant_enabled=false`` → ``(None, None)``。A failure stops the run: this
    node touches no model capability, so nothing here is "the model cannot"
    (ADR-0030 decision 4). ``predict_manifest`` 僅作 in-DAG 排序依賴(與 ``compute_test_metrics``
    同慣例;三個資料輸入皆無 node producer,不掛此依賴會被 topo-sort 排到 predict 前讀到
    未寫入的預測)。

    Reads ``dataset.test_snap_dates``' months only, from both tables, and ranks
    with ``utils.ranking.rank_by_score_then_item`` on ``schema``'s score column
    (ADR-0030 decisions 5 and 12). The ``score`` column this hands
    ``compute_quadrant_cases`` is a name the two modules agree on, not the
    prediction table's column, so it stays ``score`` whatever ``schema`` calls
    that one.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.utils.ranking import rank_by_score_then_item

    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None, None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    # The rank window is a query group; the two joins back to ``test_model_input``
    # are at candidate grain, so they take identity (ADR-0025 decision 2).
    group_cols = schema["query_group_columns"]
    identity_cols = schema["identity_columns"]

    # Decision — which months: dataset.test_snap_dates, the ones this run
    # predicted and compute_shap_diagnostics describes (node rule 14). Both
    # tables keep every month ever written under their version, so an
    # unfiltered read grows with that history, not with this run.
    months = [str(m) for m in (parameters.get("dataset") or {}).get("test_snap_dates") or []]
    # A runtime backstop: A36 rejects this config before Spark starts.
    if not months:
        raise ValueError(
            "select_shap_population: dataset.test_snap_dates is unset or empty, "
            "so there is no month to pick the quadrant population from.")
    # Compared as text, the rule compute_test_metrics reads the same table
    # with (pipelines/training/steps/scored_months.restrict_to_scored_months,
    # whose docstring says why). Written out here because a library module
    # may not import a pipeline's steps/ (S3); once ADR-0030 decision 6 moves
    # this node into the training pipeline, it calls that function instead.
    time_text = F.col(schema["time"]).cast("string")
    in_months = time_text == months[0] if len(months) == 1 else time_text.isin(months)
    training_eval_predictions = training_eval_predictions.filter(in_months)
    test_model_input = test_model_input.filter(in_months)

    labeled = None
    try:
        # Decision — rank with the rule evaluation ranks with (score, then
        # item, then each event column), so a declared event cannot make the
        # quadrants' top-1 differ from evaluation's.
        ranked = training_eval_predictions.withColumn(
            "_rank",
            rank_by_score_then_item(
                group_cols, score_col, item_col, schema.get("event", [])),
        )

        is_top = F.col("_rank") <= F.lit(top_k_decision)
        is_pos = F.col(label_col) == F.lit(1)
        quadrant = (
            F.when(is_top & is_pos, F.lit("TP"))
            .when(is_top & ~is_pos, F.lit("FP"))
            .when(~is_top & is_pos, F.lit("FN"))
            .otherwise(F.lit("TN"))
        )
        ck = F.concat_ws("|", *[F.col(c).cast("string") for c in identity_cols])
        # 下面兩條分支各自 toPandas() 一次(兩個 action)。不 persist 的話,rank 的
        # shuffle 會整個重跑一遍。StorageLevel 顯式寫出、不靠預設:這份中間結果在生產
        # 資料量下裝不進 executor 記憶體時要能落磁碟,而不是被丟掉重算。
        labeled = (ranked.withColumn("quadrant", quadrant).withColumn("_ck", ck)
                   .persist(StorageLevel.MEMORY_AND_DISK))

        # ---- 輸出 1:profile 抽樣(crc32 每格取 <= per_cell;P2b-1 行為不變)----
        w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
            F.crc32(F.col("_ck")), F.col("_ck"))
        sampled = (
            labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
            .where(F.col("_cell_rn") <= F.lit(per_cell))
        )
        keyset = sampled.select(*identity_cols, "quadrant")
        pop_pdf = keyset.join(
            test_model_input, on=identity_cols, how="inner").toPandas()

        # ---- 輸出 2:全格極值案例(role=high/low)----
        # 不對稱 tiebreak:同分格 high/low 落不同列;真正單行格才落同一列。
        w_high = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col(score_col).desc(), F.col("_ck").asc())
        w_low = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col(score_col).asc(), F.col("_ck").desc())
        highs = (labeled.withColumn("_rn", F.row_number().over(w_high))
                 .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("high")))
        lows = (labeled.withColumn("_rn", F.row_number().over(w_low))
                .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("low")))
        extremes = highs.unionByName(lows).select(
            *identity_cols, "quadrant", "role",
            F.col("_rank").alias("rank"), F.col(score_col).alias("score"),
            F.col(label_col).alias("label"))
        # test_model_input 也有 label 欄 → drop 以免 join 後 ambiguous(label 非特徵)。
        feats_only = (test_model_input.drop(label_col)
                      if label_col in test_model_input.columns else test_model_input)
        case_pdf = extremes.join(
            feats_only, on=identity_cols, how="inner").toPandas()
    finally:
        # Runner 只釋放 MemoryDataset,不碰 Spark DataFrame 的 storage(core/runner.py
        # 與 core/catalog.py 都沒有 unpersist)。少了這裡,這份 cache 會佔著
        # executor 直到 SparkSession 結束。finally 而非成功路徑:a failure above
        # leaves this function too, on its way to stopping the run.
        if labeled is not None:
            try:
                labeled.unpersist()
            except Exception as release_error:
                # Logged, not raised. Raised from here it would replace the
                # exception the body is propagating (the one that says what
                # went wrong); on the success path the results are already in
                # the driver, and the leaked cache is the whole cost.
                logger.warning(
                    "select_shap_population: unpersist failed: %s", release_error)

    logger.info(
        "select_shap_population: pop_rows=%d case_rows=%d items=%d per_cell=%d",
        len(pop_pdf), len(case_pdf),
        pop_pdf[item_col].nunique() if len(pop_pdf) else 0, per_cell,
    )
    return pop_pdf, case_pdf

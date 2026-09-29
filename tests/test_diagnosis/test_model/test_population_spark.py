"""Tests for select_shap_population (Spark 選樣:rank/象限/每格抽樣/join)."""

import pytest


def _params(per_cell=30, top_k=1, enabled=True, months=("2024-01-31",)):
    return {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                                   "item": "prod_name", "label": "label"}},
            "dataset": {"test_snap_dates": list(months)},
            "diagnostics": {"shap": {"quadrant_enabled": enabled,
                                     "quadrant_top_k_decision": top_k,
                                     "quadrant_sample_per_cell": per_cell}}}


_PRED_COLS = ["snap_date", "cust_id", "prod_name", "score", "label"]
_FEAT_COLS = ["snap_date", "cust_id", "prod_name", "f0", "f1"]


def test_quadrant_assignment_and_features_joined(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1),   # rank1 adopted -> TP
         ("2024-01-31", "c1", "B", 0.2, 0),   # rank2 not     -> TN
         ("2024-01-31", "c2", "A", 0.8, 0),   # rank1 not     -> FP
         ("2024-01-31", "c2", "B", 0.3, 1)],  # rank2 adopted -> FN
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0),
         ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2),
         ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    pdf, _cases = select_shap_population(preds, feats, _params())
    q = {(r.cust_id, r.prod_name): r.quadrant for r in pdf.itertuples()}
    assert q[("c1", "A")] == "TP"
    assert q[("c1", "B")] == "TN"
    assert q[("c2", "A")] == "FP"
    assert q[("c2", "B")] == "FN"
    assert {"f0", "f1"} <= set(pdf.columns)        # 特徵 join 進來
    assert len(pdf) == 4


def test_per_cell_cap_and_determinism(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    # (A, TP) 有 2 列;per_cell=1 → 只留 1,且兩次結果相同
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1),
         ("2024-01-31", "c1", "B", 0.1, 0),
         ("2024-01-31", "c2", "A", 0.9, 1),
         ("2024-01-31", "c2", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0),
         ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2),
         ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    p = _params(per_cell=1)
    a, _ = select_shap_population(preds, feats, p)
    b, _ = select_shap_population(preds, feats, p)
    tp_a = a[(a.prod_name == "A") & (a.quadrant == "TP")]
    tp_b = b[(b.prod_name == "A") & (b.quadrant == "TP")]
    assert len(tp_a) == 1
    assert list(tp_a["cust_id"]) == list(tp_b["cust_id"])   # 確定性


def test_disabled_returns_none(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    preds = spark.createDataFrame([("2024-01-31", "c1", "A", 0.9, 1)], _PRED_COLS)
    feats = spark.createDataFrame([("2024-01-31", "c1", "A", 1.0, 2.0)], _FEAT_COLS)
    assert select_shap_population(preds, feats, _params(enabled=False)) == (None, None)


def test_case_rows_extremes_role_and_features(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    # c1/c2/c4 三位客戶,item A 都排第1(score 高於 B)→ (A, TP)。
    # (A, TP) 有 3 列,分數 0.9/0.7/0.5 → high=c1, low=c4。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.1, 0),
         ("2024-01-31", "c2", "A", 0.7, 1), ("2024-01-31", "c2", "B", 0.1, 0),
         ("2024-01-31", "c4", "A", 0.5, 1), ("2024-01-31", "c4", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2), ("2024-01-31", "c2", "B", 1.3, 2.3),
         ("2024-01-31", "c4", "A", 1.4, 2.4), ("2024-01-31", "c4", "B", 1.5, 2.5)],
        _FEAT_COLS)
    _pop, cases = select_shap_population(preds, feats, _params())
    a_tp = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")]
    roles = {r.role: r.cust_id for r in a_tp.itertuples()}
    assert roles["high"] == "c1"          # 全格最高分
    assert roles["low"] == "c4"           # 全格最低分
    assert {"f0", "f1"} <= set(cases.columns)          # 特徵 join 進來
    assert {"quadrant", "role", "rank", "score", "label"} <= set(cases.columns)
    assert float(a_tp[a_tp.role == "high"]["score"].iloc[0]) == 0.9


def test_case_rows_single_row_cell_marks_same_row(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    # (A, TP) 只有 c1 一列 → high 與 low 落在同一 group-key。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1)],
        _FEAT_COLS)
    _pop, cases = select_shap_population(preds, feats, _params())
    a_tp = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")]
    hi = a_tp[a_tp.role == "high"].iloc[0]
    lo = a_tp[a_tp.role == "low"].iloc[0]
    assert (hi.snap_date, hi.cust_id) == (lo.snap_date, lo.cust_id)   # 同一列


def test_case_rows_tiebreak_same_score_picks_distinct_rows(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    # (A, TP) 兩列同分(0.9)→ 不對稱 tiebreak 必須挑到不同列(high≠low)。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.1, 0),
         ("2024-01-31", "c2", "A", 0.9, 1), ("2024-01-31", "c2", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2), ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    _pop, cases = select_shap_population(preds, feats, _params())
    a_tp = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")]
    hi = a_tp[a_tp.role == "high"]["cust_id"].iloc[0]
    lo = a_tp[a_tp.role == "low"]["cust_id"].iloc[0]
    assert hi != lo          # 同分也挑到不同列(_ck ASC vs DESC)


def test_case_rows_feed_into_compute_quadrant_cases(spark, tmp_path, monkeypatch):
    """整合:select_shap_population 的 case_rows 直接餵進 compute_quadrant_cases,
    守住 Spark 產出↔pandas 消費的欄位契約(任一側 alias 改名都會被此測試抓到)。"""
    import numpy as np

    from tests.adapter_fits import fit_lightgbm
    from recsys_tfb.diagnosis.model.shap_cases import compute_quadrant_cases
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    monkeypatch.chdir(tmp_path)
    rng = np.random.RandomState(0)
    Xtr = rng.randn(200, 2)
    ytr = (Xtr[:, 0] > 0).astype(float)
    # feature_name mirrors production: prepare_train_inputs always sets it, and
    # compute_quadrant_cases now takes the model's declaration as authoritative.
    adapter = fit_lightgbm(
        Xtr, ytr,
        {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
         "num_leaves": 4, "seed": 1, "num_iterations": 10,
         "early_stopping_rounds": 0},
        feature_names=["f0", "f1"],
    )
    prep = {"feature_columns": ["f0", "f1"], "categorical_columns": [], "category_mappings": {}}
    params = _params()
    params["model_version"] = "mv_integ"
    # c1: A rank1 label1→TP;B rank2 label0→TN。c2: A rank1 label0→FP;B rank2 label1→FN。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.2, 0),
         ("2024-01-31", "c2", "A", 0.8, 0), ("2024-01-31", "c2", "B", 0.3, 1)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2), ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    _pop, case_rows = select_shap_population(preds, feats, params)
    manifest, figures = compute_quadrant_cases(adapter, case_rows, prep, params)
    assert set(manifest) == {"A", "B"}
    tp = manifest["A"]["TP"]["high"]     # metadata 須經 seam 完整帶到
    assert tp["rendered"] and tp["cust_id"] == "c1" and tp["label"] == 1 and tp["rank"] == 1
    assert tp["png"] == "cases/A/TP_high.png" and "A/TP_high.png" in figures


# ---- persist / unpersist(T5):cache 不得留在 executor 上 ----------------------

def _persistent_rdd_ids(spark):
    """SparkSession 目前掛著的 Spark cache。

    外部觀察 Spark 自己的狀態,不是斷言「``unpersist()`` 有沒有被呼叫過」——
    後者換個寫法就繞過去了,前者繞不過。
    """
    return set(spark.sparkContext._jsc.getPersistentRDDs().keySet().toArray())


def _preds_and_feats(spark):
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.2, 0),
         ("2024-01-31", "c2", "A", 0.8, 0), ("2024-01-31", "c2", "B", 0.3, 1)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2), ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    return preds, feats


def test_success_path_leaves_no_spark_cache(spark):
    """成功跑完後 SparkSession 不得留下這個 node 的 cache。

    Runner 只釋放 MemoryDataset,不碰 Spark DataFrame 的 storage——少了
    unpersist,那份 cache 會佔著 executor 直到 SparkSession 結束。
    """
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    preds, feats = _preds_and_feats(spark)
    before = _persistent_rdd_ids(spark)
    pop, cases = select_shap_population(preds, feats, _params())
    assert pop is not None and cases is not None      # 兩條分支真的都跑到了
    assert _persistent_rdd_ids(spark) - before == set()


def test_failure_path_leaves_no_spark_cache_and_stops_the_run(spark, monkeypatch):
    """第一條分支跑完、第二條炸掉:cache 仍要釋放,而錯誤往上拋、training 停下
    (ADR-0030 decision 4:這個 node 不碰模型能力,沒有「模型做不到」可言)。

    這是 try/finally 而非「只在成功路徑釋放」的理由;失敗路徑同樣會離開這個函式。
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    def _boom(self, other, allowMissingColumns=False):
        raise RuntimeError("injected failure after the first toPandas()")

    monkeypatch.setattr(DataFrame, "unionByName", _boom)
    preds, feats = _preds_and_feats(spark)
    before = _persistent_rdd_ids(spark)
    with pytest.raises(RuntimeError, match="injected failure"):
        select_shap_population(preds, feats, _params())
    assert _persistent_rdd_ids(spark) - before == set()


def test_ranked_frame_is_persisted_with_explicit_memory_and_disk(spark, monkeypatch):
    """排名＋象限標記那份結果真的被 persist,且 StorageLevel 是顯式指定的。

    沒有這條的話,上面兩條「不得留下 cache」在「根本沒 persist」時也會綠
    (假綠形態:不存在斷言同時被「正確釋放」與「根本沒嘗試」滿足)。
    斷言讀的是 Spark 自己記的 storage level,不是呼叫紀錄。
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    original_unpersist = DataFrame.unpersist
    observed = []

    def _spy(self, blocking=False):
        observed.append(self.storageLevel)            # 釋放前先問 Spark 現在存哪
        return original_unpersist(self, blocking)

    monkeypatch.setattr(DataFrame, "unpersist", _spy)
    preds, feats = _preds_and_feats(spark)
    pop, _cases = select_shap_population(preds, feats, _params())
    assert pop is not None
    assert observed, "node 沒有釋放任何 persist 的 DataFrame"
    level = observed[0]
    assert (level.useMemory, level.useDisk) == (True, True)   # MEMORY_AND_DISK


def test_unpersist_failure_is_logged_not_raised(spark, monkeypatch):
    """釋放失敗(例如 SparkSession 已死)只記 log:raise 在 ``finally`` 裡會蓋掉
    body 正在往上拋的那個說明出錯原因的例外;成功路徑上結果已經在 driver,漏掉
    的只是那份 cache。
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    def _boom(self, blocking=False):
        raise RuntimeError("simulated dead SparkSession on release")

    monkeypatch.setattr(DataFrame, "unpersist", _boom)
    preds, feats = _preds_and_feats(spark)
    pop, cases = select_shap_population(preds, feats, _params())
    assert pop is not None and cases is not None      # 成功路徑仍回得了結果



# ---- ADR-0030 decisions 5 and 12: months, tie rule, score column -------------

def test_reads_only_the_configured_test_months(spark):
    """The prediction table and test_model_input keep every month written
    under their version; a month outside dataset.test_snap_dates must not
    reach the population or the cases (node rule 14). The stray month's
    customer would be the extreme of its cell if it were read."""
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    preds, feats = _preds_and_feats(spark)
    stray_p = spark.createDataFrame(
        [("2023-12-31", "c9", "A", 0.99, 1), ("2023-12-31", "c9", "B", 0.01, 0)],
        _PRED_COLS)
    stray_f = spark.createDataFrame(
        [("2023-12-31", "c9", "A", 9.0, 9.0), ("2023-12-31", "c9", "B", 9.0, 9.0)],
        _FEAT_COLS)
    pop, cases = select_shap_population(
        preds.unionByName(stray_p), feats.unionByName(stray_f), _params())
    assert set(pop["snap_date"]) == {"2024-01-31"}
    assert set(cases["snap_date"]) == {"2024-01-31"}
    assert "c9" not in set(pop["cust_id"]) | set(cases["cust_id"])


def test_no_configured_test_month_stops_rather_than_reading_everything(spark):
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    preds, feats = _preds_and_feats(spark)
    with pytest.raises(ValueError, match="test_snap_dates"):
        select_shap_population(preds, feats, _params(months=()))


_EVENT_PRED_COLS = ["snap_date", "cust_id", "prod_name", "ts", "score", "label"]
_EVENT_FEAT_COLS = ["snap_date", "cust_id", "prod_name", "ts", "f0", "f1"]


def _event_params():
    p = _params()
    p["schema"]["columns"]["event"] = "ts"
    return p


def test_with_event_declared_top1_is_evaluations_top1(spark):
    """Two rows of one item in one query group, tied on score: evaluation
    ranks the earlier event first (utils/ranking.py), so the quadrants must
    too. The later event comes first in the input so that the old window —
    score, then item, nothing after — has no reason to agree."""
    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population
    from recsys_tfb.evaluation.metrics_spark import rank_within_query

    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 2, 0.9, 0),
         ("2024-01-31", "c1", "A", 1, 0.9, 1),
         ("2024-01-31", "c1", "B", 1, 0.1, 0)],
        _EVENT_PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 2, 1.0, 2.0),
         ("2024-01-31", "c1", "A", 1, 1.1, 2.1),
         ("2024-01-31", "c1", "B", 1, 1.2, 2.2)],
        _EVENT_FEAT_COLS)
    params = _event_params()
    schema = get_schema(params)

    pop, _cases = select_shap_population(preds, feats, params)
    evaluation = rank_within_query(
        preds, schema["query_group_columns"], "score", "prod_name", ["ts"]).toPandas()
    top_ts = int(evaluation.loc[evaluation["pos"] == 1, "ts"].iloc[0])
    assert top_ts == 1
    quadrant = {int(r.ts): r.quadrant for r in pop[pop.prod_name == "A"].itertuples()}
    assert quadrant == {1: "TP", 2: "TN"}


def test_the_score_column_follows_schema(spark):
    """A deployment that calls the score column something else. What goes to
    compute_quadrant_cases is still called ``score`` — the two modules'
    agreement, not the prediction table's column."""
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    preds, feats = _preds_and_feats(spark)
    renamed = preds.withColumnRenamed("score", "pred")
    params = _params()
    params["schema"]["columns"]["score"] = "pred"

    pop, cases = select_shap_population(renamed, feats, params)
    q = {(r.cust_id, r.prod_name): r.quadrant for r in pop.itertuples()}
    assert q == {("c1", "A"): "TP", ("c1", "B"): "TN",
                 ("c2", "A"): "FP", ("c2", "B"): "FN"}
    high = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")
                 & (cases.role == "high")]
    assert float(high["score"].iloc[0]) == 0.9

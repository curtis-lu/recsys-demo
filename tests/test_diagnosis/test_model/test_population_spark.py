"""Tests for select_shap_population (Spark 選樣:rank/象限/每格抽樣/join)."""


def _params(per_cell=30, top_k=1, enabled=True):
    return {"schema": {"time": "snap_date", "entity": ["cust_id"],
                       "item": "prod_name", "label": "label"},
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
    import lightgbm as lgb
    import numpy as np

    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    from recsys_tfb.diagnosis.model.shap_cases import compute_quadrant_cases
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    monkeypatch.chdir(tmp_path)
    rng = np.random.RandomState(0)
    Xtr = rng.randn(200, 2)
    ytr = (Xtr[:, 0] > 0).astype(float)
    # feature_name mirrors production: prepare_train_inputs always sets it, and
    # compute_quadrant_cases now takes the model's declaration as authoritative.
    ds = lgb.Dataset(Xtr, label=ytr, feature_name=["f0", "f1"], free_raw_data=False)
    adapter = LightGBMAdapter()
    adapter.train(Xtr, ytr, None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 10,
                   "early_stopping_rounds": 0},
                  train_dataset=ds)
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
    manifest = compute_quadrant_cases(adapter, case_rows, prep, params)
    assert set(manifest) == {"A", "B"}
    tp = manifest["A"]["TP"]["high"]     # metadata 須經 seam 完整帶到
    assert tp["rendered"] and tp["cust_id"] == "c1" and tp["label"] == 1 and tp["rank"] == 1
    assert (tmp_path / "data/models/mv_integ/diagnostics/cases/A/TP_high.png").exists()


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


def test_failure_path_leaves_no_spark_cache_and_stays_best_effort(spark, monkeypatch):
    """第一條分支跑完、第二條炸掉:cache 仍要釋放,且契約不變(只 warn)。

    這是 try/finally 而非「只在成功路徑釋放」的理由;失敗路徑同樣會離開這個函式。
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    def _boom(self, other, allowMissingColumns=False):
        raise RuntimeError("injected failure after the first toPandas()")

    monkeypatch.setattr(DataFrame, "unionByName", _boom)
    preds, feats = _preds_and_feats(spark)
    before = _persistent_rdd_ids(spark)
    out = select_shap_population(preds, feats, _params())
    assert out == (None, None)                        # best-effort:不中斷訓練
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


def test_unpersist_failure_does_not_break_best_effort(spark, monkeypatch):
    """釋放失敗(例如 SparkSession 已死)不得把 best-effort 變成硬失敗。

    persist 之前,``except`` 之後沒有任何會 raise 的東西;persist 帶進了一個新的
    失敗來源,而 ``finally`` 裡的 raise 會蓋掉上面的 return。
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    def _boom(self, blocking=False):
        raise RuntimeError("simulated dead SparkSession on release")

    monkeypatch.setattr(DataFrame, "unpersist", _boom)
    preds, feats = _preds_and_feats(spark)
    pop, cases = select_shap_population(preds, feats, _params())
    assert pop is not None and cases is not None      # 成功路徑仍回得了結果

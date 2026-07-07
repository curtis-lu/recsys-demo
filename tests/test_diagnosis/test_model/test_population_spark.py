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
    import numpy as np

    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    from recsys_tfb.diagnosis.model.shap_cases import compute_quadrant_cases
    from recsys_tfb.diagnosis.model.population_spark import select_shap_population

    monkeypatch.chdir(tmp_path)
    rng = np.random.RandomState(0)
    Xtr = rng.randn(200, 2)
    ytr = (Xtr[:, 0] > 0).astype(float)
    adapter = LightGBMAdapter()
    adapter.train(Xtr, ytr, None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 10, "early_stopping_rounds": 0})
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

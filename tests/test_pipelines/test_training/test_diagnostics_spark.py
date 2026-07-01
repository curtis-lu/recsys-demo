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
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
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
    pdf = select_shap_population(preds, feats, _params())
    q = {(r.cust_id, r.prod_name): r.quadrant for r in pdf.itertuples()}
    assert q[("c1", "A")] == "TP"
    assert q[("c1", "B")] == "TN"
    assert q[("c2", "A")] == "FP"
    assert q[("c2", "B")] == "FN"
    assert {"f0", "f1"} <= set(pdf.columns)        # 特徵 join 進來
    assert len(pdf) == 4


def test_per_cell_cap_and_determinism(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
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
    a = select_shap_population(preds, feats, p)
    b = select_shap_population(preds, feats, p)
    tp_a = a[(a.prod_name == "A") & (a.quadrant == "TP")]
    tp_b = b[(b.prod_name == "A") & (b.quadrant == "TP")]
    assert len(tp_a) == 1
    assert list(tp_a["cust_id"]) == list(tp_b["cust_id"])   # 確定性


def test_disabled_returns_none(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    preds = spark.createDataFrame([("2024-01-31", "c1", "A", 0.9, 1)], _PRED_COLS)
    feats = spark.createDataFrame([("2024-01-31", "c1", "A", 1.0, 2.0)], _FEAT_COLS)
    assert select_shap_population(preds, feats, _params(enabled=False)) is None

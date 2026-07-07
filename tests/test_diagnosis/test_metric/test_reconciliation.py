"""theoretical_offsets：抽樣×權重的正負類曝險比 → per-cell 理論偏移＋item 摘要帶。"""

import math

import pytest

from recsys_tfb.diagnosis.metric.reconciliation import theoretical_offsets


def _params(overrides=None, sample_ratio=1.0, group_keys=None,
            weights=None, weight_keys=None):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "dataset": {
            "sample_ratio": sample_ratio,
            "sample_group_keys": group_keys
                or ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio_overrides": overrides or {},
        },
        "training": {
            "sample_weight_keys": weight_keys or ["prod_name"],
            "sample_weights": weights or {},
        },
    }


def test_neg_only_retention_gives_minus_log_r():
    # 只砍負類保留 0.5 → offset = −ln 0.5 = +0.693（手冊3 Ch10 logQ）
    out = theoretical_offsets(_params(overrides={"mass|fund_bond|0": 0.5}))
    cell = out["cells"]["mass|fund_bond"]
    assert cell["r_neg"] == 0.5 and cell["r_pos"] == 1.0
    assert cell["offset"] == pytest.approx(math.log(2))
    band = out["by_item"]["fund_bond"]
    assert band["min"] == band["max"] == pytest.approx(math.log(2))
    assert band["approx"] is True and band["n_cells"] == 1


def test_base_config_ccard_ins_band():
    # 現行 conf/base 的實際 overrides → 帶 [ln(10/9), ln 2]
    out = theoretical_offsets(_params(overrides={
        "mass|ccard_ins|0": 0.5,
        "affluent|ccard_ins|0": 0.9,
        "hnw|ccard_ins|0": 0.8,
    }))
    band = out["by_item"]["ccard_ins"]
    assert band["min"] == pytest.approx(math.log(1 / 0.9))
    assert band["max"] == pytest.approx(math.log(2))
    assert band["n_cells"] == 3


def test_symmetric_retention_cancels():
    # 正負類同率 → 0（label 對稱不移動 level）
    out = theoretical_offsets(_params(overrides={
        "mass|fund_bond|0": 0.5, "mass|fund_bond|1": 0.5,
    }))
    assert out["cells"]["mass|fund_bond"]["offset"] == pytest.approx(0.0)


def test_default_sample_ratio_fills_missing_class():
    # 全域 sample_ratio=0.8，只 override 負類 0.4 → offset = ln(0.8/0.4)
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond|0": 0.4}, sample_ratio=0.8,
    ))
    assert out["cells"]["mass|fund_bond"]["offset"] == pytest.approx(
        math.log(0.8 / 0.4)
    )


def test_label_not_in_group_keys_gives_no_sampling_offset():
    # label 不在 sample_group_keys → 抽樣對 label 對稱 → 無 cell
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond": 0.5},
        group_keys=["cust_segment_typ", "prod_name"],
    ))
    assert out["cells"] == {} and out["by_item"] == {}
    assert any("label" in n for n in out["notes"])


def test_label_aware_weights_shift():
    # weight_keys 含 label：正類 boost 2.0 → offset = ln 2；與抽樣疊乘
    out = theoretical_offsets(_params(
        overrides={"mass|fund_bond|0": 0.5},
        weights={"fund_bond|1": 2.0},
        weight_keys=["prod_name", "label"],
    ))
    # cell key 是 sample_group_keys 的非 label 維（mass|fund_bond）；
    # 權重 cell key 是 weight_keys 的非 label 維（fund_bond）——兩組維度
    # 不同時各自細列，item 摘要帶取聯集
    band = out["by_item"]["fund_bond"]
    assert band["max"] == pytest.approx(math.log(2) + math.log(2))


def test_label_not_in_weight_keys_gives_no_weight_offset():
    # 現行 config：weight_keys=[prod_name] 無 label → 權重貢獻 0
    out = theoretical_offsets(_params(weights={"fund_bond": 3.0}))
    assert out["cells"] == {} and out["by_item"] == {}


def _eval_df(spark, rows):
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score",
                "score_uncalibrated", "label"],
    )


def _full_params(**kw):
    p = _params(**{k: v for k, v in kw.items()
                   if k in ("overrides", "sample_ratio", "group_keys",
                            "weights", "weight_keys")})
    p["evaluation"] = {
        "diagnosis": {
            "reconciliation": {
                "enabled": True,
                "score_col": kw.get("score_col", "score_uncalibrated"),
                "explained_threshold": kw.get("threshold", 0.3),
            },
        },
    }
    return p


def test_calibration_gap_known_value(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import calibration_gap_by_item
    # item A：p̄=0.6、ȳ=0.5 → gap = logit(0.6) − logit(0.5) = ln(1.5)
    rows = [
        ("20240331", "C0", "A", 0.6, 0.6, 1),
        ("20240331", "C1", "A", 0.6, 0.6, 0),
    ]
    out = calibration_gap_by_item(_eval_df(spark, rows), _full_params(), "score")
    assert out["A"]["gap"] == pytest.approx(math.log(1.5))
    assert out["A"]["p_mean"] == pytest.approx(0.6)
    assert out["A"]["y_rate"] == pytest.approx(0.5)
    assert out["A"]["n_rows"] == 2


def test_calibration_gap_degenerate_rate_guarded(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import calibration_gap_by_item
    rows = [("20240331", "C0", "A", 0.6, 0.6, 1)]  # ȳ=1 → logit 未定義
    out = calibration_gap_by_item(_eval_df(spark, rows), _full_params(), "score")
    assert out["A"]["gap"] is None and out["A"]["reason"]


def test_reconcile_verdict_and_dual_columns(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # 理論帶 [ln2, ln2]（注入 0.5）；uncalibrated gap = ln(1.5/0.5... ) 構造：
    # A：score_uncalibrated p̄=2/3、ȳ=1/3 → gap = logit(2/3)−logit(1/3) = 2 ln 2
    #    帶 [ln2,ln2]、residual = 2ln2 − ln2 = ln2 ≈ 0.693 > 0.3 → 不可解釋
    # B：無 override → 帶 [0,0]；gap=0 → 可解釋
    rows = [
        ("20240331", "C0", "A", 0.5, 2 / 3, 1),
        ("20240331", "C1", "A", 0.5, 2 / 3, 0),
        ("20240331", "C2", "A", 0.5, 2 / 3, 0),
        ("20240331", "C0", "B", 0.5, 0.5, 1),
        ("20240331", "C1", "B", 0.5, 0.5, 0),
    ]
    params = _full_params(overrides={"mass|A|0": 0.5})
    out = reconcile(_eval_df(spark, rows), params)
    a = out["by_item"]["A"]
    assert a["theory_min"] == a["theory_max"] == pytest.approx(math.log(2))
    assert a["gap"] == pytest.approx(2 * math.log(2))
    assert a["residual"] == pytest.approx(math.log(2))
    assert a["verdict"] == "不可解釋"
    assert "gap_calibrated" in a  # score 欄對照
    b = out["by_item"]["B"]
    assert b["theory_min"] == b["theory_max"] == 0.0
    assert b["verdict"] == "可解釋"
    assert out["all_explained"] is False
    assert out["score_col_used"] == "score_uncalibrated"
    assert out["fallback"] is False


def test_reconcile_gap_inside_band_is_explained(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # gap = ln2 恰在帶 [ln2, ln2] 內 → residual 0 → 可解釋
    rows = [
        ("20240331", "C0", "A", 0.5, 0.5, 1),
        ("20240331", "C1", "A", 0.5, 0.5, 0),
    ]
    # p̄=0.5、ȳ=0.5 → gap=0；帶 [0,0]（無 override）→ 可解釋
    out = reconcile(_eval_df(spark, rows), _full_params())
    assert out["by_item"]["A"]["verdict"] == "可解釋"
    assert out["all_explained"] is True


def test_reconcile_fallback_when_uncalibrated_missing(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    df = spark.createDataFrame(
        [("20240331", "C0", "A", 0.5, 1), ("20240331", "C1", "A", 0.5, 0)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    out = reconcile(df, _full_params())
    assert out["fallback"] is True and out["score_col_used"] == "score"


def test_reconcile_global_reference_median(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # 3 個中性 item（無 override，帶 [0,0]）＋ 1 個有 override 的 item。
    # 每 item 2 列（1 正 1 負）、ȳ 皆 0.5，用 score_uncalibrated 控制 p̄：
    #   neutral1 p̄=0.30 → gap = logit(0.30) = ln(3/7)  ≈ -0.8473
    #   neutral2 p̄=0.25 → gap = logit(0.25) = ln(1/3)  ≈ -1.0986（中位數）
    #   neutral3 p̄=0.20 → gap = logit(0.20) = ln(1/4)  ≈ -1.3863
    #   offset   p̄=0.40 → gap = logit(0.40) = ln(2/3)  ≈ -0.4055（override
    #     mass|prod_offset|0=0.5 → 理論帶 [ln2, ln2]）
    # 全局參考值 = median(3 個中性 gap) = ln(1/3)。
    # offset item 的 gap_vs_global = ln(2/3) − ln(1/3) = ln2，恰落理論帶內。
    rows = [
        ("20240331", "C0", "prod_neutral1", 0.30, 0.30, 1),
        ("20240331", "C1", "prod_neutral1", 0.30, 0.30, 0),
        ("20240331", "C2", "prod_neutral2", 0.25, 0.25, 1),
        ("20240331", "C3", "prod_neutral2", 0.25, 0.25, 0),
        ("20240331", "C4", "prod_neutral3", 0.20, 0.20, 1),
        ("20240331", "C5", "prod_neutral3", 0.20, 0.20, 0),
        ("20240331", "C6", "prod_offset", 0.40, 0.40, 1),
        ("20240331", "C7", "prod_offset", 0.40, 0.40, 0),
    ]
    params = _full_params(overrides={"mass|prod_offset|0": 0.5})
    out = reconcile(_eval_df(spark, rows), params)

    assert out["global"]["method"] == "median_of_config_neutral_items"
    assert out["global"]["reference"] == pytest.approx(math.log(1 / 3))
    assert out["global"]["n_neutral_items"] == 3
    assert out["global"]["pooled_gap"] is not None

    for item in ("prod_neutral1", "prod_neutral2", "prod_neutral3"):
        assert out["by_item"][item]["verdict"] == "可解釋"

    d = out["by_item"]["prod_offset"]
    assert d["theory_min"] == d["theory_max"] == pytest.approx(math.log(2))
    assert d["gap_vs_global"] == pytest.approx(math.log(2))
    assert d["residual"] == pytest.approx(0.0, abs=1e-6)
    assert d["verdict"] == "可解釋"


def test_reconcile_fallback_reference_zero_when_few_neutral(spark):
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    # 只有 1 個中性 item（B）→ 候選數 < 3 → global_reference 退回 0.0，
    # method 是 fallback，行為同原絕對語意（gap_vs_global == gap）。
    rows = [
        ("20240331", "C0", "A", 0.5, 2 / 3, 1),
        ("20240331", "C1", "A", 0.5, 2 / 3, 0),
        ("20240331", "C2", "A", 0.5, 2 / 3, 0),
        ("20240331", "C0", "B", 0.5, 0.5, 1),
        ("20240331", "C1", "B", 0.5, 0.5, 0),
    ]
    params = _full_params(overrides={"mass|A|0": 0.5})
    out = reconcile(_eval_df(spark, rows), params)

    assert out["global"]["reference"] == 0.0
    assert out["global"]["method"] == "insufficient_neutral_items_fallback_zero"
    assert out["global"]["n_neutral_items"] == 1

    a = out["by_item"]["A"]
    assert a["gap_vs_global"] == pytest.approx(a["gap"])
    assert a["verdict"] == "不可解釋"
    b = out["by_item"]["B"]
    assert b["gap_vs_global"] == pytest.approx(0.0)
    assert b["verdict"] == "可解釋"

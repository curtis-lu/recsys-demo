"""build_quadrant_summary：兩軸合成象限＋傷害觀測（best-effort 降級）。"""

import pytest

from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary


def _params(auc_threshold=0.6, gap_band=0.35, top_k=1):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "evaluation": {
            "diagnosis": {
                "quadrant": {
                    "enabled": True,
                    "auc_threshold": auc_threshold,
                    "gap_band": gap_band,
                    "top_k_occupancy": top_k,
                },
            },
        },
    }


def _eval_df(spark):
    # 兩個 query。item A：判別力好（正例分高）；item B：常數分數（判別力零）
    # 且永遠佔 rank 1、以負例壓 A 的正例（C1 那個 query）。
    rows = [
        # query C0：A 正例 0.9 排 1、B 負例 0.8 排 2
        ("20240331", "C0", "A", 0.9, 1, 1),
        ("20240331", "C0", "B", 0.8, 0, 2),
        # query C1：B 負例 0.8 排 1、A 正例 0.7 排 2 → B 壓制 +1
        ("20240331", "C1", "B", 0.8, 0, 1),
        ("20240331", "C1", "A", 0.7, 1, 2),
        # query C2：A 負例 0.1 排 2、B 正例 0.8 排 1
        ("20240331", "C2", "B", 0.8, 1, 1),
        ("20240331", "C2", "A", 0.1, 0, 2),
    ]
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def _label_df(spark):
    return spark.createDataFrame(
        [("20240331", "C0", "A", 1), ("20240331", "C1", "A", 1),
         ("20240331", "C2", "B", 1)],
        schema=["snap_date", "cust_id", "prod_name", "label"],
    )


def _recon(by_item):
    return {"enabled": True, "by_item": by_item}


def _ci(per_item):
    return {"enabled": True, "per_item": per_item}


def test_quadrant_labels_and_aggressor(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=_ci({"A": {"ap": 0.8, "ci_low": 0.6, "ci_high": 0.9,
                             "n_pos": 2}}),
        reconciliation=_recon({
            "A": {"gap_vs_global": 0.0},
            "B": {"gap_vs_global": 0.9},
        }),
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    # A：AUC=1.0（正例 0.9/0.7 > 負例 0.1）、gvg=0 → 健康
    assert a["auc"] == pytest.approx(1.0)
    assert a["level_status"] == "正常" and a["disc_status"] == "好"
    assert a["quadrant"] == "健康" and a["is_aggressor"] is False
    assert a["ap_sampled"] == pytest.approx(0.8)
    b = out["by_item"]["B"]
    # B：常數分數 → AUC=0.5（差）；gvg=0.9 > 0.35 → 偏高 → 常數高分型加害者
    assert b["auc"] == pytest.approx(0.5)
    assert b["level_status"] == "偏高" and b["disc_status"] == "差"
    assert b["quadrant"] == "加害者（常數高分型）"
    assert b["is_aggressor"] is True
    assert b["suppression_count"] == 1
    assert b["ap_sampled"] is None  # metric_ci 沒給 B → None 不炸
    assert a["suppression_count"] == 0  # 零壓制補 0
    assert out["thresholds"]["gap_band"] == pytest.approx(0.35)
    assert out["cross_purchase"]["n_buyers"]["A"] == 2


def test_low_level_side_labels(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=None,
        reconciliation=_recon({
            "A": {"gap_vs_global": -0.9},
            "B": {"gap_vs_global": -0.9},
        }),
        parameters=_params(),
    )
    assert out["by_item"]["A"]["quadrant"] == "受害者（水準偏低、判別力好）"
    assert out["by_item"]["B"]["quadrant"] == "雙重受害"


def test_degrades_when_upstreams_are_stubs(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci={"enabled": False},
        reconciliation={"enabled": False},
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    assert a["gap_vs_global"] is None
    assert a["level_status"] == "無法評估" and a["quadrant"] == "無法評估"
    assert a["auc"] == pytest.approx(1.0)  # AUC 軸照算
    assert out["sources"] == {"reconciliation": False, "metric_ci": False}
    assert len(out["notes"]) == 2

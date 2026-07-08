"""triage：合成三層診斷 dict（quadrant/reconciliation/offset_sweep/gain_ledger）
→ per-item 判定表（框架 Ch4 判讀流程）。純 dict fixture，不碰 Spark。"""
import pytest

from recsys_tfb.diagnosis.metric.triage import STARTER_CAVEAT, triage


def _quadrant(by_item, enabled=True):
    return {"enabled": enabled, "by_item": by_item}


def _base_quadrant_by_item():
    return {
        "cfg": {"auc": 0.7, "disc_status": "好", "level_status": "偏低",
                "gap_vs_global": -0.6, "auc_reason": None},
        "reb": {"auc": 0.7, "disc_status": "好", "level_status": "偏高",
                "gap_vs_global": 0.5, "auc_reason": None},
        "stv": {"auc": 0.5, "disc_status": "差", "level_status": "正常",
                "gap_vs_global": 0.0, "auc_reason": None, "y_rate": 0.02},
        "feat": {"auc": 0.5, "disc_status": "差", "level_status": "正常",
                 "gap_vs_global": 0.0, "auc_reason": None, "y_rate": 0.05},
        "ok": {"auc": 0.8, "disc_status": "好", "level_status": "正常",
               "gap_vs_global": 0.0, "auc_reason": None, "y_rate": 0.4},
    }


def _recon_entry(theory_min, theory_max, approx):
    return {
        "verdict": "可解釋", "theory_min": theory_min, "theory_max": theory_max,
        "theory_approx": approx, "residual": 0.0,
    }


def _base_reconciliation():
    return {
        "enabled": True,
        "by_item": {
            "cfg": _recon_entry(0.1, 0.7, True),
            "reb": _recon_entry(0.0, 0.0, False),
            "stv": _recon_entry(0.0, 0.0, False),
            "feat": _recon_entry(0.0, 0.0, False),
            "ok": _recon_entry(0.0, 0.0, False),
        },
        "theory": {"by_item": {"cfg": {"min": 0.1, "max": 0.7, "mean": 0.4}}},
    }


def _sweep_entry(dsc, loo=0.0):
    return {"delta_star_centered": dsc, "loo_contribution_holdout": loo}


def _base_offset_sweep():
    return {
        "enabled": True,
        "per_item": {
            "cfg": _sweep_entry(-0.5),
            "reb": _sweep_entry(0.45),
            "stv": _sweep_entry(0.3),
            "feat": _sweep_entry(0.1),
            "ok": _sweep_entry(0.0),
        },
    }


def _base_gain_ledger():
    return {
        "enabled": True,
        "fallback": False,
        "per_item": {
            "cfg": {"context_gain_share": 0.3},
            "reb": {"context_gain_share": 0.3},
            "stv": {"context_gain_share": 0.02},
            "feat": {"context_gain_share": 0.35},
            "ok": {"context_gain_share": 0.3},
        },
    }


def test_six_verdicts_full_fixture():
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_reconciliation(),
        _base_offset_sweep(),
        _base_gain_ledger(),
        parameters={},
    )
    v = out["verdicts"]
    assert v["cfg"]["verdict"] == "水準-配置型"
    assert v["reb"]["verdict"] == "水準-指標再平衡型"
    assert v["stv"]["verdict"] == "餓死型"
    assert v["feat"]["verdict"] == "特徵缺失型"
    assert v["ok"]["verdict"] == "健康"

    assert v["cfg"]["lever"].startswith("槓桿1")
    assert v["reb"]["lever"].startswith("槓桿2")
    assert v["stv"]["lever"].startswith("槓桿3")
    assert v["feat"]["lever"].startswith("槓桿5")
    assert v["ok"]["lever"] == "無（維持觀測）"

    # starter 精確值
    assert v["cfg"]["starter"]["type"] == "logq_offset"
    assert v["cfg"]["starter"]["value"] == pytest.approx(0.4)
    assert v["cfg"]["starter"]["band"] == [0.1, 0.7]
    assert v["cfg"]["starter"]["caveat"] == STARTER_CAVEAT

    assert v["reb"]["starter"]["type"] == "delta_star_centered"
    assert v["reb"]["starter"]["value"] == pytest.approx(0.45)

    assert v["stv"]["starter"]["type"] == "item_weight"
    assert v["stv"]["starter"]["value"] == pytest.approx(4.47)

    assert v["feat"]["starter"] is None
    assert v["ok"]["starter"] is None

    assert out["gain_ledger_present"] is True
    assert out["thresholds"] == {"starve_ratio": 0.25, "weight_cap": 8.0}
    assert out["summary"] == {
        "水準-配置型": 1, "水準-指標再平衡型": 1, "餓死型": 1,
        "特徵缺失型": 1, "健康": 1,
    }
    assert any("待條件化 SHAP 佐證" in n for n in v["feat"]["notes"])


def test_priority_order_config_over_disc_low():
    """level_off + config_signal + disc_low 同時成立 → 配置型優先，
    且留下判別力也偏低的次要 note。"""
    by_item = {
        "x": {"auc": 0.5, "disc_status": "差", "level_status": "偏低",
              "gap_vs_global": -0.5, "auc_reason": None},
    }
    recon = {
        "enabled": True,
        "by_item": {"x": _recon_entry(0.1, 0.5, False)},
        "theory": {"by_item": {"x": {"min": 0.1, "max": 0.5, "mean": 0.3}}},
    }
    out = triage(
        _quadrant(by_item), recon, _base_offset_sweep(), _base_gain_ledger(), {}
    )
    vx = out["verdicts"]["x"]
    assert vx["verdict"] == "水準-配置型"
    assert any("條件判別力" in n for n in vx["notes"])


def test_gain_ledger_none_degrades_stv_to_no_structural_evidence():
    out = triage(
        _quadrant(_base_quadrant_by_item()), _base_reconciliation(),
        _base_offset_sweep(), None, {},
    )
    assert out["gain_ledger_present"] is False
    assert out["verdicts"]["stv"]["verdict"] == "餓死型或特徵缺失型（無結構層證據）"
    assert any("gain_ledger" in n for n in out["notes"])


def test_gain_ledger_fallback_degrades_same_as_none():
    gl = {"enabled": True, "fallback": True, "per_item": None}
    out = triage(
        _quadrant(_base_quadrant_by_item()), _base_reconciliation(),
        _base_offset_sweep(), gl, {},
    )
    assert out["gain_ledger_present"] is False
    assert out["verdicts"]["stv"]["verdict"] == "餓死型或特徵缺失型（無結構層證據）"


def test_quadrant_stub_empty_verdicts():
    out = triage(
        {"enabled": False}, _base_reconciliation(),
        _base_offset_sweep(), _base_gain_ledger(), {},
    )
    assert out["verdicts"] == {}
    assert any("quadrant" in n for n in out["notes"])


def test_quadrant_none_empty_verdicts():
    out = triage(None, None, None, None, {})
    assert out["verdicts"] == {}
    assert out["enabled"] is True
    assert out["gain_ledger_present"] is False


def test_offset_sweep_stub_reb_starter_none():
    out = triage(
        _quadrant(_base_quadrant_by_item()), _base_reconciliation(),
        {"enabled": False}, _base_gain_ledger(), {},
    )
    reb = out["verdicts"]["reb"]
    assert reb["verdict"] == "水準-指標再平衡型"
    assert reb["starter"] is None
    assert any("offset_sweep" in n for n in reb["notes"])


def test_auc_reason_insufficient_sample_forces_healthy():
    by_item = _base_quadrant_by_item()
    by_item["x"] = {
        "auc": None, "disc_status": "差", "level_status": "正常",
        "gap_vs_global": 0.0, "auc_reason": "樣本不足",
    }
    out = triage(
        _quadrant(by_item), _base_reconciliation(),
        _base_offset_sweep(), _base_gain_ledger(), {},
    )
    vx = out["verdicts"]["x"]
    assert vx["verdict"] == "健康"
    assert any("AUC" in n for n in vx["notes"])


def test_starved_zero_y_rate_starter_none():
    by_item = _base_quadrant_by_item()
    by_item["zero"] = {
        "auc": 0.5, "disc_status": "差", "level_status": "正常",
        "gap_vs_global": 0.0, "auc_reason": None, "y_rate": 0.0,
    }
    gl = _base_gain_ledger()
    gl["per_item"]["zero"] = {"context_gain_share": 0.0}
    out = triage(
        _quadrant(by_item), _base_reconciliation(), _base_offset_sweep(), gl, {}
    )
    vz = out["verdicts"]["zero"]
    assert vz["verdict"] == "餓死型"
    assert vz["starter"] is None
    assert any("y_rate" in n for n in vz["notes"])

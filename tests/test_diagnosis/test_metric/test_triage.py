"""triage：合成診斷 dict（quadrant/offset_sweep/gain_ledger）
→ per-item 判定表（框架 Ch4 判讀流程）。純 dict fixture，不碰 Spark。"""
import pytest

from recsys_tfb.diagnosis.metric.triage import STARTER_CAVEAT, triage


def _quadrant(by_item, enabled=True):
    return {"enabled": enabled, "by_item": by_item}


def _base_quadrant_by_item():
    return {
        "stv": {"auc": 0.5, "disc_status": "差",
                "auc_reason": None, "y_rate": 0.02},
        "feat": {"auc": 0.5, "disc_status": "差",
                 "auc_reason": None, "y_rate": 0.05},
        "ok": {"auc": 0.8, "disc_status": "好",
               "auc_reason": None, "y_rate": 0.4},
    }


def _sweep_entry(dsc, loo=0.0):
    return {"delta_star_centered": dsc, "loo_contribution_holdout": loo}


def _base_offset_sweep():
    return {
        "enabled": True,
        "per_item": {
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
            "stv": {"context_gain_share": 0.02},
            "feat": {"context_gain_share": 0.35},
            "ok": {"context_gain_share": 0.3},
        },
    }


def test_three_verdicts_full_fixture():
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_offset_sweep(),
        _base_gain_ledger(),
        parameters={},
    )
    v = out["verdicts"]
    assert v["stv"]["verdict"] == "餓死型"
    assert v["feat"]["verdict"] == "特徵缺失型"
    assert v["ok"]["verdict"] == "健康"

    assert v["stv"]["lever"].startswith("槓桿3")
    assert v["feat"]["lever"].startswith("槓桿5")
    assert v["ok"]["lever"] == "無（維持觀測）"

    # starter 精確值
    assert v["stv"]["starter"]["type"] == "item_weight"
    assert v["stv"]["starter"]["value"] == pytest.approx(4.47)
    assert v["stv"]["starter"]["caveat"] == STARTER_CAVEAT

    assert v["feat"]["starter"] is None
    assert v["ok"]["starter"] is None

    assert out["gain_ledger_present"] is True
    assert out["thresholds"] == {"starve_ratio": 0.25, "weight_cap": 8.0}
    assert out["summary"] == {"餓死型": 1, "特徵缺失型": 1, "健康": 1}
    assert any("待條件化 SHAP 佐證" in n for n in v["feat"]["notes"])


def test_level_verdicts_and_recon_evidence_fields_are_gone():
    """水準型判定與對帳證據欄已退場——不得再出現在輸出裡。"""
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_offset_sweep(),
        _base_gain_ledger(),
        {},
    )
    assert out["verdicts"], "fixture 應產出至少一個 item 的判定"
    for v in out["verdicts"].values():
        assert v["verdict"] not in ("水準-配置型", "水準-指標再平衡型")
        for gone in ("level_status", "gap_vs_global", "recon_verdict",
                     "theory_min", "theory_max", "residual"):
            assert gone not in v["evidence"]


def test_gain_ledger_none_degrades_stv_to_no_structural_evidence():
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_offset_sweep(), None, {},
    )
    assert out["gain_ledger_present"] is False
    assert out["verdicts"]["stv"]["verdict"] == "餓死型或特徵缺失型（無結構層證據）"
    assert any("gain_ledger" in n for n in out["notes"])


def test_gain_ledger_fallback_degrades_same_as_none():
    gl = {"enabled": True, "fallback": True, "per_item": None}
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_offset_sweep(), gl, {},
    )
    assert out["gain_ledger_present"] is False
    assert out["verdicts"]["stv"]["verdict"] == "餓死型或特徵缺失型（無結構層證據）"


def test_quadrant_stub_empty_verdicts():
    out = triage(
        {"enabled": False},
        _base_offset_sweep(), _base_gain_ledger(), {},
    )
    assert out["verdicts"] == {}
    assert any("quadrant" in n for n in out["notes"])


def test_quadrant_none_empty_verdicts():
    out = triage(None, None, None, {})
    assert out["verdicts"] == {}
    assert out["enabled"] is True
    assert out["gain_ledger_present"] is False


def test_offset_sweep_stub_leaves_top_note():
    """offset_sweep 停用後不再有起手值靠它（槓桿2 已退場），但健康 item 的
    δ* 漂移觀測會從缺——仍要留 top-level note。"""
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        {"enabled": False}, _base_gain_ledger(), {},
    )
    assert any("offset_sweep" in n for n in out["notes"])
    assert out["verdicts"]["ok"]["verdict"] == "健康"


def test_healthy_with_drifting_delta_star_leaves_note():
    """健康判定 + |δ*_centered| >= 0.3 + holdout LOO 為正 → 留早期水準漂移 note。
    這是 offset_sweep 在 triage 裡僅存的用途（槓桿2 退場後）。"""
    sweep = {
        "enabled": True,
        "per_item": {"ok": _sweep_entry(0.5, loo=0.2)},
    }
    out = triage(
        _quadrant({"ok": {"auc": 0.8, "disc_status": "好",
                          "auc_reason": None, "y_rate": 0.4}}),
        sweep, _base_gain_ledger(), {},
    )
    v = out["verdicts"]["ok"]
    assert v["verdict"] == "健康"
    assert any("漂移" in n for n in v["notes"])


def test_auc_reason_insufficient_sample_forces_healthy():
    by_item = _base_quadrant_by_item()
    by_item["x"] = {
        "auc": None, "disc_status": "差", "auc_reason": "樣本不足",
    }
    out = triage(
        _quadrant(by_item),
        _base_offset_sweep(), _base_gain_ledger(), {},
    )
    vx = out["verdicts"]["x"]
    assert vx["verdict"] == "健康"
    assert any("AUC" in n for n in vx["notes"])


def test_starved_zero_y_rate_starter_none():
    by_item = _base_quadrant_by_item()
    by_item["zero"] = {
        "auc": 0.5, "disc_status": "差", "auc_reason": None, "y_rate": 0.0,
    }
    gl = _base_gain_ledger()
    gl["per_item"]["zero"] = {"context_gain_share": 0.0}
    out = triage(
        _quadrant(by_item), _base_offset_sweep(), gl, {}
    )
    vz = out["verdicts"]["zero"]
    assert vz["verdict"] == "餓死型"
    assert vz["starter"] is None
    assert any("y_rate" in n for n in vz["notes"])


def test_disc_unmeasured_leaves_note():
    """opus 總審 nit 1：disc_status「無法評估」（AUC 算不出）要留 note，
    免得只看 verdict 的讀者誤以為判別力已查過沒問題。"""
    by_item = _base_quadrant_by_item()
    by_item["x"] = {
        "auc": None, "disc_status": "無法評估",
        "auc_reason": None, "y_rate": 0.1,
    }
    out = triage(
        _quadrant(by_item),
        _base_offset_sweep(), _base_gain_ledger(), {},
    )
    vx = out["verdicts"]["x"]
    assert vx["verdict"] == "健康"
    assert any("判別力軸" in n and "無法評估" in n for n in vx["notes"])

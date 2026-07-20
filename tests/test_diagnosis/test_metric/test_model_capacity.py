import pytest

from recsys_tfb.diagnosis.metric.model_capacity._compute import compute

LEDGER = {
    "total_gain": 100.0,
    "item_id_gain": 60.0,
    "post_item_context_gain": 30.0,
    "per_item": {"ccard_ins": {"context_gain": 20.0},
                 "fund_bond": {"context_gain": 10.0}},
}
PARAMS = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": True}}}}


def test_gain_shares_sum_to_one():
    out = compute(LEDGER, None, PARAMS)
    s = (out["summary"]["item_id_gain_share"]
         + out["summary"]["context_gain_share"]
         + out["summary"]["unaccounted_gain_share"])
    assert s == pytest.approx(1.0, abs=1e-9)


def test_unaccounted_is_residual_not_assumed_zero():
    out = compute(LEDGER, None, PARAMS)
    assert out["summary"]["unaccounted_gain_share"] == pytest.approx(0.10)


def test_degrades_when_gain_ledger_absent():
    out = compute(None, None, PARAMS)
    assert out["enabled"] is True and out["available"] is False
    assert "gain_ledger" in out["reason"]


def test_degrades_when_gain_ledger_disabled_upstream():
    """訓練側關掉時落地的是 stub，不是 None——兩條路徑都要走到 available=False，
    且 reason 要分辨得出是哪一種（缺檔 vs 上游關掉）。
    """
    out = compute({"enabled": False}, None, PARAMS)
    assert out["available"] is False
    assert out["reason"] != compute(None, None, PARAMS)["reason"]


def test_joins_item_ability_when_present():
    ability = {"per_item": [{"item": "ccard_ins", "query_centered_auc": 0.62}]}
    out = compute(LEDGER, ability, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["query_centered_auc"] == pytest.approx(0.62)


def test_missing_ability_leaves_auc_null_without_raising():
    out = compute(LEDGER, None, PARAMS)
    assert all(r.get("query_centered_auc") is None for r in out["per_item"])


def test_disabled_item_ability_stub_is_treated_as_absent():
    """``item_ability.enabled = false`` 時上游落地的是 ``{"enabled": False}``
    stub，不是 None。少了這條，stub 會走進 ``.get("per_item", [])`` 拿到空
    list 而**靜默**產出一張沒有任何點的散點圖。
    """
    out = compute(LEDGER, {"enabled": False}, PARAMS)
    assert all(r.get("query_centered_auc") is None for r in out["per_item"])
    assert any("item_ability" in n for n in out["notes"])


def test_all_return_paths_share_one_key_set():
    full = compute(LEDGER, {"per_item": []}, PARAMS)
    no_ledger = compute(None, None, PARAMS)
    p = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": False}}}}
    disabled = compute(LEDGER, None, p)
    assert set(full) == set(no_ledger) == set(disabled)

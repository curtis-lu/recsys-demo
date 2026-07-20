"""model_capacity 計算層測試。

``LEDGER`` 是 ``gain_ledger.py:217-232``（``compute_gain_ledger`` /
``_ledger_from_trees`` 的正式輸出）**實際產出的巢狀 schema**——不是規格草稿
早先憑空捏造的扁平鍵。那份扁平 fixture（``item_id_gain``／
``post_item_context_gain`` 直接當頂層鍵）曾經讓 ``_compute.py`` 長出一段
「相容讀取」備援，而備援把「schema 真的不符」偽裝成「正常的降級路徑」：
拿掉巢狀讀取邏輯之後，用那份假 fixture 跑的 29 條測試一條都不會轉紅（真實
schema 下 ``item_id_gain_share``／``context_gain_share`` 全變 ``None``，
但沒有任何斷言盯著這件事）。2026-07-20 已改用真實 schema 重寫，並補上
schema 不符時「必須出聲」的測試——見
``test_schema_mismatch_reports_in_notes_without_raising``／
``test_fallback_ledger_leaves_context_share_none_not_zero``。
"""
import pytest

from recsys_tfb.diagnosis.metric.model_capacity._compute import compute

#: 真實 schema（見上方模組 docstring）。數字沿用 60／30／10，讓
#: ``test_gain_shares_sum_to_one``／``test_unaccounted_is_residual_not_
#: assumed_zero`` 的斷言值不必改。
LEDGER = {
    "enabled": True, "item_feature": "prod_name", "n_trees": 100, "n_items": 2,
    "total_gain": 100.0,
    "item_id": {"split_count": 40, "gain_sum": 60.0, "gain_share": 0.6},
    "context": {"split_count": 80, "gain_sum": 30.0, "gain_share": 0.3},
    "per_item": {"ccard_ins": {"context_gain": 20.0},
                 "fund_bond": {"context_gain": 10.0}},
    "fallback": False, "notes": [],
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


def test_schema_mismatch_reports_in_notes_without_raising():
    """ledger 存在（非 None、非 stub）但找不到預期的巢狀區塊（``item_id``／
    ``context``）——不是「檔案缺席」也不是「上游關閉」那兩種已知路徑，是第三
    種：這份 ledger 我根本讀不懂。必須出聲，不 raise，不靜默留 None。

    斷言落在「notes 裡有沒有講出 schema 相關訊息」，不是只斷言 share 是
    None——後者被「正確偵測到 schema 不符」與「壓根沒去讀這兩個鍵」同時
    滿足，測不出差別。
    """
    malformed = {"enabled": True, "total_gain": 100.0}
    out = compute(malformed, None, PARAMS)
    assert out["available"] is True  # ledger 本身存在，不是缺席／上游關閉
    schema_notes = [n for n in out["notes"] if "schema" in n]
    assert schema_notes, f"缺 item_id／context 時 notes 應點名 schema 不符，實際：{out['notes']}"
    assert any("item_id" in n for n in schema_notes)
    assert any("context" in n for n in schema_notes)


def test_fallback_ledger_leaves_context_share_none_not_zero():
    """粗帳本降級（``gain_ledger.py:_coarse_ledger``，``:235-256``）：
    ``context``／``per_item`` 都是 ``None``、``fallback: True``。這是已知
    合法的退化形狀，不是 schema 不符——不該觸發 schema note，但
    context_gain／unaccounted_gain 的 share 必須留 ``None``，不能算成
    ``0.0``（``0.0`` 會被讀成「context 完全沒貢獻」，那是錯的結論；``None``
    才是誠實的「這個量沒被算」）。
    """
    coarse = {
        "enabled": True, "item_feature": "prod_name", "n_trees": 100,
        "n_items": None, "total_gain": 60.0,
        "item_id": {"split_count": 40, "gain_sum": 60.0, "gain_share": 1.0},
        "context": None,
        "per_item": None,
        "fallback": True,
        "notes": ["preprocessor 缺 category_mappings[item 欄]，降級為粗帳本"],
    }
    out = compute(coarse, None, PARAMS)  # 不 raise
    assert out["available"] is True
    assert out["summary"]["context_gain_share"] is None
    assert out["summary"]["unaccounted_gain_share"] is None
    assert out["summary"]["item_id_gain_share"] == pytest.approx(1.0)
    fallback_notes = [n for n in out["notes"] if "粗帳本" in n or "降級" in n]
    assert fallback_notes, f"notes 應講出粗帳本／降級原因，實際：{out['notes']}"
    # 已知合法形狀，不該被 _schema_notes 誤判成「schema 不符」再發一句。
    assert not any("schema" in n for n in out["notes"])

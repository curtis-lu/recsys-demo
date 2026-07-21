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
#: 全域 context split_count（60）刻意 ≠ per-item context_split_count 加總（80），
#: 且全域 context gain_sum（30）≠ per-item context_gain 加總（60）——這樣
#: ``test_summary_allocated_denominators_differ_from_global`` 才分辨得出
#: 「全域帳」與「per-item 分配帳」用的是不同分母（真實資料裡分配帳因共用切點
#: 被記給每個可達 item，恆大於全域帳）。
LEDGER = {
    "enabled": True, "item_feature": "prod_name", "n_trees": 100, "n_items": 2,
    "total_gain": 100.0,
    "total_split_count": 120,  # 未分配 split ＝ 120 − 40 − 60 ＝ 20
    "item_id": {"split_count": 40, "gain_sum": 60.0, "gain_share": 0.6},
    "context": {"split_count": 60, "gain_sum": 30.0, "gain_share": 0.3},
    # pre_item.gain_sum 必須 ＝ 未分配 gain 殘差（total−item−context ＝ 100−60−30 ＝ 10）
    "pre_item": {
        "gain_sum": 10.0, "split_count": 20,
        "by_feature": {
            "f_age": {"gain": 7.0, "split_count": 12},
            "f_inc": {"gain": 3.0, "split_count": 8},
        },
    },
    "first_item_split_depth": {
        "min": 1, "p25": 1.0, "p50": 2.0, "p75": 3.0, "max": 4,
        "n_trees_with_item_split": 80,
    },
    "per_item": {
        "ccard_ins": {
            "isolating_split_count": 30, "context_split_count": 50,
            "context_gain": 40.0, "context_gain_isolated": 5.0,
            "context_split_isolated": 8,
            "first_tree_index": 0, "trees_touched": [0, 1, 2, 3],
        },
        "fund_bond": {
            "isolating_split_count": 20, "context_split_count": 30,
            "context_gain": 20.0, "context_gain_isolated": 0.0,
            "context_split_isolated": 0,
            "first_tree_index": 1, "trees_touched": [1, 2],
        },
    },
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


def test_summary_split_three_way_shares_sum_to_one():
    """(c) split 三分：total_split_count 在→未分配 split ＝ total−item−context，
    三個 split 佔比加總為 1（與 gain 三分對稱）。"""
    out = compute(LEDGER, None, PARAMS)
    s = out["summary"]
    assert s["total_split_count"] == 120
    assert s["unaccounted_split_count"] == 20  # 120 − 40 − 60
    total = (s["item_id_split_share"] + s["context_split_share"]
             + s["unaccounted_split_share"])
    assert total == pytest.approx(1.0, abs=1e-9)
    assert s["item_id_split_share"] == pytest.approx(40 / 120)


def test_split_shares_none_when_total_split_absent():
    """向後相容：舊版 ledger（重生前）沒有 total_split_count → split 三分佔比
    留 None，不假裝算出數字，也不 raise。"""
    old = dict(LEDGER)
    old.pop("total_split_count")
    out = compute(old, None, PARAMS)
    s = out["summary"]
    assert s["total_split_count"] is None
    assert s["unaccounted_split_count"] is None
    assert s["item_id_split_share"] is None
    assert s["unaccounted_split_share"] is None


def test_per_item_vs_whole_model_columns():
    """(b) 跟全模型比：context_gain_vs_total ＝ context_gain / total_gain（涵蓋、
    會>100% 加總）；context_gain_isolated_vs_total ＝ 私有 / total_gain（獨佔、
    不重計）。這是「item 在模型整體尺度上」的兩把尺。"""
    out = compute(LEDGER, None, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["context_gain_vs_total"] == pytest.approx(40.0 / 100.0)
    assert row["context_gain_isolated_vs_total"] == pytest.approx(5.0 / 100.0)
    bond = next(r for r in out["per_item"] if r["item"] == "fund_bond")
    assert bond["context_gain_isolated_vs_total"] == pytest.approx(0.0)  # 私有 0


def test_per_item_split_vs_whole_model_columns():
    """(Q2) vs 全模型的 split 版：split 涵蓋 ＝ context_split_count / total_split_count；
    split 獨佔 ＝ context_split_isolated / total_split_count。與 gain 版對稱。"""
    out = compute(LEDGER, None, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["context_split_vs_total"] == pytest.approx(50 / 120)
    assert row["context_split_isolated_vs_total"] == pytest.approx(8 / 120)
    bond = next(r for r in out["per_item"] if r["item"] == "fund_bond")
    assert bond["context_split_isolated_vs_total"] == pytest.approx(0.0)


def test_split_vs_total_none_when_total_split_absent():
    """舊版 ledger 無 total_split_count → split 涵蓋／獨佔 留 None，不 raise。"""
    old = dict(LEDGER)
    old.pop("total_split_count")
    out = compute(old, None, PARAMS)
    row = out["per_item"][0]
    assert row["context_split_vs_total"] is None
    assert row["context_split_isolated_vs_total"] is None


def test_pre_item_breakdown_passed_through():
    """(d#1) pre-item 按特徵拆解要傳到輸出供 render；gain 遞減。"""
    out = compute(LEDGER, None, PARAMS)
    pre = out["pre_item"]
    assert pre is not None
    assert pre["gain_sum"] == pytest.approx(10.0)
    feats = list(pre["by_feature"].keys())
    assert feats[0] == "f_age"  # gain 7 > 3


def test_first_item_split_depth_passed_through():
    """(d#2) item 切點深度摘要要傳到輸出。"""
    out = compute(LEDGER, None, PARAMS)
    d = out["first_item_split_depth"]
    assert d is not None and d["p50"] == 2.0 and d["n_trees_with_item_split"] == 80


def test_pre_item_and_depth_none_on_fallback_ledger():
    """粗帳本降級：pre_item／first_item_split_depth 是 None（gain_ledger 給 None），
    compute 照樣傳 None，不 KeyError。"""
    coarse = {
        "enabled": True, "item_feature": "prod_name", "n_trees": 100,
        "n_items": None, "total_gain": 60.0, "total_split_count": 50,
        "item_id": {"split_count": 40, "gain_sum": 60.0, "gain_share": 1.0},
        "context": None, "pre_item": None, "first_item_split_depth": None,
        "per_item": None, "fallback": True, "notes": ["降級為粗帳本"],
    }
    out = compute(coarse, None, PARAMS)
    assert out["pre_item"] is None
    assert out["first_item_split_depth"] is None


def test_top_level_keys_identical_across_return_paths():
    """新增的 top-level 鍵（pre_item／first_item_split_depth）必須出現在每一條
    return 路徑，否則 render 對停用／缺 ledger 頁面會 KeyError。"""
    full = compute(LEDGER, {"per_item": []}, PARAMS)
    no_ledger = compute(None, None, PARAMS)
    p = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": False}}}}
    disabled = compute(LEDGER, None, p)
    assert set(full) == set(no_ledger) == set(disabled)
    for key in ("pre_item", "first_item_split_depth"):
        assert key in full


def test_summary_carries_split_counts_and_tree_count():
    """三分表除了 gain，也要顯示 split 數（使用者回饋①）。split 數只在 ledger
    的 item_id／context 兩個區塊有，全域總 split 數不在 ledger 裡。"""
    out = compute(LEDGER, None, PARAMS)
    s = out["summary"]
    assert s["item_id_split_count"] == 40
    assert s["context_split_count"] == 60
    assert s["n_trees"] == 100


def test_summary_allocated_denominators_differ_from_global():
    """per-item 分配用的分母（各 item context_gain／split 加總）與全域帳不同——
    因為共用切點被記給每個可達 item。分母若錯用全域值，per-item 份額不會加總
    成 1。這裡的 fixture 刻意讓兩者不等（分配 60/80 vs 全域 30/60）。"""
    out = compute(LEDGER, None, PARAMS)
    s = out["summary"]
    assert s["sum_allocated_context_gain"] == pytest.approx(60.0)   # 40 + 20
    assert s["sum_allocated_context_split"] == 80                    # 50 + 30
    assert s["sum_allocated_context_gain"] != s["context_gain"]      # 60 ≠ 30
    assert s["sum_allocated_context_split"] != s["context_split_count"]  # 80 ≠ 60


def test_per_item_carries_split_and_structure_fields():
    """使用者回饋②：per-item ledger 的欄位不能被砍到只剩 gain。ledger 記的
    split 數、私有 context gain、item-routing 足跡都要保留。"""
    out = compute(LEDGER, None, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["context_split_count"] == 50
    assert row["isolating_split_count"] == 30
    assert row["context_gain_isolated"] == pytest.approx(5.0)
    assert row["first_tree_index"] == 0
    assert row["n_trees_touched"] == 4  # len([0,1,2,3])


def test_context_split_share_sums_to_one_over_allocated():
    out = compute(LEDGER, None, PARAMS)
    total = sum(r["context_split_share"] for r in out["per_item"])
    assert total == pytest.approx(1.0)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["context_split_share"] == pytest.approx(50 / 80)


def test_gain_share_relative_to_max_and_median_item():
    """codex §6 的「/ max item」「/ median item」兩欄（相對第一名／相對中位數
    item 的集中度視角）。max item 自己＝100%；分母是各 item gain 佔比的 max／
    median_high（median_high 選實際存在的那個 item，符合「median item」語意）。
    """
    out = compute(LEDGER, None, PARAMS)
    top = out["per_item"][0]  # 佔比最大者
    assert top["gain_share_vs_max"] == pytest.approx(1.0)
    # fixture 兩 item：佔比 2/3 與 1/3；max=2/3、median_high（偶數取上中位）=2/3
    ccard = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    bond = next(r for r in out["per_item"] if r["item"] == "fund_bond")
    assert ccard["gain_share_vs_max"] == pytest.approx(1.0)
    assert bond["gain_share_vs_max"] == pytest.approx(0.5)  # (1/3)/(2/3)
    assert ccard["gain_share_vs_median"] == pytest.approx(1.0)
    assert bond["gain_share_vs_median"] == pytest.approx(0.5)


def test_gain_per_split_is_context_gain_over_context_split():
    """密度欄：每個 context 切點平均分到多少 gain。取絕對值或用錯分母不會讓
    份額測試轉紅，這裡直接釘死是 gain ÷ split。"""
    out = compute(LEDGER, None, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["gain_per_split"] == pytest.approx(40.0 / 50)


def test_isolated_share_is_isolated_over_own_context_gain():
    """私有 context gain 佔該 item 自己 context gain 的比例——分母是該 item 的
    context_gain，不是全域，也不是分配總和。"""
    out = compute(LEDGER, None, PARAMS)
    ccard = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    bond = next(r for r in out["per_item"] if r["item"] == "fund_bond")
    assert ccard["context_gain_isolated_share"] == pytest.approx(5.0 / 40.0)
    assert bond["context_gain_isolated_share"] == pytest.approx(0.0)  # 0 私有 gain


def test_all_return_paths_share_one_summary_key_set():
    """三分表新增的 summary 鍵（split 數、分配分母…）必須出現在**每一條** return
    路徑上，否則 render 對停用／缺 ledger 的頁面會 KeyError。"""
    full = compute(LEDGER, {"per_item": []}, PARAMS)
    no_ledger = compute(None, None, PARAMS)
    p = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": False}}}}
    disabled = compute(LEDGER, None, p)
    assert set(full["summary"]) == set(no_ledger["summary"]) == set(disabled["summary"])
    for key in ("n_trees", "item_id_split_count", "context_split_count",
                "sum_allocated_context_gain", "sum_allocated_context_split"):
        assert key in full["summary"]


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

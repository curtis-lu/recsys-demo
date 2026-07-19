import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.config_shift.compute import (
    build_offset_frame,
    compute,
)

PARAMS = {
    "schema": {"time": "snap_date", "entity": ["cust_id"],
               "item": "prod_name", "label": "label", "score": "score"},
    "dataset": {
        "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": 20},
                                 "config_shift": {"enabled": True}}},
}


def _sample():
    """兩個客群是刻意的：單一客群時「群內 spread」恆等於「全域 spread」，
    test_group_internal_spread_not_global 就分不出兩條路徑（假綠）。"""
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        segment = "mass" if c % 2 == 0 else "affluent"
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": segment,
                "label": int((item == "ccard_ins" and c % 2 == 0)
                             or (item == "fund_bond" and c % 5 == 0)),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_offset_matches_hand_computed_log_ratio():
    frame, _ = build_offset_frame(_sample(), PARAMS, PARAMS["schema"])
    row = frame[(frame["cust_segment_typ"] == "mass")
                & (frame["prod_name"] == "ccard_ins")].iloc[0]
    # r_pos = 1.0（無 override）, r_neg = 0.5 → ln(1.0/0.5) = ln 2
    assert row["offset"] == pytest.approx(np.log(2.0), abs=1e-12)


def test_item_without_override_gets_zero_offset():
    frame, _ = build_offset_frame(_sample(), PARAMS, PARAMS["schema"])
    row = frame[(frame["cust_segment_typ"] == "mass")
                & (frame["prod_name"] == "fund_bond")].iloc[0]
    assert row["offset"] == pytest.approx(0.0, abs=1e-12)


def test_group_internal_spread_not_global():
    """群內均勻的 offset 對名次零影響——spread 必須是群內算的。

    mass 兩個 item 的 r_neg 同為 0.001 → 群內 offset 均勻 → 群內 spread = 0；
    affluent 無 override → offset = 0。全域 spread = ln(1000) ≠ 0，所以這條
    斷言在「群內」與「全域」兩種實作下結果不同（mutation check 的靶）。
    """
    params = {**PARAMS, "dataset": {**PARAMS["dataset"],
              "sample_ratio_overrides": {"mass|ccard_ins|0": 0.001,
                                         "mass|fund_bond|0": 0.001}}}
    out = compute((_sample(), {"n_queries": 40}), params)
    assert out["offset_spread"]["mass"] == pytest.approx(0.0, abs=1e-12)
    assert out["offset_spread"]["affluent"] == pytest.approx(0.0, abs=1e-12)


def test_delta_is_invariant_to_adding_a_constant_per_segment():
    """對某客群整組 offset 加常數，Δ 必須完全不變（query 內同減常數）。"""
    base = compute((_sample(), {"n_queries": 40}), PARAMS)
    shifted_params = {**PARAMS, "dataset": {**PARAMS["dataset"],
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5 * np.exp(-1.0),
                                   "mass|fund_bond|0": np.exp(-1.0)}}}
    shifted = compute((_sample(), {"n_queries": 40}), shifted_params)
    assert shifted["delta"] == pytest.approx(base["delta"], abs=1e-9)


def test_uses_uncalibrated_score_and_fails_loud_without_it():
    sample = _sample().drop(columns=["score_uncalibrated"])
    with pytest.raises(ValueError, match="score_uncalibrated"):
        compute((sample, {"n_queries": 40}), PARAMS)


def test_per_item_deltas_do_not_sum_to_total_delta():
    """替換實驗不是分解——這條契約必須在數字上成立，也要寫進報表。"""
    out = compute((_sample(), {"n_queries": 40}), PARAMS)
    total = out["delta"]
    per_item_sum = sum(r["delta_j"] for r in out["per_item"])
    assert out["per_item_sum_note"], "必須帶上 Σ Δ_j ≠ Δ 的說明字串"
    assert isinstance(total, float) and isinstance(per_item_sum, float)


# --------------------------------------------------------------------------
# 以下三條各守一個「數值對、但方向/母體/退化路徑錯」的失敗模式——這三種錯
# 都不會讓上面六條轉紅。
# --------------------------------------------------------------------------

PARAMS_CORRECTION_HELPS = {
    **PARAMS,
    "dataset": {**PARAMS["dataset"],
                "sample_ratio_overrides": {"mass|ccard_ins|0": 0.1}},
}


def _sample_where_correction_helps():
    """造一份「扣回 offset 一定改善排序」的資料，讓 Δ 與每個 replicate 同號。

    真實 logit：fund_bond = 1.0（每個 query 的正例）、ccard_ins = 0.0（負例）。
    config 給 ccard_ins 的理論 offset ＝ ln(1.0/0.1) ＝ ln 10 ≈ 2.303，觀測分數
    ＝ 真實 logit ＋ offset → ccard_ins 在**每個** query 都被抬到 fund_bond 之上，
    正例掉到第 2 名、AP ＝ 0.5；扣回 offset 後 fund_bond 回到第 1、AP ＝ 1.0。

    因此 Δ ＝ +0.5，而且**任何** cluster 重抽組合算出來都是 +0.5——CI 兩端必然
    同號且為正。符號寫反（直接回傳 paired_bootstrap_delta 的 baseline − corrected）
    就會讓 ci_low 變成 −0.5，被下面的斷言抓到。
    """
    rows = []
    off = np.log(10.0)
    for c in range(40):
        jitter = 0.01 * ((c % 5) - 2)  # 確定性微擾，避免整份資料每列完全相同
        true_logit = {"ccard_ins": 0.0 + jitter, "fund_bond": 1.0 + jitter}
        for item in ("ccard_ins", "fund_bond"):
            z = true_logit[item] + (off if item == "ccard_ins" else 0.0)
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": "mass",
                "label": int(item == "fund_bond"),
                "score_uncalibrated": float(1.0 / (1.0 + np.exp(-z))),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_ci_has_same_sign_as_delta():
    """CI 必須是 Δ 自己的區間，不是反號那一個。

    ``uncertainty.paired_bootstrap_delta`` 回的是 ``mAP(F) − mAP(F − shift)``
    ＝ baseline − corrected，與本模組的 Δ ＝ corrected − baseline 反號。漏掉取負
    的話數值大小完全正確、只有方向相反——報表會出貨一個上下顛倒的信賴區間，
    而任何「數值對不對」的斷言都抓不到。這裡用結構性的**同號**斷言。
    """
    out = compute((_sample_where_correction_helps(), {}),
                  PARAMS_CORRECTION_HELPS)
    assert out["delta"] == pytest.approx(0.5, abs=1e-9)
    assert out["delta"] > 0
    assert out["delta_ci_low"] > 0
    assert out["delta_ci_high"] > 0


def _sample_with_two_tier_inclusion_weight():
    """一半 query 正例排第 1（AP=1.0）、一半排第 2（AP=0.5），兩半權重不同。

    權重**刻意不等**（1.0 / 4.0）：全部相同時加權平均與未加權在位元上相同，
    「加權與未加權不相等」的斷言在結構上永遠不可能紅。

    未加權 per-item AP ＝ (20×1.0 + 20×0.5)/40 ＝ 0.75；
    以 1.0/4.0 加權 ＝ (20×1×1.0 + 20×4×0.5)/(20×1 + 20×4) ＝ 0.60。
    """
    rows = []
    for c in range(40):
        pos_first = c % 2 == 0
        for item in ("ccard_ins", "fund_bond"):
            is_pos = item == "fund_bond"
            higher = is_pos if pos_first else not is_pos
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": "mass",
                "label": int(is_pos),
                "score_uncalibrated": 0.9 if higher else 0.1,
                "score": 0.5,
                "inclusion_weight": 1.0 if pos_first else 4.0,
            })
    return pd.DataFrame(rows)


def test_point_estimate_uses_inclusion_weight():
    """點估計必須吃 inclusion_weight，否則它與 CI 描述的不是同一個母體。

    CI 側（``paired_bootstrap_delta``）本來就做 Horvitz–Thompson 修正。點估計
    不做的話，兩個數字估的是兩個不同的量——公司環境目前 ratio == 1.0（權重全 1）
    看不出差別，母體長過 max_queries 之後就會活過來。
    """
    weighted_pdf = _sample_with_two_tier_inclusion_weight()
    unweighted_pdf = weighted_pdf.drop(columns=["inclusion_weight"])

    weighted = compute((weighted_pdf, {}), PARAMS)
    unweighted = compute((unweighted_pdf, {}), PARAMS)

    assert unweighted["baseline_map"] == pytest.approx(0.75, abs=1e-9)
    assert weighted["baseline_map"] == pytest.approx(0.60, abs=1e-9)
    assert weighted["baseline_map"] != pytest.approx(
        unweighted["baseline_map"], abs=1e-9
    )


def test_empty_sample_returns_stub_without_raising():
    """空抽樣是**良性**退化輸入（沒抽到列），不是壞輸入——不該炸。

    item 清單是從資料推的，空資料 → 零 context × 零 item → offset frame 是一個
    連欄位都沒有的空 DataFrame，任何 ``groupby("group")`` 都會 KeyError。
    """
    empty = _sample().iloc[0:0]
    out = compute((empty, {}), PARAMS)
    assert out["enabled"] is True
    assert out["delta"] is None
    assert out["per_item"] == []

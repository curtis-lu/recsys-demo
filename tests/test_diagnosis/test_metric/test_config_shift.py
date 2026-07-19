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

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.config_shift._compute import (
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


PARAMS_COUPLED = {
    **PARAMS,
    "dataset": {**PARAMS["dataset"],
                "sample_ratio_overrides": {"mass|a|0": 0.5, "mass|b|0": 0.25}},
    "evaluation": {"diagnosis": {"ci": {"enabled": False},
                                 "config_shift": {"enabled": True}}},
}


def _sample_with_coupled_items():
    """三個 item、其中兩個有**互不相同的非零** offset，正例輪流分佈在三者身上。

    現行 ``PARAMS`` 造不出這條契約：它只有一個 item 有非零 offset，實測
    ``delta`` 與 ``Σ Δ_j`` 恰好相等（−0.0125），任何「兩者不相等」的斷言都會
    立刻紅。要讓名次真的互相耦合，至少要兩個 item 各自把別人擠開。
    offset：a = ln 2 ≈ 0.693、b = ln 4 ≈ 1.386、c = 0。
    """
    rng = np.random.default_rng(7)
    rows = []
    for c in range(60):
        for item in ("a", "b", "c"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": "mass",
                "label": int("abc"[c % 3] == item),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_per_item_deltas_do_not_sum_to_total_delta():
    """Σ Δ_j ≠ Δ：逐項替換是 M 次獨立介入，不是把 Δ 拆成 M 份。

    一個 item 的分數變動會改變**同一 query 內所有 item 的名次**，各項效果透過
    名次互相耦合，沒有守恆律可言。把 Δ_j 照比例縮放成一個「加起來剛好等於 Δ」
    的偽分解，會讓讀者以為可以逐項歸因——這條就是擋那個。
    """
    out = compute((_sample_with_coupled_items(), {}), PARAMS_COUPLED)
    total = out["delta"]
    per_item_sum = sum(r["delta_j"] for r in out["per_item"])
    assert abs(per_item_sum - total) > 1e-6, (
        f"Σ Δ_j ({per_item_sum}) 與 Δ ({total}) 不該相等——相等代表逐項結果被"
        f"當成可加分解"
    )


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


# --------------------------------------------------------------------------
# context 欄不保證是 entity 級 —— 「一個 query 落在單一 context group 內」
# 這個前提不成立時，offset_spread 會漏報真實抵達排序的偏移。
# --------------------------------------------------------------------------

PARAMS_ITEM_LEVEL_CONTEXT = {
    "schema": {},
    "dataset": {
        "sample_group_keys": ["prod_tier", "label"],
        "sample_ratio": 1.0,
        # 兩層各自內部比例一致 → 每個 context group 內 offset 均勻 → 群內
        # spread 全為 0，但 hi 與 lo 之間差 ln(1/0.1)，而**同一個 query 同時
        # 含有兩層的 item**，所以這個差真的會改動名次。
        "sample_ratio_overrides": {"hi|0": 0.1, "lo|0": 1.0},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": False},
                                 "config_shift": {"enabled": True}}},
}


def _sample_with_item_level_context():
    """context 欄 ``prod_tier`` 是 **item 級**屬性：每個 query 內都有兩種值。"""
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        for item, tier in (("a", "hi"), ("b", "hi"), ("c", "lo"), ("d", "lo")):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "prod_tier": tier,
                "label": int(item == "c"),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_item_level_context_offset_spread_underreports_and_is_flagged():
    """群內 spread 全 0，但逐 query 的實際 offset 範圍非 0，且 notes 要講明。

    這是計畫原稿寫錯的地方：它把「群內同加常數 → Δ 不變」當成無條件成立的
    不變量。只有當 context 欄在每個 query 內為常數（entity 級屬性）才成立；
    context 取自產品層級這種 item 級屬性時，一個 query 內同時存在多個 context
    group，group 間的 offset 差**確實**會改動名次。
    """
    out = compute((_sample_with_item_level_context(), {}),
                  PARAMS_ITEM_LEVEL_CONTEXT)

    # config 視角：每個 group 內部完全均勻，看不出任何偏移。
    assert out["offset_spread"] == {"hi": pytest.approx(0.0, abs=1e-12),
                                    "lo": pytest.approx(0.0, abs=1e-12)}
    # 實際抵達排序的視角：每個 query 內都橫跨兩層，範圍 = ln(1/0.1) = ln 10。
    assert out["query_offset_spread"]["p50"] == pytest.approx(np.log(10.0),
                                                              abs=1e-9)
    assert out["query_offset_spread"]["max"] > 0
    # 而且 Δ 明確非 0——「處處無偏移」是錯的。
    assert abs(out["delta"]) > 1e-6
    assert any("query_offset_spread" in n for n in out["notes"]), out["notes"]


# --------------------------------------------------------------------------
# offset 查表零命中必須看得見：Δ ≈ 0 是本模組宣稱「可排除這個方向」的訊號，
# 靜默歸零會讓讀者排除掉真正的原因。
# --------------------------------------------------------------------------

PARAMS_INT_CONTEXT = {
    "schema": {},
    "dataset": {
        "sample_group_keys": ["seg", "prod_name", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"3|a|0": 0.2},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": False},
                                 "config_shift": {"enabled": True}}},
}


def _sample_with_int_context(dtype):
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        for item in ("a", "b"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "seg": 3,
                "label": int(item == "a"),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    pdf = pd.DataFrame(rows)
    pdf["seg"] = pdf["seg"].astype(dtype)
    return pdf


def test_dtype_mismatch_zero_hit_override_is_reported_not_silent():
    """int64 → float64 讓 key 由 '3|a|0' 變 '3.0|a|0'，offset 靜默歸零。

    這個 dtype 變化不是假想：Spark 的整數欄只要含任一 NULL，``toPandas()``
    後就**必然**是 float64。實測同資料同 config，只換 dtype →
    offset_spread 由 1.609 掉到 0.0、delta 由 −0.200 掉到 0.0、notes 全空。
    零命中不該 raise（config 有一條用不到的 override 是合法的），但必須看得見。
    """
    hit = compute((_sample_with_int_context("int64"), {}), PARAMS_INT_CONTEXT)
    miss = compute((_sample_with_int_context("float64"), {}), PARAMS_INT_CONTEXT)

    # 命中的那一側：override 生效，沒有零命中回報。
    assert hit["offset_spread"]["3"] == pytest.approx(np.log(1 / 0.2), abs=1e-9)
    assert hit["unmatched_override_keys"] == []

    # 未命中的那一側：offset 歸零，但**必須**留下痕跡。
    assert miss["offset_spread"]["3.0"] == pytest.approx(0.0, abs=1e-12)
    assert miss["delta"] == pytest.approx(0.0, abs=1e-12)
    assert miss["unmatched_override_keys"] == [
        {"config": "dataset.sample_ratio_overrides", "key": "3|a|0"}
    ]
    assert any("3|a|0" in n for n in miss["notes"]), miss["notes"]


# --------------------------------------------------------------------------
# 三條 return 路徑的 key set 必須一致，否則 render 會在最少被跑到的路徑上炸。
# --------------------------------------------------------------------------

def test_declared_items_missing_from_sample_are_reported():
    """宣告了卻沒被抽到的 item 不能無聲消失。

    offset 矩陣只枚舉觀測到的 (context, item) 組合，所以這種 item 會整列不見。
    報表上「少一列」與「這個 item 沒有偏移」長得一模一樣，而讀者會把沉默讀成
    沒問題——跟零命中的 override key 是同一種病。
    """
    params = {**PARAMS, "schema": {"categorical_values": {
        "prod_name": ["ccard_ins", "fund_bond", "never_sampled"]}}}
    out = compute((_sample(), {}), params)

    assert out["items"] == ["ccard_ins", "fund_bond"]
    assert out["items_declared_not_observed"] == ["never_sampled"]
    assert any("never_sampled" in n for n in out["notes"]), out["notes"]

    # 沒宣告 categorical_values 時差集恆為空，且**不該**為此發 note——那是正常
    # 情形，為正常情形發 note 會讓 notes 變成雜訊、真正的觀測被淹掉。
    plain = compute((_sample(), {}), PARAMS)
    assert plain["items_declared_not_observed"] == []
    assert not any("categorical_values" in n for n in plain["notes"])


def test_all_return_paths_share_the_same_key_set():
    """停用／空樣本／完整三條路徑的頂層 key set 必須完全相同。

    render 會直接讀 ``data["items"]`` 這類鍵。若某條 return 少了一個鍵，錯誤
    只會在「診斷被停用」或「抽樣為空」這兩條最少被跑到的路徑上出現——正是最
    晚被發現的那種 KeyError。
    """
    disabled_params = {
        **PARAMS,
        "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": 20},
                                     "config_shift": {"enabled": False}}},
    }
    disabled = compute((_sample(), {"n_queries": 40}), disabled_params)
    empty = compute((_sample().iloc[0:0], {}), PARAMS)
    full = compute((_sample(), {"n_queries": 40}), PARAMS)

    assert set(disabled) == set(full), set(disabled) ^ set(full)
    assert set(empty) == set(full), set(empty) ^ set(full)

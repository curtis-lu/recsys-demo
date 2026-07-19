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
    assert out["offset_spread_by_context"]["mass"] == pytest.approx(0.0, abs=1e-12)
    assert out["offset_spread_by_context"]["affluent"] == pytest.approx(0.0, abs=1e-12)


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
        # 三層各自內部比例一致 → 每個 context group 內 offset 均勻 → 群內
        # spread 全為 0，但層與層之間有差，而**同一個 query 同時含有多層的
        # item**，所以這個差真的會改動名次。
        # offset：hi = ln(1/0.1) = ln 10、mid = ln(1/0.5) = ln 2、lo = 0。
        "sample_ratio_overrides": {"hi|0": 0.1, "mid|0": 0.5, "lo|0": 1.0},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": False},
                                 "config_shift": {"enabled": True}}},
}

_TIER_OF = {"h1": "hi", "m1": "mid", "l1": "lo", "l2": "lo"}


def _sample_with_item_level_context(hi_query_weight=1.0):
    """context 欄 ``prod_tier`` 是 **item 級**屬性，且**不同 query 的 offset
    範圍不同**。

    後者是刻意的：若每個 query 的候選組合都一樣，逐 query 的 spread 會全部相等
    而**恰好等於全域 max − min**，「逐 query」與「全域」兩種實作在這份資料上位元
    相同，斷言在結構上不可能紅（本 repo 反覆踩到的假綠形態）。

    三種 query 各 20 個，spread 三個不同的值：
      (l1, l2) → 同屬 lo，spread 0
      (l1, m1) → lo vs mid，spread ln 2  ≈ 0.6931
      (l1, h1) → lo vs hi， spread ln 10 ≈ 2.3026
    ``hi_query_weight`` 只給第三種 query，用來驗分位數有沒有吃 inclusion_weight。
    """
    rng = np.random.default_rng(0)
    plan = [(("l1", "l2"), 1.0), (("l1", "m1"), 1.0),
            (("l1", "h1"), hi_query_weight)]
    rows = []
    qid = 0
    for candidates, weight in plan:
        for _ in range(20):
            qid += 1
            for k, item in enumerate(candidates):
                rows.append({
                    "snap_date": "2026-01-31", "cust_id": f"c{qid}",
                    "prod_name": item, "prod_tier": _TIER_OF[item],
                    "label": int(k == 1),
                    "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                    "score": 0.5,
                    "inclusion_weight": weight,
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
    assert out["offset_spread_by_context"] == {
        "hi": pytest.approx(0.0, abs=1e-12),
        "mid": pytest.approx(0.0, abs=1e-12),
        "lo": pytest.approx(0.0, abs=1e-12),
    }
    # 而且 Δ 明確非 0——「處處無偏移」是錯的。
    assert abs(out["delta"]) > 1e-6
    assert any("query_offset_spread" in n for n in out["notes"]), out["notes"]


def test_query_offset_spread_is_per_query_not_global():
    """分位數必須來自逐 query 的分布，不是一個全域 max − min。

    全域實作會讓三個分位數塌成同一個值（全域範圍 = ln 10）。這裡的資料刻意讓
    三種 query 有三個不同的 spread，分位數才有分辨力。
    """
    out = compute((_sample_with_item_level_context(), {}),
                  PARAMS_ITEM_LEVEL_CONTEXT)
    q = out["query_offset_spread"]

    assert q["max"] == pytest.approx(np.log(10.0), abs=1e-9)
    assert q["p50"] == pytest.approx(np.log(2.0), abs=1e-9)
    assert q["p90"] == pytest.approx(np.log(10.0), abs=1e-9)
    # 全域實作下三者相等 —— 這兩條就是分辨力本身。
    assert q["p50"] != pytest.approx(q["max"], abs=1e-9)
    assert q["p90"] > q["p50"]
    assert q["n_queries"] == 60
    assert q["n_queries_multi_candidate"] == 60


def test_query_offset_spread_respects_inclusion_weight():
    """分位數要吃 inclusion_weight，否則講的是樣本而不是母體。

    delta／CI／n_pos_effective 全部走 HT 加權，分位數不走的話，分層抽樣一啟用
    （``max_queries`` 觸發時就會）這個號稱「真正抵達排序的偏移」就跟其他數字
    描述的不是同一個母體。
    """
    plain = compute((_sample_with_item_level_context(1.0), {}),
                    PARAMS_ITEM_LEVEL_CONTEXT)
    upweighted = compute((_sample_with_item_level_context(20.0), {}),
                         PARAMS_ITEM_LEVEL_CONTEXT)

    # 把 spread 最大的那 20 個 query 的權重放大 20 倍 → 中位數必須往上移。
    assert plain["query_offset_spread"]["p50"] == pytest.approx(np.log(2.0),
                                                                abs=1e-9)
    assert upweighted["query_offset_spread"]["p50"] == pytest.approx(
        np.log(10.0), abs=1e-9)
    assert (upweighted["query_offset_spread"]["mean"]
            > plain["query_offset_spread"]["mean"])


def test_offset_matrix_has_no_cartesian_product_cells():
    """矩陣只列實際觀測到的 (context, item)，不補不存在的格子。

    笛卡兒積會虛報「hi 層 × 只存在於 lo 層的產品」這種不存在的偏移，而且把它
    算進該 group 的中位數，污染 offset_centered。
    """
    out = compute((_sample_with_item_level_context(), {}),
                  PARAMS_ITEM_LEVEL_CONTEXT)
    assert set(out["offset_matrix"]["hi"]) == {"h1"}
    assert set(out["offset_matrix"]["mid"]) == {"m1"}
    assert set(out["offset_matrix"]["lo"]) == {"l1", "l2"}


def test_divergence_between_two_spread_views_is_reported_numerically():
    """兩個視角不一致時要靠**數值對帳**講出來，不是靠結構推斷。

    結構檢查（context 欄在 query 內是否非常數）得先猜對「這次會不會分歧」，
    而它猜錯過：context 欄含 NULL 時 ``nunique()`` 預設吃掉 NaN，於是分歧最大
    的那一次正好不觸發。數值比較沒有這個破口。
    """
    out = compute((_sample_with_item_level_context(), {}),
                  PARAMS_ITEM_LEVEL_CONTEXT)
    assert max(out["offset_spread_by_context"].values()) == pytest.approx(
        0.0, abs=1e-12)
    assert out["query_offset_spread"]["max"] > 0
    assert any("兩個 spread 視角不一致" in n for n in out["notes"]), out["notes"]


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
    assert hit["offset_spread_by_context"]["3"] == pytest.approx(np.log(1 / 0.2), abs=1e-9)
    assert hit["unmatched_override_keys"] == []

    # 未命中的那一側：offset 歸零，但**必須**留下痕跡。
    assert miss["offset_spread_by_context"]["3.0"] == pytest.approx(0.0, abs=1e-12)
    assert miss["delta"] == pytest.approx(0.0, abs=1e-12)
    assert miss["unmatched_override_keys"] == [
        {"config": "dataset.sample_ratio_overrides", "key": "3|a|0"}
    ]
    assert any("3|a|0" in n for n in miss["notes"]), miss["notes"]


# --------------------------------------------------------------------------
# 三條 return 路徑的 key set 必須一致，否則 render 會在最少被跑到的路徑上炸。
# --------------------------------------------------------------------------

PARAMS_NULL_CONTEXT = {
    "schema": {},
    "dataset": {
        "sample_group_keys": ["prod_tier", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"hi|0": 0.1, "nan|0": 0.01},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": False},
                                 "config_shift": {"enabled": True}}},
}


def _sample_with_null_context():
    """context 欄含 NULL，而且在 query 內非常數。

    Spark 的整數欄只要含任一 NULL，``toPandas()`` 之後必然是 float64 帶 NaN
    ——這不是假想情境，跟 dtype 那個坑同源。offset 查表的 key 走 ``str(nan)``
    ＝ ``"nan"``（dataset pipeline 實際組 key 的方式），所以 override 是命中的，
    零命中儀器不會報，只有 NULL group 本身會從矩陣裡消失。
    """
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        for item, tier in (("a", "hi"), ("b", float("nan"))):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "prod_tier": tier,
                "label": int(item == "a"),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def test_null_context_group_survives_and_is_visible():
    """NULL context group 不能從 offset 矩陣裡消失。

    pandas 的 ``groupby`` / ``nunique`` 預設 ``dropna=True`` 會把整個 NULL group
    丟掉，而 ``row_offsets`` 照樣把那些列的 offset 算進 delta 與
    query_offset_spread。結果：讀者看到一個非零的偏移量，卻在矩陣裡找不到任何
    group 能解釋它——而且零命中儀器不會報（key 其實命中了）。
    """
    out = compute((_sample_with_null_context(), {}), PARAMS_NULL_CONTEXT)

    # NULL group 必須在，且用明確標籤而不是印成 'nan'。
    assert set(out["offset_matrix"]) == {"hi", "<NULL>"}
    assert out["offset_matrix"]["<NULL>"]["b"] == pytest.approx(
        np.log(100.0), abs=1e-9)
    assert out["offset_matrix"]["hi"]["a"] == pytest.approx(
        np.log(10.0), abs=1e-9)
    # 零命中儀器在這裡**不**會報（key 命中了），所以它擋不住這個洞。
    assert out["unmatched_override_keys"] == []
    # 而 query 內橫跨 hi 與 NULL 兩群，結構檢查必須認得 NaN 也是一種值。
    assert any("prod_tier" in n for n in out["notes"]), out["notes"]


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


def test_per_item_reports_both_raw_and_effective_positive_counts():
    """n_pos_effective 必須是 HT 加權計數，不能退化成原始列數。

    mAP 與 min_positives／shrinkage_k／weight_alpha 吃的是加權計數
    （``metrics.py`` 的 weights 路徑）。只報原始列數，讀者就會拿一個母體去篩
    另一個母體算出來的數字——與先前修掉的「點估計與 CI 估不同的量」同一種病。
    """
    out = compute((_sample_with_two_tier_inclusion_weight(), {}), PARAMS)
    by_item = {r["item"]: r for r in out["per_item"]}
    fund = by_item["fund_bond"]

    # fund_bond 在 40 個 query 各有 1 個正例列；一半權重 1.0、一半 4.0。
    assert fund["n_pos_raw"] == 40
    assert fund["n_pos_effective"] == pytest.approx(20 * 1.0 + 20 * 4.0)
    assert fund["n_pos_effective"] != pytest.approx(float(fund["n_pos_raw"]))
    assert out["sample"]["n_positive_rows_effective"] == pytest.approx(100.0)


def _sample_with_unbounded_scores():
    """lambdarank 那類 pairwise/listwise objective 的原始分數：無界、不在 (0,1)。"""
    pdf = _sample()
    pdf["score_uncalibrated"] = np.linspace(-3.5, 5.25, len(pdf))
    return pdf


def test_non_probabilistic_scores_flag_the_objective_assumption():
    """分數不在 (0,1) 時要點名 Δ 的推導前提可能不成立。

    offset = ln(r_pos/r_neg) 是 **log-odds 上**的加性常數，只對 pointwise 機率型
    objective 成立。``training.objective`` 允許 lambdarank，此時 score 是無界原始
    分數，相減沒有理論基礎。這裡刻意**不** fail-loud——offset 矩陣與兩個 spread
    是純 config 算術、與 objective 無關，砍掉整項診斷是過度反應。
    """
    out = compute((_sample_with_unbounded_scores(), {}), PARAMS)

    assert any("pointwise" in n for n in out["notes"]), out["notes"]
    # 診斷本身仍然跑完，config 算術那半仍有值。
    assert out["delta"] is not None
    assert out["offset_spread_by_context"]["mass"] == pytest.approx(
        np.log(2.0), abs=1e-9)


def test_ratio_overrides_ignored_when_label_absent_from_group_keys():
    """label 不在 group keys 時，override 一條都不會生效——這件事要說出來。

    數學上正確（正負例同比例下採樣不改變 log-odds），但輸出上「config 真的沒
    影響」與「我漏寫 label」完全一樣：都是全 0、無 note。這裡不列舉 key（那會
    誤報成零命中），只陳述事實。
    """
    params = {**PARAMS, "dataset": {
        "sample_group_keys": ["cust_segment_typ", "prod_name"],  # 漏了 label
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5, "mass|fund_bond|0": 0.25},
    }}
    out = compute((_sample(), {}), params)

    assert out["delta"] == pytest.approx(0.0, abs=1e-12)
    assert out["unmatched_override_keys"] == []   # 不誤報成零命中
    assert any("sample_group_keys" in n for n in out["notes"]), out["notes"]


def test_field_notes_document_the_non_obvious_fields():
    """JSON 要自我說明：哪個計數是加權的、分位數有沒有插值。"""
    out = compute((_sample(), {"n_queries": 40}), PARAMS)
    for key in ("offset_spread_by_context", "query_offset_spread",
                "n_pos_raw", "n_pos_effective"):
        assert key in out["field_notes"] and out["field_notes"][key]


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

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.item_ability._compute import (
    compute, descending_ranks, presort_by_score, weighted_auc_presorted,
)


def test_descending_ranks_are_one_based_highest_score_first():
    """名次 1 給同一 query 內分數最高的列；跨 query 各自從 1 起算。"""
    groups = np.array([0, 0, 0, 1, 1])
    score = np.array([0.1, 0.9, 0.5, 0.2, 0.8])
    # q0: 0.9→1, 0.5→2, 0.1→3 ; q1: 0.8→1, 0.2→2
    np.testing.assert_array_equal(
        descending_ranks(groups, score), np.array([3.0, 1.0, 2.0, 2.0, 1.0])
    )


def test_per_item_reports_ranks_not_percentiles():
    """名次欄是原始名次（≥1 的整數空間），不是 rank÷query_size 的百分位。

    合成樣本每 query 只有 2 個候選，名次只可能是 1 或 2——若實作退回百分位
    （0.5／1.0）這條會紅。
    """
    out = compute((_sample(), {"n_queries": 40}), _params(n_boot=0))
    item = out["per_item"][0]
    assert set(item["positive_ranks"]) <= {1, 2}
    assert item["median_positive_rank"] in (1.0, 1.5, 2.0)
    for key in ("p10_positive_rank", "p25_positive_rank",
                "median_positive_rank", "p75_positive_rank", "p90_positive_rank"):
        assert item[key] is not None and item[key] >= 1.0


def test_candidates_per_query_reports_query_size():
    """名次的分母（每 query 幾個候選）要交代清楚。合成樣本固定 2 個候選。"""
    out = compute((_sample(), {"n_queries": 40}), _params(n_boot=0))
    cpq = out["candidates_per_query"]
    assert cpq == {"min": 2, "median": 2.0, "max": 2}


def test_weighted_auc_matches_hand_computed_value():
    # 分數 [3,1,2]，label [1,0,1]：唯一的負例分數 1 排最後
    # → 兩個正例都贏過它 → AUC = 1.0
    score = np.array([3.0, 1.0, 2.0])
    order, tie_starts = presort_by_score(score)
    labels = np.array([1, 0, 1])[order]
    weights = np.ones(3)
    assert weighted_auc_presorted(labels, weights, tie_starts) == pytest.approx(1.0)


def test_weighted_auc_handles_ties_with_half_credit():
    """同分給 0.5 分。tie_starts 是**必要**參數——只給 label 與 weight
    無從分辨「同分」與「正例贏」，兩者的 AUC 分別是 0.5 與 1.0。
    """
    score = np.array([1.0, 1.0])
    order, tie_starts = presort_by_score(score)
    labels = np.array([1, 0])[order]
    assert weighted_auc_presorted(labels, np.ones(2), tie_starts) == pytest.approx(0.5)


def test_ties_and_distinct_scores_differ():
    """反向釘住上一條：把同分拆開，同一組 label 的 AUC 必須改變。
    少了這條，一個忽略 tie_starts 的實作也能讓上面兩條同時綠
    （只要它剛好回 0.5 …… 不會，但斷言之間互相印證比較穩）。
    """
    labels = np.array([1, 0])
    tied_order, tied_starts = presort_by_score(np.array([1.0, 1.0]))
    dist_order, dist_starts = presort_by_score(np.array([2.0, 1.0]))
    tied = weighted_auc_presorted(labels[tied_order], np.ones(2), tied_starts)
    dist = weighted_auc_presorted(labels[dist_order], np.ones(2), dist_starts)
    assert tied != dist


def _weighted_auc_reference(labels, weights, tie_starts):
    """逐同分組的參考實作（＝向量化前的原版邏輯），只給等價性測試用。"""
    n = len(labels)
    if n == 0:
        return None
    yy = np.asarray(labels, np.int64)
    w = np.asarray(weights, np.float64)
    pos_total = float(w[yy == 1].sum())
    neg_total = float(w[yy == 0].sum())
    if pos_total <= 0 or neg_total <= 0:
        return None
    numer = 0.0
    neg_before = 0.0
    for i in range(len(tie_starts) - 1):
        s, e = int(tie_starts[i]), int(tie_starts[i + 1])
        pos_w = float(w[s:e][yy[s:e] == 1].sum())
        neg_w = float(w[s:e][yy[s:e] == 0].sum())
        numer += pos_w * (neg_before + 0.5 * neg_w)
        neg_before += neg_w
    return float(numer / (pos_total * neg_total))


def test_weighted_auc_vectorized_matches_reference():
    """效能修正（reduceat 向量化）不得改變輸出：對隨機資料（含大量同分與加權）
    與逐同分組的參考實作一致到浮點精度。破壞向量化（例如 neg_before 忘了做成
    獨佔前綴和）這條會紅。"""
    rng = np.random.default_rng(7)
    for trial in range(30):
        n = int(rng.integers(1, 400))
        # 整數分數製造大量同分組；連續分數則 tie group ≈ n
        score = rng.integers(0, max(2, n // 3), size=n).astype(float)
        labels = (rng.random(n) < 0.4).astype(np.int64)
        w = rng.uniform(0.1, 5.0, size=n)
        order, ts = presort_by_score(score)
        yy, ww = labels[order], w[order]
        ref = _weighted_auc_reference(yy, ww, ts)
        vec = weighted_auc_presorted(yy, ww, ts)
        if ref is None:
            assert vec is None
        else:
            assert vec == pytest.approx(ref, abs=1e-12), f"trial {trial}: {vec} != {ref}"


def test_bootstrap_sorts_once_per_item_regardless_of_n_boot(monkeypatch):
    """效能契約：排序次數 ＝ item 數，與 n_boot 無關。

    腳本原版每次 weighted_auc 呼叫都重排（N_items × (n_boot+2) 次排序）。

    這裡數的是本模組自己的 presort_by_score，不是 np.argsort——np.argsort
    連 pandas 內部的排序都會數進來，得到的數字既不穩定也指不出是誰在排。
    斷言用「等於 item 數」而不只是「與 n_boot 無關」：後者對一個
    「每個 item 排兩次」的實作照樣成立。
    """
    import recsys_tfb.diagnosis.metric.item_ability._compute as m

    calls = {"n": 0}
    real = m.presort_by_score
    monkeypatch.setattr(
        m, "presort_by_score",
        lambda *a, **k: (calls.__setitem__("n", calls["n"] + 1), real(*a, **k))[1],
    )

    sample = _sample()
    n_items = sample["prod_name"].nunique()
    compute((sample, {"n_queries": 40}), _params(n_boot=5))
    few = calls["n"]
    calls["n"] = 0
    compute((sample, {"n_queries": 40}), _params(n_boot=200))
    many = calls["n"]
    assert few == many, f"排序次數隨 n_boot 增長：{few} → {many}"
    # raw 與 query-centered 各排一次 → 每個 item 兩次
    assert many == 2 * n_items, f"預期 {2 * n_items} 次排序，實得 {many}"


def test_reports_both_raw_and_query_centered_auc():
    out = compute((_sample(), {"n_queries": 40}), _params())
    item = out["per_item"][0]
    assert "raw_within_item_auc" in item
    assert "query_centered_auc" in item
    assert "auc_gap_raw_minus_centered" in item


def test_auc_gap_is_raw_minus_centered_not_absolute():
    """方向釘死。取絕對值或反號都不會讓任何數值測試轉紅——散點圖偏離
    對角線的**方向**是這項診斷的全部意義，反了就讀反了。
    """
    out = compute((_sample(), {"n_queries": 40}), _params())
    for r in out["per_item"]:
        if r["raw_within_item_auc"] is None or r["query_centered_auc"] is None:
            continue
        assert r["auc_gap_raw_minus_centered"] == pytest.approx(
            r["raw_within_item_auc"] - r["query_centered_auc"]
        )


def test_inclusion_weight_changes_the_auc():
    """HT 權重必須真的餵進 AUC。把某一層的權重從 1 改成 8，AUC 應該改變；
    若實作忘了乘權重，兩次結果會完全相同。
    """
    base = _sample()
    base["stratum"] = "take_all"
    base["inclusion_weight"] = 1.0
    heavy = base.copy()
    heavy.loc[heavy["cust_id"] < "c20", "inclusion_weight"] = 8.0
    a = compute((base, {"n_queries": 40}), _params(n_boot=0))
    b = compute((heavy, {"n_queries": 40}), _params(n_boot=0))
    assert a["per_item"][0]["raw_within_item_auc"] != \
        b["per_item"][0]["raw_within_item_auc"]


def test_requires_uncalibrated_score():
    sample = _sample().drop(columns=["score_uncalibrated"])
    with pytest.raises(ValueError, match="score_uncalibrated"):
        compute((sample, {"n_queries": 40}), _params())


def test_disabled_returns_stub_with_same_key_set():
    """三條 return 路徑（停用／空樣本／完整）key set 必須相同——
    照抄 config_shift 的契約（見 config_shift/_compute.py::compute docstring）。
    """
    full = compute((_sample(), {"n_queries": 40}), _params())
    p = _params()
    p["evaluation"]["diagnosis"]["item_ability"]["enabled"] = False
    stub = compute((_sample(), {"n_queries": 40}), p)
    empty = compute((_sample().iloc[0:0], {"n_queries": 0}), _params())
    assert set(stub) == set(full) == set(empty)
    assert stub["enabled"] is False


def _params(n_boot=20):
    return {
        "schema": {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name", "label": "label", "score": "score"},
        "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": n_boot},
                                     "sample": {"seed": 42},
                                     "item_ability": {"enabled": True, "top_n": 30}}},
    }


def _sample():
    rng = np.random.default_rng(1)
    rows = []
    for c in range(40):
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c:02d}",
                "prod_name": item,
                "label": int(rng.random() < 0.3),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
                "stratum": "take_all",
                "inclusion_weight": 1.0,
            })
    return pd.DataFrame(rows)

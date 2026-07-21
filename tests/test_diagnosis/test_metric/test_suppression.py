import time

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.suppression._compute import (
    compute, cross_purchase_stats,
)


def _params(top_examples=50):
    return {
        "schema": {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name", "label": "label", "score": "score"},
        "evaluation": {"diagnosis": {
            "sample": {"seed": 42},
            "suppression": {"enabled": True, "top_examples": top_examples},
        }},
    }


def _row(cust, item, label, score):
    return {"snap_date": "2026-01-31", "cust_id": cust, "prod_name": item,
            "label": label, "score_uncalibrated": score, "score": 0.5,
            "stratum": "take_all", "inclusion_weight": 1.0}


def _two_query_sample():
    """兩個 query（c1、c2），各自都是 A（負例、分數較高）排在 B（正例、分數
    較低）之前 → (B, A) 這一格在兩個 query 都貢獻分攤，保證非零；兩個 query
    也讓等價比對／軸序測試有重複 pair 可測。"""
    return pd.DataFrame([
        _row("c1", "A", 0, 0.9), _row("c1", "B", 1, 0.4),
        _row("c2", "A", 0, 0.8), _row("c2", "B", 1, 0.3),
    ])


def _sample_with_suppression_and_cross_purchase():
    """``_two_query_sample`` 的壓制情境（c1／c2：A 壓制 B）之上，加一個 A、B
    都是正例的第三個 query（c3）。

    ``cross_purchase_stats`` 只看 label=1 的列；``_two_query_sample`` 裡 A
    永遠是負例，兩個 item 裡只有 B 曾經是正例——``item_units`` 天生只會有
    一個 key，湊不出 ``item_j != item_k`` 的交叉購買列（實測：直接對
    ``_two_query_sample`` 跑 ``compute``，``cross_purchase`` 恆為 ``[]``）。
    c3 讓 A、B 都進入正例集合，同時 A／B 仍然是 ``axis_order`` 裡的一對
    （來自 c1／c2 的壓制關係）——這樣『compute 有沒有把資料接上』與『同軸
    限制』才有東西可測。
    """
    rows = _two_query_sample().to_dict("records")
    rows.append(_row("c3", "A", 1, 0.5))
    rows.append(_row("c3", "B", 1, 0.5))
    return pd.DataFrame(rows)


def _cross_sample():
    """A 是熱門 item（20 個 query 單位中 16 個買）、B 是小眾（僅 3 個買）。
    只用來驗 cross_purchase_stats 回傳的列帶有 lift／n_joint／n_j／n_k／
    p_k_given_j 這些欄位——不驗數值，數值層面的驗證在 _independent_sample／
    _coupled_sample。每個 query 單位對 A、B 都各給一列（label 依是否購買），
    讓分母 n_units 精確等於 20。"""
    rows = []
    for i in range(20):
        cust = f"e{i:02d}"
        rows.append(_row(cust, "A", int(i < 16), 0.8))
        rows.append(_row(cust, "B", int(i < 3), 0.6))
    return pd.DataFrame(rows)


def _independent_sample():
    """X 與 Y 的購買在 query 單位上各自獨立抽出（同一個 rng 依序呼叫兩次
    ``random``，兩次抽樣互不相關）。query 單位數 2000（single snap_date 下
    等於 entity 數）遠超建議下限 400，讓有限樣本誤差落在 abs=0.15 容差內、
    不會 flaky。每個 query 單位對 X、Y 都各給一列，分母 n_units 精確等於
    2000。"""
    rng = np.random.default_rng(123)
    n = 2000
    buys_x = rng.random(n) < 0.3
    buys_y = rng.random(n) < 0.4
    rows = []
    for i in range(n):
        cust = f"e{i:05d}"
        rows.append(_row(cust, "X", int(buys_x[i]), 0.5))
        rows.append(_row(cust, "Y", int(buys_y[i]), 0.5))
    return pd.DataFrame(rows)


def _coupled_sample():
    """P 與 Q 幾乎總是一起買：買 P 的 query 單位有 90% 也買 Q，不買 P 的只有
    10% 也買 Q → Q 對 P 的依賴造成 lift 明顯 > 1（理論值 ≈ 0.9/0.42 ≈ 2.14）。
    每個 query 單位對 P、Q 都各給一列（label 依是否購買），讓分母 n_units
    精確等於 1000（single snap_date 下等於 entity 數）。"""
    rng = np.random.default_rng(7)
    n = 1000
    rows = []
    for i in range(n):
        cust = f"e{i:04d}"
        buys_p = rng.random() < 0.4
        buys_q = (rng.random() < 0.9) if buys_p else (rng.random() < 0.1)
        rows.append(_row(cust, "P", int(buys_p), 0.8))
        rows.append(_row(cust, "Q", int(buys_q), 0.8))
    return pd.DataFrame(rows)


def _many_query_sample(n_queries=30, n_items=5, seed=11):
    """>=30 個 query（每個 query＝一個 cust_id）、item 數>=5，同一組
    (positive_item, suppressor_item) 在很多 query 裡重複出現——向量化散射
    累加最容易錯在『重複鍵被覆蓋而不是累加』，這正是這個 fixture 要撞到的
    地方。每個 query 保證至少一個正例，避免整條 query 被『沒有正例』跳過而
    對等價比對沒有貢獻。"""
    rng = np.random.default_rng(seed)
    items = [f"i{k}" for k in range(n_items)]
    rows = []
    for q in range(n_queries):
        cust = f"c{q:03d}"
        scores = rng.uniform(0.05, 0.95, size=n_items)
        labels = (rng.random(n_items) < 0.4).astype(int)
        if labels.sum() == 0:
            labels[rng.integers(0, n_items)] = 1
        for item, score, label in zip(items, scores, labels):
            rows.append(_row(cust, item, int(label), float(score)))
    return pd.DataFrame(rows)


def _scale_sample(n_queries: int, n_items: int, seed: int = 99) -> pd.DataFrame:
    """規模計時用：n_queries × n_items 列、label 隨機（p=0.4）。用來製造
    足夠的 (positive, suppressor) 成對數以撞到 100k+ 的 n_misordered_pairs
    門檻，向量化建構（不逐列 append dict）保持 fixture 本身夠快。"""
    rng = np.random.default_rng(seed)
    n = n_queries * n_items
    cust = np.repeat([f"c{q:05d}" for q in range(n_queries)], n_items)
    item = np.tile([f"i{k:02d}" for k in range(n_items)], n_queries)
    label = (rng.random(n) < 0.4).astype(int)
    score = rng.uniform(0.05, 0.95, size=n)
    return pd.DataFrame({
        "snap_date": "2026-01-31",
        "cust_id": cust,
        "prod_name": item,
        "label": label,
        "score_uncalibrated": score,
        "score": 0.5,
        "stratum": "take_all",
        "inclusion_weight": 1.0,
    })


def _reference_pair_ledger(sample: pd.DataFrame, params: dict) -> dict:
    """依 ``scripts/suppression_ledger_diagnosis.py:452-574`` 逐位元組抄的
    雙層 Python 迴圈版（外層 query、次層正例列、內層負例列），只算
    allocated_ap_gap／affected_positive_rows／mean_score_margin 三個值，用來
    對照向量化版本 ``compute`` 的正確性。刻意不重用 ``_compute.py`` 的任何
    聚合邏輯——共用了就測不出兩者是否一致。
    """
    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.diagnosis.metric._common import metric_params, query_key, to_logit

    schema = get_schema(params)
    query_cols = [schema["time"], *schema["entity"]]
    mp = metric_params(params)

    groups = pd.factorize(query_key(sample, query_cols))[0]
    items = sample[schema["item"]].astype(str).to_numpy()
    y = sample[schema["label"]].to_numpy(dtype=np.int64)
    z, _ = to_logit(sample["score_uncalibrated"].to_numpy(dtype=np.float64))

    sort_idx = np.lexsort((-z, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y[sort_idx].astype(np.float64)
    item_sorted = items[sort_idx]
    z_sorted = z[sort_idx]
    boundaries = np.concatenate([
        [0], np.flatnonzero(np.diff(g_sorted)) + 1, [len(g_sorted)],
    ])

    pair_stats: dict[tuple[str, str], dict] = {}
    for qi in range(len(boundaries) - 1):
        s, e = boundaries[qi], boundaries[qi + 1]
        local_len = e - s
        yq = y_sorted[s:e]
        if yq.sum() == 0:
            continue
        ranks = np.arange(1, local_len + 1, dtype=np.float64)
        k_eff = float(mp["k"]) if mp["k"] is not None else float(local_len)
        cum = np.cumsum(yq)
        contrib = np.where(ranks <= k_eff, cum / ranks, 0.0)
        pos_recip_prefix = np.cumsum(
            np.where((yq == 1) & (ranks <= k_eff), 1.0 / ranks, 0.0)
        )
        pos_positions = np.flatnonzero(yq == 1)
        neg_positions = np.flatnonzero(yq == 0)

        for b in pos_positions:
            pos_orig = int(sort_idx[s + b])
            positive_item = str(item_sorted[s + b])
            above = neg_positions[neg_positions < b]
            if len(above) == 0:
                continue
            a_rank = above + 1.0
            new_contrib = np.where(a_rank <= k_eff, (cum[above] + 1.0) / a_rank, 0.0)
            intermediate_pos_gain = pos_recip_prefix[b - 1] - pos_recip_prefix[above]
            raw_severity = new_contrib - contrib[b] + intermediate_pos_gain
            raw_total_for_row = float(raw_severity.sum())
            row_ap_gap = max(0.0, 1.0 - float(contrib[b]))
            allocated_gap = (
                raw_severity / raw_total_for_row * row_ap_gap
                if raw_total_for_row > 0.0 and row_ap_gap > 0.0
                else np.zeros_like(raw_severity)
            )
            for a, gap_d in zip(above, allocated_gap):
                suppressor_item = str(item_sorted[s + a])
                score_margin = float(z_sorted[s + a] - z_sorted[s + b])
                key = (positive_item, suppressor_item)
                pstat = pair_stats.setdefault(key, {
                    "affected_positive_rows": set(),
                    "allocated_ap_gap": 0.0,
                    "_score_margins": [],
                })
                pstat["affected_positive_rows"].add(pos_orig)
                pstat["allocated_ap_gap"] += float(gap_d)
                pstat["_score_margins"].append(score_margin)

    return {
        key: {
            "allocated_ap_gap": v["allocated_ap_gap"],
            "affected_positive_rows": len(v["affected_positive_rows"]),
            "mean_score_margin": float(np.mean(v["_score_margins"])),
        }
        for key, v in pair_stats.items()
    }


def test_counts_negatives_ranked_above_each_positive():
    # 一個 query：A=0.9(label 0) 排在 B=0.5(label 1) 之前 → B 被 A 壓制一次
    sample = pd.DataFrame([_row("c1", "A", 0, 0.9), _row("c1", "B", 1, 0.5)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_suppressed_positive_rows"] == 1
    assert out["mean_negatives_above_positive"] == pytest.approx(1.0)


def test_no_suppression_when_positive_ranks_first():
    """反向釘住上一條。少了它，一個「把每個正例都算成被壓制一次」的
    實作也能讓上面那條綠。"""
    sample = pd.DataFrame([_row("c1", "A", 0, 0.5), _row("c1", "B", 1, 0.9)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_suppressed_positive_rows"] == 0
    assert out["n_misordered_pairs"] == 0


def test_pair_ledger_attributes_gap_to_the_suppressor():
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    pair = next(p for p in out["pair_ledger"]
                if p["positive_item"] == "B" and p["suppressor_item"] == "A")
    assert pair["allocated_ap_gap"] > 0


def test_allocated_gap_sums_to_the_row_level_ap_gap():
    """會計恆等式：分攤是把單列的 AP 缺口切開，切完要等於原本那塊。

    ⚠ 等號兩邊必須來自**獨立算出來的量**：左邊是分攤結果的加總，右邊是
    逐列累加的 row_ap_gap 本身。舊版拿同一張成對表的三種分組互比，
    分組不同不代表來源獨立，那個等式對任何 gap 值恆成立（實測：把分攤
    比例的分母換成常數 1.0，測試照樣綠）。
    """
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    allocated = sum(p["allocated_ap_gap"] for p in out["pair_ledger"])
    assert out["total_row_ap_gap_allocated"] > 0, \
        "fixture 沒有製造出任何分攤，這條測試會退化成 0 == 0"
    assert allocated == pytest.approx(out["total_row_ap_gap_allocated"])


def test_axis_order_is_sorted_and_shared():
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    assert out["axis_order"] == sorted(out["axis_order"])
    assert set(out["axis_order"]) >= {"A", "B"}


def test_cross_purchase_reports_lift_not_only_conditional_probability():
    """熱門 item 對任何 j 的 P(k|j) 都高——只給條件機率會退化成
    『熱門那行整片亮』，那張圖畫的是熱門度不是關聯。"""
    stats = cross_purchase_stats(_cross_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "B" and r["item_k"] == "A")
    assert {"lift", "n_joint", "n_j", "n_k", "p_k_given_j"} <= set(row)


def test_cross_purchase_lift_is_about_one_for_independent_items():
    stats = cross_purchase_stats(_independent_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "X" and r["item_k"] == "Y")
    assert row["lift"] == pytest.approx(1.0, abs=0.15)


def test_cross_purchase_lift_exceeds_one_for_items_bought_together():
    """反向釘住上一條：構造真的相關的一對，lift 必須明顯 > 1。
    只驗『獨立時 ≈ 1』的話，一個恆回 1.0 的實作也會綠。"""
    stats = cross_purchase_stats(_coupled_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "P" and r["item_k"] == "Q")
    assert row["lift"] > 1.5


def test_compute_output_carries_the_cross_purchase_data_not_just_its_field_notes():
    """單元測試驗『cross_purchase_stats 算得對不對』，這條驗『它有沒有進產物』。

    上一輪四條單元測試全綠，而 compute 根本沒呼叫它——所有測試都直接
    呼叫該函式，於是「算得對」全覆蓋、「有進產物」零覆蓋。
    """
    out = compute(
        (_sample_with_suppression_and_cross_purchase(), {"n_queries": 3}), _params())
    assert out["cross_purchase"], "compute 沒有把 cross_purchase_stats 的結果放進輸出"
    assert out["n_units"] > 0
    row = out["cross_purchase"][0]
    assert {"item_j", "item_k", "n_joint", "n_j", "n_k", "p_k_given_j", "lift"} <= set(row)


def test_cross_purchase_is_restricted_to_the_shared_axis():
    """兩張圖同軸序是並排對照的技術前提；多出來的格子對不上。"""
    out = compute(
        (_sample_with_suppression_and_cross_purchase(), {"n_queries": 3}), _params())
    axis = set(out["axis_order"])
    assert out["cross_purchase"], "fixture 沒有製造出任何交叉購買列可驗"
    for r in out["cross_purchase"]:
        assert r["item_j"] in axis and r["item_k"] in axis


def test_empty_sample_returns_stub_without_raising():
    """良性退化輸入：沒有任何正例列。不得 raise，也不得回一個
    看起來像『算過了而且是零』的結果——n_positive_rows 必須是 0。"""
    sample = pd.DataFrame([_row("c1", "A", 0, 0.9), _row("c1", "B", 0, 0.5)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_positive_rows"] == 0
    assert out["pair_ledger"] == []
    assert out["axis_order"] == []


def test_vectorised_allocation_matches_the_slow_reference():
    """向量化散射累加最容易錯在『重複鍵被覆蓋而不是累加』——同一個
    (受害 item, 壓制者 item) 會在很多不同 query 反覆出現，那正是這個
    bug 會顯現的地方。所以 fixture 必須是多 query、多重複 pair 的。
    """
    sample = _many_query_sample()      # 30 個 query，item 數 5，pair 大量重複
    fast = compute((sample, {"n_queries": 30}), _params())["pair_ledger"]
    slow = _reference_pair_ledger(sample, _params())
    assert len(fast) == len(slow)
    fast_by_key = {(r["positive_item"], r["suppressor_item"]): r for r in fast}
    for key, ref in slow.items():
        got = fast_by_key[key]
        assert got["allocated_ap_gap"] == pytest.approx(ref["allocated_ap_gap"])
        assert got["affected_positive_rows"] == ref["affected_positive_rows"]
        assert got["mean_score_margin"] == pytest.approx(ref["mean_score_margin"])


def test_scales_to_a_realistic_pair_count():
    """效能契約。腳本原版的內層逐 pair 迴圈在這個規模要數十秒；
    向量化後應在數秒內。門檻設得很鬆（30s）是刻意的——這條要抓的是
    『退回逐 pair 迴圈』這種量級差，不是機器快慢。
    """
    sample = _scale_sample(n_queries=3000, n_items=20)   # ≈ 6 萬列
    t0 = time.monotonic()
    out = compute((sample, {"n_queries": 3000}), _params())
    elapsed = time.monotonic() - t0
    assert out["n_misordered_pairs"] > 100_000, "fixture 沒有製造出足夠的成對數"
    assert elapsed < 30.0, f"耗時 {elapsed:.1f}s"

"""within_item_auc：per-item ROC-AUC（midrank rank-sum，無 UDF）。

關鍵 fixture＝全平手 item 恰得 0.5——框架的核心診斷對象正是近常數分數的
冷門 item；min-rank 直接代入 rank-sum 公式會在這裡系統性偏差。
"""

import numpy as np
import pytest

from recsys_tfb.diagnosis.metric.discrimination import within_item_auc


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
    }


def _df(spark, rows):
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def _numpy_midrank_auc(scores, labels):
    """sklearn-free 手算參考實作（midrank）。"""
    scores = np.asarray(scores, dtype=float)
    labels = np.asarray(labels)
    order = np.argsort(scores, kind="mergesort")
    s_sorted = scores[order]
    ranks = np.empty(len(scores), dtype=float)
    i = 0
    while i < len(s_sorted):
        j = i
        while j + 1 < len(s_sorted) and s_sorted[j + 1] == s_sorted[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    n_pos = int(labels.sum())
    n_neg = len(labels) - n_pos
    r_pos = ranks[labels == 1].sum()
    return (r_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def test_perfect_separation_gives_one(spark):
    rows = [
        ("20240331", "C0", "A", 0.1, 0), ("20240331", "C1", "A", 0.2, 0),
        ("20240331", "C2", "A", 0.8, 1), ("20240331", "C3", "A", 0.9, 1),
    ]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(1.0)
    assert out["A"]["n_pos"] == 2 and out["A"]["n_neg"] == 2


def test_all_ties_constant_score_gives_exactly_half(spark):
    # 常數分數＝條件判別力為零的極端；rank-sum＋midrank 下 AUC 恰為 0.5。
    rows = [("20240331", f"C{i}", "A", 0.7, int(i < 3)) for i in range(10)]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(0.5)


def test_partial_ties_match_midrank_semantics(spark):
    # scores [1,1,2,2]、labels [0,1,0,1]：midrank 1.5,1.5,3.5,3.5
    # R⁺ = 1.5+3.5 = 5 → AUC = (5 − 2·3/2) / (2·2) = 0.5
    rows = [
        ("20240331", "C0", "A", 1.0, 0), ("20240331", "C1", "A", 1.0, 1),
        ("20240331", "C2", "A", 2.0, 0), ("20240331", "C3", "A", 2.0, 1),
    ]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(0.5)


def test_single_class_item_is_none_with_reason(spark):
    rows = [("20240331", "C0", "A", 0.5, 1), ("20240331", "C1", "A", 0.6, 1)]
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] is None and out["A"]["reason"]


def test_level_shift_immunity_across_items(spark):
    # 兩個 item 內部排序型態相同、只差整體常數 +5 → AUC 相同。
    # （within-item AUC 從不跨 item 比較，per-item 常數偏移整個被消掉。）
    rows = []
    for i, (s, y) in enumerate([(0.1, 0), (0.4, 1), (0.2, 0), (0.6, 1)]):
        rows.append(("20240331", f"C{i}", "A", s, y))
        rows.append(("20240331", f"C{i}", "B", s + 5.0, y))
    out = within_item_auc(_df(spark, rows), _params())
    assert out["A"]["auc"] == pytest.approx(out["B"]["auc"])


def test_numpy_parity_on_random_data(spark):
    rng = np.random.default_rng(42)
    rows = []
    for item in ("A", "B"):
        n = 60
        scores = np.round(rng.random(n), 1)  # 一位小數 → 大量平手
        labels = (rng.random(n) < 0.3).astype(int)
        if labels.sum() == 0:
            labels[0] = 1
        if labels.sum() == n:
            labels[0] = 0
        for i in range(n):
            rows.append(("20240331", f"C{item}{i}", item,
                         float(scores[i]), int(labels[i])))
    out = within_item_auc(_df(spark, rows), _params())
    for item in ("A", "B"):
        sub = [(r[3], r[4]) for r in rows if r[2] == item]
        expected = _numpy_midrank_auc([s for s, _ in sub], [y for _, y in sub])
        assert out[item]["auc"] == pytest.approx(expected)

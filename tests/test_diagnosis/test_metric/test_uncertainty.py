"""bootstrap_per_item_ci：cluster bootstrap 的決定性、退化案例、覆蓋性質。

下半部（``Test*Paired*`` / ``test_paired_*``）測 ``paired_bootstrap_delta``：
分層配對 cluster bootstrap——層內重抽、同一組重抽樣本上算兩個 mAP 再取差。
"""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import (
    bootstrap_per_item_ci,
    paired_bootstrap_delta,
)


def _params(n_boot=50, k=None, metric_extra=None, seed=42):
    metric = {"weight_alpha": 0.0, "k": k, "min_positives": 0, "shrinkage_k": 0}
    if metric_extra:
        metric.update(metric_extra)
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {
            "metric": metric,
            "diagnosis": {
                "sample": {"max_queries": 1000,
                           "min_pos_queries_per_item": 1, "seed": seed},
                "ci": {"enabled": True, "n_boot": n_boot},
            },
        },
    }


def _pdf(rows):
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


THREE_CUST = [
    # A：C0 rank1（contrib 1.0）、C1 rank2（0.5）→ AP 0.75, n_pos=2
    # B：C2 rank1（1.0）→ AP 1.0, n_pos=1；等權 macro = 0.875
    ("20240331", "C0", "A", 0.9, 1),
    ("20240331", "C0", "B", 0.1, 0),
    ("20240331", "C1", "A", 0.1, 1),
    ("20240331", "C1", "B", 0.9, 0),
    ("20240331", "C2", "A", 0.1, 0),
    ("20240331", "C2", "B", 0.9, 1),
]


def test_point_estimates_match_metric_family():
    out = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out["per_item"]["A"]["ap"] == pytest.approx(0.75)
    assert out["per_item"]["A"]["n_pos"] == 2
    assert out["per_item"]["B"]["ap"] == pytest.approx(1.0)
    assert out["macro"]["ap"] == pytest.approx(0.875)
    assert out["n_boot"] == 50


def test_ci_brackets_point_and_is_deterministic():
    out1 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    out2 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out1 == out2
    m = out1["macro"]
    assert m["ci_low"] <= m["ap"] <= m["ci_high"]
    a = out1["per_item"]["A"]
    assert a["ci_low"] <= a["ap"] <= a["ci_high"]


def test_single_cluster_degenerates_to_zero_width():
    rows = [
        ("20240331", "C0", "A", 0.9, 1),
        ("20240331", "C0", "B", 0.1, 0),
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params())
    a = out["per_item"]["A"]
    assert a["ap"] == a["ci_low"] == a["ci_high"] == pytest.approx(1.0)


def test_k_truncation_zeroes_deep_positive():
    rows = [
        ("20240331", "C0", "A", 0.9, 0),
        ("20240331", "C0", "B", 0.1, 1),   # rank 2；k=1 → contrib 0
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params(k=1))
    assert out["per_item"]["B"]["ap"] == pytest.approx(0.0)
    assert out["k"] == 1


def test_metric_params_flow_into_macro():
    out = bootstrap_per_item_ci(
        _pdf(THREE_CUST), _params(metric_extra={"weight_alpha": 1.0})
    )
    assert out["macro"]["ap"] == pytest.approx(5 / 6)


# ---------------------------------------------------------------------------
# paired_bootstrap_delta
# ---------------------------------------------------------------------------

_ITEMS = ["A", "B", "C", "D"]
_DATES = ["20240331", "20240630"]


def _metric_kwargs():
    return {"k": None, "weight_alpha": 0.0, "min_positives": 0,
            "shrinkage_k": 0.0}


def _tiny_shift():
    """只動一個 item 的分數——足以改動部分 query 的名次，但兩個 mAP 仍高度相關。"""
    return {"A": 0.15}


def _base_rows(n_clusters=40, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for c in range(n_clusters):
        cid = f"C{c:03d}"
        for d in _DATES:
            scores = rng.random(len(_ITEMS))
            pos = int(rng.integers(0, len(_ITEMS)))  # 每個 query 恰一個正例
            for j, item in enumerate(_ITEMS):
                rows.append({
                    "group": f"{d}|{cid}",
                    "cluster": cid,
                    "item": item,
                    "label": 1 if j == pos else 0,
                    "score": float(scores[j]),
                })
    return pd.DataFrame(rows)


def _frame(n_clusters=40, seed=0):
    """單層、無 inclusion_weight 欄——即舊呼叫端的形狀（向後相容路徑）。"""
    return _base_rows(n_clusters, seed)


def _stratum_of(cluster_id: str) -> str:
    # 每 4 個 cluster 有 1 個在 take_all → 兩層大小刻意不等（10 / 30），
    # 全體一起重抽時各層總量幾乎必然偏離，讓層內重抽的 mutation 測得到。
    return "take_all" if int(cluster_id[1:]) % 4 == 0 else "hash_ratio"


def _frame_two_strata(n_clusters=40, seed=0, hash_weight=4.0):
    frame = _base_rows(n_clusters, seed)
    frame["stratum"] = frame["cluster"].map(_stratum_of)
    frame["inclusion_weight"] = np.where(
        frame["stratum"] == "take_all", 1.0, hash_weight
    )
    return frame


def _stratum_sizes(n_clusters=40):
    sizes = {}
    for c in range(n_clusters):
        s = _stratum_of(f"C{c:03d}")
        sizes[s] = sizes.get(s, 0) + 1
    return sizes


def _bootstrap_macro_series(frame, shift, n_boot, seed):
    """對照組：一份 naive 的 cluster bootstrap，只算單一個 mAP 的分佈。

    刻意不分層、也不與另一邊配對——這正是 ``paired_bootstrap_delta`` 要取代
    的寫法，用來證明「分開各自 bootstrap 再相減」的 CI 會寬得多。
    """
    from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

    groups = pd.factorize(frame["group"])[0]
    clusters = pd.factorize(frame["cluster"])[0]
    items = frame["item"].to_numpy()
    y = frame["label"].to_numpy()
    off = frame["item"].map(shift).fillna(0.0).to_numpy(dtype=float)
    score = frame["score"].to_numpy(dtype=float) - off
    base_w = (
        frame["inclusion_weight"].to_numpy(dtype=float)
        if "inclusion_weight" in frame.columns
        else np.ones(len(frame))
    )
    n_clusters = int(clusters.max()) + 1
    rng = np.random.default_rng(seed)
    out = np.empty(n_boot, dtype=float)
    for b in range(n_boot):
        draw = rng.integers(0, n_clusters, n_clusters)
        mult = np.bincount(draw, minlength=n_clusters).astype(float)
        w = base_w * mult[clusters]
        out[b] = compute_macro_per_item_map(
            groups, items, y, score, weights=w, **_metric_kwargs()
        )
    return out


def _independent_delta_ci(frame, n_boot, seed):
    """兩個 mAP 各自獨立 bootstrap 再相減——配對版要顯著比它窄。"""
    base = _bootstrap_macro_series(frame, {}, n_boot, seed)
    shifted = _bootstrap_macro_series(frame, _tiny_shift(), n_boot, seed + 1000)
    d = base - shifted
    return float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5))


def test_paired_delta_ci_is_narrower_than_independent_cis():
    """配對的必要性：兩個 mAP 高度相關，分開算 CI 再相減會寬到測不到。"""
    lo, hi = paired_bootstrap_delta(
        _frame(), _metric_kwargs(), shift=_tiny_shift(), n_boot=200, seed=1)
    ind_lo, ind_hi = _independent_delta_ci(_frame(), n_boot=200, seed=1)
    assert (hi - lo) < (ind_hi - ind_lo), (
        f"paired width={hi - lo:.6f} independent width={ind_hi - ind_lo:.6f}"
    )


def test_paired_resampling_stays_within_strata():
    """分層設計下，重抽必須在層內進行——跨層重抽會扭曲層的相對比重。"""
    drawn = []
    paired_bootstrap_delta(
        _frame_two_strata(), _metric_kwargs(), shift=_tiny_shift(),
        n_boot=20, seed=1, _record_draws=drawn)
    assert len(drawn) == 20
    for replicate in drawn:
        assert set(replicate) == set(_stratum_sizes())
        for stratum, n in replicate.items():
            assert n == _stratum_sizes()[stratum], \
                f"層 {stratum} 重抽後大小改變（{n} != {_stratum_sizes()[stratum]}）"


def test_paired_cluster_spanning_two_strata_keeps_sizes():
    """cluster 橫跨兩層（同客戶不同期落在不同層）時，層大小仍須不變。

    重抽單位是 ``(stratum, cluster)`` 而非 cluster——否則橫跨的 cluster 要
    被指派到哪一層沒有定義，指派錯就會把該層的 cluster 數改掉。
    """
    frame = _base_rows(n_clusters=12, seed=3)
    # 依日期分層 → 每個 cluster 兩層都出現
    frame["stratum"] = np.where(
        frame["group"].str.startswith(_DATES[0]), "take_all", "hash_ratio")
    frame["inclusion_weight"] = 1.0
    drawn = []
    paired_bootstrap_delta(
        frame, _metric_kwargs(), shift=_tiny_shift(),
        n_boot=10, seed=2, _record_draws=drawn)
    for replicate in drawn:
        assert replicate == {"take_all": 12, "hash_ratio": 12}


def test_paired_zero_shift_gives_ci_containing_zero():
    lo, hi = paired_bootstrap_delta(
        _frame(), _metric_kwargs(), shift={}, n_boot=200, seed=1)
    assert lo <= 0.0 <= hi


def test_paired_deterministic_given_seed():
    a = paired_bootstrap_delta(_frame(), _metric_kwargs(), shift=_tiny_shift(),
                               n_boot=50, seed=7)
    b = paired_bootstrap_delta(_frame(), _metric_kwargs(), shift=_tiny_shift(),
                               n_boot=50, seed=7)
    assert a == b


def test_paired_frame_without_strata_columns_is_accepted():
    """舊呼叫端（沒有 stratum / inclusion_weight 欄）視為單層、全 1 權重。"""
    frame = _frame()
    assert "stratum" not in frame.columns
    assert "inclusion_weight" not in frame.columns
    drawn = []
    lo, hi = paired_bootstrap_delta(
        frame, _metric_kwargs(), shift=_tiny_shift(), n_boot=20, seed=1,
        _record_draws=drawn)
    assert np.isfinite(lo) and np.isfinite(hi) and lo <= hi
    for replicate in drawn:
        assert list(replicate.values()) == [40]  # 單層，全部 40 個 cluster


def test_paired_inclusion_weight_changes_the_estimate():
    """權重必須真的進到計算——同 seed 下抽樣結果相同，差異只可能來自權重。"""
    a = paired_bootstrap_delta(
        _frame_two_strata(hash_weight=4.0), _metric_kwargs(),
        shift=_tiny_shift(), n_boot=100, seed=5)
    b = paired_bootstrap_delta(
        _frame_two_strata(hash_weight=25.0), _metric_kwargs(),
        shift=_tiny_shift(), n_boot=100, seed=5)
    assert a != b, f"inclusion_weight 被吞掉了：{a} == {b}"


def test_paired_aggregation_matches_compute_macro_per_item_map():
    """內部的加權聚合必須與 evaluation.metrics 的公開實作等價。

    ``paired_bootstrap_delta`` 為了效能把 contributions 提到迴圈外、自行做
    bincount 聚合（沿用 ``bootstrap_per_item_ci`` 的骨架），這條測試把它釘
    在 ``compute_macro_per_item_map`` 上，避免兩份公式日後漂移。
    """
    from recsys_tfb.diagnosis.metric.uncertainty import _weighted_macro
    from recsys_tfb.evaluation.metrics import (
        compute_macro_per_item_map,
        positive_row_contributions,
    )

    frame = _frame(n_clusters=15, seed=11)
    groups = pd.factorize(frame["group"])[0]
    items = frame["item"].to_numpy()
    y = frame["label"].to_numpy()
    score = frame["score"].to_numpy(dtype=float)
    rng = np.random.default_rng(0)
    row_w = rng.integers(1, 5, len(frame)).astype(float)

    contrib, row_idx = positive_row_contributions(groups, y, score, None)
    _, item_inv = np.unique(items[row_idx], return_inverse=True)
    mine = _weighted_macro(
        contrib, item_inv, int(item_inv.max()) + 1, row_w[row_idx],
        _metric_kwargs())
    theirs = compute_macro_per_item_map(
        groups, items, y, score, weights=row_w, **_metric_kwargs())
    assert mine == pytest.approx(theirs, rel=1e-12, abs=1e-12)

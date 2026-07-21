import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import (
    iter_stratified_cluster_multipliers, paired_bootstrap_delta,
)


def _frame(n=60):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "group": np.repeat(np.arange(n // 2), 2),
        "cluster": np.repeat([f"e{i}" for i in range(n // 2)], 2),
        "item": np.tile(["a", "b"], n // 2),
        "label": rng.integers(0, 2, n),
        "score": rng.random(n),
        "stratum": np.repeat(["take_all", "hash_ratio"], n // 2),
        "inclusion_weight": np.repeat([1.0, 4.0], n // 2),
    })


def test_multipliers_stay_within_stratum():
    """層內重抽：某一層的乘數總和必須恆等於該層的單位數。

    跨層一起抽的話這個總和會隨機漂移——那正是 paired_bootstrap_delta
    docstring 第 2 點要避免的事。
    """
    f = _frame()
    clusters = pd.factorize(f["cluster"])[0]
    strata = f["stratum"].to_numpy()
    n_rows_per_stratum = {s: int((strata == s).sum()) for s in set(strata)}
    for mult in iter_stratified_cluster_multipliers(clusters, strata, 20, 7):
        for s, n_rows in n_rows_per_stratum.items():
            sel = strata == s
            # 每列一個乘數，同層的乘數總和 ＝ 該層列數（每個單位被抽到的
            # 次數總和 ＝ 單位數，而每個單位在此 fixture 各對應 2 列）
            assert mult[sel].sum() == pytest.approx(n_rows)


def test_refactor_leaves_paired_bootstrap_bit_identical():
    """抽骨架不准改數字。基準值由重構**前**的實作在同一 fixture 上跑出來，
    抄進這裡當黃金值——不是事後拿新實作的輸出回填。

    取得方式（2026-07-20）：`git stash` 暫存本次重構的 uncertainty.py 改動，
    在重構前的 `paired_bootstrap_delta` 上對同一組 fixture 跑一次拿到
    ``(-0.03205111434108531, 0.125683145273914)``，`git stash pop` 還原後
    再對重構後的實作跑同一組輸入比對逐位元相同，才把這兩個數字抄進來。
    """
    GOLDEN_LO = -0.03205111434108531
    GOLDEN_HI = 0.125683145273914
    f = _frame()
    mp = {"k": None, "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0.0}
    lo, hi = paired_bootstrap_delta(f, mp, {"a": 0.3}, n_boot=50, seed=42)
    assert (lo, hi) == (GOLDEN_LO, GOLDEN_HI)  # ← 實作者填入重構前實測值

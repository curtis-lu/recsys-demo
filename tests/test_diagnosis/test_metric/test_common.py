def test_per_item_ap_available_from_common():
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    assert callable(per_item_ap)


def test_item_ability_reuses_the_shared_per_item_ap():
    """釘住「同一個函式物件」而不是「兩邊算出來一樣」。

    後者對一份被複製貼上的副本照樣成立——而複製品會漂移，這正是本 task
    要消滅的東西。
    """
    import recsys_tfb.diagnosis.metric._common as common
    import recsys_tfb.diagnosis.metric.item_ability._compute as ia
    assert ia.per_item_ap is common.per_item_ap


def test_query_key_joins_multiple_columns_with_pipe():
    import pandas as pd
    from recsys_tfb.diagnosis.metric._common import query_key
    pdf = pd.DataFrame({"t": [1, 1, 2], "e": ["a", "b", "a"]})
    out = query_key(pdf, ["t", "e"])
    # 逐欄以 "|" 併鍵、非字串欄先 astype(str)
    assert list(out) == ["1|a", "1|b", "2|a"]


def test_query_key_single_column_has_no_separator():
    import pandas as pd
    from recsys_tfb.diagnosis.metric._common import query_key
    pdf = pd.DataFrame({"t": [1, 2]})
    out = query_key(pdf, ["t"])
    assert list(out) == ["1", "2"]


def test_sample_arrays_without_inclusion_weight_returns_none_ht_weights():
    """缺 inclusion_weight 欄：ht_weights 必須是 None（未加權路徑的語意標記），
    row_weights 是全 1 的 float 陣列。這兩條路對 mAP 的 weights 參數是位元
    等價的，所以只驗數值守不住『有沒有加權』——必須斷言 `is None` 這個結構性
    差異。順帶釘住：groups 的 factorize 分群、y 為 int64、item 欄一律被
    astype(str) 強制轉字串（故意用數字型 item id 行使這條轉換）。"""
    import numpy as np
    import pandas as pd
    from recsys_tfb.diagnosis.metric._common import sample_arrays
    schema = {"time": "t", "entity": ["e"], "item": "item", "label": "y"}
    pdf = pd.DataFrame({
        "t": [1, 1, 2],
        "e": ["a", "a", "b"],
        "item": [10, 20, 10],                                  # 數字型 → 行使 astype(str)
        "y": [1, 0, 1],
    })
    groups, items, y, ht_weights, row_weights = sample_arrays(pdf, schema)
    assert ht_weights is None                                  # 結構性：未加權路徑
    assert row_weights.dtype == np.float64
    np.testing.assert_array_equal(row_weights, np.ones(3))
    np.testing.assert_array_equal(groups, [0, 0, 1])           # (1,a),(1,a),(2,b)
    np.testing.assert_array_equal(items, ["10", "20", "10"])   # 數字被轉成字串
    assert all(isinstance(x, str) for x in items)              # 每個都是字串，證明 astype(str)
    np.testing.assert_array_equal(y, [1, 0, 1])
    assert y.dtype == np.int64


def test_sample_arrays_with_inclusion_weight_populates_both():
    """有 inclusion_weight：ht_weights 與 row_weights 都是該權重陣列。"""
    import numpy as np
    import pandas as pd
    from recsys_tfb.diagnosis.metric._common import sample_arrays
    schema = {"time": "t", "entity": ["e"], "item": "item", "label": "y"}
    pdf = pd.DataFrame({
        "t": [1, 1, 2],
        "e": ["a", "a", "b"],
        "item": ["x", "z", "x"],
        "y": [1, 0, 1],
        "inclusion_weight": [2.0, 2.0, 5.0],
    })
    _g, _i, _y, ht_weights, row_weights = sample_arrays(pdf, schema)
    assert ht_weights is not None                              # 結構性：加權路徑
    np.testing.assert_array_equal(ht_weights, [2.0, 2.0, 5.0])
    np.testing.assert_array_equal(row_weights, [2.0, 2.0, 5.0])


def test_ci_for_corrected_minus_baseline_flips_sign_and_forwards_args(monkeypatch):
    """兩件事一起釘：(1) 符號翻轉＋上下界對調——paired_bootstrap_delta 回反向差
    (baseline − corrected) 的 [lo, hi]，wrapper 要回 corrected − baseline 即
    (-hi, -lo)；(2) frame/metric_kwargs/shift/n_boot/seed 有正確轉發。純吞引數的
    stub 測不出轉發錯位，改用記錄 call args 的 spy。"""
    import recsys_tfb.diagnosis.metric._common as common
    calls = {}

    def spy(frame, metric_kwargs, shift, *, n_boot, seed):
        calls.update(frame=frame, metric_kwargs=metric_kwargs, shift=shift,
                     n_boot=n_boot, seed=seed)
        return (0.1, 0.4)

    monkeypatch.setattr(common, "paired_bootstrap_delta", spy)
    sentinel_frame, sentinel_shift = object(), object()
    lo, hi = common.ci_for_corrected_minus_baseline(
        frame=sentinel_frame, metric_kwargs={"k": 3}, shift=sentinel_shift,
        n_boot=17, seed=99,
    )
    assert (lo, hi) == (-0.4, -0.1)                    # 符號翻轉＋界對調
    assert calls == {                                  # 引數逐一正確轉發
        "frame": sentinel_frame, "metric_kwargs": {"k": 3},
        "shift": sentinel_shift, "n_boot": 17, "seed": 99,
    }


def test_to_logit_transforms_scores_in_unit_interval():
    import numpy as np
    from recsys_tfb.diagnosis.metric._common import to_logit
    # logit(0.5) = 0；logit(sigmoid(1)) = 1
    z, warns = to_logit(np.array([0.5, 0.7310585786300049]))
    assert warns == []
    np.testing.assert_allclose(z, [0.0, 1.0], atol=1e-9)


def test_to_logit_passes_through_out_of_range_scores_with_warning():
    import numpy as np
    from recsys_tfb.diagnosis.metric._common import to_logit
    raw = np.array([-0.5, 1.2, 3.0])                           # 超出 (0,1)
    z, warns = to_logit(raw)
    np.testing.assert_array_equal(z, raw)                      # 原樣（copy），未變換
    assert len(warns) == 1 and "略過 logit" in warns[0]


def test_per_item_ap_ground_truth_ranking_and_macro():
    """小型手算 ground-truth，一次釘住三件事：
    (1) within-query cumulative precision（名次決定分子/分母）、
    (2) 每個 item 對它多個正例列取平均、(3) 預設參數下 macro = item 等權平均。
    現有兩條測試只證『item_ability 用的是同一個函式物件』，證不了那物件算得對。"""
    import numpy as np
    import pytest
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    # 3 個 query；item "a" 在 q0 名次 1（prec 1.0）、在 q1 名次 2（prec 0.5）
    # → a 的 per-item AP = (1.0+0.5)/2 = 0.75；item "b" 在 q2 名次 1 → 1.0。
    groups = np.array([0, 0, 1, 1, 2, 2])
    items = np.array(["a", "n0", "a", "n1", "b", "n2"])
    y = np.array([1, 0, 1, 0, 1, 0])
    score = np.array([0.9, 0.1, 0.2, 0.8, 0.7, 0.1])
    mp = {"k": None, "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0.0}
    ap, counts, macro = per_item_ap(groups, items, y, score, mp)
    assert ap == pytest.approx({"a": 0.75, "b": 1.0})
    assert counts == {"a": 2, "b": 1}
    assert macro == pytest.approx(0.875)                       # 等權 mean([0.75, 1.0])


def test_to_logit_clips_exact_0_and_1_to_finite():
    """score 恰為 0.0 / 1.0 落在 (0,1) 判斷內（min≥0 且 max≤1），走 clip 路徑。
    clip 到 [eps, 1-eps] 是安全網，讓 log 不吐 ±inf——移掉 clip 這條就失守。
    用有限性斷言釘住它（前面兩條 in-interval/超界測試都碰不到 0/1 邊界）。"""
    import numpy as np
    from recsys_tfb.diagnosis.metric._common import to_logit
    z, warns = to_logit(np.array([0.0, 1.0]))
    assert warns == []                                         # 0/1 不算超界，不警告
    assert np.isfinite(z).all()                                # clip 後有限；無 clip 會是 ±inf
    assert z[0] < 0 < z[1]                                     # 0→負、1→正（clip 到兩端仍有限）


def test_per_item_ap_respects_k_truncation():
    """mp['k'] 會轉發給 positive_row_contributions：名次 > k 的正例貢獻歸零。
    b 在 q1 名次 2，k=1 時它的 AP 應為 0（k=None 時是 0.5），證明 k 真的有被
    轉發、不是寫死 None。"""
    import numpy as np
    import pytest
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    groups = np.array([0, 0, 1, 1])
    items = np.array(["a", "n0", "b", "n1"])
    y = np.array([1, 0, 1, 0])
    score = np.array([0.9, 0.1, 0.2, 0.8])                     # a 名次1、b 名次2
    mp = {"k": 1, "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0.0}
    ap, counts, macro = per_item_ap(groups, items, y, score, mp)
    assert ap == pytest.approx({"a": 1.0, "b": 0.0})           # b 被 k=1 截斷歸零
    assert macro == pytest.approx(0.5)                         # mean([1.0, 0.0])

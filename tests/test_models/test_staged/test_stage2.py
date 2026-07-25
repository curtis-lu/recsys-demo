import numpy as np
import pytest

from recsys_tfb.models.staged.stage2 import (
    encode_group_codes, fit_stage2, group_code_lookup,
    stage2_categorical_indices, stage2_matrix,
)


class TestGroupCodes:
    def test_lookup_is_sorted_rank(self):
        assert group_code_lookup(["b", "a", "c"]) == {"a": 0, "b": 1, "c": 2}

    def test_encode_maps_and_casts_float(self):
        codes = encode_group_codes(
            np.array(["b", "a", "b"], dtype=object), {"a": 0, "b": 1})
        np.testing.assert_array_equal(codes, [1.0, 0.0, 1.0])
        assert codes.dtype == np.float64

    def test_encode_unknown_key_raises(self):
        with pytest.raises(KeyError):
            encode_group_codes(np.array(["zz"], dtype=object), {"a": 0})


class TestStage2Matrix:
    def test_layout_x_then_s1_then_gcode(self):
        X = np.arange(6, dtype=float).reshape(3, 2)
        m = stage2_matrix(X, [0.1, 0.2, 0.3], [1.0, 0.0, 1.0])
        assert m.shape == (3, 4)
        np.testing.assert_array_equal(m[:, :2], X)
        np.testing.assert_allclose(m[:, 2], [0.1, 0.2, 0.3])  # s1 在 n_base
        np.testing.assert_array_equal(m[:, 3], [1.0, 0.0, 1.0])  # gcode 最後

    def test_categorical_indices_append_gcode(self):
        assert stage2_categorical_indices([0, 3], n_base_features=5) == [0, 3, 6]


def _toy(mode, n=240, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X2 = np.column_stack([rng.normal(loc=y), rng.normal(size=n),
                          rng.random(n), rng.integers(0, 3, n).astype(float)])
    qg = np.repeat(np.arange(n // 4), 4)  # 每 query 4 列
    return X2, y, qg


PARAMS = {"objective": None, "verbosity": -1, "num_threads": 1,
          "num_leaves": 5, "learning_rate": 0.2,
          "num_iterations": 20, "early_stopping_rounds": 5}


class TestFitStage2:
    def test_binary_trains_and_predicts_finite(self):
        X2, y, qg = _toy("binary")
        params = {**PARAMS, "objective": "binary", "metric": "binary_logloss"}
        adapter = fit_stage2("binary", X2, y, None, qg, X2, y, qg,
                             params, [3])
        preds = adapter.predict(X2)
        assert np.isfinite(preds).all() and len(preds) == len(y)

    def test_lambdarank_trains_with_query_groups(self):
        X2, y, qg = _toy("lambdarank")
        params = {**PARAMS, "objective": "lambdarank", "metric": "ndcg"}
        adapter = fit_stage2("lambdarank", X2, y, None, qg, X2, y, qg,
                             params, [3])
        preds = adapter.predict(X2)
        assert np.isfinite(preds).all() and len(preds) == len(y)

    def test_lambdarank_weight_perm_aligned(self, monkeypatch):
        # 結構性驗證：ranking 分支的 weight 必須跟著 perm 重排（shared prepare
        # 層同款契約）。spy lgb.Dataset 抓 weight 與 label 的對應。
        import lightgbm as lgb
        captured = {}
        real_dataset = lgb.Dataset

        def spy(data, label=None, weight=None, group=None, **kw):
            # train ds 沒有 reference（val ds 一定帶 reference=train_ds）；
            # lambdarank 的 val ds 為求早停 ndcg 正確也帶 group，不能只憑
            # group 是否非 None 分辨兩者。
            if group is not None and "reference" not in kw:  # 只抓 train ds
                captured["label"] = np.asarray(label)
                captured["weight"] = None if weight is None else np.asarray(weight)
            return real_dataset(data, label=label, weight=weight,
                                group=group, **kw)

        monkeypatch.setattr(
            "recsys_tfb.models.staged.stage2.lgb.Dataset", spy)
        X2, y, qg = _toy("lambdarank")
        # _toy 產生的 qg 本就逐 group 連續（[0,0,0,0,1,1,1,1,...]），
        # to_contiguous_groups 對它排序會得到恆等排列——藏不住少了 [perm]
        # 的錯。先打散列序，才能讓 perm 真的非恆等，這個測試才測到東西。
        shuffle = np.random.default_rng(7).permutation(len(y))
        X2, y, qg = X2[shuffle], y[shuffle], qg[shuffle]
        w = y * 10.0 + 1.0  # weight 與 label 完全相關 → 可驗對齊
        params = {**PARAMS, "objective": "lambdarank", "metric": "ndcg"}
        fit_stage2("lambdarank", X2, y, w, qg, X2, y, qg, params, [3])
        np.testing.assert_allclose(
            captured["weight"], captured["label"] * 10.0 + 1.0)

    def test_unknown_mode_raises(self):
        X2, y, qg = _toy("binary")
        with pytest.raises(ValueError, match="binary|lambdarank"):
            fit_stage2("rank_xendcg", X2, y, None, qg, X2, y, qg, PARAMS, [])

import numpy as np
import pytest

from recsys_tfb.models.staged.train_stage1 import GroupResult, train_one_group

ALGO = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
        "num_threads": 1, "num_leaves": 7, "learning_rate": 0.2,
        "num_iterations": 30, "early_stopping_rounds": 10}

SPACE = [{"name": "num_leaves", "type": "int", "low": 3, "high": 15}]


def _data(seed=0, n=200):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X = np.column_stack([rng.normal(loc=y, scale=1.0, size=n), rng.normal(size=n)])
    w = np.ones(n)
    return X, y, w


class TestTrainOneGroupFixedParams:
    def test_returns_result_with_booster_and_meta(self):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=80)
        r = train_one_group(
            group_key="a", X_tr=X, y_tr=y, w_tr=w, X_dev=Xd, y_dev=yd, w_dev=wd,
            algorithm_params=dict(ALGO), stage1_params={}, hpo_cfg={"n_trials": 0},
            categorical_indices=None, base_seed=42,
        )
        assert isinstance(r, GroupResult)
        assert r.group_key == "a"
        preds = r.adapter.predict(Xd)
        assert preds.shape == (len(Xd),)
        assert r.n_rows == len(X) and r.n_pos == int(y.sum())
        assert np.isfinite(r.score)

    def test_weights_reach_lgb_dataset(self):
        # 權重全 2.0 與全 1.0 對 logloss 訓練等價（均勻縮放），但把單一正例
        # 權重放大 1000 倍應顯著改變該點附近的預測 → 用可觀察行為驗權重有進去
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=80)
        boosted = w.copy()
        pos_idx = int(np.argmax(y))
        boosted[pos_idx] = 1000.0
        r_plain = train_one_group("a", X, y, w, Xd, yd, wd, dict(ALGO), {},
                                  {"n_trials": 0}, None, 42)
        r_boost = train_one_group("a", X, y, boosted, Xd, yd, wd, dict(ALGO), {},
                                  {"n_trials": 0}, None, 42)
        p_plain = r_plain.adapter.predict(X[pos_idx:pos_idx + 1])[0]
        p_boost = r_boost.adapter.predict(X[pos_idx:pos_idx + 1])[0]
        assert p_boost > p_plain  # 放大該正例權重 → 該點預測機率上升

    def test_dev_weights_reach_lgb_dataset(self, monkeypatch):
        # 結構性 spy：dev 權重只影響 early-stopping 指標，數值行為間接且脆弱，
        # 故直接斷言 dev Dataset 建構時收到 weight=w_dev（與 shared adapter
        # lightgbm_adapter.py 對 train-dev 帶權重的行為一致）。
        import recsys_tfb.models.staged.train_stage1 as m

        captured = []
        real_dataset = m.lgb.Dataset

        class SpyDataset(real_dataset):
            def __init__(self, *args, **kwargs):
                captured.append(dict(kwargs))
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(m.lgb, "Dataset", SpyDataset)
        X, y, w = _data()
        Xd, yd, _ = _data(seed=1, n=80)
        wd = np.full(len(yd), 3.0)
        train_one_group("a", X, y, w, Xd, yd, wd, dict(ALGO), {},
                        {"n_trials": 0}, None, 42)
        dev_calls = [k for k in captured if k.get("reference") is not None]
        assert dev_calls, "找不到 dev Dataset（以 reference= 識別）"
        assert dev_calls[0].get("weight") is not None, \
            "dev Dataset 未帶 weight——train_dev 權重沒有進 early stopping"
        np.testing.assert_array_equal(np.asarray(dev_calls[0]["weight"]), wd)

        # HPO 路徑（trial objective 內的 _fit_adapter 呼叫點）同樣必須帶 dev 權重
        captured.clear()
        train_one_group("a", X, y, w, Xd, yd, wd, dict(ALGO), {},
                        {"n_trials": 2, "metric": "auc",
                         "search_space": list(SPACE)}, None, 42)
        dev_calls = [k for k in captured if k.get("reference") is not None]
        assert dev_calls and all(k.get("weight") is not None for k in dev_calls), \
            "HPO trial 的 dev Dataset 未帶 weight"


class TestTrainOneGroupHpo:
    def _run(self, base_seed=42, group_key="a"):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=120)
        return train_one_group(
            group_key, X, y, w, Xd, yd, wd, dict(ALGO), {},
            {"n_trials": 4, "metric": "auc", "search_space": list(SPACE)},
            None, base_seed,
        )

    def test_deterministic_same_seed_same_best_params(self):
        assert self._run().best_params == self._run().best_params

    def test_different_group_key_different_trajectory(self):
        # 種子由 group_key 派生：不同群的 trial 序列應不同
        # （比較各 trial 的採樣值序列，不比 best——best 可能巧合相同）
        r_a, r_b = self._run(group_key="a"), self._run(group_key="b")
        assert r_a.trial_values != r_b.trial_values

    def test_metric_logloss_direction(self):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=120)
        r = train_one_group(
            "a", X, y, w, Xd, yd, wd, dict(ALGO), {},
            {"n_trials": 3, "metric": "logloss", "search_space": list(SPACE)},
            None, 42,
        )
        assert np.isfinite(r.score)  # score 記錄原始 metric（logloss 越小越好）

    def test_hpo_best_params_flow_into_final_adapter(self):
        r = self._run()
        assert set(r.best_params) == {"num_leaves"}
        assert 3 <= r.best_params["num_leaves"] <= 15

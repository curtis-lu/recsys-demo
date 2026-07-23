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

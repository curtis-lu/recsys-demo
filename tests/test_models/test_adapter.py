"""Tests for ModelAdapter ABC, LightGBMAdapter, and adapter registry."""

import numpy as np
import pytest

from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from tests.adapter_fits import fit_lightgbm


@pytest.fixture
def tiny_data():
    rng = np.random.RandomState(42)
    X_train = rng.randn(40, 3)
    y_train = rng.binomial(1, 0.3, 40).astype(float)
    X_val = rng.randn(10, 3)
    y_val = rng.binomial(1, 0.3, 10).astype(float)
    return X_train, y_train, X_val, y_val


@pytest.fixture
def train_params():
    return {
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "num_leaves": 4,
        "seed": 42,
        "num_iterations": 10,
        "early_stopping_rounds": 5,
    }


class TestModelAdapterABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            ModelAdapter()

    def test_incomplete_subclass_raises(self):
        class Partial(ModelAdapter):
            def train(self, X_train, y_train, X_val, y_val, params):
                pass

        with pytest.raises(TypeError):
            Partial()


class TestLightGBMAdapter:
    def test_train_and_predict(self, tiny_data, train_params):
        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)

        preds = adapter.predict(X_val)
        assert isinstance(preds, np.ndarray)
        assert preds.shape == (len(X_val),)
        assert np.all(preds >= 0) and np.all(preds <= 1)

    def test_predict_before_train_raises(self):
        adapter = LightGBMAdapter()
        with pytest.raises(RuntimeError):
            adapter.predict(np.zeros((5, 3)))

    def test_save_and_load(self, tmp_path, tiny_data, train_params):
        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)
        preds_original = adapter.predict(X_val)

        filepath = str(tmp_path / "model.txt")
        adapter.save(filepath)

        loaded = LightGBMAdapter()
        loaded.load(filepath)
        preds_loaded = loaded.predict(X_val)

        np.testing.assert_array_almost_equal(preds_original, preds_loaded)

    def test_feature_importance(self, tiny_data, train_params):
        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)

        fi = adapter.feature_importance()
        assert isinstance(fi, dict)
        assert len(fi) == 3  # 3 features

    def test_feature_importance_split_and_gain(self, tiny_data, train_params):
        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)

        split = adapter.feature_importance(kind="split")
        gain = adapter.feature_importance(kind="gain")
        assert set(split) == set(gain)
        assert all(isinstance(v, float) for v in split.values())
        assert all(isinstance(v, float) for v in gain.values())
        # default kind is "split" (backward compatible)
        assert adapter.feature_importance() == split

    def test_booster_property(self, tiny_data, train_params):
        import lightgbm as lgb
        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)
        assert isinstance(adapter.booster, lgb.Booster)

    def test_log_to_mlflow(self, tmp_path, tiny_data, train_params):
        import mlflow

        X_train, y_train, X_val, y_val = tiny_data
        adapter = fit_lightgbm(
            X_train, y_train, train_params, X_val=X_val, y_val=y_val)

        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        mlflow.set_experiment("test_adapter")
        with mlflow.start_run():
            adapter.log_to_mlflow()

        experiment = mlflow.get_experiment_by_name("test_adapter")
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1


class TestAdapterRegistry:
    def test_get_lightgbm(self):
        adapter = get_adapter("lightgbm")
        assert isinstance(adapter, LightGBMAdapter)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown algorithm"):
            get_adapter("unknown_algo")


def test_lightgbm_train_uses_log_period_from_params(monkeypatch, tiny_data):
    """`log_period` in params controls lgb.log_evaluation(period=...) and is
    popped before lgb.train (LightGBM warns on unknown params otherwise)."""
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    captured_periods: list[int] = []
    real_log_evaluation = lgb.log_evaluation

    def spy_log_evaluation(period=0, *args, **kwargs):
        captured_periods.append(period)
        return real_log_evaluation(period=period, *args, **kwargs)

    monkeypatch.setattr(lgb, "log_evaluation", spy_log_evaluation)

    X_train, y_train, X_val, y_val = tiny_data
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "num_leaves": 4,
        "seed": 42,
        "num_iterations": 5,
        "early_stopping_rounds": 3,
        "log_period": 2,
    }
    adapter = fit_lightgbm(X_train, y_train, params, X_val=X_val, y_val=y_val)

    assert captured_periods == [2], (
        f"expected log_evaluation called once with period=2, got {captured_periods}"
    )
    # log_period must be popped — LightGBM rejects unknown params silently but
    # the booster's saved params would otherwise carry it.
    assert "log_period" not in adapter.booster.params


def test_lightgbm_train_default_log_period_silent(monkeypatch, tiny_data, train_params):
    """Without `log_period` in params, default is 0 (silent) — preserves
    existing behavior."""
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    captured_periods: list[int] = []
    real_log_evaluation = lgb.log_evaluation

    def spy_log_evaluation(period=0, *args, **kwargs):
        captured_periods.append(period)
        return real_log_evaluation(period=period, *args, **kwargs)

    monkeypatch.setattr(lgb, "log_evaluation", spy_log_evaluation)

    X_train, y_train, X_val, y_val = tiny_data
    fit_lightgbm(X_train, y_train, train_params, X_val=X_val, y_val=y_val)

    assert captured_periods == [0]


def _saved_bins(tmp_path):
    """train.bin + train_dev.bin written through the adapter, as the cache does."""
    rng = np.random.default_rng(42)
    X_tr = rng.normal(size=(50, 3))
    y_tr = (rng.uniform(size=50) > 0.5).astype(int)
    X_dev = rng.normal(size=(20, 3))
    y_dev = (rng.uniform(size=20) > 0.5).astype(int)
    names = ["f0", "f1", "f2"]

    adapter = LightGBMAdapter()
    ds_tr = adapter.build_train_data(
        X_tr, y_tr, feature_names=names, categorical_features=[]).construct()
    adapter.save_train_data(ds_tr, str(tmp_path / "tr.bin"))
    ds_dev = adapter.build_train_data(
        X_dev, y_dev, feature_names=names, categorical_features=[],
        reference=ds_tr).construct()
    adapter.save_train_data(ds_dev, str(tmp_path / "dev.bin"))
    return str(tmp_path / "tr.bin"), str(tmp_path / "dev.bin")


def test_lightgbm_trains_on_binaries_read_back_from_disk(tmp_path):
    """load_train_data hands back what train() early-stops on, dev binned like train."""
    tr_bin, dev_bin = _saved_bins(tmp_path)
    adapter = LightGBMAdapter()
    ds_tr = adapter.load_train_data(tr_bin, weight=np.ones(50))
    ds_dev = adapter.load_train_data(dev_bin, weight=np.ones(20), reference=ds_tr)
    assert ds_tr.num_data() == 50
    assert ds_dev.num_data() == 20

    adapter.train(
        ds_tr, {"objective": "binary", "verbose": -1},
        num_iterations=5, early_stopping_rounds=3, valid_data=ds_dev,
    )
    assert adapter.booster.num_trees() > 0
    assert 1 <= adapter.best_iteration <= 5


@pytest.mark.parametrize("n_weights", [0, 49, 51])
def test_lightgbm_load_train_data_refuses_a_weight_vector_of_the_wrong_length(
    tmp_path, n_weights,
):
    """Length 0 is the case LightGBM itself lets through: an all-ones test
    passes vacuously on an empty array and set_weight discards it."""
    tr_bin, _ = _saved_bins(tmp_path)
    with pytest.raises(ValueError, match="binary holds 50 rows"):
        LightGBMAdapter().load_train_data(tr_bin, weight=np.ones(n_weights))


def test_lightgbm_load_train_data_applies_the_weights(tmp_path):
    tr_bin, _ = _saved_bins(tmp_path)
    weight = np.linspace(0.5, 2.0, 50)
    ds = LightGBMAdapter().load_train_data(tr_bin, weight=weight)
    np.testing.assert_allclose(ds.get_weight(), weight)


def test_lightgbm_train_leaves_the_callers_params_alone(tiny_data):
    """The old train() popped the round keys out of the caller's dict."""
    X_train, y_train, _, _ = tiny_data
    adapter = LightGBMAdapter()
    params = {"objective": "binary", "verbose": -1, "log_period": 0,
              "num_iterations": 99, "early_stopping_rounds": 7}
    before = dict(params)
    adapter.train(
        adapter.build_train_data(
            X_train, y_train, feature_names=["a", "b", "c"],
            categorical_features=[]),
        params, num_iterations=3,
    )
    assert params == before


def test_lightgbm_round_cap_is_the_argument_not_a_params_copy(tiny_data):
    """A `num_iterations` left inside params must not override the cap:
    LightGBM would read it in preference to num_boost_round."""
    X_train, y_train, _, _ = tiny_data
    adapter = LightGBMAdapter()
    adapter.train(
        adapter.build_train_data(
            X_train, y_train, feature_names=["a", "b", "c"],
            categorical_features=[]),
        {"objective": "binary", "verbose": -1, "min_data_in_leaf": 1,
         "num_iterations": 50},
        num_iterations=3,
    )
    assert adapter.booster.current_iteration() == 3

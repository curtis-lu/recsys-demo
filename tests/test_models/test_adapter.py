"""Tests for ModelAdapter ABC, LightGBMAdapter, and adapter registry."""

import numpy as np
import pytest

from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


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
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        preds = adapter.predict(X_val)
        assert isinstance(preds, np.ndarray)
        assert preds.shape == (len(X_val),)
        assert np.all(preds >= 0) and np.all(preds <= 1)

    def test_predict_before_train_raises(self):
        adapter = LightGBMAdapter()
        with pytest.raises(RuntimeError):
            adapter.predict(np.zeros((5, 3)))

    def test_save_and_load(self, tmp_path, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())
        preds_original = adapter.predict(X_val)

        filepath = str(tmp_path / "model.txt")
        adapter.save(filepath)

        loaded = LightGBMAdapter()
        loaded.load(filepath)
        preds_loaded = loaded.predict(X_val)

        np.testing.assert_array_almost_equal(preds_original, preds_loaded)

    def test_feature_importance(self, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        fi = adapter.feature_importance()
        assert isinstance(fi, dict)
        assert len(fi) == 3  # 3 features

    def test_log_to_mlflow(self, tmp_path, tiny_data, train_params):
        import mlflow

        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

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

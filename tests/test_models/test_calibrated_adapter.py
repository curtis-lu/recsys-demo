"""Tests for CalibratedModelAdapter."""

import numpy as np
import pytest

from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
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


@pytest.fixture
def trained_base(tiny_data, train_params):
    """Return a trained LightGBMAdapter."""
    adapter = LightGBMAdapter()
    X_train, y_train, X_val, y_val = tiny_data
    adapter.train(X_train, y_train, X_val, y_val, train_params.copy())
    return adapter


@pytest.fixture
def cal_data():
    """Calibration data (separate from training)."""
    rng = np.random.RandomState(99)
    X_cal = rng.randn(30, 3)
    y_cal = rng.binomial(1, 0.3, 30).astype(float)
    return X_cal, y_cal


class TestCalibratedModelAdapter:
    def test_fit_calibrator_isotonic(self, trained_base, cal_data, tiny_data):
        X_cal, y_cal = cal_data
        _, _, X_val, _ = tiny_data

        cal_adapter = CalibratedModelAdapter(trained_base, method="isotonic")
        cal_adapter.fit_calibrator(X_cal, y_cal)

        raw = trained_base.predict(X_val)
        calibrated = cal_adapter.predict(X_val)

        assert isinstance(calibrated, np.ndarray)
        assert calibrated.shape == raw.shape
        # Calibrated scores should differ from raw (at least some)
        assert not np.allclose(calibrated, raw)

    def test_fit_calibrator_sigmoid(self, trained_base, cal_data, tiny_data):
        X_cal, y_cal = cal_data
        _, _, X_val, _ = tiny_data

        cal_adapter = CalibratedModelAdapter(trained_base, method="sigmoid")
        cal_adapter.fit_calibrator(X_cal, y_cal)

        raw = trained_base.predict(X_val)
        calibrated = cal_adapter.predict(X_val)

        assert isinstance(calibrated, np.ndarray)
        assert calibrated.shape == raw.shape
        assert not np.allclose(calibrated, raw)

    def test_predict_uncalibrated_returns_raw(self, trained_base, cal_data, tiny_data):
        X_cal, y_cal = cal_data
        _, _, X_val, _ = tiny_data

        cal_adapter = CalibratedModelAdapter(trained_base, method="isotonic")
        cal_adapter.fit_calibrator(X_cal, y_cal)

        raw = trained_base.predict(X_val)
        uncalibrated = cal_adapter.predict_uncalibrated(X_val)
        np.testing.assert_array_equal(raw, uncalibrated)

    def test_predict_without_calibrator_returns_raw(self, trained_base, tiny_data):
        """When calibrator is not fitted, predict() returns raw scores."""
        _, _, X_val, _ = tiny_data
        cal_adapter = CalibratedModelAdapter(trained_base, method="isotonic")

        raw = trained_base.predict(X_val)
        result = cal_adapter.predict(X_val)
        np.testing.assert_array_equal(raw, result)

    def test_save_and_load_roundtrip(self, tmp_path, trained_base, cal_data, tiny_data):
        X_cal, y_cal = cal_data
        _, _, X_val, _ = tiny_data

        cal_adapter = CalibratedModelAdapter(trained_base, method="isotonic")
        cal_adapter.fit_calibrator(X_cal, y_cal)
        preds_before = cal_adapter.predict(X_val)

        filepath = str(tmp_path / "model.txt")
        cal_adapter.save(filepath)

        # Load into a fresh wrapper
        new_base = LightGBMAdapter()
        loaded = CalibratedModelAdapter(new_base)
        loaded.load(filepath)
        preds_after = loaded.predict(X_val)

        np.testing.assert_array_almost_equal(preds_before, preds_after)
        assert loaded.method == "isotonic"

    def test_feature_importance_delegates(self, trained_base):
        cal_adapter = CalibratedModelAdapter(trained_base)
        fi = cal_adapter.feature_importance()
        assert fi == trained_base.feature_importance()

    def test_unknown_method_raises(self, trained_base, cal_data):
        X_cal, y_cal = cal_data
        cal_adapter = CalibratedModelAdapter(trained_base, method="unknown")
        with pytest.raises(ValueError, match="Unknown calibration method"):
            cal_adapter.fit_calibrator(X_cal, y_cal)

    def test_properties(self, trained_base):
        cal_adapter = CalibratedModelAdapter(trained_base, method="sigmoid")
        assert cal_adapter.base is trained_base
        assert cal_adapter.method == "sigmoid"

    def test_train_delegates(self, tiny_data, train_params):
        base = LightGBMAdapter()
        cal_adapter = CalibratedModelAdapter(base, method="isotonic")
        X_train, y_train, X_val, y_val = tiny_data
        cal_adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        # Base should now be trained
        preds = base.predict(X_val)
        assert isinstance(preds, np.ndarray)
        assert preds.shape == (len(X_val),)

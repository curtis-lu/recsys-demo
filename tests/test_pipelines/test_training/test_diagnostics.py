"""Unit tests for training diagnostics (pure-python, no Spark)."""

import numpy as np
import pytest

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training import diagnostics as diag


@pytest.fixture
def fitted_adapter():
    rng = np.random.RandomState(0)
    X = rng.randn(120, 4)
    # feature 3 is pure noise → expected dead / low importance
    y = (X[:, 0] + X[:, 1] > 0).astype(float)
    adapter = LightGBMAdapter()
    params = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
              "num_leaves": 4, "seed": 0, "num_iterations": 20, "early_stopping_rounds": 0}
    adapter.train(X, y, None, None, params)
    return adapter


def test_compute_feature_importance_shape_and_dead(fitted_adapter):
    params = {"diagnostics": {"feature_importance": {"enabled": True}}}
    out = diag.compute_feature_importance(fitted_adapter, params)
    assert set(out) == {"ranked", "dead_features"}
    # ranked sorted by gain desc
    gains = [r["gain"] for r in out["ranked"]]
    assert gains == sorted(gains, reverse=True)
    # every ranked row has feature/split/gain
    assert all({"feature", "split", "gain"} <= set(r) for r in out["ranked"])
    # dead_features are exactly the split==0 ones
    dead = {r["feature"] for r in out["ranked"] if r["split"] == 0}
    assert set(out["dead_features"]) == dead


def test_compute_feature_importance_disabled(fitted_adapter):
    params = {"diagnostics": {"feature_importance": {"enabled": False}}}
    assert diag.compute_feature_importance(fitted_adapter, params) == {}

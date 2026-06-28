"""attribution 接縫單元測試（pure-python）。"""
import numpy as np
import pytest

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.diagnostics import attribution


def _fitted():
    rng = np.random.RandomState(0)
    X = rng.randn(80, 3)
    y = (X[:, 0] > 0).astype(float)
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
            "num_leaves": 4, "seed": 0, "num_iterations": 10, "early_stopping_rounds": 0})
    return a, X


def test_feature_attributions_shape():
    a, X = _fitted()
    sv = attribution.feature_attributions(a, X, ["f0", "f1", "f2"])
    assert sv.shape == (80, 3)


def test_attribution_budget_units_positive():
    a, _ = _fitted()
    assert attribution.attribution_budget_units(a) >= 1


def test_feature_attributions_raises_without_booster():
    class NoBooster:
        pass
    with pytest.raises(TypeError, match="booster"):
        attribution.feature_attributions(NoBooster(), np.zeros((2, 3)), ["a", "b", "c"])

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.adapter import (
    StagedMissingGroupError, StagedModelAdapter,
)
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.nodes import _predict_for_partition


def _tiny_lgb(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(60) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=60), rng.normal(size=60)])
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
                               "num_threads": 1, "num_leaves": 4,
                               "num_iterations": 8,
                               "early_stopping_rounds": 0})
    return a


class TestPredictForPartition:
    def _staged(self):
        m = StagedModelAdapter()
        m.add_group("A", _tiny_lgb(0), meta={})
        m.set_partition_keys(["seg"])
        return m

    def test_shared_adapter_uses_plain_predict(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.2], "seg": ["A"]})
        X = pdf[["f1", "f2"]].values
        scores = _predict_for_partition(_tiny_lgb(), X, pdf)
        assert scores.shape == (1,)

    def test_staged_adapter_routes_by_partition_keys(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.3], "f2": [0.2, 0.1],
                            "seg": ["A", "A"]})
        X = pdf[["f1", "f2"]].values
        scores = _predict_for_partition(self._staged(), X, pdf)
        assert scores.shape == (2,) and np.isfinite(scores).all()

    def test_staged_missing_group_raises(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.2], "seg": ["ZZ"]})
        X = pdf[["f1", "f2"]].values
        with pytest.raises(StagedMissingGroupError, match="'ZZ'"):
            _predict_for_partition(self._staged(), X, pdf)

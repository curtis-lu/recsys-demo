"""Tests for ModelAdapterDataset round-trip and sidecar isolation.

The hpo_best_model catalog entry lives in a hpo/ subdirectory precisely so
its model_meta.json sidecar cannot collide with the final model's — these
tests pin that behavior.
"""

import numpy as np

from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
from recsys_tfb.models.base import get_adapter


def _tiny_adapter():
    rng = np.random.default_rng(42)
    X = rng.normal(size=(80, 3))
    y = (X[:, 0] > 0).astype(int)
    adapter = get_adapter("lightgbm")
    adapter.train(
        X, y, X, y,
        {"objective": "binary", "num_iterations": 5,
         "min_child_samples": 5, "verbose": -1},
    )
    return adapter, X


class TestModelAdapterDatasetRoundTrip:
    def test_save_load_predict_consistency(self, tmp_path):
        adapter, X = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds.save(adapter)
        assert ds.exists()
        loaded = ds.load()
        np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))

    def test_sidecar_isolation_between_model_and_hpo_model(self, tmp_path):
        adapter, _ = _tiny_adapter()
        ds_model = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds_hpo = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds_model.save(adapter)
        ds_hpo.save(adapter)
        # each directory carries its own sidecar — no cross-talk
        assert (tmp_path / "model_meta.json").exists()
        assert (tmp_path / "hpo" / "model_meta.json").exists()
        assert ds_hpo.load() is not None
        assert ds_model.load() is not None

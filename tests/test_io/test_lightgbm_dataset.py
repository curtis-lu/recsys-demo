"""Tests for ModelAdapterDataset I/O adapter."""

import json

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


@pytest.fixture
def trained_adapter():
    """Train a minimal LightGBMAdapter for testing."""
    rng = np.random.RandomState(42)
    X = rng.randn(20, 2)
    y = rng.binomial(1, 0.5, 20).astype(float)

    adapter = LightGBMAdapter()
    adapter.train(X, y, X, y, {
        "objective": "binary",
        "verbosity": -1,
        "num_leaves": 4,
        "num_iterations": 5,
        "early_stopping_rounds": 5,
    })
    return adapter, X


class TestModelAdapterDataset:
    def test_save_load_roundtrip(self, tmp_path, trained_adapter):
        adapter, X = trained_adapter
        filepath = str(tmp_path / "model.txt")
        ds = ModelAdapterDataset(filepath=filepath)

        ds.save(adapter)
        loaded = ds.load()

        assert isinstance(loaded, LightGBMAdapter)
        np.testing.assert_array_almost_equal(
            adapter.predict(X), loaded.predict(X)
        )

    def test_meta_sidecar_created(self, tmp_path, trained_adapter):
        adapter, _ = trained_adapter
        filepath = str(tmp_path / "model.txt")
        ds = ModelAdapterDataset(filepath=filepath)
        ds.save(adapter)

        meta_path = tmp_path / "model_meta.json"
        assert meta_path.exists()

        with open(meta_path) as f:
            meta = json.load(f)
        assert meta["algorithm"] == "lightgbm"
        assert "LightGBMAdapter" in meta["adapter_class"]
        assert "saved_at" in meta

    def test_exists_before_and_after_save(self, tmp_path, trained_adapter):
        adapter, _ = trained_adapter
        filepath = str(tmp_path / "model.txt")
        ds = ModelAdapterDataset(filepath=filepath)

        assert not ds.exists()
        ds.save(adapter)
        assert ds.exists()

    def test_creates_parent_directory(self, tmp_path, trained_adapter):
        adapter, _ = trained_adapter
        filepath = str(tmp_path / "subdir" / "deep" / "model.txt")
        ds = ModelAdapterDataset(filepath=filepath)
        ds.save(adapter)
        assert ds.exists()

    def test_fallback_without_meta(self, tmp_path, trained_adapter):
        """Load without model_meta.json should fallback to LightGBM."""
        adapter, X = trained_adapter
        filepath = str(tmp_path / "model.txt")

        # Save model directly (no sidecar)
        adapter.save(filepath)

        ds = ModelAdapterDataset(filepath=filepath)
        loaded = ds.load()

        assert isinstance(loaded, LightGBMAdapter)
        np.testing.assert_array_almost_equal(
            adapter.predict(X), loaded.predict(X)
        )

"""Tests for LightGBMDataset I/O adapter."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.lightgbm_dataset import LightGBMDataset


@pytest.fixture
def tiny_booster():
    """Train a minimal LightGBM Booster for testing."""
    import lightgbm as lgb

    rng = np.random.RandomState(42)
    X = pd.DataFrame({"a": rng.randn(20), "b": rng.randn(20)})
    y = rng.binomial(1, 0.5, 20).astype(float)
    ds = lgb.Dataset(X, label=y, free_raw_data=False)
    booster = lgb.train(
        {"objective": "binary", "verbosity": -1, "num_leaves": 4},
        ds,
        num_boost_round=5,
    )
    return booster, X


class TestLightGBMDataset:
    def test_save_load_roundtrip(self, tmp_path, tiny_booster):
        booster, X = tiny_booster
        filepath = str(tmp_path / "model.txt")
        ds = LightGBMDataset(filepath=filepath)

        ds.save(booster)
        loaded = ds.load()

        np.testing.assert_array_almost_equal(
            booster.predict(X), loaded.predict(X)
        )

    def test_exists_before_and_after_save(self, tmp_path, tiny_booster):
        booster, _ = tiny_booster
        filepath = str(tmp_path / "model.txt")
        ds = LightGBMDataset(filepath=filepath)

        assert not ds.exists()
        ds.save(booster)
        assert ds.exists()

    def test_saved_file_is_text(self, tmp_path, tiny_booster):
        booster, _ = tiny_booster
        filepath = str(tmp_path / "model.txt")
        ds = LightGBMDataset(filepath=filepath)
        ds.save(booster)

        with open(filepath) as f:
            content = f.read()
        assert "tree" in content or "num_trees" in content

    def test_creates_parent_directory(self, tmp_path, tiny_booster):
        booster, _ = tiny_booster
        filepath = str(tmp_path / "subdir" / "deep" / "model.txt")
        ds = LightGBMDataset(filepath=filepath)
        ds.save(booster)
        assert ds.exists()

import json
import numpy as np
import pytest

from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
from recsys_tfb.models.base import get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import (
    StagedMissingGroupError, StagedModelAdapter,
)


def _tiny_adapter(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(80) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=80), rng.normal(size=80)])
    a = LightGBMAdapter()
    a.train(X, y, None, None,
            {"objective": "binary", "verbosity": -1, "num_threads": 1,
             "num_leaves": 4, "num_iterations": 10,
             "early_stopping_rounds": 0})
    return a


def _staged(groups=("a", "b")):
    m = StagedModelAdapter()
    for i, g in enumerate(groups):
        m.add_group(g, _tiny_adapter(seed=i),
                    meta={"best_params": {}, "score": 0.5, "metric": "auc",
                          "n_rows": 80, "n_pos": 30, "train_seconds": 0.1})
    m.set_partition_keys(["seg"])
    return m


class TestPredictRouted:
    def test_routes_rows_to_own_group_model(self):
        m = _staged()
        X = np.random.default_rng(1).normal(size=(6, 2))
        keys = np.array(["a", "b", "a", "b", "a", "b"], dtype=object)
        scores, mask = m.predict_routed(X, keys, on_missing="raise")
        assert mask.all() and scores.shape == (6,)
        only_a, _ = m.predict_routed(X, np.array(["a"] * 6, dtype=object),
                                     on_missing="raise")
        # 同列不同群模型分數應不同（兩個模型不同 seed 訓練）
        assert not np.allclose(scores, only_a)

    def test_missing_group_raise_lists_counts(self):
        m = _staged()
        X = np.zeros((3, 2))
        keys = np.array(["a", "zz", "zz"], dtype=object)
        with pytest.raises(StagedMissingGroupError, match="'zz'.*2"):
            m.predict_routed(X, keys, on_missing="raise")

    def test_missing_group_skip_returns_mask_and_stats(self):
        m = _staged()
        X = np.zeros((3, 2))
        keys = np.array(["a", "zz", "zz"], dtype=object)
        scores, mask = m.predict_routed(X, keys, on_missing="skip")
        assert mask.tolist() == [True, False, False]
        assert np.isnan(scores[~mask]).all()
        assert m.last_missing_stats == {"zz": 2}

    def test_plain_predict_raises_guidance(self):
        with pytest.raises(NotImplementedError, match="predict_routed"):
            _staged().predict(np.zeros((1, 2)))


class TestSaveLoadBundle(object):
    def test_roundtrip_via_model_adapter_dataset(self, tmp_path):
        m = _staged()
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(m)
        meta = json.loads((tmp_path / "v1" / "model_meta.json").read_text())
        assert meta["algorithm"] == "staged"
        loaded = ds.load()
        assert isinstance(loaded, StagedModelAdapter)
        X = np.random.default_rng(2).normal(size=(4, 2))
        keys = np.array(["a", "b", "a", "b"], dtype=object)
        s1, _ = m.predict_routed(X, keys, on_missing="raise")
        s2, _ = loaded.predict_routed(X, keys, on_missing="raise")
        np.testing.assert_allclose(s1, s2)

    def test_save_leaves_no_tmp_dir(self, tmp_path):
        filepath = tmp_path / "v1" / "model.txt"
        ModelAdapterDataset(filepath=str(filepath)).save(_staged())
        leftovers = [p for p in (tmp_path / "v1").iterdir()
                     if p.name.startswith("stage1") and p.name != "stage1"]
        assert leftovers == []

    def test_load_detects_missing_group_file(self, tmp_path):
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(_staged())
        victim = next((tmp_path / "v1" / "stage1").glob("*.txt"))
        victim.unlink()
        with pytest.raises(ValueError, match="bundle"):
            ds.load()

    def test_load_detects_bundle_id_mismatch(self, tmp_path):
        # 模擬混血 bundle：index 是舊 run 的、stage1/ 是新 run 的
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(_staged())
        stale_index = filepath.read_text()
        ds.save(_staged(groups=("a", "b")))  # 第二次 save（新 bundle_id）
        filepath.write_text(stale_index)     # index 換回舊的
        with pytest.raises(ValueError, match="bundle"):
            ds.load()


class TestRegistry:
    def test_staged_registered(self):
        assert get_adapter("staged") is not None

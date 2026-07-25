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


@pytest.fixture
def two_group_adapter():
    """A/B 兩群、各一個真 LightGBM booster、2 個特徵欄。"""
    return _staged(groups=("A", "B"))


@pytest.fixture
def real_stage2_adapter():
    """用 Task 4 的 fit_stage2("binary", ...) 以 4 欄 X2 訓練的真 adapter
    （對齊 two_group_adapter 的 2 個 base 特徵 + s1 + gcode = 4 欄）。"""
    from recsys_tfb.models.staged.stage2 import fit_stage2

    rng = np.random.default_rng(3)
    n = 240
    y = (rng.random(n) < 0.3).astype(int)
    X2 = np.column_stack([rng.normal(loc=y), rng.normal(size=n),
                          rng.random(n), rng.integers(0, 2, n).astype(float)])
    qg = np.repeat(np.arange(n // 4), 4)
    params = {"objective": "binary", "metric": "binary_logloss",
              "verbosity": -1, "num_threads": 1, "num_leaves": 5,
              "learning_rate": 0.2, "num_iterations": 20,
              "early_stopping_rounds": 5}
    return fit_stage2("binary", X2, y, None, qg, X2, y, None, qg, params, [3])


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


class _FakeStage2:
    """記錄輸入矩陣的假 stage-2 adapter（save/load 走真 LightGBM 的測試另計）。"""
    def __init__(self):
        self.seen = None
    def predict(self, X2):
        self.seen = np.asarray(X2)
        return np.full(len(X2), 7.0)


class TestStage2Composition:
    def test_predict_routed_feeds_stage2_matrix(self, two_group_adapter):
        model = two_group_adapter
        fake = _FakeStage2()
        model.set_stage2(fake, {"mode": "binary", "oof_folds": 3})
        X = np.random.default_rng(0).normal(size=(6, 2))
        keys = np.array(["A", "B", "A", "B", "A", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys, on_missing="raise")
        assert mask.all()
        np.testing.assert_array_equal(scores, 7.0)      # 全走 stage-2
        assert fake.seen.shape == (6, 4)                 # X(2)+s1+gcode
        np.testing.assert_array_equal(
            fake.seen[:, 3], [0, 1, 0, 1, 0, 1])        # gcode=sorted rank
        assert np.isfinite(fake.seen[:, 2]).all()        # s1 分數已填入

    def test_skip_mode_missing_rows_stay_nan(self, two_group_adapter):
        model = two_group_adapter
        model.set_stage2(_FakeStage2(), {"mode": "binary"})
        X = np.zeros((3, 2))
        keys = np.array(["A", "ZZ", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys, on_missing="skip")
        assert not mask[1] and np.isnan(scores[1])
        assert mask[0] and mask[2] and (scores[[0, 2]] == 7.0).all()

    def test_stage2_mode_property(self, two_group_adapter):
        assert two_group_adapter.stage2_mode == "none"
        two_group_adapter.set_stage2(_FakeStage2(), {"mode": "lambdarank"})
        assert two_group_adapter.stage2_mode == "lambdarank"


class TestStage2Persistence:
    def test_save_load_roundtrip_with_stage2(self, tmp_path, two_group_adapter,
                                             real_stage2_adapter):
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary",
                                               "oof_folds": 3})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        assert (tmp_path / "mv" / "stage2" / "model.txt").exists()
        assert (tmp_path / "mv" / "stage2" / ".bundle_id").exists()
        loaded = StagedModelAdapter()
        loaded.load(str(fp))
        assert loaded.stage2_mode == "binary"
        X = np.random.default_rng(1).normal(size=(4, 2))
        keys = np.array(["A", "B", "A", "B"], dtype=object)
        s0, _ = model.predict_routed(X, keys)
        s1, _ = loaded.predict_routed(X, keys)
        np.testing.assert_allclose(s0, s1)

    def test_load_fails_on_stage2_bundle_id_mismatch(self, tmp_path,
                                                     two_group_adapter,
                                                     real_stage2_adapter):
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary"})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        (tmp_path / "mv" / "stage2" / ".bundle_id").write_text("tampered")
        with pytest.raises(ValueError, match="stage2"):
            StagedModelAdapter().load(str(fp))

    def test_save_without_stage2_removes_stale_dir(self, tmp_path,
                                                   two_group_adapter,
                                                   real_stage2_adapter):
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary"})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        fresh = StagedModelAdapter()          # 無 stage-2 的新 bundle
        for k in model.group_keys:
            fresh.add_group(k, model._groups[k], meta={})
        fresh.set_partition_keys(model.partition_keys)
        fresh.save(str(fp))
        assert not (tmp_path / "mv" / "stage2").exists()  # 殘留清掉
        loaded = StagedModelAdapter()
        loaded.load(str(fp))                  # 不因 index 無 stage2 而炸
        assert loaded.stage2_mode == "none"

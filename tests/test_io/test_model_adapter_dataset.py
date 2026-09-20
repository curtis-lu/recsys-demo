"""Tests for ModelAdapterDataset round-trip, sidecar isolation, and the
calibrated-model guard.

The hpo_best_model catalog entry lives in a hpo/ subdirectory precisely so
its model_meta.json sidecar cannot collide with the final model's — these
tests pin that behavior.
"""

import json

import numpy as np
import pytest

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


def _rewrite_meta(path, **overrides):
    """Patch the sidecar an already-saved model wrote, to stand in for a model
    produced by a pre-#411 version of the framework."""
    meta = json.loads(path.read_text())
    meta.update(overrides)
    path.write_text(json.dumps(meta, indent=2))


class TestModelAdapterDatasetRoundTrip:
    def test_save_load_predict_consistency(self, tmp_path):
        adapter, X = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds.save(adapter)
        assert ds.exists()
        loaded = ds.load()
        np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))

    def test_save_writes_no_calibration_flag(self, tmp_path):
        """Calibration left the framework in #411, so nothing saved now claims
        anything about it — old readers see the key absent, not ``false``."""
        adapter, _ = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds.save(adapter)
        meta = json.loads((tmp_path / "model_meta.json").read_text())
        assert "calibrated" not in meta
        assert "calibration_method" not in meta
        # ...and the sidecar is still the thing that picks the adapter back up.
        assert meta["algorithm"] == "lightgbm"

    def test_sidecar_isolation_between_model_and_hpo_model(self, tmp_path):
        adapter, _ = _tiny_adapter()
        ds_hpo = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds_hpo.save(adapter)
        ds_model = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds_model.save(adapter)

        # each directory carries its own sidecar — no cross-talk
        assert (tmp_path / "model_meta.json").exists()
        assert (tmp_path / "hpo" / "model_meta.json").exists()

        # The cross-talk that would matter: a sidecar in the PARENT directory
        # that refuses to load must not reach the model under hpo/.
        _rewrite_meta(tmp_path / "model_meta.json", calibrated=True,
                      calibration_method="isotonic")
        loaded_hpo = ds_hpo.load()
        _, X_probe = _tiny_adapter()
        np.testing.assert_allclose(
            loaded_hpo.predict(X_probe), adapter.predict(X_probe)
        )
        with pytest.raises(ValueError):
            ds_model.load()


class TestCalibratedModelIsRefused:
    """A model saved by a pre-#411 version with a calibrator attached.

    The calibrator class is gone, so there is nothing to rebuild it with. The
    two failures this guard replaces are both worse than an error: an
    ``ImportError`` naming a module the operator has never heard of, or —
    if the flag were simply ignored — a silent switch to uncalibrated scores
    under a model whose downstream consumers were told the scores were
    probabilities.
    """

    def test_a_calibrated_model_raises_on_load(self, tmp_path):
        adapter, _ = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds.save(adapter)
        _rewrite_meta(tmp_path / "model_meta.json", calibrated=True,
                      calibration_method="isotonic")

        with pytest.raises(ValueError) as exc:
            ds.load()
        message = str(exc.value)
        assert "calibrat" in message.lower()
        assert "retrain" in message.lower()
        assert str(tmp_path / "model_meta.json") in message

    def test_calibrated_false_loads_normally(self, tmp_path):
        """Every model this framework ever shipped writes ``calibrated: false``
        when calibration was off — they stay loadable, extra keys and all."""
        adapter, X = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds.save(adapter)
        _rewrite_meta(tmp_path / "model_meta.json", calibrated=False,
                      saved_at="2026-01-01T00:00:00+00:00")

        loaded = ds.load()
        np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))

    def test_a_sidecar_without_the_key_loads_normally(self, tmp_path):
        adapter, X = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds.save(adapter)
        loaded = ds.load()
        np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))

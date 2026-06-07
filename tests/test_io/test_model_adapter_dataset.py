"""Tests for ModelAdapterDataset I/O adapter."""


def test_model_adapter_dataset_round_trips_composite(tmp_path):
    import json
    import numpy as np
    from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
    from tests.test_models.test_composite_adapter import _make_adapter

    a = _make_adapter()
    ds = ModelAdapterDataset(str(tmp_path / "model.txt"))
    ds.save(a)
    # meta records algorithm=composite (registry match on type)
    meta = json.load(open(tmp_path / "model_meta.json"))
    assert meta["algorithm"] == "composite"
    assert meta["calibrated"] is False

    loaded = ds.load()
    X = np.array([[0.1, 0.0, 0.3], [0.4, 1.0, 0.6]])
    np.testing.assert_allclose(loaded.predict(X), a.predict(X), rtol=1e-6)

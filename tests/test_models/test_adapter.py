"""Tests for ModelAdapter ABC, LightGBMAdapter, and adapter registry."""

import numpy as np
import pytest

from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


@pytest.fixture
def tiny_data():
    rng = np.random.RandomState(42)
    X_train = rng.randn(40, 3)
    y_train = rng.binomial(1, 0.3, 40).astype(float)
    X_val = rng.randn(10, 3)
    y_val = rng.binomial(1, 0.3, 10).astype(float)
    return X_train, y_train, X_val, y_val


@pytest.fixture
def train_params():
    return {
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "num_leaves": 4,
        "seed": 42,
        "num_iterations": 10,
        "early_stopping_rounds": 5,
    }


class TestModelAdapterABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            ModelAdapter()

    def test_incomplete_subclass_raises(self):
        class Partial(ModelAdapter):
            def train(self, X_train, y_train, X_val, y_val, params):
                pass

        with pytest.raises(TypeError):
            Partial()


class TestLightGBMAdapter:
    def test_train_and_predict(self, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        preds = adapter.predict(X_val)
        assert isinstance(preds, np.ndarray)
        assert preds.shape == (len(X_val),)
        assert np.all(preds >= 0) and np.all(preds <= 1)

    def test_predict_before_train_raises(self):
        adapter = LightGBMAdapter()
        with pytest.raises(RuntimeError):
            adapter.predict(np.zeros((5, 3)))

    def test_save_and_load(self, tmp_path, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())
        preds_original = adapter.predict(X_val)

        filepath = str(tmp_path / "model.txt")
        adapter.save(filepath)

        loaded = LightGBMAdapter()
        loaded.load(filepath)
        preds_loaded = loaded.predict(X_val)

        np.testing.assert_array_almost_equal(preds_original, preds_loaded)

    def test_feature_importance(self, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        fi = adapter.feature_importance()
        assert isinstance(fi, dict)
        assert len(fi) == 3  # 3 features

    def test_log_to_mlflow(self, tmp_path, tiny_data, train_params):
        import mlflow

        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        mlflow.set_experiment("test_adapter")
        with mlflow.start_run():
            adapter.log_to_mlflow()

        experiment = mlflow.get_experiment_by_name("test_adapter")
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1


class TestAdapterRegistry:
    def test_get_lightgbm(self):
        adapter = get_adapter("lightgbm")
        assert isinstance(adapter, LightGBMAdapter)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown algorithm"):
            get_adapter("unknown_algo")


def test_model_adapter_prepare_train_inputs_is_abstract():
    """Any concrete subclass of ModelAdapter must implement prepare_train_inputs."""
    import pytest
    from recsys_tfb.models.base import ModelAdapter

    class DummyAdapter(ModelAdapter):
        def train(self, X_train, y_train, X_val, y_val, params): ...
        def predict(self, X): ...
        def save(self, filepath): ...
        def load(self, filepath): ...
        def feature_importance(self): ...
        def log_to_mlflow(self): ...

    with pytest.raises(TypeError, match="prepare_train_inputs"):
        DummyAdapter()


def test_lightgbm_prepare_train_inputs_writes_bins(tmp_path):
    """prepare_train_inputs writes train.bin, train_dev.bin, _SUCCESS."""
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    df_dev = pd.DataFrame(
        {
            "cust_id": ["c5", "c6"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.5, 2.5],
            "label": [1, 0],
        }
    )
    train_dir = tmp_path / "train.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df_tr.to_parquet(train_dir, engine="pyarrow")
    df_dev.to_parquet(dev_dir, engine="pyarrow")

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"
    train_h, dev_h = adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)),
        ParquetHandle(str(dev_dir)),
        prep_meta,
        parameters,
        str(cache_dir),
    )

    assert (cache_dir / "lgb" / "train.bin").exists()
    assert (cache_dir / "lgb" / "train_dev.bin").exists()
    assert (cache_dir / "lgb" / "_SUCCESS").exists()
    assert train_h.role == "train"
    assert dev_h.role == "train_dev"


def test_lightgbm_prepare_train_inputs_cache_hit(tmp_path, monkeypatch):
    """Second call with valid _SUCCESS marker skips lgb.Dataset.construct."""
    import pandas as pd
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    df_dev = df_tr.copy()
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df_tr.to_parquet(train_dir)
    df_dev.to_parquet(dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }
    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )
    assert (cache_dir / "lgb" / "_SUCCESS").exists()

    construct_calls = []
    real_construct = lgb.Dataset.construct

    def spy_construct(self):
        construct_calls.append(1)
        return real_construct(self)

    monkeypatch.setattr(lgb.Dataset, "construct", spy_construct)

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    assert construct_calls == [], "cache hit should not call lgb.Dataset.construct"


def test_lightgbm_prepare_train_inputs_partial_cache_rebuild(tmp_path):
    """If lgb/ exists but _SUCCESS is missing, rmtree and rebuild."""
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df.to_parquet(train_dir)
    df.to_parquet(dev_dir)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }
    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    # Simulate crash: remove _SUCCESS but leave bins
    (cache_dir / "lgb" / "_SUCCESS").unlink()

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    assert (cache_dir / "lgb" / "_SUCCESS").exists()
    assert (cache_dir / "lgb" / "train.bin").exists()


def test_lightgbm_train_accepts_prebuilt_datasets(tmp_path):
    """train() with train_dataset= / val_dataset= kwargs uses pre-built Datasets."""
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    rng = np.random.default_rng(42)
    X_tr = rng.normal(size=(50, 3))
    y_tr = (rng.uniform(size=50) > 0.5).astype(int)
    X_dev = rng.normal(size=(20, 3))
    y_dev = (rng.uniform(size=20) > 0.5).astype(int)

    train_bin = tmp_path / "tr.bin"
    dev_bin = tmp_path / "dev.bin"
    ds_tr = lgb.Dataset(X_tr, label=y_tr, free_raw_data=False).construct()
    ds_tr.save_binary(str(train_bin))
    ds_dev = lgb.Dataset(
        X_dev, label=y_dev, reference=ds_tr, free_raw_data=False
    ).construct()
    ds_dev.save_binary(str(dev_bin))

    loaded_tr = lgb.Dataset(str(train_bin))
    loaded_dev = lgb.Dataset(str(dev_bin), reference=loaded_tr)

    adapter = LightGBMAdapter()
    adapter.train(
        X_train=None, y_train=None, X_val=None, y_val=None,
        params={
            "objective": "binary",
            "verbose": -1,
            "num_iterations": 5,
            "early_stopping_rounds": 3,
        },
        train_dataset=loaded_tr,
        val_dataset=loaded_dev,
    )

    assert adapter.booster is not None
    assert adapter.booster.num_trees() > 0

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

    def test_feature_importance_split_and_gain(self, tiny_data, train_params):
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

        split = adapter.feature_importance(kind="split")
        gain = adapter.feature_importance(kind="gain")
        assert set(split) == set(gain)
        assert all(isinstance(v, float) for v in split.values())
        assert all(isinstance(v, float) for v in gain.values())
        # default kind is "split" (backward compatible)
        assert adapter.feature_importance() == split

    def test_booster_property(self, tiny_data, train_params):
        import lightgbm as lgb
        adapter = LightGBMAdapter()
        X_train, y_train, X_val, y_val = tiny_data
        adapter.train(X_train, y_train, X_val, y_val, train_params.copy())
        assert isinstance(adapter.booster, lgb.Booster)

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

    assert (cache_dir / "lgb" / "binary" / "train.bin").exists()
    assert (cache_dir / "lgb" / "binary" / "train_dev.bin").exists()
    assert (cache_dir / "lgb" / "binary" / "_SUCCESS").exists()
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
    assert (cache_dir / "lgb" / "binary" / "_SUCCESS").exists()

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
    (cache_dir / "lgb" / "binary" / "_SUCCESS").unlink()

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    assert (cache_dir / "lgb" / "binary" / "_SUCCESS").exists()
    assert (cache_dir / "lgb" / "binary" / "train.bin").exists()


def test_lightgbm_train_uses_log_period_from_params(monkeypatch, tiny_data):
    """`log_period` in params controls lgb.log_evaluation(period=...) and is
    popped before lgb.train (LightGBM warns on unknown params otherwise)."""
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    captured_periods: list[int] = []
    real_log_evaluation = lgb.log_evaluation

    def spy_log_evaluation(period=0, *args, **kwargs):
        captured_periods.append(period)
        return real_log_evaluation(period=period, *args, **kwargs)

    monkeypatch.setattr(lgb, "log_evaluation", spy_log_evaluation)

    X_train, y_train, X_val, y_val = tiny_data
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "num_leaves": 4,
        "seed": 42,
        "num_iterations": 5,
        "early_stopping_rounds": 3,
        "log_period": 2,
    }
    adapter = LightGBMAdapter()
    adapter.train(X_train, y_train, X_val, y_val, params)

    assert captured_periods == [2], (
        f"expected log_evaluation called once with period=2, got {captured_periods}"
    )
    # log_period must be popped — LightGBM rejects unknown params silently but
    # the booster's saved params would otherwise carry it.
    assert "log_period" not in adapter.booster.params


def test_lightgbm_train_default_log_period_silent(monkeypatch, tiny_data, train_params):
    """Without `log_period` in params, default is 0 (silent) — preserves
    existing behavior."""
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    captured_periods: list[int] = []
    real_log_evaluation = lgb.log_evaluation

    def spy_log_evaluation(period=0, *args, **kwargs):
        captured_periods.append(period)
        return real_log_evaluation(period=period, *args, **kwargs)

    monkeypatch.setattr(lgb, "log_evaluation", spy_log_evaluation)

    X_train, y_train, X_val, y_val = tiny_data
    adapter = LightGBMAdapter()
    adapter.train(X_train, y_train, X_val, y_val, train_params.copy())

    assert captured_periods == [0]


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


def test_lightgbm_prepare_passes_categorical_feature(tmp_path, monkeypatch):
    """prepare_train_inputs sets categorical_feature on lgb.Dataset."""
    import lightgbm as lgb
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df.to_parquet(train_dir)
    df.to_parquet(dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],  # prod_name index = 1
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    # Spy on lgb.Dataset.__init__ to capture categorical_feature args passed
    # during construction. lgb binary format does not persist categorical_feature,
    # so we must verify the argument at build time rather than after loading.
    captured_cat_features = []
    real_init = lgb.Dataset.__init__

    def spy_init(self, data, *args, **kwargs):
        cf = kwargs.get("categorical_feature", "auto")
        if cf != "auto" and not isinstance(data, str):
            # Only record non-binary-load calls (binary load passes a file path str)
            captured_cat_features.append(cf)
        real_init(self, data, *args, **kwargs)

    monkeypatch.setattr(lgb.Dataset, "__init__", spy_init)

    adapter = LightGBMAdapter()
    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)),
        ParquetHandle(str(dev_dir)),
        prep_meta,
        parameters,
        str(tmp_path / "cache"),
    )

    # Both train and dev datasets should have been built with categorical_feature=[1]
    # (prod_name is at index 1 in feature_columns=["feat_a", "prod_name"])
    assert len(captured_cat_features) == 2, (
        f"Expected 2 lgb.Dataset builds with categorical_feature, got: {captured_cat_features}"
    )
    for cat_attr in captured_cat_features:
        # lgb may store as list[int] (indexes) or list[str] (column names like "Column_1")
        assert cat_attr in ([1], ["prod_name"], ["Column_1"]), (
            f"Unexpected categorical_feature value: {cat_attr}"
        )


def _ranking_parameters(objective):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"algorithm_params": {"objective": objective}},
    }


def _ranking_frames():
    import pandas as pd
    # 3 customers x 2 products on one snap_date => 3 query groups of size 2
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "label": [1, 0, 0, 1, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c4", "c5", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["fund", "ccard"] * 2,
        "feat_a": [1.5, 2.5, 3.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def test_prepare_train_inputs_binary_family_subpath(tmp_path):
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("binary"), str(cache),
    )
    assert (cache / "lgb" / "binary" / "_SUCCESS").exists()
    assert not (cache / "lgb" / "ranking").exists()
    ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
    assert ds.get_group() is None  # binary path: no group set


def test_prepare_train_inputs_ranking_sets_group(tmp_path):
    import lightgbm as lgb
    import numpy as np
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    train_h, dev_h = LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("lambdarank"), str(cache),
    )
    assert (cache / "lgb" / "ranking" / "_SUCCESS").exists()
    assert "ranking" in train_h.bin_path and train_h.role == "train"
    assert "ranking" in dev_h.bin_path and dev_h.role == "train_dev"

    ds_tr = lgb.Dataset(train_h.bin_path).construct()
    g_tr = ds_tr.get_group()
    assert g_tr is not None
    np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2, 2]))
    assert int(np.sum(g_tr)) == 6  # all train rows covered

    ds_dv = lgb.Dataset(dev_h.bin_path, reference=ds_tr).construct()
    g_dv = ds_dv.get_group()
    np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
    assert int(np.sum(g_dv)) == 4


def test_prepare_train_inputs_both_families_coexist(tmp_path):
    """Switching objective rebuilds in its own sub-path; never reuses the
    other family's binary."""
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    a = LightGBMAdapter()
    a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                           prep_meta, _ranking_parameters("binary"), str(cache))
    a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                           prep_meta, _ranking_parameters("lambdarank"), str(cache))
    assert (cache / "lgb" / "binary" / "_SUCCESS").exists()
    assert (cache / "lgb" / "ranking" / "_SUCCESS").exists()


def _weight_frames():
    import pandas as pd
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["a", "b"] * 3,
        "cust_segment_typ": ["mass"] * 6,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "label": [1, 0, 0, 1, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c4", "c5", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["a", "b"] * 2,
        "cust_segment_typ": ["mass"] * 4,
        "feat_a": [1.5, 2.5, 3.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def _weight_params(objective):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {
            "algorithm_params": {"objective": objective},
            "sample_weights": {"mass|a": 3.0}},
    }


def test_prepare_train_inputs_binary_bin_carries_feature_names(tmp_path):
    """The cached .bin persists real feature names from feature_columns, so a
    booster trained on it (the hpo_best final-model path) reports real names —
    not LightGBM's positional Column_N defaults."""
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("binary"), str(cache),
    )
    ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
    assert ds.get_feature_name() == ["feat_a", "prod_name"]


def test_prepare_train_inputs_ranking_bin_carries_feature_names(tmp_path):
    """Ranking branch: both train and train_dev .bin carry real feature names."""
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    train_h, dev_h = LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("lambdarank"), str(cache),
    )
    ds_tr = lgb.Dataset(train_h.bin_path).construct()
    ds_dv = lgb.Dataset(dev_h.bin_path).construct()
    assert ds_tr.get_feature_name() == ["feat_a", "prod_name"]
    assert ds_dv.get_feature_name() == ["feat_a", "prod_name"]


def test_feature_selection_subpath_empty_when_no_selection():
    """No selection -> empty sub-segment, so the lgb cache path stays
    `lgb/<family>/` byte-identical to pre-feature-selection behavior."""
    from recsys_tfb.models.lightgbm_adapter import _feature_selection_subpath

    assert _feature_selection_subpath({"training": {}}, ["a", "b"]) == ""
    assert _feature_selection_subpath(
        {"training": {"feature_selection": {"exclude": []}}}, ["a", "b"]
    ) == ""


def test_feature_selection_subpath_hashes_surviving_features_when_active():
    """Active selection -> `fs_<hash8>` keyed by the surviving feature set, so
    different subsets get different .bin dirs (no stale-bin collision)."""
    from recsys_tfb.models.lightgbm_adapter import _feature_selection_subpath

    p = {"training": {"feature_selection": {"exclude": ["x"]}}}
    s1 = _feature_selection_subpath(p, ["a", "b"])
    s2 = _feature_selection_subpath(p, ["a"])
    assert s1.startswith("fs_") and len(s1) == 11
    assert s1 != s2


def test_feature_selection_isolates_bin_from_full_feature_cache(tmp_path):
    """A subset model (feature_selection active) must build its .bin under a
    feature-hash subdir, leaving the full-feature binary that shares the same
    base/train_variant cache dir untouched — and its bin reflects the subset."""
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    cache = tmp_path / "variant"

    # full-feature run (no selection) -> canonical path
    full_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        full_meta, _ranking_parameters("binary"), str(cache),
    )
    full_bin = cache / "lgb" / "binary" / "train.bin"
    assert full_bin.exists()

    # subset run (exclude feat_a) -> separate fs_ subdir
    subset_meta = {
        "feature_columns": ["prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    params = _ranking_parameters("binary")
    params["training"]["feature_selection"] = {"exclude": ["feat_a"]}
    train_h, _ = LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        subset_meta, params, str(cache),
    )
    assert "/lgb/binary/fs_" in train_h.bin_path.replace("\\", "/")
    # full-feature bin untouched (no overwrite / collision)
    assert lgb.Dataset(str(full_bin)).construct().get_feature_name() == [
        "feat_a", "prod_name"]
    # subset bin carries only the kept feature
    assert lgb.Dataset(train_h.bin_path).construct().get_feature_name() == [
        "prod_name"]


class TestPrepareTrainInputsWeight:
    def _prep(self):
        return {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
        }

    def test_binary_branch_bakes_weight_into_binary(self, tmp_path):
        import lightgbm as lgb
        import numpy as np
        from recsys_tfb.io.handles import ParquetHandle
        df_tr, df_dev = _weight_frames()
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        df_tr.to_parquet(tr); df_dev.to_parquet(dv)
        cache = tmp_path / "variant"
        LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            self._prep(), _weight_params("binary"), str(cache))
        ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
        w = ds.get_weight()
        assert w is not None
        assert sorted(set(np.round(w, 3))) == [1.0, 3.0]

    def test_ranking_branch_bakes_weight_aligned_with_perm(self, tmp_path):
        import lightgbm as lgb
        import numpy as np
        from recsys_tfb.io.handles import ParquetHandle
        df_tr, df_dev = _weight_frames()
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        df_tr.to_parquet(tr); df_dev.to_parquet(dv)
        cache = tmp_path / "variant"
        LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            self._prep(), _weight_params("lambdarank"), str(cache))
        ds = lgb.Dataset(str(cache / "lgb" / "ranking" / "train.bin")).construct()
        w = ds.get_weight()
        assert w is not None
        assert sorted(set(np.round(w, 3))) == [1.0, 3.0]

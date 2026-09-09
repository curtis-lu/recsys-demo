"""Tests for ModelAdapter ABC, LightGBMAdapter, and adapter registry."""

import numpy as np
import pytest

from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter



def _write_model_input(df, path, **kwargs):
    """Write a fixture frame the way ``build_model_input`` writes a real one.

    Since #283 every numeric feature column is cast to
    ``dataset.numeric_feature_storage_type`` (default float32), so a fixture
    that leaves them float64 is not a shape the training read can ever meet —
    invariant B9 rejects it before reading a row. Casting here rather than in
    each frame literal keeps the fixtures readable and keeps "what a
    model_input looks like" in one place.
    """
    import pandas as pd

    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].astype("float32")
    out.to_parquet(path, **kwargs)


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
    _write_model_input(df_tr, train_dir, engine="pyarrow")
    _write_model_input(df_dev, dev_dir, engine="pyarrow")

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name", "label": "label"}}}

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
    _write_model_input(df_tr, train_dir)
    _write_model_input(df_dev, dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name", "label": "label"}}}
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
    _write_model_input(df, train_dir)
    _write_model_input(df, dev_dir)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name", "label": "label"}}}
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
    _write_model_input(df, train_dir)
    _write_model_input(df, dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],  # prod_name index = 1
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name", "label": "label"}}}

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


def test_prepare_train_inputs_binary_objective_subpath(tmp_path):
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
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
    # Unchanged from before the per-objective split: a binary objective still
    # lands in lgb/binary/, so existing caches stay valid.
    assert (cache / "lgb" / "binary" / "_SUCCESS").exists()
    assert not (cache / "lgb" / "lambdarank").exists()
    assert not (cache / "lgb" / "rank_xendcg").exists()
    ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
    assert ds.get_group() is None  # binary path: no group set


def test_prepare_train_inputs_ranking_sets_group(tmp_path):
    import lightgbm as lgb
    import numpy as np
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
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
    assert (cache / "lgb" / "lambdarank" / "_SUCCESS").exists()
    assert "lambdarank" in train_h.bin_path and train_h.role == "train"
    assert "lambdarank" in dev_h.bin_path and dev_h.role == "train_dev"

    ds_tr = lgb.Dataset(train_h.bin_path).construct()
    g_tr = ds_tr.get_group()
    assert g_tr is not None
    np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2, 2]))
    assert int(np.sum(g_tr)) == 6  # all train rows covered

    ds_dv = lgb.Dataset(dev_h.bin_path, reference=ds_tr).construct()
    g_dv = ds_dv.get_group()
    np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
    assert int(np.sum(g_dv)) == 4


def test_prepare_train_inputs_three_objectives_coexist(tmp_path):
    """All three objectives keep their own dir under one cache root.

    lambdarank and rank_xendcg used to share a single "ranking" dir; their
    .bin are identical today, so this pins the isolation ahead of the
    follow-up that makes their training matrices differ.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    a = LightGBMAdapter()
    for obj in ("binary", "lambdarank", "rank_xendcg"):
        a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                               prep_meta, _ranking_parameters(obj), str(cache))
        assert (cache / "lgb" / obj / "_SUCCESS").exists()
        assert (cache / "lgb" / obj / "train.bin").exists()
    assert not (cache / "lgb" / "ranking").exists()


@pytest.mark.parametrize(
    "first,second",
    [("lambdarank", "rank_xendcg"), ("rank_xendcg", "lambdarank")],
)
def test_ranking_objective_switch_never_hits_the_other_bin(
    tmp_path, caplog, first, second
):
    """Either switch order: the second objective BUILDS, it does not hit.

    Both orders matter — a key that folds the two onto one segment is
    order-blind, so testing one direction only would still pass under half
    the regression.
    """
    import logging

    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    a = LightGBMAdapter()
    a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                           prep_meta, _ranking_parameters(first), str(cache))

    # The evidence is the absence of the cache-hit log line, so it is coupled
    # to the exact wording of `logger.info("lgb binary cache hit at %s", ...)`
    # in LightGBMAdapter.prepare_train_inputs — reword that log and this
    # assertion goes vacuously green. Keep the two in sync. (The stronger
    # check — the two .bin differing in row count — only becomes available
    # once lambdarank starts dropping zero-positive groups.)
    caplog.clear()
    with caplog.at_level(
        logging.INFO, logger="recsys_tfb.models.lightgbm_adapter"
    ):
        train_h, dev_h = a.prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            prep_meta, _ranking_parameters(second), str(cache),
        )
    assert "cache hit" not in caplog.text
    assert second in train_h.bin_path
    assert second in dev_h.bin_path
    assert (cache / "lgb" / first / "_SUCCESS").exists()
    assert (cache / "lgb" / second / "_SUCCESS").exists()


def _zero_positive_frames():
    """Ranking fixture in which some query groups hold no positive at all.

    train: 4 customers x 2 products on one snap_date -> 4 groups / 8 rows,
    of which c3 and c4 are all-negative.
    train_dev: 3 customers x 2 products -> 3 groups / 6 rows, of which d3 is
    all-negative. The two splits drop a *different* number of groups so a
    filter wired to only one of them cannot pass by coincidence.
    """
    import pandas as pd
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3", "c4", "c4"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 8),
        "prod_name": ["fund", "ccard"] * 4,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        "label": [1, 0, 0, 1, 0, 0, 0, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["d1", "d1", "d2", "d2", "d3", "d3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        "feat_a": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
        "label": [0, 1, 1, 0, 0, 0],
    })
    return df_tr, df_dev


def _ranking_prep_meta():
    return {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


def _prepare_zero_positive(tmp_path, objective, cache_name="variant"):
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _zero_positive_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
    cache = tmp_path / cache_name
    handles = LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        _ranking_prep_meta(), _ranking_parameters(objective), str(cache),
    )
    return cache, handles


def _bin_groups(path, reference=None):
    import lightgbm as lgb
    return lgb.Dataset(str(path), reference=reference).construct()


class TestZeroPositiveGroupFilter:
    """lambdarank trains only on query groups holding a positive; nothing else does.

    The assertions read the built ``.bin`` back — the group vector and the row
    count it covers — rather than any intermediate the adapter computed, so a
    filter that ran but never reached the Dataset would still fail them.
    """

    def test_lambdarank_drops_the_all_negative_groups(self, tmp_path):
        import numpy as np
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "lambdarank")

        ds_tr = _bin_groups(train_h.bin_path)
        g_tr = ds_tr.get_group()
        # 4 groups / 8 rows in, c3 and c4 all-negative -> 2 groups / 4 rows.
        np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2]))
        assert int(np.sum(g_tr)) == 4
        assert ds_tr.num_data() == 4

        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        g_dv = ds_dv.get_group()
        # 3 groups / 6 rows in, d3 all-negative -> 2 groups / 4 rows.
        np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
        assert int(np.sum(g_dv)) == 4
        assert ds_dv.num_data() == 4

        # Every surviving row's label vector still has a positive per group.
        assert int(np.sum(ds_tr.get_label())) == 2
        assert int(np.sum(ds_dv.get_label())) == 2

    def test_rank_xendcg_keeps_every_group(self, tmp_path):
        import numpy as np
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "rank_xendcg")

        ds_tr = _bin_groups(train_h.bin_path)
        np.testing.assert_array_equal(
            np.sort(ds_tr.get_group()), np.array([2, 2, 2, 2])
        )
        assert ds_tr.num_data() == 8

        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        np.testing.assert_array_equal(
            np.sort(ds_dv.get_group()), np.array([2, 2, 2])
        )
        assert ds_dv.num_data() == 6

    def test_binary_keeps_every_row_and_sets_no_group(self, tmp_path):
        """Regression guard: the non-ranking path is untouched by all of this."""
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "binary")

        ds_tr = _bin_groups(train_h.bin_path)
        assert ds_tr.get_group() is None
        assert ds_tr.num_data() == 8
        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        assert ds_dv.get_group() is None
        assert ds_dv.num_data() == 6

    def test_lambdarank_bin_holds_strictly_fewer_rows_than_rank_xendcg(
        self, tmp_path
    ):
        """The cache split is real, not just two directory names.

        Three objectives under one cache root, same input. Row counts differing
        is what proves the second objective built its own ``.bin`` instead of
        being served the first one's — "all three dirs exist" would pass on the
        directory rename alone.
        """
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        df_tr, df_dev = _zero_positive_frames()
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
        cache = tmp_path / "variant"
        adapter = LightGBMAdapter()
        rows = {}
        for obj in ("lambdarank", "rank_xendcg", "binary"):
            train_h, _ = adapter.prepare_train_inputs(
                ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                _ranking_prep_meta(), _ranking_parameters(obj), str(cache),
            )
            rows[obj] = _bin_groups(train_h.bin_path).num_data()
        assert rows["lambdarank"] < rows["rank_xendcg"]
        assert rows["rank_xendcg"] == rows["binary"] == 8

    def test_weights_follow_the_surviving_rows(self, tmp_path):
        """Weights are filtered with their rows, not left behind to misalign.

        c3/c4 are the dropped groups and carry the ``3.0`` weight on their
        ``ccard`` row, so a weight vector sliced by a different mask (or not at
        all) shows up as the wrong multiset here.
        """
        import numpy as np
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        df_tr, df_dev = _zero_positive_frames()
        # Weight only the rows of groups that survive (c1/c2's "fund" row) and
        # of groups that do not (c3/c4's "fund" row) -- same weight, different
        # fate, so the surviving multiset pins which rows were kept.
        df_tr["cust_segment_typ"] = ["keep", "keep", "keep", "keep",
                                     "drop", "drop", "drop", "drop"]
        df_dev["cust_segment_typ"] = ["keep", "keep", "keep", "keep",
                                      "drop", "drop"]
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
        params = _ranking_parameters("lambdarank")
        params["training"]["sample_weights"] = {"keep|fund": 5.0,
                                                "drop|fund": 7.0}
        params["training"]["sample_weight_keys"] = ["cust_segment_typ",
                                                    "prod_name"]
        cache = tmp_path / "variant"
        train_h, dev_h = LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            _ranking_prep_meta(), params, str(cache),
        )
        # Read off the handle, not the .bin: since #318 the binary carries no
        # weights and the key columns beside it are what the filter had to
        # keep aligned. Same question, same failure mode.
        w_tr = train_h.sample_weights(params, _ranking_prep_meta())
        # 4 surviving rows: c1/c2's fund row weighted 5.0, their ccard row 1.0.
        # 7.0 appears nowhere -- every row carrying it was in a dropped group.
        assert sorted(np.round(w_tr, 3).tolist()) == [1.0, 1.0, 5.0, 5.0]

    def test_filter_counts_reach_the_log(self, tmp_path, caplog):
        """What was dropped is visible without opening the ``.bin``."""
        import logging
        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.models.lightgbm_adapter"
        ):
            _prepare_zero_positive(tmp_path, "lambdarank")
        text = caplog.text
        assert "zero-positive" in text
        # train: 2 of 4 groups and 4 of 8 rows dropped, 2 groups left.
        assert "train" in text
        assert "2/4 groups" in text and "4/8 rows" in text
        # train_dev: 1 of 3 groups and 2 of 6 rows dropped, 2 groups left.
        assert "1/3 groups" in text and "2/6 rows" in text

    def test_counts_sidecar_survives_a_cache_hit(self, tmp_path):
        """The numbers outlive the build, so a cache-hit run still reports them.

        The filter runs while the ``.bin`` is built. A second run hits the
        cache and extracts nothing, but its manifest still has to describe the
        matrix it trains on -- so the counts are persisted next to the binary
        and read back through the handle.
        """
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        cache, (train_h, _) = _prepare_zero_positive(tmp_path, "lambdarank")
        built = train_h.group_filter_counts()
        assert built["objective"] == "lambdarank"
        assert built["train"] == {
            "groups_total": 4, "groups_kept": 2, "groups_dropped": 2,
            "rows_total": 8, "rows_kept": 4, "rows_dropped": 4,
        }
        assert built["train_dev"] == {
            "groups_total": 3, "groups_kept": 2, "groups_dropped": 1,
            "rows_total": 6, "rows_kept": 4, "rows_dropped": 2,
        }

        df_tr, df_dev = _zero_positive_frames()
        hit_h, _ = LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tmp_path / "tr.parquet")),
            ParquetHandle(str(tmp_path / "dv.parquet")),
            _ranking_prep_meta(), _ranking_parameters("lambdarank"), str(cache),
        )
        assert hit_h.group_filter_counts() == built

    @pytest.mark.parametrize("objective", ["rank_xendcg", "binary"])
    def test_no_sidecar_when_nothing_is_filtered(self, tmp_path, objective):
        """An unfiltered objective leaves its cache dir exactly as it was.

        Writing an empty report into ``lgb/binary/`` would be a change to the
        non-ranking artifact set for no gain; absence *is* the answer.
        """
        cache, (train_h, _) = _prepare_zero_positive(tmp_path, objective)
        assert train_h.group_filter_counts() is None
        assert not (cache / "lgb" / objective / "group_filter_counts.json").exists()

    def test_a_bin_built_before_the_filter_existed_is_rebuilt_not_served(
        self, tmp_path, caplog
    ):
        """A pre-#315 lambdarank cache holds every row. Serving it is wrong.

        #314 already gave lambdarank its own segment, so a .bin built between
        the two lands at exactly the path this run wants, with `_SUCCESS` set
        and every row still in it. Nothing else catches it: the lgb cache is
        not keyed by model_version, and `finalize_model`'s refit branch
        re-reads the parquet and *does* filter — so a hit here puts the search
        and the final model on different matrices under one set of
        hyperparameters.

        Staged the way it really arises: build under rank_xendcg (which keeps
        every row, exactly as lambdarank did before this change) and move the
        directory onto lambdarank's segment.
        """
        import logging
        import shutil

        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        df_tr, df_dev = _zero_positive_frames()
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
        cache = tmp_path / "variant"
        LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            _ranking_prep_meta(), _ranking_parameters("rank_xendcg"),
            str(cache),
        )
        shutil.move(str(cache / "lgb" / "rank_xendcg"),
                    str(cache / "lgb" / "lambdarank"))
        assert (cache / "lgb" / "lambdarank" / "_SUCCESS").exists()

        caplog.clear()
        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.models.lightgbm_adapter"
        ):
            train_h, _ = LightGBMAdapter().prepare_train_inputs(
                ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                _ranking_prep_meta(), _ranking_parameters("lambdarank"),
                str(cache),
            )
        assert "cache hit" not in caplog.text
        assert "predates the zero-positive group filter" in caplog.text
        # Rebuilt, so the rows are the filtered ones and the counts are there.
        assert _bin_groups(train_h.bin_path).num_data() == 4
        assert train_h.group_filter_counts()["train"]["rows_dropped"] == 4


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


def _interleaved_weight_frames():
    """Query groups whose rows are *not* contiguous in the parquet.

    ``_weight_frames`` writes each customer's two rows side by side, so
    ``to_contiguous_groups`` returns the identity permutation and a sidecar
    left unpermuted would still look right. Here the three customers are
    interleaved, so the permutation genuinely reorders rows and a
    weight-to-row misalignment shows up as different numbers rather than as
    nothing at all.

    Rows are (cust, prod): c1/a c2/a c3/a c1/b c2/b c3/b. Group ids by first
    appearance are [0,1,2,0,1,2], so the permutation is [0,3,1,4,2,5] and the
    binary's rows are c1/a c1/b c2/a c2/b c3/a c3/b — prod_name alternating
    a,b,a,b,a,b where the parquet had a,a,a,b,b,b.
    """
    import pandas as pd
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c2", "c3", "c1", "c2", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["a", "a", "a", "b", "b", "b"],
        "cust_segment_typ": ["mass"] * 6,
        "feat_a": [1.0, 3.0, 5.0, 2.0, 4.0, 6.0],
        # Per customer exactly one positive, so every group survives the
        # zero-positive filter and this fixture isolates the permutation.
        "label": [1, 0, 1, 0, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c5", "c4", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["a", "a", "b", "b"],
        "cust_segment_typ": ["mass"] * 4,
        "feat_a": [1.5, 3.5, 2.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def _weight_params(objective, sample_weights=None, weight_keys=None):
    """Parameters whose ``sample_weights`` table can actually match a row.

    ``sample_weight_keys`` has to be spelled out: it defaults to
    ``[schema.item]`` alone, and a two-part config key like ``"mass|a"`` then
    matches nothing — every row stays at weight 1.0, LightGBM stores no weight
    vector at all, and a test asserting on weights fails for a reason that has
    nothing to do with what it is testing. That is exactly how the two tests
    this file used to carry were red on main (known-pitfalls.md §5).
    """
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {
            "algorithm_params": {"objective": objective},
            "sample_weight_keys": (
                ["cust_segment_typ", "prod_name"] if weight_keys is None
                else weight_keys
            ),
            "sample_weights": (
                {"mass|a": 3.0} if sample_weights is None else sample_weights
            ),
        },
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
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
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
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
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
    `lgb/<objective>/` byte-identical to pre-feature-selection behavior."""
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
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
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


def test_feature_selection_subpath_nests_under_each_objective(tmp_path):
    """The two cache dimensions compose: the same feature subset under two
    ranking objectives gets two distinct dirs, neither overwriting the other."""
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
    cache = tmp_path / "variant"
    subset_meta = {
        "feature_columns": ["prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    paths = {}
    for obj in ("lambdarank", "rank_xendcg"):
        params = _ranking_parameters(obj)
        params["training"]["feature_selection"] = {"exclude": ["feat_a"]}
        train_h, _ = LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            subset_meta, params, str(cache),
        )
        p = train_h.bin_path.replace("\\", "/")
        assert f"/lgb/{obj}/fs_" in p
        paths[obj] = p
    assert paths["lambdarank"] != paths["rank_xendcg"]


class TestPrepareTrainInputsWeight:
    """Where sample weights live, and what keeps them answering today's config.

    They used to be baked into the ``.bin``. The cache path is
    base/train_variant/objective and mentions ``training.sample_weights``
    nowhere, so a changed weight table hit the same directory and trained on
    the previous run's weights with nothing raised (#318). What the binary
    carries now is nothing; what sits beside it is the *key columns*, and each
    run resolves its own table against them.
    """

    def _prep(self):
        return {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
        }

    def _build(self, tmp_path, objective, params=None, frames=None):
        """Build the cache once; return (train_handle, dev_handle, lgb_dir)."""
        from recsys_tfb.io.handles import ParquetHandle
        df_tr, df_dev = frames or _weight_frames()
        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        _write_model_input(df_tr, tr); _write_model_input(df_dev, dv)
        cache = tmp_path / "variant"
        handles = LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tr)), ParquetHandle(str(dv)),
            self._prep(), params or _weight_params(objective), str(cache))
        return handles[0], handles[1], cache / "lgb" / objective

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_bin_carries_no_weight(self, tmp_path, objective):
        """The whole point: nothing weight-shaped is frozen into the binary.

        ``get_weight()`` is ``None`` for an unweighted Dataset, so this is the
        direct read of "the .bin cannot carry a stale weight table".
        """
        import lightgbm as lgb
        self._build(tmp_path, objective)
        for name in ("train.bin", "train_dev.bin"):
            ds = lgb.Dataset(
                str(tmp_path / "variant" / "lgb" / objective / name)
            ).construct()
            assert ds.get_weight() is None

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_resolved_weights_match_the_configured_table(
        self, tmp_path, objective
    ):
        """Same numbers the old baked-in vector held, resolved on read."""
        import numpy as np
        train_h, dev_h, _ = self._build(tmp_path, objective)
        params = _weight_params(objective)
        for handle, n_rows in ((train_h, 6), (dev_h, 4)):
            w = handle.sample_weights(params, self._prep())
            assert len(w) == n_rows
            assert sorted(set(np.round(w, 3))) == [1.0, 3.0]

    def test_sidecar_rows_follow_the_binary_permutation(self, tmp_path):
        """Weights must be in the *binary's* row order, not the parquet's.

        The fixture interleaves query groups, so the ranking branch permutes
        rows into contiguous blocks: prod_name goes from a,a,a,b,b,b in the
        parquet to a,b,a,b,a,b in the binary, and the weights with it. A
        sidecar written before the permutation would give [3,3,3,1,1,1] —
        every weight on the wrong row, and no error anywhere.
        """
        import lightgbm as lgb
        import numpy as np
        train_h, _, lgb_dir = self._build(
            tmp_path, "lambdarank", frames=_interleaved_weight_frames())

        ds = lgb.Dataset(str(lgb_dir / "train.bin")).construct()
        # Labels prove the permutation actually happened: the parquet's
        # 1,0,1,0,1,0 becomes 1,0,0,1,1,0 once grouped by customer.
        assert list(ds.get_label()) == [1.0, 0.0, 0.0, 1.0, 1.0, 0.0]

        w = train_h.sample_weights(_weight_params("lambdarank"), self._prep())
        assert list(np.round(w, 3)) == [3.0, 1.0, 3.0, 1.0, 3.0, 1.0]

    def test_sidecar_rows_follow_the_zero_positive_filter(self, tmp_path):
        """Dropped query groups take their weight-key rows with them.

        lambdarank strips groups holding no positive (#315). The sidecar is
        cut by the same mask in the same place; a sidecar left at full length
        would silently shift every weight by however many rows were dropped
        ahead of it.
        """
        import numpy as np
        import pandas as pd
        # c1 has no positive at all -> its two rows are dropped; the surviving
        # rows are c2/a c2/b c3/a c3/b -> weights 3,1,3,1.
        df_tr = pd.DataFrame({
            "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "prod_name": ["a", "b"] * 3,
            "cust_segment_typ": ["mass"] * 6,
            "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "label": [0, 0, 1, 0, 0, 1],
        })
        _, df_dev = _weight_frames()
        train_h, _, _ = self._build(
            tmp_path, "lambdarank", frames=(df_tr, df_dev))

        assert train_h.group_filter_counts()["train"]["rows_dropped"] == 2
        w = train_h.sample_weights(_weight_params("lambdarank"), self._prep())
        assert list(np.round(w, 3)) == [3.0, 1.0, 3.0, 1.0]

    def test_empty_sample_weights_resolves_to_all_ones(self, tmp_path):
        """No table configured is the pre-existing behaviour, unchanged."""
        import numpy as np
        params = _weight_params("binary", sample_weights={})
        train_h, _, _ = self._build(tmp_path, "binary", params=params)
        w = train_h.sample_weights(params, self._prep())
        assert np.array_equal(w, np.ones(6))

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_changed_weight_table_reuses_the_binary(self, tmp_path, objective):
        """A new weight table costs a resolve, not a rebinning.

        The bug this replaces was the same cache hit answering with stale
        weights. Keeping the hit *and* getting fresh weights is the whole
        reason the keys are cached rather than the vector — so both halves are
        asserted here: same file, different numbers.
        """
        import numpy as np
        train_h, _, lgb_dir = self._build(tmp_path, objective)
        before = (lgb_dir / "train.bin").stat().st_mtime_ns

        heavier = _weight_params(objective, sample_weights={"mass|a": 7.0})
        train_h2, _, _ = self._build(tmp_path, objective, params=heavier)

        assert (lgb_dir / "train.bin").stat().st_mtime_ns == before
        w = train_h2.sample_weights(heavier, self._prep())
        assert sorted(set(np.round(w, 3))) == [1.0, 7.0]

    def test_missing_sidecar_forces_rebuild(self, tmp_path):
        """A pre-#318 cache dir has weights inside the .bin and no way to say so.

        Serving it trains on whichever weights that run was configured with.
        The sidecar's absence is the only evidence available, so it is what
        triggers the rebuild.
        """
        from pathlib import Path
        from recsys_tfb.io.handles import ParquetHandle, weight_keys_sidecar
        train_h, _, lgb_dir = self._build(tmp_path, "binary")
        Path(weight_keys_sidecar(str(lgb_dir / "train.bin"))).unlink()
        before = (lgb_dir / "train.bin").stat().st_mtime_ns

        LightGBMAdapter().prepare_train_inputs(
            ParquetHandle(str(tmp_path / "tr.parquet")),
            ParquetHandle(str(tmp_path / "dv.parquet")),
            self._prep(), _weight_params("binary"), str(tmp_path / "variant"))

        assert (lgb_dir / "train.bin").stat().st_mtime_ns != before
        assert Path(weight_keys_sidecar(str(lgb_dir / "train.bin"))).exists()

    def test_changed_weight_keys_forces_rebuild(self, tmp_path):
        """Different key *columns* cannot be answered from the cached ones.

        The resolver's "weight-key column absent" backstop returns all-ones
        gracefully, which here would mean a whole search trained unweighted
        and reported under a model_version that says otherwise.
        """
        import numpy as np
        train_h, _, lgb_dir = self._build(tmp_path, "binary")
        before = (lgb_dir / "train.bin").stat().st_mtime_ns

        by_item = _weight_params(
            "binary", sample_weights={"a": 5.0}, weight_keys=["prod_name"])
        train_h2, _, _ = self._build(tmp_path, "binary", params=by_item)

        assert (lgb_dir / "train.bin").stat().st_mtime_ns != before
        w = train_h2.sample_weights(by_item, self._prep())
        assert sorted(set(np.round(w, 3))) == [1.0, 5.0]

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_weight_keys_absent_from_model_input_stay_all_ones(
        self, tmp_path, objective
    ):
        """A weight-key column the parquet does not have degrades gracefully.

        This is a real config: production ``feature_table`` carries
        ``cust_segment_typ`` and the synthetic one does not, and A9a validates
        the config's *declarations*, not the parquet. Before the weights moved
        out of the .bin this path returned all-ones with an INACTIVE log line,
        and it still has to.

        The trap it guards is that the sidecar then has **no columns**, and a
        column-less parquet cannot carry a row count — pyarrow writes
        ``num_rows=0``. A length-zero weight vector is not merely wrong, it is
        *invisible*: ``set_weight`` maps any all-ones array to ``None`` and a
        length-zero array is vacuously all-ones, so LightGBM's own length check
        never runs and the search trains unweighted.
        """
        import numpy as np
        absent = _weight_params(
            objective, sample_weights={"vip": 9.0}, weight_keys=["no_such_col"])
        train_h, dev_h, _ = self._build(tmp_path, objective, params=absent)
        for handle, n_rows in ((train_h, 6), (dev_h, 4)):
            w = handle.sample_weights(absent, self._prep())
            assert np.array_equal(w, np.ones(n_rows))

    def test_sample_weights_without_a_sidecar_raises(self, tmp_path):
        """Never all-ones by accident.

        A handle whose sidecar is gone cannot be re-weighted, and all-ones is
        a plausible-looking answer that would train the whole search before
        anyone noticed.
        """
        from pathlib import Path
        from recsys_tfb.io.handles import weight_keys_sidecar
        train_h, _, lgb_dir = self._build(tmp_path, "binary")
        Path(weight_keys_sidecar(str(lgb_dir / "train.bin"))).unlink()
        with pytest.raises(FileNotFoundError, match="sample-weight key sidecar"):
            train_h.sample_weights(_weight_params("binary"), self._prep())

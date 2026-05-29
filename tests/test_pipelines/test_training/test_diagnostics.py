"""Unit tests for training diagnostics (pure-python, no Spark)."""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training import diagnostics as diag


@pytest.fixture
def fitted_adapter():
    rng = np.random.RandomState(0)
    X = rng.randn(120, 4)
    # feature 3 is pure noise → expected dead / low importance
    y = (X[:, 0] + X[:, 1] > 0).astype(float)
    adapter = LightGBMAdapter()
    params = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
              "num_leaves": 4, "seed": 0, "num_iterations": 20, "early_stopping_rounds": 0}
    adapter.train(X, y, None, None, params)
    return adapter


def test_compute_feature_importance_shape_and_dead(fitted_adapter):
    params = {"diagnostics": {"feature_importance": {"enabled": True}}}
    out = diag.compute_feature_importance(fitted_adapter, params)
    assert set(out) == {"ranked", "dead_features"}
    # ranked sorted by gain desc
    gains = [r["gain"] for r in out["ranked"]]
    assert gains == sorted(gains, reverse=True)
    # every ranked row has feature/split/gain
    assert all({"feature", "split", "gain"} <= set(r) for r in out["ranked"])
    # dead_features are exactly the split==0 ones
    dead = {r["feature"] for r in out["ranked"] if r["split"] == 0}
    assert set(out["dead_features"]) == dead


def test_compute_feature_importance_disabled(fitted_adapter):
    params = {"diagnostics": {"feature_importance": {"enabled": False}}}
    assert diag.compute_feature_importance(fitted_adapter, params) == {}


def _write_parquet(tmp_path, pdf) -> ParquetHandle:
    path = str(tmp_path / "feat.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path)
    return ParquetHandle(path=path)


def test_compute_feature_statistics(tmp_path):
    pdf = pd.DataFrame({
        "f_num": [1.0, 2.0, 3.0, None],     # null_rate 0.25
        "f_const": [5.0, 5.0, 5.0, 5.0],    # single_value
        "f_cat": ["a", "b", "a", "c"],      # n_distinct 3, non-numeric
    })
    handle = _write_parquet(tmp_path, pdf)
    preprocessor = {"feature_columns": ["f_num", "f_const", "f_cat"]}
    params = {"diagnostics": {"feature_stats": {"enabled": True, "high_null_threshold": 0.5}}}

    out = diag.compute_feature_statistics(handle, preprocessor, params)

    assert out["f_num"]["null_rate"] == 0.25
    assert out["f_num"]["n_distinct"] == 3
    assert out["f_num"]["high_null"] is False
    assert out["f_num"]["mean"] == pytest.approx(2.0)
    assert out["f_const"]["single_value"] is True
    assert out["f_cat"]["n_distinct"] == 3
    # 非數值欄不應有 mean
    assert "mean" not in out["f_cat"]


def test_compute_feature_statistics_sampling(tmp_path):
    pdf = pd.DataFrame({"f": list(range(100))})
    handle = _write_parquet(tmp_path, pdf)
    preprocessor = {"feature_columns": ["f"]}
    params = {"diagnostics": {"feature_stats": {"enabled": True, "sample_rows": 10}}}
    out = diag.compute_feature_statistics(handle, preprocessor, params)
    # 抽樣後仍回傳該特徵的統計（n_distinct 受抽樣上限約束）
    assert out["f"]["n_distinct"] <= 10

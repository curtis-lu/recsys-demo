"""Unit tests for training diagnostics (pure-python, no Spark)."""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shap as _shap_mod  # noqa: F401  (ensure dependency present)

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


@pytest.fixture
def shap_setup(tmp_path, monkeypatch):
    """小 test parquet（含 item/label/兩數值特徵，一個稀有 item）+ fitted adapter。

    省略 schema → get_schema 預設 item=prod_name, label=label，對上欄名。
    item 'rare' 僅 2 列（觸發 take-all / low_coverage）。
    """
    monkeypatch.chdir(tmp_path)  # diagnostics_dir 會寫到 ./data/...
    rng = np.random.RandomState(1)
    n = 200
    f0 = rng.randn(n)
    f1 = rng.randn(n)
    prod = np.array(["A"] * 99 + ["B"] * 99 + ["rare"] * 2)
    label = (f0 + f1 > 0).astype(int)
    pdf = pd.DataFrame({"f0": f0, "f1": f1, "prod_name": prod, "label": label})
    path = str(tmp_path / "test.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path)
    handle = ParquetHandle(path=path)

    adapter = LightGBMAdapter()
    aparams = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
               "num_leaves": 4, "seed": 1, "num_iterations": 15, "early_stopping_rounds": 0}
    adapter.train(np.c_[f0, f1], label.astype(float), None, None, aparams)

    preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": [], "category_mappings": {}}
    parameters = {
        "model_version": "testmv",
        "diagnostics": {"shap": {"enabled": True, "top_k": 2, "n_examples": 1,
                                 "min_rows_per_item": 30, "sample_rows": 150,
                                 "max_budget": 4000000}},
    }
    return adapter, handle, preprocessor, parameters


def test_shap_single_call_and_outputs(shap_setup, monkeypatch):
    adapter, handle, preprocessor, parameters = shap_setup

    import shap
    calls = {"n": 0}
    real_sv = shap.TreeExplainer.shap_values

    def counting_sv(self, X, *a, **k):
        calls["n"] += 1
        return real_sv(self, X, *a, **k)

    monkeypatch.setattr(shap.TreeExplainer, "shap_values", counting_sv)

    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)

    assert calls["n"] == 1                          # 單次計算
    assert set(out) >= {"global", "per_item", "examples"}
    assert len(out["global"]["top_features"]) == 2
    assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
               for r in out["global"]["top_features"])
    assert "rare" in out["per_item"]
    assert out["per_item"]["rare"]["low_coverage"] is True
    assert out["per_item"]["rare"]["n_sampled"] <= 2
    assert {"n_sampled", "n_positive", "score_min", "score_max", "score_mean", "low_coverage"} \
        <= set(out["per_item"]["rare"])
    assert {"high", "low"} <= set(out["examples"])
    items_in_examples = {e["item"] for e in out["examples"]["per_item_high"]}
    assert {"A", "B", "rare"} <= items_in_examples
    d = diag.diagnostics_dir(parameters)
    assert (d / "shap_summary.png").exists()


def test_shap_disabled(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["enabled"] = False
    assert diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters) == {}


def test_shap_budget_guard_reduces_sample(shap_setup, caplog):
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["max_budget"] = 1  # 強制觸發降抽樣
    with caplog.at_level("WARNING"):
        out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert out != {}
    assert any("budget" in r.getMessage().lower() for r in caplog.records)

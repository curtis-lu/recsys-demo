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
        "diagnostics": {"shap": {"enabled": True, "top_k": 2,
                                 "min_rows_per_item": 30, "sample_rows": 150,
                                 "max_budget": 4000000}},
    }
    return adapter, handle, preprocessor, parameters


def test_shap_single_call_and_outputs(shap_setup, monkeypatch):
    adapter, handle, preprocessor, parameters = shap_setup
    # 這裡驗證的是 sample A 的「單次 SHAP 供 global/per_item 兩用」;
    # 正例 profile 的第二次 SHAP(decoupled sample B)另由 positive_profile 測試覆蓋,
    # 故此關閉 profile_positive 以隔離 sample-A 的單次計算語意。
    parameters["diagnostics"]["shap"]["profile_positive"] = False

    import shap
    calls = {"n": 0}
    real_sv = shap.TreeExplainer.shap_values

    def counting_sv(self, X, *a, **k):
        calls["n"] += 1
        return real_sv(self, X, *a, **k)

    monkeypatch.setattr(shap.TreeExplainer, "shap_values", counting_sv)

    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)

    assert calls["n"] == 1                          # 單次計算
    assert set(out) >= {"global", "per_item", "item_idiosyncrasy"}
    assert "examples" not in out
    assert len(out["global"]["top_features"]) == 2
    assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
               for r in out["global"]["top_features"])
    assert "rare" in out["per_item"]
    assert out["per_item"]["rare"]["low_coverage"] is True
    assert out["per_item"]["rare"]["n_sampled"] <= 2
    assert {"n_sampled", "n_positive", "score_min", "score_max", "score_mean", "low_coverage"} \
        <= set(out["per_item"]["rare"])
    from recsys_tfb.pipelines.training.diagnostics.paths import summary_dir
    assert (summary_dir(parameters) / "shap_summary_global.png").exists()


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


def test_per_item_top_features_signed(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    for blk in out["per_item"].values():
        assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
                   for r in blk["top_features"])


def test_per_item_profile_positive_and_coverage(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["positive_min_rows"] = 5
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    for blk in out["per_item"].values():
        assert "top_features_positive" in blk
        assert "positive_low_coverage" in blk
        if blk["n_positive"] >= 5:
            assert blk["top_features_positive"] is not None
            assert all("mean_signed_shap" in r for r in blk["top_features_positive"])
            assert blk["positive_low_coverage"] is False
        else:
            assert blk["top_features_positive"] is None
            assert blk["positive_low_coverage"] is True


def test_divergence_identical_is_zero():
    from recsys_tfb.pipelines.training.diagnostics.shap_per_item import _divergence
    import numpy as np
    v = np.array([3.0, 1.0, 2.0, 0.5])
    div, idio = _divergence(v, v, "jaccard_topk", 2, ["a", "b", "c", "d"])
    assert div == 0.0
    assert idio == []


def test_divergence_disjoint_top_is_one():
    from recsys_tfb.pipelines.training.diagnostics.shap_per_item import _divergence
    import numpy as np
    item = np.array([0.0, 0.0, 5.0, 4.0])   # top2 = idx {2,3}
    glob = np.array([5.0, 4.0, 0.0, 0.0])   # top2 = idx {0,1}
    div, idio = _divergence(item, glob, "jaccard_topk", 2, ["a", "b", "c", "d"])
    assert div == 1.0
    assert set(idio) == {"c", "d"}


def test_divergence_identical_spearman_is_zero():
    from recsys_tfb.pipelines.training.diagnostics.shap_per_item import _divergence
    import numpy as np
    v = np.array([3.0, 1.0, 2.0, 0.5])
    div, _ = _divergence(v, v, "spearman", 2, ["a", "b", "c", "d"])
    assert div == pytest.approx(0.0)


def test_divergence_reversed_spearman_is_one():
    from recsys_tfb.pipelines.training.diagnostics.shap_per_item import _divergence
    import numpy as np
    va = np.array([1.0, 2.0, 3.0, 4.0])
    vb = np.array([4.0, 3.0, 2.0, 1.0])
    div, _ = _divergence(va, vb, "spearman", 2, ["a", "b", "c", "d"])
    assert div == pytest.approx(1.0)


def test_per_item_divergence_and_idiosyncrasy(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    for blk in out["per_item"].values():
        assert 0.0 <= blk["divergence_from_global"] <= 1.0
        assert isinstance(blk["idiosyncratic_features"], list)
    idio = out["item_idiosyncrasy"]
    divs = [r["divergence_from_global"] for r in idio]
    assert divs == sorted(divs, reverse=True)
    assert {r["item"] for r in idio} == set(out["per_item"])


def test_per_item_signed_can_be_negative(shap_setup):
    # carry-over guard: mean_signed_shap must be the SIGNED mean, not abs.
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    found = any(
        r["mean_signed_shap"] is not None and r["mean_signed_shap"] < r["mean_abs_shap"]
        for blk in out["per_item"].values() for r in blk["top_features"]
    )
    assert found


def test_summary_pngs_global_and_per_item(shap_setup):
    from recsys_tfb.pipelines.training.diagnostics.paths import (
        per_item_summary_dir, safe_name, summary_dir)
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert (summary_dir(parameters) / "shap_summary_global.png").exists()
    pidir = per_item_summary_dir(parameters)
    for item in out["per_item"]:
        assert (pidir / f"shap_summary__{safe_name(item)}.png").exists()
    # 舊命名不應再產出
    assert not (diag.diagnostics_dir(parameters) / "waterfall_high_0.png").exists()


def test_per_item_beeswarm_can_be_disabled(shap_setup):
    import os
    from recsys_tfb.pipelines.training.diagnostics.paths import (
        per_item_summary_dir, summary_dir)
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["per_item_beeswarm"] = False
    diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert (summary_dir(parameters) / "shap_summary_global.png").exists()
    pidir = per_item_summary_dir(parameters)
    assert len(os.listdir(pidir)) == 0


def test_profile_positive_can_be_disabled(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["profile_positive"] = False
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    for blk in out["per_item"].values():
        assert blk["top_features_positive"] is None
        assert blk["positive_low_coverage"] is False


def test_shap_plot_failure_does_not_abort(shap_setup, monkeypatch):
    import shap
    adapter, handle, preprocessor, parameters = shap_setup
    # force EVERY summary_plot to raise; diagnostics must still return a dict.
    def boom(*a, **k):
        raise RuntimeError("plot exploded")
    monkeypatch.setattr(shap, "summary_plot", boom)
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert isinstance(out, dict)
    assert "per_item" in out and len(out["per_item"]) >= 1


def test_divergence_integration_multifeature(tmp_path, monkeypatch):
    import numpy as np, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    monkeypatch.chdir(tmp_path)
    rng = np.random.RandomState(7)
    n = 240
    X4 = rng.randn(n, 4)
    prod = np.array((["A"] * (n // 3)) + (["B"] * (n // 3)) + (["C"] * (n - 2 * (n // 3))))
    # item-dependent signal so per-item |SHAP| rankings can differ across the 4 features
    label = np.where(prod == "A", (X4[:, 0] > 0),
             np.where(prod == "B", (X4[:, 2] > 0), (X4[:, 3] > 0))).astype(int)
    pdf = pd.DataFrame({"f0": X4[:, 0], "f1": X4[:, 1], "f2": X4[:, 2],
                        "f3": X4[:, 3], "prod_name": prod, "label": label})
    path = str(tmp_path / "t4.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path)
    handle = ParquetHandle(path=path)
    adapter = LightGBMAdapter()
    adapter.train(X4, label.astype(float), None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 8, "seed": 7, "num_iterations": 30, "early_stopping_rounds": 0})
    preprocessor = {"feature_columns": ["f0", "f1", "f2", "f3"],
                    "categorical_columns": [], "category_mappings": {}}
    for metric in ("jaccard_topk", "spearman"):
        parameters = {"model_version": f"mv4_{metric}",
                      "diagnostics": {"shap": {"enabled": True, "top_k": 4,
                                               "min_rows_per_item": 10, "sample_rows": 240,
                                               "max_budget": 4000000,
                                               "divergence_metric": metric, "divergence_top_k": 2}}}
        out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
        for blk in out["per_item"].values():
            assert 0.0 <= blk["divergence_from_global"] <= 1.0
            assert set(blk["idiosyncratic_features"]) <= {"f0", "f1", "f2", "f3"}
        idio = out["item_idiosyncrasy"]
        divs = [r["divergence_from_global"] for r in idio]
        assert divs == sorted(divs, reverse=True)
        assert {r["item"] for r in idio} == set(out["per_item"])
        # non-degenerate: with 4 features and top_k=2, at least one item should diverge from global
        assert any(d > 0.0 for d in divs)


def test_feature_statistics_bounded_take(tmp_path, monkeypatch):
    import numpy as np
    import pandas as pd
    from recsys_tfb.pipelines.training.diagnostics import data_access

    n = 400
    rng = np.random.RandomState(0)
    pdf = pd.DataFrame({"f0": rng.randn(n), "f1": rng.randn(n)})
    path = str(tmp_path / "train.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path, row_group_size=50)
    handle = ParquetHandle(path=path)
    preprocessor = {"feature_columns": ["f0", "f1"]}
    parameters = {"diagnostics": {"feature_stats": {"enabled": True, "sample_rows": 100}}}

    seen = {}
    real_take = data_access.take_rows

    def spy_take(p, indices, columns):
        seen["n_indices"] = len(indices)
        return real_take(p, indices, columns)

    monkeypatch.setattr(data_access, "take_rows", spy_take)
    stats = diag.compute_feature_statistics(handle, preprocessor, parameters)

    # bounded: only sample_rows rows were taken, not the full 400
    assert seen["n_indices"] == 100
    assert set(stats) == {"f0", "f1"}
    assert "mean" in stats["f0"]


def test_shap_does_not_full_load_to_pandas(shap_setup, monkeypatch):
    # 重構意圖：SHAP 路徑不得再呼叫 ParquetHandle.to_pandas()（全量物化）。
    from recsys_tfb.io.handles import ParquetHandle

    def boom(self):
        raise AssertionError("compute_shap_diagnostics must not call to_pandas()")

    monkeypatch.setattr(ParquetHandle, "to_pandas", boom)
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert set(out) >= {"global", "per_item"}


def test_shap_on_hive_partitioned_cache(tmp_path):
    # prod_name 為分區欄時（生產 cache 佈局），需能從分區重建並正常產出。
    import numpy as np
    import pandas as pd
    import pyarrow.dataset as pads
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    rng = np.random.RandomState(7)
    n = 240
    prod = np.where(np.arange(n) % 2 == 0, "A", "B")
    X = rng.randn(n, 3)
    label = (X[:, 0] + (prod == "A") * 0.5 > 0).astype(int)
    pdf = pd.DataFrame({"f0": X[:, 0], "f1": X[:, 1], "f2": X[:, 2],
                        "prod_name": prod, "snap_date": "2024-01-31", "label": label})
    base = str(tmp_path / "parted")
    pads.write_dataset(
        __import__("pyarrow").Table.from_pandas(pdf), base, format="parquet",
        partitioning=["snap_date", "prod_name"], partitioning_flavor="hive",
    )
    handle = ParquetHandle(path=base)
    adapter = LightGBMAdapter()
    # prod_name is a declared categorical feature (feature_columns 含它)；_pdf_to_X 會把它
    # encode 成 code(A=0,B=1) 併入 X，故模型須以同樣 4 欄矩陣訓練（否則 predict 4!=3）。
    X_train = np.column_stack([X, (prod == "B").astype(float)])
    adapter.train(X_train, label.astype(float), None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 8, "seed": 7, "num_iterations": 20, "early_stopping_rounds": 0})
    preprocessor = {"feature_columns": ["f0", "f1", "f2", "prod_name"],
                    "categorical_columns": ["prod_name"],
                    "category_mappings": {"prod_name": ["A", "B"]}}
    parameters = {"model_version": "mvpart",
                  "schema": {"item": "prod_name", "label": "label"},
                  "diagnostics": {"shap": {"enabled": True, "top_k": 3,
                                           "min_rows_per_item": 10, "sample_rows": 120,
                                           "max_budget": 4000000}}}
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert set(out["per_item"]) == {"A", "B"}     # prod_name 從分區重建成功
    assert len(out["global"]["top_features"]) == 3


def test_positive_profile_covered_by_targeted_sampling(tmp_path, monkeypatch):
    # A 的正例夠多但辦卡率低:全域 item 分層樣本幾乎撈不到正例;針對正樣本抽後
    # top_features_positive 應為非 null(coverage 修好)。
    import numpy as np
    import pandas as pd
    rng = np.random.RandomState(0)
    n = 4000
    prod = np.where(np.arange(n) % 2 == 0, "A", "B")
    f0 = rng.randn(n)
    f1 = rng.randn(n)
    # 稀疏正例(~3%),但絕對數量夠(A 的正例 > positive_min_rows)
    label = (rng.rand(n) < 0.03).astype(int)
    pdf = pd.DataFrame({"f0": f0, "f1": f1, "prod_name": prod, "label": label})
    path = str(tmp_path / "test.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path, row_group_size=500)
    handle = ParquetHandle(path=path)

    adapter = LightGBMAdapter()
    adapter.train(np.c_[f0, f1], label.astype(float), None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 15, "early_stopping_rounds": 0})
    preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": [],
                    "category_mappings": {}}
    parameters = {"model_version": "mvpos",
                  "diagnostics": {"shap": {"enabled": True, "top_k": 2,
                                           "min_rows_per_item": 30, "sample_rows": 300,
                                           "max_budget": 4000000,
                                           "profile_positive": True,
                                           "positive_min_rows": 20,
                                           "positive_sample_per_item": 40}}}
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    # 至少一個 item 的正例 profile 因針對抽樣而有值
    assert any(out["per_item"][it]["top_features_positive"] is not None
               for it in out["per_item"])


def test_positive_profile_skipped_when_disabled(shap_setup, monkeypatch):
    # profile_positive=False → 不跑第二次 SHAP(不呼叫 attribution 於正例樣本)。
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["profile_positive"] = False

    import recsys_tfb.pipelines.training.diagnostics.shap_per_item as spi
    calls = {"n": 0}
    real = spi.feature_attributions

    def counting(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(spi, "feature_attributions", counting)
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert calls["n"] == 1                     # 只有 sample A 那一次
    for it in out["per_item"]:
        assert out["per_item"][it]["top_features_positive"] is None
        assert out["per_item"][it]["positive_low_coverage"] is False


def test_positive_profile_extra_pass_and_bounded(tmp_path, monkeypatch):
    # profile_positive on + 正例存在 → sample A + sample B = 恰好 2 次 SHAP;
    # 且 sample B 的 take_rows 只取 <= positive_sample_per_item * n_items 列(記憶體 bound,spec §6#5)。
    import numpy as np
    import pandas as pd
    from recsys_tfb.pipelines.training.diagnostics import data_access
    import recsys_tfb.pipelines.training.diagnostics.shap_per_item as spi

    rng = np.random.RandomState(0)
    n = 2000
    prod = np.where(np.arange(n) % 2 == 0, "A", "B")   # n_items = 2
    f0, f1 = rng.randn(n), rng.randn(n)
    label = (rng.rand(n) < 0.08).astype(int)           # 每 item 正例足夠
    pdf = pd.DataFrame({"f0": f0, "f1": f1, "prod_name": prod, "label": label})
    path = str(tmp_path / "t.parquet")
    pq.write_table(pa.Table.from_pandas(pdf), path, row_group_size=400)
    handle = ParquetHandle(path=path)
    adapter = LightGBMAdapter()
    adapter.train(np.c_[f0, f1], label.astype(float), None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 15, "early_stopping_rounds": 0})
    preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": [],
                    "category_mappings": {}}
    per_item = 40
    parameters = {"model_version": "mvx",
                  "diagnostics": {"shap": {"enabled": True, "top_k": 2,
                                           "min_rows_per_item": 30, "sample_rows": 300,
                                           "max_budget": 4000000, "profile_positive": True,
                                           "positive_min_rows": 20,
                                           "positive_sample_per_item": per_item}}}

    shap_calls = {"n": 0}
    real_attr = spi.feature_attributions

    def counting_attr(*a, **k):
        shap_calls["n"] += 1
        return real_attr(*a, **k)

    take_lens = []
    real_take = data_access.take_rows

    def spy_take(p, indices, columns):
        take_lens.append(len(indices))
        return real_take(p, indices, columns)

    monkeypatch.setattr(spi, "feature_attributions", counting_attr)
    monkeypatch.setattr(data_access, "take_rows", spy_take)

    diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)

    assert shap_calls["n"] == 2                 # sample A + sample B,恰好一次額外 pass
    assert len(take_lens) == 2                  # sample A take,再 sample B take
    assert take_lens[1] <= per_item * 2         # sample B bounded by per_item * n_items

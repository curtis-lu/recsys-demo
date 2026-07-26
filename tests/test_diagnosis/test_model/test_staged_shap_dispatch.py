"""Task 5:SHAP 三函式(compute_shap_diagnostics/compute_quadrant_profiles/
compute_quadrant_cases)的 staged 分派——透過 resolve_attribution_inputs
把 [X|stage1_score|partition_gcode] 餵給 SHAP,shared 路徑逐位元不變。"""
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys_tfb.diagnosis.model.staged import resolve_attribution_inputs
from recsys_tfb.io.handles import ParquetHandle


class _FakeStaged:
    """duck-typed staged 模型 stub:有 predict_routed/stage2_mode(is_staged/
    has_stage2 判斷依據),無 .predict——與真 StagedModelAdapter 契約一致。"""

    stage2_mode = "binary"
    partition_keys = ["grp"]
    booster = None  # budget guard 用不到(呼叫端已 monkeypatch attribution_budget_units)

    def predict_routed(self, X, keys, on_missing="raise"):
        return np.arange(len(X), dtype=float), np.ones(len(X), bool)

    def stage2_matrix_for(self, X, keys):
        s1 = np.arange(len(X), dtype=float)
        g = np.zeros(len(X))
        return np.column_stack([np.asarray(X, float), s1, g])


class TestResolveForStaged:
    """前提回歸鎖:Batch B 已實作的 helper,留著確認契約沒被本 task 改壞。"""

    def test_matrix_gets_two_extra_columns_and_names(self):
        pdf = pd.DataFrame({"grp": ["a", "a"], "f1": [0.0, 1.0]})
        X = pdf[["f1"]].to_numpy(float)
        X_eff, cols = resolve_attribution_inputs(_FakeStaged(), pdf, X, ["f1"])
        assert X_eff.shape == (2, 3)
        assert cols == ["f1", "stage1_score", "partition_gcode"]
        assert np.allclose(X_eff[:, 1], [0.0, 1.0])


class TestShapNodeUsesResolvedInputs:
    def test_shap_diagnostics_feeds_staged_matrix(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # diagnostics_dir 會寫到 ./data/...
        import recsys_tfb.diagnosis.model.shap_per_item as spi

        rng = np.random.RandomState(0)
        n = 40
        f0 = rng.randn(n)
        f1 = rng.randn(n)
        prod = np.array(["A"] * 20 + ["B"] * 20)
        grp = np.array(["g1"] * 20 + ["g2"] * 20)
        label = (f0 + f1 > 0).astype(int)
        pdf = pd.DataFrame({"f0": f0, "f1": f1, "prod_name": prod,
                            "grp": grp, "label": label})
        path = str(tmp_path / "test.parquet")
        pq.write_table(pa.Table.from_pandas(pdf), path)
        handle = ParquetHandle(path=path)

        model = _FakeStaged()

        seen = {}

        def spy_attr(m, X, feature_cols, **kwargs):
            seen["shape"] = X.shape
            seen["feature_cols"] = list(feature_cols)
            return np.zeros((X.shape[0], X.shape[1]))

        monkeypatch.setattr(spi, "feature_attributions", spy_attr)
        monkeypatch.setattr(spi, "attribution_budget_units", lambda m: 10)

        preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": [],
                        "category_mappings": {}}
        parameters = {
            "model_version": "testmv_staged_shap",
            "diagnostics": {"shap": {"enabled": True, "top_k": 2,
                                     "min_rows_per_item": 5, "sample_rows": 40,
                                     "max_budget": 4_000_000,
                                     "profile_positive": False}},
        }
        spi.compute_shap_diagnostics(model, handle, preprocessor, parameters)

        n_base = len(preprocessor["feature_columns"])
        assert seen["shape"][1] == n_base + 2
        assert seen["feature_cols"][-2:] == ["stage1_score", "partition_gcode"]

    def test_positive_profiles_not_double_resolved(self, tmp_path, monkeypatch):
        # 回歸鎖（controller 修復 2026-07-26）：主函式 resolve 後若把「已擴充」
        # 名單傳進 _positive_profiles，函式內再 resolve 會二次擴充——
        # n+4 名單對 n+2 矩陣。斷言：每一次 feature_attributions 呼叫的
        # 名單長度都等於矩陣欄數（全域 pass＋正例 pass 都要成立）。
        monkeypatch.chdir(tmp_path)
        import recsys_tfb.diagnosis.model.shap_per_item as spi

        rng = np.random.RandomState(0)
        n = 40
        f0 = rng.randn(n)
        f1 = rng.randn(n)
        prod = np.array(["A"] * 20 + ["B"] * 20)
        grp = np.array(["g1"] * 20 + ["g2"] * 20)
        label = (f0 + f1 > 0).astype(int)
        pdf = pd.DataFrame({"f0": f0, "f1": f1, "prod_name": prod,
                            "grp": grp, "label": label})
        path = str(tmp_path / "test.parquet")
        pq.write_table(pa.Table.from_pandas(pdf), path)
        handle = ParquetHandle(path=path)

        model = _FakeStaged()
        calls = []

        def spy_attr(m, X, feature_cols, **kwargs):
            calls.append((X.shape[1], list(feature_cols)))
            return np.zeros((X.shape[0], X.shape[1]))

        monkeypatch.setattr(spi, "feature_attributions", spy_attr)
        monkeypatch.setattr(spi, "attribution_budget_units", lambda m: 10)

        preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": [],
                        "category_mappings": {}}
        parameters = {
            "model_version": "testmv_staged_shap_pos",
            "diagnostics": {"shap": {"enabled": True, "top_k": 2,
                                     "min_rows_per_item": 5, "sample_rows": 40,
                                     "max_budget": 4_000_000,
                                     "profile_positive": True,
                                     "positive_min_rows": 3,
                                     "positive_sample_per_item": 10}},
        }
        spi.compute_shap_diagnostics(model, handle, preprocessor, parameters)

        assert len(calls) >= 2  # 全域一次＋正例一次
        for width, cols in calls:
            assert len(cols) == width
            assert cols[-2:] == ["stage1_score", "partition_gcode"]

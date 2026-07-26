"""stage2=none per-group diagnostics runner（Task 7，PR-C Batch B）。"""

import json

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.model.staged import compute_staged_group_diagnostics
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.partition import group_slug
from recsys_tfb.models.staged.train_stage1 import _fit_adapter


class _HandleStub:
    """簡單 handle stub：只需 .path（見 diagnostics/model/data_access.py 的
    ParquetHandle 使用慣例，這裡不必是真正的 ParquetHandle 類別）。"""

    def __init__(self, path):
        self.path = path


def _group_pdf(seed, n, grp_value):
    rng = np.random.RandomState(seed)
    f0 = rng.randn(n)
    f1 = rng.randn(n)
    y = (f0 + f1 > 0).astype(float)
    prod = np.where(np.arange(n) % 2 == 0, "P1", "P2")
    return pd.DataFrame({"f0": f0, "f1": f1, "grp": grp_value,
                         "prod_name": prod, "label": y})


_FIT_PARAMS = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
              "num_leaves": 7, "min_data_in_leaf": 5, "seed": 0,
              "num_iterations": 5, "early_stopping_rounds": 0}
_FEATURE_COLS = ["f0", "f1"]
# 兩群列數刻意不同（60 vs 80／30 vs 40）——mutation check 的前提。
_N_TRAIN = {"a": 60, "b": 80}
_N_TEST = {"a": 30, "b": 40}


def _build_setup(tmp_path):
    model = StagedModelAdapter()
    model.set_partition_keys(["grp"])
    train_frames, test_frames = [], []
    for i, key in enumerate(sorted(_N_TRAIN)):
        n_tr = _N_TRAIN[key]
        df_tr = _group_pdf(seed=10 + i, n=n_tr, grp_value=key)
        X_tr = df_tr[_FEATURE_COLS].to_numpy()
        y_tr = df_tr["label"].to_numpy()
        w_tr = np.ones(len(y_tr))
        n_dev = max(10, n_tr // 4)
        X_dev, y_dev = X_tr[:n_dev], y_tr[:n_dev]
        adapter = _fit_adapter(X_tr, y_tr, w_tr, X_dev, y_dev, dict(_FIT_PARAMS),
                               None, feature_names=_FEATURE_COLS)
        model.add_group(key, adapter, {"n_rows": n_tr})
        train_frames.append(df_tr)
        df_te = _group_pdf(seed=20 + i, n=_N_TEST[key], grp_value=key)
        test_frames.append(df_te)

    train_pdf = pd.concat(train_frames, ignore_index=True)
    test_pdf = pd.concat(test_frames, ignore_index=True)
    train_path = str(tmp_path / "train.parquet")
    test_path = str(tmp_path / "test.parquet")
    train_pdf.to_parquet(train_path)
    test_pdf.to_parquet(test_path)

    preprocessor = {"feature_columns": _FEATURE_COLS, "categorical_columns": [],
                    "category_mappings": {}}
    parameters = {
        "model_version": "staged-groups-test",
        "schema": {"columns": {"item": "prod_name", "label": "label"}},
        "diagnostics": {
            "feature_importance": {"enabled": True},
            "gain_ledger": {"enabled": True},
            "feature_stats": {"enabled": True, "sample_rows": 1000},
            "shap": {"enabled": True, "sample_rows": 50, "top_k": 2},
        },
    }
    return {
        "model": model,
        "train_handle": _HandleStub(train_path),
        "test_handle": _HandleStub(test_path),
        "preprocessor": preprocessor,
        "parameters": parameters,
    }


def _groups_dir(tmp_path, parameters):
    return (tmp_path / "data" / "models" / parameters["model_version"]
            / "diagnostics" / "groups")


class TestStagedGroupDiagnostics:
    def test_artifacts_written_per_group_and_manifest_indexes_them(
            self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup = _build_setup(tmp_path)

        manifest = compute_staged_group_diagnostics(
            setup["model"], setup["train_handle"], setup["test_handle"],
            setup["preprocessor"], setup["parameters"])

        slug_a, slug_b = group_slug("a"), group_slug("b")
        assert set(manifest["groups"]) == {slug_a, slug_b}

        base = _groups_dir(tmp_path, setup["parameters"])
        for slug in (slug_a, slug_b):
            d = base / slug
            assert (d / "feature_importance.json").exists()
            assert (d / "gain_ledger.json").exists()
            assert (d / "feature_statistics.json").exists()
            assert (d / "shap_top_features.json").exists()
            assert manifest["groups"][slug]["error"] is None

        # 兩群 n_train_rows 各自等於 fixture 造的實際列數（60/80，不能相同）。
        assert manifest["groups"][slug_a]["n_train_rows"] == 60
        assert manifest["groups"][slug_b]["n_train_rows"] == 80
        # 兩群 test 列數也不同（30/40）——SHAP 抽樣不得把兩群混在一起算。
        assert manifest["groups"][slug_a]["n_shap_sampled"] == 30
        assert manifest["groups"][slug_b]["n_shap_sampled"] == 40

        # importance 鍵＝真特徵名（read-back importance.py:8-22 的實際回傳結構：
        # {"ranked": [{"feature","split","gain"}], "dead_features": [...]}）。
        imp_a = json.loads((base / slug_a / "feature_importance.json").read_text())
        ranked_features = {r["feature"] for r in imp_a["ranked"]}
        assert ranked_features <= set(_FEATURE_COLS)
        assert not any(f.startswith("Column_") for f in ranked_features)

    def test_one_bad_group_is_isolated_not_fatal(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup = _build_setup(tmp_path)
        slug_a, slug_b = group_slug("a"), group_slug("b")

        class _BoomAdapter:
            def feature_importance(self, kind="split"):
                raise RuntimeError("boom")

            def predict(self, X):
                raise RuntimeError("boom")

            @property
            def booster(self):
                raise RuntimeError("boom")

        setup["model"]._groups["a"] = _BoomAdapter()

        manifest = compute_staged_group_diagnostics(
            setup["model"], setup["train_handle"], setup["test_handle"],
            setup["preprocessor"], setup["parameters"])

        assert manifest["groups"][slug_a]["error"] is not None
        assert "boom" in manifest["groups"][slug_a]["error"]

        entry_b = manifest["groups"][slug_b]
        assert entry_b["error"] is None
        base = _groups_dir(tmp_path, setup["parameters"])
        for fname in ("feature_importance.json", "gain_ledger.json",
                     "feature_statistics.json", "shap_top_features.json"):
            assert (base / slug_b / fname).exists()

    def test_missing_partition_column_fails_loud(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup = _build_setup(tmp_path)
        # test parquet 沒有 partition key 欄 "grp" → 整體 raise（不進 manifest）。
        bad_test_pdf = pd.DataFrame({
            "f0": np.zeros(5), "f1": np.zeros(5),
            "prod_name": ["P1"] * 5, "label": [0.0] * 5,
        })
        bad_test_path = str(tmp_path / "test_missing_grp.parquet")
        bad_test_pdf.to_parquet(bad_test_path)
        setup["test_handle"] = _HandleStub(bad_test_path)

        # 實跑確認：缺 partition key 欄在 data_access.read_column 的
        # pyarrow.dataset scan 就地炸開，型別為 pyarrow.lib.ArrowInvalid
        # （"No match for FieldRef.Name(grp) in ..."）。
        import pyarrow as pa

        with pytest.raises(pa.lib.ArrowInvalid, match="grp"):
            compute_staged_group_diagnostics(
                setup["model"], setup["train_handle"], setup["test_handle"],
                setup["preprocessor"], setup["parameters"])

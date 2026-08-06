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


# ---- F1（審查發現）：item_col 為 categorical 特徵＋category_mappings 非空時，
# per-group gain ledger 須走完整路徑（非 _coarse_ledger fallback）。既有
# `_build_setup` 的 fixture 用 `category_mappings: {}`，恆走粗帳本；這裡另建
# 一組 item 欄「已編碼類別碼＋可解碼」的 fixture 涵蓋完整路徑。

_ITEM_CATEGORIES = ["A", "B"]
_ITEM_FEATURE_COLS = ["f0", "f1", "prod_name"]
_ITEM_FIT_PARAMS = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                    "num_leaves": 7, "min_data_in_leaf": 5, "seed": 0,
                    "num_iterations": 8, "early_stopping_rounds": 0}


def _group_pdf_item_categorical(seed, n, grp_value):
    """item 欄（``prod_name``）為整數類別碼（0/1，對應 ``_ITEM_CATEGORIES``），
    且與 label 高度相關（10% 雜訊）——逼 LightGBM 早期即在 item 欄切分，確保
    完整路徑的 per-item 帳本非空。"""
    rng = np.random.RandomState(seed)
    f0 = rng.randn(n)
    f1 = rng.randn(n)
    prod_code = rng.randint(0, 2, size=n)
    flip = rng.rand(n) < 0.1
    y = np.where(flip, 1 - prod_code, prod_code).astype(float)
    return pd.DataFrame({"f0": f0, "f1": f1, "grp": grp_value,
                         "prod_name": prod_code, "label": y})


def _build_setup_item_categorical(tmp_path):
    model = StagedModelAdapter()
    model.set_partition_keys(["grp"])
    train_frames, test_frames = [], []
    for i, key in enumerate(sorted(_N_TRAIN)):
        n_tr = _N_TRAIN[key]
        df_tr = _group_pdf_item_categorical(seed=10 + i, n=n_tr, grp_value=key)
        X_tr = df_tr[_ITEM_FEATURE_COLS].to_numpy()
        y_tr = df_tr["label"].to_numpy()
        w_tr = np.ones(len(y_tr))
        n_dev = max(10, n_tr // 4)
        X_dev, y_dev = X_tr[:n_dev], y_tr[:n_dev]
        # prod_name 是 _ITEM_FEATURE_COLS 的 index 2 → categorical_indices=[2]。
        adapter = _fit_adapter(X_tr, y_tr, w_tr, X_dev, y_dev, dict(_ITEM_FIT_PARAMS),
                               [2], feature_names=_ITEM_FEATURE_COLS)
        model.add_group(key, adapter, {"n_rows": n_tr})
        train_frames.append(df_tr)
        df_te = _group_pdf_item_categorical(seed=20 + i, n=_N_TEST[key], grp_value=key)
        test_frames.append(df_te)

    train_pdf = pd.concat(train_frames, ignore_index=True)
    test_pdf = pd.concat(test_frames, ignore_index=True)
    train_path = str(tmp_path / "train_item.parquet")
    test_path = str(tmp_path / "test_item.parquet")
    train_pdf.to_parquet(train_path)
    test_pdf.to_parquet(test_path)

    preprocessor = {
        "feature_columns": _ITEM_FEATURE_COLS,
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": list(_ITEM_CATEGORIES)},
    }
    parameters = {
        "model_version": "staged-groups-item-test",
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

    def test_per_group_gain_ledger_uses_full_path_when_item_decodable(
            self, tmp_path, monkeypatch):
        """F1：item_col 為 categorical 特徵且 category_mappings 非空時，per-group
        gain_ledger.json 須是完整路徑輸出（``fallback: False``、``per_item``／
        ``context`` 有值），不是 ``_coarse_ledger`` 降級（``fallback: True``、
        ``per_item``/``context`` 皆 None，見 gain_ledger.py:313-339）。"""
        monkeypatch.chdir(tmp_path)
        setup = _build_setup_item_categorical(tmp_path)

        manifest = compute_staged_group_diagnostics(
            setup["model"], setup["train_handle"], setup["test_handle"],
            setup["preprocessor"], setup["parameters"])

        slug_a, slug_b = group_slug("a"), group_slug("b")
        base = _groups_dir(tmp_path, setup["parameters"])
        for slug in (slug_a, slug_b):
            assert manifest["groups"][slug]["error"] is None
            ledger = json.loads((base / slug / "gain_ledger.json").read_text())
            # 判別鍵：只有完整路徑才有 fallback=False 且 per_item/context 非 None；
            # 粗帳本恆為 fallback=True 且兩者皆 None（gain_ledger.py:319-333）。
            assert ledger["fallback"] is False
            assert ledger["per_item"] is not None
            assert set(ledger["per_item"]) == set(_ITEM_CATEGORIES)
            assert ledger["context"] is not None
            # 完整路徑才有能力算出的欄位：粗帳本恆為 None（gain_ledger.py:330-332）。
            assert ledger["pre_item"] is not None
            assert ledger["first_item_split_depth"] is not None
            # 訊號強烈相關，至少一棵樹會在 item 欄切分——否則完整路徑跑完但
            # decode 邏輯（_decode_threshold／per-item reachable 走訪）沒被真正
            # 執行到，覆蓋等於白做。
            assert ledger["item_id"]["split_count"] > 0

    def test_per_group_shap_respects_tree_count_budget_guard(
            self, tmp_path, monkeypatch):
        """F2：per-group SHAP 抽樣列數除了 shap.sample_rows 上限外，還須受
        max_budget 樹數預算閘限制（比照 shared 版 compute_shap_diagnostics，
        shap_per_item.py:136-143）。fixture 兩群 booster 各 5 棵樹（num_iterations=5、
        early_stopping_rounds=0，binary objective 恆 1 樹/迭代）；
        max_budget=n_trees*10=50 → eff=max(1, 50//5)=10，兩群 test 列數（30/40）
        皆 > 10，才有意義驗證縮減真的生效。"""
        monkeypatch.chdir(tmp_path)
        setup = _build_setup(tmp_path)
        setup["parameters"]["diagnostics"]["shap"]["max_budget"] = 50

        manifest = compute_staged_group_diagnostics(
            setup["model"], setup["train_handle"], setup["test_handle"],
            setup["preprocessor"], setup["parameters"])

        slug_a, slug_b = group_slug("a"), group_slug("b")
        assert manifest["groups"][slug_a]["error"] is None
        assert manifest["groups"][slug_b]["error"] is None
        assert manifest["groups"][slug_a]["n_shap_sampled"] <= 10
        assert manifest["groups"][slug_b]["n_shap_sampled"] <= 10
        # 兩群原本列數（30/40）都 > 10，縮減真的生效（不是巧合等於上限）。
        assert manifest["groups"][slug_a]["n_shap_sampled"] == 10
        assert manifest["groups"][slug_b]["n_shap_sampled"] == 10

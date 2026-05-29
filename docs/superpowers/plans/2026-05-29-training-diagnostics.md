# Training Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 training pipeline 產出特徵基本統計、LightGBM 原生 importance、SHAP 三類診斷並記錄到 MLflow,供業務可解釋性與模型健康檢查。

**Architecture:** 方案 B — 三個純計算 node(`compute_feature_statistics` / `compute_feature_importance` / `compute_shap_diagnostics`)放在新模組 `diagnostics.py`,各自吃 DAG 既有 handle、回傳結構化 dict(catalog 以 `JSONDataset` 持久化到 `data/models/${model_version}/diagnostics/`);SHAP node 額外把 PNG 寫進同一 dir。`log_experiment` 退化成薄記錄層,上傳整個 diagnostics dir + 幾個 scalar metric。

**Tech Stack:** Python 3.10、LightGBM 4.6.0、SHAP 0.42.1(`TreeExplainer`, `tree_path_dependent`)、matplotlib 3.10.9(`Agg`)、pyarrow 14、pandas 1.5.3、MLflow 3.1.0、pytest 7.3.1。

**參照 spec:** `docs/superpowers/specs/2026-05-29-training-diagnostics-design.md`

---

## 環境前提(每個 step 跑測試都用)

worktree:`/Users/curtislu/projects/recsys_tfb/.worktrees/training-diagnostics`
跑測試一律:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/training-diagnostics
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```
commit 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/training-diagnostics ...`(或先 `cd` worktree root)。

## File Structure

| 檔案 | 責任 | 動作 |
|---|---|---|
| `src/recsys_tfb/pipelines/training/diagnostics.py` | 三個診斷計算函式 + `diagnostics_dir` helper | Create |
| `src/recsys_tfb/models/base.py` | `feature_importance(kind=...)` ABC 簽章 | Modify |
| `src/recsys_tfb/models/lightgbm_adapter.py` | gain importance + `booster` property | Modify |
| `src/recsys_tfb/models/calibrated_adapter.py` | 委派 `kind` 與 `booster` | Modify |
| `src/recsys_tfb/pipelines/training/nodes.py` | `log_experiment` 薄記錄層 | Modify |
| `src/recsys_tfb/pipelines/training/pipeline.py` | 接 3 個 diagnostic node + 擴 log_experiment inputs | Modify |
| `conf/base/catalog.yaml` | 3 個 `JSONDataset` artifact | Modify |
| `conf/base/parameters_training.yaml` | top-level `diagnostics` block | Modify |
| `tests/test_core/test_versioning.py` | model_version 對 diagnostics 不變性 | Modify |
| `tests/test_models/test_adapter.py` | gain importance + booster property | Modify |
| `tests/test_models/test_calibrated_adapter.py` | 委派 kind/booster | Modify |
| `tests/test_pipelines/test_training/test_diagnostics.py` | 三個診斷函式單元測試 | Create |
| `tests/test_pipelines/test_training/test_nodes.py` | `log_experiment` 薄記錄層(mock mlflow) | Modify |

---

## Task 1: 加上 `diagnostics` config block + model_version 不變性測試

確立 config 形狀,並先用測試鎖死「diagnostics 放 top-level → 不影響 model_version」。

**Files:**
- Modify: `conf/base/parameters_training.yaml`
- Test: `tests/test_core/test_versioning.py`

- [ ] **Step 1: 在 `parameters_training.yaml` 加 top-level `diagnostics` block**

加在檔案最末(與既有 `training:` 同層的 top-level key,**不可**縮排到 `training:` 之下):

```yaml
# 訓練診斷產物（特徵統計 / 原生 importance / SHAP）。
# 刻意放 top-level（與 mlflow / cache 同層）：compute_model_version 只雜湊
# training: block，故此 block 不影響 model_version（見 test_versioning）。
diagnostics:
  feature_stats:
    enabled: true
    sample_rows: 500000
    high_null_threshold: 0.5
  feature_importance:
    enabled: true
  shap:
    enabled: true
    sample_rows: 2000
    top_k: 30
    n_examples: 5
    min_rows_per_item: 30
    max_budget: 4000000   # sample_rows * n_trees 超過則自動降抽樣
```

- [ ] **Step 2: 寫不變性測試(會失敗,因為測試尚未存在)**

加到 `tests/test_core/test_versioning.py`:

```python
def test_model_version_invariant_to_diagnostics():
    from recsys_tfb.core.versioning import compute_model_version

    base = {"training": {"algorithm": "lightgbm", "algorithm_params": {"objective": "binary"}}}
    with_diag = {
        "training": base["training"],
        "diagnostics": {"shap": {"enabled": True, "sample_rows": 2000}},
    }
    v_base = compute_model_version(base, "ds123456", "tr123456")
    v_diag = compute_model_version(with_diag, "ds123456", "tr123456")
    assert v_base == v_diag
```

- [ ] **Step 3: 跑測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_core/test_versioning.py::test_model_version_invariant_to_diagnostics -q`
Expected: PASS(`_model_version_payload` 只取 `training:`,top-level `diagnostics` 自動排除——這步同時驗證了既有行為符合設計)。

- [ ] **Step 4: Commit**

```bash
git add conf/base/parameters_training.yaml tests/test_core/test_versioning.py
git commit -m "feat(training): add top-level diagnostics config block (excluded from model_version)"
```

---

## Task 2: Adapter 支援 gain importance 與 booster 取用

`compute_feature_importance` 需要 split+gain;SHAP node 需要底層 booster。

**Files:**
- Modify: `src/recsys_tfb/models/base.py:47`
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py:99`
- Modify: `src/recsys_tfb/models/calibrated_adapter.py:125`
- Test: `tests/test_models/test_adapter.py`, `tests/test_models/test_calibrated_adapter.py`

- [ ] **Step 1: 寫失敗測試(adapter gain + booster)**

加到 `tests/test_models/test_adapter.py` 的 `TestLightGBMAdapter`:

```python
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
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_models/test_adapter.py::TestLightGBMAdapter::test_feature_importance_split_and_gain tests/test_models/test_adapter.py::TestLightGBMAdapter::test_booster_property -q`
Expected: FAIL(`feature_importance()` 不接受 `kind`;無 `booster` 屬性)。

- [ ] **Step 3: 改 ABC 簽章 `base.py`**

`src/recsys_tfb/models/base.py:47` 改為:

```python
    @abstractmethod
    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        """Return {feature_name: importance_score}. kind in {"split","gain"}."""
```

- [ ] **Step 4: 實作 `lightgbm_adapter.py`**

替換 `src/recsys_tfb/models/lightgbm_adapter.py:99-104` 的 `feature_importance`,並加 `booster` property:

```python
    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        if kind not in ("split", "gain"):
            raise ValueError(f"kind must be 'split' or 'gain', got {kind!r}")
        names = self._booster.feature_name()
        importances = self._booster.feature_importance(importance_type=kind).astype(float)
        return dict(zip(names, importances))

    @property
    def booster(self) -> "lgb.Booster":
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        return self._booster
```

- [ ] **Step 5: 委派 `calibrated_adapter.py`**

`src/recsys_tfb/models/calibrated_adapter.py:125-127` 改為:

```python
    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        return self._base.feature_importance(kind=kind)

    @property
    def booster(self):
        return self._base.booster
```

- [ ] **Step 6: 加 calibrated 委派測試**

加到 `tests/test_models/test_calibrated_adapter.py`(沿用該檔既有 fixture 建 calibrated adapter;若 fixture 名不同,對齊該檔)。最小可行:

```python
    def test_feature_importance_and_booster_delegate(self, fitted_calibrated_adapter):
        adapter = fitted_calibrated_adapter
        split = adapter.feature_importance(kind="split")
        gain = adapter.feature_importance(kind="gain")
        assert set(split) == set(gain)
        assert adapter.booster is adapter._base.booster
```

> 注:`fitted_calibrated_adapter` 用該檔現有建構 calibrated adapter 的 fixture/工廠;若無現成 fixture,在測試內以 `LightGBMAdapter` 訓練後包進 `CalibratedAdapter`,比照該檔既有測試的建法。

- [ ] **Step 7: 跑全部 adapter 測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_models/ -q`
Expected: PASS(含既有 `adapter.feature_importance()` 無參呼叫,因 default kind="split" 保持相容)。

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/models/base.py src/recsys_tfb/models/lightgbm_adapter.py src/recsys_tfb/models/calibrated_adapter.py tests/test_models/
git commit -m "feat(models): feature_importance(kind=split|gain) + booster accessor"
```

---

## Task 3: `diagnostics.py` 模組 + `diagnostics_dir` + `compute_feature_importance`

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics.py`
- Create: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試**

建立 `tests/test_pipelines/test_training/test_diagnostics.py`:

```python
"""Unit tests for training diagnostics (pure-python, no Spark)."""

import numpy as np
import pytest

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
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: FAIL(`diagnostics` 模組不存在)。

- [ ] **Step 3: 建立模組 + 實作**

建立 `src/recsys_tfb/pipelines/training/diagnostics.py`:

```python
"""Training diagnostics: feature stats, native importance, SHAP.

純計算函式（over driver-local parquet / booster），無 Spark 依賴，供
training pipeline 的 diagnostic node 使用，產物由 log_experiment 上傳 MLflow。
產物路徑沿用 catalog 慣例 data/models/<model_version>/diagnostics/。
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve（並建立）診斷產物 dir，對齊 catalog 的
    data/models/${model_version}/diagnostics/ 慣例。"""
    mv = parameters["model_version"]
    d = Path("data") / "models" / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d


def compute_feature_importance(model, parameters: dict) -> dict:
    """LightGBM split + gain importance，依 gain 排序，標出 dead features。"""
    cfg = parameters.get("diagnostics", {}).get("feature_importance", {})
    if not cfg.get("enabled", True):
        return {}
    split = model.feature_importance(kind="split")
    gain = model.feature_importance(kind="gain")
    ranked = sorted(
        ({"feature": f, "split": float(split[f]), "gain": float(gain[f])} for f in split),
        key=lambda r: r["gain"],
        reverse=True,
    )
    dead = sorted(f for f, v in split.items() if v == 0)
    logger.info("feature_importance: %d features, %d dead", len(ranked), len(dead))
    return {"ranked": ranked, "dead_features": dead}
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(training): diagnostics module + compute_feature_importance"
```

---

## Task 4: `compute_feature_statistics`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics.py`
- Modify: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試**

加到 `test_diagnostics.py`(頂部補 import):

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys_tfb.io.handles import ParquetHandle


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
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k feature_statistics -q`
Expected: FAIL(`compute_feature_statistics` 不存在)。

- [ ] **Step 3: 實作(加到 `diagnostics.py`)**

頂部加 import:

```python
import numpy as np
import pandas as pd
```

新增函式:

```python
def _to_native(v):
    """np scalar / NaN → JSON-safe python scalar（NaN → None）。"""
    if v is None:
        return None
    f = float(v)
    return None if np.isnan(f) else f


def compute_feature_statistics(train_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """逐特徵 null_rate / mean,std,min,max（數值）/ n_distinct + single_value,high_null 旗標。"""
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])

    import pyarrow.parquet as pq

    table = pq.read_table(train_parquet_handle.path, columns=feature_cols)
    n = table.num_rows
    if n > sample_rows:
        idx = np.sort(np.random.RandomState(42).choice(n, size=sample_rows, replace=False))
        table = table.take(idx)
        logger.info("feature_statistics: sampled %d of %d rows", sample_rows, n)
    pdf = table.to_pandas()

    stats: dict = {}
    for col in feature_cols:
        s = pdf[col]
        null_rate = float(s.isna().mean())
        n_distinct = int(s.nunique(dropna=True))
        entry = {
            "null_rate": null_rate,
            "n_distinct": n_distinct,
            "single_value": n_distinct <= 1,
            "high_null": null_rate >= high_null_threshold,
        }
        if pd.api.types.is_numeric_dtype(s):
            entry["mean"] = _to_native(s.mean())
            entry["std"] = _to_native(s.std())
            entry["min"] = _to_native(s.min())
            entry["max"] = _to_native(s.max())
        stats[col] = entry
    logger.info("feature_statistics: %d features summarized", len(stats))
    return stats
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k feature_statistics -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(training): compute_feature_statistics"
```

---

## Task 5: `compute_shap_diagnostics`(核心:單次計算三用 + 族群代表分層 + budget guard + PNG)

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics.py`
- Modify: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試**

加到 `test_diagnostics.py`。先加一個能被 SHAP/extract 用的小資料 fixture(含 item / label / 特徵欄,且一個稀有 item):

```python
import shap as _shap_mod  # noqa: F401  (確保依賴存在)


@pytest.fixture
def shap_setup(tmp_path, monkeypatch):
    """建一個能跑 extract_Xy._pdf_to_X 的小 test parquet + fitted adapter。

    schema 預設 item=prod_name, label=label；feature_columns 為兩個數值欄。
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

    preprocessor = {"feature_columns": ["f0", "f1"], "categorical_columns": []}
    parameters = {
        "model_version": "testmv",
        "schema": {"item": "prod_name", "label": "label",
                   "time": "snap_date", "entity": ["cust_id"], "identity_columns": []},
        "diagnostics": {"shap": {"enabled": True, "top_k": 2, "n_examples": 1,
                                 "min_rows_per_item": 30, "sample_rows": 150,
                                 "max_budget": 4000000}},
    }
    return adapter, handle, preprocessor, parameters


def test_shap_single_call_and_outputs(shap_setup, monkeypatch):
    adapter, handle, preprocessor, parameters = shap_setup

    # spy：確保只呼叫一次 shap_values
    import shap
    calls = {"n": 0}
    real_init = shap.TreeExplainer.__init__
    real_sv = shap.TreeExplainer.shap_values

    def counting_sv(self, X, *a, **k):
        calls["n"] += 1
        return real_sv(self, X, *a, **k)

    monkeypatch.setattr(shap.TreeExplainer, "shap_values", counting_sv)

    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)

    assert calls["n"] == 1                          # 單次計算
    assert set(out) >= {"global", "per_item", "examples"}
    # global top_k
    assert len(out["global"]["top_features"]) == 2
    assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
               for r in out["global"]["top_features"])
    # per-item 含稀有 item + low_coverage 旗標
    assert "rare" in out["per_item"]
    assert out["per_item"]["rare"]["low_coverage"] is True
    assert out["per_item"]["rare"]["n_sampled"] <= 2
    assert {"n_sampled", "n_positive", "score_min", "score_max", "score_mean", "low_coverage"} \
        <= set(out["per_item"]["rare"])
    # examples 有 high/low
    assert {"high", "low"} <= set(out["examples"])
    # 稀有高分涵蓋：每個 item 至少一筆高分 example
    items_in_examples = {e["item"] for e in out["examples"]["per_item_high"]}
    assert {"A", "B", "rare"} <= items_in_examples
    # PNG 落地
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
    assert any("budget" in r.message.lower() for r in caplog.records)
```

> 注:`get_schema(parameters)` 會讀 `parameters["schema"]`(若 repo 是讀別的 key,對齊 `src/recsys_tfb/core/schema.py` 的實際行為——必要時改 fixture 的 `schema` 放法,但 item/label 值維持 `prod_name`/`label`)。

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k shap -q`
Expected: FAIL(`compute_shap_diagnostics` 不存在)。

- [ ] **Step 3: 實作(加到 `diagnostics.py`)**

頂部 import 區加(matplotlib 非互動 backend 必須在 import pyplot 前設定):

```python
from recsys_tfb.core.logging import log_step
```

新增 helper 與主函式:

```python
def _stratified_item_sample(pdf, item_col, total, min_per_item, seed):
    """族群代表抽樣：依 item 分層，item 內純隨機；每 item 至少 min_per_item，
    不足全取（take-all）。回傳選中的 positional indices（對 pdf.iloc）。"""
    rng = np.random.RandomState(seed)
    groups = {item: np.where(pdf[item_col].values == item)[0]
              for item in pd.unique(pdf[item_col])}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)


def compute_shap_diagnostics(model, test_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """SHAP 全域 / per-item（族群代表）/ 代表性個例。單次 shap_values 三用。"""
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("enabled", True):
        return {}

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    top_k = int(cfg.get("top_k", 30))
    n_examples = int(cfg.get("n_examples", 5))
    min_per_item = int(cfg.get("min_rows_per_item", 30))
    sample_rows = int(cfg.get("sample_rows", 2000))
    max_budget = int(cfg.get("max_budget", 4_000_000))

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    feature_cols = list(preprocessor["feature_columns"])

    pdf = test_parquet_handle.to_pandas()

    booster = model.booster
    n_trees = booster.num_trees()
    # budget guard：sample_rows * n_trees 超過 max_budget 則降抽樣
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d → reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    idx = _stratified_item_sample(pdf, item_col, eff_sample, min_per_item, seed=42)
    sample_pdf = pdf.iloc[idx].reset_index(drop=True)

    X = _pdf_to_X(sample_pdf, preprocessor, parameters)
    scores = model.predict(X)

    with log_step(logger, "shap_values"):
        explainer = shap.TreeExplainer(booster)      # tree_path_dependent (default)
        shap_values = explainer.shap_values(X)        # SINGLE call
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 3:                         # 某些版本回傳 [classes, n, feat]
        shap_values = shap_values[-1]
    shap_values = shap_values[:, : len(feature_cols)] # 去掉可能的 bias 欄

    # ---- 全域 ----
    mean_abs = np.abs(shap_values).mean(axis=0)
    mean_signed = shap_values.mean(axis=0)
    order = np.argsort(mean_abs)[::-1][:top_k]
    global_top = [
        {"feature": feature_cols[i], "mean_abs_shap": float(mean_abs[i]),
         "mean_signed_shap": float(mean_signed[i])}
        for i in order
    ]

    # ---- per-item（族群代表 + 覆蓋率 metadata）----
    items = sample_pdf[item_col].values
    labels = sample_pdf[label_col].values if label_col in sample_pdf else np.zeros(len(sample_pdf))
    per_item = {}
    for item in pd.unique(items):
        mask = items == item
        ai = np.abs(shap_values[mask]).mean(axis=0)
        o = np.argsort(ai)[::-1][:top_k]
        sc = scores[mask]
        per_item[str(item)] = {
            "top_features": [{"feature": feature_cols[i], "mean_abs_shap": float(ai[i])} for i in o],
            "n_sampled": int(mask.sum()),
            "n_positive": int(np.sum(labels[mask] == 1)),
            "score_min": float(sc.min()), "score_max": float(sc.max()),
            "score_mean": float(sc.mean()),
            "low_coverage": bool(mask.sum() < min_per_item),
        }

    # ---- 代表性個例（全域 high/low + 每 item 一筆高分）----
    def _example(i):
        return {"item": str(items[i]), "score": float(scores[i]),
                "shap": {feature_cols[j]: float(shap_values[i, j]) for j in range(len(feature_cols))}}

    hi = np.argsort(scores)[::-1][:n_examples]
    lo = np.argsort(scores)[:n_examples]
    per_item_high = []
    for item in pd.unique(items):
        pos = np.where(items == item)[0]
        best = pos[np.argmax(scores[pos])]
        per_item_high.append(_example(best))
    examples = {"high": [_example(i) for i in hi],
                "low": [_example(i) for i in lo],
                "per_item_high": per_item_high}

    # ---- PNG ----
    d = diagnostics_dir(parameters)
    plt.figure()
    shap.summary_plot(shap_values, features=X, feature_names=feature_cols, show=False)
    plt.tight_layout()
    plt.savefig(d / "shap_summary.png", dpi=100)
    plt.close()
    for rank, i in enumerate(hi):
        plt.figure()
        shap.summary_plot(shap_values[[i]], features=X[[i]], feature_names=feature_cols,
                          plot_type="bar", show=False)
        plt.tight_layout()
        plt.savefig(d / f"waterfall_high_{rank}.png", dpi=100)
        plt.close()

    logger.info("shap diagnostics: n_sample=%d n_trees=%d items=%d",
                len(idx), n_trees, len(per_item))
    return {"global": {"top_features": global_top}, "per_item": per_item, "examples": examples}
```

> SHAP 0.42 的 `shap_values` 對單輸出 tree 回傳 `[n, n_feature]`(bias 在 `explainer.expected_value`);上面 `ndim==3` 與 `[:, :len(feature_cols)]` 為跨版本防呆。waterfall 用 `summary_plot(plot_type="bar")` 對單列即逐特徵貢獻,避開 `shap.plots.waterfall` 對 Explanation 物件的相依差異。

- [ ] **Step 4: 跑測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k shap -q`
Expected: PASS。若 `get_schema` 讀 key 與 fixture 不符而失敗,依該檔實際行為調 fixture 的 `schema` 放置(item/label 值不變),再跑。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(training): compute_shap_diagnostics (single-call, stratified, budget guard, PNG)"
```

---

## Task 6: `log_experiment` 薄記錄層 + pipeline 接線 + catalog

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:693-738`
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`
- Modify: `conf/base/catalog.yaml`
- Modify: `tests/test_pipelines/test_training/test_nodes.py`

- [ ] **Step 1: 寫失敗測試(log_experiment 記 artifacts + scalar)**

加到 `tests/test_pipelines/test_training/test_nodes.py`(沿用該檔既有 mlflow mock 風格;若無,用 `unittest.mock.patch`):

```python
def test_log_experiment_logs_diagnostics(monkeypatch, tmp_path):
    import recsys_tfb.pipelines.training.nodes as nodes

    logged_metrics, logged_artifacts = {}, []
    monkeypatch.setattr(nodes.mlflow, "set_tracking_uri", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "set_experiment", lambda *a, **k: None)

    class _Run:
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(nodes.mlflow, "start_run", lambda *a, **k: _Run())
    monkeypatch.setattr(nodes.mlflow, "log_params", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "log_param", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "log_metric", lambda k, v: logged_metrics.__setitem__(k, v))
    monkeypatch.setattr(nodes.mlflow, "log_artifacts", lambda d, *a, **k: logged_artifacts.append(d))

    monkeypatch.chdir(tmp_path)
    parameters = {"model_version": "mv1", "mlflow": {}, "training": {}}
    # 預先放一個 diagnostics dir
    from recsys_tfb.pipelines.training.diagnostics import diagnostics_dir
    diagnostics_dir(parameters)

    class _Model:
        def log_to_mlflow(self): pass

    eval_results = {"overall_map": 0.5, "per_item_map_attr": {}, "n_queries": 10, "n_excluded_queries": 0}
    feature_statistics = {"f0": {"single_value": True, "high_null": False, "null_rate": 0.0, "n_distinct": 1}}
    feature_importance = {"ranked": [], "dead_features": ["f3", "f4"]}
    shap_diagnostics = {"global": {"top_features": []}, "per_item": {}, "examples": {}}

    nodes.log_experiment(_Model(), {}, 10, eval_results, feature_statistics,
                         feature_importance, shap_diagnostics, parameters)

    assert logged_metrics["n_dead_features"] == 2
    assert logged_metrics["n_single_value_features"] == 1
    assert logged_metrics["n_high_null_features"] == 0
    assert len(logged_artifacts) == 1  # 整個 diagnostics dir 上傳一次
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::test_log_experiment_logs_diagnostics -q`
Expected: FAIL(`log_experiment` 簽章不含診斷參數)。

- [ ] **Step 3: 改寫 `log_experiment`**

`src/recsys_tfb/pipelines/training/nodes.py:693`,擴充簽章與 body。在現有 `model.log_to_mlflow()` 之後、`with mlflow.start_run()` 區塊內新增診斷記錄;簽章插在 `evaluation_results` 之後、`parameters` 之前:

```python
def log_experiment(
    model: ModelAdapter,
    best_params: dict,
    best_iteration: int,
    evaluation_results: dict,
    feature_statistics: dict,
    feature_importance: dict,
    shap_diagnostics: dict,
    parameters: dict,
) -> None:
    """Log training results + diagnostics to MLflow."""
    from recsys_tfb.pipelines.training.diagnostics import diagnostics_dir
    ...
    # （既有 set_tracking_uri / set_experiment / start_run / log_params / metrics 不變）
            model.log_to_mlflow()

            # --- diagnostics scalar summary ---
            if feature_importance:
                mlflow.log_metric("n_dead_features", len(feature_importance.get("dead_features", [])))
            if feature_statistics:
                mlflow.log_metric(
                    "n_single_value_features",
                    sum(1 for s in feature_statistics.values() if s.get("single_value")),
                )
                mlflow.log_metric(
                    "n_high_null_features",
                    sum(1 for s in feature_statistics.values() if s.get("high_null")),
                )

            # --- diagnostics artifacts（JSON 由 catalog 寫入、PNG 由 shap node 寫入，
            #     此處整包 dir 上傳）---
            diag_dir = diagnostics_dir(parameters)
            if any(diag_dir.iterdir()):
                mlflow.log_artifacts(str(diag_dir))
```

> `feature_statistics` / `feature_importance` / `shap_diagnostics` 任一為 `{}`(該診斷 disabled)時對應 metric 自動略過;`diag_dir` 為空時不上傳。

- [ ] **Step 4: 跑測試確認 PASS**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::test_log_experiment_logs_diagnostics -q`
Expected: PASS

- [ ] **Step 5: 接線 pipeline**

`src/recsys_tfb/pipelines/training/pipeline.py`:
- import 新增:`from recsys_tfb.pipelines.training.diagnostics import (compute_feature_importance, compute_feature_statistics, compute_shap_diagnostics)`
- 在 `compute_test_mAP_spark` node 之後、`log_experiment` 之前插入三個 node:

```python
        Node(
            compute_feature_statistics,
            inputs=["train_parquet_handle", "preprocessor", "parameters"],
            outputs="feature_statistics",
        ),
        Node(
            compute_feature_importance,
            inputs=["model", "parameters"],
            outputs="feature_importance",
        ),
        Node(
            compute_shap_diagnostics,
            inputs=["model", "test_parquet_handle", "preprocessor", "parameters"],
            outputs="shap_diagnostics",
        ),
```

- 改 `log_experiment` node 的 inputs:

```python
        Node(
            log_experiment,
            inputs=[
                "model", "best_params", "best_iteration", "evaluation_results",
                "feature_statistics", "feature_importance", "shap_diagnostics",
                "parameters",
            ],
            outputs=None,
        ),
```

- [ ] **Step 6: 加 catalog 三個 JSONDataset**

`conf/base/catalog.yaml`,接在 `evaluation_results` 之後:

```yaml
feature_statistics:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/feature_statistics.json

feature_importance:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/feature_importance.json

shap_diagnostics:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/shap_diagnostics.json
```

- [ ] **Step 7: 跑 pipeline 結構測試 + 既有 training node 測試**

Run: `PYTHONPATH=src .venv/bin/python -m pytest tests/test_pipelines/test_training/test_pipeline.py tests/test_pipelines/test_training/test_nodes.py -q`
Expected: PASS。若 `test_pipeline.py` 斷言 node 數量/名稱,更新該斷言以涵蓋 3 個新 node。

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py src/recsys_tfb/pipelines/training/pipeline.py conf/base/catalog.yaml tests/test_pipelines/test_training/
git commit -m "feat(training): wire diagnostics nodes + thin log_experiment logging layer"
```

---

## Task 7: 全套件回歸 + graphify 更新

**Files:** 無(驗證)

- [ ] **Step 1: 跑 diagnostics + 受影響測試**

Run:
```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/test_pipelines/test_training/ tests/test_models/ tests/test_core/test_versioning.py -q
```
Expected: 全 PASS。(刻意不跑 `tests/test_evaluation` 全包——~33 分鐘且與本次無關;符合 §測試效能。)

- [ ] **Step 2: 更新 graphify code graph**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`

- [ ] **Step 3: 收尾**

進入 `superpowers:finishing-a-development-branch` 決定 merge / PR。

---

## Self-Review

**Spec coverage:**
- §2 方案 B 三 node + 薄記錄層 → Task 3/4/5/6 ✅
- §3.1 feature stats(null/mean/std/min/max/n_distinct/旗標/抽樣)→ Task 4 ✅
- §3.2 split+gain+dead → Task 2(adapter)+ Task 3 ✅
- §3.3 SHAP 單次三用 / tree_path_dependent / 依 item 分層 / budget guard / per-item 族群代表+覆蓋率+low_coverage / 個例 high-low + 稀有高分涵蓋 / PNG → Task 5 ✅
- §4 log_experiment artifacts + scalar(dead/high-null/single-value)+ 容錯 → Task 6 ✅
- §5 diagnostics top-level config + model_version 不變性測試 → Task 1 ✅
- §6 測試策略(pure-python 秒級,不碰 Spark)→ Task 2-6 測試皆符合 ✅
- 排除收斂診斷 → 計畫無任何 train-mAP / 收斂 metric ✅

**Placeholder scan:** 無 TBD/TODO;每個改碼 step 皆含完整程式碼。兩處標注「依該檔實際行為對齊」(calibrated fixture、get_schema key)為 repo-specific 探查,非 placeholder——已給 fallback 指示。

**Type consistency:**
- `feature_importance(kind="split"|"gain")` 簽章在 base/lightgbm/calibrated(Task 2)與呼叫端 `compute_feature_importance`(Task 3)一致。
- `compute_feature_*` 回傳 dict 形狀與 `log_experiment` 讀取(`dead_features`、`single_value`、`high_null`)一致(Task 3/4/6)。
- `diagnostics_dir(parameters)` 在 diagnostics.py 定義、SHAP node 與 log_experiment 共用,簽章一致。
- node outputs(`feature_statistics`/`feature_importance`/`shap_diagnostics`)= catalog key = log_experiment inputs,三處名稱一致(Task 6)。

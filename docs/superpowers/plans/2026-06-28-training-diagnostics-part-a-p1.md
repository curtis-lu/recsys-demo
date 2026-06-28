# Training Diagnostics 強化 — P1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 training 的 SHAP 診斷從「只有 mean|SHAP|、無方向、不跨 item 比較」升級為 per-item **signed** 驅動表 + **正樣本對照** + **divergence-from-global 偏離度**,並把 `diagnostics.py` 重構成可分階段擴充的 `diagnostics/` 子套件。

**Architecture:** 將 `pipelines/training/diagnostics.py`(單檔)拆成 `diagnostics/` 子套件(一個關注點一檔),`__init__.py` re-export 既有公開函式維持 import 相容;所有 SHAP 取值經新的 `attribution.feature_attributions` 接縫(two-stage 無關);per-item 計算加 signed/positive/divergence;個別案例圖改為 per-item beeswarm 進 `summary/` 資料夾。全部新 config 留 `diagnostics:` top-level,不動 `model_version`。

**Tech Stack:** Python 3.10、LightGBM 4.6.0、SHAP 0.42.1(TreeExplainer)、numpy/pandas、matplotlib(Agg)、pytest 7.3.1。

**規範(每個 Task 共用):**
- 在 worktree root 跑測試,絕對 venv python + `PYTHONPATH=src`:
  `cd /Users/curtislu/projects/recsys_tfb/.worktrees/training-diag-part-a`
  `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
- 這些 diagnostics 測試是純 python(無 Spark),秒級。
- 每個 commit 訊息結尾加 `Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29`(以下 commit 指令省略不重複寫)。
- commit 會觸發 graphify post-commit hook 自動重建 graph(無需手動)。
- 範圍:**只做 P1**;P2(象限案例)、P3(optuna/學習曲線)各自獨立 plan。

---

## File Structure(P1 鎖定的分解)

新增 `src/recsys_tfb/pipelines/training/diagnostics/`(套件,取代同名 `.py`):

| 檔 | 責任 |
|---|---|
| `__init__.py` | re-export `diagnostics_dir / compute_feature_importance / compute_feature_statistics / compute_shap_diagnostics`,維持舊 import 路徑 |
| `_util.py` | `_to_native`(JSON-safe scalar) |
| `paths.py` | `diagnostics_dir` + `summary_dir / per_item_summary_dir / safe_name` |
| `importance.py` | `compute_feature_importance`(搬移,行為不變) |
| `feature_stats.py` | `compute_feature_statistics`(搬移,行為不變) |
| `sampling.py` | `_stratified_item_sample`(搬移,行為不變) |
| `attribution.py` | `feature_attributions / attribution_budget_units`(新,two-stage 無關接縫) |
| `shap_per_item.py` | `compute_shap_diagnostics` orchestrator + `_signed_profile / _divergence / _rankdata` |

修改:`conf/base/parameters_training.yaml`(P1 config 鍵)、`tests/test_pipelines/test_training/test_diagnostics.py`(summary PNG 路徑)、docs。
新增測試:`tests/test_pipelines/test_training/test_attribution.py`。
刪除:`src/recsys_tfb/pipelines/training/diagnostics.py`。

---

## Task 1: 重構 `diagnostics.py` → `diagnostics/` 子套件(行為不變)

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics/{__init__,_util,paths,importance,feature_stats,sampling,shap_per_item}.py`
- Delete: `src/recsys_tfb/pipelines/training/diagnostics.py`
- Safety net: `tests/test_pipelines/test_training/test_diagnostics.py`(既有,不改)

- [ ] **Step 1: 建立套件目錄與 `_util.py`、`paths.py`、`sampling.py`**

`diagnostics/_util.py`:
```python
"""Shared JSON-safe helpers for training diagnostics."""
import numpy as np


def _to_native(v):
    """np scalar / NaN → JSON-safe python scalar（NaN → None）。"""
    if v is None:
        return None
    f = float(v)
    return None if np.isnan(f) else f
```

`diagnostics/paths.py`:
```python
"""診斷產物路徑 helper。"""
from pathlib import Path


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve（並建立）診斷產物 dir，對齊 catalog 的
    data/models/${model_version}/diagnostics/ 慣例。"""
    mv = parameters["model_version"]
    d = Path("data") / "models" / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d
```

`diagnostics/sampling.py`:
```python
"""SHAP 抽樣策略。"""
import numpy as np
import pandas as pd


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
```

- [ ] **Step 2: 建立 `importance.py`、`feature_stats.py`(從舊檔搬移,改相對 import)**

`diagnostics/importance.py`:
```python
"""LightGBM 原生 feature importance 診斷。"""
import logging

logger = logging.getLogger(__name__)


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

`diagnostics/feature_stats.py`:
```python
"""逐特徵統計診斷（train-only）。"""
import logging

import numpy as np
import pandas as pd

from ._util import _to_native

logger = logging.getLogger(__name__)


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

- [ ] **Step 3: 建立 `shap_per_item.py`(搬移 `compute_shap_diagnostics`,改相對 import)**

把舊 `diagnostics.py` 的 `compute_shap_diagnostics`(函式體 L108–222)**原封搬入** `diagnostics/shap_per_item.py`,只改檔頭 import 與三處呼叫來源:
- 檔頭加:
```python
"""per-item SHAP 診斷 orchestrator。"""
import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step

from ._util import _to_native
from .paths import diagnostics_dir
from .sampling import _stratified_item_sample

logger = logging.getLogger(__name__)
```
- 函式體內 `diagnostics_dir(...)`、`_to_native(...)`、`_stratified_item_sample(...)` 的呼叫維持原樣(現在來自上面 import)。
- 移除原本檔內這三者的定義(它們已搬到各自子模組)。

- [ ] **Step 4: 建立 `__init__.py`(re-export,維持相容)**

`diagnostics/__init__.py`:
```python
"""Training diagnostics 套件（feature stats / importance / SHAP）。

對外維持與舊 diagnostics.py 相容的 import 介面（pipeline.py、nodes.py、既有測試）。
"""
from .feature_stats import compute_feature_statistics
from .importance import compute_feature_importance
from .paths import diagnostics_dir
from .shap_per_item import compute_shap_diagnostics

__all__ = [
    "compute_feature_statistics",
    "compute_feature_importance",
    "compute_shap_diagnostics",
    "diagnostics_dir",
]
```

- [ ] **Step 5: 刪除舊單檔**

Run: `git rm src/recsys_tfb/pipelines/training/diagnostics.py`
(注意:刪檔前確認上面四個子模組都已建立;`git rm` 同時從 worktree 與 index 移除。)

- [ ] **Step 6: 跑既有測試確認行為不變(回歸網)**

Run:
```
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics.py -q
```
Expected: PASS(既有 7 測試全綠;import 相容、行為不變)。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/
git rm src/recsys_tfb/pipelines/training/diagnostics.py 2>/dev/null; true
git commit -m "refactor(diagnostics): split diagnostics.py into diagnostics/ subpackage (no behavior change)"
```

---

## Task 2: `attribution.py` two-stage 無關接縫

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics/attribution.py`
- Test: `tests/test_pipelines/test_training/test_attribution.py`
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_training/test_attribution.py`:
```python
"""attribution 接縫單元測試（pure-python）。"""
import numpy as np
import pytest

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.diagnostics import attribution


def _fitted():
    rng = np.random.RandomState(0)
    X = rng.randn(80, 3)
    y = (X[:, 0] > 0).astype(float)
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
            "num_leaves": 4, "seed": 0, "num_iterations": 10, "early_stopping_rounds": 0})
    return a, X


def test_feature_attributions_shape():
    a, X = _fitted()
    sv = attribution.feature_attributions(a, X, ["f0", "f1", "f2"])
    assert sv.shape == (80, 3)


def test_attribution_budget_units_positive():
    a, _ = _fitted()
    assert attribution.attribution_budget_units(a) >= 1


def test_feature_attributions_raises_without_booster():
    class NoBooster:
        pass
    with pytest.raises(TypeError, match="booster"):
        attribution.feature_attributions(NoBooster(), np.zeros((2, 3)), ["a", "b", "c"])
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_attribution.py -q`
Expected: FAIL(`ModuleNotFoundError` / `AttributeError: module ... has no attribute 'attribution'`)。

- [ ] **Step 3: 實作 `attribution.py`**

```python
"""模型結構無關的特徵歸因接縫（SHAP）。

今天走 LightGBM booster 的 TreeExplainer；這是日後支援 composite（two-stage）
模型的唯一改點——上層診斷一律經 feature_attributions / attribution_budget_units，
不直接觸碰 model.booster。
"""
import numpy as np


def _resolve_booster(model):
    booster = getattr(model, "booster", None)
    if booster is None:
        raise TypeError(
            f"{type(model).__name__} 無 booster；SHAP 歸因不支援"
            "（請在此 seam 擴充 composite 模型）"
        )
    return booster


def feature_attributions(model, X, feature_names) -> np.ndarray:
    """回傳 (n_rows, n_features) 的 SHAP 值；去掉可能的 bias 欄。"""
    import shap

    booster = _resolve_booster(model)
    sv = np.asarray(shap.TreeExplainer(booster).shap_values(X))
    if sv.ndim == 3:                      # 某些版本回 [classes, n, feat]
        sv = sv[-1]
    return sv[:, : len(feature_names)]


def attribution_budget_units(model) -> int:
    """budget guard 的成本因子（今天 = booster 樹數）。"""
    booster = getattr(model, "booster", None)
    return int(booster.num_trees()) if booster is not None else 1
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_attribution.py -q`
Expected: PASS(3 測試)。

- [ ] **Step 5: 把 `shap_per_item.py` 改用接縫**

在 `shap_per_item.py` 的 `compute_shap_diagnostics` 內:
- 檔頭新增:
```python
from .attribution import attribution_budget_units, feature_attributions
```
- 移除原本 `import shap` 之後對 `model.booster` / `explainer` 的直接使用,改為:
```python
# 原:
#   booster = model.booster
#   n_trees = booster.num_trees()
# 改:
n_trees = attribution_budget_units(model)
```
```python
# 原:
#   with log_step(logger, "shap_values"):
#       explainer = shap.TreeExplainer(booster)
#       shap_values = explainer.shap_values(X)
#   shap_values = np.asarray(shap_values)
#   if shap_values.ndim == 3:
#       shap_values = shap_values[-1]
#   shap_values = shap_values[:, : len(feature_cols)]
# 改:
with log_step(logger, "shap_values"):
    shap_values = feature_attributions(model, X, feature_cols)
```
(`import shap` 仍保留——後續 PNG 用 `shap.summary_plot`。)

- [ ] **Step 6: 跑既有 SHAP 測試確認單次計算仍成立**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS(`test_shap_single_call_and_outputs` 對 `shap.TreeExplainer.shap_values` 的單次計算 monkeypatch 仍命中,因接縫內部即呼叫同一 class method)。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/attribution.py \
        src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
        tests/test_pipelines/test_training/test_attribution.py
git commit -m "feat(diagnostics): two-stage-agnostic feature_attributions seam"
```

---

## Task 3: per-item top_features 加方向(`mean_signed_shap`)

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試(加到 test_diagnostics.py 末端)**

```python
def test_per_item_top_features_signed(shap_setup):
    adapter, handle, preprocessor, parameters = shap_setup
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    for blk in out["per_item"].values():
        assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
                   for r in blk["top_features"])
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py::test_per_item_top_features_signed -q`
Expected: FAIL(`top_features` 目前只有 `mean_abs_shap`)。

- [ ] **Step 3: 加 `_signed_profile` helper 並用於 per_item**

在 `shap_per_item.py` 加 helper(函式外層):
```python
def _signed_profile(sv_subset, feature_cols, top_k):
    """回傳 (top_features[含 signed], mean_abs 向量)。mean_abs 供 divergence 用。"""
    ai = np.abs(sv_subset).mean(axis=0)
    si = sv_subset.mean(axis=0)
    order = np.argsort(ai)[::-1][:top_k]
    profile = [{"feature": feature_cols[i],
                "mean_abs_shap": _to_native(ai[i]),
                "mean_signed_shap": _to_native(si[i])} for i in order]
    return profile, ai
```
在 per_item 迴圈把原本:
```python
ai = np.abs(shap_values[mask]).mean(axis=0)
o = np.argsort(ai)[::-1][:top_k]
... "top_features": [{"feature": feature_cols[i], "mean_abs_shap": _to_native(ai[i])} for i in o], ...
```
改為:
```python
prof_all, ai = _signed_profile(shap_values[mask], feature_cols, top_k)
... "top_features": prof_all, ...
```

- [ ] **Step 4: 跑測試確認通過(含既有)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
        tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): signed per-item SHAP direction (mean_signed_shap)"
```

---

## Task 4: per-item 正樣本對照(`top_features_positive` + `positive_low_coverage`)

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試**

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py::test_per_item_profile_positive_and_coverage -q`
Expected: FAIL(無這兩個 key)。

- [ ] **Step 3: 實作正樣本 profile**

讀 config(`compute_shap_diagnostics` 開頭與其他 cfg.get 並列):
```python
positive_min_rows = int(cfg.get("positive_min_rows", 20))
```
在 per_item 迴圈內(已有 `mask`、`labels`):
```python
pos_mask = mask & (labels == 1)
n_pos = int(pos_mask.sum())
if n_pos >= positive_min_rows:
    prof_pos, _ = _signed_profile(shap_values[pos_mask], feature_cols, top_k)
    pos_low = False
else:
    prof_pos, pos_low = None, True
```
在該 item 的 dict 加:
```python
"top_features_positive": prof_pos,
"positive_low_coverage": pos_low,
```
(`n_positive` 沿用既有 `int(np.sum(labels[mask] == 1))`,可改用上面的 `n_pos`。)

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
        tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): per-item positive-only SHAP profile + coverage flag"
```

---

## Task 5: divergence-from-global + idiosyncrasy 排序

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試(helper 直接測 + 整合測)**

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k divergence -q`
Expected: FAIL(`_divergence` 未定義 / 無 `item_idiosyncrasy`)。

- [ ] **Step 3: 實作 `_divergence` / `_rankdata` 並接入**

在 `shap_per_item.py` 加(函式外層):
```python
def _rankdata(a):
    order = np.argsort(a)
    ranks = np.empty(len(a), dtype=float)
    ranks[order] = np.arange(len(a), dtype=float)
    return ranks


def _divergence(item_abs, global_abs, metric, k, feature_cols):
    """per-item |SHAP| 排序 vs 全域排序的偏離度（0=一致, 1=完全不同）。"""
    k = min(int(k), len(feature_cols))
    i_order = np.argsort(item_abs)[::-1]
    g_top = set(np.argsort(global_abs)[::-1][:k].tolist())
    i_top = set(i_order[:k].tolist())
    if metric == "spearman":
        ir, gr = _rankdata(item_abs), _rankdata(global_abs)
        div = (1.0 - float(np.corrcoef(ir, gr)[0, 1])) / 2.0 if ir.std() and gr.std() else 0.0
    else:  # jaccard_topk
        inter, union = len(i_top & g_top), len(i_top | g_top)
        div = (1.0 - inter / union) if union else 0.0
    idio = [feature_cols[i] for i in i_order[:k] if i not in g_top]
    return float(div), idio
```
讀 config(與其他 cfg.get 並列):
```python
divergence_metric = str(cfg.get("divergence_metric", "jaccard_topk"))
divergence_top_k = int(cfg.get("divergence_top_k", 15))
```
在 per_item 迴圈前確保有全域 mean_abs 基準(沿用既有 `mean_abs` 變數;若名稱不同則 `global_abs = np.abs(shap_values).mean(axis=0)`)。迴圈內(已有 `ai`):
```python
div, idio = _divergence(ai, mean_abs, divergence_metric, divergence_top_k, feature_cols)
```
在該 item dict 加:
```python
"divergence_from_global": _to_native(div),
"idiosyncratic_features": idio,
```
在 `per_item` 組完後、`return` 前加排序表:
```python
item_idiosyncrasy = sorted(
    ({"item": k,
      "divergence_from_global": v["divergence_from_global"],
      "idiosyncratic_features": v["idiosyncratic_features"]}
     for k, v in per_item.items()),
    key=lambda r: r["divergence_from_global"],
    reverse=True,
)
```
並把回傳 dict 加上 `"item_idiosyncrasy": item_idiosyncrasy`。

- [ ] **Step 4: 跑測試確認通過(含既有全部)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
        tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): divergence-from-global score + item_idiosyncrasy ranking"
```

---

## Task 6: per-item beeswarm 圖 + `summary/` 資料夾 + 移除 waterfall_high

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/paths.py`
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試 + 更新既有 summary PNG 斷言**

加新測試:
```python
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
```
更新既有 `test_shap_single_call_and_outputs` 末兩行:
```python
# 原:
#   d = diag.diagnostics_dir(parameters)
#   assert (d / "shap_summary.png").exists()
# 改:
from recsys_tfb.pipelines.training.diagnostics.paths import summary_dir
assert (summary_dir(parameters) / "shap_summary_global.png").exists()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -k summary -q`
Expected: FAIL(無 `summary_dir` / 舊路徑)。

- [ ] **Step 3: `paths.py` 加 summary helpers**

```python
import re
```
(檔頭),並加:
```python
def summary_dir(parameters: dict) -> Path:
    d = diagnostics_dir(parameters) / "summary"
    d.mkdir(parents=True, exist_ok=True)
    return d


def per_item_summary_dir(parameters: dict) -> Path:
    d = summary_dir(parameters) / "per_item"
    d.mkdir(parents=True, exist_ok=True)
    return d


def safe_name(s) -> str:
    """檔名安全化（item 值可能含空白/斜線）。"""
    return re.sub(r"[^0-9A-Za-z._-]+", "_", str(s))
```

- [ ] **Step 4: `shap_per_item.py` PNG 區塊改寫**

檔頭 import 改為:
```python
from .paths import diagnostics_dir, per_item_summary_dir, safe_name, summary_dir
```
把原 PNG 區塊(`d = diagnostics_dir(...)` + `shap_summary.png` + `for rank, i in enumerate(hi): waterfall_high_{rank}.png`)整段替換為:
```python
sdir = summary_dir(parameters)
plt.figure()
shap.summary_plot(shap_values, features=X, feature_names=feature_cols, show=False)
plt.tight_layout()
plt.savefig(sdir / "shap_summary_global.png", dpi=100)
plt.close()

if cfg.get("per_item_beeswarm", True):
    pdir = per_item_summary_dir(parameters)
    for item in pd.unique(items):
        m = items == item
        plt.figure()
        shap.summary_plot(shap_values[m], features=X[m],
                          feature_names=feature_cols, show=False)
        plt.tight_layout()
        plt.savefig(pdir / f"shap_summary__{safe_name(item)}.png", dpi=100)
        plt.close()
```
(保留既有 `examples`(high/low/per_item_high)JSON 區塊不動 — back-compat;只移除其 PNG 產出。)

- [ ] **Step 5: 跑測試確認通過(含既有全部)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/paths.py \
        src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
        tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): per-item beeswarm into summary/, drop misleading waterfall_high PNGs"
```

---

## Task 7: P1 config 鍵落 `parameters_training.yaml`

**Files:**
- Modify: `conf/base/parameters_training.yaml`(`diagnostics.shap` 區塊)
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試(toggle 行為)**

```python
def test_per_item_beeswarm_can_be_disabled(shap_setup):
    import os
    from recsys_tfb.pipelines.training.diagnostics.paths import summary_dir
    adapter, handle, preprocessor, parameters = shap_setup
    parameters["diagnostics"]["shap"]["per_item_beeswarm"] = False
    diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert (summary_dir(parameters) / "shap_summary_global.png").exists()
    pidir = summary_dir(parameters) / "per_item"
    assert (not pidir.exists()) or (len(os.listdir(pidir)) == 0)
```

- [ ] **Step 2: 跑測試確認失敗或通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py::test_per_item_beeswarm_can_be_disabled -q`
Expected: 若 Task 6 已讀 `per_item_beeswarm` → 可能 PASS;此 task 主要是把鍵落 yaml + 鎖住行為。

- [ ] **Step 3: 編輯 `conf/base/parameters_training.yaml`**

在 `diagnostics.shap` 區塊(現有 `enabled/sample_rows/top_k/n_examples/min_rows_per_item/max_budget` 之後)加:
```yaml
    # --- P1: per-item 強化（皆 top-level，不影響 model_version）---
    profile_positive: true          # 同時算 label==1 的 signed profile 對照
    positive_min_rows: 20           # 正樣本 < 此 → positive_low_coverage、profile_positive=null
    divergence_metric: jaccard_topk # per-item 驅動排序 vs 全域；∈ {jaccard_topk, spearman}
    divergence_top_k: 15            # 偏離度用的 top-k
    per_item_beeswarm: true         # 每 item 輸出一張 beeswarm（summary/per_item/）
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q`
Expected: PASS(全檔)。

- [ ] **Step 5: Commit**

```bash
git add conf/base/parameters_training.yaml tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): P1 shap config keys (signed/positive/divergence/beeswarm)"
```

---

## Task 8: 文件更新(training.md / design-principles.md / README 最小)

**Files:**
- Modify: `docs/pipelines/training.md`
- Modify: `docs/design-principles.md`
- Modify: `README.md`

- [ ] **Step 1: `docs/pipelines/training.md` 診斷段**

在診斷段落補:per-item `top_features` 含 `mean_signed_shap`(方向)、`top_features_positive`(正樣本對照)+ `positive_low_coverage`、`divergence_from_global` + `item_idiosyncrasy`(點名最該 per-item 化的 item);輸出資料夾 `diagnostics/summary/`(`shap_summary_global.png` + `per_item/shap_summary__<item>.png`);P1 新 config 鍵(`profile_positive / positive_min_rows / divergence_metric / divergence_top_k / per_item_beeswarm`,皆 top-level、不影響 model_version);attribution 接縫定位(two-stage 無關)。

- [ ] **Step 2: `docs/design-principles.md`**

若有「診斷/可解釋性」原則段,補一句:診斷以「per-item 預測能力 / 指向優化方向」為主軸,SHAP 取值經模型結構無關的 `feature_attributions` 接縫。

- [ ] **Step 3: `README.md` 最小調整**

line 37/135 已泛稱「訓練診斷」;至多把「訓練診斷」一處點到「含 per-item SHAP(方向/偏離度)」,**不堆細節**(細節在 `docs/pipelines/training.md`)。

- [ ] **Step 4: Commit**

```bash
git add docs/pipelines/training.md docs/design-principles.md README.md
git commit -m "docs(diagnostics): P1 per-item SHAP (signed/positive/divergence/beeswarm)"
```

---

## P1 完成驗收

- [ ] 全檔測試綠:
```
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics.py \
  tests/test_pipelines/test_training/test_attribution.py -q
```
- [ ] 連帶確認 import 相容(pipeline 仍可建):
```
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from recsys_tfb.pipelines.training.pipeline import create_pipeline; create_pipeline(); print('ok')"
```
- [ ] 不動 `model_version`:確認所有新鍵在 `diagnostics:`(top-level),不在 `training:` block。
- [ ] 開 PR(P1);P2(象限案例)、P3(optuna/學習曲線)各自獨立 plan + PR。

---

## Self-Review(已執行)

- **Spec 覆蓋**:§5.1→Task2;§5.2 signed→Task3 / positive→Task4 / divergence+idiosyncrasy→Task5;§5.3 beeswarm+移除 waterfall→Task6;§5.4 config→Task7;§4 子套件+back-compat→Task1;§5.5 文件→Task8;§5.6 階段取捨(profile_positive 受現有抽樣限制)→已在 Task4 測試以 `positive_min_rows` 門檻表達、文件 Task8 註明。**無 P1 gap。**
- **Placeholder scan**:無 TBD/TODO;每個會動 code 的 step 都附完整 code 或精確 import/呼叫替換。Task1 Step3 為「搬移既有函式體」附精確 import 改動(非佔位)。
- **Type/命名一致**:`_signed_profile`(Task3)回傳 `(profile, mean_abs)` 於 Task5 以 `ai` 取用一致;`_divergence(item_abs, global_abs, metric, k, feature_cols)` 簽章在 Task5 helper 測與整合呼叫一致;`summary_dir/per_item_summary_dir/safe_name`(Task6 paths)與測試引用一致;全域基準變數沿用 `mean_abs`(Task5 Step3 已註明若名稱不同則改 `global_abs`)。

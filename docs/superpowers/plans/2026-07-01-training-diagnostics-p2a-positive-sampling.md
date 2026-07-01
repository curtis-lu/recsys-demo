# Training Diagnostics P2a — 正例 profile 針對正樣本抽樣 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 把 per_item 的 `top_features_positive` 改成「針對 label==1 抽樣」(decoupled sample B),coverage 穩定;`global` / per_item 全列 / divergence / examples / beeswarm **逐位元不變**。

**Architecture:** 於 `compute_shap_diagnostics` 既有 item 分層 sample A(不動)之外,新增正例目標 sample B:讀 `label` 全欄 → `_positive_item_sample` 只在正例中依 item 分層抽 → `take_rows` → 第二次(小)SHAP → 每 item 正例 profile。per_item 的正例三欄改取自 sample B。

**Tech Stack:** Python 3.10、numpy、pandas、shap 0.42、pyarrow 14、pytest。純 python 測試,秒級。

**設計來源:** `docs/superpowers/specs/2026-07-01-training-diagnostics-p2a-positive-sampling-design.md`

**測試執行(worktree,絕對 venv python + PYTHONPATH):**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

---

## File Structure

- **Modify** `src/recsys_tfb/pipelines/training/diagnostics/sampling.py` — 新增 `_positive_item_sample`。
- **Modify** `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py` — 新增 `_positive_profiles` helper + 接線 + config 鍵。
- **Modify** `tests/test_pipelines/test_training/test_diagnostics_sampling.py` — `_positive_item_sample` 測試。
- **Modify** `tests/test_pipelines/test_training/test_diagnostics.py` — 正例 coverage 行為測試 + 全列不變回歸。

回歸網:`tests/test_pipelines/test_training/`(#93 後 122 passed)全程須維持綠(正例相關斷言可依新語意更新,不弱化其他)。

---

## Task 1: `_positive_item_sample`(只在正例中依 item 分層抽)

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/sampling.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics_sampling.py`

- [ ] **Step 1: 寫失敗測試**（append 到 `test_diagnostics_sampling.py`）

```python
from recsys_tfb.pipelines.training.diagnostics.sampling import _positive_item_sample


def test_positive_sample_only_picks_positives():
    items = np.array(["A", "B", "A", "B", "A", "B"])
    labels = np.array([1, 0, 1, 0, 0, 1])
    idx = _positive_item_sample(items, labels, per_item=10, seed=42)
    assert list(labels[idx]) == [1] * len(idx)          # 只抽到正例
    assert set(items[idx]) <= {"A", "B"}


def test_positive_sample_per_item_cap_and_take_all():
    # A 有 3 正例、B 有 1 正例;per_item=2 → A 取 2、B 取 1(不足全取)
    items = np.array(["A", "A", "A", "A", "B", "B"])
    labels = np.array([1, 1, 1, 0, 1, 0])
    idx = _positive_item_sample(items, labels, per_item=2, seed=0)
    picked = items[idx]
    assert (picked == "A").sum() == 2
    assert (picked == "B").sum() == 1
    assert list(idx) == sorted(idx)


def test_positive_sample_empty_when_no_positives():
    items = np.array(["A", "B", "A"])
    labels = np.array([0, 0, 0])
    idx = _positive_item_sample(items, labels, per_item=5, seed=1)
    assert len(idx) == 0


def test_positive_sample_deterministic():
    items = np.array(["A", "B"] * 20)
    labels = np.array([1, 0] * 20)
    a = _positive_item_sample(items, labels, per_item=5, seed=42)
    b = _positive_item_sample(items, labels, per_item=5, seed=42)
    assert list(a) == list(b)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_sampling.py -q -k positive`
Expected: FAIL(`ImportError: cannot import name '_positive_item_sample'`)。

- [ ] **Step 3: 實作**(append 到 `sampling.py`)

```python
def _positive_item_sample(item_values, label_values, per_item, seed):
    """只在 label==1 的列中、依 item 分層抽樣（每 item 至多 per_item,不足全取）。

    回傳選中的 positional indices（升序,對齊 dataset 順序）。用於正例 profile
    的「針對正樣本抽樣」,與全域 item 分層樣本解耦,避免稀疏正樣本 coverage 不足。
    """
    item_values = np.asarray(item_values)
    label_values = np.asarray(label_values)
    pos_all = np.where(label_values == 1)[0]
    if pos_all.size == 0:
        return np.array([], dtype=int)
    rng = np.random.RandomState(seed)
    pos_items = item_values[pos_all]
    selected = []
    for item in pd.unique(pos_items):
        pos = pos_all[pos_items == item]
        take = min(len(pos), int(per_item))
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_sampling.py -q`
Expected: PASS(既有 4 + 新 4 = 8 passed)。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/sampling.py tests/test_pipelines/test_training/test_diagnostics_sampling.py
git commit -m "feat(diagnostics): _positive_item_sample (positive-targeted, item-stratified)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 2: `compute_shap_diagnostics` 用 sample B 算正例 profile

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

- [ ] **Step 1: 寫失敗測試**（append 到 `test_diagnostics.py`;沿用檔頂既有 import：`from recsys_tfb.pipelines.training import diagnostics as diag`、`pa`/`pq`、`ParquetHandle`、`LightGBMAdapter`、numpy/pandas）

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q -k "positive_profile"`
Expected: FAIL(舊實作:正例 profile 走 sample A;`profile_positive=False` 也只有一次 SHAP,但 coverage 測試會因 sample A 撈不到足夠正例而 `top_features_positive` 全 None → 第一個測試 fail)。

- [ ] **Step 3: 實作**

(a) `shap_per_item.py` 讀 config 區(在 `profile_positive = bool(...)` 附近)新增:
```python
    positive_sample_per_item = int(cfg.get("positive_sample_per_item", 30))
```

(b) 新增 module-level helper(放在 `compute_shap_diagnostics` 之前):
```python
def _positive_profiles(model, path, item_values, item_col, label_col, feature_cols,
                       take_cols, preprocessor, parameters, *, profile_positive,
                       per_item, min_rows, top_k):
    """針對正樣本(label==1)抽樣、單獨跑一次 SHAP,回傳每 item 的正例 profile。

    回傳 {item_str: (top_features_positive|None, n_positive, positive_low_coverage)}。
    profile_positive 關閉或資料無 label 欄 → 回傳 {}(呼叫端以預設處理)。與全域
    item 分層樣本解耦,避免稀疏正樣本 coverage 不足。
    """
    from recsys_tfb.io.extract import _pdf_to_X

    if not profile_positive or label_col not in data_access.schema_names(path):
        return {}
    all_labels = data_access.read_column(path, label_col)
    pos_idx = _positive_item_sample(item_values, all_labels, per_item, seed=42)
    if len(pos_idx) == 0:
        return {}
    pos_pdf = data_access.take_rows(path, pos_idx, columns=take_cols).reset_index(drop=True)
    log_data_volume(logger, "shap.positive_sample_pdf", pos_pdf, deep=True)
    X_pos = _pdf_to_X(pos_pdf, preprocessor, parameters)
    with log_step(logger, "shap_values_positive"):
        shap_pos = feature_attributions(model, X_pos, feature_cols)
    pos_items = pos_pdf[item_col].values
    out = {}
    for item in pd.unique(pos_items):
        m = pos_items == item
        n = int(m.sum())
        if n >= min_rows:
            prof, _ = _signed_profile(shap_pos[m], feature_cols, top_k)
            out[str(item)] = (prof, n, False)
        else:
            out[str(item)] = (None, n, True)
    return out
```
注意需 `from .sampling import _positive_item_sample`(把既有 import 由 `from .sampling import _stratified_item_sample` 改為同時 import 兩者)。

(c) 在 `compute_shap_diagnostics` 內、per_item 迴圈**之前**(global 算完後)新增:
```python
    positive_profiles = _positive_profiles(
        model, path, item_values, item_col, label_col, feature_cols, take_cols,
        preprocessor, parameters, profile_positive=profile_positive,
        per_item=positive_sample_per_item, min_rows=positive_min_rows, top_k=top_k)
```

(d) per_item 迴圈內:移除 `labels = ...`(不再需要)與整段 `pos_mask ... prof_pos, pos_low` 計算,改為從 `positive_profiles` 取值。具體:

刪除:
```python
    items = sample_pdf[item_col].values
    labels = sample_pdf[label_col].values if label_col in sample_pdf else np.zeros(len(sample_pdf))
    per_item = {}
```
改為:
```python
    items = sample_pdf[item_col].values
    per_item = {}
```

刪除迴圈內:
```python
        # -- positive-only profile (adopters vs all-rows) --
        pos_mask = mask & (labels == 1)
        n_pos = int(pos_mask.sum())
        if not profile_positive:
            prof_pos, pos_low = None, False
        elif n_pos >= positive_min_rows:
            prof_pos, _ = _signed_profile(shap_values[pos_mask], feature_cols, top_k)
            pos_low = False
        else:
            prof_pos, pos_low = None, True
```
改為:
```python
        # -- positive-only profile (decoupled positive-targeted sample B) --
        prof_pos, n_pos, pos_low = positive_profiles.get(
            str(item), (None, 0, bool(profile_positive)))
```
(per_item dict 組裝的欄位名不變:`n_positive`=n_pos、`top_features_positive`=prof_pos、`positive_low_coverage`=pos_low。)

- [ ] **Step 4: 跑測試確認通過(新測試 + 全 diagnostics 回歸)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py tests/test_pipelines/test_training/test_diagnostics_sampling.py tests/test_pipelines/test_training/test_diagnostics_data_access.py -q`
Expected: PASS。若既有 SHAP 測試對 `top_features_positive` 舊(flaky)值有斷言,依新語意更新(來源改 sample B),不得弱化 `global`/per_item 全列/divergence/examples 的斷言。

- [ ] **Step 5: 全 training-dir 最終回歸**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/ -q`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "feat(diagnostics): positive profile via decoupled positive-targeted sample (fix coverage)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Self-Review(plan 對 spec)

- **Spec 覆蓋**:§3.1 `_positive_item_sample`→Task 1;§3.2 sample B + helper + 接線→Task 2;§3.3 config→Task 2(a);§5 不變式→Task 2 Step 4-5 回歸;§6 測試→Task 1(抽樣正確性)+ Task 2(coverage 修好 / disabled 不跑第二次 SHAP)。
- **型別/簽名一致**:`_positive_item_sample(item_values, label_values, per_item, seed)` Task 1 定義、Task 2 helper 呼叫一致;`_positive_profiles(...)` 回傳 `{item_str:(prof,n,low)}`,per_item 迴圈以 `.get(str(item),(None,0,profile_positive))` 取用一致。
- **無 placeholder**:每步含實際 code 與指令。
- **不變式保護**:sample A 路徑(global/per_item 全列/divergence/examples/beeswarm)完全不動;僅正例三欄來源改 sample B。

# Training Diagnostics 記憶體重構 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 training diagnostics 兩節點（`compute_feature_statistics`、`compute_shap_diagnostics`）「先全量載入再取小樣本」的 driver 記憶體浪費，輸出逐位元不變。

**Architecture:** 抽出 `diagnostics/data_access.py`（唯一碰 `pyarrow.dataset` 的 I/O 層），以「`count_rows` → 算 positional index → `take_rows` 只取那些列」取代全量 `to_pandas()`/`read_table`。利用 cache 是 hive 分區（`snap_date=…/prod_name=…/`）這件事，SHAP 只讀 item 分區欄來分層。讀取採確定性有序（`use_threads=False`、fragment 依序），與 `pyarrow.parquet.read_table` 同順序，故 positional index 可 1:1 對應 → 輸出 byte-for-byte 不變。

**Tech Stack:** Python 3.10、pyarrow 14.0.1、numpy 1.25、pandas 1.5.3、pytest 7.3.1。純 python 測試（無 Spark），秒級。

**設計來源:** `docs/superpowers/specs/2026-07-01-training-diagnostics-memory-refactor-design.md`

**測試執行方式（worktree,務必用絕對 venv python + PYTHONPATH）:**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-mem-refactor
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

---

## File Structure

- **Create** `src/recsys_tfb/pipelines/training/diagnostics/data_access.py` — bounded parquet I/O（`count_rows` / `schema_names` / `read_column` / `take_rows`）。
- **Create** `tests/test_pipelines/test_training/test_diagnostics_data_access.py` — data_access 單元測試（flat + partitioned fixture）。
- **Create** `tests/test_pipelines/test_training/test_diagnostics_sampling.py` — `_stratified_item_sample` 新簽名測試。
- **Modify** `src/recsys_tfb/pipelines/training/diagnostics/sampling.py` — `_stratified_item_sample` 由吃 `pdf, item_col` 改吃 `item_values`。
- **Modify** `src/recsys_tfb/pipelines/training/diagnostics/feature_stats.py` — 改用 data_access。
- **Modify** `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py` — 改用 data_access + 新 sampler 簽名。
- **Modify** `tests/test_pipelines/test_training/test_diagnostics.py` — 補行為測試（no full load）+ hive-partitioned fixture 等價測試。

回歸網:現有 `tests/test_pipelines/test_training/test_diagnostics.py` + `test_attribution.py`（共 24）全程必須維持綠。

---

## Task 1: `data_access.py` — bounded parquet I/O 層

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics/data_access.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics_data_access.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_training/test_diagnostics_data_access.py`:

```python
"""Tests for diagnostics.data_access — bounded parquet reads."""
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pads
import pytest

from recsys_tfb.pipelines.training.diagnostics import data_access as da


def _frame(n=50):
    rng = np.random.RandomState(0)
    return pd.DataFrame({
        "f0": rng.randn(n),
        "f1": rng.randn(n),
        "prod_name": np.where(np.arange(n) % 2 == 0, "A", "B"),
        "snap_date": np.where(np.arange(n) % 3 == 0, "2024-01-31", "2024-02-29"),
        "label": (rng.rand(n) > 0.7).astype(int),
    })


@pytest.fixture
def flat_path(tmp_path):
    pdf = _frame()
    path = str(tmp_path / "flat.parquet")
    # multiple row groups to exercise the batch-gather path
    pq.write_table(pa.Table.from_pandas(pdf), path, row_group_size=7)
    return path, pdf


@pytest.fixture
def part_path(tmp_path):
    pdf = _frame()
    base = str(tmp_path / "parted")
    pads.write_dataset(
        pa.Table.from_pandas(pdf), base, format="parquet",
        partitioning=["snap_date", "prod_name"], partitioning_flavor="hive",
    )
    return base, pdf


def test_count_rows_flat(flat_path):
    path, pdf = flat_path
    assert da.count_rows(path) == len(pdf)


def test_count_rows_partitioned(part_path):
    path, pdf = part_path
    assert da.count_rows(path) == len(pdf)


def test_schema_names_includes_partition_cols(part_path):
    path, _ = part_path
    names = set(da.schema_names(path))
    assert {"f0", "f1", "label", "prod_name", "snap_date"} <= names


def test_read_column_partition_col_reconstructed(part_path):
    path, pdf = part_path
    got = da.read_column(path, "prod_name")
    # order == dataset order == pq.read_table order
    ref = pq.read_table(path).to_pandas()["prod_name"].to_numpy()
    assert list(got) == list(ref)


def test_take_rows_matches_iloc_flat(flat_path):
    path, pdf = flat_path
    ref = pq.read_table(path, columns=["f0", "f1", "label"]).to_pandas()
    idx = np.sort(np.random.RandomState(1).choice(len(pdf), size=10, replace=False))
    got = da.take_rows(path, idx, columns=["f0", "f1", "label"]).reset_index(drop=True)
    exp = ref.iloc[idx].reset_index(drop=True)
    pd.testing.assert_frame_equal(got, exp)


def test_take_rows_matches_iloc_partitioned(part_path):
    path, pdf = part_path
    ref = pq.read_table(path, columns=["f0", "f1", "label", "prod_name"]).to_pandas()
    idx = np.sort(np.random.RandomState(2).choice(len(pdf), size=12, replace=False))
    got = da.take_rows(path, idx, columns=["f0", "f1", "label", "prod_name"])
    got = got.reset_index(drop=True)
    exp = ref.iloc[idx].reset_index(drop=True)
    pd.testing.assert_frame_equal(got[["f0", "f1", "label"]], exp[["f0", "f1", "label"]])
    assert list(got["prod_name"]) == list(exp["prod_name"])


def test_take_rows_empty_returns_typed_empty(flat_path):
    path, _ = flat_path
    got = da.take_rows(path, np.array([], dtype=np.int64), columns=["f0", "label"])
    assert list(got.columns) == ["f0", "label"]
    assert len(got) == 0
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_data_access.py -q`
Expected: FAIL（`ModuleNotFoundError: data_access` / `AttributeError`）。

- [ ] **Step 3: 實作 `data_access.py`**

`src/recsys_tfb/pipelines/training/diagnostics/data_access.py`:

```python
"""Bounded, memory-frugal parquet reads for training diagnostics.

I/O layer: the only place in diagnostics that touches ``pyarrow.dataset``.
Reads operate on the hive-partitioned ``*_model_input`` caches
(``…/snap_date=…/prod_name=…/``) written by the training cache nodes.

Row order is the deterministic path-sorted fragment order (``use_threads=False``),
identical to ``pyarrow.parquet.read_table``. So positional indices computed
against one read map 1:1 onto another — the byte-for-byte invariant the
diagnostics memory refactor relies on.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _dataset(path: str):
    import pyarrow.dataset as pads

    return pads.dataset(path, format="parquet", partitioning="hive")


def count_rows(path: str) -> int:
    """Total row count from parquet metadata (no data scan)."""
    return int(_dataset(path).count_rows())


def schema_names(path: str) -> list:
    """All column names (including hive partition columns)."""
    return list(_dataset(path).schema.names)


def read_column(path: str, col: str) -> np.ndarray:
    """Read a single column (incl. a hive partition column) as a 1-D numpy array.

    Returns all N values in deterministic fragment order. Used to stratify the
    SHAP sample by item without materializing the full feature table.
    """
    table = _dataset(path).to_table(columns=[col])
    return table.column(col).to_numpy(zero_copy_only=False)


def take_rows(path: str, indices, columns: list) -> pd.DataFrame:
    """Read only the rows at positional ``indices``, projected to ``columns``.

    Memory is bounded to the output plus one row-group batch — the full table is
    never materialized. ``indices`` must be sorted ascending; positions index
    into the deterministic fragment order, so the result equals
    ``read_table(path, columns).to_pandas().iloc[indices]`` byte-for-byte.
    """
    import pyarrow as pa

    ds = _dataset(path)
    idx = np.asarray(indices, dtype=np.int64)
    if idx.size == 0:
        return ds.head(0, columns=list(columns)).to_pandas()

    scanner = ds.scanner(columns=list(columns), use_threads=False)
    out_batches = []
    offset = 0
    pos = 0
    n_idx = idx.size
    for batch in scanner.to_batches():
        n = batch.num_rows
        if n == 0:
            continue
        local = []
        while pos < n_idx and idx[pos] < offset + n:
            local.append(int(idx[pos] - offset))
            pos += 1
        if local:
            out_batches.append(batch.take(pa.array(local, type=pa.int64())))
        offset += n
        if pos >= n_idx:
            break
    if not out_batches:
        return ds.head(0, columns=list(columns)).to_pandas()
    return pa.Table.from_batches(out_batches).to_pandas()
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_data_access.py -q`
Expected: PASS（8 passed）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/data_access.py tests/test_pipelines/test_training/test_diagnostics_data_access.py
git commit -m "feat(diagnostics): add data_access bounded parquet I/O (count/read_column/take_rows)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 2: `sampling.py` — `_stratified_item_sample` 改吃 item_values

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/sampling.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics_sampling.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_training/test_diagnostics_sampling.py`:

```python
"""Tests for diagnostics.sampling._stratified_item_sample (item_values 簽名)."""
import numpy as np

from recsys_tfb.pipelines.training.diagnostics.sampling import _stratified_item_sample


def test_returns_sorted_unique_valid_indices():
    items = np.array(["A", "B", "A", "A", "B", "C", "A", "B"])
    idx = _stratified_item_sample(items, total=6, min_per_item=1, seed=42)
    assert list(idx) == sorted(idx)
    assert len(set(idx.tolist())) == len(idx)
    assert idx.min() >= 0 and idx.max() < len(items)


def test_deterministic_same_seed():
    items = np.array(["A", "B"] * 20)
    a = _stratified_item_sample(items, total=8, min_per_item=1, seed=42)
    b = _stratified_item_sample(items, total=8, min_per_item=1, seed=42)
    assert list(a) == list(b)


def test_min_per_item_take_all_when_scarce():
    # C has only 1 row; with min_per_item=3 it is taken in full (take-all).
    items = np.array(["A", "A", "A", "A", "B", "B", "B", "B", "C"])
    idx = _stratified_item_sample(items, total=9, min_per_item=3, seed=0)
    taken = items[idx]
    assert (taken == "C").sum() == 1  # the single C row present


def test_per_item_floor_from_total():
    # 3 items, total=9 -> per_item=max(min, 9//3)=3 each (all have >=3 rows).
    items = np.array(["A"] * 5 + ["B"] * 5 + ["C"] * 5)
    idx = _stratified_item_sample(items, total=9, min_per_item=1, seed=1)
    taken = items[idx]
    assert (taken == "A").sum() == 3
    assert (taken == "B").sum() == 3
    assert (taken == "C").sum() == 3
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_sampling.py -q`
Expected: FAIL（舊簽名 `_stratified_item_sample(pdf, item_col, …)` → `TypeError`）。

- [ ] **Step 3: 改寫 `sampling.py`**

整檔改為（僅簽名/內部由 `pdf[item_col]` 改為 `item_values`；抽樣邏輯、seed、順序不變）:

```python
"""分層抽樣 helper（SHAP 診斷使用）。"""

import numpy as np
import pandas as pd


def _stratified_item_sample(item_values, total, min_per_item, seed):
    """族群代表抽樣：依 item 分層，item 內純隨機；每 item 至少 min_per_item，
    不足全取（take-all）。回傳選中的 positional indices（升序，對齊 dataset 順序）。

    ``item_values`` 是每列的 item 值（1-D array-like，dataset 順序）。行為與過去
    吃整個 pdf 的版本一致：``pd.unique`` 決定 item 順序、``np.where`` 給每 item 的
    升序位置、``rng.choice`` 以固定 seed 抽樣。
    """
    item_values = np.asarray(item_values)
    rng = np.random.RandomState(seed)
    groups = {item: np.where(item_values == item)[0]
              for item in pd.unique(item_values)}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_sampling.py -q`
Expected: PASS（4 passed）。

> 注意：此步驟會讓 `shap_per_item.py` 的呼叫端暫時型別不符（下一 task 修）；但 `compute_shap_diagnostics` 的既有測試會在 Task 4 才恢復綠。本 task 只需新 sampling 測試通過。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/sampling.py tests/test_pipelines/test_training/test_diagnostics_sampling.py
git commit -m "refactor(diagnostics): _stratified_item_sample takes item_values array

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 3: `feature_stats.py` — bounded read via data_access

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/feature_stats.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`（新增行為測試，co-locate 於既有 feature_stats 測試處）

- [ ] **Step 1: 寫失敗（行為）測試**

在 `tests/test_pipelines/test_training/test_diagnostics.py` 末尾新增（沿用該檔既有 import 慣例：`from recsys_tfb.pipelines.training import diagnostics as diag`、`import pyarrow as pa, pyarrow.parquet as pq`、`from recsys_tfb.io.handles import ParquetHandle`）:

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_training/test_diagnostics.py::test_feature_statistics_bounded_take" -q`
Expected: FAIL（舊實作走 `pq.read_table` 全量、不呼叫 `data_access.take_rows` → `KeyError: 'n_indices'`）。

- [ ] **Step 3: 改寫 `feature_stats.py`**

整檔:

```python
"""逐特徵統計（null rate / mean / std / min / max / n_distinct）。"""

import logging

import numpy as np
import pandas as pd

from . import data_access
from ._util import _to_native

logger = logging.getLogger(__name__)


def compute_feature_statistics(train_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """逐特徵 null_rate / mean,std,min,max（數值）/ n_distinct + single_value,high_null 旗標。

    記憶體：先由 metadata 取列數，再只讀抽中的 ``sample_rows`` 列（bounded take），
    不再全量讀入 train 後才下採樣。抽樣 idx 與過去相同（``RandomState(42).choice``），
    輸出逐位元不變。
    """
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])

    path = train_parquet_handle.path
    n = data_access.count_rows(path)
    if n > sample_rows:
        idx = np.sort(np.random.RandomState(42).choice(n, size=sample_rows, replace=False))
        logger.info("feature_statistics: bounded take %d of %d rows", sample_rows, n)
    else:
        idx = np.arange(n, dtype=np.int64)
        logger.info("feature_statistics: reading all %d rows (<= sample_rows)", n)
    pdf = data_access.take_rows(path, idx, columns=feature_cols)

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

- [ ] **Step 4: 跑測試確認通過（新測試 + 既有 feature_stats 回歸）**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q -k "feature_stat or feature_statistics"`
Expected: PASS（含既有 `test_compute_feature_statistics` / `test_compute_feature_statistics_sampling` + 新 `test_feature_statistics_bounded_take`）。

> 若既有 `test_compute_feature_statistics_sampling` 對輸出數值有斷言：因抽樣 idx（同 seed）與讀入列相同，輸出不變，應維持綠。若因舊測試直接依賴 `pq.read_table` mock 而 fail，需調整測試改為對 `data_access` 的等價驗證（不得放寬對數值不變的檢查）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/feature_stats.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "perf(diagnostics): feature_statistics bounded take (count->idx->take), output unchanged

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 4: `shap_per_item.py` — bounded read via data_access

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`（新增行為 + partitioned fixture 測試）

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_pipelines/test_training/test_diagnostics.py` 新增兩個測試:

```python
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
    adapter.train(X, label.astype(float), None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 8, "seed": 7, "num_iterations": 20, "early_stopping_rounds": 0})
    preprocessor = {"feature_columns": ["f0", "f1", "f2", "prod_name"],
                    "categorical_columns": ["prod_name"],
                    "category_mappings": {"prod_name": ["A", "B"]}}
    parameters = {"model_version": "mvpart",
                  "schema": {"item": "prod_name", "label": "label"},
                  "diagnostics": {"shap": {"enabled": True, "top_k": 3, "n_examples": 1,
                                           "min_rows_per_item": 10, "sample_rows": 120,
                                           "max_budget": 4000000}}}
    out = diag.compute_shap_diagnostics(adapter, handle, preprocessor, parameters)
    assert set(out["per_item"]) == {"A", "B"}     # prod_name 從分區重建成功
    assert len(out["global"]["top_features"]) == 3
```

> `shap_setup` fixture 目前寫 flat parquet（`prod_name` 為一般欄）——新程式用 `data_access`（`pads.dataset(path, partitioning="hive")`）對 flat 單檔亦可（單 fragment、`prod_name` 為檔內欄），故既有 24 測試即 flat 路徑回歸網；上面新增 partitioned 測試補分區路徑。若 `shap_setup` 的 `schema` 未顯式指定，`get_schema` 預設 `item=prod_name`/`label=label`，與 fixture 欄名相符。

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py -q -k "full_load or hive_partitioned"`
Expected: FAIL（舊實作呼叫 `to_pandas()` → AssertionError；且 `_stratified_item_sample` 舊簽名已於 Task 2 改掉 → 既有 SHAP 測試此刻亦 red，本 task 一併恢復）。

- [ ] **Step 3: 改寫 `shap_per_item.py` 抽樣段**

`compute_shap_diagnostics` 內，將「載入 + 抽樣」段（現行 `pdf = test_parquet_handle.to_pandas()` … `sample_pdf = pdf.iloc[idx].reset_index(drop=True)`）替換為 bounded 版本。完整替換如下（其餘 global/per_item/examples/PNG 段**不動**）:

將 import 區加入:
```python
from . import data_access
```

將自 `pdf = test_parquet_handle.to_pandas()` 起、至 `sample_pdf = pdf.iloc[idx].reset_index(drop=True)` 止的區塊，改為:

```python
    path = test_parquet_handle.path

    n_trees = attribution_budget_units(model)
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d -> reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    # 只讀 item 分區欄做分層（避免全量物化 test）
    item_values = data_access.read_column(path, item_col)
    idx = _stratified_item_sample(item_values, eff_sample, min_per_item, seed=42)
    if len(idx) == 0:
        logger.warning("shap diagnostics: empty sample after stratification; skipping")
        return {}

    # 只取抽中的列 × (feature 欄 + label 欄)；item_col 已在 feature_cols 內
    take_cols = list(feature_cols)
    if label_col in data_access.schema_names(path) and label_col not in take_cols:
        take_cols.append(label_col)
    sample_pdf = data_access.take_rows(path, idx, columns=take_cols).reset_index(drop=True)
    logger.info("shap diagnostics: n_total=%d n_sampled=%d n_cols=%d",
                len(item_values), len(sample_pdf), len(take_cols))
```

（`X = _pdf_to_X(sample_pdf, preprocessor, parameters)` 及以後全部不變。）

- [ ] **Step 4: 跑測試確認通過（新測試 + 全 diagnostics 回歸）**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics.py tests/test_pipelines/test_training/test_diagnostics_data_access.py tests/test_pipelines/test_training/test_diagnostics_sampling.py -q`
Expected: PASS（全部；含原 24 SHAP/feature_stats 回歸 + 新增測試）。

- [ ] **Step 5: 全 diagnostics 相關測試最終回歸**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/ -q`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py tests/test_pipelines/test_training/test_diagnostics.py
git commit -m "perf(diagnostics): SHAP bounded sampling via item-column read + take_rows, output unchanged

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Self-Review（plan 對 spec）

- **Spec 覆蓋**:§4.1 feature_stats→Task 3;§4.2 SHAP→Task 4;§5 data_access→Task 1、sampling→Task 2;§6 byte-for-byte→既有 24 測試回歸網（Task 3/4 Step 4-5）；§7 觀測性→feature_stats/SHAP 的 log 行；§8 測試策略→Task 1（data_access 單元 + flat/partitioned）、Task 4（no-full-load 行為 + partitioned 等價）、Task 3（bounded take 行為）。無 gap。
- **型別/簽名一致**:`_stratified_item_sample(item_values, total, min_per_item, seed)` 於 Task 2 定義、Task 4 呼叫一致;`data_access.{count_rows,schema_names,read_column,take_rows}` 於 Task 1 定義、Task 3/4 呼叫一致。
- **無 placeholder**:每步含實際 code 與指令。
- **已知順序相依**:Task 2 改 sampler 簽名會讓 SHAP 既有測試暫 red，Task 4 恢復（已於 Task 2 Step 4 註明）。subagent-driven-development 逐 task 執行,故此暫態可接受。

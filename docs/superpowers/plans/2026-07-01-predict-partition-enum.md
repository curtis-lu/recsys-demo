# Predict Partition 枚舉去物化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `predict_and_write_test_predictions` 枚舉 distinct `(snap_date, prod_name)` partition 的方式,從
「投影 partition 欄再 `to_pandas().drop_duplicates()`」(driver 端 O(rows) 材料化)改成
「讀 pyarrow fragment/partition metadata」(O(n_fragments)、零資料掃描),輸出行為完全不變。

**Architecture:** 在 `diagnostics/data_access.py`(PR #93 的唯一 `pyarrow.dataset` I/O 層)新增
`distinct_partitions(path, columns) -> list`,內部用 `Dataset.get_fragments()` +
`pyarrow.dataset.get_partition_keys()` 逐 fragment 取值、去重、排序回傳。
`nodes.py` 的 `predict_and_write_test_predictions` 改呼叫這個新函式取代原本的
`ds.to_table(columns=...).to_pandas().drop_duplicates()...` 區塊,迴圈內部邏輯不動。

**Tech Stack:** Python 3.10 / pyarrow 14.0.1(已釘,免安裝)/ pandas 1.5.3 / pytest 7.3.1。
純 driver 端邏輯,不涉及 Spark。

**Spec:** `docs/superpowers/specs/2026-07-01-predict-partition-enum-design.md`

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/predict-partition-enum`(branch
`feat/predict-partition-enum`,off `origin/main` @ `ac281a0`)。所有指令一律用絕對 venv python +
`PYTHONPATH=src`:

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <path> -q
```

---

### Task 1: `data_access.distinct_partitions()` — TDD

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/data_access.py`(新增函式於檔尾)
- Test: `tests/test_pipelines/test_training/test_diagnostics_data_access.py`(新增 imports + 4 個測試於檔尾)

- [ ] **Step 1: 在測試檔頂部加入 `Path` import**

在 `tests/test_pipelines/test_training/test_diagnostics_data_access.py` 現有 import 區塊
(`"""Tests for diagnostics.data_access — bounded parquet reads."""` 之後)加入:

```python
from pathlib import Path
```

使該區塊變成:

```python
"""Tests for diagnostics.data_access — bounded parquet reads."""
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pads
import pytest

from recsys_tfb.pipelines.training.diagnostics import data_access as da
```

- [ ] **Step 2: 在檔尾(`test_take_rows_empty_returns_typed_empty` 之後)加入 4 個失敗測試**

```python
def test_distinct_partitions_matches_reference_and_is_sorted(part_path):
    path, pdf = part_path
    columns = ["snap_date", "prod_name"]
    expected = sorted(set(pdf[columns].drop_duplicates().itertuples(index=False, name=None)))
    got = da.distinct_partitions(path, columns)
    assert got == expected


def test_distinct_partitions_dedupes_multi_file_partition(tmp_path):
    # Two separate write_dataset calls into the SAME partition dir, mimicking
    # Spark's multi-task output — must collapse to a single tuple.
    base = str(tmp_path / "multi")
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "prod_name": ["prod_A"] * 4,
        "x": range(4),
    })
    pads.write_dataset(
        pa.Table.from_pandas(pdf.iloc[:2]), base, format="parquet",
        partitioning=["snap_date", "prod_name"], partitioning_flavor="hive",
        basename_template="task0-{i}.parquet",
    )
    pads.write_dataset(
        pa.Table.from_pandas(pdf.iloc[2:]), base, format="parquet",
        partitioning=["snap_date", "prod_name"], partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        basename_template="task1-{i}.parquet",
    )
    got = da.distinct_partitions(base, ["snap_date", "prod_name"])
    assert got == [("2025-01-31", "prod_A")]


def test_distinct_partitions_ignores_non_data_files(part_path):
    path, pdf = part_path
    columns = ["snap_date", "prod_name"]
    Path(path, "_SUCCESS").write_text("")
    Path(path, ".DS_Store").write_text("")
    expected = sorted(set(pdf[columns].drop_duplicates().itertuples(index=False, name=None)))
    got = da.distinct_partitions(path, columns)
    assert got == expected


def test_distinct_partitions_empty_dataset_returns_empty_list(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    got = da.distinct_partitions(str(empty_dir), ["snap_date", "prod_name"])
    assert got == []
```

- [ ] **Step 3: 執行測試,確認全部因 `AttributeError` 失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics_data_access.py -k distinct_partitions -v
```
Expected: 4 個測試 FAIL,錯誤訊息含
`AttributeError: module 'recsys_tfb.pipelines.training.diagnostics.data_access' has no attribute 'distinct_partitions'`

- [ ] **Step 4: 在 `data_access.py` 檔尾(`take_rows` 函式之後)加入實作**

```python
def distinct_partitions(path: str, columns: list) -> list:
    """Enumerate distinct hive-partition value tuples for ``columns``.

    Reads fragment/partition metadata only (``Dataset.get_fragments()`` +
    ``pyarrow.dataset.get_partition_keys()``) — O(n_fragments), never O(rows).
    Returns tuples in the given column order, deduplicated and sorted ascending.
    """
    import pyarrow.dataset as pads

    ds = _dataset(path)
    seen = set()
    for fragment in ds.get_fragments():
        keys = pads.get_partition_keys(fragment.partition_expression)
        seen.add(tuple(keys[c] for c in columns))
    return sorted(seen)
```

- [ ] **Step 5: 執行測試,確認全部通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics_data_access.py -v
```
Expected: 全部 PASS(含既有 7 個 + 新增 4 個 = 11 個)。

- [ ] **Step 6: Commit**

```bash
git add tests/test_pipelines/test_training/test_diagnostics_data_access.py \
        src/recsys_tfb/pipelines/training/diagnostics/data_access.py
git commit -m "feat(diagnostics): add distinct_partitions — zero-scan hive partition enumeration"
```

---

### Task 2: 把 `distinct_partitions()` 接進 `predict_and_write_test_predictions`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:803-849`
- Test(不改,僅用於回歸驗證): `tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py`

- [ ] **Step 1: 確認現有 3 個 predict 節點測試目前為 PASS(修改前基線)**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py -v
```
Expected: 3 個 PASS。

- [ ] **Step 2: 編輯 `nodes.py`,把下列區塊(含枚舉物化 + 舊 for-loop 開頭)**

```python
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _pdf_to_X

    schema_cfg = get_schema(parameters)
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    item_col = schema_cfg["item"]
    label_col = schema_cfg["label"]
    if len(entity_cols) != 1:
        raise ValueError(
            f"predict_and_write_test_predictions expects single entity column; "
            f"got {entity_cols}."
        )
    cust_id_col = entity_cols[0]
    model_version = parameters["model_version"]

    # partitioning="hive" tells pyarrow to reconstruct (snap_date, prod_name)
    # columns from the snap_date=*/prod_name=* directory tree produced by
    # HiveTableDataset.save() (and by the test fixture's pq.write_to_dataset).
    ds = pads.dataset(test_parquet_handle.path, format="parquet", partitioning="hive")

    # Enumerate distinct (snap_date, prod_name) values by projecting just the
    # two partition columns and de-duplicating. Note: select-on-partition-cols
    # in pyarrow still materializes one row per data row (the values are filled
    # from directory names per fragment), so this is two-string-columns-wide,
    # not zero I/O. At production scale (~220M rows × 2 short strings) the
    # transient DataFrame fits comfortably on the 128GB driver — much cheaper
    # than reading any feature columns — and drop_duplicates collapses it to
    # n_snap_dates * n_prods rows immediately.
    partition_table = ds.to_table(columns=[time_col, item_col])
    log_data_volume(logger, "predict.partition_table", partition_table)
    partition_pdf = partition_table.to_pandas()
    log_data_volume(logger, "predict.partition_pdf", partition_pdf, deep=False)
    partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])
    log_data_volume(logger, "predict.partition_pdf_unique", partition_pdf, deep=False)

    snap_dates_seen: set[str] = set()
    prods_seen: set[str] = set()
    n_rows_written = 0
    is_calibrated = isinstance(model, CalibratedModelAdapter)

    for _, row in partition_pdf.iterrows():
        snap_date = str(row[time_col])
        prod_name = str(row[item_col])

        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
```

**換成:**

```python
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _pdf_to_X
    from recsys_tfb.pipelines.training.diagnostics import data_access as da

    schema_cfg = get_schema(parameters)
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    item_col = schema_cfg["item"]
    label_col = schema_cfg["label"]
    if len(entity_cols) != 1:
        raise ValueError(
            f"predict_and_write_test_predictions expects single entity column; "
            f"got {entity_cols}."
        )
    cust_id_col = entity_cols[0]
    model_version = parameters["model_version"]

    # partitioning="hive" tells pyarrow to reconstruct (snap_date, prod_name)
    # columns from the snap_date=*/prod_name=* directory tree produced by
    # HiveTableDataset.save() (and by the test fixture's pq.write_to_dataset).
    ds = pads.dataset(test_parquet_handle.path, format="parquet", partitioning="hive")

    snap_dates_seen: set[str] = set()
    prods_seen: set[str] = set()
    n_rows_written = 0
    is_calibrated = isinstance(model, CalibratedModelAdapter)

    # da.distinct_partitions enumerates (snap_date, prod_name) from fragment/
    # directory metadata only (O(n_fragments), zero row scan) — unlike
    # projecting the partition columns via ds.to_table(), which materializes
    # one row per data row before de-duplicating. str() matches the old
    # code's cast exactly: pyarrow infers partition-column types from the
    # directory name (usually str, but int for numeric-looking values), and
    # the old ds.to_table(...).to_pandas() path already applied str() before
    # building the filter below — preserve that here too so behavior
    # (including the pre-existing ArrowNotImplementedError this would raise
    # for numeric-looking partition values, unchanged by this refactor) stays
    # byte-for-byte identical to before.
    for raw_snap_date, raw_prod_name in da.distinct_partitions(
        test_parquet_handle.path, [time_col, item_col]
    ):
        snap_date = str(raw_snap_date)
        prod_name = str(raw_prod_name)

        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
```

其餘函式內容(迴圈內的 `part_table = ds.to_table(filter=...)` 以下到函式結尾)完全不動。

**Code review addendum(於 Task 1 code-quality review 發現,已驗證並收斂於此)**:`pyarrow.dataset.get_partition_keys()` 的回傳型別跟隨 pyarrow 對 hive partition 目錄名的型別推斷——對純數字目錄名(如 `snap_date=20260701`)會推斷成 `int`,不保證是 `str`。已用本機 venv 對 pyarrow 14.0.1 實測確認:**舊碼**(`ds.to_table(columns=...).to_pandas()` → `str(row[...])` → `pads.field(...) == snap_date_str` filter)在這種數字型 partition 情境下,filter 早就會因 `ArrowNotImplementedError: Function 'equal' has no kernel matching input types (int32, string)` 而炸掉——這是**既有限制,不是本次改動造成的退化**。因此上面採用「保留 `str()` cast」的寫法,是刻意讓新碼在任何 partition 值型別下都與舊碼行為(包含這個既有的失敗模式)逐位元相同,而不是趁機修掉這個跟本次任務無關的既有 bug。

- [ ] **Step 3: 執行 predict 節點既有測試,確認行為不變(全 PASS,無需改測試本身)**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py -v
```
Expected: 同 Step 1 的 3 個測試,仍全部 PASS(save call_count==4、manifest 內容、
score/score_uncalibrated 邏輯皆不變)。

- [ ] **Step 4: 連同 Task 1 的新測試一起跑一次,確認組合無干擾**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py \
  tests/test_pipelines/test_training/test_diagnostics_data_access.py -q
```
Expected: 全部 PASS(3 + 11 = 14 個測試)。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py
git commit -m "perf(training): predict partition enum via data_access.distinct_partitions, output unchanged"
```

---

### Task 3: Rebuild graphify graph(專案規則,無需 commit — graphify-out/ 已被 .gitignore 排除)

**Files:** 無(僅重建本機 graph 快取)

- [ ] **Step 1: 執行 rebuild**

Run:
```bash
python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: 印出 `Rebuilt: N nodes, M edges, K communities`,無例外。

（`graphify-out/graph.json` 與 `graphify-out/GRAPH_REPORT.md` 皆在 `.gitignore` 中，本步驟不產生
需要 commit 的變更；graphify 的 post-commit hook 在 Task 1/2 的 commit 時已自動觸發過一次。）

---

## 完成後檢查清單(對照 spec §2/§4/§5)

- [ ] `distinct_partitions` 為 O(n_fragments),不再有任何 `ds.to_table(columns=[...]).to_pandas()`
      枚舉呼叫殘留於 `predict_and_write_test_predictions`。
- [ ] `test_diagnostics_data_access.py` 全綠(11 個測試,含新增 4 個)。
- [ ] `test_predict_and_write_test_predictions.py` 全綠且**檔案本身未被修改**(diff 應為 0)。
- [ ] `nodes.py` 僅有 Task 2 描述的那一段改動,無其他改動(`extract_Xy`/`X[perm]` 等不動)。

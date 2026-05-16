# Training Pipeline 資料量可觀測性 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 training pipeline 所有記憶體高風險物件實體化點，輸出統一 schema 的資料量記錄（筆數/特徵數/bytes/dtype），補滿 4 個未記錄缺口並收斂既有零散 log。

**Architecture:** 在 `core/logging.py` 新增單一 duck-typed helper `log_data_volume`（不 import pandas/numpy/pyarrow/lightgbm，維持 core 零重依賴），對 pandas/numpy/pyarrow/lgb/file 物件輸出 `extra={"event":"data_volume","volume":{...}}`；`JsonFormatter` 白名單加一個 `"volume"` key；逐點埋設並 retrofit 既有 log。

**Tech Stack:** Python 3.10、pytest 7.3.1、pandas 1.5.3、numpy 1.25.0、pyarrow 14.0.1、LightGBM 4.6.0。測試以主 `.venv` 跑：`/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest`（worktree `pyproject` 設 `pythonpath=["src"]`，受測碼為 worktree 的 `src/`）。

**Spec:** `docs/superpowers/specs/2026-05-16-training-memory-observability-design.md`

---

## File Structure

| 檔案 | 責任 | 動作 |
|---|---|---|
| `src/recsys_tfb/core/logging.py` | 新增 `log_data_volume` + `_human_bytes`；`JsonFormatter` 白名單加 `"volume"` | Modify |
| `src/recsys_tfb/io/extract.py` | N1/N2 + retrofit（X_df→deep=True、X/y→helper、移除 shape-only 行、保留 n_groups slim） | Modify |
| `src/recsys_tfb/models/lightgbm_adapter.py` | N3/N4/N5/N6（ds_train/ds_dev/.bin/cache-hit） | Modify |
| `src/recsys_tfb/pipelines/training/nodes.py` | retrofit :362 + N7（finalize）+ N8/N9/N10（predict） | Modify |
| `tests/test_core/test_logging.py` | helper 單元測試 + JSON 整合測試 | Modify |
| `tests/test_io/test_extract.py` | 更新 `test_extract_xy_logs_size_summaries` 斷言 | Modify |

無新檔；helper 併入既有 `core/logging.py`（與 `log_step` 同責任域：結構化記錄）。

---

## Task 1: `log_data_volume` helper — pandas + numpy dispatch

**Files:**
- Modify: `src/recsys_tfb/core/logging.py`（檔尾，`log_step` 之後）
- Test: `tests/test_core/test_logging.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_core/test_logging.py` 檔尾加入：

```python
class TestLogDataVolume:
    def _vol_records(self, caplog):
        return [
            r for r in caplog.records
            if getattr(r, "event", None) == "data_volume"
        ]

    def test_pandas_dataframe(self, caplog):
        import pandas as pd

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "my_df", df)

        recs = self._vol_records(caplog)
        assert len(recs) == 1
        vol = recs[0].volume
        assert vol["name"] == "my_df"
        assert vol["kind"] == "pandas"
        assert vol["rows"] == 3
        assert vol["cols"] == 2
        assert vol["bytes"] > 0
        assert vol["deep"] is True

    def test_numpy_2d_array(self, caplog):
        import numpy as np

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        arr = np.zeros((5, 4), dtype=np.float64)
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "X", arr)

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "numpy"
        assert vol["rows"] == 5
        assert vol["cols"] == 4
        assert vol["bytes"] == 5 * 4 * 8
        assert vol["dtype"] == "float64"

    def test_numpy_1d_array(self, caplog):
        import numpy as np

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "y", np.arange(7))

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "numpy"
        assert vol["rows"] == 7
        assert vol["cols"] == 1
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_logging.py::TestLogDataVolume -q`
Expected: FAIL — `ImportError: cannot import name 'log_data_volume'`

- [ ] **Step 3: 實作 helper（pandas + numpy + 摘要）**

在 `src/recsys_tfb/core/logging.py` 檔尾（`log_step` 函式之後）加入。`Path` 已於檔首 import，無需新增 import：

```python
def _human_bytes(n: "int | None") -> str:
    if n is None:
        return "?"
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(v) < 1024.0:
            return f"{v:.1f}{unit}"
        v /= 1024.0
    return f"{v:.1f}PB"


def log_data_volume(logger, name, obj, *, deep: bool = True, **fields) -> None:
    """Emit a uniform data-volume record for a memory-heavy object.

    Duck-typed dispatch keeps core/logging.py free of pandas/numpy/pyarrow/
    lightgbm imports. Observation must never break the real computation: any
    failure downgrades to a WARNING and returns.
    """
    if obj is None:
        logger.warning(
            "log_data_volume skipped: obj is None name=%s", name,
            extra={"event": "data_volume_skipped"},
        )
        return

    try:
        if hasattr(obj, "num_data"):  # lightgbm.Dataset
            kind, rows, cols = "lgb_dataset", obj.num_data(), obj.num_feature()
            n_bytes, dtype = None, None
        elif hasattr(obj, "memory_usage"):  # pandas.DataFrame
            kind = "pandas"
            rows = len(obj)
            cols = obj.shape[1] if obj.ndim > 1 else 1
            n_bytes = int(obj.memory_usage(deep=deep).sum())
            dts = {str(t) for t in getattr(obj, "dtypes", [])}
            dtype = next(iter(dts)) if len(dts) == 1 else "mixed"
        elif hasattr(obj, "num_rows") and hasattr(obj, "column_names"):  # pyarrow.Table
            kind = "arrow"
            rows, cols, n_bytes, dtype = (
                obj.num_rows, obj.num_columns, obj.nbytes, None
            )
        elif hasattr(obj, "nbytes"):  # numpy.ndarray
            kind = "numpy"
            shape = obj.shape
            rows = shape[0] if shape else 0
            cols = shape[1] if len(shape) > 1 else 1
            n_bytes, dtype = obj.nbytes, str(obj.dtype)
        elif isinstance(obj, (str, Path)):  # file path
            p = Path(obj)
            if not p.exists():
                logger.warning(
                    "log_data_volume skipped: path missing name=%s path=%s",
                    name, p, extra={"event": "data_volume_skipped"},
                )
                return
            kind, rows, cols = "file", None, None
            n_bytes, dtype = p.stat().st_size, None
        else:
            logger.warning(
                "log_data_volume unsupported kind name=%s type=%s",
                name, type(obj).__name__,
                extra={"event": "data_volume_skipped"},
            )
            return
    except Exception as e:  # noqa: BLE001 — observation must not raise
        logger.warning(
            "log_data_volume failed name=%s exc=%s: %s",
            name, type(e).__name__, repr(e)[:200],
            extra={
                "event": "data_volume_skipped",
                "exception_type": type(e).__name__,
            },
        )
        return

    volume = {
        "name": name, "kind": kind, "rows": rows, "cols": cols,
        "bytes": n_bytes, "dtype": dtype, "deep": deep, **fields,
    }
    logger.info(
        "data_volume name=%s kind=%s rows=%s cols=%s bytes=%s dtype=%s",
        name, kind,
        f"{rows:,}" if isinstance(rows, int) else rows,
        cols, _human_bytes(n_bytes), dtype,
        extra={"event": "data_volume", "volume": volume},
    )
```

- [ ] **Step 4: 跑測試確認通過**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_logging.py::TestLogDataVolume -q`
Expected: PASS（3 tests）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/logging.py tests/test_core/test_logging.py
git commit -m "feat(logging): add log_data_volume helper (pandas/numpy)"
```

---

## Task 2: helper — pyarrow / lgb / file 分支 + 分派順序

**Files:**
- Modify: `src/recsys_tfb/core/logging.py`（已於 Task 1 含全部分支；本 task 僅補測試驗證）
- Test: `tests/test_core/test_logging.py`

- [ ] **Step 1: 寫測試（pyarrow、lgb stub、file、分派順序）**

在 `class TestLogDataVolume` 內加入：

```python
    def test_pyarrow_table(self, caplog):
        import pyarrow as pa

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        tbl = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "labels_table", tbl)

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "arrow"
        assert vol["rows"] == 3
        assert vol["cols"] == 2
        assert vol["bytes"] == tbl.nbytes

    def test_lgb_dataset_duck_typed_stub(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        class FakeLgbDataset:
            def num_data(self):
                return 1000

            def num_feature(self):
                return 42

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "ds_train", FakeLgbDataset())

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "lgb_dataset"
        assert vol["rows"] == 1000
        assert vol["cols"] == 42

    def test_file_path(self, caplog, tmp_path):
        from recsys_tfb.core.logging import log_data_volume

        f = tmp_path / "train.bin"
        f.write_bytes(b"\x00" * 2048)
        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "train.bin", str(f))

        vol = self._vol_records(caplog)[0].volume
        assert vol["kind"] == "file"
        assert vol["bytes"] == 2048

    def test_dispatch_order_arrow_not_numpy(self, caplog):
        # pyarrow.Table has BOTH .nbytes and .num_rows; must dispatch as arrow.
        import pyarrow as pa

        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        tbl = pa.table({"x": [1, 2]})
        assert hasattr(tbl, "nbytes")  # would match numpy branch if mis-ordered
        with caplog.at_level(logging.INFO, logger="test_ldv"):
            log_data_volume(logger, "t", tbl)

        assert self._vol_records(caplog)[0].volume["kind"] == "arrow"
```

- [ ] **Step 2: 跑測試確認通過**（分支已於 Task 1 實作）

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_logging.py::TestLogDataVolume" -q`
Expected: PASS（含新增 4 tests，共 7）

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/test_logging.py
git commit -m "test(logging): cover arrow/lgb/file dispatch + ordering"
```

---

## Task 3: helper — error handling（None / unsupported / exception / missing file）

**Files:**
- Modify: `src/recsys_tfb/core/logging.py`（分支已於 Task 1 含；本 task 補測試）
- Test: `tests/test_core/test_logging.py`

- [ ] **Step 1: 寫測試**

在 `class TestLogDataVolume` 內加入：

```python
    def test_none_obj_warns_does_not_raise(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "nothing", None)

        assert not self._vol_records(caplog)
        assert any(
            r.levelno == logging.WARNING and "obj is None" in r.getMessage()
            for r in caplog.records
        )

    def test_unsupported_type_warns(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "weird", object())

        assert not self._vol_records(caplog)
        assert any("unsupported kind" in r.getMessage() for r in caplog.records)

    def test_sizing_exception_is_swallowed(self, caplog):
        from recsys_tfb.core.logging import log_data_volume

        class Exploding:
            def memory_usage(self, deep=True):
                raise RuntimeError("boom")

            ndim = 2
            shape = (1, 1)

            def __len__(self):
                return 1

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "bad", Exploding())  # must NOT raise

        assert not self._vol_records(caplog)
        assert any(
            "log_data_volume failed" in r.getMessage()
            and getattr(r, "exception_type", None) == "RuntimeError"
            for r in caplog.records
        )

    def test_missing_file_path_warns(self, caplog, tmp_path):
        from recsys_tfb.core.logging import log_data_volume

        logger = logging.getLogger("test_ldv")
        with caplog.at_level(logging.WARNING, logger="test_ldv"):
            log_data_volume(logger, "ghost", str(tmp_path / "nope.bin"))

        assert not self._vol_records(caplog)
        assert any("path missing" in r.getMessage() for r in caplog.records)
```

- [ ] **Step 2: 跑測試確認通過**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_logging.py::TestLogDataVolume" -q`
Expected: PASS（共 11 tests）

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/test_logging.py
git commit -m "test(logging): cover log_data_volume error handling"
```

---

## Task 4: JSON 整合 — `JsonFormatter` 白名單加 `"volume"`

**Files:**
- Modify: `src/recsys_tfb/core/logging.py:73-75`
- Test: `tests/test_core/test_logging.py`（`TestJsonFormatter`）

- [ ] **Step 1: 寫失敗測試**

在 `class TestJsonFormatter` 內加入：

```python
    def test_volume_field_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="data_volume", args=(), exc_info=None,
        )
        record.event = "data_volume"
        record.volume = {
            "name": "extract_Xy.pdf", "kind": "pandas",
            "rows": 100, "cols": 12, "bytes": 4096,
            "dtype": "mixed", "deep": True,
        }
        parsed = json.loads(formatter.format(record))
        assert parsed["event"] == "data_volume"
        assert parsed["volume"]["name"] == "extract_Xy.pdf"
        assert parsed["volume"]["rows"] == 100
        assert parsed["volume"]["bytes"] == 4096
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_logging.py::TestJsonFormatter::test_volume_field_included" -q`
Expected: FAIL — `KeyError: 'volume'`

- [ ] **Step 3: 白名單加 `"volume"`**

`src/recsys_tfb/core/logging.py` 第 73-75 行，把：

```python
        for key in ("event", "node", "step", "duration_seconds", "input_names",
                     "output_names", "status", "error_message",
                     "exception_type", "node_count", "dataset_name"):
```

改為：

```python
        for key in ("event", "node", "step", "duration_seconds", "input_names",
                     "output_names", "status", "error_message",
                     "exception_type", "node_count", "dataset_name", "volume"):
```

- [ ] **Step 4: 跑測試確認通過**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_logging.py" -q`
Expected: PASS（全檔，含既有 + 新增）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/logging.py tests/test_core/test_logging.py
git commit -m "feat(logging): surface volume payload in JsonFormatter"
```

---

## Task 5: Retrofit `io/extract.py` — N1/N2 + deep=True + 移除 shape-only

**Files:**
- Modify: `src/recsys_tfb/io/extract.py`
- Test: `tests/test_io/test_extract.py:111`（`test_extract_xy_logs_size_summaries`）

- [ ] **Step 1: 先改既有測試斷言（紅燈）**

`tests/test_io/test_extract.py` 的 `test_extract_xy_logs_size_summaries`（第 111-136 行），把第 119-136 行整段改為：

```python
    vol = {
        r.volume["name"]: r.volume
        for r in caplog.records
        if getattr(r, "event", None) == "data_volume"
    }
    messages = [r.getMessage() for r in caplog.records]
    # Entry summary（保留既有 domain log）
    assert any(
        "extract_Xy start" in m and "n_feature_cols=3" in m and "label=label" in m
        for m in messages
    )
    # N1: full pdf sized via helper (deep=True)
    assert vol["extract_Xy.pdf"]["kind"] == "pandas"
    assert vol["extract_Xy.pdf"]["rows"] == 3
    assert vol["extract_Xy.pdf"]["deep"] is True
    # retrofit: X_df via helper, deep=True (was deep=False)
    assert vol["_pdf_to_X.X_df"]["rows"] == 3
    assert vol["_pdf_to_X.X_df"]["cols"] == 3
    assert vol["_pdf_to_X.X_df"]["deep"] is True
    # encode_categoricals summary（保留既有 domain log）
    assert any(
        "deferred_cats=" in m and "prod_name" in m and "count=1" in m for m in messages
    )
    # retrofit: X / y via helper numpy branch
    assert vol["extract_Xy.X"]["kind"] == "numpy"
    assert vol["extract_Xy.X"]["rows"] == 3
    assert vol["extract_Xy.X"]["cols"] == 3
    assert vol["extract_Xy.y"]["kind"] == "numpy"
    assert vol["extract_Xy.y"]["rows"] == 3
    # D1: shape-only "parquet loaded" line removed
    assert not any("parquet loaded" in m for m in messages)
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_io/test_extract.py::test_extract_xy_logs_size_summaries" -q`
Expected: FAIL — `KeyError: 'extract_Xy.pdf'`（helper 尚未埋設）

- [ ] **Step 3: 改 `io/extract.py`**

3a. 檔首 import 區（第 19 行附近，`from recsys_tfb.core.logging import log_step` 那行）改為：

```python
from recsys_tfb.core.logging import log_data_volume, log_step
```

3b. `_pdf_to_X` 第 90-96 行：

```python
    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    logger.info(
        "_pdf_to_X: X_df rows=%d n_features=%d mem=%.1fMB",
        len(X_df), X_df.shape[1],
        X_df.memory_usage(deep=False).sum() / 1024**2,
    )
```

改為：

```python
    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    log_data_volume(logger, "_pdf_to_X.X_df", X_df, deep=True)
```

3c. `extract_Xy` 第 149-154 行：

```python
    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    logger.info(
        "extract_Xy: parquet loaded rows=%d cols=%d",
        len(pdf), len(pdf.columns),
    )
```

改為（N1，移除 shape-only 行）：

```python
    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    log_data_volume(logger, "extract_Xy.pdf", pdf, deep=True)
```

3d. `extract_Xy` 第 156-163 行：

```python
    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values

    logger.info(
        "extract_Xy: X shape=%s dtype=%s nbytes=%.1fMB; y len=%d dtype=%s",
        X.shape, X.dtype, X.nbytes / 1024**2,
        len(y), y.dtype,
    )

    return X, y
```

改為：

```python
    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values

    log_data_volume(logger, "extract_Xy.X", X)
    log_data_volume(logger, "extract_Xy.y", y)

    return X, y
```

3e. `extract_Xy_with_groups` 第 207-212 行（N2，移除 shape-only）：

```python
    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    logger.info(
        "extract_Xy_with_groups: parquet loaded rows=%d cols=%d",
        len(pdf), len(pdf.columns),
    )
```

改為：

```python
    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    log_data_volume(logger, "extract_Xy_with_groups.pdf", pdf, deep=True)
```

3f. `extract_Xy_with_groups` 第 224-230 行：

```python
    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    logger.info(
        "extract_Xy_with_groups: X_df rows=%d n_features=%d mem=%.1fMB",
        len(X_df), X_df.shape[1],
        X_df.memory_usage(deep=False).sum() / 1024**2,
    )
```

改為：

```python
    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    log_data_volume(logger, "extract_Xy_with_groups.X_df", X_df, deep=True)
```

3g. `extract_Xy_with_groups` 第 245-257 行：

```python
    with log_step(logger, "to_numpy"):
        X = X_df.values
        y = pdf[label_col].values
        groups = (
            pdf.groupby(group_cols, sort=False).ngroup().to_numpy(dtype=np.int64)
        )
    logger.info(
        "extract_Xy_with_groups: X shape=%s dtype=%s nbytes=%.1fMB; "
        "y len=%d dtype=%s; n_groups=%d",
        X.shape, X.dtype, X.nbytes / 1024**2,
        len(y), y.dtype,
        int(groups.max()) + 1 if len(groups) else 0,
    )

    return X, y, groups
```

改為（X/y/groups 走 helper；n_groups 為衍生計數，保留 slim domain log，spec §6.2）：

```python
    with log_step(logger, "to_numpy"):
        X = X_df.values
        y = pdf[label_col].values
        groups = (
            pdf.groupby(group_cols, sort=False).ngroup().to_numpy(dtype=np.int64)
        )
    log_data_volume(logger, "extract_Xy_with_groups.X", X)
    log_data_volume(logger, "extract_Xy_with_groups.y", y)
    log_data_volume(logger, "extract_Xy_with_groups.groups", groups)
    logger.info(
        "extract_Xy_with_groups: n_groups=%d",
        int(groups.max()) + 1 if len(groups) else 0,
    )

    return X, y, groups
```

- [ ] **Step 4: 跑受影響測試確認通過**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py -q`
Expected: PASS（全 11 tests；`test_extract_xy_emits_sub_step_events` 與 `test_extract_xy_logs_parquet_metadata_before_read` 不受影響，因 `log_step` 與 `_log_parquet_metadata` 未動）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "refactor(io): route extract_Xy volume logs through log_data_volume

N1/N2 full-pdf sizing, X_df deep=True (fixes string-col undercount),
X/y/groups via helper, drop redundant shape-only lines (D1)."
```

---

## Task 6: `models/lightgbm_adapter.py` — N3/N4/N5/N6

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py`
- Regression: `tests/test_models/test_adapter.py`, `tests/test_io/test_lightgbm_dataset.py`

> 依 spec §9「埋點不寫專屬測試（YAGNI）」：本 task 為純埋點，不新增 log-assert 測試；驗證＝既有測試續綠 + grep 確認埋點存在。

- [ ] **Step 1: 加 import**

`src/recsys_tfb/models/lightgbm_adapter.py` 檔首 import 區，加入：

```python
from recsys_tfb.core.logging import log_data_volume
```

（若該檔已 import `logger`/logging，維持；只新增上行。）

- [ ] **Step 2: cache-hit 分支（N5，第 141-146 行）**

```python
        if success.exists():
            logger.info("lgb binary cache hit at %s", lgb_dir)
            return (
                LgbDatasetHandle(bin_path=str(train_bin), role="train"),
                LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
            )
```

改為：

```python
        if success.exists():
            logger.info("lgb binary cache hit at %s", lgb_dir)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            return (
                LgbDatasetHandle(bin_path=str(train_bin), role="train"),
                LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
            )
```

- [ ] **Step 3: build 分支（N3/N4/N6，第 172-199 行）**

```python
        X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
        ds_train = lgb.Dataset(
            X_tr,
            label=y_tr,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        ds_train.save_binary(str(train_bin))
        del X_tr, y_tr

        X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)
        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        ds_dev.save_binary(str(dev_bin))
        del X_dev, y_dev, ds_train, ds_dev

        success.touch()
        logger.info(
            "lgb binary cache written: train=%s, train_dev=%s",
            train_bin, dev_bin,
        )
```

改為：

```python
        X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
        ds_train = lgb.Dataset(
            X_tr,
            label=y_tr,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        log_data_volume(logger, "prepare.ds_train", ds_train)
        ds_train.save_binary(str(train_bin))
        log_data_volume(logger, "prepare.train.bin", str(train_bin))
        del X_tr, y_tr

        X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)
        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        log_data_volume(logger, "prepare.ds_dev", ds_dev)
        ds_dev.save_binary(str(dev_bin))
        log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
        del X_dev, y_dev, ds_train, ds_dev

        success.touch()
        logger.info(
            "lgb binary cache written: train=%s, train_dev=%s",
            train_bin, dev_bin,
        )
```

- [ ] **Step 4: 跑回歸測試 + grep 驗證埋點**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_adapter.py tests/test_io/test_lightgbm_dataset.py -q
grep -n "log_data_volume" src/recsys_tfb/models/lightgbm_adapter.py
```
Expected: pytest PASS；grep 顯示 6 處（cache-hit 2 + build ds_train/.bin/ds_dev/.bin 4）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/lightgbm_adapter.py
git commit -m "feat(models): log lgb dataset + .bin volume in prepare_train_inputs"
```

---

## Task 7: `pipelines/training/nodes.py` — retrofit :362 + N7（finalize）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Regression: `tests/test_pipelines/test_training/test_nodes.py`

> 純埋點，不新增 log-assert 測試（spec §9）。

- [ ] **Step 1: 加 import**

`src/recsys_tfb/pipelines/training/nodes.py` 第 13 行：

```python
from recsys_tfb.core.logging import log_step
```

改為：

```python
from recsys_tfb.core.logging import log_data_volume, log_step
```

- [ ] **Step 2: retrofit tune_hyperparameters（第 361-364 行）**

```python
        logger.info(
            "ds_train rows=%d features=%d; ds_dev rows=%d",
            ds_train.num_data(), ds_train.num_feature(), ds_dev.num_data(),
        )
```

改為（helper 補齊 ds_dev.num_feature）：

```python
        log_data_volume(logger, "tune.ds_train", ds_train)
        log_data_volume(logger, "tune.ds_dev", ds_dev)
```

- [ ] **Step 3: N7 — finalize_model concat 後（第 462-464 行）**

```python
    X_full = np.concatenate([X_tr, X_dv], axis=0)
    y_full = np.concatenate([y_tr, y_dv], axis=0)
    del X_tr, y_tr, X_dv, y_dv
```

改為：

```python
    X_full = np.concatenate([X_tr, X_dv], axis=0)
    y_full = np.concatenate([y_tr, y_dv], axis=0)
    log_data_volume(logger, "finalize.X_full", X_full)
    log_data_volume(logger, "finalize.y_full", y_full)
    del X_tr, y_tr, X_dv, y_dv
```

- [ ] **Step 4: 跑回歸測試 + grep 驗證**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -q
grep -n "log_data_volume" src/recsys_tfb/pipelines/training/nodes.py
```
Expected: pytest PASS；grep 顯示 4 處（tune ds_train/ds_dev + finalize X_full/y_full）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py
git commit -m "feat(training): volume logs for tune datasets + finalize concat"
```

---

## Task 8: `pipelines/training/nodes.py` — N8/N9/N10（predict 節點）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`（`predict_and_write_test_predictions`）
- Regression: `tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py`

> 純埋點，不新增 log-assert 測試（spec §9）。import 已於 Task 7 加入。

- [ ] **Step 1: N8 — pass0（第 586-593 行）**

```python
    with log_step(logger, "pass0_positive_set"):
        labels_table = ds.to_table(columns=[cust_id_col, time_col, label_col])
        labels_pdf = labels_table.to_pandas()
        positives_pdf = labels_pdf[labels_pdf[label_col] == 1]
        positive_set: dict[str, set] = {
            str(snap): set(grp[cust_id_col].astype(str))
            for snap, grp in positives_pdf.groupby(time_col)
        }
```

改為：

```python
    with log_step(logger, "pass0_positive_set"):
        labels_table = ds.to_table(columns=[cust_id_col, time_col, label_col])
        log_data_volume(logger, "predict.labels_table", labels_table)
        labels_pdf = labels_table.to_pandas()
        log_data_volume(logger, "predict.labels_pdf", labels_pdf, deep=True)
        positives_pdf = labels_pdf[labels_pdf[label_col] == 1]
        positive_set: dict[str, set] = {
            str(snap): set(grp[cust_id_col].astype(str))
            for snap, grp in positives_pdf.groupby(time_col)
        }
```

- [ ] **Step 2: N9 — partition 列舉（第 610-611 行）**

```python
    partition_pdf = ds.to_table(columns=[time_col, item_col]).to_pandas()
    partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])
```

改為（D3：partition_pdf ~220M×2，deep=False 只取筆數）：

```python
    partition_table = ds.to_table(columns=[time_col, item_col])
    log_data_volume(logger, "predict.partition_table", partition_table)
    partition_pdf = partition_table.to_pandas()
    log_data_volume(logger, "predict.partition_pdf", partition_pdf, deep=False)
    partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])
    log_data_volume(logger, "predict.partition_pdf_unique", partition_pdf, deep=False)
```

- [ ] **Step 3: N10 — 每 partition（第 622-630 行）**

```python
        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
            part_table = ds.to_table(
                filter=(pads.field(time_col) == snap_date)
                & (pads.field(item_col) == prod_name)
            )
            part_pdf = part_table.to_pandas()

            keep_custs = positive_set.get(snap_date, set())
            part_pdf = part_pdf[part_pdf[cust_id_col].astype(str).isin(keep_custs)]
```

改為：

```python
        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
            part_table = ds.to_table(
                filter=(pads.field(time_col) == snap_date)
                & (pads.field(item_col) == prod_name)
            )
            log_data_volume(
                logger, f"predict.part_table[{snap_date}/{prod_name}]", part_table
            )
            part_pdf = part_table.to_pandas()
            log_data_volume(
                logger, f"predict.part_pdf[{snap_date}/{prod_name}]",
                part_pdf, deep=True,
            )

            keep_custs = positive_set.get(snap_date, set())
            part_pdf = part_pdf[part_pdf[cust_id_col].astype(str).isin(keep_custs)]
```

- [ ] **Step 4: 跑回歸測試 + grep 驗證**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py -q
grep -nc "log_data_volume" src/recsys_tfb/pipelines/training/nodes.py
```
Expected: pytest PASS；grep count = 11（Task 7 的 4 + 本 task 7：labels_table/labels_pdf/partition_table/partition_pdf/partition_pdf_unique/part_table/part_pdf）

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py
git commit -m "feat(training): volume logs for predict pass0/partition scan (N8-N10)"
```

---

## Task 9: 全套件回歸 + 收尾

**Files:** 無（驗證）

- [ ] **Step 1: 跑全相關測試套件**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_logging.py tests/test_io/ tests/test_models/ \
  tests/test_pipelines/test_training/ -q
```
Expected: PASS, 0 failed（baseline 25 + 新增；既有 training/io/models 測試不回歸）

- [ ] **Step 2: 同步 graphify（CLAUDE.md 規範）**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: 重建完成；`graphify-out/` 變更不納入功能 commit（hook 產物）。

- [ ] **Step 3: 收尾**

呼叫 superpowers:finishing-a-development-branch，依其指引選擇 merge / PR / cleanup（不自行 push 或 merge，等使用者決定）。

---

## Self-Review

**Spec coverage：**
- §4 helper（pandas/numpy/arrow/lgb/file + 分派順序 + schema）→ Task 1/2
- §5 JSON 整合（白名單 +`volume`）→ Task 4
- §6.1 N1–N10 → N1/N2 Task 5；N3–N6 Task 6；N7 Task 7；N8–N10 Task 8
- §6.2 retrofit（X_df deep=True、X/y helper、移除 shape-only、:362、_log_parquet_metadata 不動、衍生計數不動）→ Task 5（extract）/Task 7（:362）；`_log_parquet_metadata`、n_samples/n 明確不動
- §7 D1（移除 shape-only）Task 5 Step1/3c/3e；D2（per-partition 保留）Task 8 Step3；D3（partition_pdf deep=False）Task 8 Step2
- §8 error handling（None/unsupported/exception/missing-file）→ Task 1 實作 + Task 3 測試
- §9 testing（dispatch/order/deep/error/JSON + 回歸更新 + 埋點不寫專屬測試）→ Task 1–5；Task 6–8 採回歸+grep
- §10 限制對齊：helper 僅 stdlib（`Path` 已 import），duck-typing 不引入新依賴 → Task 1 實作
- §11 out of scope：未改 `pd.read_parquet` 路徑、未動 `_log_parquet_metadata`、無 Spark count、無旗標 → 計畫無相關 task（正確排除）

**Placeholder scan：** 無 TBD/TODO；所有 code step 含完整程式碼。

**Type consistency：** helper 名稱 `log_data_volume`、`_human_bytes` 全篇一致；`extra` key 全篇 `"volume"`；`event` 值 `"data_volume"` / `"data_volume_skipped"` 一致；volume schema key（name/kind/rows/cols/bytes/dtype/deep）Task 1 定義，Task 4/5 斷言一致。

無缺漏。

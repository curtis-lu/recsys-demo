# `extract_Xy` Pre-read Parquet Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `extract_Xy` 的 entry log 之後、`read_parquet` step 之前打一行 metadata-only INFO log（num_rows / num_columns / num_row_groups / total_uncompressed_mb / schema_types），讓 OOM 發生在 `read_parquet` 時仍能事後看見 parquet shape。

**Architecture:** 單檔變動 `src/recsys_tfb/io/extract.py`。資料來源 `pyarrow.dataset.dataset(handle.path, format="parquet")`，同時 cover single-file 與 multi-file directory。probe 包在 try/except 中，失敗時 log WARNING 不擋主流程。

**Tech Stack:** Python 3.10, pyarrow 14.0.1 (`pyarrow.dataset` 模組，pandas/lgb 之外 repo 已依賴), pytest 7.3.1.

**Spec:** `docs/superpowers/specs/2026-05-12-extract-xy-pre-read-metadata-design.md`

---

## File Structure

- **Modify** `src/recsys_tfb/io/extract.py` — 新增一個 module-level `_log_parquet_metadata(handle)` helper（小、純 logging、catch-all 用 broad except 隔離 observability 失敗）+ 在 `extract_Xy` 入口 log 後呼叫
- **Modify** `tests/test_io/test_extract.py` — 2 個新 caplog test：happy path 內容 + 失敗 path 警告

不動 `nodes.py`、不動 `handles.py`、不動 spec 一輪的 sub-step observability。

---

## Task 1: 加上 pre-read parquet metadata observability

**Files:**
- Modify: `tests/test_io/test_extract.py`
- Modify: `src/recsys_tfb/io/extract.py`

- [ ] **Step 1: 寫 failing tests**

在 `tests/test_io/test_extract.py` 結尾 append 兩個新 test：

```python
# ---------------------------------------------------------------------------
# Pre-read parquet metadata observability
# ---------------------------------------------------------------------------


def test_extract_xy_logs_parquet_metadata_before_read(
    tmp_path: Path, caplog
) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    messages = [r.getMessage() for r in caplog.records]
    metadata_logs = [m for m in messages if "parquet metadata" in m]
    assert len(metadata_logs) == 1
    m = metadata_logs[0]
    # _make_df_with_cat → 6 cols: cust_id, snap_date, prod_name, feat_a, feat_b, label
    assert "num_rows=3" in m
    assert "num_columns=6" in m
    assert "num_row_groups=" in m
    assert "total_uncompressed_mb=" in m
    assert "schema_types=" in m

    # Metadata log MUST come BEFORE the read_parquet step_started event,
    # otherwise the whole feature (visible even when read_parquet OOMs) breaks.
    records = caplog.records
    metadata_idx = next(
        i for i, r in enumerate(records) if "parquet metadata" in r.getMessage()
    )
    read_parquet_started_idx = next(
        i
        for i, r in enumerate(records)
        if getattr(r, "event", None) == "step_started"
        and getattr(r, "step", None) == "read_parquet"
    )
    assert metadata_idx < read_parquet_started_idx


def test_extract_xy_metadata_probe_failure_logs_warning_but_does_not_block(
    tmp_path: Path, caplog
) -> None:
    """When the metadata probe raises (e.g. bogus path), log WARNING and
    let extract_Xy proceed; the downstream pandas read will fail loudly on
    its own — we don't want observability to mask or replace that error."""
    from recsys_tfb.io.extract import extract_Xy
    from recsys_tfb.io.handles import ParquetHandle

    bogus = ParquetHandle(path=str(tmp_path / "does_not_exist.parquet"))

    with caplog.at_level(logging.WARNING, logger="recsys_tfb.io.extract"):
        with pytest.raises(Exception):
            extract_Xy(
                bogus, _make_prep_meta_with_cat(), _make_parameters_with_cat()
            )

    warning_messages = [
        r.getMessage() for r in caplog.records if r.levelname == "WARNING"
    ]
    assert any(
        "parquet metadata probe failed" in m for m in warning_messages
    )
```

並在檔案頂端 imports 加上 `pytest`：

```python
"""Tests for io.extract.extract_Xy."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
```

- [ ] **Step 2: 跑 test 確認 FAIL**

```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

預期：先前 4 個 test 通過、2 個新 test FAIL（happy path 找不到 "parquet metadata" 字串；失敗 path 找不到 "parquet metadata probe failed" warning）。

- [ ] **Step 3: 實作**

把 `src/recsys_tfb/io/extract.py` 改成下面這份（在 `extract_Xy` 入口 log 之後、`read_parquet` step 之前插入 `_log_parquet_metadata(handle)` 呼叫；helper 定義在模組層）：

```python
"""Convert a ParquetHandle into algorithm-agnostic numpy (X, y) arrays.

Encapsulates deferred categorical encoding (e.g. prod_name) that the dataset
pipeline keeps as raw string values; downstream training code expects fully
numeric numpy arrays.

Moved out of pipelines/training/nodes.py so that ModelAdapter implementations
(e.g. LightGBMAdapter.prepare_train_inputs) can reuse it without circular
imports.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle

logger = logging.getLogger(__name__)


def _log_parquet_metadata(handle: ParquetHandle) -> None:
    """Log parquet shape & uncompressed size before the actual read.

    Uses pyarrow.dataset so a single .parquet file *and* a multi-file
    parquet directory both work. Metadata-only — no row data read, no
    measurable memory cost.

    Observability failures (e.g. path missing) are caught and downgraded
    to WARNING so the probe never blocks the real read. The downstream
    pandas read will then surface the real error itself.
    """
    path = getattr(handle, "path", "<unknown>")
    try:
        import pyarrow.dataset as pads

        ds = pads.dataset(path, format="parquet")
        n_rows = ds.count_rows()
        n_cols = len(ds.schema)
        total_bytes = 0
        n_row_groups = 0
        for frag in ds.get_fragments():
            md = frag.metadata
            n_row_groups += md.num_row_groups
            for rg_i in range(md.num_row_groups):
                rg = md.row_group(rg_i)
                for col_i in range(rg.num_columns):
                    total_bytes += rg.column(col_i).total_uncompressed_size
        type_counts: dict[str, int] = {}
        for t in ds.schema.types:
            key = str(t)
            type_counts[key] = type_counts.get(key, 0) + 1
        logger.info(
            "extract_Xy: parquet metadata num_rows=%d num_columns=%d "
            "num_row_groups=%d total_uncompressed_mb=%.1f schema_types=%s",
            n_rows, n_cols, n_row_groups,
            total_bytes / 1024**2,
            type_counts,
        )
    except Exception as e:
        logger.warning(
            "extract_Xy: parquet metadata probe failed path=%s err=%s",
            path, e,
        )


def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.

    Emits sub-step ``log_step`` events (``read_parquet`` → ``slice_features`` →
    ``encode_categoricals`` (skipped when no deferred cats) → ``to_numpy``) and
    per-step INFO size summaries so OOM-killed runs can be diagnosed from log.
    A pre-read parquet metadata INFO is also emitted before ``read_parquet`` so
    shape/uncompressed-size are visible even if the pandas read OOMs.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

    logger.info(
        "extract_Xy start path=%s n_feature_cols=%d label=%s identity_cols=%s",
        getattr(handle, "path", "<unknown>"),
        len(feature_cols),
        label_col,
        identity_cols,
    )

    _log_parquet_metadata(handle)

    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    logger.info(
        "extract_Xy: parquet loaded rows=%d cols=%d",
        len(pdf), len(pdf.columns),
    )

    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    logger.info(
        "extract_Xy: X_df rows=%d n_features=%d mem=%.1fMB",
        len(X_df), X_df.shape[1],
        X_df.memory_usage(deep=False).sum() / 1024**2,
    )

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes
        logger.info(
            "extract_Xy: encoded deferred_cats=%s count=%d",
            deferred_cats, len(deferred_cats),
        )

    with log_step(logger, "to_numpy"):
        X = X_df.values
        y = pdf[label_col].values
    logger.info(
        "extract_Xy: X shape=%s dtype=%s nbytes=%.1fMB; y len=%d dtype=%s",
        X.shape, X.dtype, X.nbytes / 1024**2,
        len(y), y.dtype,
    )

    return X, y
```

- [ ] **Step 4: 跑 test 確認 PASS**

```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

預期：6 個 test 全綠（4 個既有 + 2 個新）。

- [ ] **Step 5: 跑相鄰 suite 確認沒 regress**

```bash
.venv/bin/pytest tests/test_io/ tests/test_core/test_logging.py tests/test_pipelines/ -v
```

預期：全綠（先前 296 個 pass 的範圍）。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "$(cat <<'EOF'
feat(io.extract): extract_Xy 加上 pre-read parquet metadata observability

在 entry log 之後、read_parquet step 之前打一行 metadata-only INFO：
num_rows / num_columns / num_row_groups / total_uncompressed_mb / schema_types。
資料來源 pyarrow.dataset.dataset()，同時 cover single-file 與 multi-file
parquet directory。probe 包在 try/except，失敗 log WARNING 不擋主流程。

公司環境 1.7GB on-disk val_model_input.parquet 在 64GB RAM 環境仍 OOM 於
to_pandas()；缺解壓縮後實際大小的證據。本 patch 補上這條 log，下次 kill
仍能事後看見 parquet 真實 shape，據此再開新 spec 修 OOM 本身。

Spec: docs/superpowers/specs/2026-05-12-extract-xy-pre-read-metadata-design.md
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- entry log 後、`read_parquet` 前打 metadata INFO → Step 3 `_log_parquet_metadata(handle)` 呼叫位置 ✓
- 五欄資訊（`num_rows` / `num_columns` / `num_row_groups` / `total_uncompressed_mb` / `schema_types`）→ Step 3 `logger.info(...)` 格式字串 ✓
- 使用 `pyarrow.dataset.dataset(path, format="parquet")` 同時支援 single-file 與 multi-file directory → Step 3 ✓
- 失敗 path 用 WARNING 不擋主流程 → Step 3 try/except + Step 1 第二個 test ✓
- 不快取、不 thresholding、不動其他 `to_pandas` → 只在 `_log_parquet_metadata` 與 `extract_Xy` 入口加一行呼叫，無其他改動 ✓
- 預期 log 在 `Step started: read_parquet` 之前出現 → Step 1 第一個 test 用 `metadata_idx < read_parquet_started_idx` 驗證 ✓

**Placeholder scan:** 無 TBD / TODO；test 程式碼與實作碼完整可貼可跑。✓

**Type / name consistency:** `parquet metadata` 字串、`num_rows=` / `num_columns=` / `num_row_groups=` / `total_uncompressed_mb=` / `schema_types=` 五個 token、`parquet metadata probe failed` warning 字串，test 與實作完全對齊；helper 名稱 `_log_parquet_metadata`、變數名 `n_rows` / `n_cols` / `n_row_groups` / `total_bytes` / `type_counts` 內部一致。✓

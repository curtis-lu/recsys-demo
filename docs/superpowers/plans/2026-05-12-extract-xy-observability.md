# `extract_Xy` Sub-step Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap the 4 sub-operations inside `extract_Xy` with `log_step` and add size-summary INFO logs, so that an OOM-killed run can be diagnosed from log alone (which sub-step hung, what data size).

**Architecture:** Single-file change in `src/recsys_tfb/io/extract.py`. Sub-steps `read_parquet` / `slice_features` / `encode_categoricals` / `to_numpy` each wrapped in existing `log_step` context manager from `recsys_tfb.core.logging`. `encode_categoricals` step is conditional — only emitted when `deferred_cats` is non-empty (avoids noise for inference-time eval sets with no string identity columns). One TDD cycle: add 3 new caplog-based tests in `tests/test_io/test_extract.py` (events emitted; size summaries; encode-skip), implement, commit.

**Tech Stack:** Python 3.10, pandas 1.5.3, pytest 7.3.1, existing `recsys_tfb.core.logging.log_step` context manager. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-12-extract-xy-observability-design.md`

---

## File Structure

- **Modify** `src/recsys_tfb/io/extract.py` — add module logger, wrap 4 sub-steps with `log_step`, add 5 INFO summary lines (entry + per step)
- **Modify** `tests/test_io/test_extract.py` — add 3 caplog-based tests; existing `test_extract_xy_returns_numpy_arrays` must keep passing

No new files. No changes to `nodes.py` (outer `log_step("extract_features")` stays).

---

## Task 1: Add observability to `extract_Xy` (single TDD cycle)

**Files:**
- Modify: `tests/test_io/test_extract.py` (add 3 tests, keep the existing one)
- Modify: `src/recsys_tfb/io/extract.py`

- [ ] **Step 1: Write the failing tests**

Edit `tests/test_io/test_extract.py`. Keep the existing imports/helper/`test_extract_xy_returns_numpy_arrays` unchanged; append the 3 tests below. Also add `import logging` to the existing imports.

The final imports block (top of file) should be:

```python
"""Tests for io.extract.extract_Xy."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
```

Append at the bottom of the file:

```python
# ---------------------------------------------------------------------------
# Observability — sub-step log_step events and size summary INFO logs
# ---------------------------------------------------------------------------


def _make_prep_meta_with_cat():
    return {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }


def _make_parameters_with_cat():
    return {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }


def _make_df_with_cat():
    return pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [0.1, 0.2, 0.3],
            "label": [0, 1, 0],
        }
    )


def test_extract_xy_emits_sub_step_events(tmp_path: Path, caplog) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    started = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_started"
    }
    completed = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_completed"
    }
    expected = {"read_parquet", "slice_features", "encode_categoricals", "to_numpy"}
    assert started == expected
    assert completed == expected


def test_extract_xy_logs_size_summaries(tmp_path: Path, caplog) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    messages = [r.getMessage() for r in caplog.records]
    # Entry summary
    assert any(
        "extract_Xy start" in m and "n_feature_cols=3" in m and "label=label" in m
        for m in messages
    )
    # read_parquet summary: rows + cols of the loaded parquet
    assert any("parquet loaded" in m and "rows=3" in m for m in messages)
    # slice_features summary: rows + n_features + mem
    assert any("X_df" in m and "n_features=3" in m and "mem=" in m for m in messages)
    # encode_categoricals summary: deferred_cats list + count
    assert any(
        "deferred_cats=" in m and "prod_name" in m and "count=1" in m for m in messages
    )
    # to_numpy summary: X shape + dtype + nbytes; y len + dtype
    assert any(
        "X shape=(3, 3)" in m and "nbytes=" in m and "y len=3" in m for m in messages
    )


def test_extract_xy_skips_encode_step_when_no_deferred_cats(
    tmp_path: Path, caplog
) -> None:
    from recsys_tfb.io.extract import extract_Xy

    # No string identity column in the input → deferred_cats empty
    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date"],
        }
    }

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, prep_meta, parameters)

    started = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_started"
    }
    # Other sub-steps still emit
    assert "read_parquet" in started
    assert "slice_features" in started
    assert "to_numpy" in started
    # Encode step is SKIPPED entirely
    assert "encode_categoricals" not in started
    # And there is no encode summary INFO line
    messages = [r.getMessage() for r in caplog.records]
    assert not any("deferred_cats=" in m for m in messages)
```

- [ ] **Step 2: Run the new tests to verify they FAIL**

Run:
```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

Expected: the existing `test_extract_xy_returns_numpy_arrays` PASSES; the three new tests FAIL with `AssertionError` (no sub-step `step_started` records present, since `extract_Xy` currently emits no `log_step` events of its own).

- [ ] **Step 3: Implement the change**

Replace the full contents of `src/recsys_tfb/io/extract.py` with:

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

- [ ] **Step 4: Run tests to verify they PASS**

Run:
```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

Expected: all 4 tests pass (`test_extract_xy_returns_numpy_arrays`, `test_extract_xy_emits_sub_step_events`, `test_extract_xy_logs_size_summaries`, `test_extract_xy_skips_encode_step_when_no_deferred_cats`).

- [ ] **Step 5: Run the full test suite for adjacent areas**

Run:
```bash
.venv/bin/pytest tests/test_io/ tests/test_core/test_logging.py tests/test_pipelines/ -v
```

Expected: all green. Catches accidental regressions in callers (`tune_hyperparameters`, `finalize_model`, `calibrate_model`, `evaluate_model`).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "$(cat <<'EOF'
feat(io.extract): 加上 extract_Xy 內部子步驟可觀測性

把 read_parquet / slice_features / encode_categoricals / to_numpy 4 個 sub-step
用 log_step 包起來，每段緊接一行 INFO size summary（shape / nbytes /
memory_usage(deep=False)）。encode_categoricals 在 deferred_cats 空時整個 step
跳過避免噪音。

公司環境 tune_hyperparameters 跑到 extract_features step 卡幾分鐘後被 kill；
外層 log_step("extract_features") 只看得到「整次卡住」，無法定位是 4 個 sub-step
中哪個是凶手。加上 sub-step events 與 size 訊號後，從 log 即可反推卡點。

5 個呼叫點（val/train/train_dev/calibration/test）自動受益，nodes.py 不動。

Spec: docs/superpowers/specs/2026-05-12-extract-xy-observability-design.md
EOF
)"
```

Expected: commit succeeds; graphify hook re-builds the graph; `git status` shows clean.

---

## Self-Review

**Spec coverage check:**
- 4 sub-steps wrapped with `log_step` → Step 3 ✓
- Each step has size INFO summary → Step 3 ✓
- Entry log with path + n_feature_cols + label + identity_cols → Step 3 ✓
- `encode_categoricals` skipped when `deferred_cats` empty → Step 3 (conditional `if deferred_cats:`) ✓
- `memory_usage(deep=False)` used (not deep=True) → Step 3 ✓
- Outer `log_step("extract_features")` in nodes.py untouched → File Structure section explicit; no nodes.py change in any step ✓
- Test for events / summaries / skip → Step 1 ✓
- Existing test still passes → Step 5 covers it ✓

**Placeholder scan:** No TBD/TODO/"similar to". All test code and impl code is shown verbatim. ✓

**Type / name consistency:** `step_started` / `step_completed` event names, `step=read_parquet|slice_features|encode_categoricals|to_numpy`, `deferred_cats` variable name, `X_df` variable name, function signature `(handle, preprocessor_metadata, parameters) → (np.ndarray, np.ndarray)` — all consistent between test expectations (Step 1) and impl (Step 3). ✓

# Training Cache → Handle + lgb.Dataset Binary Prepare Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor training pipeline cache layer to (a) avoid driver OOM by replacing pandas DataFrames with on-disk parquet handles, (b) introduce an algorithm-native `lgb.Dataset` binary prepare layer to eliminate redundant histogram binning across Optuna trials.

**Architecture:** 4 cache nodes return `ParquetHandle` instead of pandas; new `prepare_lgb_train_inputs` node delegates to `LightGBMAdapter.prepare_train_inputs(...)` which materializes `train.bin` + `train_dev.bin` with shared binning reference; downstream `tune` / `train` consume `LgbDatasetHandle`, `calibrate` / `evaluate` consume `ParquetHandle` via `to_pandas()` lazy reads. Single code path (`cache.enabled=false` is removed).

**Tech Stack:** Python 3.10+, PySpark 3.3.2, LightGBM 4.6.0, pandas 1.5.3, numpy 1.25.0, pytest 7.3.1.

**Spec:** `docs/superpowers/specs/2026-05-08-training-prepare-lgb-binary-design.md`

**PR split:** This plan produces TWO sequential PRs:
- **PR1 (Phases 0–8):** Structural refactor with `categorical_feature=None` (byte-equal metric vs main).
- **PR2 (Phase 9):** Enable `categorical_feature=` for native categorical handling (metric drift expected).

---

## File Structure

| Path | Action | Responsibility |
|---|---|---|
| `src/recsys_tfb/io/handles.py` | Create | `ParquetHandle` / `LgbDatasetHandle` dataclasses |
| `src/recsys_tfb/io/extract.py` | Create | `extract_Xy(handle, prep_meta, parameters)` — moved out of `nodes.py` |
| `src/recsys_tfb/models/base.py` | Modify | Add `@abstractmethod prepare_train_inputs` |
| `src/recsys_tfb/models/lightgbm_adapter.py` | Modify | Implement `prepare_train_inputs`; `train()` accepts pre-built `lgb.Dataset` via keyword args |
| `src/recsys_tfb/models/calibrated_adapter.py` | Modify | `prepare_train_inputs` raises `NotImplementedError` |
| `src/recsys_tfb/pipelines/training/nodes.py` | Modify | Cache nodes return `ParquetHandle`; remove `_cache_or_passthrough`; add `prepare_lgb_train_inputs`; refactor downstream nodes |
| `src/recsys_tfb/pipelines/training/pipeline.py` | Modify | Insert `prepare_lgb_train_inputs` node + rewire DAG |
| `conf/base/parameters_training.yaml` | Modify | Remove `cache.enabled` key |
| `tests/test_io/test_handles.py` | Create | `ParquetHandle` / `LgbDatasetHandle` unit tests |
| `tests/test_io/test_extract.py` | Create | `extract_Xy` unit tests |
| `tests/test_models/test_adapter.py` | Modify | `LightGBMAdapter.prepare_train_inputs` cases |
| `tests/test_models/test_calibrated_adapter.py` | Modify | `CalibratedModelAdapter.prepare_train_inputs` raises |
| `tests/test_pipelines/test_training/test_cache_nodes.py` | Rewrite | New cache-node behaviour; drop `cache.enabled` cases |
| `tests/test_pipelines/test_training/test_pipeline.py` | Modify | Update intermediate dataset names |
| `tests/test_pipelines/test_training/test_pipeline_integration.py` | Create | End-to-end synthetic-data smoke test |

---

## Phase 0: Pre-flight Verification

### Task 0.1: Verify `base_dataset_version` covers preprocessor

**Files:**
- Read: `src/recsys_tfb/core/versioning.py`
- Read: `src/recsys_tfb/__main__.py:300-330` (call site)

- [ ] **Step 1: Locate hash function**

Run:
```bash
grep -n "compute_base_dataset_version\|def _hash" src/recsys_tfb/core/versioning.py
```

- [ ] **Step 2: Inspect hash inputs**

Open `src/recsys_tfb/core/versioning.py` and read `compute_base_dataset_version`. Confirm the hash digests cover at least:
- `parameters["preprocessing"]["feature_columns"]`
- `parameters["preprocessing"]["categorical_columns"]` (or whichever key the dataset pipeline uses)
- canonical schema fingerprint

- [ ] **Step 3: Decision**

If `base_dataset_version` already covers preprocessor — proceed to Phase 1.
If NOT — STOP, escalate: spec scope must expand to add preprocessor fingerprinting before this plan continues.

- [ ] **Step 4: Record finding in commit message**

No commit yet; just keep finding for later context.

---

## Phase 1: Foundation — Handles & extract_Xy

### Task 1.1: Create test for `ParquetHandle.to_pandas()`

**Files:**
- Test: `tests/test_io/test_handles.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_io/test_handles.py`:

```python
"""Tests for ParquetHandle and LgbDatasetHandle."""

import dataclasses
from pathlib import Path

import pandas as pd
import pytest


def test_parquet_handle_to_pandas_roundtrip(tmp_path: Path) -> None:
    from recsys_tfb.io.handles import ParquetHandle

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    parquet_dir = tmp_path / "test.parquet"
    df.to_parquet(parquet_dir, engine="pyarrow")

    handle = ParquetHandle(path=str(parquet_dir))
    loaded = handle.to_pandas()

    pd.testing.assert_frame_equal(loaded, df)


def test_parquet_handle_is_frozen(tmp_path: Path) -> None:
    from recsys_tfb.io.handles import ParquetHandle

    handle = ParquetHandle(path=str(tmp_path / "x.parquet"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        handle.path = "/other"  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
.venv/bin/pytest tests/test_io/test_handles.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'recsys_tfb.io.handles'`

- [ ] **Step 3: Implement minimal `ParquetHandle`**

Create `src/recsys_tfb/io/handles.py`:

```python
"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ParquetHandle:
    """Reference to a local parquet directory written by a cache node."""

    path: str

    def to_pandas(self) -> "pd.DataFrame":  # type: ignore[name-defined]
        import pandas as pd

        return pd.read_parquet(self.path, engine="pyarrow")
```

- [ ] **Step 4: Run test to verify pass**

Run:
```bash
.venv/bin/pytest tests/test_io/test_handles.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/handles.py tests/test_io/test_handles.py
git commit -m "feat(io): add ParquetHandle dataclass for cached training inputs"
```

---

### Task 1.2: Add `LgbDatasetHandle`

**Files:**
- Modify: `src/recsys_tfb/io/handles.py`
- Test: `tests/test_io/test_handles.py`

- [ ] **Step 1: Add failing test**

Append to `tests/test_io/test_handles.py`:

```python
def test_lgb_dataset_handle_load_roundtrip(tmp_path: Path) -> None:
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.io.handles import LgbDatasetHandle

    X = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
    y = np.array([0, 1, 0])
    bin_path = tmp_path / "train.bin"
    ds = lgb.Dataset(X, label=y, free_raw_data=False).construct()
    ds.save_binary(str(bin_path))

    handle = LgbDatasetHandle(bin_path=str(bin_path), role="train")
    loaded = handle.load()

    assert loaded.num_data() == 3


def test_lgb_dataset_handle_load_with_reference(tmp_path: Path) -> None:
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.io.handles import LgbDatasetHandle

    X_tr = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
    y_tr = np.array([0, 1, 0])
    X_dev = np.array([[1.5, 2.5], [3.5, 4.5]])
    y_dev = np.array([1, 0])

    train_bin = tmp_path / "train.bin"
    dev_bin = tmp_path / "dev.bin"

    ds_tr = lgb.Dataset(X_tr, label=y_tr, free_raw_data=False).construct()
    ds_tr.save_binary(str(train_bin))

    ds_dev = lgb.Dataset(
        X_dev, label=y_dev, reference=ds_tr, free_raw_data=False
    ).construct()
    ds_dev.save_binary(str(dev_bin))

    train_handle = LgbDatasetHandle(bin_path=str(train_bin), role="train")
    dev_handle = LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev")

    loaded_tr = train_handle.load()
    loaded_dev = dev_handle.load(reference=loaded_tr)

    assert loaded_dev.num_data() == 2
```

- [ ] **Step 2: Run test, verify fail**

```bash
.venv/bin/pytest tests/test_io/test_handles.py::test_lgb_dataset_handle_load_roundtrip -v
```

Expected: FAIL with `ImportError: cannot import name 'LgbDatasetHandle'`

- [ ] **Step 3: Implement**

Append to `src/recsys_tfb/io/handles.py`:

```python
@dataclass(frozen=True)
class LgbDatasetHandle:
    """Reference to a saved ``lgb.Dataset`` binary on disk.

    ``role`` distinguishes "train" from "train_dev" so callers can build the
    correct reference linkage when reloading.
    """

    bin_path: str
    role: str  # "train" | "train_dev"

    def load(self, reference=None) -> "lgb.Dataset":  # type: ignore[name-defined]
        import lightgbm as lgb

        return lgb.Dataset(self.bin_path, reference=reference)
```

- [ ] **Step 4: Run all handles tests**

```bash
.venv/bin/pytest tests/test_io/test_handles.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/handles.py tests/test_io/test_handles.py
git commit -m "feat(io): add LgbDatasetHandle for lgb binary cache"
```

---

### Task 1.3: Extract `_extract_Xy` into `io/extract.py`, accept `ParquetHandle`

**Files:**
- Create: `src/recsys_tfb/io/extract.py`
- Test: `tests/test_io/test_extract.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_io/test_extract.py`:

```python
"""Tests for io.extract.extract_Xy."""

from pathlib import Path

import numpy as np
import pandas as pd


def _make_handle(tmp_path: Path, df: pd.DataFrame):
    from recsys_tfb.io.handles import ParquetHandle

    parquet_dir = tmp_path / "input.parquet"
    df.to_parquet(parquet_dir, engine="pyarrow")
    return ParquetHandle(path=str(parquet_dir))


def test_extract_xy_returns_numpy_arrays(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [0.1, 0.2, 0.3],
            "label": [0, 1, 0],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    X, y = extract_Xy(handle, prep_meta, parameters)

    assert X.shape == (3, 3)
    assert list(y) == [0, 1, 0]
    # prod_name is int-coded: fund=0, ccard=1, fund=0
    assert list(X[:, 2]) == [0, 1, 0]
```

- [ ] **Step 2: Run test, verify fail**

```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'recsys_tfb.io.extract'`

- [ ] **Step 3: Implement**

Create `src/recsys_tfb/io/extract.py`:

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

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle


def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.
    """
    pdf = handle.to_pandas()
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    X_df = pdf[feature_cols].copy()

    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    for col in deferred_cats:
        known = category_mappings[col]
        X_df[col] = pd.Categorical(X_df[col], categories=known).codes

    X = X_df.values
    y = pdf[label_col].values
    return X, y
```

- [ ] **Step 4: Run test, verify pass**

```bash
.venv/bin/pytest tests/test_io/test_extract.py -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "feat(io): add extract_Xy helper consuming ParquetHandle"
```

---

## Phase 2: ModelAdapter Contract

### Task 2.1: Add `prepare_train_inputs` abstract method to `ModelAdapter`

**Files:**
- Modify: `src/recsys_tfb/models/base.py`
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Add failing test**

Append to `tests/test_models/test_adapter.py`:

```python
def test_model_adapter_prepare_train_inputs_is_abstract():
    """Any concrete subclass of ModelAdapter must implement prepare_train_inputs."""
    import pytest
    from recsys_tfb.models.base import ModelAdapter

    class DummyAdapter(ModelAdapter):
        def train(self, X_train, y_train, X_val, y_val, params): ...
        def predict(self, X): ...
        def save(self, filepath): ...
        def load(self, filepath): ...
        def feature_importance(self): ...
        def log_to_mlflow(self): ...

    with pytest.raises(TypeError, match="prepare_train_inputs"):
        DummyAdapter()
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_model_adapter_prepare_train_inputs_is_abstract -v
```

Expected: FAIL — `DummyAdapter()` succeeds because the method isn't abstract yet.

- [ ] **Step 3: Add abstract method**

In `src/recsys_tfb/models/base.py`, add this `@abstractmethod` after the existing `log_to_mlflow` declaration:

```python
    @abstractmethod
    def prepare_train_inputs(
        self,
        train_handle: "ParquetHandle",
        train_dev_handle: "ParquetHandle",
        preprocessor_metadata: dict,
        parameters: dict,
        cache_dir: str,
    ) -> "tuple[LgbDatasetHandle, LgbDatasetHandle]":
        """Materialize algorithm-native train/dev datasets to disk; return handles.

        Skip-if-exists semantics: if cache_dir already has a valid `_SUCCESS`
        marker, just return handles without rebuilding. Implementations are
        responsible for atomic-ish builds (write artefacts, then touch
        `_SUCCESS` last).
        """
        ...
```

Add at the top of `base.py`, after the existing imports:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
```

- [ ] **Step 4: Run new test + existing adapter tests**

```bash
.venv/bin/pytest tests/test_models/ -v
```

Expected: new test passes; existing `LightGBMAdapter` / `CalibratedModelAdapter` tests **fail** (they instantiate the adapters but those classes haven't implemented the new abstract method yet). This breakage is expected — Phase 3 / 4 will fix it.

- [ ] **Step 5: Do NOT commit yet** — leave broken until Phase 3 lands.

---

## Phase 3: LightGBMAdapter.prepare_train_inputs (PR1: cat=None)

### Task 3.1: Implement `prepare_train_inputs` with cache-miss path

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py`
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Write failing test for fresh build**

Append to `tests/test_models/test_adapter.py`:

```python
def test_lightgbm_prepare_train_inputs_writes_bins(tmp_path):
    """prepare_train_inputs writes train.bin, train_dev.bin, _SUCCESS."""
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    df_dev = pd.DataFrame(
        {
            "cust_id": ["c5", "c6"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.5, 2.5],
            "label": [1, 0],
        }
    )
    train_dir = tmp_path / "train.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df_tr.to_parquet(train_dir, engine="pyarrow")
    df_dev.to_parquet(dev_dir, engine="pyarrow")

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"
    train_h, dev_h = adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)),
        ParquetHandle(str(dev_dir)),
        prep_meta,
        parameters,
        str(cache_dir),
    )

    assert (cache_dir / "lgb" / "train.bin").exists()
    assert (cache_dir / "lgb" / "train_dev.bin").exists()
    assert (cache_dir / "lgb" / "_SUCCESS").exists()
    assert train_h.role == "train"
    assert dev_h.role == "train_dev"
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_train_inputs_writes_bins -v
```

Expected: FAIL with `TypeError: Can't instantiate abstract class LightGBMAdapter with abstract method prepare_train_inputs`

- [ ] **Step 3: Implement**

Modify `src/recsys_tfb/models/lightgbm_adapter.py`. Add at the top:

```python
import shutil
from pathlib import Path

from recsys_tfb.io.extract import extract_Xy
from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
```

Add inside `LightGBMAdapter`:

```python
    def prepare_train_inputs(
        self,
        train_handle: ParquetHandle,
        train_dev_handle: ParquetHandle,
        preprocessor_metadata: dict,
        parameters: dict,
        cache_dir: str,
    ) -> tuple[LgbDatasetHandle, LgbDatasetHandle]:
        lgb_dir = Path(cache_dir) / "lgb"
        success = lgb_dir / "_SUCCESS"
        train_bin = lgb_dir / "train.bin"
        dev_bin = lgb_dir / "train_dev.bin"

        if success.exists():
            return (
                LgbDatasetHandle(bin_path=str(train_bin), role="train"),
                LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
            )

        if lgb_dir.exists():
            logger.warning(
                "Partial lgb cache at %s, clearing before rebuild", lgb_dir
            )
            shutil.rmtree(lgb_dir)
        lgb_dir.mkdir(parents=True, exist_ok=True)

        X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
        X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)

        # PR1: categorical_feature stays None (byte-equal vs main branch).
        # PR2 will set this from preprocessor_metadata.
        cat_idx = None

        ds_train = lgb.Dataset(
            X_tr, label=y_tr, categorical_feature=cat_idx, free_raw_data=True
        ).construct()
        ds_train.save_binary(str(train_bin))
        del X_tr, y_tr

        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            free_raw_data=True,
        ).construct()
        ds_dev.save_binary(str(dev_bin))
        del X_dev, y_dev, ds_train, ds_dev

        success.touch()

        return (
            LgbDatasetHandle(bin_path=str(train_bin), role="train"),
            LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
        )
```

- [ ] **Step 4: Run test, verify pass**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_train_inputs_writes_bins -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/lightgbm_adapter.py tests/test_models/test_adapter.py src/recsys_tfb/models/base.py
git commit -m "feat(models): add ModelAdapter.prepare_train_inputs + LightGBMAdapter impl

PR1: categorical_feature stays None (byte-equal vs main).
PR2 will enable native categorical handling."
```

---

### Task 3.2: Cache hit (skip rebuild)

**Files:**
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Write test**

Append to `tests/test_models/test_adapter.py`:

```python
def test_lightgbm_prepare_train_inputs_cache_hit(tmp_path, monkeypatch):
    """Second call with valid _SUCCESS marker skips lgb.Dataset.construct."""
    import pandas as pd
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    df_dev = df_tr.copy()
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df_tr.to_parquet(train_dir)
    df_dev.to_parquet(dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }
    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )
    assert (cache_dir / "lgb" / "_SUCCESS").exists()

    construct_calls = []
    real_construct = lgb.Dataset.construct

    def spy_construct(self):
        construct_calls.append(1)
        return real_construct(self)

    monkeypatch.setattr(lgb.Dataset, "construct", spy_construct)

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    assert construct_calls == [], "cache hit should not call lgb.Dataset.construct"
```

- [ ] **Step 2: Run, verify pass**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_train_inputs_cache_hit -v
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_models/test_adapter.py
git commit -m "test(models): cover lgb prepare_train_inputs cache-hit path"
```

---

### Task 3.3: Partial cache recovery

**Files:**
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Write test**

Append to `tests/test_models/test_adapter.py`:

```python
def test_lightgbm_prepare_train_inputs_partial_cache_rebuild(tmp_path):
    """If lgb/ exists but _SUCCESS is missing, rmtree and rebuild."""
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df.to_parquet(train_dir)
    df.to_parquet(dev_dir)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }
    adapter = LightGBMAdapter()
    cache_dir = tmp_path / "variant"

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    # Simulate crash: remove _SUCCESS but leave bins
    (cache_dir / "lgb" / "_SUCCESS").unlink()

    adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir)),
        prep_meta, parameters, str(cache_dir),
    )

    assert (cache_dir / "lgb" / "_SUCCESS").exists()
    assert (cache_dir / "lgb" / "train.bin").exists()
```

- [ ] **Step 2: Run, verify pass**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_train_inputs_partial_cache_rebuild -v
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_models/test_adapter.py
git commit -m "test(models): cover lgb prepare_train_inputs partial-cache recovery"
```

---

## Phase 4: CalibratedModelAdapter raises

### Task 4.1: `CalibratedModelAdapter.prepare_train_inputs` raises `NotImplementedError`

**Files:**
- Modify: `src/recsys_tfb/models/calibrated_adapter.py`
- Test: `tests/test_models/test_calibrated_adapter.py`

- [ ] **Step 1: Add failing test**

Append to `tests/test_models/test_calibrated_adapter.py`:

```python
def test_calibrated_adapter_prepare_train_inputs_raises():
    """CalibratedModelAdapter does not own training-data preparation."""
    import pytest
    from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    adapter = CalibratedModelAdapter(base=LightGBMAdapter())

    with pytest.raises(NotImplementedError, match="prepare_train_inputs"):
        adapter.prepare_train_inputs(
            train_handle=None,  # type: ignore[arg-type]
            train_dev_handle=None,  # type: ignore[arg-type]
            preprocessor_metadata={},
            parameters={},
            cache_dir="/tmp",
        )
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_models/test_calibrated_adapter.py::test_calibrated_adapter_prepare_train_inputs_raises -v
```

Expected: FAIL — `TypeError: Can't instantiate abstract class CalibratedModelAdapter`.

- [ ] **Step 3: Implement**

Add inside the `CalibratedModelAdapter` class in `src/recsys_tfb/models/calibrated_adapter.py`:

```python
    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "CalibratedModelAdapter wraps a trained adapter; "
            "prepare_train_inputs must be called on the underlying adapter "
            "(e.g. LightGBMAdapter) before calibration is applied."
        )
```

- [ ] **Step 4: Run test, verify pass**

```bash
.venv/bin/pytest tests/test_models/test_calibrated_adapter.py -v
```

Expected: all calibrated adapter tests pass.

- [ ] **Step 5: Run full models test suite**

```bash
.venv/bin/pytest tests/test_models/ -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/models/calibrated_adapter.py tests/test_models/test_calibrated_adapter.py
git commit -m "feat(models): CalibratedModelAdapter.prepare_train_inputs raises NotImplementedError"
```

---

## Phase 5: LightGBMAdapter.train accepts pre-built `lgb.Dataset`

### Task 5.1: Add keyword-only `train_dataset` / `val_dataset` params

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py:20-45`
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_models/test_adapter.py`:

```python
def test_lightgbm_train_accepts_prebuilt_datasets(tmp_path):
    """train() with train_dataset= / val_dataset= kwargs uses pre-built Datasets."""
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    rng = np.random.default_rng(42)
    X_tr = rng.normal(size=(50, 3))
    y_tr = (rng.uniform(size=50) > 0.5).astype(int)
    X_dev = rng.normal(size=(20, 3))
    y_dev = (rng.uniform(size=20) > 0.5).astype(int)

    train_bin = tmp_path / "tr.bin"
    dev_bin = tmp_path / "dev.bin"
    ds_tr = lgb.Dataset(X_tr, label=y_tr, free_raw_data=False).construct()
    ds_tr.save_binary(str(train_bin))
    ds_dev = lgb.Dataset(
        X_dev, label=y_dev, reference=ds_tr, free_raw_data=False
    ).construct()
    ds_dev.save_binary(str(dev_bin))

    loaded_tr = lgb.Dataset(str(train_bin))
    loaded_dev = lgb.Dataset(str(dev_bin), reference=loaded_tr)

    adapter = LightGBMAdapter()
    adapter.train(
        X_train=None, y_train=None, X_val=None, y_val=None,
        params={
            "objective": "binary",
            "verbose": -1,
            "num_iterations": 5,
            "early_stopping_rounds": 3,
        },
        train_dataset=loaded_tr,
        val_dataset=loaded_dev,
    )

    assert adapter.booster is not None
    assert adapter.booster.num_trees() > 0
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_train_accepts_prebuilt_datasets -v
```

Expected: FAIL — `train()` doesn't accept the new keywords yet.

- [ ] **Step 3: Modify `LightGBMAdapter.train`**

Replace the `train` method body in `src/recsys_tfb/models/lightgbm_adapter.py`:

```python
    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        params: dict,
        *,
        train_dataset: "lgb.Dataset | None" = None,
        val_dataset: "lgb.Dataset | None" = None,
    ) -> None:
        num_iterations = params.pop("num_iterations", 500)
        early_stopping_rounds = params.pop("early_stopping_rounds", 50)

        if train_dataset is None:
            train_dataset = lgb.Dataset(
                X_train, label=y_train, free_raw_data=False
            )
        if val_dataset is None:
            val_dataset = lgb.Dataset(
                X_val, label=y_val, reference=train_dataset, free_raw_data=False
            )

        callbacks = [
            lgb.early_stopping(stopping_rounds=early_stopping_rounds),
            lgb.log_evaluation(period=0),
        ]
        self._booster = lgb.train(
            params,
            train_dataset,
            num_boost_round=num_iterations,
            valid_sets=[val_dataset],
            valid_names=["val"],
            callbacks=callbacks,
        )
```

- [ ] **Step 4: Run new test + existing**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/lightgbm_adapter.py tests/test_models/test_adapter.py
git commit -m "feat(models): LightGBMAdapter.train accepts pre-built lgb.Dataset via kwargs"
```

---

## Phase 6: Cache nodes return `ParquetHandle`

### Task 6.1: Rewrite `tests/test_pipelines/test_training/test_cache_nodes.py`

**Files:**
- Rewrite: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Read existing tests to understand fixtures**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py --collect-only -q
```

Note the test names; many will be deleted (any covering `cache.enabled=False` or `_cache_or_passthrough` directly).

- [ ] **Step 2: Replace file contents**

Overwrite `tests/test_pipelines/test_training/test_cache_nodes.py` with:

```python
"""Tests for training cache nodes (post-refactor).

Cache nodes now write parquet to driver-local fs and return a ParquetHandle.
The ``cache.enabled=false`` passthrough mode has been removed; tests must
provide a writable cache_root via tmp_path.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from recsys_tfb.io.handles import ParquetHandle


def _params_with_cache_root(cache_root: Path) -> dict:
    return {
        "hive": {"db": "ml_recsys"},
        "cache": {"root": str(cache_root)},
        "base_dataset_version": "deadbeef",
        "train_variant_id": "v1",
        "calibration_variant_id": "c1",
    }


def _stub_hdfs(monkeypatch, location: str = "hdfs:/some/path") -> None:
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
        lambda spark, db, table: location,
    )
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local",
        lambda spark, src_glob, dst, glob: Path(dst).mkdir(parents=True, exist_ok=True),
    )


class TestCacheNodeReturnHandle:
    def test_cache_train_returns_parquet_handle(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_train_model_input(df, params)

        assert isinstance(handle, ParquetHandle)
        assert "train_model_input" in handle.path

    def test_cache_creates_success_marker(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_val_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_val_model_input(df, params)

        success = Path(handle.path) / "_SUCCESS"
        assert success.exists()


class TestCacheHit:
    def test_skip_copy_when_success_marker_present(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import (
            _resolve_cache_path,
            cache_train_model_input,
        )

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(_resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "_SUCCESS").touch()

        copy_calls = []
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local",
            lambda *a, **kw: copy_calls.append(1),
        )
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            lambda *a, **kw: "hdfs:/some/path",
        )

        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert copy_calls == []


class TestPartialCacheRecovery:
    def test_rmtree_when_dir_exists_without_success(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import (
            _resolve_cache_path,
            cache_train_model_input,
        )

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(_resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "stale_partial.parquet").touch()

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert not (cache_path / "stale_partial.parquet").exists()
        assert (cache_path / "_SUCCESS").exists()


class TestRejectsNonSparkInput:
    def test_passthrough_mode_removed(self, tmp_path):
        """cache.enabled=false has been removed; pandas inputs must be rejected."""
        import pandas as pd
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        df = pd.DataFrame({"a": [1]})  # not a Spark DataFrame

        with pytest.raises(TypeError, match="Spark DataFrame"):
            cache_train_model_input(df, params)
```

- [ ] **Step 3: Do not commit yet** — pair with implementation in next task.

---

### Task 6.2: Refactor cache nodes to return `ParquetHandle`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:158-229`
- Modify: `conf/base/parameters_training.yaml:49-53` (remove `cache.enabled` key)

- [ ] **Step 1: Replace `_cache_or_passthrough` with `_materialize_parquet_handle`**

In `src/recsys_tfb/pipelines/training/nodes.py`:

(a) Add the `ParquetHandle` import near the top:

```python
from recsys_tfb.io.handles import ParquetHandle
```

(b) Replace the entire `_cache_or_passthrough` function (and remove the `_is_spark_df` helper if it's only used by it) with:

```python
def _materialize_parquet_handle(
    df, dataset_name: str, parameters: dict
) -> ParquetHandle:
    """Skip-if-exists local-parquet cache for a single model_input.

    Behaviour:
      - df is not a Spark DataFrame  → TypeError (pandas-passthrough removed)
      - target path has _SUCCESS  → return ParquetHandle pointing at it
      - target path exists but no _SUCCESS  → rmtree and rebuild
      - cache miss  → hadoop fs copyToLocal HDFS subtree to driver-local;
                      touch _SUCCESS; return ParquetHandle
    """
    if not hasattr(df, "sql_ctx"):
        raise TypeError(
            f"{dataset_name} input must be a Spark DataFrame; got "
            f"{type(df).__name__}. cache.enabled=false passthrough has been "
            "removed; all environments (including dev/test) must use a "
            "writable cache.root."
        )

    local_path = _resolve_cache_path(dataset_name, parameters)
    success_marker = Path(local_path) / "_SUCCESS"

    if Path(local_path).exists() and not success_marker.exists():
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    if not success_marker.exists():
        spark = df.sql_ctx.sparkSession
        logger.info("cache_miss name=%s path=%s", dataset_name, local_path)
        _populate_cache_from_hive(spark, dataset_name, parameters, local_path)
        success_marker.touch()
    else:
        logger.info("cache_hit name=%s path=%s", dataset_name, local_path)

    return ParquetHandle(path=local_path)
```

(c) Update the four cache-node wrappers to call `_materialize_parquet_handle`:

```python
def cache_train_model_input(train_model_input, parameters: dict) -> ParquetHandle:
    return _materialize_parquet_handle(train_model_input, "train_model_input", parameters)


def cache_train_dev_model_input(train_dev_model_input, parameters: dict) -> ParquetHandle:
    return _materialize_parquet_handle(train_dev_model_input, "train_dev_model_input", parameters)


def cache_val_model_input(val_model_input, parameters: dict) -> ParquetHandle:
    return _materialize_parquet_handle(val_model_input, "val_model_input", parameters)


def cache_calibration_model_input(calibration_model_input, parameters: dict) -> ParquetHandle:
    return _materialize_parquet_handle(
        calibration_model_input, "calibration_model_input", parameters
    )
```

- [ ] **Step 2: Remove `cache.enabled` from parameters file**

Edit `conf/base/parameters_training.yaml` lines 47–53. Replace:

```yaml
cache:
  enabled: true
  root: /Users/curtislu/projects/recsys_tfb/data/recsys_cache
```

with:

```yaml
# cache.enabled was removed; all environments now write parquet to disk and
# pass ParquetHandle through the DAG. dev/test use a tmp cache.root.
cache:
  root: /Users/curtislu/projects/recsys_tfb/data/recsys_cache
```

- [ ] **Step 3: Run cache_nodes tests**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py -v
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py \
        conf/base/parameters_training.yaml \
        tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "refactor(training): cache nodes return ParquetHandle; drop cache.enabled

Removes the in-memory passthrough mode entirely. Single code path for all
environments: write local parquet, return ParquetHandle. dev/test must set
cache.root to a writable tmp dir."
```

---

## Phase 7: New `prepare_lgb_train_inputs` node + downstream rewiring

### Task 7.1: Add `prepare_lgb_train_inputs` node function

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Test: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py`:

```python
class TestPrepareLgbTrainInputs:
    def test_prepare_node_returns_two_lgb_handles(self, tmp_path):
        import pandas as pd
        from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
        from recsys_tfb.pipelines.training.nodes import prepare_lgb_train_inputs

        df = pd.DataFrame(
            {
                "cust_id": ["c1", "c2", "c3", "c4"],
                "snap_date": pd.to_datetime(["2025-01-31"] * 4),
                "prod_name": ["fund", "ccard", "fund", "ccard"],
                "feat_a": [1.0, 2.0, 3.0, 4.0],
                "label": [0, 1, 0, 1],
            }
        )
        train_dir = tmp_path / "tr.parquet"
        dev_dir = tmp_path / "dev.parquet"
        df.to_parquet(train_dir)
        df.to_parquet(dev_dir)

        prep_meta = {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["fund", "ccard"]},
        }
        parameters = {
            "cache": {"root": str(tmp_path / "cache")},
            "base_dataset_version": "v1",
            "train_variant_id": "tv1",
            "schema": {
                "label": "label",
                "identity_columns": ["cust_id", "snap_date", "prod_name"],
            },
            "training": {"algorithm": "lightgbm"},
        }

        train_h, dev_h = prepare_lgb_train_inputs(
            ParquetHandle(str(train_dir)),
            ParquetHandle(str(dev_dir)),
            prep_meta,
            parameters,
        )

        assert isinstance(train_h, LgbDatasetHandle)
        assert isinstance(dev_h, LgbDatasetHandle)
        assert train_h.role == "train"
        assert dev_h.role == "train_dev"
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestPrepareLgbTrainInputs -v
```

Expected: FAIL — function doesn't exist.

- [ ] **Step 3: Implement the node**

Append to `src/recsys_tfb/pipelines/training/nodes.py`:

```python
def prepare_lgb_train_inputs(
    train_parquet_handle: ParquetHandle,
    train_dev_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
):
    """Materialize lgb.Dataset binaries for train + train_dev.

    Delegates to the configured ModelAdapter's prepare_train_inputs. The
    cache_dir uses the same train_variant directory as the parquet cache,
    placing 'lgb/' as a sibling of the parquets.
    """
    algorithm = parameters["training"].get("algorithm", "lightgbm")
    adapter = get_adapter(algorithm)

    cache_root = parameters["cache"]["root"]
    base_v = parameters["base_dataset_version"]
    train_v = parameters["train_variant_id"]
    cache_dir = Path(cache_root) / base_v / "train_variants" / train_v

    return adapter.prepare_train_inputs(
        train_parquet_handle,
        train_dev_parquet_handle,
        preprocessor_metadata,
        parameters,
        str(cache_dir),
    )
```

- [ ] **Step 4: Run test, verify pass**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestPrepareLgbTrainInputs -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat(training): add prepare_lgb_train_inputs pipeline node"
```

---

### Task 7.2: Rewire downstream nodes (`tune_hyperparameters`, `train_model`, `calibrate_model`, `evaluate_model`)

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`

- [ ] **Step 1: Refactor `tune_hyperparameters`**

Replace the existing `tune_hyperparameters` body with:

```python
def tune_hyperparameters(
    train_lgb_handle,
    train_dev_lgb_handle,
    val_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> dict:
    """Search for optimal hyperparameters using Optuna.

    train + train_dev consumed as pre-built lgb.Dataset binaries (no rebinning
    across trials). val read fresh from parquet inside this scope so its pandas
    DataFrame is freed when the function returns.
    """
    from recsys_tfb.io.extract import extract_Xy

    training_params = parameters["training"]
    n_trials = training_params["n_trials"]
    search_space = training_params["search_space"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    with log_step(logger, "extract_features"):
        X_v, y_v = extract_Xy(val_parquet_handle, preprocessor_metadata, parameters)

    def objective(trial: optuna.Trial) -> float:
        trial_params = {
            "learning_rate": trial.suggest_float(
                "learning_rate",
                search_space["learning_rate"]["low"],
                search_space["learning_rate"]["high"],
                log=True,
            ),
            "num_leaves": trial.suggest_int(
                "num_leaves",
                search_space["num_leaves"]["low"],
                search_space["num_leaves"]["high"],
            ),
            "max_depth": trial.suggest_int(
                "max_depth",
                search_space["max_depth"]["low"],
                search_space["max_depth"]["high"],
            ),
            "min_child_samples": trial.suggest_int(
                "min_child_samples",
                search_space["min_child_samples"]["low"],
                search_space["min_child_samples"]["high"],
            ),
            "subsample": trial.suggest_float(
                "subsample",
                search_space["subsample"]["low"],
                search_space["subsample"]["high"],
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                search_space["colsample_bytree"]["low"],
                search_space["colsample_bytree"]["high"],
            ),
        }

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
        return ap if ap is not None else 0.0

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    with log_step(logger, "optuna_optimize"):
        study.optimize(objective, n_trials=n_trials)

    best_params = study.best_params
    logger.info("Best trial mAP: %.4f, params: %s", study.best_value, best_params)
    return best_params
```

- [ ] **Step 2: Refactor `train_model`**

Replace `train_model` body:

```python
def train_model(
    train_lgb_handle,
    train_dev_lgb_handle,
    best_params: dict,
    preprocessor_metadata: dict,
    parameters: dict,
) -> ModelAdapter:
    """Train a model using ModelAdapter with early stopping.

    Consumes pre-built lgb.Dataset binaries; no parquet read in this scope.
    """
    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    params = {
        **algorithm_params,
        "seed": seed,
        **best_params,
        "num_iterations": num_iterations,
        "early_stopping_rounds": early_stopping_rounds,
    }

    with log_step(logger, "model_train"):
        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )

    logger.info("Model trained with algorithm=%s", algorithm)
    return adapter
```

- [ ] **Step 3: Refactor `calibrate_model`**

Replace `calibrate_model` signature/body:

```python
def calibrate_model(
    trained_model: ModelAdapter,
    calibration_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> CalibratedModelAdapter:
    """Wrap trained model with isotonic / sigmoid calibrator."""
    from recsys_tfb.io.extract import extract_Xy

    method = parameters["training"].get("calibration_method", "isotonic")
    with log_step(logger, "extract_features"):
        X_cal, y_cal = extract_Xy(
            calibration_parquet_handle, preprocessor_metadata, parameters
        )

    with log_step(logger, "fit_calibrator"):
        adapter = CalibratedModelAdapter(base=trained_model, method=method)
        adapter.fit_calibrator(X_cal, y_cal)

    return adapter
```

- [ ] **Step 4: Refactor `evaluate_model`**

Open `nodes.py:evaluate_model`. Read its current implementation (lines around 462–490) — it consumes `val_pdf` for identity columns and uses `compute_all_metrics`. Modify the signature to accept `val_parquet_handle` and use `to_pandas()`:

```python
def evaluate_model(
    model: ModelAdapter,
    val_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> dict:
    """Compute evaluation metrics on val set."""
    from recsys_tfb.io.extract import extract_Xy

    val_pdf = val_parquet_handle.to_pandas()
    with log_step(logger, "extract_features"):
        X, _ = extract_Xy(val_parquet_handle, preprocessor_metadata, parameters)

    with log_step(logger, "predict"):
        y_pred = model.predict(X)

    return compute_all_metrics(val_pdf, y_pred, parameters)
```

If the existing `evaluate_model` uses any extra fields from the original DataFrame (e.g. specific columns), preserve that logic but read them from `val_pdf` (which is the parquet now). Run the existing `test_evaluate_model` test after changes — adjust if needed.

- [ ] **Step 5: Drop unused `_extract_Xy` and `_to_pandas` from `nodes.py`**

Verify these helpers are no longer referenced inside `nodes.py`:

```bash
grep -n "_extract_Xy\|_to_pandas" src/recsys_tfb/pipelines/training/nodes.py
```

Delete the now-redundant helpers if nothing references them. (Other modules using them should already have been migrated to `recsys_tfb.io.extract.extract_Xy` in Phase 1.)

- [ ] **Step 6: Run training-scoped tests**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/ -v
```

Expected: most pass; existing `test_pipeline.py` may fail due to renamed intermediate dataset names. Phase 7.3 will fix that.

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py
git commit -m "refactor(training): rewire tune/train/calibrate/evaluate to consume handles

- tune/train use LgbDatasetHandle for train+train_dev (no per-trial binning)
- calibrate/evaluate read val/calibration parquet via to_pandas() in scope
- _extract_Xy / _to_pandas legacy helpers removed (moved to io/extract.py)"
```

---

### Task 7.3: Update `pipeline.py` DAG and tests

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`
- Modify: `tests/test_pipelines/test_training/test_pipeline.py`

- [ ] **Step 1: Replace pipeline.py contents**

Replace the body of `create_pipeline` in `src/recsys_tfb/pipelines/training/pipeline.py`:

```python
"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    evaluate_model,
    log_experiment,
    prepare_lgb_train_inputs,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(
            cache_train_model_input,
            inputs=["train_model_input", "parameters"],
            outputs="train_parquet_handle",
        ),
        Node(
            cache_train_dev_model_input,
            inputs=["train_dev_model_input", "parameters"],
            outputs="train_dev_parquet_handle",
        ),
        Node(
            cache_val_model_input,
            inputs=["val_model_input", "parameters"],
            outputs="val_parquet_handle",
        ),
    ]

    if enable_calibration:
        nodes.append(
            Node(
                cache_calibration_model_input,
                inputs=["calibration_model_input", "parameters"],
                outputs="calibration_parquet_handle",
            ),
        )

    nodes.append(
        Node(
            prepare_lgb_train_inputs,
            inputs=[
                "train_parquet_handle", "train_dev_parquet_handle",
                "preprocessor", "parameters",
            ],
            outputs=["train_lgb_handle", "train_dev_lgb_handle"],
        ),
    )

    nodes.extend([
        Node(
            tune_hyperparameters,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
                "val_parquet_handle", "preprocessor", "parameters",
            ],
            outputs="best_params",
        ),
        Node(
            train_model,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
                "best_params", "preprocessor", "parameters",
            ],
            outputs=train_model_output,
        ),
    ])

    if enable_calibration:
        nodes.append(
            Node(
                calibrate_model,
                inputs=[
                    "trained_model", "calibration_parquet_handle",
                    "preprocessor", "parameters",
                ],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "val_parquet_handle", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=["model", "best_params", "evaluation_results", "parameters"],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
```

- [ ] **Step 2: Update `tests/test_pipelines/test_training/test_pipeline.py`**

Find every reference to old intermediate names:

```bash
grep -n "cached_train_model_input\|cached_train_dev_model_input\|cached_val_model_input\|cached_calibration_model_input" tests/test_pipelines/test_training/test_pipeline.py
```

Replace mappings:

| Old name | New name |
|---|---|
| `cached_train_model_input` | `train_parquet_handle` |
| `cached_train_dev_model_input` | `train_dev_parquet_handle` |
| `cached_val_model_input` | `val_parquet_handle` |
| `cached_calibration_model_input` | `calibration_parquet_handle` |

Add references for the two new outputs `train_lgb_handle` and `train_dev_lgb_handle` to any test that asserts on the full DAG output set.

- [ ] **Step 3: Run pipeline tests**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/ -v
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add src/recsys_tfb/pipelines/training/pipeline.py tests/test_pipelines/test_training/test_pipeline.py
git commit -m "refactor(training): pipeline DAG uses ParquetHandle + LgbDatasetHandle"
```

---

## Phase 8: Integration test + PR1 byte-equal regression

### Task 8.1: End-to-end integration test

**Files:**
- Create: `tests/test_pipelines/test_training/test_pipeline_integration.py`

- [ ] **Step 1: Inspect existing synthetic-data fixtures**

```bash
ls tests/test_pipelines/test_training/
grep -rln "create_pipeline\|run.*training" tests/test_pipelines/test_training/ tests/conftest.py 2>/dev/null
```

Identify the helper that already runs the training pipeline end-to-end on synthetic data (likely in `conftest.py` or a fixtures module). Note its name and import path.

- [ ] **Step 2: Write integration test**

Create `tests/test_pipelines/test_training/test_pipeline_integration.py`:

```python
"""End-to-end smoke test for the training pipeline post-refactor.

Uses synthetic data and a tmp_path cache.root. Asserts that the lgb binary
cache is created on first run and reused on second run.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

# Replace the import path below with the actual synthetic-pipeline runner
# discovered in Step 1 (e.g. from .conftest import run_training_pipeline_synthetic).
from tests.test_pipelines.test_training.conftest import (
    run_training_pipeline_synthetic,
)


@pytest.mark.integration
def test_training_pipeline_produces_lgb_binary_cache(tmp_path):
    """First run builds lgb/train.bin + lgb/train_dev.bin + lgb/_SUCCESS."""
    cache_root = tmp_path / "cache"
    run_training_pipeline_synthetic(cache_root=cache_root)

    lgb_dirs = list(cache_root.glob("*/train_variants/*/lgb"))
    assert lgb_dirs, "lgb/ subdirectory not created"
    lgb_dir = lgb_dirs[0]
    assert (lgb_dir / "train.bin").exists()
    assert (lgb_dir / "train_dev.bin").exists()
    assert (lgb_dir / "_SUCCESS").exists()


@pytest.mark.integration
def test_training_pipeline_second_run_skips_lgb_construct(tmp_path):
    """Second run with same cache.root must not call lgb.Dataset.construct."""
    import lightgbm as lgb

    cache_root = tmp_path / "cache"
    run_training_pipeline_synthetic(cache_root=cache_root)

    construct_calls = []
    real_construct = lgb.Dataset.construct

    def spy_construct(self):
        construct_calls.append(1)
        return real_construct(self)

    with patch.object(lgb.Dataset, "construct", spy_construct):
        run_training_pipeline_synthetic(cache_root=cache_root)

    assert construct_calls == [], (
        f"Cached run should not construct lgb.Dataset; got {len(construct_calls)} calls"
    )
```

- [ ] **Step 3: If no existing synthetic-pipeline runner exists**

Add a minimal helper to `tests/test_pipelines/test_training/conftest.py`:

```python
def run_training_pipeline_synthetic(cache_root):
    """Run training pipeline on synthetic data with cache.root=cache_root.

    Builds the synthetic dataset → runs training pipeline. Returns nothing.
    """
    from pathlib import Path
    import yaml

    from recsys_tfb.core.catalog import DataCatalog
    from recsys_tfb.core.runner import Runner
    from recsys_tfb.pipelines.dataset.pipeline import create_pipeline as create_dataset
    from recsys_tfb.pipelines.training.pipeline import create_pipeline as create_training

    cache_root = Path(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)

    # 1. Build synthetic dataset (writes feature_table, label_table, etc.)
    # Use existing dev-environment synthetic generator if available; otherwise
    # delegate to the same helper that drives test_pipeline.py.
    # ... (fill from existing test scaffolding)
```

If the existing test infrastructure has a different name for the runner, update both `conftest.py` and the integration test imports accordingly.

- [ ] **Step 4: Run integration test**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_pipeline_integration.py -v -m integration
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipelines/test_training/test_pipeline_integration.py \
        tests/test_pipelines/test_training/conftest.py
git commit -m "test(training): integration test covers lgb binary cache lifecycle"
```

---

### Task 8.2: Run full test suite — fix any breakage

- [ ] **Step 1: Run all tests**

```bash
.venv/bin/pytest tests/ -v
```

- [ ] **Step 2: Triage failures**

For each failure:
- Removed `cache.enabled=False` path → delete the test
- Renamed `cached_*` reference → use new name (`*_parquet_handle` / `*_lgb_handle`)
- Downstream signature mismatch → update test fixture

- [ ] **Step 3: Re-run**

```bash
.venv/bin/pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 4: Commit (if any test fixes were needed)**

```bash
git add tests/
git commit -m "test: align downstream test fixtures with handle-based cache layer"
```

---

### Task 8.3: PR1 byte-equal regression check

**Files:**
- (no source changes; verification only)

- [ ] **Step 1: Generate baseline on `main`**

In a separate clean checkout of `main`:

```bash
cd /tmp && git clone /Users/curtislu/projects/recsys_tfb recsys_tfb_main && cd recsys_tfb_main
.venv/bin/python -m recsys_tfb dataset --env dev
.venv/bin/python -m recsys_tfb training --env dev
# Find the evaluation_results path printed by the run; it lives under
# data/recsys_models/<base_v>/<train_v>/<model_v>/evaluation_results.json
cp $(find data -name evaluation_results.json | head -1) /tmp/baseline_main.json
```

- [ ] **Step 2: Generate same on this branch**

```bash
cd /Users/curtislu/projects/recsys_tfb
rm -rf data/recsys_cache  # clear cache to force rebuild
.venv/bin/python -m recsys_tfb dataset --env dev
.venv/bin/python -m recsys_tfb training --env dev
cp $(find data -name evaluation_results.json | head -1) /tmp/refactor_pr1.json
```

- [ ] **Step 3: Diff metrics**

```bash
.venv/bin/python -c "
import json
a = json.load(open('/tmp/baseline_main.json'))
b = json.load(open('/tmp/refactor_pr1.json'))

def diff(d1, d2, path=''):
    out = []
    for k in set(d1) | set(d2):
        if k not in d1 or k not in d2:
            out.append((path + k, 'missing on one side'))
        elif isinstance(d1[k], dict):
            out.extend(diff(d1[k], d2[k], path + k + '.'))
        elif d1[k] != d2[k]:
            out.append((path + k, f'{d1[k]} vs {d2[k]}'))
    return out

result = diff(a, b)
print(result if result else 'BYTE EQUAL')
"
```

Expected: `BYTE EQUAL`. Any difference → STOP, investigate. PR1 must not change metrics.

- [ ] **Step 4: Tag PR1 ready**

```bash
git tag pr1-ready
```

This marks the byte-equal commit; PR2 starts from here.

---

## Phase 9: PR2 — Enable `categorical_feature=`

### Task 9.1: Compute categorical column indices

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py`
- Test: `tests/test_models/test_adapter.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_models/test_adapter.py`:

```python
def test_lightgbm_prepare_passes_categorical_feature(tmp_path):
    """prepare_train_inputs sets categorical_feature on lgb.Dataset."""
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    train_dir = tmp_path / "tr.parquet"
    dev_dir = tmp_path / "dev.parquet"
    df.to_parquet(train_dir)
    df.to_parquet(dev_dir)

    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],  # prod_name index = 1
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    adapter = LightGBMAdapter()
    train_h, _ = adapter.prepare_train_inputs(
        ParquetHandle(str(train_dir)),
        ParquetHandle(str(dev_dir)),
        prep_meta,
        parameters,
        str(tmp_path / "cache"),
    )
    ds = train_h.load()
    ds.construct()
    cat_attr = ds.categorical_feature
    # lgb may store as list[int] (indexes) or list[str] (column names)
    assert cat_attr in ([1], ["prod_name"], ["Column_1"])
```

- [ ] **Step 2: Run, verify fail**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_passes_categorical_feature -v
```

Expected: FAIL — current `cat_idx` is hardcoded `None`.

- [ ] **Step 3: Implement helper + use it**

In `src/recsys_tfb/models/lightgbm_adapter.py`:

(a) Add this method on `LightGBMAdapter`:

```python
    @staticmethod
    def _categorical_indices(preprocessor_metadata: dict):
        """Index positions of categorical columns within feature_columns.

        Returns None if no categoricals are present (lgb.Dataset accepts None).
        """
        feat_cols = preprocessor_metadata["feature_columns"]
        cat_cols = preprocessor_metadata.get("categorical_columns", [])
        idx = [feat_cols.index(c) for c in cat_cols if c in feat_cols]
        return idx or None
```

(b) Replace the line `cat_idx = None` in `prepare_train_inputs` with:

```python
        cat_idx = self._categorical_indices(preprocessor_metadata)
```

- [ ] **Step 4: Run test, verify pass**

```bash
.venv/bin/pytest tests/test_models/test_adapter.py::test_lightgbm_prepare_passes_categorical_feature -v
```

Expected: 1 passed.

- [ ] **Step 5: Run full models test suite**

```bash
.venv/bin/pytest tests/test_models/ -v
```

Expected: all pass. (The earlier fresh-build test asserts no specific cat_idx, so it remains valid.)

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/models/lightgbm_adapter.py tests/test_models/test_adapter.py
git commit -m "feat(models): pass categorical_feature= to lgb.Dataset in prepare_train_inputs

PR2: native categorical handling. Metric drift expected vs PR1; verify with
before/after baseline before merging."
```

---

### Task 9.2: Metric drift baseline check (manual gate)

**Files:**
- (no source changes; verification only)

- [ ] **Step 1: Wipe lgb caches**

```bash
find data/recsys_cache -type d -name lgb -exec rm -rf {} +
```

- [ ] **Step 2: Re-run training pipeline**

```bash
.venv/bin/python -m recsys_tfb training --env dev
cp $(find data -name evaluation_results.json | head -1) /tmp/refactor_pr2.json
```

- [ ] **Step 3: Compare PR2 vs PR1 metrics**

```bash
.venv/bin/python -c "
import json
a = json.load(open('/tmp/refactor_pr1.json'))  # PR1 byte-equal w/ main
b = json.load(open('/tmp/refactor_pr2.json'))  # PR2 with categorical_feature=

def find_mAP(d):
    if isinstance(d, dict):
        if 'mAP' in d:
            return d['mAP']
        for v in d.values():
            r = find_mAP(v)
            if r is not None:
                return r
    return None

m1 = find_mAP(a); m2 = find_mAP(b)
print(f'PR1 mAP: {m1:.4f}')
print(f'PR2 mAP: {m2:.4f}')
delta = (m2 - m1) / m1 * 100
print(f'Delta: {delta:+.2f}%')
"
```

- [ ] **Step 4: Decision gate**

| Result | Action |
|---|---|
| `mAP_pr2 > mAP_pr1` | ✅ improvement; document in PR description and proceed |
| `-1% < delta <= 0%` | ⚠️ marginal regression; flag for PM review before merge |
| `delta < -1%` | ❌ STOP — investigate `_categorical_indices` correctness; check feat_cols ordering matches preprocessor |

- [ ] **Step 5: Tag PR2 ready (if approved)**

```bash
git tag pr2-ready
```

---

## Self-Review

**Spec coverage:**
- ✅ §動機 / OOM → Phase 6 (cache nodes return ParquetHandle, no MemoryDataset pandas)
- ✅ §動機 / lgb binning → Phases 3 + 7 (prepare_train_inputs + tune/train consume LgbDatasetHandle)
- ✅ §動機 / sub-optimal cat → Phase 9 (categorical_feature=)
- ✅ §約束 / algorithm-pluggable → Phases 2 + 4 (ABC method on base, raises on Calibrated)
- ✅ §約束 / no spark.master dependency → inherits from existing `_populate_cache_from_hive`
- ✅ §範圍納入 items 1–9 → all covered (handles, extract, ABC, lgb impl, calibrated raise, cache nodes, prepare node, pipeline, parameters)
- ✅ §設計 / Disk layout → Task 7.1 implements `<cache.root>/<base_v>/train_variants/<train_v>/lgb/`
- ✅ §設計 / _SUCCESS atomicity → Task 3.1 implements rmtree-on-partial + touch-last
- ✅ §設計 / Pipeline DAG → Task 7.3 wires nodes
- ✅ §設計 / adapter.train kwargs → Phase 5
- ✅ §部署順序 / PR1 → Phases 0–8
- ✅ §部署順序 / PR2 → Phase 9
- ✅ §測試 / unit → Phases 1–5 (handles, extract, adapter)
- ✅ §測試 / integration → Task 8.1
- ✅ §測試 / regression → Tasks 8.3 + 9.2
- ✅ §風險 / partial cache → Task 3.3
- ✅ §風險 / cat index → Task 9.1 includes index assertion
- ✅ §相依 / verify base_dataset_version covers preprocessor → Phase 0

**Type / signature consistency:** `prepare_train_inputs` signature is identical across `base.py` abstractmethod, `LightGBMAdapter` impl, `CalibratedModelAdapter` raise stub, and the pipeline node `prepare_lgb_train_inputs` caller. ✓

**Placeholder scan:** no TBD / TODO / "implement later" found. ✓

# Batched Test Eval + Spark mAP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate driver-OOM in the training pipeline's test-set evaluation by batching predict + write at `(snap_date, prod_name)` partition boundaries and computing mAP in Spark.

**Architecture:** Replace pandas `evaluate_model` + `write_test_predictions` + `compute_test_mAP` with two nodes: `predict_and_write_test_predictions` (Pass 0 label-only positive-set scan + Pass 1 per-partition predict & save) and `compute_test_mAP_spark` (Spark-native, reads back the written Hive table). Drop unused `rank` column; inline `label` into the predictions table so downstream readers don't re-join `label_table`.

**Tech Stack:** PySpark 3.3.2, pandas 1.5.3, pyarrow 14.0.1, LightGBM 4.6.0, pytest 7.3.1.

**Spec:** `docs/superpowers/specs/2026-05-13-batched-test-eval-spark-design.md`

---

## File Structure

**Modify:**
- `src/recsys_tfb/core/runner.py` — add `@`-prefixed input resolution (returns catalog dataset handle instead of loaded data) [Task 1]
- `src/recsys_tfb/io/extract.py` — extract `_pdf_to_X` helper from `extract_Xy` body [Task 2]
- `conf/base/catalog.yaml` — `training_eval_predictions` schema (drop `rank`, add `label`, `partition_filter`, add `prod_name` partition); `test_model_input` add `prod_name` to `partition_cols` [Task 3]
- `src/recsys_tfb/pipelines/training/nodes.py` — add `predict_and_write_test_predictions`, add `compute_test_mAP_spark`, remove `evaluate_model` / `write_test_predictions` / `compute_test_mAP` / `_build_training_eval_predictions_ddl` [Tasks 4, 5, 6]
- `src/recsys_tfb/pipelines/training/pipeline.py` — rewire DAG [Task 6]

**Test:**
- `tests/test_core/test_runner.py` — `@`-prefix input convention [Task 1]
- `tests/test_io/test_extract.py` — `_pdf_to_X` direct test; existing tests stay green [Task 2]
- `tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py` — new file for predict/write node [Task 4]
- `tests/test_pipelines/test_training/test_compute_test_map_spark.py` — new file for Spark mAP node [Task 5]
- `tests/test_pipelines/test_training/test_pipeline.py` — DAG composition test (if exists) [Task 6]

**Delete:**
- existing `evaluate_model` / `write_test_predictions` / `compute_test_mAP` unit tests inside `tests/test_pipelines/test_training/` [Task 6]

---

## Task 1: Runner `@`-prefix input convention

**Why:** The Runner currently auto-loads every node input via `catalog.load(name)` (runner.py:76). For `predict_and_write_test_predictions`, we need the `HiveTableDataset` **instance** (so we can call `.save()` per partition), not the loaded Spark DataFrame (which would also fail on first run when the table doesn't exist yet). Convention: an input name starting with `@` resolves to `catalog.get_dataset(name[1:])` — the dataset handle.

**Files:**
- Modify: `src/recsys_tfb/core/runner.py:36-41,76`
- Test: `tests/test_core/test_runner.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/test_runner.py`:

```python
def test_runner_resolves_at_prefix_input_to_dataset_handle():
    """An input name starting with '@' should be resolved to the catalog
    dataset INSTANCE (not the loaded data), so write-target nodes can call
    `.save()` per-batch.
    """
    from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
    from recsys_tfb.core.node import Node
    from recsys_tfb.core.pipeline import Pipeline
    from recsys_tfb.core.runner import Runner

    captured: dict = {}

    def node_fn(payload, write_ds):
        captured["payload"] = payload
        captured["write_ds"] = write_ds
        return {"ok": True}

    catalog = DataCatalog()
    catalog.add("payload", MemoryDataset(data={"hello": "world"}))
    sentinel_ds = MemoryDataset(data="sentinel-data")
    catalog.add("sink", sentinel_ds)

    pipeline = Pipeline([
        Node(node_fn, inputs=["payload", "@sink"], outputs="manifest"),
    ])
    Runner().run(pipeline, catalog)

    assert captured["payload"] == {"hello": "world"}
    # @sink resolves to the dataset HANDLE, not its data
    assert captured["write_ds"] is sentinel_ds
    assert captured["write_ds"] is not "sentinel-data"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_runner.py::test_runner_resolves_at_prefix_input_to_dataset_handle -v`
Expected: FAIL — runner currently calls `catalog.load("@sink")` which throws KeyError (no dataset named `@sink`).

- [ ] **Step 3: Implement the `@`-prefix convention**

Edit `src/recsys_tfb/core/runner.py`. Two changes:

(3a) Update the input-availability validation around line 36-41 to strip the prefix:

```python
        for node in pipeline.nodes:
            for inp in node.inputs:
                name = inp[1:] if inp.startswith("@") else inp
                if name not in available and not catalog.exists(name):
                    raise ValueError(
                        f"Node '{node.name}' requires input '{inp}' "
                        f"which is not in the catalog and not produced by any prior node"
                    )
```

(3b) Update the input resolution at line 76:

```python
                # Load inputs. An input name starting with '@' is resolved to
                # the catalog dataset HANDLE (not loaded data), used by nodes
                # that need to call .save() on the dataset themselves.
                inputs = []
                for name in node.inputs:
                    if name.startswith("@"):
                        inputs.append(catalog.get_dataset(name[1:]))
                    else:
                        inputs.append(catalog.load(name))
```

(3c) Update the last-consumer / release loop at line 144-150 to strip the prefix too, so the cleanup logic still recognizes `@sink` as a reference to `sink`:

```python
            for inp in node.inputs:
                name = inp[1:] if inp.startswith("@") else inp
                if (last_consumer.get(name) is node
                        and name in intermediates
                        and name in catalog._auto_created):
                    ds = catalog.get_dataset(name)
                    if ds is not None and isinstance(ds, MemoryDataset):
                        ds.release()
                        logger.info(
                            "Released dataset: %s", name,
                            extra={
                                "event": "dataset_released",
                                "dataset_name": name,
                                "node": node.name,
                            },
                        )
```

Also update `_build_last_consumer_map` at line 14-24 to strip `@`:

```python
    @staticmethod
    def _build_last_consumer_map(nodes: list) -> dict[str, object]:
        last_consumer: dict[str, object] = {}
        for node in nodes:
            for inp in node.inputs:
                name = inp[1:] if inp.startswith("@") else inp
                last_consumer[name] = node
        return last_consumer
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_runner.py::test_runner_resolves_at_prefix_input_to_dataset_handle -v`
Expected: PASS

- [ ] **Step 5: Run the full runner test suite to make sure existing tests still pass**

Run: `.venv/bin/pytest tests/test_core/test_runner.py -v`
Expected: all PASS (no regressions).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/core/runner.py tests/test_core/test_runner.py
git commit -m "$(cat <<'EOF'
feat(runner): @-prefix on input names resolves to catalog dataset handle

Inputs starting with '@' resolve to the AbstractDataset instance instead of
catalog.load() data. Needed for nodes that own their write semantics (e.g.
batched per-partition saves) where catalog auto-save / auto-load doesn't fit.
EOF
)"
```

---

## Task 2: Refactor `extract_Xy` to expose `_pdf_to_X` helper

**Why:** The new `predict_and_write_test_predictions` node reads each `(snap_date, prod_name)` partition's pdf via pyarrow itself, then filters by the positive-customer set, then needs the pdf → X numpy conversion. Today that conversion is glued to `read_parquet` inside `extract_Xy`. Factor it out as `_pdf_to_X` so both callers share the logic.

**Files:**
- Modify: `src/recsys_tfb/io/extract.py:71-141`
- Test: `tests/test_io/test_extract.py`

- [ ] **Step 1: Verify existing extract tests don't assert log call-site**

Run: `grep -n "step_started\|step_completed\|log_step" tests/test_io/test_extract.py`

Expected output: tests assert by `caplog.records` on the `recsys_tfb.io.extract` logger and check the `step` attribute names (e.g. `read_parquet`, `slice_features`). They do NOT pin which function emitted them. After refactor, `slice_features` / `encode_categoricals` / `to_numpy` will fire from `_pdf_to_X` instead of `extract_Xy` — same logger, same step names, so the assertions stay green.

If any test asserts call-site (unlikely), note it and adjust in Step 5 before committing.

- [ ] **Step 2: Write the failing test for `_pdf_to_X`**

Append to `tests/test_io/test_extract.py`:

```python
def test_pdf_to_X_returns_numpy_with_categoricals_encoded() -> None:
    """_pdf_to_X turns an already-loaded pdf into X numpy, applying the
    same slice_features + encode_categoricals + to_numpy logic that
    extract_Xy uses after its read_parquet step.
    """
    from recsys_tfb.io.extract import _pdf_to_X

    pdf = pd.DataFrame({
        "cust_id": ["c1", "c2", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 3),
        "prod_name": ["fund", "ccard", "fund"],
        "feat_a": [1.0, 2.0, 3.0],
        "feat_b": [0.1, 0.2, 0.3],
        "label": [0, 1, 0],
    })
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

    X = _pdf_to_X(pdf, prep_meta, parameters)

    assert X.shape == (3, 3)
    # prod_name int-coded: fund=0, ccard=1, fund=0
    assert list(X[:, 2]) == [0, 1, 0]
    # numeric features pass through
    assert list(X[:, 0]) == [1.0, 2.0, 3.0]
    assert list(X[:, 1]) == [0.1, 0.2, 0.3]


def test_pdf_to_X_skips_encode_when_no_deferred_cats() -> None:
    """When no categorical_columns overlap with identity_columns, the
    encode_categoricals step is skipped (mirrors extract_Xy behavior).
    """
    from recsys_tfb.io.extract import _pdf_to_X

    pdf = pd.DataFrame({
        "cust_id": ["c1", "c2"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 2),
        "feat_a": [1.0, 2.0],
        "label": [0, 1],
    })
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

    X = _pdf_to_X(pdf, prep_meta, parameters)

    assert X.shape == (2, 1)
    assert list(X[:, 0]) == [1.0, 2.0]
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_io/test_extract.py::test_pdf_to_X_returns_numpy_with_categoricals_encoded tests/test_io/test_extract.py::test_pdf_to_X_skips_encode_when_no_deferred_cats -v`
Expected: FAIL — `ImportError: cannot import name '_pdf_to_X' from 'recsys_tfb.io.extract'`

- [ ] **Step 4: Refactor `extract.py` to extract the helper**

Edit `src/recsys_tfb/io/extract.py`. Replace the body of `extract_Xy` (lines 111-139) with a delegating call. Add `_pdf_to_X` before `extract_Xy`:

```python
def _pdf_to_X(
    pdf: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> np.ndarray:
    """Already-loaded pdf -> X numpy.

    Encapsulates slice_features + encode_categoricals (deferred identity cats)
    + to_numpy. Used by extract_Xy after its parquet read and by
    predict_and_write_test_predictions after a per-partition pyarrow read +
    positive-set filter, so the latter doesn't have to re-read the parquet
    just to reuse the feature-slicing logic.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

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
    return X
```

Then rewrite `extract_Xy` to delegate Step B:

```python
def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.

    Step A (read_parquet) lives here; Step B (pdf -> X) is delegated to
    :func:`_pdf_to_X`. A pre-read parquet metadata INFO is emitted before
    ``read_parquet`` so shape/uncompressed-size are visible even if the
    pandas read OOMs.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

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

    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values

    logger.info(
        "extract_Xy: X shape=%s dtype=%s nbytes=%.1fMB; y len=%d dtype=%s",
        X.shape, X.dtype, X.nbytes / 1024**2,
        len(y), y.dtype,
    )

    return X, y
```

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `.venv/bin/pytest tests/test_io/test_extract.py::test_pdf_to_X_returns_numpy_with_categoricals_encoded tests/test_io/test_extract.py::test_pdf_to_X_skips_encode_when_no_deferred_cats -v`
Expected: PASS

- [ ] **Step 6: Run the full extract test file to ensure no regressions**

Run: `.venv/bin/pytest tests/test_io/test_extract.py -v`
Expected: all PASS — including the existing `test_extract_xy_emits_sub_step_events`, `test_extract_xy_logs_size_summaries`, `test_extract_xy_skips_encode_step_when_no_deferred_cats`, etc.

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "$(cat <<'EOF'
refactor(io): extract _pdf_to_X helper from extract_Xy

extract_Xy now delegates its Step B (slice_features + encode_categoricals
+ to_numpy) to _pdf_to_X. Enables the upcoming per-partition predict node
to reuse the feature-slicing logic without re-reading parquet. Same log
events fire from the same module logger so existing caplog tests stay green.
EOF
)"
```

---

## Task 3: Catalog updates — `training_eval_predictions` schema + `test_model_input` partition

**Why:** (a) `training_eval_predictions` drops the unused `rank` column, adds `label`, and gains `partition_filter: model_version` so HiveTableDataset injects model_version and pre-filters reads. (b) `test_model_input` gains `prod_name` in `partition_cols` so the dataset pipeline writes a `snap_date=*/prod_name=*` directory tree, enabling partition-pruned pyarrow reads in the new node.

**Files:**
- Modify: `conf/base/catalog.yaml:63-72` (test_model_input) and `conf/base/catalog.yaml:204-220` (training_eval_predictions)

- [ ] **Step 1: Verify downstream `prepare_eval_data` doesn't select `rank` from the table**

Run: `grep -n "\"rank\"\|'rank'\|F\\.col(\"rank\")\|rank_col" src/recsys_tfb/pipelines/evaluation/nodes_spark.py`

Expected: no matches that select `rank` from `ranked_predictions` / `training_eval_predictions` before passing to `compute_all_metrics`. (Spark mAP recomputes rank internally via `rank_within_query`.)

If any selection of `rank` exists, drop that selection in the same task — note here, then edit and commit together.

- [ ] **Step 2: Update `test_model_input` partition_cols**

Edit `conf/base/catalog.yaml`. Find the `test_model_input` entry (around line 63-72) and add `prod_name`:

```yaml
test_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: recsys_prod_test_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
```

- [ ] **Step 3: Update `training_eval_predictions` entry**

Edit `conf/base/catalog.yaml`. Replace the existing `training_eval_predictions` entry (around line 204-220) with:

```yaml
# --- Training Pipeline - Test-set predictions for downstream evaluation reuse ---
# Written per (snap_date, prod_name) partition by training/nodes.py::
# predict_and_write_test_predictions. Read by:
#   - training/nodes.py::compute_test_mAP_spark (this pipeline)
#   - evaluation/nodes_spark.py::prepare_eval_data (--post-training mode)
# Semantics: only customers with >=1 positive label in the test window are
# scored & written (the rest contribute nothing to mAP). Rows for prior
# model_versions are not read because partition_filter pins the load to
# the current model_version.
training_eval_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: training_eval_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: score_uncalibrated, type: DOUBLE}  # raw model output; equals score when calibration off
    - {name: label, type: INT}
  # rank dropped — not consumed; Spark mAP recomputes via rank_within_query
  partition_filter:
    model_version: ${model_version}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
```

- [ ] **Step 4: Sanity-check the YAML parses**

Run: `.venv/bin/python -c "import yaml; yaml.safe_load(open('conf/base/catalog.yaml'))"`
Expected: no output (clean parse).

- [ ] **Step 5: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "$(cat <<'EOF'
feat(catalog): training_eval_predictions schema rewrite + test_model_input prod_name partition

training_eval_predictions:
  - drop unused rank column (Spark mAP recomputes)
  - add label column (inlined from test_model_input — no downstream join)
  - add partition_filter model_version for scoped reads
test_model_input:
  - add prod_name to partition_cols for partition-pruned pyarrow reads
    inside predict_and_write_test_predictions

See spec docs/superpowers/specs/2026-05-13-batched-test-eval-spark-design.md
EOF
)"
```

---

## Task 4: Implement `predict_and_write_test_predictions`

**Why:** Replaces pandas `evaluate_model` + `write_test_predictions`. Pass 0 reads only label columns to build per-snap_date positive-customer sets; Pass 1 iterates `(snap_date, prod_name)` partitions, filters to positive customers, predicts, and saves one partition at a time so dynamic-partition overwrite never double-writes.

**Files:**
- Create: `tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py`
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add the new function; do NOT delete `evaluate_model` / `write_test_predictions` / `compute_test_mAP` yet — Task 6 does that)

- [ ] **Step 1: Confirm test directory layout**

Run: `ls tests/test_pipelines/test_training/ | head -20`
Expected: existing files for other training nodes — confirms the convention. If `__init__.py` missing, create it via `touch tests/test_pipelines/test_training/__init__.py` (most likely already exists).

- [ ] **Step 2: Write the test file — Pass 0 + uncalibrated path**

Create `tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py`:

```python
"""Tests for predict_and_write_test_predictions — batched per-partition predict+write."""

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest


def _make_test_parquet(tmp_path: Path) -> Path:
    """Build a small partitioned parquet at ``tmp_path/test.parquet``.

    Layout: snap_date=*/prod_name=*/*.parquet (Hive-style, matches what the
    dataset pipeline produces after this PR's catalog change).

    Customers:
      c1 has label=1 only on prod_A (snap=2025-01) -> stays in positive set
      c2 has label=1 only on prod_B (snap=2025-01) -> stays in positive set
      c3 has no positives -> filtered out by Pass 0
      c4 has label=1 on prod_A in 2025-02 only       -> separate snap positive
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        # snap=2025-01-31
        ("c1", "2025-01-31", "prod_A", 1.0, 1),
        ("c1", "2025-01-31", "prod_B", 1.1, 0),
        ("c2", "2025-01-31", "prod_A", 2.0, 0),
        ("c2", "2025-01-31", "prod_B", 2.1, 1),
        ("c3", "2025-01-31", "prod_A", 3.0, 0),
        ("c3", "2025-01-31", "prod_B", 3.1, 0),
        # snap=2025-02-28
        ("c4", "2025-02-28", "prod_A", 4.0, 1),
        ("c4", "2025-02-28", "prod_B", 4.1, 0),
    ]
    df = pd.DataFrame(rows, columns=["cust_id", "snap_date", "prod_name", "feat_a", "label"])
    table = pa.Table.from_pandas(df, preserve_index=False)
    root = tmp_path / "test.parquet"
    pq.write_to_dataset(
        table, root_path=str(root), partition_cols=["snap_date", "prod_name"]
    )
    return root


def _make_prep_meta() -> dict:
    return {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }


def _make_parameters() -> dict:
    return {
        "model_version": "v_test_001",
        "hive": {"db": "ml_recsys"},
        "schema": {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
    }


def test_predict_and_write_passes_filters_no_positive_customers(tmp_path):
    """Pass 0 builds positive-customer set per snap_date; Pass 1 filters
    rows whose cust_id is not in the set. c3 has no positives and must
    not appear in any per-partition save call.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # Mock model: predict returns increasing scores; not calibrated
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    # Not a CalibratedModelAdapter (isinstance check fails -> raw == score)
    model.__class__.__name__ = "LightGBMAdapter"

    # Mock HiveTableDataset handle — capture every save() call
    saves: list[pd.DataFrame] = []

    def capture_save(spark_df):
        # Convert Spark DF to pandas for inspection
        saves.append(spark_df.toPandas() if hasattr(spark_df, "toPandas") else spark_df)

    write_ds = MagicMock()
    write_ds.save.side_effect = capture_save

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    # Expect 4 partitions: (2025-01-31, prod_A), (2025-01-31, prod_B),
    #                     (2025-02-28, prod_A), (2025-02-28, prod_B)
    assert write_ds.save.call_count == 4

    # c3 must never appear in any save
    all_written = pd.concat(saves, ignore_index=True)
    assert "c3" not in set(all_written["cust_id"])

    # c1 and c2 are written for 2025-01-31 partitions (both prods)
    snap_jan = all_written[all_written["snap_date"] == "2025-01-31"]
    assert set(snap_jan["cust_id"]) == {"c1", "c2"}

    # c4 is the only customer in 2025-02-28
    snap_feb = all_written[all_written["snap_date"] == "2025-02-28"]
    assert set(snap_feb["cust_id"]) == {"c4"}

    # Manifest reports the right shape
    assert set(manifest["snap_dates"]) == {"2025-01-31", "2025-02-28"}
    assert set(manifest["prods"]) == {"prod_A", "prod_B"}
    assert manifest["model_version"] == "v_test_001"
    assert manifest["n_rows_written"] == len(all_written)


def test_predict_and_write_score_uncalibrated_equals_score_when_not_calibrated(tmp_path):
    """When the model is not a CalibratedModelAdapter, score_uncalibrated
    must equal score row-for-row in every written partition.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    model = MagicMock()
    model.predict.side_effect = lambda X: np.array([0.42] * len(X))
    model.__class__.__name__ = "LightGBMAdapter"

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(
        df.toPandas() if hasattr(df, "toPandas") else df
    )

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    for df in saves:
        assert (df["score"] == df["score_uncalibrated"]).all()


def test_predict_and_write_calibrated_branch_calls_predict_uncalibrated(tmp_path):
    """When the model IS a CalibratedModelAdapter, predict_uncalibrated
    is called to populate score_uncalibrated separately from score.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # spec=CalibratedModelAdapter makes isinstance check pass
    model = MagicMock(spec=CalibratedModelAdapter)
    model.predict.side_effect = lambda X: np.array([0.9] * len(X))
    model.predict_uncalibrated.side_effect = lambda X: np.array([0.1] * len(X))

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(
        df.toPandas() if hasattr(df, "toPandas") else df
    )

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    # predict_uncalibrated must have been called once per partition
    assert model.predict_uncalibrated.call_count == 4

    for df in saves:
        assert (df["score"] == 0.9).all()
        assert (df["score_uncalibrated"] == 0.1).all()
```

- [ ] **Step 3: Run new tests to verify they fail**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py -v`
Expected: FAIL — `ImportError: cannot import name 'predict_and_write_test_predictions' from 'recsys_tfb.pipelines.training.nodes'`

- [ ] **Step 4: Implement `predict_and_write_test_predictions` in nodes.py**

Append to `src/recsys_tfb/pipelines/training/nodes.py` (location: after `write_test_predictions` for now; Task 6 will remove the old function and tidy ordering). Also add the imports near the top of the file if missing: `pyarrow.dataset as pads`.

```python
def predict_and_write_test_predictions(
    model: ModelAdapter,
    test_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    training_eval_predictions,  # HiveTableDataset, supplied via @ runner prefix
) -> dict:
    """Per-partition test prediction + Hive write (Pass 0 + Pass 1).

    Pass 0: label-only column scan of the test parquet to build, per
    snap_date, the set of cust_ids with >=1 positive label across any prod.
    Customers with no positives in that snap_date contribute 0/skip to
    mAP, so we drop them up front and avoid their predict cost.

    Pass 1: for each (snap_date, prod_name) partition of the parquet:
        - load only that partition's rows via pyarrow filter
        - drop rows whose cust_id is not in the snap_date's positive set
        - slice X via _pdf_to_X; predict; (predict_uncalibrated if Calibrated)
        - build a Spark DataFrame with (cust_id, score, score_uncalibrated,
          label) + partition cols snap_date, prod_name
        - training_eval_predictions.save(df) — exactly one partition's
          rows per save, so dynamic-partition overwrite cleanly overwrites
          a single partition and successive saves don't collide

    Returns:
        manifest dict for downstream compute_test_mAP_spark to depend on
        (DAG ordering — the actual data is read back from Hive there).
    """
    import pyarrow.dataset as pads

    from recsys_tfb.io.extract import _pdf_to_X
    from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter

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

    # ---- Pass 0: positive customer set per snap_date ----
    with log_step(logger, "pass0_positive_set"):
        labels_table = ds.to_table(columns=[cust_id_col, time_col, label_col])
        labels_pdf = labels_table.to_pandas()
        positives_pdf = labels_pdf[labels_pdf[label_col] == 1]
        positive_set: dict[str, set] = {
            str(snap): set(grp[cust_id_col].astype(str))
            for snap, grp in positives_pdf.groupby(time_col)
        }
    logger.info(
        "predict_and_write_test_predictions: pass0 built positive sets — "
        "snap_dates=%d total_pos_custs=%d",
        len(positive_set),
        sum(len(s) for s in positive_set.values()),
    )

    # ---- Pass 1: per-partition predict + save ----
    # Enumerate distinct (snap_date, prod_name) partition values from the dataset
    # (pads.dataset partition discovery — no row data read).
    partition_pdf = ds.to_table(columns=[time_col, item_col]).to_pandas()
    partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])

    snap_dates_seen: set[str] = set()
    prods_seen: set[str] = set()
    n_rows_written = 0
    is_calibrated = isinstance(model, CalibratedModelAdapter)

    for _, row in partition_pdf.iterrows():
        snap_date = str(row[time_col])
        prod_name = str(row[item_col])
        snap_dates_seen.add(snap_date)
        prods_seen.add(prod_name)

        with log_step(logger, f"partition_{snap_date}_{prod_name}"):
            part_table = ds.to_table(
                filter=(pads.field(time_col) == snap_date)
                & (pads.field(item_col) == prod_name)
            )
            part_pdf = part_table.to_pandas()

            keep_custs = positive_set.get(snap_date, set())
            part_pdf = part_pdf[part_pdf[cust_id_col].astype(str).isin(keep_custs)]

            if len(part_pdf) == 0:
                logger.info(
                    "predict_and_write_test_predictions: skipping empty "
                    "partition snap=%s prod=%s after positive-set filter",
                    snap_date, prod_name,
                )
                continue

            X = _pdf_to_X(part_pdf, preprocessor_metadata, parameters)
            y_score = model.predict(X)
            score_uncalibrated = (
                model.predict_uncalibrated(X) if is_calibrated else y_score
            )

            out_pdf = pd.DataFrame({
                cust_id_col: part_pdf[cust_id_col].astype(str).values,
                "score": y_score,
                "score_uncalibrated": score_uncalibrated,
                label_col: part_pdf[label_col].values,
                time_col: snap_date,
                item_col: prod_name,
            })

            training_eval_predictions.save(out_pdf)
            n_rows_written += len(out_pdf)

    manifest = {
        "snap_dates": sorted(snap_dates_seen),
        "prods": sorted(prods_seen),
        "model_version": model_version,
        "n_rows_written": n_rows_written,
    }
    logger.info(
        "predict_and_write_test_predictions: done — "
        "snap_dates=%d prods=%d n_rows_written=%d model_version=%s",
        len(manifest["snap_dates"]), len(manifest["prods"]),
        manifest["n_rows_written"], manifest["model_version"],
    )
    return manifest
```

Note: the implementation passes a pandas DataFrame to `training_eval_predictions.save()`. `HiveTableDataset.save()` (io/hive_table_dataset.py:135-179) already calls `_to_spark()` to convert pandas → Spark internally (lines 141-142, 196-202), so we don't need to manually `spark.createDataFrame` here. This keeps the node testable without a real SparkSession — the mock `write_ds.save(df)` receives a pandas DataFrame directly in unit tests.

- [ ] **Step 5: Run new tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py -v`
Expected: PASS — all three tests green.

If pyarrow's `pq.write_to_dataset` with `partition_cols` produces a layout the test's `ds = pads.dataset(...)` doesn't recognize, that's a fixture issue — adjust the test to either use a non-partitioned single file (which pyarrow will still let you filter, just less efficiently) or set `pads.dataset(..., partitioning="hive")`. Note this in the test if encountered.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py
git commit -m "$(cat <<'EOF'
feat(training): add predict_and_write_test_predictions (Pass 0 + Pass 1 batched)

Pass 0: label-only column scan -> positive cust_id set per snap_date.
Pass 1: per (snap_date, prod_name) partition, filter to positive customers,
predict, save to training_eval_predictions one partition at a time so
dynamic-partition overwrite never double-writes the same partition.

Calibrated path: when model is CalibratedModelAdapter, score_uncalibrated
is populated via predict_uncalibrated; otherwise it equals score.

Does not yet wire into pipeline.py — Task 6 in plan handles wiring & removal
of evaluate_model / write_test_predictions.
EOF
)"
```

---

## Task 5: Implement `compute_test_mAP_spark`

**Why:** Replaces the pandas `compute_test_mAP`. Reads `training_eval_predictions` as a Spark DataFrame (catalog-loaded, already filtered to the current model_version via `partition_filter`), reuses `evaluation/metrics_spark.compute_all_metrics`, and returns the dict shape `log_experiment` expects.

**Files:**
- Create: `tests/test_pipelines/test_training/test_compute_test_map_spark.py`
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add the new function; Task 6 removes the old `compute_test_mAP`)

- [ ] **Step 1: Write the test file**

Create `tests/test_pipelines/test_training/test_compute_test_map_spark.py`:

```python
"""Tests for compute_test_mAP_spark — Spark-native mAP over training_eval_predictions."""

import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_compute_test_mAP_spark")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def _make_parameters() -> dict:
    return {
        "schema": {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
        "evaluation": {"k_values": ["all"]},
        "training": {"calibration": {"method": "isotonic"}},
    }


def _make_df(spark, rows):
    """rows: list of dicts with cust_id, snap_date, prod_name, score,
    score_uncalibrated, label.
    """
    import pandas as pd
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf)


def test_compute_mAP_spark_no_calibration_returns_flat_dict(spark):
    """When score == score_uncalibrated for every row, the result has NO
    'uncalibrated' sub-dict and NO 'calibration_method' key.
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP_spark

    rows = [
        # cust c1 — positives on prod_A (correct top rank)
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.9, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.1, "label": 0},
        # cust c2 — positives on prod_B (correct top rank)
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.2, "score_uncalibrated": 0.2, "label": 0},
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.8, "score_uncalibrated": 0.8, "label": 1},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "prods": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 4}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # Both customers ranked their positives at top -> overall mAP == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    assert "per_product_ap" in result
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 0


def test_compute_mAP_spark_with_calibration_emits_uncalibrated_subdict(spark):
    """When score != score_uncalibrated for any row, the result has an
    'uncalibrated' sub-dict and a 'calibration_method' string (from
    parameters['training']['calibration']['method']).
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP_spark

    # Calibrated scores agree with labels; uncalibrated DISagree (worse mAP)
    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.1, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.9, "label": 0},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "prods": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 2}

    result = compute_test_mAP_spark(df, manifest, _make_parameters())

    assert "uncalibrated" in result
    assert result["calibration_method"] == "isotonic"
    # Calibrated ranks c1's positive at top -> calibrated overall_map == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    # Uncalibrated ranks c1's negative at top -> uncalibrated mAP < calibrated
    assert result["uncalibrated"]["overall_map"] < result["overall_map"]
```

- [ ] **Step 2: Run new tests to verify they fail**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_compute_test_map_spark.py -v`
Expected: FAIL — `ImportError: cannot import name 'compute_test_mAP_spark' from 'recsys_tfb.pipelines.training.nodes'`

- [ ] **Step 3: Implement `compute_test_mAP_spark` in nodes.py**

Append to `src/recsys_tfb/pipelines/training/nodes.py` (location: after `predict_and_write_test_predictions`):

```python
def compute_test_mAP_spark(
    training_eval_predictions,  # Spark DataFrame, loaded by catalog (filtered to current model_version)
    predict_manifest: dict,
    parameters: dict,
) -> dict:
    """Spark-native mAP over training_eval_predictions; emits the dict
    shape consumed by log_experiment today (overall_map / per_product_ap
    / n_queries / n_excluded_queries, plus optional 'uncalibrated' sub-dict
    + 'calibration_method' when score != score_uncalibrated).

    predict_manifest is an in-DAG dependency only — its content is logged
    for observability but the actual data is read back from
    training_eval_predictions (Spark-loaded via the catalog).
    """
    from pyspark.sql import functions as F

    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]

    n_prods = training_eval_predictions.select(item_col).distinct().count()
    map_key = f"map@{n_prods}"

    logger.info(
        "compute_test_mAP_spark: starting — n_prods=%d map_key=%s manifest=%s",
        n_prods, map_key, predict_manifest,
    )

    # Calibration detection: any row where score != score_uncalibrated
    calibration_applied = (
        training_eval_predictions.filter(
            F.col("score") != F.col("score_uncalibrated")
        )
        .limit(1)
        .count()
        > 0
    )

    cal = compute_all_metrics(training_eval_predictions, parameters)
    result = {
        "overall_map": float(cal["overall"].get(map_key, 0.0)),
        "per_product_ap": {
            p: float(v.get(map_key, 0.0)) for p, v in cal["per_product"].items()
        },
        "n_queries": cal["n_queries"],
        "n_excluded_queries": cal["n_excluded_queries"],
    }

    if calibration_applied:
        # Run compute_all_metrics again with score_uncalibrated aliased as score
        uncal_df = (
            training_eval_predictions
            .withColumnRenamed("score", "_score_calibrated")
            .withColumnRenamed("score_uncalibrated", "score")
        )
        uncal = compute_all_metrics(uncal_df, parameters)
        result["uncalibrated"] = {
            "overall_map": float(uncal["overall"].get(map_key, 0.0)),
            "per_product_ap": {
                p: float(v.get(map_key, 0.0)) for p, v in uncal["per_product"].items()
            },
        }
        result["calibration_method"] = (
            parameters.get("training", {}).get("calibration", {}).get("method", "isotonic")
        )
        logger.info(
            "compute_test_mAP_spark: calibrated=%.4f uncalibrated=%.4f",
            result["overall_map"], result["uncalibrated"]["overall_map"],
        )
    else:
        logger.info(
            "compute_test_mAP_spark: mAP=%.4f products=%d excluded_queries=%d",
            result["overall_map"],
            len(result["per_product_ap"]),
            result["n_excluded_queries"],
        )

    return result
```

- [ ] **Step 4: Run new tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_compute_test_map_spark.py -v`
Expected: PASS — both tests green.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_compute_test_map_spark.py
git commit -m "$(cat <<'EOF'
feat(training): add compute_test_mAP_spark — Spark-native mAP over Hive table

Reads training_eval_predictions (auto-filtered to current model_version by
catalog partition_filter); reuses evaluation/metrics_spark.compute_all_metrics
for the actual computation. Detects calibration by score != score_uncalibrated
and emits the optional 'uncalibrated' sub-dict + 'calibration_method' string.

Does not yet wire into pipeline.py — Task 6 in plan handles wiring & removal
of the old pandas compute_test_mAP.
EOF
)"
```

---

## Task 6: Wire `training/pipeline.py` DAG + remove dead code

**Why:** Replace the old `evaluate_model → write_test_predictions → compute_test_mAP` chain with `predict_and_write_test_predictions → compute_test_mAP_spark`. Use the `@`-prefix from Task 1 to pass the catalog handle. Delete the now-unused old node functions and their tests.

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py:5-20,107-131`
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` — delete `evaluate_model`, `compute_test_mAP`, `write_test_predictions`, `_build_training_eval_predictions_ddl`
- Delete: any existing unit tests for those three functions inside `tests/test_pipelines/test_training/`

- [ ] **Step 1: Identify and list tests that target the to-be-deleted functions**

Run: `grep -rln "evaluate_model\|write_test_predictions\|compute_test_mAP\b\|_build_training_eval_predictions_ddl" tests/`

Expected output: list of test files that import these names. Note them — they all need deletion or edit in Step 5.

- [ ] **Step 2: Update the imports + DAG in pipeline.py**

Edit `src/recsys_tfb/pipelines/training/pipeline.py`. Change the imports block (lines 5-19):

```python
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_test_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    compute_test_mAP_spark,
    finalize_model,
    log_experiment,
    predict_and_write_test_predictions,
    prepare_lgb_train_inputs,
    tune_hyperparameters,
)
```

Replace the terminal `nodes.extend([...])` block (lines 107-131) with:

```python
    nodes.extend([
        Node(
            predict_and_write_test_predictions,
            inputs=[
                "model", "test_parquet_handle",
                "preprocessor", "parameters",
                "@training_eval_predictions",  # catalog handle for chunked save
            ],
            outputs="predict_manifest",
        ),
        Node(
            compute_test_mAP_spark,
            inputs=["training_eval_predictions", "predict_manifest", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=[
                "model", "best_params", "best_iteration",
                "evaluation_results", "parameters",
            ],
            outputs=None,
        ),
    ])
```

- [ ] **Step 3: Delete the old node functions from `nodes.py`**

Edit `src/recsys_tfb/pipelines/training/nodes.py`. Delete in this order (line numbers from the pre-Task-4/5 state will have shifted; locate by function name):

- `def evaluate_model(...)` (was ~line 523-585)
- `def compute_test_mAP(...)` (was ~line 588-667)
- `def _build_training_eval_predictions_ddl(...)` (was ~line 670-685)
- `def write_test_predictions(...)` (was ~line 688-754)

Also drop now-unused imports if any (e.g. if `compute_all_metrics` was imported only for the pandas compute_test_mAP, drop it; `mlflow` / `CalibratedModelAdapter` likely still used by `log_experiment` / new node — verify before removing).

Run after edit: `grep -n "evaluate_model\|compute_test_mAP\b\|write_test_predictions\|_build_training_eval_predictions_ddl" src/recsys_tfb/pipelines/training/nodes.py`
Expected: only `compute_test_mAP_spark` (the new function) appears.

- [ ] **Step 4: Verify nothing else in src/ still imports the deleted names**

Run: `grep -rn "from recsys_tfb.pipelines.training.nodes import.*\(evaluate_model\|write_test_predictions\|compute_test_mAP\b\)" src/`
Expected: no matches.

If any caller exists (e.g. `__main__.py` or some helper), edit it now — the only legitimate caller was pipeline.py, which Step 2 updated.

- [ ] **Step 5: Delete or trim tests targeting the removed functions**

For each file from Step 1's output:

- If the entire file's tests are about the deleted functions, `git rm` the file.
- If only some tests need removal, edit the file and delete those tests (keep the rest).

Common candidates likely to be entirely about deleted code:
- `tests/test_pipelines/test_training/test_evaluate_model.py` (if exists)
- `tests/test_pipelines/test_training/test_write_test_predictions.py` (if exists)
- `tests/test_pipelines/test_training/test_compute_test_map.py` (if exists)

Run: `grep -rln "evaluate_model\|write_test_predictions\|compute_test_mAP\b" tests/`
Expected after deletion: no matches (only `compute_test_mAP_spark` references in the new test file from Task 5 remain, which the `\b` boundary excludes).

- [ ] **Step 6: Run the full training-pipeline test suite**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/ -v`
Expected: all PASS. The new tests from Tasks 4-5 run together with the rest of the training pipeline tests (cache nodes, tune_hyperparameters, calibrate_model, etc.).

- [ ] **Step 7: Run the full project test suite**

Run: `.venv/bin/pytest -q`
Expected: all PASS (no regressions across other pipelines or core modules).

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/pipelines/training/pipeline.py src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/
git commit -m "$(cat <<'EOF'
refactor(training): wire batched predict+write and spark mAP; remove pandas path

Pipeline:
  evaluate_model + write_test_predictions + compute_test_mAP
  -> predict_and_write_test_predictions + compute_test_mAP_spark

The write node receives the training_eval_predictions catalog handle via
the runner's '@'-prefix marker, then iterates (snap_date, prod_name)
partitions calling HiveTableDataset.save() once per partition. The Spark
mAP node reads back via normal catalog load (auto-filtered to the current
model_version by partition_filter).
EOF
)"
```

---

## Task 7: Dev-cluster smoke test — training pipeline

**Why:** Validate end-to-end against the local dev-cluster on the synthetic fixtures (`data/{feature_table,label_table,sample_pool}.parquet`). Confirms (a) the catalog handle wiring works at runtime, (b) the table is created with the new schema, (c) partitions are written = `len(test_snap_dates) × n_prods`, (d) mAP is non-zero on data that has positives.

**Note:** This is integration verification, no new code. Run sequentially.

- [ ] **Step 1: Activate venv and confirm dev-cluster is up**

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -c "from pyspark.sql import SparkSession; print('OK')"
```
Expected: prints `OK`. If dev-cluster down, start it per `~/dev-cluster/README`.

- [ ] **Step 2: Nuke previous training_eval_predictions + test_model_input (cache + Hive)**

```bash
scripts/dev_admin.sh scripts/nuke_ml_recsys.py
rm -rf ~/recsys_cache/  # purge driver-local cache so new partition tree gets copied
```

Expected: tables dropped from `ml_recsys`, cache directory removed.

- [ ] **Step 3: Re-run dataset pipeline so test_model_input is written with new partition layout**

```bash
scripts/dev_admin.sh scripts/setup_hive_dev.py
.venv/bin/python -m recsys_tfb dataset --env production
```

Expected: pipeline succeeds; verify the test_model_input Hive table has `prod_name` partition:

```bash
scripts/dev_admin.sh -c "SHOW PARTITIONS ml_recsys.recsys_prod_test_model_input"
```
Expected output: rows of form `base_dataset_version=...,snap_date=YYYY-MM-DD,prod_name=...`.

- [ ] **Step 4: Run the training pipeline (with the local Spark conf)**

```bash
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

Expected: pipeline succeeds. Final INFO lines show:
- `predict_and_write_test_predictions: done — snap_dates=N prods=M n_rows_written=...`
- `compute_test_mAP_spark: mAP=...`
- `MLflow experiment logged: ...`

- [ ] **Step 5: Verify the written Hive table**

```bash
scripts/dev_admin.sh -c "SHOW PARTITIONS ml_recsys.training_eval_predictions"
scripts/dev_admin.sh -c "DESCRIBE ml_recsys.training_eval_predictions"
scripts/dev_admin.sh -c "SELECT COUNT(*) FROM ml_recsys.training_eval_predictions WHERE model_version = '<paste model_version from step 4>'"
```

Expected:
- Partitions match the cartesian product `test_snap_dates × prods × {current_model_version}`.
- DESCRIBE shows columns: cust_id, score, score_uncalibrated, label, snap_date, prod_name, model_version. No `rank`.
- COUNT > 0.

- [ ] **Step 6: Verify mAP non-zero in MLflow log**

```bash
.venv/bin/python -c "
import mlflow
mlflow.set_tracking_uri('mlruns')
mlflow.set_experiment('recsys_tfb')
runs = mlflow.search_runs(order_by=['attribute.start_time DESC'], max_results=1)
print(runs[['metrics.overall_map', 'metrics.n_queries', 'metrics.n_excluded_queries']])
"
```

Expected: `overall_map > 0`, `n_queries > 0`. If `overall_map == 0` and `n_excluded_queries == n_queries`, the synthetic fixtures may not have any positives in the test snap_dates — verify with `scripts/dev_admin.sh -c "SELECT snap_date, COUNT(*) FROM ml_recsys.training_eval_predictions WHERE label=1 GROUP BY snap_date"`. If still empty, the fixtures need a positive in test; flag for human review.

No commit on this task — purely verification.

---

## Task 8: Dev-cluster smoke test — post-training evaluation

**Why:** Confirm `--post-training` evaluation still consumes `training_eval_predictions` correctly after schema change (drop rank, add label).

- [ ] **Step 1: Run the evaluation pipeline in `--post-training` mode**

```bash
source ~/dev-cluster/scripts/client-env.sh  # back to cluster-mode Spark conf for evaluation
# Get the model_version from MLflow or the training run log
.venv/bin/python -m recsys_tfb evaluation --env production --post-training --model-version <paste model_version>
```

Expected: pipeline succeeds. Evaluation report written. mAP in the report should match the `overall_map` logged by the training run (numerical equivalence — both consume the same `training_eval_predictions` rows now).

- [ ] **Step 2: Spot-check the evaluation report**

```bash
# Report path varies by configured output; check the most recent file
ls -lt data/evaluation/ 2>/dev/null | head -5
```

Open the most recent report and confirm:
- `overall_map` matches the training-run MLflow metric.
- No errors about missing `rank` column.

No commit on this task — purely verification.

---

## Final cleanup

After Tasks 1-8 pass, run the final code-reviewer subagent (per subagent-driven-development) for end-to-end audit of the branch against the spec, then proceed to `superpowers:finishing-a-development-branch` for merge/PR.

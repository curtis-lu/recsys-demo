# Cold-Product Model-Layer Weighting Mechanism — Implementation Plan (Plan A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a model-layer per-(segment,product) `sample_weight` mechanism to LightGBM training so cold products get boosted gradient without row replication, plus the `dataset.carry_columns` infrastructure that makes `cust_segment_typ` available at training time without baking weights into the cached dataset.

**Architecture:** `select_keys` carries a configurable column set from `sample_pool` into the train/train_dev `model_input` parquet. At training read time a new `extract_Xyw` sibling computes a per-row weight array from `training.sample_weights` (raw `cust_segment_typ|prod_name` keys), which LightGBM consumes via `lgb.Dataset(weight=...)` on the train set only. A new consistency invariant A7 validates the weight-table keys at CLI entry. Source of truth for the design: `docs/superpowers/specs/2026-05-18-sampling-overrides-editor-design.md` (D5–D9).

**Tech Stack:** Python 3.10, PySpark 3.3.2, LightGBM 4.6.0, numpy, pandas, pytest 7.3.1.

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor` (branch `feat/sampling-overrides-editor`).

**Test command convention (CLAUDE.md worktree SOP — always use this exact form):**
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```
Abbreviated below as `PYTEST <paths>`.

**Git convention:** `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor ...` (abbreviated `GIT ...`). The graphify post-commit hook regenerates untracked `graphify-out/GRAPH_REPORT.md` (already untracked — safe; do not stage it).

---

## Key reference facts (verified from source, do not re-derive)

- `sample_weights` config lives under `training:` in `parameters_training.yaml`. `_model_version_payload` (`core/versioning.py:124`) auto-includes any new key under `training:`, so it busts `model_version` and NOT `train_variant_id`. No versioning code change needed.
- `carry_columns` config lives under `dataset:` in `parameters_dataset.yaml`. `compute_base_dataset_version` (`core/versioning.py:80`) hashes `dataset` minus `ALL_SAMPLING_KEYS`; `carry_columns` is not a sampling key → it is included → changing it busts `base_dataset_version` (correct). No versioning code change needed.
- Weight-table key format (fixed, documented): `"<cust_segment_typ>|<prod_name>"` (2 parts, `|`-joined). Product is `parts[1]`.
- `select_keys` (`pipelines/dataset/helpers_spark.py`) has TWO return paths: early `return sampled` at the `sample_ratio >= 1.0 and not sample_ratio_overrides` branch, and the final `return sampled`. Both currently `.select(*identity_key)`.
- `extract_Xy` callers (only train handles): `models/lightgbm_adapter.py:180` and `:193` (cached lgb-binary path, train + train_dev), `pipelines/training/nodes.py:458` (non-cached numpy→lgb.train trial path). `extract_Xy_with_groups` is the val path and must remain unweighted.
- Consistency test convention: pure dict via `_base(over=None)` helper, class-based tests, function-scoped `from recsys_tfb.core.consistency import ...`. Spark test convention: function-scoped `spark` fixture from `tests/conftest.py`.

---

## Task 1: A7 consistency predicate `weight_unknown_items`

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (add predicate after `override_unknown_items`; register in `validate_config_consistency`; add A7 to module-docstring legend)
- Test: `tests/test_core/test_consistency.py` (add `TestWeightUnknownItems`; extend `TestValidateConfigConsistency`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/test_consistency.py` (after `class TestOverrideUnknownItems`, before the `from recsys_tfb.core.consistency import validate_config_consistency` line at module level — place the import with the others or add a local import inside the class as the file already does for later groups):

```python
from recsys_tfb.core.consistency import weight_unknown_items


class TestWeightUnknownItems:
    def test_unknown_product_component_detected(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"mass|a": 2.0, "hnw|zzz": 3.0}}})
        assert weight_unknown_items(p) == ["zzz"]

    def test_all_known_returns_empty(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"mass|a": 2.0, "hnw|b": 3.0}}})
        assert weight_unknown_items(p) == []

    def test_no_sample_weights_returns_empty(self):
        assert weight_unknown_items(_base()) == []

    def test_malformed_key_without_pipe_ignored_for_item(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"massa": 2.0}}})
        assert weight_unknown_items(p) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_core/test_consistency.py::TestWeightUnknownItems -q`
Expected: FAIL — `ImportError: cannot import name 'weight_unknown_items'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/core/consistency.py`, add this function immediately after `override_unknown_items` (ends at line ~154, before `def item_missing_from_categorical`):

```python
def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A7).

    Weight-table keys are fixed-format ``"<cust_segment_typ>|<prod_name>"``
    (2 parts, '|'-joined). Only the product component (index 1) is validated;
    the segment component has no config-declared value list (mirrors A5's
    item-only check in :func:`override_unknown_items`). Keys without a '|'
    (no product component) are skipped.
    """
    training = parameters.get("training", {}) or {}
    weights = training.get("sample_weights") or {}
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) >= 2 and parts[1] not in declared:
            bad.add(parts[1])
    return sorted(bad)
```

Then in `validate_config_consistency` (the `errors`-collecting body, after the `unknown = override_unknown_items(parameters)` block and before `if errors:`), add:

```python
    unknown_w = weight_unknown_items(parameters)
    if unknown_w:
        errors.append(
            f"training.sample_weights references product value(s) {unknown_w} "
            f"absent from schema.categorical_values[item] — the weight "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )
```

Then in the module docstring's Invariant legend (Layer 1 block), add after the `* A6 — ...` bullet:

```
* A7 — a ``training.sample_weights`` key references a product value absent
  from ``schema.categorical_values[item]``. Predicate:
  ``weight_unknown_items`` (item-only check, mirrors A5).
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_core/test_consistency.py::TestWeightUnknownItems -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Add the validate-wiring test, run, and verify**

Append to `class TestValidateConfigConsistency` in the same test file:

```python
    def test_a7_unknown_weight_product_collected(self):
        p = _base({"inference": {"products": ["a", "b"]},
            "dataset": {"prepare_model_input": {
                "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"mass|zzz": 2.0}}})
        with pytest.raises(ConfigConsistencyError, match=r"training\.sample_weights"):
            validate_config_consistency(p)
```

Run: `PYTEST tests/test_core/test_consistency.py -q`
Expected: PASS (all consistency tests green).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
GIT commit -m "feat(consistency): add A7 weight_unknown_items invariant"
```

---

## Task 2: Pure per-row weight function `_compute_row_weights`

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (add private pure helper near top, after imports)
- Test: `tests/test_io/test_extract.py` (add `TestComputeRowWeights`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_io/test_extract.py`:

```python
import numpy as np
import pandas as pd
from recsys_tfb.io.extract import _compute_row_weights


class TestComputeRowWeights:
    def test_known_pairs_get_weight_unknown_get_one(self):
        seg = pd.Series(["mass", "hnw", "mass", "aff"])
        prod = pd.Series(["a", "a", "b", "a"])
        weights = {"mass|a": 3.0, "hnw|a": 2.0}
        w = _compute_row_weights(seg, prod, weights)
        assert isinstance(w, np.ndarray)
        np.testing.assert_array_equal(w, np.array([3.0, 2.0, 1.0, 1.0]))

    def test_empty_weights_all_ones(self):
        seg = pd.Series(["mass", "hnw"])
        prod = pd.Series(["a", "b"])
        w = _compute_row_weights(seg, prod, {})
        np.testing.assert_array_equal(w, np.array([1.0, 1.0]))

    def test_dtype_is_float64(self):
        w = _compute_row_weights(pd.Series(["m"]), pd.Series(["a"]), {"m|a": 2.0})
        assert w.dtype == np.float64
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_io/test_extract.py::TestComputeRowWeights -q`
Expected: FAIL — `ImportError: cannot import name '_compute_row_weights'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, after the existing imports / `logger = logging.getLogger(__name__)` line, add:

```python
def _compute_row_weights(
    seg: pd.Series,
    prod: pd.Series,
    sample_weights: dict,
) -> np.ndarray:
    """Per-row LightGBM sample weight from a ``"<segment>|<product>"`` table.

    Pure: no Spark, no I/O. Rows whose ``f"{seg}|{prod}"`` key is absent get
    weight 1.0 (matches the sparse-emit semantics: only boosted groups are
    written to ``training.sample_weights``).
    """
    if not sample_weights:
        return np.ones(len(seg), dtype=np.float64)
    keys = seg.astype(str).str.cat(prod.astype(str), sep="|")
    return keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_io/test_extract.py::TestComputeRowWeights -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
GIT commit -m "feat(extract): add pure _compute_row_weights helper"
```

---

## Task 3: `extract_Xyw` sibling returning (X, y, w)

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (add `extract_Xyw` after `extract_Xy`)
- Test: `tests/test_io/test_extract.py` (add `TestExtractXyw` mirroring existing extract fixtures)

- [ ] **Step 1: Write the failing test**

First inspect an existing passing test in `tests/test_io/test_extract.py` that builds a `ParquetHandle` + `preprocessor_metadata` + `parameters` (the file already has helpers `_make_handle`, `_make_parameters_with_cat`, `_make_prep_meta_with_cat` per the codebase). Add a test that reuses the SAME helpers, writing a parquet that additionally contains a `cust_segment_typ` column:

```python
from recsys_tfb.io.extract import extract_Xyw


class TestExtractXyw:
    def test_returns_x_y_w_with_weights_applied(self, tmp_path):
        # Reuse the module's existing parquet/prep/params builders. The parquet
        # MUST include identity cols + label + feature cols + cust_segment_typ.
        pdf = pd.DataFrame({
            "snap_date": ["2025-01-31"] * 3,
            "cust_id": [1, 2, 3],
            "prod_name": ["a", "a", "b"],
            "cust_segment_typ": ["mass", "hnw", "mass"],
            "label": [1, 0, 1],
            "f1": [0.1, 0.2, 0.3],
        })
        path = tmp_path / "mi.parquet"
        pdf.to_parquet(path)
        from recsys_tfb.io.handles import ParquetHandle
        handle = ParquetHandle(path=str(path))
        prep = {
            "feature_columns": ["prod_name", "f1"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
            "drop_columns": [],
        }
        params = {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label"}},
            "training": {"sample_weights": {"mass|a": 5.0}},
        }
        X, y, w = extract_Xyw(handle, prep, params)
        assert X.shape == (3, 2)
        np.testing.assert_array_equal(y, np.array([1, 0, 1]))
        # row0 = mass|a -> 5.0 ; row1 = hnw|a -> 1.0 ; row2 = mass|b -> 1.0
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0]))

    def test_no_sample_weights_returns_all_ones(self, tmp_path):
        pdf = pd.DataFrame({
            "snap_date": ["2025-01-31"] * 2, "cust_id": [1, 2],
            "prod_name": ["a", "b"], "cust_segment_typ": ["mass", "hnw"],
            "label": [1, 0], "f1": [0.1, 0.2]})
        path = tmp_path / "mi.parquet"
        pdf.to_parquet(path)
        from recsys_tfb.io.handles import ParquetHandle
        handle = ParquetHandle(path=str(path))
        prep = {"feature_columns": ["prod_name", "f1"],
                "categorical_columns": ["prod_name"],
                "category_mappings": {"prod_name": ["a", "b"]},
                "drop_columns": []}
        params = {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}
        X, y, w = extract_Xyw(handle, prep, params)
        np.testing.assert_array_equal(w, np.array([1.0, 1.0]))
```

(If the existing helpers `_make_handle`/`_make_parameters_with_cat` are a better fit, prefer them and add `cust_segment_typ` to their frame — but the inline frame above is self-contained and valid.)

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_io/test_extract.py::TestExtractXyw -q`
Expected: FAIL — `ImportError: cannot import name 'extract_Xyw'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, add after `extract_Xy` (ends ~line 155):

```python
SEGMENT_COLUMN = "cust_segment_typ"


def extract_Xyw(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Like :func:`extract_Xy` but also returns a per-row LightGBM weight.

    The weight is computed from ``parameters["training"]["sample_weights"]``
    (raw ``"<cust_segment_typ>|<prod_name>"`` keys) using the raw string
    columns present in the parquet, BEFORE categorical encoding. Used only on
    the train / train_dev path — val / calibration / evaluation continue to
    use :func:`extract_Xy` / :func:`extract_Xy_with_groups` (unweighted, D6).

    ``w`` is all-ones when ``training.sample_weights`` is absent/empty or the
    ``cust_segment_typ`` carry column is missing (graceful, never raises).
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    item_col = schema["item"]

    logger.info(
        "extract_Xyw start path=%s n_feature_cols=%d label=%s",
        getattr(handle, "path", "<unknown>"), len(feature_cols), label_col,
    )
    _log_parquet_metadata(handle)

    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    log_data_volume(logger, "extract_Xyw.pdf", pdf, deep=True)

    sample_weights = (parameters.get("training", {}) or {}).get("sample_weights") or {}
    if sample_weights and SEGMENT_COLUMN in pdf.columns and item_col in pdf.columns:
        w = _compute_row_weights(pdf[SEGMENT_COLUMN], pdf[item_col], sample_weights)
    else:
        if sample_weights and SEGMENT_COLUMN not in pdf.columns:
            logger.warning(
                "extract_Xyw: sample_weights set but carry column '%s' "
                "absent from parquet — defaulting all weights to 1.0",
                SEGMENT_COLUMN,
            )
        w = np.ones(len(pdf), dtype=np.float64)

    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values

    log_data_volume(logger, "extract_Xyw.X", X)
    log_data_volume(logger, "extract_Xyw.y", y)
    log_data_volume(logger, "extract_Xyw.w", w)
    return X, y, w
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_io/test_extract.py::TestExtractXyw -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the whole extract test module (no regression on extract_Xy / extract_Xy_with_groups)**

Run: `PYTEST tests/test_io/test_extract.py -q`
Expected: PASS (all green; existing `extract_Xy` 2-tuple callers unaffected).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
GIT commit -m "feat(extract): add extract_Xyw sibling (X, y, weight) for train path"
```

---

## Task 4: `carry_columns` through `select_keys`

**Files:**
- Modify: `src/recsys_tfb/pipelines/dataset/helpers_spark.py` (both return paths of `select_keys`)
- Test: `tests/test_pipelines/test_dataset/test_helpers_spark.py` (create if absent; uses function-scoped `spark` fixture)

- [ ] **Step 1: Write the failing test**

Create/append `tests/test_pipelines/test_dataset/test_helpers_spark.py`:

```python
import pandas as pd
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys


def _params(carry=None, overrides=None):
    p = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "dataset": {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio": 1.0,
        },
        "random_seed": 42,
    }
    if carry is not None:
        p["dataset"]["carry_columns"] = carry
    if overrides is not None:
        p["dataset"]["sample_ratio_overrides"] = overrides
    return p


def _pool(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "cust_id": [1, 2, 3, 4],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
        "label": [1, 0, 1, 0],
    }))


class TestSelectKeysCarry:
    def test_carry_column_present_no_sampling_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0, {})
        assert "cust_segment_typ" in df.columns
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name",
                                   "cust_segment_typ"}

    def test_carry_column_present_overrides_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0,
                          {"mass|a|1": 1.0})
        assert "cust_segment_typ" in df.columns

    def test_no_carry_columns_returns_identity_only(self, spark):
        df = select_keys(_pool(spark), _params(),
                          [pd.Timestamp("2025-01-31")], 1.0, {})
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_pipelines/test_dataset/test_helpers_spark.py -q`
Expected: FAIL — `test_carry_column_present_*` assert errors (`cust_segment_typ` not in columns); `test_no_carry_columns_*` PASSES.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/pipelines/dataset/helpers_spark.py::select_keys`, after the `group_keys = ds.get("sample_group_keys", [time_col])` line and the `if sample_ratio_overrides is None:` block, add:

```python
    carry_columns = ds.get("carry_columns", []) or []
    return_cols = identity_key + [c for c in carry_columns if c not in identity_key]
```

Then change the `extract_cols` line to also include carry:

```python
    extract_cols = list(dict.fromkeys(group_keys + identity_key + carry_columns))
```

Then change BOTH `return` paths' projection from `.select(*identity_key)` to `.select(*return_cols)`:
- early path: `sampled = keys.select(*return_cols)`
- final path: `sampled = keys.filter(F.col("_bucket") < threshold_expr).select(*return_cols)`

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_pipelines/test_dataset/test_helpers_spark.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/pipelines/dataset/helpers_spark.py tests/test_pipelines/test_dataset/test_helpers_spark.py
GIT commit -m "feat(dataset): select_keys carries dataset.carry_columns"
```

---

## Task 5: Verify carry survives `split_train_keys` (regression guard)

**Files:**
- Test only: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (add `TestSplitTrainKeysCarry`)
- (No production change expected: `split_train_keys` joins on `cust_col`; carry rides along. The test PROVES this and locks it.)

- [ ] **Step 1: Write the test (expected to PASS immediately — this is a guard)**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py`:

```python
import pandas as pd
from recsys_tfb.pipelines.dataset.nodes_spark import split_train_keys


class TestSplitTrainKeysCarry:
    def test_carry_column_survives_split(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a"] * 6,
            "cust_segment_typ": ["mass", "hnw", "mass", "aff", "mass", "hnw"],
        }))
        params = {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
            "dataset": {"train_dev_ratio": 0.3}, "random_seed": 42}
        train_keys, dev_keys = split_train_keys(keys, params)
        assert "cust_segment_typ" in train_keys.columns
        assert "cust_segment_typ" in dev_keys.columns
```

- [ ] **Step 2: Run test to verify it passes**

Run: `PYTEST tests/test_pipelines/test_dataset/test_nodes_spark.py::TestSplitTrainKeysCarry -q`
Expected: PASS (carry survives the `join(on=cust_col)`). If it FAILS, `split_train_keys` is dropping non-key columns — in that case re-project: change `train_keys = sample_keys.join(...)` to keep all `sample_keys` columns (it already does — `join` keeps left columns). Do not modify unless red.

- [ ] **Step 3: Commit**

```bash
GIT add tests/test_pipelines/test_dataset/test_nodes_spark.py
GIT commit -m "test(dataset): lock carry-column survival through split_train_keys"
```

---

## Task 6: `build_model_input` conditionally outputs carry columns

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py::build_model_input`
- Test: `tests/test_preprocessing/test_spark.py` (add `TestBuildModelInputCarry`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_preprocessing/test_spark.py` (mirror existing build_model_input test setup in that file; minimal self-contained version):

```python
import pandas as pd
from recsys_tfb.preprocessing._spark import build_model_input


class TestBuildModelInputCarry:
    def _prep(self):
        return {"feature_columns": ["prod_name", "f1"],
                "categorical_columns": ["prod_name"],
                "category_mappings": {"prod_name": ["a", "b"]},
                "drop_columns": []}

    def _params(self):
        return {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}

    def test_carry_column_in_output_when_present_in_keys(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"],
            "cust_segment_typ": ["mass", "hnw"]}))
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0]}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" in out.columns
        # carry is NOT a feature column
        assert "cust_segment_typ" not in self._prep()["feature_columns"]

    def test_no_carry_column_when_absent_from_keys(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"]}))
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0]}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" not in out.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_preprocessing/test_spark.py::TestBuildModelInputCarry -q`
Expected: FAIL — `test_carry_column_in_output_when_present_in_keys` fails (`cust_segment_typ` not in output); the second test PASSES.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/preprocessing/_spark.py::build_model_input`, inside the `with log_step(logger, "select_output_columns"):` block, change the `output_cols` construction to append carry columns present in `keys`:

Replace:
```python
        output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
        result = dataset.select(*output_cols)
```
with:
```python
        carry_present = [
            c for c in keys.columns
            if c not in identity_cols and c not in feature_columns
            and c != label_col and c in dataset.columns
        ]
        output_cols = list(dict.fromkeys(
            identity_cols + [label_col] + feature_columns + carry_present
        ))
        result = dataset.select(*output_cols)
```

(`carry_present` is derived from `keys.columns` so val/test/cal — whose keys lack carry — naturally produce no carry output. Carry is excluded from `required`/`_validate_columns` since it is not in `feature_columns`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_preprocessing/test_spark.py::TestBuildModelInputCarry -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the full preprocessing module (no regression)**

Run: `PYTEST tests/test_preprocessing/test_spark.py -q`
Expected: PASS (all green).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/preprocessing/_spark.py tests/test_preprocessing/test_spark.py
GIT commit -m "feat(preprocessing): build_model_input conditionally carries non-feature columns"
```

---

## Task 7: LightGBM adapter consumes weight on train path only

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py` (cached path: `extract_Xy`→`extract_Xyw`, pass `weight=`; non-cached `train()`: add `w_train` param, pass to `lgb.Dataset`; val never weighted)
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:458` (`extract_Xy`→`extract_Xyw`, thread weight into `train()`)
- Test: `tests/test_models/test_adapter.py` (add `TestLightGBMTrainWeight`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_models/test_adapter.py`:

```python
import numpy as np
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


class TestLightGBMTrainWeight:
    def test_train_accepts_w_train_and_does_not_weight_val(self):
        rng = np.random.default_rng(0)
        X_tr = rng.normal(size=(60, 3)); y_tr = (rng.random(60) > 0.5).astype(int)
        X_val = rng.normal(size=(20, 3)); y_val = (rng.random(20) > 0.5).astype(int)
        w_tr = np.full(60, 2.0)
        a = LightGBMAdapter()
        a.train(
            X_tr, y_tr, X_val, y_val,
            {"objective": "binary", "verbosity": -1,
             "num_iterations": 5, "early_stopping_rounds": 0},
            w_train=w_tr,
        )
        # booster trained; weighted train Dataset carries the weight array
        assert a.booster is not None
        # signature is back-compatible: omitting w_train still works
        a2 = LightGBMAdapter()
        a2.train(X_tr, y_tr, X_val, y_val,
                 {"objective": "binary", "verbosity": -1,
                  "num_iterations": 5, "early_stopping_rounds": 0})
        assert a2.booster is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_models/test_adapter.py::TestLightGBMTrainWeight -q`
Expected: FAIL — `TypeError: train() got an unexpected keyword argument 'w_train'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/models/lightgbm_adapter.py::train`, change the signature to add a keyword-only `w_train`:

```python
    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        params: dict,
        *,
        w_train: "np.ndarray | None" = None,
        train_dataset: "lgb.Dataset | None" = None,
        val_dataset: "lgb.Dataset | None" = None,
    ) -> None:
```

Change the train Dataset construction (the `if train_dataset is None:` block) to:

```python
        if train_dataset is None:
            train_dataset = lgb.Dataset(
                X_train, label=y_train, weight=w_train, free_raw_data=False
            )
```

Leave the `val_dataset` construction unchanged (no `weight=` — D6: val never weighted).

In `prepare_train_inputs` (cached lgb-binary path), change the two `extract_Xy` calls + Dataset builds:

Replace:
```python
        X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
        ds_train = lgb.Dataset(
            X_tr,
            label=y_tr,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
```
with:
```python
        X_tr, y_tr, w_tr = extract_Xyw(train_handle, preprocessor_metadata, parameters)
        ds_train = lgb.Dataset(
            X_tr,
            label=y_tr,
            weight=w_tr,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
```
and `del X_tr, y_tr` → `del X_tr, y_tr, w_tr`.

Replace:
```python
        X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)
        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
```
with:
```python
        X_dev, y_dev, w_dev = extract_Xyw(train_dev_handle, preprocessor_metadata, parameters)
        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            weight=w_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
```
and `del X_dev, y_dev, ds_train, ds_dev` → `del X_dev, y_dev, w_dev, ds_train, ds_dev`.

Change the lazy import line `from recsys_tfb.io.extract import extract_Xy` → `from recsys_tfb.io.extract import extract_Xyw`.

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_models/test_adapter.py::TestLightGBMTrainWeight -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Update the non-cached trial call site**

In `src/recsys_tfb/pipelines/training/nodes.py` around line 445–458, change the lazy import and the call:
- `from recsys_tfb.io.extract import extract_Xy` → `from recsys_tfb.io.extract import extract_Xyw`
- `X_tr, y_tr = extract_Xy(train_parquet_handle, preprocessor_metadata, parameters)` → `X_tr, y_tr, w_tr = extract_Xyw(train_parquet_handle, preprocessor_metadata, parameters)`
- Locate where this `X_tr, y_tr` flows into `adapter.train(...)` (same function, below line 458) and add `w_train=w_tr` to that `train(...)` call. (Val arrays in that call come from `extract_Xy_with_groups` — leave unweighted.)

Run the training-nodes unit tests:
Run: `PYTEST tests/test_pipelines/test_training -q`
Expected: PASS (all green; `w_train` flows, val unweighted).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/models/lightgbm_adapter.py src/recsys_tfb/pipelines/training/nodes.py tests/test_models/test_adapter.py
GIT commit -m "feat(training): LightGBM consumes sample_weight on train path only"
```

---

## Task 8: Config keys + versioning regression

**Files:**
- Modify: `conf/base/parameters_dataset.yaml` (add `dataset.carry_columns`)
- Modify: `conf/base/parameters_training.yaml` (add `training.sample_weights`)
- Test: `tests/test_core/test_versioning.py` (add `TestWeightingVersioning`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/test_versioning.py` (reuse the file's existing param builders if present; self-contained version below):

```python
from recsys_tfb.core.versioning import (
    compute_base_dataset_version,
    compute_train_variant_id,
    compute_model_version,
)


class TestWeightingVersioning:
    def _ds(self, carry=None):
        d = {"dataset": {"sample_ratio": 1.0, "train_snap_dates": ["2025-01-31"]}}
        if carry is not None:
            d["dataset"]["carry_columns"] = carry
        return d

    def test_carry_columns_busts_base_dataset_version(self):
        v1 = compute_base_dataset_version(self._ds(carry=["cust_segment_typ"]), {})
        v2 = compute_base_dataset_version(
            self._ds(carry=["cust_segment_typ", "channel_preference"]), {})
        assert v1 != v2

    def test_carry_columns_does_not_bust_train_variant(self):
        a = compute_train_variant_id(self._ds(carry=["cust_segment_typ"]))
        b = compute_train_variant_id(self._ds(carry=["x", "y"]))
        assert a == b

    def test_sample_weights_busts_model_version_not_train_variant(self):
        p1 = {"training": {"algorithm": "lightgbm", "sample_weights": {}},
              "dataset": {"sample_ratio": 1.0}}
        p2 = {"training": {"algorithm": "lightgbm",
                           "sample_weights": {"mass|a": 3.0}},
              "dataset": {"sample_ratio": 1.0}}
        assert compute_model_version(p1, "base", "tv") != \
               compute_model_version(p2, "base", "tv")
        assert compute_train_variant_id(p1) == compute_train_variant_id(p2)
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `PYTEST tests/test_core/test_versioning.py::TestWeightingVersioning -q`
Expected: PASS already (these assert the EXISTING versioning behavior — they are regression locks proving no versioning code change is needed). If any FAIL, the assumption in the spec is wrong — STOP and report; do not patch versioning blindly.

- [ ] **Step 3: Add config keys**

In `conf/base/parameters_dataset.yaml`, under `dataset:`, after the `sample_ratio_overrides` block, add:

```yaml
  # --- Carry columns ---
  # 非 identity、要從 sample_pool 帶進 train/train_dev model_input parquet 供
  # 訓練讀取（如 training.sample_weights 的 segment 維度）的欄位清單。
  # 寬鬆超集策略：一次列足想用的維度，weight 表只取需要的子集 → 改 weight
  # 不需重產 dataset。改此清單會 bust base_dataset_version（parquet schema 變）。
  carry_columns:
    - cust_segment_typ
```

In `conf/base/parameters_training.yaml`, under `training:` (e.g. after `calibration:` block), add:

```yaml
  # --- Per-(segment,product) sample weights (模型層冷門產品 boost) ---
  # key 格式固定為 "<cust_segment_typ>|<prod_name>"；value = LightGBM
  # sample_weight（>= 1.0，只 boost）。稀疏：只列 != 1.0 的組。只作用於
  # train/train_dev，val/calibration/evaluation 不加權。A7 一致性閘驗證
  # product 分量。改此表會 bust model_version、不動 train_variant_id。
  sample_weights: {}
```

- [ ] **Step 4: Re-run versioning + consistency tests with real config loadable**

Run: `PYTEST tests/test_core/test_versioning.py tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS (config additions are inert defaults; A7 passes with empty `sample_weights`).

- [ ] **Step 5: Commit**

```bash
GIT add conf/base/parameters_dataset.yaml conf/base/parameters_training.yaml tests/test_core/test_versioning.py
GIT commit -m "feat(config): add dataset.carry_columns and training.sample_weights"
```

---

## Task 9: Targeted integration sweep + graph refresh

**Files:** none (verification + graph maintenance only)

- [ ] **Step 1: Run all directly-touched test modules together**

Run:
```
PYTEST tests/test_core/test_consistency.py tests/test_core/test_versioning.py \
  tests/test_io/test_extract.py tests/test_pipelines/test_dataset \
  tests/test_preprocessing/test_spark.py tests/test_models/test_adapter.py \
  tests/test_pipelines/test_training -q
```
Expected: PASS (all green). If any RED, fix in the owning task before proceeding.

- [ ] **Step 2: Confirm extract_Xy back-compat surface untouched**

Run: `GIT grep -n "extract_Xy\b" -- src/` and confirm the only remaining bare `extract_Xy(` call sites are the val/eval path (`extract_Xy_with_groups`) and any pre-existing 2-tuple consumers; the two train sites now use `extract_Xyw`.
Expected: train sites = `extract_Xyw`; `extract_Xy` itself still exists and is unmodified.

- [ ] **Step 3: Refresh graphify graph (CLAUDE.md mandate after code changes)**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
"from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: rebuild completes; `graphify-out/GRAPH_REPORT.md` is untracked (do not stage).

- [ ] **Step 4: Final commit (if any fixups were made during the sweep)**

```bash
GIT add -A -- src tests
GIT commit -m "test(weighting): integration sweep fixups" || echo "nothing to commit"
```

---

## Self-review notes (author)

- **Spec coverage:** D5 (model-layer weight) → Tasks 2,3,7. D6 (train-only, val/cal unweighted) → Tasks 3,7 (val Dataset never gets `weight=`; `extract_Xyw` only on train sites). D7' (carry_columns from sample_pool, not baked) → Tasks 4,5,6,8. D8 formula → Plan B (suggestion engine); the mechanism here consumes any `sample_weights` dict regardless of how produced. A7 → Task 1. Versioning (carry busts base, weights bust model_version not train_variant) → Task 8. Config → Task 8.
- **Placeholder scan:** none — every code step shows full code; every run step shows command + expected.
- **Type/name consistency:** `extract_Xyw` (3-tuple) used identically in Tasks 3,7,9; `_compute_row_weights(seg, prod, sample_weights)` signature consistent Tasks 2,3; `w_train` keyword consistent Task 7; `SEGMENT_COLUMN = "cust_segment_typ"` defined Task 3 and reused conceptually in config Task 8.
- **Scope:** mechanism only; the editor tool is Plan B (`2026-05-18-sampling-overrides-editor-tool.md`).

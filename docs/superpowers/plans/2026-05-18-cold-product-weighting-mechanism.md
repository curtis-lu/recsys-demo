# Cold-Product Model-Layer Weighting Mechanism — Implementation Plan (Plan A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a model-layer per-(segment,product) `sample_weight` to LightGBM training so cold products get a boosted gradient without row replication, plus the `dataset.carry_columns` infrastructure that makes `cust_segment_typ` available at training time without baking weights into the cached dataset.

**Architecture:** `select_keys` carries a configurable column set from `sample_pool` into the train/train_dev `model_input` parquet. `extract_Xy` / `extract_Xy_with_groups` gain an opt-in `with_weights` that returns a per-row weight array (raw `cust_segment_typ|prod_name` keys → `training.sample_weights`) aligned to the rows they emit. LightGBM consumes it via `lgb.Dataset(weight=...)` at the four train-Dataset construction points (binary + ranking, in `prepare_train_inputs` and refit), with the same `perm` the ranking path applies to X/y. A new consistency invariant **A8** validates the weight-table keys at CLI entry. Source of truth: `docs/superpowers/specs/2026-05-18-sampling-overrides-editor-design.md` (D5–D10).

**Tech Stack:** Python 3.10, PySpark 3.3.2, LightGBM 4.6.0, numpy, pandas, pytest 7.3.1.

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor` (branch `feat/sampling-overrides-editor`, rebased onto `origin/main` = `33eb37d`, which includes PR#22 `feat/configurable-hpo-search-space`).

**Test command convention (CLAUDE.md worktree SOP — always this exact form):**
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```
Abbreviated below as `PYTEST <paths>`. Git abbreviated as `GIT ...` = `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor ...`. The graphify post-commit hook regenerates untracked `graphify-out/GRAPH_REPORT.md` — do not stage it.

---

## Key reference facts (verified from `33eb37d`, do not re-derive)

- **PR#22 reconciliation:** invariant **A7** is taken (`ranking_objective_conflicts`, `consistency.py:183`, legend bullet `consistency.py:38`, wired `consistency.py:264-265` just before the `raise` at `:268`). The new invariant here is **A8**.
- `prepare_train_inputs` (`models/lightgbm_adapter.py:122`) forks on objective family: **ranking** branch (`:196-238`) uses `extract_Xy_with_groups` then `perm,grp = to_contiguous_groups(gid)` and `lgb.Dataset(X[perm], label=y[perm], group=grp, ...)` — **rows are permuted by `perm`**; **binary** branch (`:239-270`) uses `extract_Xy` + `lgb.Dataset(X, label=y, ...)`.
- Refit (`pipelines/training/nodes.py`) also forks: ranking `:483-515` (`extract_Xy_with_groups` ×2 → concat → `perm,grp` → `lgb.Dataset(X_full[perm], ..., group=grp)`), binary `:516-541` (`extract_Xy` ×2 → concat → `lgb.Dataset(X_full, ...)`).
- Train path does NOT use `filter_groups_with_positives` (only the val/HPO path at `nodes.py:298-302` does — that path and calibration `nodes.py:583` must stay **unweighted**, D6).
- `adapter.train(...)` at refit (`nodes.py:554`) receives a prebuilt `train_dataset=ds_full`; trials use the cached lgb binaries. ⇒ **weight enters only at `lgb.Dataset` construction**; `train()` signature is NOT modified.
- `group_utils`: `RANKING_OBJECTIVES={"lambdarank","rank_xendcg"}`, `is_ranking_objective`, `objective_family→"binary"/"ranking"`, `to_contiguous_groups(gid)->(perm,grp)`.
- PR#22 did **not** touch `helpers_spark.py`, `pipelines/dataset/nodes_spark.py`, `preprocessing/_spark.py`, `core/versioning.py` (Tasks 4,5,6,8 unaffected).
- Test conventions: consistency — `_base(over=None)` (`tests/test_core/test_consistency.py:86`), class-based, group-local imports; `TestRankingObjectiveConflicts` (`:228`) is the A7 mirror to copy. Adapter — `_ranking_parameters(objective)` (`tests/test_models/test_adapter.py:467`), `test_prepare_train_inputs_binary_family_subpath` (`:496`), `..._ranking_sets_group` (`:520`) are the patterns to mirror. Spark tests use the function-scoped `spark` fixture from `tests/conftest.py`.
- Weight-table key format (fixed, documented): `"<cust_segment_typ>|<prod_name>"` (2 parts, `|`-joined); product = `parts[1]`.

---

## Task 1: A8 consistency predicate `weight_unknown_items`

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/test_consistency.py` (end of file; mirrors the `TestRankingObjectiveConflicts` group style):

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

    def test_malformed_key_without_pipe_ignored(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"massa": 2.0}}})
        assert weight_unknown_items(p) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_core/test_consistency.py::TestWeightUnknownItems -q`
Expected: FAIL — `ImportError: cannot import name 'weight_unknown_items'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/core/consistency.py`, add the predicate immediately AFTER `ranking_objective_conflicts` (ends ~`:221`, before `def validate_config_consistency` at `:223`):

```python
def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A8).

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

In `validate_config_consistency`, add AFTER the ranking A7 block (`for msg in ranking_objective_conflicts(parameters): errors.append(msg)` at `:264-265`) and BEFORE `if errors:` (`:267`):

```python
    unknown_w = weight_unknown_items(parameters)
    if unknown_w:
        errors.append(
            f"training.sample_weights references product value(s) {unknown_w} "
            f"absent from schema.categorical_values[item] — the weight "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )
```

In the module-docstring Invariant legend, add AFTER the A7 bullet (the `* A7 — a ranking ...``ranking_objective_conflicts``.` block ending ~`:41`):

```
* A8 — a ``training.sample_weights`` key references a product value absent
  from ``schema.categorical_values[item]``. Predicate:
  ``weight_unknown_items`` (product-only check, mirrors A5).
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_core/test_consistency.py::TestWeightUnknownItems -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Add the validate-wiring test, run**

Append to `class TestValidateConfigConsistency` (`tests/test_core/test_consistency.py:149`):

```python
    def test_a8_unknown_weight_product_collected(self):
        p = _base({"inference": {"products": ["a", "b"]},
            "dataset": {"prepare_model_input": {
                "categorical_columns": ["prod_name"]}},
            "training": {"sample_weights": {"mass|zzz": 2.0}}})
        with pytest.raises(ConfigConsistencyError, match=r"training\.sample_weights"):
            validate_config_consistency(p)
```

Run: `PYTEST tests/test_core/test_consistency.py -q`
Expected: PASS (all consistency tests green, including PR#22's `TestRankingObjectiveConflicts`).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
GIT commit -m "feat(consistency): add A8 weight_unknown_items invariant"
```

---

## Task 2: Pure per-row weight function `_compute_row_weights`

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (private pure helper after imports)
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
        w = _compute_row_weights(seg, prod, {"mass|a": 3.0, "hnw|a": 2.0})
        assert isinstance(w, np.ndarray)
        np.testing.assert_array_equal(w, np.array([3.0, 2.0, 1.0, 1.0]))

    def test_empty_weights_all_ones(self):
        w = _compute_row_weights(pd.Series(["m", "h"]), pd.Series(["a", "b"]), {})
        np.testing.assert_array_equal(w, np.array([1.0, 1.0]))

    def test_dtype_is_float64(self):
        w = _compute_row_weights(pd.Series(["m"]), pd.Series(["a"]), {"m|a": 2.0})
        assert w.dtype == np.float64
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_io/test_extract.py::TestComputeRowWeights -q`
Expected: FAIL — `ImportError: cannot import name '_compute_row_weights'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, after `logger = logging.getLogger(__name__)`:

```python
SEGMENT_COLUMN = "cust_segment_typ"


def _compute_row_weights(
    seg: "pd.Series",
    prod: "pd.Series",
    sample_weights: dict,
) -> np.ndarray:
    """Per-row LightGBM sample weight from a ``"<segment>|<product>"`` table.

    Pure: no Spark, no I/O. Rows whose ``f"{seg}|{prod}"`` key is absent get
    weight 1.0 (sparse-emit semantics: only boosted groups are written to
    ``training.sample_weights``).
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
GIT commit -m "feat(extract): pure _compute_row_weights helper"
```

---

## Task 3: `with_weights` opt-in on `extract_Xy` and `extract_Xy_with_groups`

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (add `_row_weights_from_pdf`; add `with_weights` kw to both extract functions)
- Test: `tests/test_io/test_extract.py` (add `TestExtractWithWeights`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_io/test_extract.py`:

```python
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.io.extract import extract_Xy, extract_Xy_with_groups


def _wparams(weights):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"sample_weights": weights},
    }


def _wprep():
    return {"feature_columns": ["prod_name", "f1"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
            "drop_columns": []}


def _wparquet(tmp_path):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "cust_id": [1, 1, 2, 2],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "mass", "hnw", "hnw"],
        "label": [1, 0, 1, 0],
        "f1": [0.1, 0.2, 0.3, 0.4]})
    p = tmp_path / "mi.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


class TestExtractWithWeights:
    def test_extract_Xy_default_is_two_tuple(self, tmp_path):
        out = extract_Xy(_wparquet(tmp_path), _wprep(), _wparams({}))
        assert len(out) == 2  # back-compat: existing callers unaffected

    def test_extract_Xy_with_weights_appends_aligned_w(self, tmp_path):
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({"mass|a": 5.0}), with_weights=True)
        assert X.shape == (4, 2)
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0, 1.0]))

    def test_extract_Xy_with_weights_no_table_all_ones(self, tmp_path):
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({}), with_weights=True)
        np.testing.assert_array_equal(w, np.ones(4))

    def test_extract_Xy_with_groups_default_is_three_tuple(self, tmp_path):
        out = extract_Xy_with_groups(_wparquet(tmp_path), _wprep(), _wparams({}))
        assert len(out) == 3  # back-compat

    def test_extract_Xy_with_groups_with_weights_appends_w(self, tmp_path):
        X, y, g, w = extract_Xy_with_groups(
            _wparquet(tmp_path), _wprep(), _wparams({"hnw|a": 4.0}),
            with_weights=True)
        assert len(g) == 4
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([1.0, 1.0, 4.0, 1.0]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_io/test_extract.py::TestExtractWithWeights -q`
Expected: FAIL — `test_extract_Xy_with_weights_*` fail with `TypeError: extract_Xy() got an unexpected keyword argument 'with_weights'`; the two `*_default_*` tests PASS.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, add this helper after `_compute_row_weights` (Task 2):

```python
def _row_weights_from_pdf(pdf: "pd.DataFrame", parameters: dict) -> np.ndarray:
    """Resolve a per-row weight array from training.sample_weights.

    All-ones when the table is absent/empty or the carry / item columns are
    not present (graceful, never raises). Computed from the *given* pdf so
    it is aligned to whatever filtering/ordering the caller has already done.
    """
    sw = (parameters.get("training", {}) or {}).get("sample_weights") or {}
    item_col = get_schema(parameters)["item"]
    if not sw or SEGMENT_COLUMN not in pdf.columns or item_col not in pdf.columns:
        return np.ones(len(pdf), dtype=np.float64)
    return _compute_row_weights(pdf[SEGMENT_COLUMN], pdf[item_col], sw)
```

Change `extract_Xy`'s signature to keyword-only `with_weights`:

```python
def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    with_weights: bool = False,
) -> tuple:
```

At the end of `extract_Xy`, replace `return X, y` with:

```python
    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy.w", w)
        return X, y, w
    return X, y
```

Change `extract_Xy_with_groups`'s signature to add the keyword (keep the existing `filter_groups_with_positives` keyword):

```python
def extract_Xy_with_groups(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    filter_groups_with_positives: bool = False,
    with_weights: bool = False,
) -> tuple:
```

In `extract_Xy_with_groups`, the final `with log_step(logger, "to_numpy"):` block computes `X`, `y`, `groups` from the (possibly already-filtered) `pdf`. Replace the trailing `return X, y, groups` with:

```python
    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy_with_groups.w", w)
        return X, y, groups, w
    return X, y, groups
```

(`pdf` here is the post-`filter_groups_with_positives` frame, so `w` is aligned 1:1 with the returned `X/y/groups` regardless of flag combination.)

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_io/test_extract.py::TestExtractWithWeights -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Full extract module regression**

Run: `PYTEST tests/test_io/test_extract.py -q`
Expected: PASS (all green — every existing 2-/3-tuple caller unaffected since `with_weights` defaults False).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
GIT commit -m "feat(extract): opt-in with_weights on extract_Xy / extract_Xy_with_groups"
```

---

## Task 4: `carry_columns` through `select_keys`

**Files:**
- Modify: `src/recsys_tfb/pipelines/dataset/helpers_spark.py` (both return paths of `select_keys`)
- Test: `tests/test_pipelines/test_dataset/test_helpers_spark.py` (create)

(PR#22 did not touch `helpers_spark.py` — line refs from the original audit hold.)

- [ ] **Step 1: Write the failing test**

Create `tests/test_pipelines/test_dataset/test_helpers_spark.py`:

```python
import pandas as pd
from recsys_tfb.pipelines.dataset.helpers_spark import select_keys


def _params(carry=None):
    p = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "dataset": {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio": 1.0},
        "random_seed": 42}
    if carry is not None:
        p["dataset"]["carry_columns"] = carry
    return p


def _pool(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "cust_id": [1, 2, 3, 4],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
        "label": [1, 0, 1, 0]}))


class TestSelectKeysCarry:
    def test_carry_present_no_sampling_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0, {})
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name",
                                   "cust_segment_typ"}

    def test_carry_present_overrides_path(self, spark):
        df = select_keys(_pool(spark), _params(carry=["cust_segment_typ"]),
                          [pd.Timestamp("2025-01-31")], 1.0, {"mass|a|1": 1.0})
        assert "cust_segment_typ" in df.columns

    def test_no_carry_returns_identity_only(self, spark):
        df = select_keys(_pool(spark), _params(),
                         [pd.Timestamp("2025-01-31")], 1.0, {})
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_pipelines/test_dataset/test_helpers_spark.py -q`
Expected: FAIL — the two `carry_present` tests assert `cust_segment_typ` missing; `no_carry` PASSES.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/pipelines/dataset/helpers_spark.py::select_keys`, after the `if sample_ratio_overrides is None:` resolution block, add:

```python
    carry_columns = ds.get("carry_columns", []) or []
    return_cols = identity_key + [c for c in carry_columns if c not in identity_key]
```

Change the `extract_cols` line to include carry:

```python
    extract_cols = list(dict.fromkeys(group_keys + identity_key + carry_columns))
```

Change BOTH return projections from `.select(*identity_key)` to `.select(*return_cols)`:
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

- [ ] **Step 1: Write the guard test (expected PASS — `join(on=cust_col)` keeps left columns)**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py`:

```python
import pandas as pd
from recsys_tfb.pipelines.dataset.nodes_spark import split_train_keys


class TestSplitTrainKeysCarry:
    def test_carry_column_survives_split(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6], "prod_name": ["a"] * 6,
            "cust_segment_typ": ["mass", "hnw", "mass", "aff", "mass", "hnw"]}))
        params = {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
            "dataset": {"train_dev_ratio": 0.3}, "random_seed": 42}
        tr, dv = split_train_keys(keys, params)
        assert "cust_segment_typ" in tr.columns
        assert "cust_segment_typ" in dv.columns
```

- [ ] **Step 2: Run test to verify it passes**

Run: `PYTEST tests/test_pipelines/test_dataset/test_nodes_spark.py::TestSplitTrainKeysCarry -q`
Expected: PASS. If RED, `split_train_keys` is projecting away non-key columns — only then re-project to keep all `sample_keys` columns; otherwise change nothing.

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

(PR#22 did not touch `preprocessing/_spark.py`.)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_preprocessing/test_spark.py`:

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

    def _frames(self, spark, with_carry):
        kcols = {"snap_date": pd.to_datetime(["2025-01-31"] * 2),
                 "cust_id": [1, 2], "prod_name": ["a", "b"]}
        if with_carry:
            kcols["cust_segment_typ"] = ["mass", "hnw"]
        keys = spark.createDataFrame(pd.DataFrame(kcols))
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0]}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        return keys, feats, labels

    def test_carry_in_output_when_present_in_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=True)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" in out.columns

    def test_no_carry_when_absent_from_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=False)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" not in out.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_preprocessing/test_spark.py::TestBuildModelInputCarry -q`
Expected: FAIL — `test_carry_in_output_when_present_in_keys` fails; the other PASSES.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/preprocessing/_spark.py::build_model_input`, inside `with log_step(logger, "select_output_columns"):`, replace:

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

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_preprocessing/test_spark.py::TestBuildModelInputCarry -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Full preprocessing regression**

Run: `PYTEST tests/test_preprocessing/test_spark.py -q`
Expected: PASS (all green).

- [ ] **Step 6: Commit**

```bash
GIT add src/recsys_tfb/preprocessing/_spark.py tests/test_preprocessing/test_spark.py
GIT commit -m "feat(preprocessing): build_model_input conditionally carries non-feature columns"
```

---

## Task 7: Inject weight at the four train `lgb.Dataset` construction points

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py::prepare_train_inputs` (ranking + binary branches)
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (refit ranking + binary branches)
- Test: `tests/test_models/test_adapter.py` (add `TestPrepareTrainInputsWeight`)

**Design:** weight enters ONLY via `lgb.Dataset(weight=...)`. `train()` is NOT modified (refit/trial pass a prebuilt `train_dataset`). val/HPO and calibration extracts are left without `with_weights` (D6).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_models/test_adapter.py` (mirrors `test_prepare_train_inputs_binary_family_subpath` / `..._ranking_sets_group`):

```python
import lightgbm as lgb
import numpy as np
import pandas as pd
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


def _weight_parquet(tmp_path, name):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 8,
        "cust_id": [1, 1, 2, 2, 3, 3, 4, 4],
        "prod_name": ["a", "b", "a", "b", "a", "b", "a", "b"],
        "cust_segment_typ": ["mass"] * 8,
        "label": [1, 0, 1, 0, 1, 0, 1, 0],
        "f1": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]})
    p = tmp_path / name
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


def _weight_prep():
    return {"feature_columns": ["prod_name", "f1"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
            "drop_columns": []}


def _weight_params(objective):
    return {"training": {
        "algorithm_params": {"objective": objective},
        "sample_weights": {"mass|a": 3.0}}}


class TestPrepareTrainInputsWeight:
    def test_binary_branch_bakes_weight_into_binary(self, tmp_path):
        cache = tmp_path / "c"
        tr = _weight_parquet(tmp_path, "tr.parquet")
        dv = _weight_parquet(tmp_path, "dv.parquet")
        LightGBMAdapter().prepare_train_inputs(
            tr, dv, _weight_prep(), _weight_params("binary"), str(cache))
        ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
        w = ds.get_weight()
        # prod_name "a" rows -> 3.0 ; "b" rows -> 1.0
        assert w is not None
        assert sorted(set(np.round(w, 3))) == [1.0, 3.0]

    def test_ranking_branch_bakes_weight_aligned_with_perm(self, tmp_path):
        cache = tmp_path / "c"
        tr = _weight_parquet(tmp_path, "tr.parquet")
        dv = _weight_parquet(tmp_path, "dv.parquet")
        LightGBMAdapter().prepare_train_inputs(
            tr, dv, _weight_prep(), _weight_params("lambdarank"), str(cache))
        ds = lgb.Dataset(str(cache / "lgb" / "ranking" / "train.bin")).construct()
        w = ds.get_weight()
        assert w is not None
        assert sorted(set(np.round(w, 3))) == [1.0, 3.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_models/test_adapter.py::TestPrepareTrainInputsWeight -q`
Expected: FAIL — `ds.get_weight()` is `None` (weight not yet threaded), assertions fail.

- [ ] **Step 3: Implement — `prepare_train_inputs` ranking branch**

In `src/recsys_tfb/models/lightgbm_adapter.py`, ranking branch (`:205-238`):

Replace the train build:
```python
            X_tr, y_tr, gid_tr = extract_Xy_with_groups(
                train_handle, preprocessor_metadata, parameters
            )
            perm_tr, grp_tr = to_contiguous_groups(gid_tr)
            ds_train = lgb.Dataset(
                X_tr[perm_tr],
                label=y_tr[perm_tr],
                group=grp_tr,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
```
with:
```python
            X_tr, y_tr, gid_tr, w_tr = extract_Xy_with_groups(
                train_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            perm_tr, grp_tr = to_contiguous_groups(gid_tr)
            ds_train = lgb.Dataset(
                X_tr[perm_tr],
                label=y_tr[perm_tr],
                weight=w_tr[perm_tr],
                group=grp_tr,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
```
Change `del X_tr, y_tr, gid_tr, perm_tr` → `del X_tr, y_tr, gid_tr, perm_tr, w_tr`.

Replace the dev build:
```python
            X_dev, y_dev, gid_dev = extract_Xy_with_groups(
                train_dev_handle, preprocessor_metadata, parameters
            )
            perm_dev, grp_dev = to_contiguous_groups(gid_dev)
            ds_dev = lgb.Dataset(
                X_dev[perm_dev],
                label=y_dev[perm_dev],
                group=grp_dev,
                reference=ds_train,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
```
with:
```python
            X_dev, y_dev, gid_dev, w_dev = extract_Xy_with_groups(
                train_dev_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            perm_dev, grp_dev = to_contiguous_groups(gid_dev)
            ds_dev = lgb.Dataset(
                X_dev[perm_dev],
                label=y_dev[perm_dev],
                weight=w_dev[perm_dev],
                group=grp_dev,
                reference=ds_train,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
```
Change `del X_dev, y_dev, gid_dev, perm_dev, ds_train, ds_dev` → add `w_dev`.

- [ ] **Step 4: Implement — `prepare_train_inputs` binary branch**

Binary branch (`:245-270`). Replace train:
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
            X_tr, y_tr, w_tr = extract_Xy(
                train_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
            ds_train = lgb.Dataset(
                X_tr,
                label=y_tr,
                weight=w_tr,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
```
`del X_tr, y_tr` → `del X_tr, y_tr, w_tr`.

Replace dev:
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
            X_dev, y_dev, w_dev = extract_Xy(
                train_dev_handle, preprocessor_metadata, parameters,
                with_weights=True,
            )
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
`del X_dev, y_dev, ds_train, ds_dev` → add `w_dev`.

- [ ] **Step 5: Run the adapter weight test to verify it passes**

Run: `PYTEST tests/test_models/test_adapter.py::TestPrepareTrainInputsWeight -q`
Expected: PASS (2 passed).

- [ ] **Step 6: Implement — refit ranking + binary in `pipelines/training/nodes.py`**

Ranking refit (`:483-515`). Change the two extracts to `with_weights=True`, add `w_full`, weight the Dataset:
- `X_tr, y_tr, gid_tr = extract_Xy_with_groups(train_parquet_handle, preprocessor_metadata, parameters)` → `X_tr, y_tr, gid_tr, w_tr = extract_Xy_with_groups(train_parquet_handle, preprocessor_metadata, parameters, with_weights=True)`
- `X_dv, y_dv, gid_dv = extract_Xy_with_groups(train_dev_parquet_handle, preprocessor_metadata, parameters)` → add `, w_dv` and `with_weights=True`
- after `y_full = np.concatenate([y_tr, y_dv])` add `w_full = np.concatenate([w_tr, w_dv])`
- `del X_tr, y_tr, X_dv, y_dv, gid_tr, gid_dv` → add `w_tr, w_dv`
- in `ds_full = lgb.Dataset(X_full[perm], label=y_full[perm], group=grp, ...)` add `weight=w_full[perm],`

Binary refit (`:516-541`):
- `X_tr, y_tr = extract_Xy(train_parquet_handle, preprocessor_metadata, parameters)` → `X_tr, y_tr, w_tr = extract_Xy(train_parquet_handle, preprocessor_metadata, parameters, with_weights=True)`
- `X_dv, y_dv = extract_Xy(train_dev_parquet_handle, preprocessor_metadata, parameters)` → add `, w_dv` + `with_weights=True`
- after `y_full = np.concatenate([y_tr, y_dv])` add `w_full = np.concatenate([w_tr, w_dv])`
- `del X_tr, y_tr, X_dv, y_dv` → add `w_tr, w_dv`
- in `ds_full = lgb.Dataset(X_full, label=y_full, ...)` add `weight=w_full,`

Leave UNCHANGED (D6, no `with_weights`): the val/HPO `extract_Xy_with_groups(..., filter_groups_with_positives=True)` (`:298-302`) and `calibrate_model`'s `extract_Xy` (`:583`).

- [ ] **Step 7: Run training-pipeline tests**

Run: `PYTEST tests/test_pipelines/test_training tests/test_models/test_adapter.py -q`
Expected: PASS (all green — refit weighted; PR#22 ranking/group tests still pass; val/cal unweighted).

- [ ] **Step 8: Commit**

```bash
GIT add src/recsys_tfb/models/lightgbm_adapter.py src/recsys_tfb/pipelines/training/nodes.py tests/test_models/test_adapter.py
GIT commit -m "feat(training): inject sample_weight at train lgb.Dataset points (binary+ranking)"
```

---

## Task 8: Config keys + versioning regression

**Files:**
- Modify: `conf/base/parameters_dataset.yaml` (add `dataset.carry_columns`)
- Modify: `conf/base/parameters_training.yaml` (add `training.sample_weights`)
- Test: `tests/test_core/test_versioning.py` (add `TestWeightingVersioning`)

(PR#22 did not touch `core/versioning.py`; the over-include / `ALL_SAMPLING_KEYS` behavior is unchanged.)

- [ ] **Step 1: Write the regression-lock test**

Append to `tests/test_core/test_versioning.py`:

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
        assert compute_train_variant_id(self._ds(carry=["cust_segment_typ"])) == \
               compute_train_variant_id(self._ds(carry=["x", "y"]))

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

- [ ] **Step 2: Run test (expected PASS — locks existing versioning behavior)**

Run: `PYTEST tests/test_core/test_versioning.py::TestWeightingVersioning -q`
Expected: PASS. If any FAIL, the spec's "versioning needs no code change" assumption is wrong — STOP and report; do not patch versioning.

- [ ] **Step 3: Add config keys**

In `conf/base/parameters_dataset.yaml`, under `dataset:`, after the `sample_ratio_overrides` block:

```yaml
  # --- Carry columns ---
  # 非 identity、要從 sample_pool 帶進 train/train_dev model_input parquet 供
  # 訓練讀取（如 training.sample_weights 的 segment 維度）的欄位清單。寬鬆超
  # 集策略：一次列足想用的維度，weight 表只取需要的子集 → 改 weight 不需重產
  # dataset。改此清單會 bust base_dataset_version（parquet schema 變）。
  carry_columns:
    - cust_segment_typ
```

In `conf/base/parameters_training.yaml`, under `training:` (sibling of `algorithm_params:` / `calibration:`):

```yaml
  # --- Per-(segment,product) sample weights (模型層冷門產品 boost) ---
  # key 固定 "<cust_segment_typ>|<prod_name>"；value = LightGBM sample_weight
  # (>= 1.0，只 boost)。稀疏：只列 != 1.0 的組。只作用於 train/train_dev，
  # val/calibration/evaluation 不加權。A8 一致性閘驗證 product 分量。改此表
  # 會 bust model_version、不動 train_variant_id。
  sample_weights: {}
```

- [ ] **Step 4: Consistency + versioning regression with new config inert defaults**

Run: `PYTEST tests/test_core/test_versioning.py tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS (empty `sample_weights` → A8 passes; defaults inert).

- [ ] **Step 5: Commit**

```bash
GIT add conf/base/parameters_dataset.yaml conf/base/parameters_training.yaml tests/test_core/test_versioning.py
GIT commit -m "feat(config): add dataset.carry_columns and training.sample_weights"
```

---

## Task 9: Targeted integration sweep + graph refresh

**Files:** none (verification + graph maintenance)

- [ ] **Step 1: Run all directly-touched test modules together**

Run:
```
PYTEST tests/test_core/test_consistency.py tests/test_core/test_versioning.py \
  tests/test_io/test_extract.py tests/test_pipelines/test_dataset \
  tests/test_preprocessing/test_spark.py tests/test_models/test_adapter.py \
  tests/test_pipelines/test_training -q
```
Expected: PASS (all green). RED → fix in the owning task.

- [ ] **Step 2: Confirm D6 (val/calibration never weighted)**

Run: `GIT grep -n "with_weights" -- src/`
Expected: `with_weights=True` appears ONLY in `prepare_train_inputs` (both branches) and the two refit branches in `nodes.py`. It must NOT appear on the `filter_groups_with_positives=True` call (`nodes.py:~298`) nor in `calibrate_model` (`nodes.py:~583`). Eyeball those two sites are unchanged.

- [ ] **Step 3: Refresh graphify graph (CLAUDE.md mandate after code changes)**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
"from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: rebuild completes; `graphify-out/GRAPH_REPORT.md` untracked (do not stage).

- [ ] **Step 4: Final fixup commit (if any)**

```bash
GIT add -A -- src tests && GIT commit -m "test(weighting): integration sweep fixups" || echo "nothing to commit"
```

---

## Self-review notes (author)

- **Spec coverage:** D5 → T2,T3,T7. D6 (val/cal unweighted) → T3 (opt-in default False), T7 (only the 4 train Datasets get `with_weights=True`/`weight=`; val & calibrate untouched), T9 S2 verifies. D7' (carry from sample_pool, not baked) → T4,T5,T6,T8. D8 formula → consumed here, produced in Plan B. D10 (A7→A8; with_weights opt-in composing with ranking `perm`; `train()` unchanged) → T1,T3,T7. Versioning regression → T8.
- **Placeholder scan:** none — every code step shows full code/exact edits; every run step shows command + expected.
- **Type/name consistency:** `_compute_row_weights(seg, prod, sample_weights)` (T2) wrapped by `_row_weights_from_pdf(pdf, parameters)` (T3) used by both extract fns; `with_weights` keyword identical T3/T7/T9; `extract_Xy → (X,y[,w])`, `extract_Xy_with_groups → (X,y,groups[,w])` arity consistent T3/T7; weight applied with the SAME `perm` as X/y in every ranking site (T7 ranking branch + refit); `A8`/`weight_unknown_items` consistent T1/T8 and Plan B Task 3.
- **PR#22 reconciliation:** all line refs re-grounded to `33eb37d`; A7 is PR#22's, this plan uses A8; `train()` signature deliberately NOT changed (refit/trial use prebuilt `train_dataset`).
- **Scope:** mechanism only; the editor tool is Plan B.

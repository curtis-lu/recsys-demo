# Generalize `sample_weights` to Composite-Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `training.sample_weights` be keyed by an arbitrary `training.sample_weight_keys` column list (default `[prod_name]`), mirroring the dataset sampler's `sample_group_keys` / `sample_ratio_overrides`, with a config-static gate that catches the cross-file data dependency on `dataset.carry_columns`.

**Architecture:** Three focused changes. (1) `io/extract.py` builds the per-row weight lookup key by `|`-joining `sample_weight_keys` column values (single-col fast path + `str.cat` for multi-col), replacing the hardcoded `cust_segment_typ|prod_name`. (2) `core/consistency.py` adds A9a (key columns must be in `identity ∪ {label} ∪ carry_columns`), A9b (key arity matches `len(sample_weight_keys)`), and generalizes A9c (product component validity). (3) `conf/base/parameters_training.yaml` declares `sample_weight_keys` and rewrites comments. Runtime default and consistency default both resolve to `[schema.item]` to avoid drift.

**Tech Stack:** Python 3.10, pandas 1.5.3, numpy 1.25.0, pytest 7.3.1.

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights`
**Run tests with:** `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q` (run from worktree root).

---

## File Structure

- Modify: `src/recsys_tfb/io/extract.py` — generalize `_compute_row_weights` + `_row_weights_from_pdf`; drop `SEGMENT_COLUMN`.
- Modify: `tests/test_io/test_extract.py` — update `TestComputeRowWeights` (signature change) + `_wparams`; add multi-col/default/down-weight cases.
- Modify: `src/recsys_tfb/core/consistency.py` — add `weight_key_columns_unavailable`, `weight_key_arity_mismatch`; generalize `weight_unknown_items`; wire all into `validate_config_consistency`; update A9 legend → A9a/A9b/A9c.
- Modify: `tests/test_core/test_consistency.py` — rewrite `TestWeightUnknownItems` for new schema; add A9a/A9b tests; add a CLI-level collect-all assertion.
- Modify: `conf/base/parameters_training.yaml` — add `sample_weight_keys: [prod_name]`; rewrite `sample_weights` comment block.

---

## Task 1: Generalize weight computation in `io/extract.py`

**Files:**
- Modify: `src/recsys_tfb/io/extract.py:25-56`
- Test: `tests/test_io/test_extract.py:393-410` (rewrite), `:417-423` (`_wparams`), plus new cases

- [ ] **Step 1: Rewrite the failing tests for the new `_compute_row_weights` signature**

Replace `tests/test_io/test_extract.py` lines 393-410 (the `from ... import _compute_row_weights` line through the end of `TestComputeRowWeights`) with:

```python
from recsys_tfb.io.extract import _compute_row_weights


class TestComputeRowWeights:
    def _pdf(self):
        return pd.DataFrame({
            "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
            "prod_name": ["a", "a", "b", "a"],
            "label": [1, 0, 1, 0],
        })

    def test_single_key_prod_name_only(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 3.0})
        assert isinstance(w, np.ndarray)
        np.testing.assert_array_equal(w, np.array([3.0, 3.0, 1.0, 3.0]))

    def test_multi_key_segment_prod(self):
        w = _compute_row_weights(
            self._pdf(), ["cust_segment_typ", "prod_name"],
            {"mass|a": 3.0, "hnw|a": 2.0})
        np.testing.assert_array_equal(w, np.array([3.0, 2.0, 1.0, 1.0]))

    def test_three_key_segment_prod_label(self):
        w = _compute_row_weights(
            self._pdf(), ["cust_segment_typ", "prod_name", "label"],
            {"mass|a|1": 5.0})
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0, 1.0]))

    def test_down_weight_below_one(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 0.5})
        np.testing.assert_array_equal(w, np.array([0.5, 0.5, 1.0, 0.5]))

    def test_empty_weights_all_ones(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {})
        np.testing.assert_array_equal(w, np.ones(4))

    def test_empty_keys_all_ones(self):
        w = _compute_row_weights(self._pdf(), [], {"a": 3.0})
        np.testing.assert_array_equal(w, np.ones(4))

    def test_dtype_is_float64(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 2.0})
        assert w.dtype == np.float64
```

- [ ] **Step 2: Run the rewritten tests to verify they fail**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py::TestComputeRowWeights -q`
Expected: FAIL — `_compute_row_weights` still has the old `(seg, prod, sample_weights)` signature, so calls with a DataFrame + key list raise `TypeError` / `AttributeError`.

- [ ] **Step 3: Rewrite `_compute_row_weights` and `_row_weights_from_pdf`; drop `SEGMENT_COLUMN`**

In `src/recsys_tfb/io/extract.py`, delete the `SEGMENT_COLUMN = "cust_segment_typ"` line (currently line 25) and replace the two functions (currently lines 28-56) with:

```python
def _compute_row_weights(
    pdf: pd.DataFrame,
    weight_keys: list,
    sample_weights: dict,
) -> np.ndarray:
    """Per-row LightGBM sample weight from a composite-key weight table.

    Pure: no Spark, no I/O. Each row's lookup key is its ``weight_keys``
    column values joined with '|' (mirrors the dataset sampler's
    ``sample_ratio_overrides`` key in pipelines/dataset/helpers_spark.py).
    Rows whose key is absent from ``sample_weights`` get weight 1.0
    (sparse-emit: only adjusted groups are written to the table).
    """
    if not sample_weights or not weight_keys:
        return np.ones(len(pdf), dtype=np.float64)
    keys = pdf[weight_keys[0]].astype(str)
    for k in weight_keys[1:]:
        keys = keys.str.cat(pdf[k].astype(str), sep="|")
    return keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)


def _row_weights_from_pdf(pdf: pd.DataFrame, parameters: dict) -> np.ndarray:
    """Resolve a per-row weight array from training.sample_weights.

    All-ones when the table is absent/empty or any configured weight-key
    column is missing from pdf (graceful, never raises; consistency gate A9a
    already blocks unavailable columns at CLI entry). Computed from the
    *given* pdf so it stays aligned to the caller's filtering/ordering.
    """
    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]
    if not sw or any(k not in pdf.columns for k in weight_keys):
        return np.ones(len(pdf), dtype=np.float64)
    return _compute_row_weights(pdf, weight_keys, sw)
```

(`get_schema` is already imported at `extract.py:20`.)

- [ ] **Step 4: Run `TestComputeRowWeights` to verify it passes**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py::TestComputeRowWeights -q`
Expected: PASS (7 tests).

- [ ] **Step 5: Update `_wparams` and add an integration weight case for the new key form**

In `tests/test_io/test_extract.py`, replace the `_wparams` helper (currently lines 417-423) with:

```python
def _wparams(weights, weight_keys=None):
    training = {"sample_weights": weights}
    if weight_keys is not None:
        training["sample_weight_keys"] = weight_keys
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": training,
    }
```

Then in `class TestExtractWithWeights` add this test (after `test_extract_Xy_with_weights_appends_aligned_w`):

```python
    def test_extract_Xy_default_key_is_prod_name(self, tmp_path):
        # no sample_weight_keys -> defaults to schema.item (prod_name)
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({"a": 7.0}), with_weights=True)
        # rows: prod a, b, a, b
        np.testing.assert_array_equal(w, np.array([7.0, 1.0, 7.0, 1.0]))

    def test_extract_Xy_three_key_segment_prod_label(self, tmp_path):
        X, y, w = extract_Xy(
            _wparquet(tmp_path), _wprep(),
            _wparams({"mass|a|1": 9.0},
                     weight_keys=["cust_segment_typ", "prod_name", "label"]),
            with_weights=True)
        # rows: mass|a|1, mass|b|0, hnw|a|1, hnw|b|0
        np.testing.assert_array_equal(w, np.array([9.0, 1.0, 1.0, 1.0]))

    def test_extract_Xy_missing_key_column_all_ones(self, tmp_path):
        # configured key column not in parquet -> graceful all-ones backstop
        X, y, w = extract_Xy(
            _wparquet(tmp_path), _wprep(),
            _wparams({"x": 5.0}, weight_keys=["not_a_real_column"]),
            with_weights=True)
        np.testing.assert_array_equal(w, np.ones(4))
```

Note: the existing `test_extract_Xy_with_weights_appends_aligned_w` uses `_wparams({"mass|a": 5.0})` with default keys (now `[prod_name]`); update that call to declare the segment key explicitly — change it to `_wparams({"mass|a": 5.0}, weight_keys=["cust_segment_typ", "prod_name"])`. Likewise update `test_extract_Xy_with_groups_with_weights_appends_w`'s `_wparams({"hnw|a": 4.0})` to `_wparams({"hnw|a": 4.0}, weight_keys=["cust_segment_typ", "prod_name"])`.

- [ ] **Step 6: Run the full extract test file**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py -q`
Expected: PASS (all tests; ~20 collected).

- [ ] **Step 7: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights commit -m "feat(training): composite-key sample_weights in extract"
```

---

## Task 2: Generalize A9 + add data-dependency gates in `core/consistency.py`

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py:354-371` (`weight_unknown_items`), `:434-440` (wiring), `:46-50` (A9 legend)
- Test: `tests/test_core/test_consistency.py:290-313` (rewrite `TestWeightUnknownItems`), add new classes

- [ ] **Step 1: Rewrite/extend the failing consistency tests**

In `tests/test_core/test_consistency.py`, replace the `TestWeightUnknownItems` block (currently lines 290-313, the `from ... import weight_unknown_items` line through `assert weight_unknown_items(p) == []` at line 313) with:

```python
from recsys_tfb.core.consistency import (
    weight_unknown_items,
    weight_key_columns_unavailable,
    weight_key_arity_mismatch,
)


class TestWeightUnknownItems:
    def test_unknown_product_component_detected(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0, "hnw|zzz": 3.0}}})
        assert weight_unknown_items(p) == ["zzz"]

    def test_single_prod_name_key_unknown_detected(self):
        p = _base({"training": {
            "sample_weight_keys": ["prod_name"],
            "sample_weights": {"a": 2.0, "zzz": 3.0}}})
        assert weight_unknown_items(p) == ["zzz"]

    def test_all_known_returns_empty(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0, "hnw|b": 3.0}}})
        assert weight_unknown_items(p) == []

    def test_item_not_in_keys_returns_empty(self):
        # schema.item absent from weight keys -> no product component to check
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ"],
            "sample_weights": {"mass": 2.0}}})
        assert weight_unknown_items(p) == []

    def test_no_sample_weights_returns_empty(self):
        assert weight_unknown_items(_base()) == []


class TestWeightKeyColumnsUnavailable:
    def test_carried_column_is_available(self):
        p = _base({"dataset": {"carry_columns": ["cust_segment_typ"]},
                   "training": {
                       "sample_weight_keys": ["cust_segment_typ", "prod_name"]}})
        assert weight_key_columns_unavailable(p) == []

    def test_label_and_item_always_available(self):
        p = _base({"training": {"sample_weight_keys": ["prod_name", "label"]}})
        assert weight_key_columns_unavailable(p) == []

    def test_uncarried_column_flagged(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"]}})
        assert weight_key_columns_unavailable(p) == ["cust_segment_typ"]

    def test_no_keys_returns_empty(self):
        assert weight_key_columns_unavailable(_base()) == []


class TestWeightKeyArityMismatch:
    def test_matching_arity_ok(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0}}})
        assert weight_key_arity_mismatch(p) == []

    def test_wrong_segment_count_flagged(self):
        p = _base({"training": {
            "sample_weight_keys": ["prod_name"],
            "sample_weights": {"mass|a": 2.0}}})
        assert weight_key_arity_mismatch(p) == ["mass|a"]

    def test_no_keys_returns_empty(self):
        p = _base({"training": {"sample_weights": {"a": 2.0}}})
        assert weight_key_arity_mismatch(p) == []
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_consistency.py::TestWeightKeyColumnsUnavailable" "tests/test_core/test_consistency.py::TestWeightKeyArityMismatch" -q`
Expected: FAIL — `ImportError`/`AttributeError`: `weight_key_columns_unavailable` and `weight_key_arity_mismatch` do not exist yet.

- [ ] **Step 3: Add the two new predicates and generalize `weight_unknown_items`**

In `src/recsys_tfb/core/consistency.py`, replace `weight_unknown_items` (currently lines 354-371) with these three functions:

```python
def weight_key_columns_unavailable(parameters: dict) -> list[str]:
    """training.sample_weight_keys columns absent from train model_input (A9a).

    The train/train_dev model_input parquet physically contains only identity
    columns, the label, dataset.carry_columns, and *encoded* features. A weight
    key must therefore be one of identity ∪ {label} ∪ carry_columns — the
    raw-valued columns. Anything else is either physically absent (weight
    silently no-ops at 1.0) or int-encoded (key never matches). This is a
    cross-file dependency: sample_weight_keys lives in parameters_training.yaml
    but carry_columns lives in parameters_dataset.yaml. Returns sorted
    offending columns; empty means OK.
    """
    schema = get_schema(parameters)
    available = (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set((parameters.get("dataset", {}) or {}).get("carry_columns") or [])
    )
    keys = (parameters.get("training", {}) or {}).get("sample_weight_keys") or []
    return sorted(k for k in keys if k not in available)


def weight_key_arity_mismatch(parameters: dict) -> list[str]:
    """training.sample_weights keys whose '|'-segment count != key arity (A9b).

    Each weight-table key is sample_weight_keys values joined with '|', so it
    must have exactly len(sample_weight_keys) segments. A miscounted key
    silently never matches any row. Returns sorted offending keys; empty
    means OK. No keys configured (arity 0) → nothing to check.
    """
    training = parameters.get("training", {}) or {}
    n = len(training.get("sample_weight_keys") or [])
    if n == 0:
        return []
    weights = training.get("sample_weights") or {}
    return sorted(str(k) for k in weights if len(str(k).split("|")) != n)


def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A9c).

    Weight-table keys are '|'-joined sample_weight_keys values. If schema.item
    is not a weight key there is no product component → nothing to check
    (mirrors A5's item-only check in override_unknown_items). Only keys whose
    segment count matches the key arity are inspected; arity errors are
    reported separately by weight_key_arity_mismatch.
    """
    training = parameters.get("training", {}) or {}
    keys = training.get("sample_weight_keys") or []
    item = get_schema(parameters)["item"]
    if item not in keys:
        return []
    idx = keys.index(item)
    weights = training.get("sample_weights") or {}
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) == len(keys) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)
```

- [ ] **Step 4: Run the predicate tests to verify they pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_core/test_consistency.py::TestWeightUnknownItems" "tests/test_core/test_consistency.py::TestWeightKeyColumnsUnavailable" "tests/test_core/test_consistency.py::TestWeightKeyArityMismatch" -q`
Expected: PASS (all classes).

- [ ] **Step 5: Wire A9a/A9b into `validate_config_consistency`**

In `src/recsys_tfb/core/consistency.py`, find the A9c block in `validate_config_consistency` (currently lines 434-440, the `unknown_w = weight_unknown_items(parameters)` block). Immediately after that `if unknown_w:` block, add:

```python
    cols_bad = weight_key_columns_unavailable(parameters)
    if cols_bad:
        errors.append(
            f"training.sample_weight_keys column(s) {cols_bad} are not in the "
            f"train model_input parquet (identity ∪ {{label}} ∪ "
            f"dataset.carry_columns) — the weight would silently never match. "
            f"Add them to dataset.carry_columns and re-run the dataset "
            f"pipeline (this busts base_dataset_version)."
        )

    arity_bad = weight_key_arity_mismatch(parameters)
    if arity_bad:
        n = len(parameters.get("training", {}).get("sample_weight_keys") or [])
        errors.append(
            f"training.sample_weights key(s) {arity_bad} do not have "
            f"{n} '|'-separated segment(s) to match "
            f"sample_weight_keys — the weight silently never matches. "
            f"Fix the key(s) or sample_weight_keys."
        )
```

- [ ] **Step 6: Add a CLI-level collect-all test asserting all three A9 errors surface together**

In `tests/test_core/test_consistency.py`, locate the test class that exercises `validate_config_consistency` collect-all (search for `validate_config_consistency`). Add this test there (if no such class exists, append a new `class TestValidateConfigConsistencyWeights:` near the end of the file, before the final import line if any):

```python
    def test_all_three_a9_errors_collected(self):
        from recsys_tfb.core.consistency import (
            validate_config_consistency, ConfigConsistencyError)
        p = _base({
            "training": {
                "sample_weight_keys": ["cust_segment_typ", "prod_name"],
                "sample_weights": {"mass|zzz": 2.0, "badkey": 3.0}}})
        # cust_segment_typ not carried (A9a), "badkey" wrong arity (A9b),
        # "zzz" unknown product (A9c)
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(p)
        msg = str(exc.value)
        assert "sample_weight_keys" in msg      # A9a
        assert "segment" in msg                  # A9b
        assert "sample_weights" in msg           # A9c
```

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_consistency.py -q -k "Weight or consistency"`
Expected: PASS. (If `_base` needs adjustment because `validate_config_consistency` reads more keys, confirm `_base()` already supplies `schema.categorical_values` and `dataset.prepare_model_input.categorical_columns` — it does, lines 86-96.)

- [ ] **Step 7: Update the A9 legend in the module docstring**

In `src/recsys_tfb/core/consistency.py`, replace the A9 bullet in the Invariant legend (currently lines 46-50, the `* A9 — ... (product-only check, mirrors A5).` bullet) with:

```
* A9 — ``training.sample_weights`` integrity (keys are '|'-joined
  ``training.sample_weight_keys`` values), split into:
    - A9a — a ``sample_weight_keys`` column ∉ identity ∪ {label} ∪
      ``dataset.carry_columns`` (cross-file: the column would be absent from
      or int-encoded in the train model_input parquet, so the weight silently
      no-ops). Predicate: ``weight_key_columns_unavailable``.
    - A9b — a ``sample_weights`` key whose '|'-segment count ≠
      ``len(sample_weight_keys)`` (silently never matches). Predicate:
      ``weight_key_arity_mismatch``.
    - A9c — a ``sample_weights`` key whose product component (when
      ``schema.item`` is a weight key) ∉ ``resolved_item_values`` (mirrors A5).
      Predicate: ``weight_unknown_items``.
```

- [ ] **Step 8: Run the full consistency test file**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_consistency.py -q`
Expected: PASS (all tests).

- [ ] **Step 9: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights commit -m "feat(consistency): A9a/A9b data-dependency gates for sample_weight_keys"
```

---

## Task 3: Config schema + comments in `parameters_training.yaml`

**Files:**
- Modify: `conf/base/parameters_training.yaml:39-44`

- [ ] **Step 1: Add `sample_weight_keys` and rewrite the `sample_weights` comment block**

In `conf/base/parameters_training.yaml`, replace the current block (lines 39-44):

```yaml
  # --- Per-(segment,product) sample weights（模型層冷門產品 boost）---
  # key 固定 "<cust_segment_typ>|<prod_name>"；value = LightGBM sample_weight
  # (>= 1.0，只 boost)。稀疏：只列 != 1.0 的組。只作用於 train/train_dev，
  # val/calibration/evaluation 不加權。A9 一致性閘驗證 product 分量。改此表
  # 會 bust model_version、不動 train_variant_id。
  sample_weights: {}
```

with:

```yaml
  # --- Composite-key sample weights（模型層調權，如冷門產品 boost）---
  # sample_weight_keys: weight key 由哪些欄位組成（順序即 "|" 串接順序），
  #   與 dataset.sample_group_keys 對稱。預設 [prod_name]：直接對產品調權。
  #   每個欄位必須 ∈ identity(snap_date,cust_id,prod_name) ∪ {label} ∪
  #   dataset.carry_columns（否則不在 train model_input parquet → weight 靜默
  #   全 1.0，A9a 一致性閘會擋）。用非 identity/label 欄位（如 cust_segment_typ）
  #   須先加進 dataset.carry_columns 並重跑 dataset pipeline（bust
  #   base_dataset_version）。
  sample_weight_keys:
    - prod_name
  # sample_weights: key = sample_weight_keys 值用 "|" 串接；value = LightGBM
  #   sample_weight（任意正數：>1 boost、<1 down-weight）。稀疏：只列 != 1.0
  #   的組，沒列到的 row = 1.0。只作用於 train/train_dev，val/calibration/
  #   evaluation 不加權。A9b 驗 key 段數、A9c 驗 product 分量。改 keys 或此表
  #   會 bust model_version、不動 train_variant_id。
  #   範例（sample_weight_keys: [cust_segment_typ, prod_name]）：
  #     "mass|ccard_ins": 2.0
  #     "affluent|fund_mix": 0.7
  sample_weights: {}
```

- [ ] **Step 2: Verify config loads and passes the consistency gate (default config)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import yaml
from recsys_tfb.core.consistency import validate_config_consistency
from pathlib import Path
params = {}
for f in Path('conf/base').glob('parameters_*.yaml'):
    params.update(yaml.safe_load(f.read_text()) or {})
validate_config_consistency(params)
print('OK: default config passes consistency gate')
print('sample_weight_keys =', params['training'].get('sample_weight_keys'))
"`
Expected: prints `OK: default config passes consistency gate` and `sample_weight_keys = ['prod_name']`. (`prod_name` ∈ identity, empty `sample_weights` → A9b/A9c trivially pass.)

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights add conf/base/parameters_training.yaml
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights commit -m "feat(config): sample_weight_keys composite-key weight config"
```

---

## Task 4: Full targeted regression + graph refresh

**Files:** none (verification only)

- [ ] **Step 1: Run all touched test files together**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py tests/test_core/test_consistency.py -q`
Expected: PASS (≈90 tests, 0 failures).

- [ ] **Step 2: Grep for any remaining hardcoded weight-key assumptions**

Run: `grep -rn "SEGMENT_COLUMN\|cust_segment_typ.*prod_name\|split(\"|\")\[1\]" src/recsys_tfb/io/extract.py src/recsys_tfb/core/consistency.py`
Expected: no matches (the old hardcoded segment|product assumptions are gone).

- [ ] **Step 3: Refresh the graphify code graph**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
Expected: prints rebuild summary (nodes/edges/communities).

- [ ] **Step 4: Final review of the diff**

Run: `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/generalize-sample-weights diff main..feat/generalize-sample-weights --stat`
Expected: 5 files changed (extract.py, test_extract.py, consistency.py, test_consistency.py, parameters_training.yaml) plus the spec/plan docs and graphify-out updates.

---

## Notes for the implementer

- **Default-to-`[item]` symmetry:** runtime (`_row_weights_from_pdf`) and the YAML both default to `[prod_name]` (= `schema.item`). The consistency predicates read `sample_weight_keys` as-is (no default); since the YAML always supplies `[prod_name]`, this is consistent in practice and avoids surprising arity checks when the key is omitted entirely.
- **Versioning:** no change to `core/versioning.py`. `sample_weight_keys` sits inside the `training:` block and is not in `MODEL_VERSION_IRRELEVANT_PARAMS`, so it is automatically hashed into `model_version`. It is intentionally **not** added to `TRAIN_SAMPLING_KEYS` (weight is a training-layer concern and must not bust the dataset sampling variant).
- **Back-compat:** existing configs set `sample_weights: {}`, so the default `[prod_name]` causes no behavior change. Any pre-existing `"<seg>|<prod>"` weight table must now also declare `sample_weight_keys: [cust_segment_typ, prod_name]` (and carry `cust_segment_typ`) or A9a/A9b will flag it — this is the intended surfacing of the previously-silent dependency.

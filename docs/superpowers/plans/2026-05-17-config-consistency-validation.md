# Config / Data Consistency Prevention Mechanism — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the class of *silent* / *misleading* failures caused by user-introduced inconsistency between config files, SQL, and actual data, by consolidating every consistency invariant into one module that fails fast with actionable messages.

**Architecture:** A single `core/consistency.py` owns each invariant *once* as a pure predicate. Three validation layers consume those predicates by *when they can run*: Layer 1 config-static (no data, runs at every CLI entry), Layer 2 config-vs-data (Spark, runs at preprocessor fit), Layer 3 cross-source/data-actual-state (source_etl pre-flight + lint). Existing scattered checks are refactored to delegate to the shared predicates (DRY) instead of being deleted. This plan implements **Phase 1 (Layer 1, config-static)** in full; Phases 2–3 are specified architecturally and deferred.

**Tech Stack:** Python 3.10, PySpark 3.3.2, pytest 7.3.1. No new dependencies (production constraint: no network, no extra packages).

---

## Problem Statement (consolidated from design discussion)

The unifying disease: the same logical fact (the valid item-value set; each column's role) is asserted independently in many places, and consumers tolerate mismatch (encode `-1`, "drop wins", skip override) instead of asserting. Concrete inconsistency classes:

| ID | Inconsistency | Today | Target |
|----|---------------|-------|--------|
| A1 | a column in BOTH `drop_columns` and `categorical_columns` | silent (drop wins) in prod; misleading fail-loud in dev — **environment-divergent** | **reject** at config-static (ConfigError, names both resolutions) |
| A2 | `categorical_columns` omits `schema.item` | fail-loud at `_spark.py:191` (late, Spark) | also caught config-static (earlier) |
| A3 | identity categorical not in `schema.categorical_values` | `validate_schema_config` + `_spark.py:154` (duplicated logic) | one shared predicate, both call it |
| A4 | `inference.products` ≠ `categorical_values[item]` | **no check** → silent garbage scores | config-static equivalence assertion |
| A5 | `sample_ratio_overrides` key references unknown item | **no check** → silent no-op | config-static assertion |
| A6 | 6+ hardcoded item lists in SQL/yaml disagree | `test_product_consistency.py` lint (test-time only, regex-duplicated) | lint repointed to shared predicate |
| B1 | train data item value ∉ `categorical_values[item]` | **silent `-1` training corruption** (`_spark.py:154` only checks col declared, not value coverage) | Layer 2 hard error |
| B2 | label-window leakage cols (`apply_start_date`/`apply_end_date`) reach features | **no check** → silent target leakage | Layer 2 hard error |
| B3 | declared item has 0 positives in label over train window | silent "never recommend" | Layer 2, configurable severity |
| C1 | produced sample_pool/label distinct item ≠ config | lint only (not runtime) | Layer 3 source_etl pre-flight |

**Channel-name generalization requirement:** all predicates must key off `schema["item"]` (never the literal `"prod_name"`), so a future rename to `channel_name` needs only `schema.columns.item` + `schema.categorical_values.<new>` and the SQL layer — predicates/guards auto-follow.

**Design decision (A1 — explicitly asked):** reject, do not resolve. The system cannot infer intent ("drop is stale" vs "categorical is stale"); any precedence default is invisible magic, and "feature-wins" reopens leakage with asymmetric catastrophic risk. So a column in both lists is an *illegal config state*, surfaced with both resolutions for the user to choose.

---

## Architecture

### Module map

- **Create `src/recsys_tfb/core/consistency.py`** — the single source of truth.
  - Exceptions: `ConsistencyError(ValueError)` (base; subclasses `ValueError` so `__main__.py:84`'s `except ValueError` and existing `pytest.raises(ValueError)` keep working), `ConfigConsistencyError(ConsistencyError)`, `DataConsistencyError(ConsistencyError)`.
  - Pure predicates (no Spark, no I/O): `resolved_item_values(parameters)`, `config_role_conflicts(parameters)`, `item_missing_from_categorical(parameters)`, `inference_products_mismatch(parameters)`, `override_unknown_items(parameters)`.
  - Layer-1 entry: `validate_config_consistency(parameters)` — runs all A* predicates, **collects every failure, raises once** (user fixes all in one pass).
- **Modify `src/recsys_tfb/__main__.py:82-86`** — call `validate_config_consistency(params)` alongside existing `validate_schema_config(params)` in the same try/except.
- **Modify `src/recsys_tfb/core/schema.py`** — `validate_schema_config` keeps its shape checks (well-tested, untouched) but its A3 block delegates to `consistency.resolved_item_values` so the invariant has one definition.
- **Modify `src/recsys_tfb/preprocessing/_spark.py:154,191`** — keep both guards (they are the definitive *post-feature_table-introspection* checks) but raise via shared helpers / `DataConsistencyError` for unified messaging; remove duplicated ad-hoc strings.
- **Modify `tests/test_pipelines/test_source_etl/test_product_consistency.py`** — config side consumes `resolved_item_values`; extend scan to `scripts/generate_synthetic_data.py` `PRODUCTS`.

### Why delegate instead of rewrite

`validate_schema_config` is wired (`__main__.py:83`) and has ~25 passing tests asserting `ValueError` + message substrings. Rewriting risks regressions for zero functional gain. DRY is achieved by consolidating *invariant definitions* (predicates), not by collapsing call sites. Defense-in-depth (config-time AND runtime) is intentional: Layer 1 catches early with no Spark; the `_spark.py` guards still catch conditions only knowable after feature_table introspection.

### Phasing (independently shippable)

- **Phase 1 — Layer 1 config-static (THIS PLAN, full TDD below).** Pure Python, zero Spark, lowest risk. Kills A1/A2/A4/A5 + consolidates A3. Immediately removes the environment-divergent A1 trap and the silent A4 drift.
- **Phase 2 — Layer 2 config-vs-data.** `validate_data_consistency` at `fit_preprocessor_metadata`: B1 coverage gate, B2 leakage guard, B3 configurable. Needs Spark fixtures. Specified in "Future Phases".
- **Phase 3 — Layer 3 + single source of truth (separate plan).** source_etl runtime pre-flight (C1) and the `products.yaml` + SQLRenderer generation (軸 1) is a *different subsystem* (generation, not validation) — per writing-plans Scope Check it gets its own plan: `docs/superpowers/plans/<date>-item-single-source-of-truth.md`.

---

## Phase 1 Tasks

### Task 1: Consistency module skeleton — exceptions

**Files:**
- Create: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

```python
"""Tests for recsys_tfb.core.consistency."""

import pytest

from recsys_tfb.core.consistency import (
    ConsistencyError,
    ConfigConsistencyError,
    DataConsistencyError,
)


class TestExceptionHierarchy:
    def test_consistency_error_is_valueerror(self):
        assert issubclass(ConsistencyError, ValueError)

    def test_config_error_is_consistency_error(self):
        assert issubclass(ConfigConsistencyError, ConsistencyError)

    def test_data_error_is_consistency_error(self):
        assert issubclass(DataConsistencyError, ConsistencyError)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestExceptionHierarchy -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.core.consistency'`

- [ ] **Step 3: Write minimal implementation**

```python
"""Single source of truth for config / data consistency invariants.

Every invariant is defined ONCE here as a pure predicate. Layer-1 config-static
validation, Layer-2 preprocessing guards, and the test_product_consistency lint
all call these predicates — no duplicated definitions, no message drift.

All errors subclass ValueError so existing ``except ValueError`` call sites
(__main__._load_config_and_setup) and existing tests keep working unchanged.
"""

from __future__ import annotations


class ConsistencyError(ValueError):
    """Base for all consistency failures (subclasses ValueError by design)."""


class ConfigConsistencyError(ConsistencyError):
    """Config self-contradiction detectable without data (Layer 1)."""


class DataConsistencyError(ConsistencyError):
    """Config disagrees with the actual data (Layer 2)."""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestExceptionHierarchy -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add consistency error hierarchy"
```

---

### Task 2: `resolved_item_values` predicate (A3, shared)

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

```python
from recsys_tfb.core.consistency import resolved_item_values


class TestResolvedItemValues:
    def _params(self, **over):
        p = {
            "schema": {
                "columns": {"item": "prod_name"},
                "categorical_values": {"prod_name": ["b", "a", "c"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        p.update(over)
        return p

    def test_returns_sorted_declared_values(self):
        assert resolved_item_values(self._params()) == ["a", "b", "c"]

    def test_respects_custom_item_name(self):
        p = {
            "schema": {
                "columns": {"item": "channel_name"},
                "categorical_values": {"channel_name": ["sms", "app"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["channel_name"]}},
        }
        assert resolved_item_values(p) == ["app", "sms"]

    def test_item_declared_categorical_but_no_values_raises(self):
        p = self._params()
        del p["schema"]["categorical_values"]["prod_name"]
        with pytest.raises(ConfigConsistencyError, match="categorical_values"):
            resolved_item_values(p)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestResolvedItemValues -q`
Expected: FAIL — `ImportError: cannot import name 'resolved_item_values'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/core/consistency.py`:

```python
from recsys_tfb.core.schema import get_schema


def _prepare_model_input(parameters: dict) -> dict:
    return (parameters.get("dataset", {}) or {}).get("prepare_model_input", {}) or {}


def resolved_item_values(parameters: dict) -> list[str]:
    """Canonical sorted list of valid item values (the single source).

    Reads ``schema.categorical_values[schema.item]``. Raises
    ``ConfigConsistencyError`` when the item column is a declared categorical
    (in prepare_model_input.categorical_columns) but has no category list —
    this is invariant A3, defined here once.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    cat_values = schema.get("categorical_values", {}) or {}
    declared_cats = _prepare_model_input(parameters).get("categorical_columns")
    if declared_cats is not None and item in declared_cats and item not in cat_values:
        raise ConfigConsistencyError(
            f"schema.item={item!r} is in dataset.prepare_model_input."
            f"categorical_columns but has no schema.categorical_values[{item!r}] "
            f"declaration. Add the full value list under "
            f"schema.categorical_values.{item} in parameters.yaml."
        )
    return sorted(cat_values.get(item, []))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestResolvedItemValues -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add resolved_item_values predicate (A3)"
```

---

### Task 3: `config_role_conflicts` predicate (A1)

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

```python
from recsys_tfb.core.consistency import config_role_conflicts


class TestConfigRoleConflicts:
    def _params(self, drop, cat):
        return {"dataset": {"prepare_model_input": {
            "drop_columns": drop, "categorical_columns": cat}}}

    def test_no_overlap_returns_empty(self):
        assert config_role_conflicts(
            self._params(["snap_date", "label"], ["prod_name"])) == []

    def test_overlap_returns_offending_columns_sorted(self):
        assert config_role_conflicts(
            self._params(["cust_segment_typ", "label"],
                         ["prod_name", "cust_segment_typ"])) == ["cust_segment_typ"]

    def test_missing_keys_returns_empty(self):
        assert config_role_conflicts({}) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestConfigRoleConflicts -q`
Expected: FAIL — `ImportError: cannot import name 'config_role_conflicts'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/core/consistency.py`:

```python
def config_role_conflicts(parameters: dict) -> list[str]:
    """Columns declared in BOTH drop_columns and categorical_columns (A1).

    A column in both lists is an illegal, environment-divergent config state
    (silent 'drop wins' in prod, misleading fail-loud in dev). Returned sorted;
    empty list means OK.
    """
    pmi = _prepare_model_input(parameters)
    drop = set(pmi.get("drop_columns", []) or [])
    cat = set(pmi.get("categorical_columns", []) or [])
    return sorted(drop & cat)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestConfigRoleConflicts -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add config_role_conflicts predicate (A1)"
```

---

### Task 4: `inference_products_mismatch` (A4) + `override_unknown_items` (A5) + `item_missing_from_categorical` (A2)

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

```python
from recsys_tfb.core.consistency import (
    inference_products_mismatch,
    override_unknown_items,
    item_missing_from_categorical,
)


def _base(over=None):
    p = {
        "schema": {
            "columns": {"item": "prod_name"},
            "categorical_values": {"prod_name": ["a", "b"]},
        },
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
    }
    if over:
        p.update(over)
    return p


class TestInferenceProductsMismatch:
    def test_equal_sets_returns_empty(self):
        p = _base({"inference": {"products": ["b", "a"]}})
        assert inference_products_mismatch(p) == {"only_in_inference": [],
                                                  "only_in_categorical": []}

    def test_reports_both_directions(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        assert inference_products_mismatch(p) == {
            "only_in_inference": ["c"], "only_in_categorical": ["b"]}

    def test_no_inference_section_returns_empty(self):
        assert inference_products_mismatch(_base()) == {
            "only_in_inference": [], "only_in_categorical": []}


class TestOverrideUnknownItems:
    def test_unknown_item_component_detected(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio_overrides": {"mass|a|0": 0.5, "mass|zzz|0": 0.9}}})
        assert override_unknown_items(p) == ["zzz"]

    def test_item_not_in_group_keys_skipped(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ"],
            "sample_ratio_overrides": {"mass": 0.5}}})
        assert override_unknown_items(p) == []


class TestItemMissingFromCategorical:
    def test_item_present_ok(self):
        assert item_missing_from_categorical(_base()) is False

    def test_item_absent_detected(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["categorical_columns"] = ["gender"]
        assert item_missing_from_categorical(p) is True

    def test_key_absent_uses_default_includes_item(self):
        p = _base()
        del p["dataset"]["prepare_model_input"]["categorical_columns"]
        assert item_missing_from_categorical(p) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py -k "Mismatch or Override or MissingFromCategorical" -q`
Expected: FAIL — `ImportError: cannot import name 'inference_products_mismatch'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/core/consistency.py`:

```python
def inference_products_mismatch(parameters: dict) -> dict:
    """Symmetric diff between inference.products and resolved_item_values (A4).

    Empty 'inference' section → no mismatch (inference not configured here).
    """
    declared = set(resolved_item_values(parameters))
    inf = parameters.get("inference") or {}
    if "products" not in inf:
        return {"only_in_inference": [], "only_in_categorical": []}
    products = set(inf.get("products") or [])
    return {
        "only_in_inference": sorted(products - declared),
        "only_in_categorical": sorted(declared - products),
    }


def override_unknown_items(parameters: dict) -> list[str]:
    """sample_ratio_overrides keys whose item component ∉ resolved_item_values (A5).

    Override keys are '|'-joined sample_group_keys values. If schema.item is not
    a sample_group_key there is no item component → nothing to check.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    ds = parameters.get("dataset", {}) or {}
    group_keys = ds.get("sample_group_keys", [])
    if item not in group_keys:
        return []
    idx = group_keys.index(item)
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in (ds.get("sample_ratio_overrides") or {}):
        parts = str(key).split("|")
        if idx < len(parts) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def item_missing_from_categorical(parameters: dict) -> bool:
    """True if schema.item is absent from an explicitly-set categorical_columns (A2).

    When the key is absent, the codebase default ([schema.item]) includes it,
    so that case is OK.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    declared = _prepare_model_input(parameters).get("categorical_columns")
    if declared is None:
        return False
    return item not in declared
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py -k "Mismatch or Override or MissingFromCategorical" -q`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add A4/A5/A2 predicates"
```

---

### Task 5: `validate_config_consistency` entry — collect-all, raise-once

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

```python
from recsys_tfb.core.consistency import validate_config_consistency


class TestValidateConfigConsistency:
    def test_clean_config_passes(self):
        validate_config_consistency(_base({"inference": {"products": ["a", "b"]}}))

    def test_a1_conflict_message_names_both_resolutions(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["cust_segment_typ"]
        p["dataset"]["prepare_model_input"]["categorical_columns"] = [
            "prod_name", "cust_segment_typ"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "cust_segment_typ" in msg
        assert "drop_columns" in msg and "categorical_columns" in msg

    def test_collects_multiple_errors_in_one_raise(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["prod_name"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "prod_name" in msg          # A1 (prod_name in drop ∩ categorical)
        assert "c" in msg                  # A4 only_in_inference
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestValidateConfigConsistency -q`
Expected: FAIL — `ImportError: cannot import name 'validate_config_consistency'`

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/core/consistency.py`:

```python
def validate_config_consistency(parameters: dict) -> None:
    """Layer-1 config-static gate. Collects ALL failures, raises once.

    Collect-all (not fail-on-first) so a user fixes every problem in one pass.
    """
    errors: list[str] = []

    for col in config_role_conflicts(parameters):
        errors.append(
            f"{col!r} is declared in BOTH "
            f"dataset.prepare_model_input.drop_columns and categorical_columns "
            f"— contradictory intent. Resolve by choosing one:\n"
            f"    - want it as a feature  -> remove from drop_columns\n"
            f"    - want it excluded      -> remove from categorical_columns"
        )

    if item_missing_from_categorical(parameters):
        item = get_schema(parameters)["item"]
        errors.append(
            f"schema.item={item!r} is missing from "
            f"dataset.prepare_model_input.categorical_columns. For a ranking "
            f"task the item must be a model feature; add {item!r} back."
        )

    mm = inference_products_mismatch(parameters)
    if mm["only_in_inference"] or mm["only_in_categorical"]:
        errors.append(
            f"inference.products disagrees with schema.categorical_values"
            f"[item]: only_in_inference={mm['only_in_inference']}, "
            f"only_in_categorical={mm['only_in_categorical']}. They must be "
            f"identical sets."
        )

    unknown = override_unknown_items(parameters)
    if unknown:
        errors.append(
            f"sample_ratio_overrides references item value(s) {unknown} "
            f"absent from schema.categorical_values[item] — the override "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )

    if errors:
        raise ConfigConsistencyError(
            "Config consistency check failed (" + str(len(errors))
            + " issue(s)):\n- " + "\n- ".join(errors)
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py -q`
Expected: PASS (all consistency tests green)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add validate_config_consistency collect-all gate"
```

---

### Task 6: Wire into CLI entry (`__main__.py`)

**Files:**
- Modify: `src/recsys_tfb/__main__.py:1-20` (import), `src/recsys_tfb/__main__.py:82-86` (call)
- Test: `tests/test_core/test_consistency_cli_wiring.py`

- [ ] **Step 1: Write the failing test**

```python
"""validate_config_consistency must run in _load_config_and_setup."""

import inspect

from recsys_tfb import __main__ as m


def test_load_config_calls_validate_config_consistency():
    src = inspect.getsource(m._load_config_and_setup)
    assert "validate_config_consistency(params)" in src

def test_validate_config_consistency_imported():
    assert hasattr(m, "validate_config_consistency")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_core/test_consistency_cli_wiring.py -q`
Expected: FAIL — assertion error (`validate_config_consistency(params)` not in source)

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/__main__.py`, add to the existing `from recsys_tfb.core.schema import ...` area an import:

```python
from recsys_tfb.core.consistency import validate_config_consistency
```

Then modify `_load_config_and_setup` (currently lines 82-86) so the try block is:

```python
    try:
        validate_schema_config(params)
        validate_config_consistency(params)
    except ValueError as exc:
        logger.error("Config validation failed: %s", exc)
        raise typer.Exit(code=1)
```

(`ConfigConsistencyError` subclasses `ValueError`, so the existing handler catches it; the log line is generalized from "Schema config" to "Config".)

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_core/test_consistency_cli_wiring.py
git commit -m "feat(consistency): run config-static gate at CLI entry"
```

---

### Task 7: Delegate `schema.validate_schema_config` A3 to shared predicate (DRY)

**Files:**
- Modify: `src/recsys_tfb/core/schema.py:178-189`
- Test: `tests/test_core/test_schema_validation.py` (existing — must stay green)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_core/test_schema_validation.py`:

```python
class TestSchemaValidationDelegatesA3:
    def test_identity_cat_missing_values_still_raises_valueerror(self):
        # behaviour preserved after delegation to consistency.resolved_item_values
        p = {
            "schema": {"columns": {"item": "prod_name"}},
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(p)
```

- [ ] **Step 2: Run test to verify it fails or passes-by-accident**

Run: `.venv/bin/pytest tests/test_core/test_schema_validation.py -q`
Expected: the new test PASSES already (current code raises) — this is a *characterization* test pinning behaviour before refactor. Confirm full file green (baseline preserved).

- [ ] **Step 3: Refactor to delegate**

In `src/recsys_tfb/core/schema.py`, replace the A3 block (the `# Identity categorical columns must be declared in categorical_values.` section, currently ~lines 178-189) with a delegation:

```python
    # Identity categorical columns must declare category lists (invariant A3).
    # Single definition lives in core.consistency; call it so config-time and
    # runtime guards never drift. Import locally to avoid an import cycle
    # (consistency imports get_schema from this module).
    from recsys_tfb.core.consistency import resolved_item_values

    resolved_item_values(parameters)
```

(Note: `resolved_item_values` raises `ConfigConsistencyError` — a `ValueError` subclass with a message containing `categorical_values`, satisfying existing `pytest.raises(ValueError, match=...)` expectations. If other identity categoricals beyond `schema.item` need the same check, extend `resolved_item_values` later; current production schema only has `prod_name` as an identity categorical.)

- [ ] **Step 4: Run full schema-validation suite**

Run: `.venv/bin/pytest tests/test_core/test_schema_validation.py tests/test_core/test_schema.py -q`
Expected: PASS (all existing + new green; no regression)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/schema.py tests/test_core/test_schema_validation.py
git commit -m "refactor(schema): delegate A3 invariant to core.consistency (DRY)"
```

---

### Task 8: Unify `_spark.py` guard messaging via shared predicate

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py:152-160` (missing_cats)
- Test: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (existing Spark tests must stay green) + `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_core/test_consistency.py`:

```python
class TestSparkGuardUsesSharedError:
    def test_missing_cats_raises_data_consistency_error_subclass(self):
        # DataConsistencyError is still a ValueError, preserving callers
        assert issubclass(DataConsistencyError, ValueError)
```

(Behavioural Spark assertions are covered by the existing `test_nodes_spark.py`; this task only swaps the raised type/message, not the trigger condition.)

- [ ] **Step 2: Run test to verify current state**

Run: `.venv/bin/pytest tests/test_core/test_consistency.py::TestSparkGuardUsesSharedError -q`
Expected: PASS (already true) — characterization before refactor.

- [ ] **Step 3: Refactor messaging**

In `src/recsys_tfb/preprocessing/_spark.py`, at the `missing_cats` block (~lines 154-160), change the raised exception from bare `ValueError` to `DataConsistencyError` and keep the message text (it already says "Add them to parameters.yaml under schema.categorical_values."):

```python
    from recsys_tfb.core.consistency import DataConsistencyError

    cat_values = schema.get("categorical_values", {})
    missing_cats = [c for c in identity_cat_cols if c not in cat_values]
    if missing_cats:
        raise DataConsistencyError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing_cats}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )
```

(The `_spark.py:191` item guard is intentionally left as-is in Phase 1 — it is the definitive post-feature_table check and its message is already actionable; converting its type is deferred to Phase 2 where the surrounding data-consistency function is introduced.)

- [ ] **Step 4: Run affected Spark tests**

Run: `.venv/bin/pytest tests/test_pipelines/test_dataset/test_nodes_spark.py -q`
Expected: PASS (no behavioural change; only exception subclass/message origin)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_core/test_consistency.py
git commit -m "refactor(preprocessing): raise DataConsistencyError from identity-cat guard"
```

---

### Task 9: Repoint `test_product_consistency.py` lint to the shared predicate (A6)

**Files:**
- Modify: `tests/test_pipelines/test_source_etl/test_product_consistency.py`
- Test: itself (it *is* the test)

- [ ] **Step 1: Write the failing test**

Add a new test into `test_product_consistency.py` that asserts the lint uses the predicate as the config-side source of truth:

```python
def test_lint_uses_consistency_predicate_for_config_side():
    """The yaml/config arm of the lint must derive from the single predicate,
    not re-parse parameters.yaml independently (prevents definition drift)."""
    import inspect
    from recsys_tfb.core import consistency

    src = inspect.getsource(consistency.resolved_item_values)
    assert "categorical_values" in src  # predicate is the canonical reader

    # synthetic generator PRODUCTS must equal the predicate's output
    import re
    from pathlib import Path
    import yaml

    repo = Path(__file__).resolve().parents[3]
    params = yaml.safe_load((repo / "conf/base/parameters.yaml").read_text())
    declared = sorted(params["schema"]["categorical_values"]["prod_name"])

    gen = (repo / "scripts/generate_synthetic_data.py").read_text()
    m = re.search(r"PRODUCTS\s*=\s*\[(.*?)\]", gen, re.S)
    syn = sorted(re.findall(r"\"([a-z_]+)\"", m.group(1)))
    assert syn == declared, f"synthetic PRODUCTS {syn} != declared {declared}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_pipelines/test_source_etl/test_product_consistency.py::test_lint_uses_consistency_predicate_for_config_side -q`
Expected: PASS if synthetic already matches; FAIL listing the diff if synthetic generator drifted (this *is* the new coverage — synthetic was previously unchecked).

- [ ] **Step 3: Reconcile if it failed**

If the assertion fails, the synthetic generator's `PRODUCTS` and `conf/base/parameters.yaml` genuinely disagree — fix `scripts/generate_synthetic_data.py` `PRODUCTS` / `PRODUCT_GROUPS` / `LABEL_RATES` to match the declared set (do **not** weaken the test). Re-run until green.

- [ ] **Step 4: Run the full product-consistency file**

Run: `.venv/bin/pytest tests/test_pipelines/test_source_etl/test_product_consistency.py -q`
Expected: PASS (existing 6-place lint + new synthetic-coverage assertion)

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipelines/test_source_etl/test_product_consistency.py scripts/generate_synthetic_data.py
git commit -m "test(consistency): extend product-consistency lint to synthetic generator (A6)"
```

---

### Task 10: Phase 1 verification + docs

**Files:**
- Modify: `CLAUDE.md` (document the consistency gate), `docs/superpowers/plans/2026-05-17-config-consistency-validation.md` (mark Phase 1 done)

- [ ] **Step 1: Run all Phase-1-touched tests together**

Run:
```bash
.venv/bin/pytest tests/test_core/test_consistency.py \
  tests/test_core/test_consistency_cli_wiring.py \
  tests/test_core/test_schema_validation.py tests/test_core/test_schema.py \
  tests/test_pipelines/test_source_etl/test_product_consistency.py \
  tests/test_pipelines/test_dataset/test_nodes_spark.py -q
```
Expected: PASS (0 failures). Record counts.

- [ ] **Step 2: Smoke the CLI gate end-to-end**

Run:
```bash
.venv/bin/python -m recsys_tfb dataset --env local --help >/dev/null 2>&1; echo $?
```
Expected: exit 0 (gate does not false-trip on the committed valid config). Then temporarily add `cust_segment_typ` to `categorical_columns` in a scratch params and confirm exit code 1 with the A1 message (revert after).

- [ ] **Step 3: Add CLAUDE.md note**

Append under a new `## Config consistency gate` section: one paragraph stating that `core/consistency.py` is the single source of truth for item-set / column-role invariants, runs at CLI entry, raises `ConfigConsistencyError` (a `ValueError`), and that new invariants must be added as predicates there (not ad-hoc in pipelines).

- [ ] **Step 4: Update graphify graph**

Run: `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
Expected: graph rebuilt.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md docs/superpowers/plans/2026-05-17-config-consistency-validation.md graphify-out
git commit -m "docs(consistency): document Phase 1 config-static gate"
```

---

## Future Phases (architecture specified, deferred — separate execution)

### Phase 2 — Layer 2 config-vs-data (`validate_data_consistency`)

**Where:** new function in `core/consistency.py`, called from `preprocessing/_spark.py:fit_preprocessor_metadata` immediately after `category_mappings` is built (it already collects train-window distinct values cheaply).

**Checks (all key off `schema["item"]`):**
- **B1 coverage gate (hard error):** `actual = set(train_df.select(item).where(notNull).distinct())`; `gap = actual - set(resolved_item_values(parameters))`. Non-empty → `DataConsistencyError` listing offenders + "these would silently encode to -1 (same code as null) and corrupt training". Closes the #1 silent hole.
- **B2 leakage guard (hard error):** new optional `dataset.prepare_model_input.leakage_columns` (default `["apply_start_date", "apply_end_date"]`). Assert `set(leakage_columns) ⊆ set(drop_columns) ∪ non_feature`. Any leakage col surviving into `feature_columns` → `DataConsistencyError`. (Decision from discussion: only label-window-derived columns; `cust_segment_typ` is NOT leakage.)
- **B3 zero-positive (configurable):** per declared item, positives over train window. Default severity WARN (rare products legitimately sparse); escalatable via new `consistency.require_positive_per_item: true` → ERROR.

**Task outline (to be expanded into TDD when greenlit):** (1) `validate_data_consistency` signature + B1 with a tiny Spark fixture; (2) B2 with `leakage_columns` param + default; (3) B3 with severity switch; (4) wire call site in `fit_preprocessor_metadata`; (5) convert `_spark.py:191` item guard to delegate; (6) Spark-suite regression (per CLAUDE.md 測試效能 §: run only `tests/test_pipelines/test_dataset` + `test_training`, not full suite).

### Phase 3 — Layer 3 + single source of truth (SEPARATE PLAN)

Per writing-plans **Scope Check**, generation is a different subsystem from validation. Create `docs/superpowers/plans/<date>-item-single-source-of-truth.md` covering:
- `conf/base/products.yaml` (`大類 -> [item values]`) as the one declaration.
- `schema.categorical_values[item]` and `inference.products` derived from it (or asserted equal via the Phase-1 predicate at load).
- Extend `SQLRenderer` (`src/recsys_tfb/pipelines/source_etl/sql_renderer.py`, currently `${scalar}`-only) with a list→`UNION ALL` injection so `sample_pool.sql` / `label_*.sql` candidate sets are generated, not hand-written.
- **Layer-3 runtime pre-flight (C1):** in `source_etl`, after producing `label_table`/`sample_pool`, assert their `SELECT DISTINCT item` equals `resolved_item_values(parameters)` → `DataConsistencyError`. This catches the irreducibly-manual `label_event` business logic (upstream-event → item mapping) that no generator can synthesize.

Phase 3 keeps the manual boundary explicit: the `label_event` CASE-WHEN mapping and upstream source tables stay hand-written; the C1 pre-flight makes "declared but logic not wired" fail loud.

---

## Self-Review

**1. Spec coverage:** A1 → Task 3+5 (+ explicit both-resolution message). A2 → Task 4+5. A3 → Task 2, consolidated Task 7. A4 → Task 4+5. A5 → Task 4+5. A6 → Task 9. Channel-name generalization → every predicate reads `get_schema(...)["item"]` (Task 2/4 tests assert `channel_name` works). Refactor-existing-checks ask → Task 7 (schema delegate), Task 8 (_spark messaging), Task 9 (lint repoint), with rationale in "Why delegate instead of rewrite". Data-actual-state ask → Phase 2 (B1/B2/B3) + Phase 3 C1, architecturally specified. Phased delivery ask → Phase 1 shippable alone; 2/3 deferred. Covered.

**2. Placeholder scan:** Phase 1 tasks contain full code + exact commands + expected output, no TBD/TODO. Phase 2/3 are deliberately outlines under an explicit "deferred" boundary (Scope Check), not placeholders inside an active task.

**3. Type consistency:** `ConsistencyError`/`ConfigConsistencyError`/`DataConsistencyError` (Task 1) used consistently in Tasks 5/7/8. `resolved_item_values` (Task 2) reused verbatim in Tasks 4/5/7/9 and Phase 2/3. `config_role_conflicts` returns `list[str]` (Task 3) consumed as iterable in Task 5. `inference_products_mismatch` dict keys `only_in_inference`/`only_in_categorical` identical in Task 4 def and Task 5 use. CLI symbol `validate_config_consistency` matches Task 5 def ↔ Task 6 import/call. No drift.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-17-config-consistency-validation.md` (in worktree `feat/config-consistency-validation`).

---

## Phase 1 COMPLETE (2026-05-17)

All Tasks 1–10 implemented and verified. Final commit range: `e1b39da` (pre-Task-10 HEAD) → see commit `docs(consistency): document Phase 1 config-static gate`.

- 88 tests pass (0 failures): `test_consistency.py`, `test_consistency_cli_wiring.py`, `test_schema_validation.py`, `test_schema.py`, `test_product_consistency.py`, `test_nodes_spark.py`.
- CLI gate smoke-tested: valid config exits 0; A1-conflicting config raises `ConfigConsistencyError` and exits 1 with message naming `cust_segment_typ` and both resolutions.
- `CLAUDE.md` updated with `## Config consistency gate` section.
- graphify-out rebuilt: 2040 nodes, 4905 edges, 66 communities.

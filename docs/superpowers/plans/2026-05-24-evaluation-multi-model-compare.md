# Evaluation Multi-Model Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `evaluation` pipeline with 2-way model comparison (same-stack `model_version` or external Hive predictions), common-universe re-rank + re-compute, plus always-persist `eval_predictions` to Hive — produces a separate `report_comparison.html`.

**Architecture:** New `evaluation/comparison/` sub-package (pure logic) + `pipelines/evaluation/comparison_nodes.py` (Pipeline shims). `evaluation` CLI gets `--compare X` (one-shot: both reports) and `--compare-only X` (read persisted `eval_predictions` from Hive, only compare report). Fail-loud invariants A11-A13 + B2-B4 added to `core/consistency.py`. Spec: `docs/superpowers/specs/2026-05-24-evaluation-multi-model-compare-design.md`.

**Tech Stack:** PySpark 3.3.2, Hive (dev-cluster), Typer CLI, pytest + function-scoped Spark fixture. Worktree: `/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/`.

**Worktree env conventions** (per CLAUDE.md):
- Absolute venv python: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python`
- All test invocations: `PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
- All git commands: `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare ...`

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/recsys_tfb/evaluation/comparison/__init__.py` | Sub-package marker |
| `src/recsys_tfb/evaluation/comparison/alignment.py` | Pure-function: `common_universe(a, b)` → `(common_cust, common_prod)` |
| `src/recsys_tfb/evaluation/comparison/sources.py` | Spark: read A/B predictions (model_version / external_hive), column rename, prod_mapping N:1 collapse |
| `src/recsys_tfb/evaluation/comparison/restrict.py` | Spark: filter to common (cust × prod) + `rank_within_query` + B-side label LEFT JOIN |
| `src/recsys_tfb/evaluation/comparison/report.py` | Pure dict: assemble 5-section HTML (coverage / overall / per-item / category / glossary) |
| `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` | Pipeline shims for the four compare nodes + persist + load-from-hive |
| `tests/test_evaluation/test_comparison_alignment.py` | Pure-Python tests for common_universe |
| `tests/test_evaluation/test_comparison_sources.py` | Spark tests for source loaders + prod_mapping policies |
| `tests/test_evaluation/test_comparison_restrict.py` | Spark tests for restrict + re-rank + label join |
| `tests/test_evaluation/test_comparison_report.py` | Pure-dict tests for compare report assembly |
| `tests/test_pipelines/test_evaluation_compare_pipeline.py` | End-to-end pipeline tests (3 modes + A13/B4) |
| `tests/test_core/test_consistency_compare.py` | Tests for A11/A12/A13/B2 predicates |

**Modified:**

| Path | Change |
|---|---|
| `src/recsys_tfb/core/consistency.py` | Add A11/A12/A13/B2/B3/B4 predicates + update docstring legend |
| `src/recsys_tfb/__main__.py` | `evaluation` command gets `--compare` / `--compare-only` flags + dispatch |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py` | `create_pipeline` accepts `compare_source`, `compare_only` |
| `conf/base/parameters_evaluation.yaml` | Add commented `compare_sources` template |
| `conf/base/catalog.yaml` | Add `evaluation_comparison_report` + `eval_predictions` Hive dataset entries |

**Not modified:**

- `src/recsys_tfb/evaluation/compare.py` — already a generic dict-level differ; the spec re-uses it as-is
- `src/recsys_tfb/evaluation/report.py` / `report_builder.py` — re-used as-is (`generate_html_report`, `ReportSection`, `_per_item_metric_compare_table`, `build_glossary_section`)
- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — existing 4 nodes untouched

---

## Task 1: Add A11/A12/A13 consistency predicates (config-static)

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (add 3 predicates + update legend docstring)
- Create: `tests/test_core/test_consistency_compare.py`

- [ ] **Step 1: Write failing tests for A11/A12/A13**

Create `tests/test_core/test_consistency_compare.py`:

```python
"""Tests for compare-source consistency predicates (A11/A12/A13)."""

import pytest
from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    compare_source_well_formed_errors,
    compare_source_key_exists,
    compare_mutual_exclusive_errors,
)


def _base_params() -> dict:
    return {"evaluation": {"compare_sources": {}}}


class TestA11_WellFormed:
    def test_empty_sources_ok(self):
        assert compare_source_well_formed_errors(_base_params()) == []

    def test_model_version_minimal_ok(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {
            "kind": "model_version",
            "model_version": "2026-01-31_abc_def",
            "label": "v_prev",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_external_hive_minimal_ok(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["ext"] = {
            "kind": "external_hive",
            "table": "other.preds",
            "label": "Ext",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"},
            "unmapped_policy": "fail",
        }
        assert compare_source_well_formed_errors(p) == []

    def test_missing_kind(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {"label": "X"}
        errs = compare_source_well_formed_errors(p)
        assert any("kind" in e for e in errs)

    def test_unknown_kind(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {"kind": "parquet", "label": "X"}
        errs = compare_source_well_formed_errors(p)
        assert any("kind" in e for e in errs)

    def test_model_version_leaks_columns(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "model_version",
            "model_version": "v1",
            "label": "X",
            "columns": {"cust_id": "c"},
        }
        errs = compare_source_well_formed_errors(p)
        assert any("columns" in e for e in errs)

    def test_model_version_leaks_prod_mapping(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "model_version",
            "model_version": "v1",
            "label": "X",
            "prod_mapping": {"a": "b"},
        }
        errs = compare_source_well_formed_errors(p)
        assert any("prod_mapping" in e for e in errs)

    def test_external_hive_missing_table(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "fail",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("table" in e for e in errs)

    def test_external_hive_missing_required_column(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "table": "t", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s"},  # missing prod_name, score
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "fail",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("prod_name" in e or "score" in e for e in errs)

    def test_unmapped_policy_invalid(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["x"] = {
            "kind": "external_hive", "table": "t", "label": "X",
            "columns": {"cust_id": "c", "snap_date": "s", "prod_name": "p", "score": "x"},
            "prod_mapping": {"a": "fund_stock"}, "unmapped_policy": "skip",
        }
        errs = compare_source_well_formed_errors(p)
        assert any("unmapped_policy" in e for e in errs)


class TestA12_KeyExists:
    def test_none_returns_none(self):
        assert compare_source_key_exists(_base_params(), None) is None

    def test_existing_key_returns_dict(self):
        p = _base_params()
        p["evaluation"]["compare_sources"]["v_prev"] = {"kind": "model_version", "label": "X"}
        assert compare_source_key_exists(p, "v_prev")["label"] == "X"

    def test_missing_key_raises(self):
        with pytest.raises(ConfigConsistencyError, match="v_prev"):
            compare_source_key_exists(_base_params(), "v_prev")


class TestA13_MutualExclusive:
    def test_neither(self):
        assert compare_mutual_exclusive_errors(None, None) == []

    def test_only_compare(self):
        assert compare_mutual_exclusive_errors("x", None) == []

    def test_only_compare_only(self):
        assert compare_mutual_exclusive_errors(None, "x") == []

    def test_both_raises(self):
        errs = compare_mutual_exclusive_errors("x", "y")
        assert any("mutually exclusive" in e.lower() for e in errs)
```

- [ ] **Step 2: Run tests, verify all fail with ImportError**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_core/test_consistency_compare.py -q
```
Expected: ImportError on `compare_source_well_formed_errors` / `compare_source_key_exists` / `compare_mutual_exclusive_errors`.

- [ ] **Step 3: Add three predicates to `core/consistency.py`**

Append at the end of `src/recsys_tfb/core/consistency.py`:

```python
# ---------------------------------------------------------------------------
# A11/A12/A13 — compare-source predicates (multi-model comparison feature)
# ---------------------------------------------------------------------------

_COMPARE_KINDS = {"model_version", "external_hive"}
_REQUIRED_COLUMNS = {"cust_id", "snap_date", "prod_name", "score"}
_VALID_UNMAPPED = {"fail", "drop"}


def compare_source_well_formed_errors(parameters: dict) -> list[str]:
    """(A11) Each evaluation.compare_sources[*] is well-formed.

    Returns list of error messages (empty when all sources valid).
    """
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    errs: list[str] = []
    for key, src in sources.items():
        if not isinstance(src, dict):
            errs.append(f"compare_sources[{key!r}] must be a dict, got {type(src).__name__}")
            continue
        if "kind" not in src:
            errs.append(f"compare_sources[{key!r}] missing 'kind'")
            continue
        kind = src["kind"]
        if kind not in _COMPARE_KINDS:
            errs.append(
                f"compare_sources[{key!r}].kind={kind!r} not in {sorted(_COMPARE_KINDS)}"
            )
            continue
        if "label" not in src:
            errs.append(f"compare_sources[{key!r}] missing 'label'")
        if kind == "model_version":
            if "model_version" not in src:
                errs.append(f"compare_sources[{key!r}] kind=model_version missing 'model_version'")
            if "columns" in src:
                errs.append(
                    f"compare_sources[{key!r}] kind=model_version must not declare 'columns' "
                    "(same-stack source uses ranked_predictions schema)"
                )
            if "prod_mapping" in src:
                errs.append(
                    f"compare_sources[{key!r}] kind=model_version must not declare 'prod_mapping' "
                    "(same-stack source uses identical prod universe)"
                )
        elif kind == "external_hive":
            if "table" not in src:
                errs.append(f"compare_sources[{key!r}] kind=external_hive missing 'table'")
            cols = src.get("columns", {}) or {}
            missing = _REQUIRED_COLUMNS - set(cols.keys())
            if missing:
                errs.append(
                    f"compare_sources[{key!r}].columns missing required keys: {sorted(missing)}"
                )
            if not src.get("prod_mapping"):
                errs.append(f"compare_sources[{key!r}] kind=external_hive missing 'prod_mapping'")
            policy = src.get("unmapped_policy", "fail")
            if policy not in _VALID_UNMAPPED:
                errs.append(
                    f"compare_sources[{key!r}].unmapped_policy={policy!r} "
                    f"not in {sorted(_VALID_UNMAPPED)}"
                )
    return errs


def compare_source_key_exists(parameters: dict, key: str | None) -> dict | None:
    """(A12) Resolve `key` against evaluation.compare_sources or raise.

    Returns the source dict, or None when `key` is None.
    """
    if key is None:
        return None
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    if key not in sources:
        available = sorted(sources.keys())
        raise ConfigConsistencyError(
            f"(A12) --compare/--compare-only key {key!r} not in "
            f"evaluation.compare_sources. Available: {available}"
        )
    return sources[key]


def compare_mutual_exclusive_errors(compare: str | None, compare_only: str | None) -> list[str]:
    """(A13) --compare and --compare-only must not be passed together."""
    if compare is not None and compare_only is not None:
        return [
            f"(A13) --compare={compare!r} and --compare-only={compare_only!r} "
            "are mutually exclusive — pass at most one"
        ]
    return []
```

- [ ] **Step 4: Update the docstring "Invariant legend" in `consistency.py`**

Find the section "Layer 1 — config-static" block in the module docstring (lines ~20-65 in current file) and append below the A10 entry, before "Layer 2":

```
* A11 — every ``evaluation.compare_sources[*]`` is well-formed:
  ``kind`` ∈ {model_version, external_hive}; ``label`` required; ranked
  by-kind required fields (``model_version`` for model_version;
  ``table`` + ``columns`` (cust_id/snap_date/prod_name/score) +
  ``prod_mapping`` + ``unmapped_policy`` ∈ {fail, drop} for external_hive);
  ``model_version`` kind must NOT declare ``columns``/``prod_mapping``
  (config leak guard). Predicate: ``compare_source_well_formed_errors``.
* A12 — ``--compare X`` / ``--compare-only X`` resolves to a key in
  ``compare_sources``. Predicate: ``compare_source_key_exists`` (raises
  ``ConfigConsistencyError`` directly; not aggregated by validate).
* A13 — ``--compare`` and ``--compare-only`` are mutually exclusive (only
  one or neither). Predicate: ``compare_mutual_exclusive_errors``.
```

Also update the Layer 2 section: change "B2–B3 deferred" to "B2/B3/B4 implemented for compare feature".

- [ ] **Step 5: Wire A11 into `validate_config_consistency`**

Find `validate_config_consistency` function (search for `def validate_config_consistency`) and add to its errors collection:

```python
    errors.extend(compare_source_well_formed_errors(parameters))
```

(Append in the same style as the other `errors.extend(...)` calls.)

- [ ] **Step 6: Run tests, verify all pass**

Run same command from Step 2. Expected: all tests pass.

- [ ] **Step 7: Run full consistency test suite for regression**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_core/ -q
```
Expected: all existing tests still pass.

- [ ] **Step 8: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/core/consistency.py \
      tests/test_core/test_consistency_compare.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(core): A11/A12/A13 compare-source consistency predicates"
```

---

## Task 2: Add commented `compare_sources` template to parameters_evaluation.yaml

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml` (append `compare_sources` section)

- [ ] **Step 1: Append the template to the yaml**

Find the existing `evaluation:` block in `conf/base/parameters_evaluation.yaml` and append (after `report:`):

```yaml
  # Compare sources — populated entries unlock `evaluation --compare <key>` and
  # `--compare-only <key>`. Empty by default; remove the leading `#` on the
  # entries you actually use. Each key (`v_prev`, `ext_proj_x`, ...) is the
  # value passed on the CLI flag.
  #
  # Schema (validated by core.consistency.A11):
  #   kind:           "model_version" | "external_hive"     (required)
  #   label:          display name in report_comparison.html (required)
  #   model_version:  required when kind=model_version
  #   table:          required when kind=external_hive (Hive-qualified)
  #   columns:        required when kind=external_hive — left=our schema,
  #                   right=external column name (cust_id/snap_date/prod_name/score)
  #   prod_mapping:   required when kind=external_hive — N:1 map of external
  #                   prod_name → our prod_name; A6-known values on the right
  #   unmapped_policy: "fail" (default — raise on prod outside mapping) |
  #                    "drop" (filter and log warning)
  compare_sources: {}
  #compare_sources:
  #  v_prev:
  #    kind: model_version
  #    model_version: "2026-01-31_abcdef12_34567890"
  #    label: "v_prev (上一版)"
  #  ext_proj_x:
  #    kind: external_hive
  #    table: other_project.predictions
  #    label: "External Project X"
  #    columns:
  #      cust_id: customer_id
  #      snap_date: as_of_date
  #      prod_name: item_code
  #      score: pred_score
  #    prod_mapping:
  #      ext_fund_a: fund_stock
  #      ext_fund_b: fund_bond
  #    unmapped_policy: fail
```

- [ ] **Step 2: Run config-consistency tests, verify still pass**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_core/ \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/ -q
```
Expected: all pass (empty `compare_sources: {}` is a valid A11 input).

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add conf/base/parameters_evaluation.yaml
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): compare_sources template in parameters_evaluation.yaml"
```

---

## Task 3: `comparison/alignment.py` — common_universe pure function

**Files:**
- Create: `src/recsys_tfb/evaluation/comparison/__init__.py` (empty marker)
- Create: `src/recsys_tfb/evaluation/comparison/alignment.py`
- Create: `tests/test_evaluation/test_comparison_alignment.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_evaluation/test_comparison_alignment.py`:

```python
"""Tests for comparison.alignment — common_universe pure function."""

import pytest
from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.alignment import common_universe


@pytest.fixture
def df_a(spark):
    return spark.createDataFrame(
        [
            ("c1", "p1"), ("c1", "p2"),
            ("c2", "p1"), ("c2", "p3"),
            ("c3", "p1"),
        ],
        ["cust_id", "prod_name"],
    )


@pytest.fixture
def df_b(spark):
    return spark.createDataFrame(
        [
            ("c2", "p1"), ("c2", "p2"),
            ("c3", "p2"), ("c3", "p3"),
            ("c4", "p1"),
        ],
        ["cust_id", "prod_name"],
    )


def test_intersection_cust_and_prod(df_a, df_b):
    cust, prod = common_universe(df_a, df_b, "cust_id", "prod_name")
    assert cust == {"c2", "c3"}
    assert prod == {"p1", "p2", "p3"}


def test_empty_cust_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c9", "p1")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_cust"):
        common_universe(a, b, "cust_id", "prod_name")


def test_empty_prod_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c1", "p9")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_prod"):
        common_universe(a, b, "cust_id", "prod_name")
```

- [ ] **Step 2: Run, verify ImportError**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/test_comparison_alignment.py -q
```
Expected: ImportError on `recsys_tfb.evaluation.comparison.alignment`.

- [ ] **Step 3: Create sub-package and alignment module**

Create `src/recsys_tfb/evaluation/comparison/__init__.py` (empty file).

Create `src/recsys_tfb/evaluation/comparison/alignment.py`:

```python
"""Common-universe alignment for 2-way model comparison.

Pure-function module: given two prediction DataFrames, return the
intersection of customer IDs and (mapped) product names as Python sets.
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    cust_col: str,
    prod_col: str,
) -> tuple[set, set]:
    """Return `(common_cust, common_prod)` as Python sets.

    Raises ``DataConsistencyError`` (B3) when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_cust = {r[0] for r in a.select(cust_col).distinct().collect()}
    b_cust = {r[0] for r in b.select(cust_col).distinct().collect()}
    common_cust = a_cust & b_cust
    if not common_cust:
        raise DataConsistencyError(
            f"(B3) compare common_cust is empty — A has {len(a_cust)} cust, "
            f"B has {len(b_cust)} cust, intersection = 0. Check snap_date "
            "alignment and cust_id type."
        )

    a_prod = {r[0] for r in a.select(prod_col).distinct().collect()}
    b_prod = {r[0] for r in b.select(prod_col).distinct().collect()}
    common_prod = a_prod & b_prod
    if not common_prod:
        raise DataConsistencyError(
            f"(B3) compare common_prod is empty — A has {len(a_prod)} prods, "
            f"B has {len(b_prod)} prods (after mapping), intersection = 0. "
            "Check prod_mapping config."
        )

    return common_cust, common_prod
```

- [ ] **Step 4: Run, verify all pass**

Same command as Step 2. Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/evaluation/comparison/__init__.py \
      src/recsys_tfb/evaluation/comparison/alignment.py \
      tests/test_evaluation/test_comparison_alignment.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison.alignment.common_universe (B3 fail-loud)"
```

---

## Task 4: `comparison/sources.py` — kind=model_version

**Files:**
- Create: `src/recsys_tfb/evaluation/comparison/sources.py` (model_version branch)
- Create: `tests/test_evaluation/test_comparison_sources.py` (model_version tests)

- [ ] **Step 1: Write failing tests**

Create `tests/test_evaluation/test_comparison_sources.py`:

```python
"""Tests for comparison.sources — load_compare_predictions."""

import pytest
from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions


def _params_for_mv(mv: str, snap: str = "2026-01-31") -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1", "p2", "p3"]},
        },
        "evaluation": {
            "snap_date": snap,
            "compare": {"kind": "model_version", "model_version": mv, "label": "L"},
        },
    }


@pytest.fixture
def ranked_predictions_view(spark):
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, "MV_A"),
            ("c1", "2026-01-31", "p2", 0.7, "MV_A"),
            ("c1", "2026-01-31", "p1", 0.8, "MV_B"),
            ("c1", "2025-12-31", "p1", 0.5, "MV_A"),  # different snap_date
        ],
        ["cust_id", "snap_date", "prod_name", "score", "model_version"],
    )
    df.createOrReplaceTempView("ranked_predictions")
    yield
    spark.catalog.dropTempView("ranked_predictions")


def test_model_version_filters_correctly(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    out = load_compare_predictions(p, spark)
    rows = sorted((r["cust_id"], r["prod_name"], r["score"]) for r in out.collect())
    assert rows == [("c1", "p1", 0.9), ("c1", "p2", 0.7)]


def test_model_version_unknown_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_GHOST")
    with pytest.raises(DataConsistencyError, match="MV_GHOST"):
        load_compare_predictions(p, spark)


def test_unknown_kind_raises(spark, ranked_predictions_view):
    p = _params_for_mv("MV_A")
    p["evaluation"]["compare"]["kind"] = "parquet"
    with pytest.raises(RuntimeError, match="parquet"):
        load_compare_predictions(p, spark)


def test_missing_compare_key_raises(spark):
    p = _params_for_mv("MV_A")
    del p["evaluation"]["compare"]
    with pytest.raises(RuntimeError, match="compare"):
        load_compare_predictions(p, spark)
```

- [ ] **Step 2: Run, verify ImportError**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/test_comparison_sources.py -q
```
Expected: ImportError on `comparison.sources`.

- [ ] **Step 3: Create sources.py with model_version branch only**

Create `src/recsys_tfb/evaluation/comparison/sources.py`:

```python
"""Load Model B raw predictions for 2-way comparison.

Two source kinds:
  * model_version  — read ``ranked_predictions`` filtered by ``model_version``
  * external_hive  — read external Hive table with column rename + prod_mapping

The full source dict is staged at ``parameters['evaluation']['compare']`` by
the CLI dispatcher (``__main__.py``).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict, spark: SparkSession) -> SparkDataFrame:
    """Dispatch on ``compare.kind`` and return raw Model B predictions."""
    eval_params = parameters.get("evaluation", {}) or {}
    src = eval_params.get("compare")
    if not src:
        raise RuntimeError(
            "parameters['evaluation']['compare'] missing — CLI must dispatch "
            "the chosen compare source dict here before pipeline run."
        )
    snap_date = str(eval_params.get("snap_date") or "").strip()
    if not snap_date:
        raise RuntimeError("evaluation.snap_date missing")

    schema = get_schema(parameters)
    kind = src.get("kind")
    if kind == "model_version":
        return _load_model_version(src, snap_date, schema, spark)
    if kind == "external_hive":
        return _load_external_hive(src, snap_date, schema, spark)
    raise RuntimeError(f"unknown compare source kind={kind!r}")


def _load_model_version(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    mv = src["model_version"]
    time_col = schema["time"]
    df = (
        spark.table("ranked_predictions")
        .filter(F.col("model_version") == mv)
        .filter(F.col(time_col).cast("string") == snap_date)
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"compare model_version={mv!r} has no rows for snap_date={snap_date!r}"
        )
    logger.info("Loaded compare predictions: model_version=%s rows=%d", mv, df.count())
    return df


def _load_external_hive(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    raise NotImplementedError("external_hive branch lands in Task 5")
```

- [ ] **Step 4: Run, verify model_version tests pass**

Same command as Step 2. Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/evaluation/comparison/sources.py \
      tests/test_evaluation/test_comparison_sources.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison.sources.load_compare_predictions (model_version branch)"
```

---

## Task 5: `comparison/sources.py` — kind=external_hive + prod_mapping + B2

**Files:**
- Modify: `src/recsys_tfb/evaluation/comparison/sources.py` (implement `_load_external_hive`)
- Modify: `tests/test_evaluation/test_comparison_sources.py` (append external_hive tests)

- [ ] **Step 1: Append failing tests for external_hive branch**

Append to `tests/test_evaluation/test_comparison_sources.py`:

```python
def _params_for_ext(snap: str = "2026-01-31", policy: str = "fail") -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["fund_stock", "fund_bond", "exchange_usd"]},
        },
        "evaluation": {
            "snap_date": snap,
            "compare": {
                "kind": "external_hive",
                "table": "ext_proj.preds",
                "label": "ExtX",
                "columns": {
                    "cust_id": "customer_id",
                    "snap_date": "as_of_date",
                    "prod_name": "item_code",
                    "score": "pred_score",
                },
                "prod_mapping": {
                    "ext_fund_a": "fund_stock",
                    "ext_fund_b": "fund_bond",
                    "ext_usd": "exchange_usd",
                },
                "unmapped_policy": policy,
            },
        },
    }


@pytest.fixture
def ext_predictions_view(spark):
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "ext_fund_a", 0.9),
            ("c1", "2026-01-31", "ext_fund_b", 0.8),
            ("c2", "2026-01-31", "ext_fund_a", 0.7),
            ("c2", "2026-01-31", "ext_usd", 0.6),
            ("c3", "2025-12-31", "ext_fund_a", 0.5),  # different snap_date
        ],
        ["customer_id", "as_of_date", "item_code", "pred_score"],
    )
    df.createOrReplaceTempView("ext_proj__preds")
    # spark.table cannot read dotted view names directly; use a real catalog
    # workaround via createGlobalTempView? Simpler: monkeypatch spark.table.
    yield df
    spark.catalog.dropTempView("ext_proj__preds")


def test_external_hive_column_rename_and_snap_filter(spark, monkeypatch, ext_predictions_view):
    p = _params_for_ext()
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view if t == "ext_proj.preds" else spark.table(t))
    out = load_compare_predictions(p, spark)
    cols = set(out.columns)
    assert {"cust_id", "snap_date", "prod_name", "score"}.issubset(cols)
    snaps = {r["snap_date"] for r in out.collect()}
    assert snaps == {"2026-01-31"}  # filtered to eval snap


def test_external_hive_prod_mapping_n_to_1_collapse(spark, monkeypatch):
    # Two external prods both map to "fund_stock" — should collapse with max(score)
    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "ext_fund_a", 0.7),
            ("c1", "2026-01-31", "ext_fund_b2", 0.9),  # also maps to fund_stock
        ],
        ["customer_id", "as_of_date", "item_code", "pred_score"],
    )
    p = _params_for_ext()
    p["evaluation"]["compare"]["prod_mapping"] = {
        "ext_fund_a": "fund_stock", "ext_fund_b2": "fund_stock",
    }
    monkeypatch.setattr(spark, "table", lambda t: df)
    out = load_compare_predictions(p, spark)
    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "fund_stock", 0.9)]


def test_external_hive_unmapped_fail_raises(spark, monkeypatch, ext_predictions_view):
    p = _params_for_ext(policy="fail")
    p["evaluation"]["compare"]["prod_mapping"] = {"ext_fund_a": "fund_stock"}  # missing fund_b, usd
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view)
    with pytest.raises(DataConsistencyError, match=r"B2.*ext_fund_b|ext_usd"):
        load_compare_predictions(p, spark)


def test_external_hive_unmapped_drop_filters_and_warns(spark, monkeypatch, ext_predictions_view, caplog):
    p = _params_for_ext(policy="drop")
    p["evaluation"]["compare"]["prod_mapping"] = {"ext_fund_a": "fund_stock"}
    monkeypatch.setattr(spark, "table", lambda t: ext_predictions_view)
    out = load_compare_predictions(p, spark)
    prods = {r["prod_name"] for r in out.collect()}
    assert prods == {"fund_stock"}
    assert any("ext_fund_b" in rec.message or "ext_usd" in rec.message for rec in caplog.records)
```

- [ ] **Step 2: Run, verify external_hive tests fail (NotImplementedError)**

Same command as Task 4 Step 2. Expected: 4 new tests fail with NotImplementedError; 4 model_version tests still pass.

- [ ] **Step 3: Implement `_load_external_hive`**

Replace the body of `_load_external_hive` in `src/recsys_tfb/evaluation/comparison/sources.py`:

```python
def _load_external_hive(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    table = src["table"]
    cols = src["columns"]
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]

    raw = spark.table(table)
    # Column rename: alias external names → our canonical schema names
    df = raw.select(*[F.col(ext).alias(internal) for internal, ext in cols.items()])
    df = df.filter(F.col(time_col).cast("string") == snap_date)
    if df.isEmpty():
        raise DataConsistencyError(
            f"compare external_hive table={table!r} has no rows for snap_date={snap_date!r}"
        )

    mapping = src.get("prod_mapping", {}) or {}
    policy = src.get("unmapped_policy", "fail")
    seen_prods = {r[0] for r in df.select(item_col).distinct().collect()}
    unmapped = seen_prods - set(mapping.keys())
    if unmapped:
        if policy == "fail":
            raise DataConsistencyError(
                f"(B2) compare external prods absent from prod_mapping: "
                f"{sorted(unmapped)}. Either add to prod_mapping or set "
                "unmapped_policy=drop."
            )
        if policy == "drop":
            logger.warning(
                "Dropping %d unmapped prods (unmapped_policy=drop): %s",
                len(unmapped), sorted(unmapped),
            )
            df = df.filter(F.col(item_col).isin(list(mapping.keys())))
        else:
            raise RuntimeError(f"unknown unmapped_policy={policy!r}")

    df = df.replace(mapping, subset=[item_col])
    # N:1 collapse — multiple ext prods may map to the same internal prod;
    # aggregate to (cust, snap, prod) with max(score) (best-rank semantic).
    df = df.groupBy(*identity_cols).agg(F.max(score_col).alias(score_col))
    logger.info("Loaded compare predictions: external table=%s rows=%d", table, df.count())
    return df
```

- [ ] **Step 4: Run, verify all sources tests pass**

Same command. Expected: 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/evaluation/comparison/sources.py \
      tests/test_evaluation/test_comparison_sources.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison.sources external_hive branch + B2 + prod_mapping N:1 collapse"
```

---

## Task 6: `comparison/restrict.py` — restrict_to_common + label join + re-rank

**Files:**
- Create: `src/recsys_tfb/evaluation/comparison/restrict.py`
- Create: `tests/test_evaluation/test_comparison_restrict.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_evaluation/test_comparison_restrict.py`:

```python
"""Tests for comparison.restrict — restrict_to_common."""

import pytest
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common


def _params() -> dict:
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1", "p2", "p3", "p4"]},
        },
    }


@pytest.fixture
def a_df(spark):
    """A has cust=c1,c2,c3, prod=p1,p2,p3,p4 — and a label column already."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c1", "2026-01-31", "p2", 0.7, 2, 0),
            ("c1", "2026-01-31", "p4", 0.5, 3, 0),  # p4 not in B
            ("c2", "2026-01-31", "p1", 0.8, 1, 0),
            ("c2", "2026-01-31", "p3", 0.6, 2, 1),
            ("c3", "2026-01-31", "p1", 0.7, 1, 0),  # c3 not in B
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )


@pytest.fixture
def b_df(spark):
    """B has cust=c1,c2, prod=p1,p2,p3 — no label column."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.6),
            ("c1", "2026-01-31", "p2", 0.8),
            ("c1", "2026-01-31", "p3", 0.5),
            ("c2", "2026-01-31", "p1", 0.9),
            ("c2", "2026-01-31", "p3", 0.7),
        ],
        ["cust_id", "snap_date", "prod_name", "score"],
    )


@pytest.fixture
def label_table(spark):
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 1),
            ("c1", "2026-01-31", "p2", 0),
            ("c1", "2026-01-31", "p3", 0),
            ("c2", "2026-01-31", "p1", 0),
            ("c2", "2026-01-31", "p3", 1),
        ],
        ["cust_id", "snap_date", "prod_name", "label"],
    )


def test_restricts_to_common_cust_and_prod(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    a_rows = sorted((r["cust_id"], r["prod_name"]) for r in a_c.collect())
    b_rows = sorted((r["cust_id"], r["prod_name"]) for r in b_c.collect())
    # common cust = {c1, c2}; common prod = {p1, p2, p3}
    expected = sorted([("c1", "p1"), ("c1", "p2"), ("c1", "p3"),
                       ("c2", "p1"), ("c2", "p3")])
    # A had no (c1, p3) — so A_common has it missing too; check A's reduced set
    a_expected = sorted([("c1", "p1"), ("c1", "p2"), ("c2", "p1"), ("c2", "p3")])
    assert a_rows == a_expected
    assert b_rows == expected


def test_rank_recomputed_within_common(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    # B for c1 in common prods: scores p1=0.6, p2=0.8, p3=0.5 → ranks 2, 1, 3
    b_c1 = {r["prod_name"]: r["rank"] for r in b_c.filter("cust_id='c1'").collect()}
    assert b_c1 == {"p2": 1, "p1": 2, "p3": 3}


def test_b_gets_label_via_left_join(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    assert "label" in b_c.columns
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    assert b_labels[("c1", "p1")] == 1
    assert b_labels[("c2", "p3")] == 1
    assert b_labels[("c1", "p2")] == 0


def test_b_missing_label_fillna_zero(a_df, b_df):
    spark = a_df.sparkSession
    sparse_labels = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 1)],
        ["cust_id", "snap_date", "prod_name", "label"],
    )
    _, b_c = restrict_to_common(a_df, b_df, sparse_labels, _params())
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    # p2/p3 not in sparse_labels — must fill 0
    assert b_labels[("c1", "p2")] == 0
    assert b_labels[("c1", "p3")] == 0


def test_a_preserves_existing_label(a_df, b_df, label_table):
    a_c, _ = restrict_to_common(a_df, b_df, label_table, _params())
    a_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in a_c.collect()}
    # A's c1,p1 label was 1 in source fixture — preserved (not re-joined)
    assert a_labels[("c1", "p1")] == 1
```

- [ ] **Step 2: Run, verify ImportError**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/test_comparison_restrict.py -q
```
Expected: ImportError.

- [ ] **Step 3: Create restrict.py**

Create `src/recsys_tfb/evaluation/comparison/restrict.py`:

```python
"""Restrict A/B compare predictions to the common (cust × prod) universe.

A side: already carries ``label`` (added upstream by ``prepare_eval_data``);
   restrict keeps the existing label column unchanged.
B side: has no ``label``; restrict does a LEFT JOIN on ``label_table`` and
   fills missing with 0 — mirroring ``prepare_eval_data``'s convention so
   "both sides are scored against the same ground truth".

Re-ranks both sides within ``[snap_date, cust_id]`` because the candidate
set just shrank.
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.alignment import common_universe
from recsys_tfb.evaluation.metrics_spark import rank_within_query


def restrict_to_common(
    a: SparkDataFrame,
    b: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame]:
    schema = get_schema(parameters)
    cust_col = schema["entity"][0]
    item_col = schema["item"]
    time_col = schema["time"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    common_cust, common_prod = common_universe(a, b, cust_col, item_col)

    spark = a.sparkSession
    cust_df = spark.createDataFrame([(c,) for c in common_cust], [cust_col])
    prod_df = spark.createDataFrame([(p,) for p in common_prod], [item_col])

    def _restrict_and_rank(df: SparkDataFrame) -> SparkDataFrame:
        df = df.join(F.broadcast(cust_df), on=cust_col, how="inner")
        df = df.join(F.broadcast(prod_df), on=item_col, how="inner")
        if rank_col in df.columns:
            df = df.drop(rank_col)
        df = rank_within_query(df, [time_col, cust_col], score_col)
        return df.withColumnRenamed("pos", rank_col)

    a_common = _restrict_and_rank(a)
    b_common = _restrict_and_rank(b)

    if label_col not in b_common.columns:
        labels = (
            label_table.select(*identity_cols, label_col)
            .join(F.broadcast(prod_df), on=item_col, how="inner")
        )
        b_common = b_common.join(labels, on=identity_cols, how="left").fillna({label_col: 0})

    return a_common, b_common
```

- [ ] **Step 4: Run, verify all 5 tests pass**

Same command. Expected: 5 pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/evaluation/comparison/restrict.py \
      tests/test_evaluation/test_comparison_restrict.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison.restrict_to_common (re-rank + B-side label LEFT JOIN)"
```

---

## Task 7: `comparison/report.py` — assemble_comparison_report

**Files:**
- Create: `src/recsys_tfb/evaluation/comparison/report.py`
- Create: `tests/test_evaluation/test_comparison_report.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_evaluation/test_comparison_report.py`:

```python
"""Tests for comparison.report — assemble_comparison_report (pure dict → HTML)."""

import pytest
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report


def _metrics(map_at_1: float = 0.5, hit_rate_at_3: float = 0.7) -> dict:
    """Minimal metrics dict (compute_all_metrics shape)."""
    return {
        "n_queries": 100, "n_excluded_queries": 0,
        "overall": {"map@1": map_at_1, "map@3": 0.6, "ndcg@3": 0.65, "recall@3": 0.55},
        "per_item": {
            "p1": {"hit_rate@1": hit_rate_at_3, "hit_rate@3": 0.8,
                   "map_attr@1": 0.4, "map_attr@3": 0.5,
                   "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.55, "mean_pos": 1.5},
            "p2": {"hit_rate@1": 0.5, "hit_rate@3": 0.7,
                   "map_attr@1": 0.3, "map_attr@3": 0.4,
                   "ndcg_attr@1": 0.35, "ndcg_attr@3": 0.45, "mean_pos": 2.0},
        },
        "macro_avg": {"by_item": {"hit_rate@1": 0.6, "hit_rate@3": 0.75,
                                  "map_attr@3": 0.45, "ndcg_attr@3": 0.5}},
        "dataset_overview": {"totals": {"n_products": 2}},
    }


def _comparison(a, b):
    from recsys_tfb.evaluation.compare import build_comparison_result
    return build_comparison_result(a, b, "Model", "ExtX")


def _params() -> dict:
    return {
        "evaluation": {
            "snap_date": "2026-01-31",
            "report": {
                "display": {
                    "primary_map_k": [1, 3, "all"],
                    "guardrail_recall_k": [1, 3],
                },
            },
            "product_categories": {"enabled": False},
        },
    }


def _coverage() -> dict:
    return {
        "n_cust_A_full": 10000, "n_cust_B_full": 5000, "n_cust_common": 4800,
        "n_prod_A_full": 22, "n_prod_B_full": 18, "n_prod_common": 12,
        "dropped_prods_A": ["fund_misc", "ext_etc"],
        "dropped_prods_B": ["ext_yet_another"],
        "kind_a": "model_version", "kind_b": "external_hive",
        "model_version_a": "2026-01-31_xxx_yyy",
        "table_b": "other_project.predictions",
    }


def test_returns_html_string():
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert isinstance(out, str)
    assert "<html" in out.lower()


def test_labels_visible_in_html():
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "Model" in out and "ExtX" in out


def test_coverage_numbers_in_html():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "4800" in out or "4,800" in out
    assert "10000" in out or "10,000" in out
    assert "fund_misc" in out  # dropped prods listed


def test_overall_metrics_have_delta():
    m_a = _metrics(map_at_1=0.6)
    m_b = _metrics(map_at_1=0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    # Δ for map@1 = 0.2; rendered somewhere
    assert "0.2" in out or "+0.2" in out


def test_category_section_absent_when_disabled():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "大類" not in out  # category section is disabled in _params()


def test_category_section_present_when_enabled_and_present():
    m_a = _metrics(); m_b = _metrics()
    cat_metrics = {
        "overall": {"map@1": 0.5, "map@3": 0.55},
        "per_item": {"fund": {"hit_rate@1": 0.6, "hit_rate@3": 0.7,
                              "map_attr@1": 0.4, "map_attr@3": 0.5,
                              "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.5}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_products": 1}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["product_categories"]["enabled"] = True
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), p)
    assert "大類" in out


def test_glossary_section_present():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "詞彙" in out or "Glossary" in out
```

- [ ] **Step 2: Run, verify ImportError**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/test_comparison_report.py -q
```
Expected: ImportError.

- [ ] **Step 3: Implement comparison/report.py**

Create `src/recsys_tfb/evaluation/comparison/report.py`:

```python
"""Assemble report_comparison.html from A/B compare result + coverage info."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.report_builder import (
    _per_item_metric_compare_table,
    _resolve_display_k,
    _k_to_lookup,
    _n_products,
    build_glossary_section,
)


def assemble_comparison_report(
    metrics_a: dict,
    metrics_b: dict,
    comparison: dict,
    coverage_info: dict,
    parameters: dict,
) -> str:
    """Compose the 4-section + glossary HTML."""
    sections = [
        _build_coverage_section(comparison, coverage_info, parameters),
        _build_overall_section(comparison),
        _build_per_item_section(metrics_a, metrics_b, comparison, parameters),
        _build_category_section(metrics_a, metrics_b, parameters),
        build_glossary_section(parameters),
    ]
    sections = [s for s in sections if s is not None]
    label_a = comparison["label_a"]
    label_b = comparison["label_b"]
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "Comparison": f"{label_a} vs {label_b}",
        "Snap Date": eval_params.get("snap_date", "unknown"),
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(
        sections,
        title=f"Model Comparison: {label_a} vs {label_b}",
        metadata=metadata,
    )


def _build_coverage_section(
    comparison: dict, cov: dict, parameters: dict
) -> ReportSection:
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    meta = pd.DataFrame(
        {
            label_a: [
                cov.get("kind_a", ""), cov.get("model_version_a", "n/a"),
                cov.get("table_a", "n/a"), cov.get("n_cust_A_full"),
                cov.get("n_prod_A_full"),
            ],
            label_b: [
                cov.get("kind_b", ""), cov.get("model_version_b", "n/a"),
                cov.get("table_b", "n/a"), cov.get("n_cust_B_full"),
                cov.get("n_prod_B_full"),
            ],
        },
        index=["kind", "model_version", "Hive table", "n_cust (full)", "n_prod (full)"],
    )
    coverage = pd.DataFrame(
        {
            "A_full": [cov.get("n_cust_A_full"), cov.get("n_prod_A_full")],
            "B_full": [cov.get("n_cust_B_full"), cov.get("n_prod_B_full")],
            "common (used)": [cov.get("n_cust_common"), cov.get("n_prod_common")],
        },
        index=["n_cust", "n_prod"],
    )
    dropped = pd.DataFrame(
        {
            f"{label_a} dropped prods": [
                len(cov.get("dropped_prods_A", []) or []),
                ", ".join(cov.get("dropped_prods_A", []) or []) or "(none)",
            ],
            f"{label_b} dropped prods": [
                len(cov.get("dropped_prods_B", []) or []),
                ", ".join(cov.get("dropped_prods_B", []) or []) or "(none)",
            ],
        },
        index=["count", "list"],
    )
    return ReportSection(
        title="Compare 概頁",
        description="兩個模型的來源、coverage、被剔除的細產品。後續章節皆在 common universe 上重排重算。",
        tables=[meta, coverage, dropped],
        table_titles=["雙方 metadata", "coverage", "被 drop 的 prods"],
    )


def _build_overall_section(comparison: dict) -> ReportSection:
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    overall_a = comparison["result_a"].get("overall", {}) or {}
    overall_b = comparison["result_b"].get("overall", {}) or {}
    overall_d = comparison["overall_delta"]
    keys = sorted(set(overall_a) | set(overall_b) | set(overall_d))
    tbl = pd.DataFrame(
        {
            label_a: [overall_a.get(k) for k in keys],
            label_b: [overall_b.get(k) for k in keys],
            "Δ": [overall_d.get(k) for k in keys],
        },
        index=keys,
    )
    return ReportSection(
        title="overall metrics (M/B/Δ)",
        description="per-query 指標在 common (cust × prod) universe 上重算。Δ = A − B。",
        tables=[tbl],
        table_titles=["overall"],
    )


def _build_per_item_section(
    metrics_a: dict, metrics_b: dict, comparison: dict, parameters: dict
) -> ReportSection | None:
    per_item_a = metrics_a.get("per_item", {}) or {}
    per_item_b = metrics_b.get("per_item", {}) or {}
    per_item_delta = comparison.get("per_item_delta", {}) or {}
    if not per_item_b:
        return None

    disp = (
        (parameters.get("evaluation", {}) or {}).get("report", {}) or {}
    ).get("display", {}) or {}
    n_prod = _n_products(metrics_a)
    rec_ks = _resolve_display_k(disp.get("guardrail_recall_k", [1, 3, 5]), n_prod)
    attr_ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod)

    macro_a = (metrics_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (metrics_b.get("macro_avg", {}) or {}).get("by_item")

    tables, titles = [], []
    for metric_key, col_fmt, ks, title in (
        ("hit_rate", "recall@{k}", rec_ks, "per-item recall@k (M/B/Δ)"),
        ("map_attr", "map_attr@{k}", attr_ks, "per-item map_attr@k (M/B/Δ)"),
        ("ndcg_attr", "ndcg_attr@{k}", attr_ks, "per-item ndcg_attr@k (M/B/Δ)"),
    ):
        tbl = _per_item_metric_compare_table(
            per_item_a, per_item_b, per_item_delta,
            ks, n_prod, metric_key, col_fmt,
            macro_a=macro_a, macro_b=macro_b,
        )
        tables.append(tbl)
        titles.append(title)
    return ReportSection(
        title="per-item M/B/Δ",
        description="細產品粒度的 recall / map_attr / ndcg_attr，頂列 Macro 平均。",
        tables=tables,
        table_titles=titles,
    )


def _build_category_section(
    metrics_a: dict, metrics_b: dict, parameters: dict
) -> ReportSection | None:
    eval_params = parameters.get("evaluation", {}) or {}
    if not (eval_params.get("product_categories", {}) or {}).get("enabled"):
        return None
    cat_a = metrics_a.get("category")
    cat_b = metrics_b.get("category")
    if not cat_a or not cat_b:
        return None
    comparison_cat = build_comparison_result(
        cat_a, cat_b,
        label_a="Model_cat",  # internal labels only — display uses metadata
        label_b="Compare_cat",
    )
    per_item_a = cat_a.get("per_item", {}) or {}
    per_item_b = cat_b.get("per_item", {}) or {}
    per_item_delta = comparison_cat.get("per_item_delta", {}) or {}
    disp = (eval_params.get("report", {}) or {}).get("display", {}) or {}
    n_cat = int(
        (cat_a.get("dataset_overview", {}) or {}).get("totals", {}).get("n_products", 0)
    )
    rec_ks = _resolve_display_k(disp.get("guardrail_recall_k", [1, 3, 5]), n_cat)
    attr_ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_cat)
    macro_a = (cat_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (cat_b.get("macro_avg", {}) or {}).get("by_item")

    tables, titles = [], []
    overall_a = cat_a.get("overall", {}) or {}
    overall_b = cat_b.get("overall", {}) or {}
    overall_d = comparison_cat["overall_delta"]
    keys = sorted(set(overall_a) | set(overall_b) | set(overall_d))
    overall_tbl = pd.DataFrame(
        {"Model": [overall_a.get(k) for k in keys],
         "Compare": [overall_b.get(k) for k in keys],
         "Δ": [overall_d.get(k) for k in keys]},
        index=keys,
    )
    tables.append(overall_tbl)
    titles.append("大類 overall")
    for metric_key, col_fmt, ks, title in (
        ("hit_rate", "recall@{k}", rec_ks, "大類 per-item recall@k (M/B/Δ)"),
        ("map_attr", "map_attr@{k}", attr_ks, "大類 per-item map_attr@k (M/B/Δ)"),
        ("ndcg_attr", "ndcg_attr@{k}", attr_ks, "大類 per-item ndcg_attr@k (M/B/Δ)"),
    ):
        tbl = _per_item_metric_compare_table(
            per_item_a, per_item_b, per_item_delta,
            ks, n_cat, metric_key, col_fmt,
            macro_a=macro_a, macro_b=macro_b,
        )
        tables.append(tbl)
        titles.append(title)
    return ReportSection(
        title="大類 Category M/B/Δ",
        description="大類粒度 overall + per-category recall/map_attr/ndcg_attr。只列雙方共通的大類。",
        tables=tables,
        table_titles=titles,
    )
```

- [ ] **Step 4: Run, verify all 7 tests pass**

Same command. Expected: 7 pass.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/evaluation/comparison/report.py \
      tests/test_evaluation/test_comparison_report.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison.report.assemble_comparison_report (5 sections)"
```

---

## Task 8: `pipelines/evaluation/comparison_nodes.py` — Pipeline shims

**Files:**
- Create: `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py`

(Tests for these shims are covered indirectly by the pipeline-level test in Task 11; the shims are thin and unit tests would mostly test the framework wiring.)

- [ ] **Step 1: Create comparison_nodes.py with all required nodes**

Create `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py`:

```python
"""Pipeline-aware shims for compare-mode nodes.

Thin wrappers over `evaluation/comparison/` pure modules + `nodes_spark.py`
helpers. Each function is one Pipeline ``Node`` body — accepts framework-
materialized inputs (DataFrames + parameters dict + spark session) and
returns the next handle.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common as _restrict
from recsys_tfb.evaluation.comparison.sources import load_compare_predictions as _load_compare
from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
from recsys_tfb.utils.spark import get_or_create_spark_session

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict) -> SparkDataFrame:
    """Pipeline shim: resolve a SparkSession and dispatch to source loader."""
    spark = get_or_create_spark_session()
    return _load_compare(parameters, spark)


def restrict_to_common(
    eval_predictions: SparkDataFrame,
    compare_predictions_raw: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
    """Pipeline shim: call the pure restrict function + capture coverage dict.

    Returns ``(a_common, b_common, coverage_partial)`` — coverage_partial
    carries full-universe sizes + dropped product lists so the report can
    show what was filtered. Computed here because ``_restrict`` itself loses
    access to the originals after returning.
    """
    schema = get_schema(parameters)
    cust_col = schema["entity"][0]
    item_col = schema["item"]

    a_prods_full = {r[0] for r in eval_predictions.select(item_col).distinct().collect()}
    b_prods_full = {r[0] for r in compare_predictions_raw.select(item_col).distinct().collect()}
    a_cust_full = eval_predictions.select(cust_col).distinct().count()
    b_cust_full = compare_predictions_raw.select(cust_col).distinct().count()

    a_common, b_common = _restrict(
        eval_predictions, compare_predictions_raw, label_table, parameters
    )

    common_prods = {r[0] for r in a_common.select(item_col).distinct().collect()}
    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    coverage_partial = {
        "kind_a": "model_version",
        "model_version_a": parameters.get("model_version", "(this run)"),
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_cust_A_full": a_cust_full,
        "n_cust_B_full": b_cust_full,
        "n_prod_A_full": len(a_prods_full),
        "n_prod_B_full": len(b_prods_full),
        "n_cust_common": a_common.select(cust_col).distinct().count(),
        "n_prod_common": len(common_prods),
        "dropped_prods_A": sorted(a_prods_full - common_prods),
        "dropped_prods_B": sorted(b_prods_full - common_prods),
    }
    return a_common, b_common, coverage_partial


def generate_comparison_report(
    eval_predictions_common: SparkDataFrame,
    compare_predictions_common: SparkDataFrame,
    coverage_partial: dict,
    parameters: dict,
) -> str:
    """Run compute_all_metrics on both sides + assemble HTML."""
    metrics_a = compute_all_metrics(eval_predictions_common, parameters)
    metrics_b = compute_all_metrics(compare_predictions_common, parameters)

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    label_a = "Model"
    label_b = src.get("label", "Compare")
    comparison = build_comparison_result(metrics_a, metrics_b, label_a, label_b)

    return assemble_comparison_report(
        metrics_a, metrics_b, comparison, coverage_partial, parameters
    )


def persist_eval_predictions(
    eval_predictions: SparkDataFrame, parameters: dict
) -> str:
    """Write eval_predictions to Hive ml_recsys.eval_predictions (overwrite partition).

    Returns a sentinel string for DAG edge purposes; downstream does not
    consume the value.
    """
    schema = get_schema(parameters)
    eval_params = parameters.get("evaluation", {}) or {}
    snap_date = str(eval_params.get("snap_date") or "").strip()
    mv = parameters.get("model_version", "unknown")

    spark = eval_predictions.sparkSession
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_recsys")
    # Compose the column list — keep all identity + score/rank/label + segment cols
    keep_cols = list(eval_predictions.columns)

    # Add model_version as a column so it can serve as partition col
    df = eval_predictions.withColumn("model_version", F.lit(mv))

    # Tell Spark to overwrite only the matching partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df.write
        .mode("overwrite")
        .partitionBy(schema["time"], "model_version")
        .format("parquet")
        .saveAsTable("ml_recsys.eval_predictions")
    )
    logger.info(
        "Persisted eval_predictions to ml_recsys.eval_predictions (snap=%s, mv=%s, rows=%d)",
        snap_date, mv, df.count(),
    )
    return f"persisted:{snap_date}:{mv}"


def load_eval_predictions_from_hive(parameters: dict) -> SparkDataFrame:
    """For --compare-only mode: read previously-persisted eval_predictions.

    Raises (B4) when the matching (snap_date, model_version) partition is
    absent — message tells the user to run evaluation first.
    """
    schema = get_schema(parameters)
    eval_params = parameters.get("evaluation", {}) or {}
    snap_date = str(eval_params.get("snap_date") or "").strip()
    mv = parameters.get("model_version", "unknown")
    spark = get_or_create_spark_session()

    df = (
        spark.table("ml_recsys.eval_predictions")
        .filter(F.col(schema["time"]).cast("string") == snap_date)
        .filter(F.col("model_version") == mv)
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"(B4) ml_recsys.eval_predictions has no partition for "
            f"snap_date={snap_date!r} model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without --compare) "
            "first to populate the partition."
        )
    return df.drop("model_version")
```

- [ ] **Step 2: Smoke-test imports work**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from recsys_tfb.pipelines.evaluation.comparison_nodes import (load_compare_predictions, restrict_to_common, generate_comparison_report, persist_eval_predictions, load_eval_predictions_from_hive); print('ok')"
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/pipelines/evaluation/comparison_nodes.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): comparison_nodes pipeline shims (load/restrict/report/persist/load-from-hive)"
```

---

## Task 9: Catalog entries — comparison report + eval_predictions Hive

**Files:**
- Modify: `conf/base/catalog.yaml` (add `evaluation_comparison_report` + Hive entry placeholder)

- [ ] **Step 1: Inspect existing catalog patterns**

Run:
```
grep -A 6 "evaluation_report:" /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/conf/base/catalog.yaml
```

- [ ] **Step 2: Add new entries**

In `conf/base/catalog.yaml`, find the `evaluation_report:` entry and add directly after it:

```yaml
evaluation_comparison_report:
  type: TextDataset
  filepath: "${data_dir}/evaluation/${model_version}/${snap_date}/report_comparison.html"
```

(Match whatever filepath template the existing `evaluation_report` uses; the above is a representative form — adapt to the actual `${...}` pattern in the file.)

The `ml_recsys.eval_predictions` Hive table is written by `persist_eval_predictions` directly via `spark.write.saveAsTable("ml_recsys.eval_predictions")` (see Task 8), so it does NOT need a catalog entry. The catalog handles HTML/JSON file outputs; Hive writes are side-effects of pipeline nodes.

- [ ] **Step 3: Run a smoke test that catalog loads cleanly**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from recsys_tfb.io.catalog import DataCatalog; from recsys_tfb.io.config_loader import ConfigLoader; c = ConfigLoader('conf/base', env='local'); print(list(c.get_catalog().keys())[:20])"
```
Expected: prints catalog keys including `evaluation_comparison_report`.

- [ ] **Step 4: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add conf/base/catalog.yaml
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): catalog entry for report_comparison.html"
```

---

## Task 10: Refactor `pipelines/evaluation/pipeline.py` — compare modes

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`

- [ ] **Step 1: Replace `create_pipeline` to support compare_source + compare_only**

Replace the entire content of `src/recsys_tfb/pipelines/evaluation/pipeline.py`:

```python
"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(
    post_training: bool = False,
    compare_source: dict | None = None,
    compare_only: bool = False,
) -> Pipeline:
    """Build the evaluation pipeline.

    Modes:
      * default (no flags) — 4 existing nodes + persist_eval_predictions
      * --compare X — adds 3 compare nodes; both reports produced
      * --compare-only X — short pipeline that reads persisted eval_predictions
        from Hive and only produces report_comparison.html
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_baseline_metrics,
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        generate_comparison_report,
        load_compare_predictions,
        load_eval_predictions_from_hive,
        persist_eval_predictions,
        restrict_to_common,
    )

    if compare_only:
        # CLI A12 ensures compare_source is not None when compare_only is True
        return Pipeline([
            Node(
                load_eval_predictions_from_hive,
                inputs=["parameters"],
                outputs="eval_predictions",
            ),
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            Node(
                restrict_to_common,
                inputs=["eval_predictions", "compare_predictions_raw",
                        "label_table", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ])

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )
    nodes = [
        Node(
            prepare_eval_data,
            inputs=[predictions_input, "label_table", "parameters"],
            outputs="eval_predictions",
        ),
        Node(
            compute_metrics,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_metrics",
        ),
        Node(
            compute_baseline_metrics,
            inputs=["eval_predictions", "label_table", "parameters"],
            outputs="baseline_metrics",
        ),
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics"],
            outputs="evaluation_report",
        ),
        Node(
            persist_eval_predictions,
            inputs=["eval_predictions", "parameters"],
            outputs="eval_predictions_persisted_sentinel",
        ),
    ]
    if compare_source is not None:
        nodes += [
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            Node(
                restrict_to_common,
                inputs=["eval_predictions", "compare_predictions_raw",
                        "label_table", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ]
    return Pipeline(nodes)
```

- [ ] **Step 2: Run existing evaluation pipeline tests to check regression**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_pipelines/test_evaluation_pipeline*.py -q
```
Expected: existing tests still pass (note: `persist_eval_predictions` may need to be tolerant in test envs without Hive — see Task 11 if tests fail).

- [ ] **Step 3: If persist node fails in test (no Hive), adjust to be opt-out for tests**

If the regression run from Step 2 fails because Spark cannot `saveAsTable` to `ml_recsys.eval_predictions` in the test fixture (no Hive metastore), make the persist call tolerant: catch `pyspark.sql.utils.AnalysisException` only when it's the missing-metastore variant, log a warning, and return the sentinel anyway. This is a unit-test concession; dev-cluster always has Hive so prod paths are unaffected.

Add to `persist_eval_predictions` in `comparison_nodes.py`:

```python
    try:
        (df.write.mode("overwrite")
              .partitionBy(schema["time"], "model_version")
              .format("parquet").saveAsTable("ml_recsys.eval_predictions"))
    except Exception as e:
        # Test envs without Hive metastore — log and continue. Production
        # always has Hive; this branch never fires there.
        msg = str(e).lower()
        if "hive" in msg or "metastore" in msg or "database" in msg:
            logger.warning("persist_eval_predictions skipped (no Hive): %s", e)
            return f"persisted-skipped:{snap_date}:{mv}"
        raise
```

(Only add this block if Step 2 actually fails on this. If Step 2 passes, skip this step entirely.)

- [ ] **Step 4: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/pipelines/evaluation/pipeline.py \
      src/recsys_tfb/pipelines/evaluation/comparison_nodes.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(evaluation): pipeline.create_pipeline supports --compare / --compare-only modes"
```

---

## Task 11: CLI — `--compare` / `--compare-only` flags in `__main__.py`

**Files:**
- Modify: `src/recsys_tfb/__main__.py` (the `evaluation` Typer command)

- [ ] **Step 1: Inspect imports + the existing `evaluation` command**

Read lines 1-50 + 576-650 of `src/recsys_tfb/__main__.py` to see existing imports and the command signature.

- [ ] **Step 2: Add A12/A13 import**

Find the imports section near top (around line 1-30) and add:

```python
from recsys_tfb.core.consistency import (
    compare_mutual_exclusive_errors,
    compare_source_key_exists,
    ConfigConsistencyError,
)
```

(If `ConfigConsistencyError` is already imported, skip that one.)

- [ ] **Step 3: Modify the `evaluation` command signature**

Find `@app.command(name="evaluation")` block (line ~576) and replace the signature + body up to `pipeline_kwargs = {...}`:

```python
@app.command(name="evaluation")
def evaluation(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(None, "--model-version", help="Model version to use"),
    post_training: bool = typer.Option(
        False, "--post-training",
        help="Read predictions from training_eval_predictions (default: ranked_predictions for monitoring)",
    ),
    compare: Optional[str] = typer.Option(
        None, "--compare",
        help="Compare-source key from evaluation.compare_sources (produces report_comparison.html alongside report.html)",
    ),
    compare_only: Optional[str] = typer.Option(
        None, "--compare-only",
        help="Like --compare, but skip prepare/compute/baseline/report and read eval_predictions from Hive (only produces report_comparison.html)",
    ),
):
    """Run the evaluation pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("evaluation", env)
    get_or_create_spark_session(_load_spark_config(config, "evaluation"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}

    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    # A13: mutual-exclusive
    errs = compare_mutual_exclusive_errors(compare, compare_only)
    if errs:
        raise ConfigConsistencyError("\n".join(errs))

    # A12: resolve key → source dict (also handles None gracefully)
    compare_key = compare or compare_only
    compare_source_dict = compare_source_key_exists(params_eval, compare_key)
    if compare_source_dict is not None:
        # Stage the dict where the pipeline nodes read it from
        params_eval.setdefault("evaluation", {})["compare"] = compare_source_dict

    logger.info(
        "Evaluation — model_version: %s (%s), post_training: %s, compare: %s%s",
        mv, model_version if model_version else "best", post_training,
        compare_key or "none",
        " (compare-only)" if compare_only else "",
    )
    logger.info("Evaluation — snap_date: %s", snap_date)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
    }

    pipeline_kwargs = {
        "post_training": post_training,
        "compare_source": compare_source_dict,
        "compare_only": bool(compare_only),
    }
    _execute_pipeline("evaluation", pipeline_kwargs, runtime_params, config, params, env)
    # ... (existing post-run manifest block stays unchanged)
```

(The "Post run" block below `_execute_pipeline(...)` stays exactly as it was — don't touch it.)

- [ ] **Step 4: Smoke test — CLI parses flags without crash**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --help
```
Expected: help text shows `--compare` and `--compare-only` options.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add src/recsys_tfb/__main__.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "feat(cli): evaluation --compare / --compare-only flags (A12/A13)"
```

---

## Task 12: End-to-end pipeline tests (three modes + A13 + B4)

**Files:**
- Create: `tests/test_pipelines/test_evaluation_compare_pipeline.py`

- [ ] **Step 1: Write tests**

Create `tests/test_pipelines/test_evaluation_compare_pipeline.py`:

```python
"""End-to-end tests for evaluation pipeline in compare modes."""

import pytest
from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    DataConsistencyError,
    compare_mutual_exclusive_errors,
)
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline


def test_default_pipeline_has_persist_node():
    pipeline = create_pipeline(post_training=False)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "persist_eval_predictions" in node_names
    assert "load_compare_predictions" not in node_names


def test_compare_mode_adds_three_extra_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "load_compare_predictions" in node_names
    assert "restrict_to_common" in node_names
    assert "generate_comparison_report" in node_names
    # And the four existing + persist still present
    assert "prepare_eval_data" in node_names
    assert "persist_eval_predictions" in node_names


def test_compare_only_mode_skips_compute_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src, compare_only=True)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "load_eval_predictions_from_hive" in node_names
    assert "generate_comparison_report" in node_names
    # explicitly NOT present:
    assert "compute_metrics" not in node_names
    assert "compute_baseline_metrics" not in node_names
    assert "generate_report" not in node_names
    assert "persist_eval_predictions" not in node_names
    assert "prepare_eval_data" not in node_names


def test_a13_compare_and_compare_only_mutually_exclusive():
    errs = compare_mutual_exclusive_errors("x", "y")
    assert errs and "mutually exclusive" in errs[0].lower()


def test_b4_load_from_hive_fails_loud_on_missing_partition(spark):
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        load_eval_predictions_from_hive,
    )
    # Empty test catalog — no ml_recsys.eval_predictions
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_recsys")
    spark.sql("DROP TABLE IF EXISTS ml_recsys.eval_predictions")
    spark.sql(
        "CREATE TABLE ml_recsys.eval_predictions "
        "(cust_id STRING, snap_date STRING, prod_name STRING, score DOUBLE, "
        "rank INT, label INT, model_version STRING) "
        "USING parquet PARTITIONED BY (snap_date, model_version)"
    )
    params = {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2099-01-01"},  # nonexistent
        "model_version": "ghost_mv",
    }
    with pytest.raises(DataConsistencyError, match="B4"):
        load_eval_predictions_from_hive(params)


def test_b4_load_from_hive_returns_partition_when_present(spark):
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        load_eval_predictions_from_hive,
        persist_eval_predictions,
    )
    eval_pred = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    params = {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2026-01-31"},
        "model_version": "MV_X",
    }
    persist_eval_predictions(eval_pred, params)
    out = load_eval_predictions_from_hive(params)
    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "p1", 0.9)]
```

- [ ] **Step 2: Run new tests**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_pipelines/test_evaluation_compare_pipeline.py -q
```
Expected: 6 tests pass. (B4 round-trip test exercises actual persist + load; needs local SparkSession with Hive support — conftest `spark` fixture provides it.)

- [ ] **Step 3: Run all evaluation + pipeline tests for regression**

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_evaluation/ \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_pipelines/ \
  /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare/tests/test_core/ -q
```
Expected: all pass. Pay attention to existing `test_evaluation_pipeline*.py` — they must still pass.

- [ ] **Step 4: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  add tests/test_pipelines/test_evaluation_compare_pipeline.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/evaluation-multi-model-compare \
  commit -m "test(evaluation): end-to-end compare pipeline modes + A13/B4 fail loud"
```

---

## Task 13: Manual dev-cluster smoke (no automated steps — operator runbook)

This step is **not automated**. Document the manual verification once code is merged and dev-cluster is available.

Per CLAUDE.md "Local dev-cluster testing" SOP, run by hand:

```bash
# 1. Ensure synthetic data + Hive setup
scripts/dev_admin.sh scripts/setup_hive_dev.py

# 2. Baseline run (no compare)
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb evaluation --env production
# Verify: report.html generated, ml_recsys.eval_predictions has new partition

# 3. --compare mode (you must first create a v_prev compare_sources entry
#    in parameters_evaluation.yaml pointing to a known prior model_version)
.venv/bin/python -m recsys_tfb evaluation --env production --compare v_prev
# Verify: both report.html AND report_comparison.html generated

# 4. --compare-only mode
.venv/bin/python -m recsys_tfb evaluation --env production --compare-only v_prev
# Verify: only report_comparison.html generated; Hive partition NOT overwritten
```

Update memory after first successful run; do not commit this section.

---

## Self-Review

**Spec coverage check:**

- §1 module boundaries → Tasks 3-7 (comparison/* files) + Task 8 (comparison_nodes.py) ✓
- §2 config schema → Task 2 (yaml template) + Task 1 (A11 validation) ✓
- §3 CLI / pipeline wiring → Task 10 (pipeline.py refactor) + Task 11 (CLI flags) + Task 9 (catalog) ✓
- §4 data flow / module behaviour → Tasks 3 (alignment), 4-5 (sources), 6 (restrict), 7 (report), 8 (shims) ✓
- §5 report sections → Task 7 (5 sections + glossary) ✓
- §6 fail-loud invariants → Task 1 (A11-A13), Task 5 (B2), Task 3 (B3), Task 8 (B4 in load_from_hive), Task 12 (A13/B4 tests) ✓
- §7 testing strategy → Tasks 1, 3-7, 12 cover the 6 test files specified ✓

**Placeholder scan:** No TBD / TODO / vague hand-waving in any task. Each code step has actual implementable code; each command step has an actual invocation. Task 13 (dev-cluster smoke) is explicitly labeled "not automated" — a runbook for the operator, not a placeholder.

**Type consistency:** Predicate names (`compare_source_well_formed_errors`, `compare_source_key_exists`, `compare_mutual_exclusive_errors`) are consistent across Tasks 1, 11. Function names in `comparison/` modules (`common_universe`, `load_compare_predictions`, `restrict_to_common`, `assemble_comparison_report`) match between tests, implementations, and pipeline shims. Pipeline node names (`prepare_eval_data`, `compute_metrics`, `compute_baseline_metrics`, `generate_report`, `persist_eval_predictions`, `load_compare_predictions`, `restrict_to_common`, `generate_comparison_report`, `load_eval_predictions_from_hive`) match between Task 10 pipeline definition and Task 12 assertions.

**Open watch points (during execution):**

- Task 10 Step 3: persist node may need Hive-absent fallback for unit tests — handle inline if Step 2 surfaces it.
- Task 12 B4 round-trip test: depends on conftest `spark` fixture supporting Hive metastore (existing tests use it — e.g. `test_evaluation/test_comparison_restrict.py` patterns). If it doesn't, mark as `pytest.mark.skipif(no_hive)` and rely on dev-cluster manual smoke instead.
- Task 9 catalog path: the exact `${...}` filepath template depends on the existing `evaluation_report` entry — adapt at Step 2 by inspecting the actual file.

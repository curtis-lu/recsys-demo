# Config/Data Consistency Phase 2 (B1 data gate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a data/config item-value-set disagreement (B1) a hard, actionable failure before any dataset node runs, instead of silent `-1` encoding corruption.

**Architecture:** One pure predicate `item_coverage_errors` in `core/consistency.py` (single source of truth, no Spark). A thin Spark caller `validate_data_consistency` in `preprocessing/_spark.py` does the windowed `distinct(item)` on `sample_pool` + `label_table` and delegates to the predicate. It is wired as a dedicated **first** side-effect node of the dataset pipeline (verified Runner semantics: insertion-order Kahn seed + full-pipeline run + no-output nodes are first-class → runs first, unbypassable, fail-fast).

**Tech Stack:** Python 3.10.9, PySpark 3.3.2, pytest 7.3.1. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-17-config-consistency-phase2-data-gate-design.md` (decisions D1–D9).

---

## Environment (read `docs/worktree-venv-setup.md` first)

All commands run from the worktree root:
`/Users/curtislu/projects/recsys_tfb/.worktrees/config-consistency-phase2`

Test/Python invocation (absolute venv python; `pyproject` `pythonpath=["src"]` makes the worktree `src` win):

```
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

Pre-flight before first task: `readlink .venv` → `/Users/curtislu/projects/recsys_tfb/.venv`; `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V` → `Python 3.10.9`.

---

## Planning refinements vs spec (intentional, justified)

- **Spec §5.3 `_configured_snap_dates` is dropped.** The codebase already has the canonical "which snap_dates does the dataset pipeline use" helper: `collect_dataset_snap_dates(parameters)` in `pipelines/dataset/nodes_shared.py` (sorted union of train/cal/val/test, already imported by `_spark.py:13` and used by `apply_preprocessor_to_features`). DRY: reuse it. It includes `calibration_snap_dates` unconditionally via `.get(..., [])` rather than gating on `enable_calibration` (spec D5's nuance); using a *different* window set than the rest of the pipeline would itself be an inconsistency, so matching the canonical helper is strictly correct.
- Everything else follows the spec exactly (D1–D9).

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/recsys_tfb/core/consistency.py` | single source of truth for invariants | **Add** pure `item_coverage_errors` |
| `src/recsys_tfb/preprocessing/_spark.py` | Spark preprocessing backend | **Add** `validate_data_consistency`; **modify** `:196` guard `ValueError`→`DataConsistencyError` |
| `src/recsys_tfb/pipelines/dataset/nodes_spark.py` | dataset pipeline node funcs | **Add** thin `validate_data_consistency` wrapper + import |
| `src/recsys_tfb/pipelines/dataset/pipeline.py` | dataset DAG definition | **Prepend** `validate_data_consistency` node |
| `tests/test_core/test_consistency.py` | predicate unit tests (fast, no Spark) | **Extend** |
| `tests/test_pipelines/test_dataset/test_nodes_spark.py` | Spark node tests | **Extend** (+ update existing `:196` test) |
| `tests/test_pipelines/test_dataset/test_pipeline.py` | DAG wiring tests | **Update** counts + add ordering test |
| `CLAUDE.md` | project rules | **Extend** Config consistency gate section |

---

## Task 1: `item_coverage_errors` pure predicate (B1, single source of truth)

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (append at end of file)
- Test: `tests/test_core/test_consistency.py` (append at end of file)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/test_consistency.py`:

```python
from recsys_tfb.core.consistency import item_coverage_errors


class TestItemCoverageErrors:
    DECL = ["a", "b", "c"]

    def test_equal_sets_returns_empty(self):
        assert item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a", "b"}) == []

    def test_sample_pool_unknown_value_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "c", "ploan"}, {"a"})
        assert len(errs) == 1
        assert "ploan" in errs[0]
        assert "sample_pool" in errs[0] and "-1" in errs[0]

    def test_sample_pool_declared_but_absent_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b"}, {"a", "b"})
        assert len(errs) == 1
        assert "'c'" in errs[0] or "c" in errs[0]
        assert "never produces" in errs[0]

    def test_label_unknown_value_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a", "mloan"})
        assert len(errs) == 1
        assert "mloan" in errs[0]
        assert "label_table" in errs[0] and "label_*.sql" in errs[0]

    def test_label_declared_but_absent_is_NOT_error_b3_deferred(self):
        # label_items missing a declared value == B3 (zero-positive), deferred.
        assert item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a"}) == []

    def test_channel_name_item_is_supported(self):
        errs = item_coverage_errors("channel_name", ["sms", "app"], {"sms", "app", "x"}, {"sms"})
        assert len(errs) == 1
        assert "channel_name" in errs[0] and "x" in errs[0]

    def test_collects_multiple_errors(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "zzz"}, {"a", "qqq"})
        # sp_unknown(zzz) + sp_missing(c) + lb_unknown(qqq) = 3
        assert len(errs) == 3
        joined = "\n".join(errs)
        assert "zzz" in joined and "qqq" in joined and "c" in joined
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_consistency.py::TestItemCoverageErrors -q`
Expected: FAIL — `ImportError: cannot import name 'item_coverage_errors'`

- [ ] **Step 3: Write minimal implementation**

Append to the end of `src/recsys_tfb/core/consistency.py`:

```python
def item_coverage_errors(
    item: str,
    declared: list[str],
    sample_pool_items: set[str],
    label_items: set[str],
) -> list[str]:
    """B1 invariant — the single definition.

    sample_pool ↔ declared must be EQUAL (both directions are hard errors):
    a value the data has but config does not encodes to -1 (same code as
    null) and corrupts training/scoring; a value config declares but
    sample_pool never produces can never be scored.

    label_table: only ``label_items - declared`` is an error (label business
    logic produced an unknown item). ``declared - label_items`` is B3
    (zero-positive), deferred — intentionally NOT reported here.

    Keys off the passed ``item`` only; never hardcodes 'prod_name'. Returns
    collect-all error strings; empty list means OK.
    """
    declared_set = set(declared)
    errors: list[str] = []

    sp_unknown = sorted(sample_pool_items - declared_set)
    if sp_unknown:
        errors.append(
            f"sample_pool has item value(s) {sp_unknown} not in "
            f"schema.categorical_values[{item!r}] — these encode to -1 "
            f"(same code as null) and silently corrupt training/scoring. Add "
            f"them to schema.categorical_values.{item} in parameters.yaml, or "
            f"fix sample_pool.sql."
        )

    sp_missing = sorted(declared_set - sample_pool_items)
    if sp_missing:
        errors.append(
            f"schema.categorical_values[{item!r}] declares value(s) "
            f"{sp_missing} that sample_pool never produces — they can never "
            f"be scored/recommended (silent). Remove them from config, or fix "
            f"sample_pool.sql to emit them."
        )

    lb_unknown = sorted(label_items - declared_set)
    if lb_unknown:
        errors.append(
            f"label_table has item value(s) {lb_unknown} not in "
            f"schema.categorical_values[{item!r}] — label business logic "
            f"(label_*.sql) produced an item the model config does not know. "
            f"Reconcile label_*.sql with schema.categorical_values.{item}."
        )

    return errors
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_consistency.py -q`
Expected: PASS (all consistency tests green, including the 7 new ones)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): add item_coverage_errors predicate (B1)"
```

---

## Task 2: `validate_data_consistency` Spark caller + node wrapper

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py` (add function after `fit_preprocessor_metadata`, before `apply_preprocessor_to_features`)
- Modify: `src/recsys_tfb/pipelines/dataset/nodes_spark.py` (import + wrapper)
- Test: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (append a test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py` (it already has module-level `import pandas as pd`, `pytestmark = pytest.mark.spark`, and fixtures `spark`, `sample_pool`, `label_table`, `parameters`):

```python
from recsys_tfb.pipelines.dataset.nodes_spark import validate_data_consistency
from recsys_tfb.core.consistency import DataConsistencyError


class TestValidateDataConsistency:
    def test_consistent_fixtures_return_none(self, sample_pool, label_table, parameters):
        # fixtures: prod_name in {exchange_fx,exchange_usd,fund_stock} ==
        # schema.categorical_values.prod_name; all snaps inside windows.
        assert validate_data_consistency(sample_pool, label_table, parameters) is None

    def test_undeclared_value_raises(self, sample_pool, label_table, parameters):
        # Shrink declared set so fund_stock (present in data) is undeclared.
        params = {
            **parameters,
            "schema": {
                **parameters["schema"],
                "categorical_values": {"prod_name": ["exchange_fx", "exchange_usd"]},
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, params)
        msg = str(ei.value)
        assert "fund_stock" in msg
        assert "sample_pool" in msg

    def test_value_only_in_non_window_snap_is_ignored(
        self, spark, sample_pool, label_table, parameters
    ):
        # 2024-12-31 is outside collect_dataset_snap_dates (train Jan-Mar,
        # val Apr, test May). An undeclared 'ploan' there must be filtered out.
        extra = spark.createDataFrame(
            pd.DataFrame([{
                "snap_date": pd.Timestamp("2024-12-31"),
                "cust_id": "C001",
                "cust_segment_typ": "mass",
                "prod_name": "ploan",
                "label": 0,
                "tenure_months": 12,
                "channel_preference": "digital",
            }])
        )
        sp = sample_pool.unionByName(extra)
        assert validate_data_consistency(sp, label_table, parameters) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_dataset/test_nodes_spark.py::TestValidateDataConsistency" -q`
Expected: FAIL — `ImportError: cannot import name 'validate_data_consistency' from 'recsys_tfb.pipelines.dataset.nodes_spark'`

- [ ] **Step 3a: Add the Spark caller to `_spark.py`**

In `src/recsys_tfb/preprocessing/_spark.py`, insert this function immediately after `fit_preprocessor_metadata` returns (i.e. after its `return preprocessor_metadata, category_mappings` line, before `def apply_preprocessor_to_features`). All needed names are already imported at module level: `F` (`:7`), `get_schema` (`:11`), `collect_dataset_snap_dates` (`:13`).

```python
def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> None:
    """Layer-2 B1 data gate. Side-effect only: raises ``DataConsistencyError``
    on violation, returns ``None`` on success. Wired as the first node of the
    dataset pipeline. See
    docs/superpowers/specs/2026-05-17-config-consistency-phase2-data-gate-design.md.

    Item values are checked on sample_pool (set-equality vs declared, both
    directions) and label_table (only data-has-unknown), restricted to the
    configured snap_date windows the pipeline actually uses.
    """
    # Local import: keep lazy to avoid an import cycle
    # (_spark -> core.schema -> core.consistency). Matches the existing
    # local-import pattern inside fit_preprocessor_metadata.
    from recsys_tfb.core.consistency import (
        DataConsistencyError,
        item_coverage_errors,
        resolved_item_values,
    )

    schema = get_schema(parameters)
    item = schema["item"]
    time_col = schema["time"]
    windows = collect_dataset_snap_dates(parameters)

    def _distinct_items(df: DataFrame) -> set:
        rows = (
            df.filter(F.col(time_col).isin(windows))
            .select(item)
            .distinct()
            .collect()
        )
        return {r[item] for r in rows if r[item] is not None}

    errors = item_coverage_errors(
        item,
        resolved_item_values(parameters),
        _distinct_items(sample_pool),
        _distinct_items(label_table),
    )
    if errors:
        raise DataConsistencyError(
            "Data consistency check failed ("
            + str(len(errors))
            + " issue(s)):\n- "
            + "\n- ".join(errors)
        )
```

- [ ] **Step 3b: Re-export a thin wrapper in `nodes_spark.py`**

In `src/recsys_tfb/pipelines/dataset/nodes_spark.py`, extend the existing import block (currently imports `apply_preprocessor_to_features as _apply_preprocessor_to_features`, `build_model_input as _build_model_input`, `fit_preprocessor_metadata as _fit_preprocessor_metadata` from `recsys_tfb.preprocessing._spark`) to also import the new function:

```python
from recsys_tfb.preprocessing._spark import (
    apply_preprocessor_to_features as _apply_preprocessor_to_features,
    build_model_input as _build_model_input,
    fit_preprocessor_metadata as _fit_preprocessor_metadata,
    validate_data_consistency as _validate_data_consistency,
)
```

Then add this wrapper next to the existing `fit_preprocessor_metadata` wrapper (same thin-delegation pattern):

```python
def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> None:
    """Layer-2 B1 data gate; first node of the dataset pipeline."""
    return _validate_data_consistency(sample_pool, label_table, parameters)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_dataset/test_nodes_spark.py::TestValidateDataConsistency" -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py src/recsys_tfb/pipelines/dataset/nodes_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "feat(dataset): add validate_data_consistency Spark gate (B1)"
```

---

## Task 3: Wire the dedicated first node into the dataset pipeline

**Files:**
- Modify: `src/recsys_tfb/pipelines/dataset/pipeline.py`
- Test: `tests/test_pipelines/test_dataset/test_pipeline.py`

- [ ] **Step 1: Update the failing tests**

In `tests/test_pipelines/test_dataset/test_pipeline.py`, make these exact edits:

Replace lines 7-10 (`test_pipeline_without_calibration`):

```python
    def test_pipeline_without_calibration(self):
        pipeline = create_pipeline()
        # 1 validate + 4 key-selection + 1 fit + 1 apply_features + 4 build_model_input = 11
        assert len(pipeline.nodes) == 11
```

Replace lines 12-15 (`test_pipeline_with_calibration`):

```python
    def test_pipeline_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 11 base + 1 select_calibration_keys + 1 build_calibration_model_input = 13
        assert len(pipeline.nodes) == 13
```

Replace the body of `test_default_parameters` (currently `assert len(pipeline.nodes) == 10`):

```python
    def test_default_parameters(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 11
```

Add this new test method to `class TestDatasetPipeline`:

```python
    def test_validate_data_consistency_runs_first(self):
        pipeline = create_pipeline()
        assert pipeline.nodes[0].name == "validate_data_consistency"
        first = pipeline.nodes[0]
        assert sorted(first.inputs) == ["label_table", "parameters", "sample_pool"]
        assert first.outputs == []
        # presence
        names = [n.name for n in pipeline.nodes]
        assert "validate_data_consistency" in names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_dataset/test_pipeline.py -q`
Expected: FAIL — `test_pipeline_without_calibration` (got 10 != 11), `test_validate_data_consistency_runs_first` (`nodes[0].name` is `select_sample_keys`, not `validate_data_consistency`).

- [ ] **Step 3: Wire the node first in `pipeline.py`**

In `src/recsys_tfb/pipelines/dataset/pipeline.py`, add `validate_data_consistency` to the `from recsys_tfb.pipelines.dataset.nodes_spark import (...)` block (keep alphabetical-ish ordering consistent with the file):

```python
    from recsys_tfb.pipelines.dataset.nodes_spark import (
        apply_preprocessor_to_features,
        build_model_input,
        fit_preprocessor_metadata,
        select_calibration_keys,
        select_test_keys,
        select_train_keys,
        select_val_keys,
        split_train_keys,
        validate_data_consistency,
    )
```

Then make the validate node the **first** element of `nodes` (insert before the existing `Node(select_train_keys, ...)`):

```python
    nodes = [
        # --- Layer-2 B1 data gate: runs first (insertion-order Kahn seed),
        # side-effect only (outputs=None), fail-fast before any sampling ---
        Node(
            validate_data_consistency,
            inputs=["sample_pool", "label_table", "parameters"],
            outputs=None,
            name="validate_data_consistency",
        ),
        # --- Key selection ---
        Node(
            select_train_keys,
            inputs=["sample_pool", "parameters"],
            outputs="sample_keys",
            name="select_sample_keys",
        ),
        # ... (rest of the existing nodes unchanged) ...
```

Leave every other node exactly as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_dataset/test_pipeline.py -q`
Expected: PASS (all `TestDatasetPipeline` tests green; counts 11/13, ordering test passes)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/pipeline.py tests/test_pipelines/test_dataset/test_pipeline.py
git commit -m "feat(dataset): wire validate_data_consistency as first pipeline node"
```

---

## Task 4: Convert the `_spark.py` A2 backstop to `DataConsistencyError` (D8)

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py` (the `:196` guard, inside `fit_preprocessor_metadata`)
- Test: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (the existing test that matches `"schema.item='prod_name' is missing"`)

- [ ] **Step 1: Tighten the existing test to pin the new type**

In `tests/test_pipelines/test_dataset/test_nodes_spark.py`, find the existing test whose body contains:

```python
        with pytest.raises(ValueError, match="schema.item='prod_name' is missing"):
            fit_preprocessor_metadata(feature_table, params)
```

Replace those two lines with (this also imports the type if not already imported at module scope — `DataConsistencyError` is exported by `recsys_tfb.core.consistency`; Task 2 already added `from recsys_tfb.core.consistency import DataConsistencyError` at module level in this file):

```python
        with pytest.raises(DataConsistencyError, match="schema.item='prod_name' is missing") as ei:
            fit_preprocessor_metadata(feature_table, params)
        assert isinstance(ei.value, ValueError)  # subclass: existing callers unaffected
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_dataset/test_nodes_spark.py" -q -k "missing or categorical"`
Expected: FAIL — the guard still raises bare `ValueError`, so `pytest.raises(DataConsistencyError, ...)` does not match (DataConsistencyError is a *subclass* of ValueError, so a bare ValueError is not caught as DataConsistencyError).

- [ ] **Step 3: Convert the guard**

In `src/recsys_tfb/preprocessing/_spark.py`, in `fit_preprocessor_metadata`, the local import `from recsys_tfb.core.consistency import DataConsistencyError` already exists earlier in the same function (the `missing_cats` guard). Change only the raised type of the item guard — replace `raise ValueError(` with `raise DataConsistencyError(` in this block (message text unchanged):

```python
    item_col = schema.get("item")
    if item_col and item_col not in feature_columns:
        raise DataConsistencyError(
            f"schema.item='{item_col}' is missing from derived feature_columns. "
            f"For a ranking task the item column must be a model feature; "
            f"otherwise the booster cannot differentiate items within a query "
            f"group and HPO mAP collapses to a constant across trials. "
            f"Fix: add '{item_col}' to "
            f"dataset.prepare_model_input.categorical_columns in "
            f"parameters_dataset.yaml. "
            f"(current categorical_columns={categorical_cols})"
        )
```

(If the local `from recsys_tfb.core.consistency import DataConsistencyError` is not in scope at this point in the function, add it on the line directly above this `if` — but verify first: the `missing_cats` guard near the top of `fit_preprocessor_metadata` already imports it locally.)

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_dataset/test_nodes_spark.py" -q`
Expected: PASS (the converted guard test passes; no other test regresses — `DataConsistencyError` is a `ValueError`, so any other `pytest.raises(ValueError)` still matches)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "refactor(preprocessing): raise DataConsistencyError from A2 item backstop (D8)"
```

---

## Task 5: Docs + focused verification

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/specs/2026-05-17-config-consistency-phase2-data-gate-design.md` (mark implemented)
- Modify: `docs/superpowers/plans/2026-05-17-config-consistency-phase2-data-gate.md` (this file — mark complete)

- [ ] **Step 1: Extend the CLAUDE.md Config consistency gate section**

In `CLAUDE.md`, in the `## Config consistency gate` section, append this sentence to the end of the existing paragraph:

```
Layer-2 資料閘 `validate_data_consistency`（`preprocessing/_spark.py`，dataset pipeline 第一個 side-effect 節點）在跑任何抽樣/前處理前，對 `sample_pool`（與 `resolved_item_values` 雙向集合相等）與 `label_table`（只擋資料端未知 item）做 windowed `distinct(item)` 檢查，raise `DataConsistencyError`；B1 的唯一定義 predicate 是同檔的 `item_coverage_errors`。
```

- [ ] **Step 2: Mark spec + plan implemented**

Append to the end of `docs/superpowers/specs/2026-05-17-config-consistency-phase2-data-gate-design.md`:

```markdown

## Phase 2 IMPLEMENTED (2026-05-17)

Tasks 1–5 complete. `item_coverage_errors` predicate + `validate_data_consistency`
Spark gate wired as the first dataset-pipeline node; `_spark.py:196` A2 backstop
converted to `DataConsistencyError`. `_configured_snap_dates` superseded by reuse
of `collect_dataset_snap_dates` (DRY). §9 venv blocker resolved before execution.
```

Append to the end of this plan file (`docs/superpowers/plans/2026-05-17-config-consistency-phase2-data-gate.md`):

```markdown

## Phase 2 COMPLETE (2026-05-17)

All Tasks 1–5 implemented and verified. Focused suites green (see Step 3).
```

- [ ] **Step 3: Run the focused test subset (per CLAUDE.md test-perf: not the full suite)**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py \
  tests/test_core/test_consistency_cli_wiring.py \
  tests/test_pipelines/test_dataset/test_pipeline.py \
  tests/test_pipelines/test_dataset/test_nodes_spark.py -q
```
Expected: PASS (0 failures). Record counts in the completion note.

- [ ] **Step 4: Rebuild the graphify graph (code changed)**

Run: `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
Expected: graph rebuilt. (If it errors with `No module named 'graphify'`, that is the known hook-env issue — note it and proceed; do not block the task on it.)

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-05-17-config-consistency-phase2-data-gate-design.md docs/superpowers/plans/2026-05-17-config-consistency-phase2-data-gate.md graphify-out
git commit -m "docs(consistency): document Phase 2 data gate; mark complete"
```

---

## Self-Review

**1. Spec coverage:**
- D1 (B1 only) → whole plan; no B2/B3 task. ✓
- D2 (both tables) → Task 2 `_distinct_items` on both `sample_pool`+`label_table`. ✓
- D3 (sample_pool set-equality both directions) → Task 1 `sp_unknown` + `sp_missing`; tested. ✓
- D4 (label only actual−declared; declared−label NOT error) → Task 1 `lb_unknown` only; `test_label_declared_but_absent_is_NOT_error_b3_deferred`. ✓
- D5 (configured windows) → Task 2 reuses `collect_dataset_snap_dates`; `test_value_only_in_non_window_snap_is_ignored`. Refinement documented. ✓
- D6 (collect-all, single `DataConsistencyError`) → Task 1 returns list; Task 2 single raise. ✓
- D7 (dedicated first side-effect node, no passthrough, `__main__` untouched) → Task 3; `test_validate_data_consistency_runs_first`. ✓
- D8 (`:196`→`DataConsistencyError`) → Task 4. ✓
- D9 (channel-name) → Task 1 `test_channel_name_item_is_supported`; predicate keys off passed `item`. ✓
- Spec §7 testing strategy (fast predicate unit tests; Spark fixture; window filter; A2 characterization; pipeline wiring) → Tasks 1/2/3/4. ✓

**2. Placeholder scan:** Every code step has complete code; every Run step has exact command + expected output. No TBD/TODO/"similar to". ✓

**3. Type consistency:** `item_coverage_errors(item, declared, sample_pool_items, label_items) -> list[str]` defined Task 1, called identically Task 2. `validate_data_consistency(sample_pool, label_table, parameters) -> None` defined Task 2 (`_spark.py`), wrapped identically in `nodes_spark.py`, referenced by the same name in Task 3 `pipeline.py` and the Task 3 ordering test. `DataConsistencyError` (Phase-1 hierarchy, `ValueError` subclass) used in Tasks 2/4. `collect_dataset_snap_dates` reused (existing). No drift. ✓

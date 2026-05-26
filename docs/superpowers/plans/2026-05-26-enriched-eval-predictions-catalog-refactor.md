# Enriched eval_predictions — Catalog-Driven Persist Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `persist_eval_predictions` and the `--compare-only` read path off hardcoded `saveAsTable("ml_recsys.eval_predictions")` / `spark.table(...)` calls and onto the project's standard `HiveTableDataset` catalog abstraction, with the Hive table renamed to `enriched_eval_predictions` to reflect its abstraction level.

**Architecture:** Add a `HiveTableDataset` catalog entry `enriched_eval_predictions` mirroring `training_eval_predictions`'s pattern. Refactor `persist_eval_predictions` into an identity pass-through (framework auto-saves via catalog). Replace `load_eval_predictions_from_hive` with a small pass-through validator `validate_enriched_eval_predictions_present` that filters by snap_date and raises `DataConsistencyError("(B4) ...")` if empty.

**Tech Stack:** Python 3.10, PySpark 3.3.2 (local Hive warehouse for tests), pytest 7.3.1.

**Spec:** `docs/superpowers/specs/2026-05-26-enriched-eval-predictions-catalog-refactor-design.md`

---

## File Structure

| File | Role | Action |
|---|---|---|
| `conf/base/catalog.yaml` | Adds `enriched_eval_predictions` HiveTableDataset entry after `training_eval_predictions` block (after line 229). | Modify |
| `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` | (1) Replace `persist_eval_predictions` body with identity pass-through; drop `parameters` arg. (2) Replace `load_eval_predictions_from_hive` with `validate_enriched_eval_predictions_present`. | Modify |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py` | (1) Rename import `load_eval_predictions_from_hive` → `validate_enriched_eval_predictions_present`. (2) `persist_eval_predictions` node: drop `parameters` input, output rename `eval_predictions_persisted_sentinel` → `enriched_eval_predictions`. (3) `--compare-only` first node: use new validator + catalog input `enriched_eval_predictions`. | Modify |
| `tests/test_pipelines/test_evaluation/test_pipeline.py` | 3 string-level assertion updates (lines 23, 54, 95). | Modify |
| `tests/test_pipelines/test_evaluation_compare_pipeline.py` | (1) Delete obsolete `test_b4_load_from_hive_*` tests (lines 62-127). (2) Update line 35 string assertion. (3) Add 5 new tests covering validator behavior + catalog round-trip + persist identity. | Modify |

**No changes** to `core/runner.py`, `core/catalog.py`, or `io/hive_table_dataset.py` — the entire refactor is consumer-side; the catalog abstraction already supports every behavior needed.

---

## Pre-flight (one-time before Task 1)

- [ ] **Step P1: Verify worktree environment**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
readlink .venv
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
```

Expected:
```
/Users/curtislu/projects/recsys_tfb/.venv
Python 3.10.9
```

If either line is wrong, fix per `docs/worktree-venv-setup.md` before proceeding.

- [ ] **Step P2: Verify spec is committed**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog log --oneline -3
```

Expected: top commit is `docs(spec): enriched_eval_predictions catalog-driven persist refactor`.

---

## Task 1: Add `enriched_eval_predictions` catalog entry

**Files:**
- Modify: `conf/base/catalog.yaml` (insert after line 229, i.e. after the `training_eval_predictions` block)

- [ ] **Step 1.1: Read current catalog.yaml around the insertion point**

Read `conf/base/catalog.yaml` lines 209-230 to confirm the `training_eval_predictions` block ends at line 229 with `- {name: prod_name, type: STRING}` and is followed at line 231 by the `score_table` comment block.

- [ ] **Step 1.2: Insert the new catalog entry**

After line 229 of `conf/base/catalog.yaml`, insert:

```yaml

# --- Evaluation Pipeline - Cached, report-ready eval_predictions ---
# Written by evaluation/comparison_nodes.py::persist_eval_predictions (the
# in-memory eval_predictions from prepare_eval_data, after label join, rank,
# and segment enrichment). Read back by --compare-only mode via catalog
# auto-load + validate_enriched_eval_predictions_present (B4 validator).
# columns: "auto" — schema is inferred from the DataFrame; if segment_sources
# config changes between runs, drop the table before rerun (ALTER TABLE
# schema evolution is a deferred follow-up).
enriched_eval_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: enriched_eval_predictions
  external: false
  columns: "auto"
  partition_filter:
    model_version: ${model_version}
  partition_cols:
    - {name: snap_date, type: STRING}
```

- [ ] **Step 1.3: Sanity-check the YAML parses**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import yaml
with open('/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf/base/catalog.yaml') as f:
    cfg = yaml.safe_load(f)
e = cfg['enriched_eval_predictions']
assert e['type'] == 'HiveTableDataset'
assert e['table'] == 'enriched_eval_predictions'
assert e['partition_filter'] == {'model_version': '\${model_version}'}
assert e['partition_cols'] == [{'name': 'snap_date', 'type': 'STRING'}]
print('catalog entry parsed OK')
"
```

Expected: `catalog entry parsed OK`.

- [ ] **Step 1.4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add conf/base/catalog.yaml
git commit -m "feat(catalog): add enriched_eval_predictions HiveTableDataset entry"
```

---

## Task 2: Refactor `persist_eval_predictions` to identity pass-through (TDD)

**Files:**
- Test: `tests/test_pipelines/test_evaluation_compare_pipeline.py` (add new test, do not delete old tests yet)
- Modify: `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` (lines 107-144)

- [ ] **Step 2.1: Write the failing test**

Append to `tests/test_pipelines/test_evaluation_compare_pipeline.py`:

```python
def test_persist_eval_predictions_returns_input_df(spark):
    """persist_eval_predictions is an identity pass-through: catalog auto-save
    handles the actual Hive write. Function returns the same DataFrame object
    passed in (referential identity, not just equality).
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        persist_eval_predictions,
    )

    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = persist_eval_predictions(df)
    assert out is df
```

- [ ] **Step 2.2: Run the test to verify it fails**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_persist_eval_predictions_returns_input_df \
  -q
```

Expected: FAIL. The current function takes `(eval_predictions, parameters)` (TypeError on missing positional argument) and returns a string (not the input DF).

- [ ] **Step 2.3: Refactor `persist_eval_predictions`**

Replace lines 107-144 of `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` with:

```python
def persist_eval_predictions(eval_predictions: SparkDataFrame) -> SparkDataFrame:
    """Pass-through node that routes the in-memory eval_predictions to the
    framework-auto-save edge for catalog entry ``enriched_eval_predictions``
    (HiveTableDataset). All write-side machinery — dynamic-partition
    overwrite, ``model_version`` partition column injection, CREATE TABLE
    IF NOT EXISTS, ``${hive.db}`` qualification — lives in the catalog
    layer. This function exists solely as the named DAG edge.
    """
    return eval_predictions
```

- [ ] **Step 2.4: Run the test to verify it passes**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_persist_eval_predictions_returns_input_df \
  -q
```

Expected: PASS.

- [ ] **Step 2.5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add src/recsys_tfb/pipelines/evaluation/comparison_nodes.py tests/test_pipelines/test_evaluation_compare_pipeline.py
git commit -m "refactor(evaluation): persist_eval_predictions becomes identity pass-through

All write-side machinery (CREATE TABLE, dynamic-partition overwrite,
model_version column injection, ml_recsys. qualification) now lives in the
HiveTableDataset catalog layer. The node exists solely as the named DAG
edge — framework auto-save handles the Hive write."
```

**Note on remaining test breakage:** After this commit, `test_b4_load_from_hive_returns_partition_when_present` (which calls `persist_eval_predictions(eval_pred, params)`) will break because the new signature drops `params`. That test is deleted in Task 6. Other tests still pass: the old `test_b4_load_from_hive_fails_loud_on_missing_partition` calls only `load_eval_predictions_from_hive(params)` (unaffected), and `test_default_pipeline_has_persist_node` / `test_compare_mode_adds_three_extra_nodes` only check function names (unaffected).

---

## Task 3: Add `validate_enriched_eval_predictions_present` (TDD)

**Files:**
- Test: `tests/test_pipelines/test_evaluation_compare_pipeline.py` (add 3 new tests)
- Modify: `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` (replace `load_eval_predictions_from_hive` at lines 147-171)

- [ ] **Step 3.1: Write 3 failing tests**

Append to `tests/test_pipelines/test_evaluation_compare_pipeline.py`:

```python
def _base_params_for_validator():
    """Minimal params dict the validator needs."""
    return {
        "schema": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank", "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2026-01-31"},
        "model_version": "MV_X",
        "hive": {"db": "ml_recsys"},
    }


def test_b4_validator_raises_when_partition_empty(spark):
    """Empty DataFrame in (simulates catalog filter returned nothing).
    Validator must raise DataConsistencyError tagged (B4).
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    empty = spark.createDataFrame(
        [],
        "cust_id STRING, snap_date STRING, prod_name STRING, "
        "score DOUBLE, rank INT, label INT",
    )
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(
            empty, _base_params_for_validator()
        )


def test_b4_validator_raises_when_snap_date_filter_yields_empty(spark):
    """DataFrame has rows but no rows match the configured evaluation.snap_date.
    Validator filters then raises B4.
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    params = _base_params_for_validator()
    params["evaluation"]["snap_date"] = "2099-01-01"  # mismatch
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(df, params)


def test_b4_validator_passes_when_partition_present(spark):
    """DataFrame has matching snap_date row → validator returns the filtered DF."""
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c2", "2025-12-31", "p1", 0.5, 1, 0),  # different snap_date, filtered out
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    out = validate_enriched_eval_predictions_present(
        df, _base_params_for_validator()
    )
    rows = [(r["cust_id"], r["snap_date"]) for r in out.collect()]
    assert rows == [("c1", "2026-01-31")]
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_raises_when_partition_empty \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_raises_when_snap_date_filter_yields_empty \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_passes_when_partition_present \
  -q
```

Expected: 3 FAIL with `ImportError: cannot import name 'validate_enriched_eval_predictions_present'`.

- [ ] **Step 3.3: Add the new validator alongside the existing `load_eval_predictions_from_hive`**

**Important:** keep `load_eval_predictions_from_hive` intact for now. `pipeline.py` still imports it (lazy import inside `create_pipeline()`); deleting it now would make every `create_pipeline(...)` call (and every pipeline structure test) raise ImportError. Task 6 deletes it once Task 5 has rewired the callers.

In `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py`, **append** the following function after line 171 (i.e. after the existing `load_eval_predictions_from_hive` body):

```python
def validate_enriched_eval_predictions_present(
    enriched_eval_predictions: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """B4 invariant — fail loud if no partition exists for the current
    (snap_date, model_version) in ``enriched_eval_predictions``.

    Pattern: small validator node, pass-through (echoes
    ``validate_predictions`` in inference pipeline). Catalog auto-loads the
    table filtered by ``model_version`` via ``partition_filter`` (which
    drops the column on the way out). This node filters by snap_date and
    asserts at least one row remains; otherwise raises
    ``DataConsistencyError`` with an actionable message.

    Used only in ``--compare-only`` mode. In default / ``--compare`` modes
    the same partition is freshly written by ``persist_eval_predictions``
    earlier in the same pipeline, so B4 cannot fire.
    """
    schema = get_schema(parameters)
    eval_params = parameters.get("evaluation", {}) or {}
    snap_date = str(eval_params.get("snap_date") or "").strip()
    mv = parameters.get("model_version", "unknown")
    hive_db = (parameters.get("hive") or {}).get("db", "ml_recsys")

    df = enriched_eval_predictions.filter(
        F.col(schema["time"]).cast("string") == snap_date
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"(B4) {hive_db}.enriched_eval_predictions has no partition "
            f"for snap_date={snap_date!r} model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without "
            "--compare) first to populate the partition."
        )
    return df
```

- [ ] **Step 3.4: Run the new validator tests to verify they pass**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_raises_when_partition_empty \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_raises_when_snap_date_filter_yields_empty \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_validator_passes_when_partition_present \
  -q
```

Expected: 3 PASS.

- [ ] **Step 3.5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add src/recsys_tfb/pipelines/evaluation/comparison_nodes.py tests/test_pipelines/test_evaluation_compare_pipeline.py
git commit -m "feat(evaluation): add validate_enriched_eval_predictions_present validator

Pass-through validator that the catalog auto-load feeds into. Filters by
snap_date and asserts a non-empty partition, raising DataConsistencyError
(B4) with an actionable hint if absent. Echoes the existing
validate_predictions pattern in inference pipeline.

The legacy load_eval_predictions_from_hive function is intentionally kept
in place for this commit (still imported by pipeline.py); it is removed in
Task 6 once Task 5 rewires the callers."
```

---

## Task 4: Catalog round-trip integration test

**Files:**
- Test: `tests/test_pipelines/test_evaluation_compare_pipeline.py` (add 1 new test)

This test verifies the end-to-end catalog mechanics: `persist_eval_predictions` returns the DF, `HiveTableDataset.save()` writes to local Hive warehouse with correct partitioning, and `HiveTableDataset.load()` reads back with `model_version` dropped.

- [ ] **Step 4.1: Add the round-trip test**

Append to `tests/test_pipelines/test_evaluation_compare_pipeline.py`:

```python
def test_persist_and_catalog_load_roundtrip(spark):
    """End-to-end: persist returns DF as-is; HiveTableDataset saves to local
    warehouse with partition_filter(model_version) + partition_cols(snap_date);
    load reads back and drops model_version.
    """
    import shutil
    from recsys_tfb.io.hive_table_dataset import HiveTableDataset
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        persist_eval_predictions,
    )

    # Start clean: drop table and warehouse dir if a previous run left them
    spark.sql("CREATE DATABASE IF NOT EXISTS ml_recsys")
    spark.sql("DROP TABLE IF EXISTS ml_recsys.enriched_eval_predictions")
    table_dir = _warehouse_table_dir(
        spark, "ml_recsys", "enriched_eval_predictions"
    )
    if table_dir.exists():
        shutil.rmtree(table_dir)

    # Mimic the catalog entry from conf/base/catalog.yaml
    ds = HiveTableDataset(
        database="ml_recsys",
        table="enriched_eval_predictions",
        columns="auto",
        partition_filter={"model_version": "MV_X"},
        partition_cols=[{"name": "snap_date", "type": "STRING"}],
        external=False,
    )

    df_in = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )

    # Framework auto-save flow: node returns DF, runner saves via catalog
    returned = persist_eval_predictions(df_in)
    assert returned is df_in  # identity guarantee re-verified
    ds.save(returned)

    # Framework auto-load flow: catalog filters by partition_filter, drops mv
    out = ds.load()
    cols = set(out.columns)
    assert "model_version" not in cols
    assert {"cust_id", "snap_date", "prod_name", "score", "rank", "label"} <= cols

    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "p1", 0.9)]
```

- [ ] **Step 4.2: Run the round-trip test**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_persist_and_catalog_load_roundtrip \
  -q
```

Expected: PASS.

- [ ] **Step 4.3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add tests/test_pipelines/test_evaluation_compare_pipeline.py
git commit -m "test(evaluation): persist + HiveTableDataset round-trip integration"
```

---

## Task 5: Wire `pipeline.py` to use new node names and catalog routing

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py` (full rewrite of `create_pipeline` body)
- Modify: `tests/test_pipelines/test_evaluation/test_pipeline.py` (3 string-level assertion updates)
- Modify: `tests/test_pipelines/test_evaluation_compare_pipeline.py` (1 string-level assertion update)

- [ ] **Step 5.1: Update `pipeline.py`**

Replace the entire contents of `src/recsys_tfb/pipelines/evaluation/pipeline.py` with:

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
      * default (no flags) — 4 metrics/report nodes + persist_eval_predictions
        (auto-saved via catalog to ``enriched_eval_predictions``
        HiveTableDataset).
      * --compare X — adds 3 compare nodes; both standalone and comparison
        reports produced.
      * --compare-only X — short pipeline that catalog-auto-loads the
        previously-persisted ``enriched_eval_predictions``, validates the
        partition (B4), and only produces report_comparison.html.
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
        persist_eval_predictions,
        restrict_to_common,
        validate_enriched_eval_predictions_present,
    )

    if compare_only:
        # CLI A12 ensures compare_source is not None when compare_only is True.
        # First node consumes "enriched_eval_predictions" — catalog auto-loads
        # via HiveTableDataset.load() with WHERE model_version=${model_version},
        # then validator filters to current snap_date and raises B4 if empty.
        return Pipeline([
            Node(
                validate_enriched_eval_predictions_present,
                inputs=["enriched_eval_predictions", "parameters"],
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
        # persist returns the same DF as-is; framework auto-saves via catalog
        # entry "enriched_eval_predictions" (HiveTableDataset). Catalog
        # injects model_version partition column + dynamic-partition overwrite.
        Node(
            persist_eval_predictions,
            inputs=["eval_predictions"],
            outputs="enriched_eval_predictions",
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

- [ ] **Step 5.2: Update `tests/test_pipelines/test_evaluation/test_pipeline.py`**

Find and replace exactly:

```python
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "eval_predictions_persisted_sentinel",
        }
```

with (occurs **twice** — lines 19-24 in `TestEvaluationPipelineDefault` and lines 50-55 in `TestEvaluationPipelinePostTraining`):

```python
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions",
        }
```

Then find:

```python
        assert names == [
            "load_eval_predictions_from_hive",
            "load_compare_predictions",
            "restrict_to_common",
            "generate_comparison_report",
        ]
```

and replace with:

```python
        assert names == [
            "validate_enriched_eval_predictions_present",
            "load_compare_predictions",
            "restrict_to_common",
            "generate_comparison_report",
        ]
```

- [ ] **Step 5.3: Update `tests/test_pipelines/test_evaluation_compare_pipeline.py`**

Find:

```python
    assert "load_eval_predictions_from_hive" in node_names
```

and replace with:

```python
    assert "validate_enriched_eval_predictions_present" in node_names
```

- [ ] **Step 5.4: Run all evaluation pipeline tests**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation/test_pipeline.py \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py \
  -q --deselect /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_load_from_hive_fails_loud_on_missing_partition \
  --deselect /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py::test_b4_load_from_hive_returns_partition_when_present
```

(`--deselect` skips the two old B4 tests that are scheduled for deletion in Task 6; they would fail now because `test_b4_load_from_hive_returns_partition_when_present` calls `persist_eval_predictions(eval_pred, params)` — wrong arity after Task 2.)

Expected: all selected tests PASS.

- [ ] **Step 5.5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add src/recsys_tfb/pipelines/evaluation/pipeline.py tests/test_pipelines/test_evaluation/test_pipeline.py tests/test_pipelines/test_evaluation_compare_pipeline.py
git commit -m "refactor(evaluation): pipeline.py uses catalog routing for enriched_eval_predictions

- Default/--compare modes: persist node output renamed
  eval_predictions_persisted_sentinel → enriched_eval_predictions
  (framework auto-saves to HiveTableDataset).
- --compare-only mode: first node is validate_enriched_eval_predictions_present
  consuming the catalog-auto-loaded enriched_eval_predictions input.
- Updated 4 string-level test assertions to match."
```

---

## Task 6: Delete obsolete `load_eval_predictions_from_hive` function and tests

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` (delete `load_eval_predictions_from_hive` function at lines 147-171)
- Modify: `tests/test_pipelines/test_evaluation_compare_pipeline.py` (delete 2 obsolete B4 tests at lines 62-127)

After Task 5, no caller imports `load_eval_predictions_from_hive` anymore. Now safe to delete the function and the two obsolete tests that exercise it.

- [ ] **Step 6.1a: Delete `load_eval_predictions_from_hive` from `comparison_nodes.py`**

Remove lines 147-171 of `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` (the entire `def load_eval_predictions_from_hive(parameters: dict) -> SparkDataFrame:` function and its docstring).

- [ ] **Step 6.1b: Delete the two obsolete B4 tests**

In `tests/test_pipelines/test_evaluation_compare_pipeline.py`, delete the entire blocks (original lines 62-127):

```python
def test_b4_load_from_hive_fails_loud_on_missing_partition(spark):
    ...  # ~30 lines
    with pytest.raises(DataConsistencyError, match="B4"):
        load_eval_predictions_from_hive(params)


def test_b4_load_from_hive_returns_partition_when_present(spark):
    ...  # ~30 lines
    persist_result = persist_eval_predictions(eval_pred, params)
    out = load_eval_predictions_from_hive(params)
    rows = [(r["cust_id"], r["prod_name"], r["score"]) for r in out.collect()]
    assert rows == [("c1", "p1", 0.9)]
```

(Keep `_warehouse_table_dir` and all other tests in the file intact — they are still used by the new `test_persist_and_catalog_load_roundtrip`.)

- [ ] **Step 6.2: Verify no remaining references**

Run:
```bash
grep -rn "load_eval_predictions_from_hive\|eval_predictions_persisted_sentinel\|ml_recsys\.eval_predictions" \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf
```

Expected: no output (zero matches). All references should be gone.

- [ ] **Step 6.3: Run full evaluation test sub-tree**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation/ \
  /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/tests/test_pipelines/test_evaluation_compare_pipeline.py \
  -q
```

Expected: all PASS (no `--deselect` needed anymore).

- [ ] **Step 6.4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add tests/test_pipelines/test_evaluation_compare_pipeline.py
git commit -m "refactor(evaluation): delete load_eval_predictions_from_hive function and obsolete tests

After Task 5 rewired pipeline.py to use validate_enriched_eval_predictions_present,
load_eval_predictions_from_hive has no callers and can be deleted.

The two old B4 round-trip tests are superseded by:
- test_b4_validator_* (3 tests for validator behavior)
- test_persist_and_catalog_load_roundtrip (end-to-end catalog mechanics)"
```

---

## Task 7: Update graphify knowledge graph

**Files:**
- Modify: `graphify-out/GRAPH_REPORT.md`, `graphify-out/graph.json` (auto-generated)

Per project CLAUDE.md (`graphify` section): after modifying code files, rebuild the graph.

- [ ] **Step 7.1: Rebuild graphify**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

Expected: rebuild log similar to: `[graphify watch] Rebuilt: <N> nodes, <M> edges, <K> communities`.

- [ ] **Step 7.2: Verify and commit if changed**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog status --short graphify-out/
```

If there are modified files (likely just `graph.json` and `GRAPH_REPORT.md`):

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
git add graphify-out/
git commit -m "chore: refresh graphify after enriched_eval_predictions refactor"
```

If `git status` is clean, skip the commit.

---

## Task 8: Manual dev-cluster verification (smoke test the refactor against real Hive)

**Files:** None (operational verification only)

This task verifies the refactor works end-to-end on the dev-cluster (Spark Standalone + Hive metastore) before merging.

- [ ] **Step 8.1: Drop the legacy Hive table**

Per spec §7, the old `ml_recsys.eval_predictions` table has no external consumers. Drop it:

```bash
cd ~/dev-cluster
echo "DROP TABLE IF EXISTS ml_recsys.eval_predictions" | docker exec -i hive-metastore bash -c "beeline -u jdbc:hive2://localhost:10000 -n hive --silent=true"
```

Alternative if the metastore is reachable from a transient pyspark container:

```bash
/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/scripts/dev_admin.sh -c \
  "from pyspark.sql import SparkSession; SparkSession.builder.appName('drop').enableHiveSupport().getOrCreate().sql('DROP TABLE IF EXISTS ml_recsys.eval_predictions')"
```

- [ ] **Step 8.2: Run evaluation in default mode against dev-cluster**

Run (per CLAUDE.md "Pipeline 與 SPARK_CONF_DIR 的對應"):

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
source ~/dev-cluster/scripts/client-env.sh
PYTHONPATH=$(pwd)/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env production
```

Expected:
- Pipeline completes.
- Log line from `HiveTableDataset.save()` reports `Wrote N partitions to ml_recsys.enriched_eval_predictions: [{'model_version': '<mv>', 'snap_date': '<date>'}]`.
- `data/evaluation/<mv>/<snap_date>/report.html` exists.

If pipeline fails, troubleshoot per `dev-cluster-spark` skill before proceeding.

- [ ] **Step 8.3: Verify Hive table contents**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/scripts/dev_admin.sh -c \
  "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('check').enableHiveSupport().getOrCreate(); spark.sql('SHOW PARTITIONS ml_recsys.enriched_eval_predictions').show(truncate=False); spark.table('ml_recsys.enriched_eval_predictions').printSchema()"
```

Expected:
- Partition listed: `model_version=<mv>/snap_date=<date>`.
- Schema includes: `cust_id`, `prod_name`, `score`, `rank`, `label`, `snap_date`, `model_version` (and any segment columns if `segment_sources` is configured).

- [ ] **Step 8.4: Run `--compare-only` mode against the populated table**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog
source ~/dev-cluster/scripts/client-env.sh
PYTHONPATH=$(pwd)/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env production --compare-only <some_configured_compare_source>
```

(Replace `<some_configured_compare_source>` with a key from `parameters_evaluation.yaml::evaluation.compare_sources`.)

Expected:
- Pipeline completes with 4 nodes.
- `data/evaluation/<mv>/<snap_date>/report_comparison.html` exists.

- [ ] **Step 8.5: Verify B4 fail-loud against a missing snap_date**

The CLI has no `--params` override flag; verify B4 by temporarily editing the parameters file:

```bash
# 1. Snapshot current snap_date
ORIG_SNAP=$(grep "^  snap_date:" /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf/base/parameters_evaluation.yaml | head -1)

# 2. Temporarily set to a non-existent date
sed -i.bak 's/^  snap_date:.*/  snap_date: "2099-01-01"/' /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf/base/parameters_evaluation.yaml

# 3. Run --compare-only; capture the failure
PYTHONPATH=$(pwd)/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env production --compare-only <some_compare_source> 2>&1 | grep -E "B4|DataConsistencyError" || echo "B4 NOT raised — investigate"

# 4. Restore the original file
mv /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf/base/parameters_evaluation.yaml.bak /Users/curtislu/projects/recsys_tfb/.worktrees/enriched-eval-predictions-catalog/conf/base/parameters_evaluation.yaml
```

Expected: the grep output contains `(B4) ml_recsys.enriched_eval_predictions has no partition for snap_date='2099-01-01' ...`. Final `git status` should show no changes (the `mv` restored the file).

- [ ] **Step 8.6: No new commits expected**

Task 8 is operational verification; no source changes are produced. If any code adjustments are needed (e.g. an unexpected runtime error reveals a missing edge case), iterate on Tasks 1-7 — do NOT slap a fix onto this task.

---

## Self-Review Checklist (for the engineer)

Before pushing the branch and opening a PR:

- [ ] All 8 tasks committed.
- [ ] `git log --oneline main..` shows a clean linear history (no merge commits, no fixups).
- [ ] `grep -rn "ml_recsys\.eval_predictions\|load_eval_predictions_from_hive\|eval_predictions_persisted_sentinel" src/ tests/ conf/` returns zero matches.
- [ ] `tests/test_pipelines/test_evaluation/` + `tests/test_pipelines/test_evaluation_compare_pipeline.py` all pass.
- [ ] Dev-cluster smoke test (Task 8) succeeded.
- [ ] No `# TODO`, `# FIXME`, or stub comments left in modified files.

---

## Out of scope (deferred to follow-up PRs, per spec §3)

- **Schema evolution** via `ALTER TABLE ADD COLUMNS` in `HiveTableDataset`. Currently when `segment_sources` config changes, drop+rerun is required.
- **Consistency-gate C1** (grep/AST-based unit test forbidding `saveAsTable(` / `insertInto(` literals in `src/recsys_tfb/pipelines/`).
- **`compute_all_metrics` incremental optimization** for `--compare-only` speed.

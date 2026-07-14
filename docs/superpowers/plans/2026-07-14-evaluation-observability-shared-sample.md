# Evaluation Observability + Shared Diagnosis Sample — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compute the driver-side diagnosis sample once per evaluation run (instead of 3×) and surface free-of-cost data-volume/step observability, without changing any output.

**Architecture:** Extract `draw_diagnosis_sample` into a single pipeline node `draw_diagnosis_sample_node` whose in-memory output feeds the three consumers (`compute_metric_ci`, `compute_offset_sweep`, `compute_pair_ledger`); add `core/logging.py::log_data_volume`/`log_step` calls at node call-sites (the repo-wide convention — runner untouched). Deterministic same-seed sampling makes the shared draw byte-identical to the current per-node draws.

**Tech Stack:** Python 3.10.9, PySpark 3.3.2, pytest. Manual Kedro-style pipeline (`core/node.py`, `core/pipeline.py`, `core/runner.py`).

**Spec:** `docs/superpowers/specs/2026-07-14-evaluation-observability-shared-sample-design.md`

---

## File Structure

- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — add `_sample_consumer_flags` helper + `draw_diagnosis_sample_node`; rewire 3 consumers to take `diagnosis_sample`; add observability calls.
- `src/recsys_tfb/pipelines/evaluation/pipeline.py` — insert the new node, rewire 3 consumer inputs.
- `src/recsys_tfb/diagnosis/metric/sample.py` — add `log_step` sub-step timing (SHOULD).
- `tests/test_pipelines/test_evaluation/test_nodes_spark.py` — new node/consumer tests.
- `tests/test_pipelines/test_evaluation/test_pipeline.py` — update pinned node count/order/outputs.

## Pre-flight (run once before Task 1)

- [ ] **Step 0: worktree pre-flight + baseline**

Run (worktree is already created on branch `feat/eval-observability`):

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # expect .../recsys_tfb/.venv and Python 3.10.9
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ tests/test_diagnosis/test_metric/ \
  tests/test_pipelines/test_resume_contracts.py -q 2>&1 | tail -20
```

Expected: baseline PASS (record the summary line; any pre-existing fail must be attributed per `docs/operations/known-pitfalls.md §5`, not to this change).

---

## Task 1: `_sample_consumer_flags` helper (single source of truth for enable gate)

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_evaluation/test_nodes_spark.py`:

```python
class TestSampleConsumerFlags:
    def test_defaults_all_true(self):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _sample_consumer_flags,
        )
        assert _sample_consumer_flags({}) == (True, True, True)

    def test_respects_disabled(self):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            _sample_consumer_flags,
        )
        params = {"evaluation": {"diagnosis": {
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": True},
            "pair_ledger": {"enabled": False},
        }}}
        assert _sample_consumer_flags(params) == (False, True, False)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestSampleConsumerFlags -q`
Expected: FAIL with `ImportError: cannot import name '_sample_consumer_flags'`.

- [ ] **Step 3: Write minimal implementation**

Add to `nodes_spark.py` (after the imports / `logger =` line, before `prepare_eval_data`):

```python
def _sample_consumer_flags(parameters: dict) -> tuple[bool, bool, bool]:
    """Return (ci_enabled, offset_sweep_enabled, pair_ledger_enabled).

    Single source of truth for the enable flags of the three diagnosis nodes
    that consume the shared sample. ``draw_diagnosis_sample_node`` draws iff any
    is True; each consumer still checks its own flag. Reading them here with the
    exact same keys/defaults as the consumers prevents gate/consumer drift.
    """
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    ci = (diag.get("ci", {}) or {}).get("enabled", True)
    sweep = (diag.get("offset_sweep", {}) or {}).get("enabled", True)
    ledger = (diag.get("pair_ledger", {}) or {}).get("enabled", True)
    return bool(ci), bool(sweep), bool(ledger)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestSampleConsumerFlags -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(eval): add _sample_consumer_flags enable-gate helper"
```

---

## Task 2: `draw_diagnosis_sample_node` (gating + faithful pass-through; not yet wired)

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_nodes_spark.py`. `_fixture`/`_params` mirror
`tests/test_diagnosis/test_metric/test_sample.py`:

```python
class TestDrawDiagnosisSampleNode:
    @staticmethod
    def _params():
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            }},
            "evaluation": {"diagnosis": {"sample": {
                "max_queries": 10, "min_pos_queries_per_item": 2, "seed": 42,
            }}},
        }

    @staticmethod
    def _eval_predictions(spark):
        rows = []
        for cust in ["H1", "H2", "H3", "H4"]:
            rows.append(("20240331", cust, "hot", 0.9, 1))
            rows.append(("20240331", cust, "cold", 0.1, 0))
        rows.append(("20240331", "C1", "hot", 0.9, 0))
        rows.append(("20240331", "C1", "cold", 0.1, 1))
        return spark.createDataFrame(
            rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
        )

    def test_returns_none_and_skips_draw_when_all_disabled(self, spark):
        from unittest.mock import patch
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": False},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
        })
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample"
        ) as spy:
            result = nodes_spark.draw_diagnosis_sample_node(
                self._eval_predictions(spark), params
            )
        assert result is None
        assert spy.call_count == 0

    def test_draws_when_one_enabled(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()
        params["evaluation"]["diagnosis"].update({
            "ci": {"enabled": True},
            "offset_sweep": {"enabled": False},
            "pair_ledger": {"enabled": False},
        })
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
            return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
        ) as spy:
            nodes_spark.draw_diagnosis_sample_node(None, params)
        assert spy.call_count == 1

    def test_node_output_equals_direct_draw(self, spark):
        # Faithfulness / behaviour-preservation: the node is a pass-through of
        # draw_diagnosis_sample. Same seed -> identical content.
        from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all three consumers default-enabled
        direct_pdf, direct_meta = draw_diagnosis_sample(
            self._eval_predictions(spark), params
        )
        node_pdf, node_meta = nodes_spark.draw_diagnosis_sample_node(
            self._eval_predictions(spark), params
        )
        assert node_meta == direct_meta
        # Order-independent content equality on the sampled rows.
        assert (
            node_pdf.sort_values(list(node_pdf.columns))
            .reset_index(drop=True)
            .equals(
                direct_pdf.sort_values(list(direct_pdf.columns))
                .reset_index(drop=True)
            )
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode -q`
Expected: FAIL with `AttributeError: ... has no attribute 'draw_diagnosis_sample_node'`.

- [ ] **Step 3: Write minimal implementation**

Add to `nodes_spark.py` (immediately after `prepare_eval_data`, before `compute_metrics`). `Optional` is already imported at the top of the module:

```python
def draw_diagnosis_sample_node(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> Optional[tuple]:
    """Draw the shared driver-side diagnosis sample ONCE per run.

    ``compute_metric_ci`` / ``compute_offset_sweep`` / ``compute_pair_ledger``
    all consume this single sample instead of each re-drawing it (same seed ->
    identical content; 3 Spark scans collapse to 1). Returns ``None`` when none
    of the three consumers is enabled — matching the previous behaviour of
    drawing zero samples in that case.
    """
    ci_on, sweep_on, ledger_on = _sample_consumer_flags(parameters)
    if not (ci_on or sweep_on or ledger_on):
        logger.info(
            "diagnosis sample: all consumers (ci/offset_sweep/pair_ledger) "
            "disabled — skipping sample draw"
        )
        return None

    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
    sample_pdf, sample_meta = draw_diagnosis_sample(eval_predictions, parameters)
    logger.info(
        "diagnosis sample drawn once for %d consumer(s): %d queries sampled "
        "(shared by metric_ci/offset_sweep/pair_ledger)",
        sum((ci_on, sweep_on, ledger_on)), sample_meta["n_queries_sampled"],
    )
    return sample_pdf, sample_meta
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(eval): add draw_diagnosis_sample_node (shared sample, gated)"
```

---

## Task 3: Rewire the 3 consumers + pipeline; prove single draw

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (3 consumer signatures/bodies)
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py` (add node, rewire inputs)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`, `.../test_pipeline.py`

- [ ] **Step 1: Write the failing "single draw" test**

Append to `test_nodes_spark.py` inside `class TestDrawDiagnosisSampleNode` (reuses its `_params`):

```python
    def test_draw_diagnosis_sample_called_once_across_three_consumers(self):
        from unittest.mock import patch
        import pandas as pd
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all three default-enabled
        stub_pdf = pd.DataFrame({
            "snap_date": ["20240331"], "cust_id": ["H1"],
            "prod_name": ["hot"], "score": [0.9], "label": [1],
        })
        stub = (stub_pdf, {"n_queries_sampled": 1})
        with patch(
            "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
            return_value=stub,
        ) as spy, patch(
            "recsys_tfb.diagnosis.metric.uncertainty.bootstrap_per_item_ci",
            return_value={"n_boot": 1},
        ), patch(
            "recsys_tfb.diagnosis.metric.offset_sweep.sweep", return_value={},
        ), patch(
            "recsys_tfb.diagnosis.metric.pair_ledger.pair_ledger",
            return_value={},
        ):
            sample = nodes_spark.draw_diagnosis_sample_node(None, params)
            nodes_spark.compute_metric_ci(sample, params)
            nodes_spark.compute_offset_sweep(sample, params)
            nodes_spark.compute_pair_ledger(sample, params)
        assert spy.call_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode::test_draw_diagnosis_sample_called_once_across_three_consumers" -q`
Expected: FAIL — current consumers each call `draw_diagnosis_sample`, so `spy.call_count == 3` (or a signature TypeError, since they still expect `eval_predictions`). Either way, RED.

- [ ] **Step 3: Rewire `compute_metric_ci`**

Replace the current `compute_metric_ci` body. New version (first param renamed, own draw removed):

```python
def compute_metric_ci(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """診斷抽樣＋cluster bootstrap CI（spec §3 Phase 1）。

    抽樣改由 ``draw_diagnosis_sample_node`` 一次抽好、經 ``diagnosis_sample``
    傳入（同 seed→內容與各自重抽相同）。停用時回傳 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    ci_cfg = ((eval_params.get("diagnosis", {}) or {}).get("ci", {}) or {})
    if not ci_cfg.get("enabled", True):
        logger.info("metric CI disabled — writing stub")
        return {"enabled": False}

    if diagnosis_sample is None:
        raise ValueError(
            "compute_metric_ci: diagnosis_sample is None while "
            "evaluation.diagnosis.ci.enabled is true — draw_diagnosis_sample_node "
            "gate is out of sync with the consumer enable flag"
        )

    from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

    sample_pdf, sample_meta = diagnosis_sample
    out = bootstrap_per_item_ci(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "metric CI computed on %d sampled queries (n_boot=%d)",
        sample_meta["n_queries_sampled"], out["n_boot"],
    )
    return out
```

- [ ] **Step 4: Rewire `compute_offset_sweep`**

Replace the current `compute_offset_sweep` body:

```python
def compute_offset_sweep(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """分流層薄 node（spec §3 Phase 4；框架診斷項目 6）。

    領域邏輯全在 ``diagnosis.metric.offset_sweep``（driver 端 numpy）。抽樣改由
    ``draw_diagnosis_sample_node`` 共用（同 seed→內容相同）。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("offset_sweep", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("offset sweep disabled — writing stub")
        return {"enabled": False}
    if diagnosis_sample is None:
        raise ValueError(
            "compute_offset_sweep: diagnosis_sample is None while "
            "evaluation.diagnosis.offset_sweep.enabled is true — "
            "draw_diagnosis_sample_node gate out of sync with the consumer flag"
        )
    from recsys_tfb.diagnosis.metric.offset_sweep import sweep

    sample_pdf, sample_meta = diagnosis_sample
    out = sweep(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "offset sweep computed: %d items, rounds=%d converged=%s, "
        "holdout mAP zero=%s star=%s",
        len(out.get("delta_star", {})), out.get("n_rounds_run"),
        out.get("converged"),
        (out.get("map_holdout") or {}).get("zero"),
        (out.get("map_holdout") or {}).get("star"),
    )
    return out
```

- [ ] **Step 5: Rewire `compute_pair_ledger`**

Replace the current `compute_pair_ledger` body:

```python
def compute_pair_ledger(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """壓制帳本薄 node（spec §3 Phase 4b；框架診斷項目 7）。

    領域邏輯全在 ``diagnosis.metric.pair_ledger``（driver 端 numpy）。抽樣改由
    ``draw_diagnosis_sample_node`` 共用（同 seed→內容相同）。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("pair_ledger", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("pair ledger disabled — writing stub")
        return {"enabled": False}
    if diagnosis_sample is None:
        raise ValueError(
            "compute_pair_ledger: diagnosis_sample is None while "
            "evaluation.diagnosis.pair_ledger.enabled is true — "
            "draw_diagnosis_sample_node gate out of sync with the consumer flag"
        )
    from recsys_tfb.diagnosis.metric.pair_ledger import pair_ledger

    sample_pdf, sample_meta = diagnosis_sample
    out = pair_ledger(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "pair ledger computed: %d mis-ordered pairs, %d suppressors, "
        "map_current=%s",
        out.get("n_mis_ordered_pairs", 0),
        len(out.get("by_suppressor", {})),
        out.get("map_current"),
    )
    return out
```

- [ ] **Step 6: Wire the node into `pipeline.py`**

In `create_pipeline`, add `draw_diagnosis_sample_node` to the imports from
`nodes_spark`, then edit the `nodes` list. Insert the new Node right after the
`prepare_eval_data` Node:

```python
        Node(
            draw_diagnosis_sample_node,
            inputs=["eval_predictions", "parameters"],
            outputs="diagnosis_sample",
        ),
```

Change the three consumer Nodes' first input from `"eval_predictions"` to
`"diagnosis_sample"`:

```python
        Node(
            compute_metric_ci,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_metric_ci",
        ),
        ...
        Node(
            compute_offset_sweep,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_offset_sweep",
        ),
        Node(
            compute_pair_ledger,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_pair_ledger",
        ),
```

Leave `compute_quadrant`, `generate_report`, `persist_eval_predictions` and the
`--compare-only` branch unchanged.

- [ ] **Step 7: Update the pinned pipeline tests**

In `tests/test_pipelines/test_evaluation/test_pipeline.py`:

- `TestEvaluationPipelineDefault.test_pipeline_has_six_nodes`: `== 11` → `== 12`.
- `TestEvaluationPipelinePostTraining.test_pipeline_has_six_nodes`: `== 11` → `== 12`.
- `TestEvaluationPipelineCompareMode.test_pipeline_has_nine_nodes`: `== 14` → `== 15`.
- Both `test_pipeline_outputs` / `test_pipeline_outputs_same_as_default`: add
  `"diagnosis_sample",` to the `expected` set.
- `test_node_names`: replace the list with the deterministic topological order
  below.

```python
        assert names == [
            "prepare_eval_data",
            "draw_diagnosis_sample_node",
            "compute_metrics",
            "compute_baseline_metrics",
            "compute_reconciliation",
            "persist_eval_predictions",
            "compute_metric_ci",
            "compute_offset_sweep",
            "compute_pair_ledger",
            "compute_quadrant",
            "assemble_triage_summary",
            "generate_report",
        ]
```

> Note: this order was hand-derived from Kahn's algorithm in
> `core/pipeline.py` (declaration-order tiebreak; the new node declared right
> after `prepare_eval_data`). If the actual `[n.name for n in pipeline.nodes]`
> differs, the sort is still deterministic — set the list to the real output,
> provided the invariant holds: `draw_diagnosis_sample_node` appears after
> `prepare_eval_data` and before all three of `compute_metric_ci` /
> `compute_offset_sweep` / `compute_pair_ledger`.

- [ ] **Step 8: Run the single-draw test + pipeline tests to verify GREEN**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode \
  tests/test_pipelines/test_evaluation/test_pipeline.py -q
```
Expected: PASS (single-draw test now green; pipeline count/order/outputs green).

- [ ] **Step 9: Mutation check (prove the single-draw test bites)**

Temporarily add, at the top of `compute_pair_ledger`'s enabled branch (after the
`sample_pdf, sample_meta = diagnosis_sample` line), a redundant re-draw:

```python
    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
    draw_diagnosis_sample(sample_pdf, parameters)  # MUTATION: redundant re-draw
```

Run the single-draw test:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  "tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode::test_draw_diagnosis_sample_called_once_across_three_consumers" -q
```
Expected: FAIL with `assert 2 == 1` (spy now called twice). This confirms the
test measures the "single shared draw" causal chain. **Remove the two mutation
lines** and re-run to confirm PASS again.

- [ ] **Step 10: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
        src/recsys_tfb/pipelines/evaluation/pipeline.py \
        tests/test_pipelines/test_evaluation/test_nodes_spark.py \
        tests/test_pipelines/test_evaluation/test_pipeline.py
git commit -m "feat(eval): share one diagnosis sample across 3 consumers (was 3x)"
```

---

## Task 4: Observability — free data-volume + sub-step timing

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (`log_data_volume` in node)
- Modify: `src/recsys_tfb/diagnosis/metric/sample.py` (`log_step` sub-steps)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

**Observability scope note (reconciles spec §5):** spec §5 listed a node-level
`log_step` (MUST) and per-consumer driver-compute `log_step` (SHOULD). After
Task 3 the sample draw is its OWN node and each consumer is its OWN node, so the
runner's existing `node_completed` event (`core/runner.py:139`) already
attributes wall-time to each: `draw_diagnosis_sample_node` = sampling time,
`compute_metric_ci`/`compute_offset_sweep`/`compute_pair_ledger` = bootstrap/
sweep/ledger time. Wrapping those in explicit `log_step` would double-count. So
this task adds only the two genuinely-new signals: `log_data_volume` (free
pandas size — not otherwise available) and the intra-sample `log_step` phase
breakdown (finer than any node-level timing). This satisfies the §5 intent
(attribute where time goes + data sizes) without redundant instrumentation.

- [ ] **Step 1: Write the failing observability test**

Append inside `class TestDrawDiagnosisSampleNode`:

```python
    def test_node_logs_free_pandas_data_volume(self, spark, caplog):
        import logging
        from recsys_tfb.pipelines.evaluation import nodes_spark
        params = self._params()  # all enabled
        with caplog.at_level(logging.INFO):
            nodes_spark.draw_diagnosis_sample_node(
                self._eval_predictions(spark), params
            )
        vols = [
            r.volume for r in caplog.records
            if getattr(r, "event", None) == "data_volume"
            and getattr(r, "volume", {}).get("name") == "diagnosis.sample_pdf"
        ]
        assert vols, "expected a data_volume event for diagnosis.sample_pdf"
        # Free pandas measurement (rows populated), NOT a Spark count.
        assert vols[0]["kind"] == "pandas"
        assert vols[0]["rows"] is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode::test_node_logs_free_pandas_data_volume" -q`
Expected: FAIL (`assert vols` — no data_volume event yet).

- [ ] **Step 3: Add `log_data_volume` to the node**

In `nodes_spark.py`, add the import near the top (with the other
`recsys_tfb.evaluation...` imports):

```python
from recsys_tfb.core.logging import log_data_volume
```

In `draw_diagnosis_sample_node`, right after the
`sample_pdf, sample_meta = draw_diagnosis_sample(...)` line:

```python
    log_data_volume(logger, "diagnosis.sample_pdf", sample_pdf, deep=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestDrawDiagnosisSampleNode::test_node_logs_free_pandas_data_volume" -q`
Expected: PASS.

- [ ] **Step 5: Add `log_step` sub-step timing in `sample.py`**

In `src/recsys_tfb/diagnosis/metric/sample.py`, add the import (next to the
existing `from recsys_tfb.core.schema import get_schema`):

```python
from recsys_tfb.core.logging import log_step
```

Wrap the three phases inside `draw_diagnosis_sample` — **computation unchanged,
only wrapped** (`take_all_items` stays OUTSIDE the pass-1 `with`, it is a pure
Python comprehension). Replace the pass-1 block with:

```python
    # ---- pass 1：正例 query 全集＋per-item 正例 query 數 ----
    with log_step(logger, "diagnosis_sample.pass1_count"):
        pos_rows = df.filter(F.col(label_col) == 1)
        pos_queries = pos_rows.select(*query_cols).distinct()
        n_pos_total = pos_queries.count()

        item_counts = {
            str(r[item_col]): int(r["cnt"])
            for r in pos_rows.select(*query_cols, item_col)
            .distinct()
            .groupBy(item_col)
            .agg(F.count(F.lit(1)).alias("cnt"))
            .collect()
        }
    take_all_items = sorted(
        it for it, c in item_counts.items() if c < floor
    )
```

Replace the pass-2 block with:

```python
    # ---- pass 2：take-all ∪ hash-ratio ----
    with log_step(logger, "diagnosis_sample.pass2_select"):
        if take_all_items:
            must = (
                pos_rows.filter(F.col(item_col).isin(take_all_items))
                .select(*query_cols)
                .distinct()
            )
            n_must = must.count()
            others = pos_queries.join(must, on=query_cols, how="left_anti")
        else:
            must = None
            n_must = 0
            others = pos_queries
        n_others = n_pos_total - n_must

        budget = max_queries - n_must
        if budget <= 0:
            logger.warning(
                "diagnosis sample: take-all queries (%d) already exceed "
                "max_queries=%d — sample is take-all only",
                n_must, max_queries,
            )
            ratio = 0.0
            sampled = must
        elif n_others == 0:
            ratio = 0.0
            sampled = must if must is not None else pos_queries.limit(0)
        else:
            ratio = min(1.0, budget / n_others)
            threshold = ratio_to_threshold(ratio)
            picked = others.filter(
                spark_bucket(others, query_cols, seed, _SITE) < threshold
            )
            sampled = picked if must is None else picked.unionByName(must)
```

Replace the `toPandas` line with:

```python
    with log_step(logger, "diagnosis_sample.to_pandas"):
        sample_pdf = df.join(sampled, on=query_cols, how="inner").toPandas()
```

(The metadata block below `sample_pdf = ...` is unchanged.)

- [ ] **Step 6: Run the existing sample tests to verify no behaviour change**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -q
```
Expected: PASS (all pre-existing sample tests still green — wrapping in
`log_step` is behaviour-preserving).

- [ ] **Step 7: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
        src/recsys_tfb/diagnosis/metric/sample.py \
        tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(eval): free data-volume + sub-step timing for diagnosis sample"
```

---

## Task 5: Full verification (no new code)

**Files:** none (verification only).

- [ ] **Step 1: RESUME_CONTRACTS unchanged**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_resume_contracts.py -q
```
Expected: PASS. The `("evaluation", ())` contract pins only `generate_report`'s
auto-included set `{prepare_eval_data, compute_metrics, compute_baseline_metrics}`;
`generate_report` does not consume `diagnosis_sample`, so the contract is
unaffected. **If this fails, STOP and report — do not relax the contract.**

- [ ] **Step 2: Full evaluation + diagnosis suites green vs baseline**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ tests/test_diagnosis/test_metric/ \
  tests/test_pipelines/test_evaluation_compare_pipeline.py -q 2>&1 | tail -20
```
Expected: PASS, matching the Step 0 baseline (no new failures).

- [ ] **Step 3: Scope check — diff stays within planned files**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability \
  diff main --stat
```
Expected: only these paths changed —
`src/recsys_tfb/pipelines/evaluation/nodes_spark.py`,
`src/recsys_tfb/pipelines/evaluation/pipeline.py`,
`src/recsys_tfb/diagnosis/metric/sample.py`,
`tests/test_pipelines/test_evaluation/test_nodes_spark.py`,
`tests/test_pipelines/test_evaluation/test_pipeline.py`,
plus the spec/plan docs. `core/runner.py`, `core/logging.py`,
`conf/base/catalog.yaml` must NOT appear.

- [ ] **Step 4: Rebuild graphify (changed code files)**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/eval-observability
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: graph rebuilt (GRAPH_REPORT.md is untracked; no commit needed).

---

## Acceptance criteria (maps to spec §9)

- [ ] All three diagnosis consumers enabled → `draw_diagnosis_sample` called
      once per run (Task 3 Step 8 green + Step 9 mutation confirms).
- [ ] Node output byte-identical to a direct draw (Task 2 `test_node_output_equals_direct_draw`).
- [ ] Gating: all disabled → 0 draws; one enabled → 1 draw (Task 2 gating tests).
- [ ] `data_volume name=diagnosis.sample_pdf` event emitted, kind=pandas, no
      Spark count (Task 4 observability test).
- [ ] `create_pipeline` wiring pinned (Task 3 pipeline tests).
- [ ] `test_resume_contracts` green (Task 5 Step 1).
- [ ] evaluation + diagnosis.metric suites green vs baseline (Task 5 Step 2).
- [ ] `git diff --stat` within scope (Task 5 Step 3).

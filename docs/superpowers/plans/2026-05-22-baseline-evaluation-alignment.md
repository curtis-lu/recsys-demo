# Baseline / Evaluation Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the popularity baseline a structurally-aligned comparison column inside the evaluation pipeline — same snap_dates, customers, products, and labels as the model — by re-scoring `eval_predictions` with historical product purchase counts.

**Architecture:** Fold the baseline into the `evaluation` pipeline as one node `compute_baseline_metrics`. It takes the model's own `eval_predictions`, replaces the `score` column with each product's historical purchase count (`sum(label)` over `[S − lookback_months, S)`), and runs a slim metrics path (`overall` + `per_item` only). The standalone `baselines` pipeline, its CLI command, and dead pandas code are removed.

**Tech Stack:** PySpark 3.3.2, pandas 1.5.3, pytest 7.3.1, Typer.

**Spec:** `docs/superpowers/specs/2026-05-22-baseline-evaluation-alignment-design.md`

---

## Conventions for every test/run command

This work happens in the worktree. Every pytest invocation MUST use the absolute venv python + the worktree's `src` on `PYTHONPATH` (per CLAUDE.md — a bare `pytest` silently runs the main tree's code):

```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-baseline-eval-alignment/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

Below this is abbreviated as `<PYTEST>`. Run commands from the worktree root
`/Users/curtislu/projects/recsys_tfb/.worktrees/feat-baseline-eval-alignment`.
The graphify post-commit hook rebuilds the graph automatically — no manual step.

---

## Task 1: Slim metrics path — `compute_overall_per_item`

Add a public slim function to `metrics_spark.py` that computes only `overall` and
`per_item`, skipping per-segment, per-item-segment, macro_avg, category collapse,
and dataset_overview (none of which the baseline report section consumes).

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (add function after `_compute_core`, before `compute_all_metrics` at line 633)
- Test: `tests/test_evaluation/test_metrics_spark_slim.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_evaluation/test_metrics_spark_slim.py`:

```python
"""Tests for the slim metrics path: compute_overall_per_item."""

import pandas as pd


def _parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {"k_values": [1, 2, 3]},
    }


def _eval_predictions(spark):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 6,
        "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
        "prod_name": ["A", "B", "C", "A", "B", "C"],
        "label": [1, 0, 1, 0, 1, 0],
        "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
    })
    return spark.createDataFrame(pdf)


def test_returns_only_overall_and_per_item(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    result = compute_overall_per_item(_eval_predictions(spark), _parameters())

    assert set(result.keys()) == {"overall", "per_item"}


def test_matches_compute_all_metrics_subset(spark):
    from recsys_tfb.evaluation.metrics_spark import (
        compute_all_metrics,
        compute_overall_per_item,
    )

    params = _parameters()
    df = _eval_predictions(spark)

    slim = compute_overall_per_item(df, params)
    full = compute_all_metrics(_eval_predictions(spark), params)

    assert slim["overall"] == full["overall"]
    assert slim["per_item"] == full["per_item"]


def test_empty_when_no_positive_queries(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [0, 0],
        "score": [0.9, 0.1],
    })
    result = compute_overall_per_item(spark.createDataFrame(pdf), _parameters())

    assert result == {"overall": {}, "per_item": {}}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_evaluation/test_metrics_spark_slim.py`
Expected: FAIL with `ImportError: cannot import name 'compute_overall_per_item'`

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/evaluation/metrics_spark.py`, insert this function immediately
before `def compute_all_metrics(` (line 633):

```python
def compute_overall_per_item(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Slim metric bundle: ``overall`` + ``per_item`` only.

    Composes the same Layer-1/2/3 building blocks as ``_compute_core`` but
    skips per-segment, per-item-segment, macro_avg, category collapse, and
    dataset_overview. Used by the popularity baseline, whose report section
    consumes only these two keys.

    Returns ``{"overall": {...}, "per_item": {...}}``; both empty when no
    query has a positive label.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {}) or {}
    k_values_raw = eval_params.get("k_values", [5, "all"])
    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = _resolve_k_values(k_values_raw, n_products)

    df = rank_within_query(eval_predictions, group_cols, score_col)
    df = add_query_total_rel(df, group_cols, label_col)
    df_with_pos = df.filter(F.col("total_rel") > 0)
    if df_with_pos.limit(1).count() == 0:
        logger.warning("No queries with positive labels found")
        return {"overall": {}, "per_item": {}}

    enriched = add_row_contributions(
        df_with_pos, group_cols, label_col, k_values
    ).cache()
    try:
        per_query = compute_per_query_metrics(
            enriched, group_cols, label_col, k_values, carry_cols=[]
        )
        overall = aggregate_overall(per_query, k_values)
        per_item = aggregate_per_item(enriched, [item_col], label_col, k_values)
        return {"overall": overall, "per_item": per_item}
    finally:
        enriched.unpersist()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `<PYTEST> tests/test_evaluation/test_metrics_spark_slim.py`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark_slim.py
git commit -m "feat(evaluation): add slim compute_overall_per_item metrics path"
```

---

## Task 2: Rewrite `evaluation/baselines.py` — Spark popularity logic

Replace the dead pandas baseline generators with two Spark functions:
`compute_purchase_counts` (per-snap_date historical purchase counts) and
`build_baseline_frame` (re-score `eval_predictions`).

**Files:**
- Modify (full rewrite): `src/recsys_tfb/evaluation/baselines.py`
- Test (full rewrite): `tests/test_evaluation/test_baselines.py`

- [ ] **Step 1: Write the failing test**

Replace the entire contents of `tests/test_evaluation/test_baselines.py` with:

```python
"""Tests for evaluation.baselines — Spark popularity baseline."""

import pandas as pd


def _parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {},
    }


def _label_table(spark):
    # History before 2025-01-31: A bought 3x, B 1x, C 0x.
    rows = []
    for snap, a, b, c in [("2024-06-30", 2, 1, 0), ("2024-12-31", 1, 0, 0)]:
        for i in range(3):
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "A", "label": 1 if i < a else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "B", "label": 1 if i < b else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "C", "label": 1 if i < c else 0})
    return spark.createDataFrame(pd.DataFrame(rows))


def test_purchase_counts_window_excludes_snap_date_and_after(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 12, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    # 12-month window [2024-01-31, 2025-01-31): both history snaps included.
    assert by_prod["A"] == 3
    assert by_prod["B"] == 1
    assert by_prod["C"] == 0


def test_purchase_counts_lookback_limits_window(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # 3-month window [2024-10-31, 2025-01-31): only the 2024-12-31 snap.
    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 3, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


def test_purchase_counts_fallback_when_no_history(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # snap_date before all history -> empty window -> fallback to full table.
    counts = compute_purchase_counts(
        _label_table(spark), ["2024-01-01"], 12, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    assert by_prod["A"] == 3  # full table


def test_build_baseline_frame_replaces_score_and_drops_model_cols(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "cust_id": ["c1", "c1", "c2", "c2"],
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
        "score": [0.9, 0.1, 0.2, 0.8],
        "rank": [1, 2, 2, 1],
        "model_version": ["v1"] * 4,
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [5, 2],
    }))

    frame = build_baseline_frame(eval_pred, counts, _parameters())
    cols = set(frame.columns)
    assert "rank" not in cols and "model_version" not in cols
    assert "score" in cols and "label" in cols

    by_key = {(r["cust_id"], r["prod_name"]): r["score"] for r in frame.collect()}
    # Every customer gets the same per-product popularity score.
    assert by_key[("c1", "A")] == 5 and by_key[("c2", "A")] == 5
    assert by_key[("c1", "B")] == 2 and by_key[("c2", "B")] == 2


def test_build_baseline_frame_fills_missing_product_with_zero(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
        "score": [0.9, 0.1],
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"], "prod_name": ["A"], "score": [5],
    }))
    frame = build_baseline_frame(eval_pred, counts, _parameters())
    by_prod = {r["prod_name"]: r["score"] for r in frame.collect()}
    assert by_prod["A"] == 5
    assert by_prod["B"] == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_evaluation/test_baselines.py`
Expected: FAIL with `ImportError: cannot import name 'compute_purchase_counts'`

- [ ] **Step 3: Write minimal implementation**

Replace the entire contents of `src/recsys_tfb/evaluation/baselines.py` with:

```python
"""Popularity baseline for evaluation — Spark.

Replaces each ``eval_predictions`` row's model score with the product's
historical purchase count (sum of positive labels in a pre-snap_date
window), yielding a global-popularity ranking aligned row-for-row with the
model's evaluation set. See
``docs/superpowers/specs/2026-05-22-baseline-evaluation-alignment-design.md``.
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def compute_purchase_counts(
    label_table: SparkDataFrame,
    snap_dates: list[str],
    lookback_months: int,
    parameters: dict,
) -> SparkDataFrame:
    """Per ``(snap_date, prod_name)`` historical purchase count.

    For each ``S`` in ``snap_dates``, count ``sum(label)`` grouped by item
    over ``label_table`` rows whose time falls in
    ``[S - lookback_months, S)``. When a window is empty, fall back to the
    full table (with a warning — the baseline may then have leakage).

    Returns a DataFrame with columns ``(time_col, item_col, score_col)``
    where ``score_col`` holds the count and ``time_col`` is the string ``S``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    ts = F.to_date(F.col(time_col))
    per_snap: list[SparkDataFrame] = []
    for s in snap_dates:
        upper = pd.Timestamp(s)
        lower = upper - pd.DateOffset(months=lookback_months)
        window = label_table.filter(
            (ts >= F.lit(str(lower.date())))
            & (ts < F.lit(str(upper.date())))
        )
        if window.limit(1).count() == 0:
            logger.warning(
                "No historical data in [%s - %d months, %s); falling back "
                "to full label_table — baseline may have leakage.",
                s, lookback_months, s,
            )
            window = label_table
        counts = (
            window.groupBy(item_col)
            .agg(F.sum(F.col(label_col)).alias(score_col))
            .withColumn(time_col, F.lit(s))
        )
        per_snap.append(counts.select(time_col, item_col, score_col))

    result = per_snap[0]
    for df in per_snap[1:]:
        result = result.unionByName(df)
    return result


def build_baseline_frame(
    eval_predictions: SparkDataFrame,
    purchase_counts: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Replace ``eval_predictions``' model score with the popularity count.

    Drops the model's ``score`` (and ``rank`` / ``model_version`` if present),
    casts ``time_col`` to string for a type-safe join, then left-joins the
    per-``(snap_date, prod_name)`` count as the new ``score``. Products with
    no count get ``score = 0``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    rank_col = schema["rank"]

    drop_cols = [
        c for c in (score_col, rank_col, "model_version")
        if c in eval_predictions.columns
    ]
    base = eval_predictions.drop(*drop_cols).withColumn(
        time_col, F.col(time_col).cast("string")
    )
    return base.join(
        F.broadcast(purchase_counts), on=[time_col, item_col], how="left"
    ).fillna(0, subset=[score_col])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `<PYTEST> tests/test_evaluation/test_baselines.py`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/baselines.py tests/test_evaluation/test_baselines.py
git commit -m "feat(evaluation): rewrite baselines.py as Spark popularity logic"
```

---

## Task 3: Add `compute_baseline_metrics` node

Add the evaluation-pipeline node that orchestrates Task 2 + Task 1: derive
snap_dates from `eval_predictions`, compute counts, build the baseline frame,
and run the slim metrics. Returns `None` when the baseline section is disabled.

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (add function after `compute_metrics`)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py` (append a class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_evaluation/test_nodes_spark.py`:

```python
class TestComputeBaselineMetrics:
    """compute_baseline_metrics: slim baseline metrics from eval_predictions."""

    @staticmethod
    def _parameters(baseline_section=True):
        return {
            "schema": {
                "columns": {
                    "time": "snap_date",
                    "entity": ["cust_id"],
                    "item": "prod_name",
                    "label": "label",
                    "score": "score",
                    "rank": "rank",
                },
            },
            "evaluation": {
                "k_values": [1, 2, 3],
                "baseline": {"lookback_months": 12},
                "report": {"sections": {"baseline": baseline_section}},
            },
        }

    @staticmethod
    def _eval_predictions(spark):
        import pandas as pd
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2025-01-31"] * 6,
            "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
            "prod_name": ["A", "B", "C", "A", "B", "C"],
            "label": [1, 0, 1, 0, 1, 0],
            "score": [0.9, 0.5, 0.1, 0.2, 0.8, 0.3],
            "rank": [1, 2, 3, 3, 1, 2],
        }))

    @staticmethod
    def _label_table(spark):
        import pandas as pd
        rows = []
        for i in range(3):
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "A", "label": 1})
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "B", "label": 1 if i < 1 else 0})
            rows.append({"snap_date": "2024-06-30", "cust_id": f"h{i}",
                         "prod_name": "C", "label": 0})
        return spark.createDataFrame(pd.DataFrame(rows))

    def test_returns_overall_and_per_item(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            self._parameters(),
        )
        assert set(result.keys()) == {"overall", "per_item"}
        assert "A" in result["per_item"]

    def test_returns_none_when_section_disabled(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            self._parameters(baseline_section=False),
        )
        assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestComputeBaselineMetrics`
Expected: FAIL with `ImportError: cannot import name 'compute_baseline_metrics'`

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`, add this function
immediately after `compute_metrics` (it ends at line 128, before `generate_report`):

```python
def compute_baseline_metrics(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> Optional[dict]:
    """Popularity-baseline metrics, aligned row-for-row with eval_predictions.

    Re-scores each eval_predictions row with the product's historical
    purchase count, then runs the slim metrics path (overall + per_item).
    Returns None when the baseline report section is disabled — the second
    metrics pass is then skipped entirely.
    """
    from recsys_tfb.evaluation.baselines import (
        build_baseline_frame,
        compute_purchase_counts,
    )
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    eval_params = parameters.get("evaluation", {}) or {}
    sections = (eval_params.get("report", {}) or {}).get("sections", {}) or {}
    if not sections.get("baseline", True):
        logger.info(
            "Baseline report section disabled — skipping baseline metrics"
        )
        return None

    schema = get_schema(parameters)
    time_col = schema["time"]
    lookback_months = (eval_params.get("baseline", {}) or {}).get(
        "lookback_months", 12
    )

    snap_dates = [
        str(r[time_col])
        for r in eval_predictions.select(time_col).distinct().collect()
    ]
    counts = compute_purchase_counts(
        label_table, snap_dates, lookback_months, parameters
    )
    baseline_frame = build_baseline_frame(eval_predictions, counts, parameters)
    metrics = compute_overall_per_item(baseline_frame, parameters)
    logger.info(
        "Baseline metrics computed (overall + per_item) for snap_dates=%s",
        snap_dates,
    )
    return metrics
```

- [ ] **Step 4: Run test to verify it passes**

Run: `<PYTEST> tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestComputeBaselineMetrics`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(evaluation): add compute_baseline_metrics node"
```

---

## Task 4: Wire the node into the evaluation pipeline

Add the node to the pipeline graph so `baseline_metrics` flows into
`generate_report`, and remove the now-obsolete `baseline_metrics=None`
injection from `__main__.py`.

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `src/recsys_tfb/__main__.py:129-132`
- Test: `tests/test_pipelines/test_evaluation/test_pipeline.py` (update)

- [ ] **Step 1: Update the failing test**

In `tests/test_pipelines/test_evaluation/test_pipeline.py`, replace the
`TestEvaluationPipelineDefault` class body and the post-training node-count
tests so they expect four nodes including `compute_baseline_metrics`:

```python
class TestEvaluationPipelineDefault:
    """Default (post_training=False) — monitoring scenario."""

    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 4

    def test_pipeline_reads_ranked_predictions(self):
        pipeline = create_pipeline()
        assert "ranked_predictions" in pipeline.inputs
        assert "training_eval_predictions" not in pipeline.inputs

    def test_pipeline_outputs_unchanged(self):
        pipeline = create_pipeline()
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "compute_metrics",
            "compute_baseline_metrics", "generate_report",
        ]
```

And in `TestEvaluationPipelinePostTraining` replace `test_pipeline_has_three_nodes`
with:

```python
    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline(post_training=True)
        assert len(pipeline.nodes) == 4
```

and update `test_pipeline_outputs_same_as_default`'s `expected` set to:

```python
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
        }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_pipelines/test_evaluation/test_pipeline.py`
Expected: FAIL — node count is 3, `baseline_metrics` not in outputs.

- [ ] **Step 3: Write minimal implementation**

Replace `src/recsys_tfb/pipelines/evaluation/pipeline.py` with:

```python
"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(post_training: bool = False) -> Pipeline:
    """Build the evaluation pipeline.

    Args:
        post_training: When True, read predictions from `training_eval_predictions`
            (post-training evaluation). When False (default), read from
            `ranked_predictions` (monthly monitoring). Mirrors
            training/pipeline.py::create_pipeline(enable_calibration=...).
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_baseline_metrics,
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )

    return Pipeline(
        [
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
                inputs=[
                    "eval_predictions",
                    "evaluation_metrics",
                    "parameters",
                    "baseline_metrics",
                ],
                outputs="evaluation_report",
            ),
        ]
    )
```

Then in `src/recsys_tfb/__main__.py`, delete these four lines (currently 129-132):

```python
    if pipeline_name == "evaluation":
        if not catalog.exists("baseline_metrics"):
            catalog.add("baseline_metrics", MemoryDataset(data=None))
            logger.info("No baseline_metrics found — report will skip baseline comparison")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `<PYTEST> tests/test_pipelines/test_evaluation/`
Expected: PASS (all evaluation pipeline tests green)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/pipeline.py src/recsys_tfb/__main__.py tests/test_pipelines/test_evaluation/test_pipeline.py
git commit -m "feat(evaluation): wire compute_baseline_metrics into the pipeline"
```

---

## Task 5: Remove the standalone `baselines` pipeline

Delete the obsolete standalone pipeline, its CLI command, registry entry,
and tests. Nothing reads them after Task 4.

**Files:**
- Delete: `src/recsys_tfb/pipelines/baselines/` (whole directory)
- Delete: `tests/test_pipelines/test_baselines/` (whole directory)
- Modify: `src/recsys_tfb/pipelines/__init__.py` (remove registry entry)
- Modify: `src/recsys_tfb/__main__.py` (remove `baselines` command, lines 649-693)
- Test: `tests/test_pipelines/test_registry.py` (update)

- [ ] **Step 1: Update the failing test**

In `tests/test_pipelines/test_registry.py`, extend `test_list_pipelines`:

```python
    def test_list_pipelines(self):
        names = list_pipelines()
        assert "dataset" in names
        assert "training" in names
        assert "baselines" not in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_pipelines/test_registry.py::TestPipelineRegistry::test_list_pipelines`
Expected: FAIL — `"baselines"` is still registered.

- [ ] **Step 3: Apply the removals**

Delete the directories:

```bash
git rm -r src/recsys_tfb/pipelines/baselines tests/test_pipelines/test_baselines
```

In `src/recsys_tfb/pipelines/__init__.py`, remove this line from `_REGISTRY`:

```python
    "baselines": "recsys_tfb.pipelines.baselines",
```

In `src/recsys_tfb/__main__.py`, delete the entire `baselines` command —
the `@app.command(name="baselines")` decorator, the `def baselines(...)`
function, and its body (currently lines 649-693, ending with
`logger.info("Pipeline 'baselines' completed successfully")`). Also delete
the blank line separating it from the preceding command.

- [ ] **Step 4: Run tests to verify they pass**

Run: `<PYTEST> tests/test_pipelines/test_registry.py`
Expected: PASS

Then verify the CLI command is gone and `__main__` still imports cleanly:

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-baseline-eval-alignment/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb --help
```
Expected: help text lists `dataset`, `training`, `inference`, `evaluation` —
NOT `baselines`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(evaluation): remove standalone baselines pipeline and CLI"
```

---

## Task 6: Simplify the `baseline` config block

Collapse `evaluation.baseline` to just `lookback_months` — `type` and
`segment_column` are dead after Task 5.

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml:34-38`
- Test: `tests/test_evaluation/test_parameters_evaluation_yaml.py` (append)

- [ ] **Step 1: Write the failing test**

First confirm the loader fixture name used in the file:

```bash
grep -n "def _load\|yaml.safe_load\|parameters_evaluation" tests/test_evaluation/test_parameters_evaluation_yaml.py | head
```

Append a test that matches the file's existing load pattern. If the file
loads the YAML via a module-level `_load()` helper returning the parsed dict,
add:

```python
def test_baseline_block_is_lookback_only():
    cfg = _load()["evaluation"]["baseline"]
    assert cfg == {"lookback_months": 12}
```

(If the file's load helper has a different name, use that name instead — the
assertion content is unchanged.)

- [ ] **Step 2: Run test to verify it fails**

Run: `<PYTEST> tests/test_evaluation/test_parameters_evaluation_yaml.py::test_baseline_block_is_lookback_only`
Expected: FAIL — block still has `type` and `segment_column`.

- [ ] **Step 3: Apply the config change**

In `conf/base/parameters_evaluation.yaml`, replace:

```yaml
  # Baseline configuration
  baseline:
    type: global_popularity
    segment_column: cust_segment_typ
    lookback_months: 12
```

with:

```yaml
  # Baseline configuration — popularity baseline lookback window (months).
  baseline:
    lookback_months: 12
```

- [ ] **Step 4: Run test to verify it passes**

Run: `<PYTEST> tests/test_evaluation/test_parameters_evaluation_yaml.py`
Expected: PASS (all tests in the file green)

- [ ] **Step 5: Commit**

```bash
git add conf/base/parameters_evaluation.yaml tests/test_evaluation/test_parameters_evaluation_yaml.py
git commit -m "chore(evaluation): collapse baseline config to lookback_months"
```

---

## Task 7: Full evaluation-suite verification

Confirm the whole evaluation surface is green after all changes.

- [ ] **Step 1: Run the evaluation + evaluation-pipeline test packages**

Run in the background (may exceed 2 minutes — per CLAUDE.md):

```
<PYTEST> tests/test_evaluation/ tests/test_pipelines/test_evaluation/ tests/test_pipelines/test_registry.py
```
Expected: all PASS, no errors, no references to removed `baselines` modules.

- [ ] **Step 2: If any test fails**

Investigate with the systematic-debugging skill. Do NOT mark the plan
complete on a red suite.

- [ ] **Step 3: Final commit (only if Step 1–2 produced fixes)**

```bash
git add -A
git commit -m "test(evaluation): fixups from full-suite verification"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Spec §3.1 (re-score eval_predictions) → Task 2 `build_baseline_frame` + Task 3 node.
- Spec §3.2 (申購數 historical window + fallback) → Task 2 `compute_purchase_counts`.
- Spec §3.3 (score = count, drop model cols) → Task 2 `build_baseline_frame`.
- Spec §3.4 (slim overall + per_item) → Task 1 `compute_overall_per_item`.
- Spec §3.5 (section gating → None) → Task 3 node early return.
- Spec §4.1 (new/rewritten files) → Tasks 1, 2, 3, 4.
- Spec §4.2 (removals) → Tasks 4 (`__main__` injection) + 5 (pipeline, CLI, registry, tests).
- Spec §4.3 (config) → Task 6.
- Spec §5 (pipeline wiring) → Task 4.
- Spec §8 (testing) → tests embedded in every task + Task 7 full-suite run.

**Placeholder scan:** No TBD/TODO. Task 6 Step 1 leaves the loader-helper name
to be confirmed by grep — this is a deliberate, bounded lookup (the file's own
convention), not a code placeholder; the assertion content is fully specified.

**Type consistency:** `compute_overall_per_item`, `compute_purchase_counts`,
`build_baseline_frame`, `compute_baseline_metrics` — names and signatures are
identical across the tasks that define and call them. `baseline_metrics` is the
single dataset name used in pipeline outputs and `generate_report` inputs.

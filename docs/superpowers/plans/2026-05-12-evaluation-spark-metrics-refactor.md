# Evaluation Spark Metrics Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `compute_metrics` fat function in `nodes_spark.py` with a clean Spark-native pipeline in a new `evaluation/metrics_spark.py` module; row-level data never leaves Spark, only small aggregated dicts are collected.

**Architecture:** 7 single-responsibility functions split into pipeline stages (`df → df` enrichers) + aggregators (`df → small dict`) + a thin orchestrator. `nodes_spark.compute_metrics` becomes a ~15-line wrapper. pandas `evaluation/metrics.py` is untouched.

**Tech Stack:** PySpark 3.3.2 (Window functions + `aggregate` / `sequence` higher-order functions, no UDF), pytest 7.3.1, pandas 1.5.3 (for parity test ground truth).

**Spec:** `docs/superpowers/specs/2026-05-12-evaluation-spark-metrics-refactor-design.md`

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/recsys_tfb/evaluation/metrics_spark.py` | All Spark-native metric functions + orchestrator |
| Modify | `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | `compute_metrics` shrinks to a thin wrapper; remove stale imports |
| Create | `tests/test_evaluation/test_metrics_spark.py` | Unit + integration + parity tests |

The `spark` fixture is already provided session-scope by `tests/conftest.py`. All test code in this plan assumes it is available.

---

## Task 1: Scaffold `metrics_spark.py` + test file

**Files:**
- Create: `src/recsys_tfb/evaluation/metrics_spark.py`
- Create: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing import-smoke test**

`tests/test_evaluation/test_metrics_spark.py`:

```python
"""Tests for evaluation.metrics_spark module."""


def test_module_imports():
    """Verify the new module imports without errors."""
    from recsys_tfb.evaluation import metrics_spark  # noqa: F401
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py::test_module_imports -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'recsys_tfb.evaluation.metrics_spark'`

- [ ] **Step 3: Create the scaffold module**

`src/recsys_tfb/evaluation/metrics_spark.py`:

```python
"""Spark-native ranking metrics computation.

Pipeline:
    eval_predictions (SparkDataFrame, joined predictions + labels)
      → rank_within_query        (Window: pos)
      → add_query_aggregates     (Window: total_rel; caller filters total_rel > 0)
      → add_row_contributions    (cum_rel, prec_at_pos, dcg_term,
                                  top_k@K, ap_contrib@K, ndcg_contrib@K)
      → aggregate_overall            (collect: small dict)
      → aggregate_by_row_dimension   (collect: per-product / per-product-segment)
      → aggregate_by_query_dimension (collect: per-segment, equal customer weight)
      → macro_average (python dict op, reused from metrics.py)

All row-level work stays in Spark; only small aggregations are collected.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import _macro_average, _resolve_k_values

logger = logging.getLogger(__name__)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py::test_module_imports -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): scaffold Spark-native metrics module"
```

---

## Task 2: `rank_within_query`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_rank_within_query_assigns_1_based_pos(spark):
    from recsys_tfb.evaluation.metrics_spark import rank_within_query

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 0.5),
            ("20240331", "C0", 0.9),
            ("20240331", "C0", 0.1),
            ("20240331", "C1", 0.8),
            ("20240331", "C1", 0.3),
        ],
        schema=["snap_date", "cust_id", "score"],
    )
    result = rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    by_score = {(r["cust_id"], r["score"]): r["pos"] for r in result}
    # C0: 0.9 → 1, 0.5 → 2, 0.1 → 3
    assert by_score[("C0", 0.9)] == 1
    assert by_score[("C0", 0.5)] == 2
    assert by_score[("C0", 0.1)] == 3
    # C1: 0.8 → 1, 0.3 → 2
    assert by_score[("C1", 0.8)] == 1
    assert by_score[("C1", 0.3)] == 2


def test_rank_within_query_independent_groups(spark):
    """pos is per-query, not global."""
    from recsys_tfb.evaluation.metrics_spark import rank_within_query

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 0.9),
            ("20240331", "C1", 0.9),
        ],
        schema=["snap_date", "cust_id", "score"],
    )
    result = rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    # Both rows get pos=1 within their own query.
    assert all(r["pos"] == 1 for r in result)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k rank_within_query -v`

Expected: FAIL with `ImportError: cannot import name 'rank_within_query'`

- [ ] **Step 3: Implement `rank_within_query`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def rank_within_query(
    df: SparkDataFrame, group_cols: list[str], score_col: str
) -> SparkDataFrame:
    """Add `pos` column: 1-based rank within each query, ordered by score desc."""
    w = Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())
    return df.withColumn("pos", F.row_number().over(w))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k rank_within_query -v`

Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): add rank_within_query Spark stage"
```

---

## Task 3: `add_query_aggregates`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_add_query_aggregates_total_rel_per_query(spark):
    from recsys_tfb.evaluation.metrics_spark import add_query_aggregates

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 1),
            ("20240331", "C0", 0),
            ("20240331", "C0", 1),
            ("20240331", "C1", 0),
            ("20240331", "C1", 0),
            ("20240331", "C2", 1),
        ],
        schema=["snap_date", "cust_id", "label"],
    )
    result = add_query_aggregates(df, ["snap_date", "cust_id"], "label").collect()
    by_cust = {r["cust_id"]: r["total_rel"] for r in result}
    # Same value should repeat across all rows of the same query.
    assert by_cust["C0"] == 2
    assert by_cust["C1"] == 0
    assert by_cust["C2"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k add_query_aggregates -v`

Expected: FAIL with `ImportError: cannot import name 'add_query_aggregates'`

- [ ] **Step 3: Implement `add_query_aggregates`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def add_query_aggregates(
    df: SparkDataFrame, group_cols: list[str], label_col: str
) -> SparkDataFrame:
    """Add `total_rel`: sum of label per query. Caller filters total_rel > 0 later."""
    w = Window.partitionBy(*group_cols)
    return df.withColumn("total_rel", F.sum(F.col(label_col)).over(w))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k add_query_aggregates -v`

Expected: PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): add add_query_aggregates Spark stage"
```

---

## Task 4: `add_row_contributions` — basic columns

This task adds `cum_rel`, `prec_at_pos`, `dcg_term`, and per-K `top_k@K`, `ap_contrib@K`. iDCG-dependent `ndcg_contrib@K` is added in Task 5.

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def _basic_enriched_input(spark):
    """A query (C0) of 3 items already ranked by score (pos column included)."""
    return spark.createDataFrame(
        [
            # snap_date, cust_id, prod, score, label, pos, total_rel
            ("20240331", "C0", "A", 0.9, 1, 1, 2),
            ("20240331", "C0", "B", 0.5, 0, 2, 2),
            ("20240331", "C0", "C", 0.1, 1, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )


def test_add_row_contributions_basic_columns(spark):
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = _basic_enriched_input(spark)
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()

    # cum_rel: 1, 1, 2
    assert [r["cum_rel"] for r in result] == [1, 1, 2]
    # prec_at_pos: 1/1, 1/2, 2/3
    assert result[0]["prec_at_pos"] == 1.0
    assert result[1]["prec_at_pos"] == 0.5
    assert abs(result[2]["prec_at_pos"] - 2 / 3) < 1e-12
    # dcg_term: label / log2(pos+1) → 1/log2(2)=1.0, 0/log2(3)=0, 1/log2(4)=0.5
    assert result[0]["dcg_term"] == 1.0
    assert result[1]["dcg_term"] == 0.0
    assert result[2]["dcg_term"] == 0.5
    # top_k@3: all in top 3
    assert all(r["top_k@3"] == 1.0 for r in result)
    # ap_contrib@3 = prec_at_pos * label * top_k → 1.0, 0, 2/3
    assert result[0]["ap_contrib@3"] == 1.0
    assert result[1]["ap_contrib@3"] == 0.0
    assert abs(result[2]["ap_contrib@3"] - 2 / 3) < 1e-12


def test_add_row_contributions_top_k_cutoff(spark):
    """top_k@2 should be 0 for pos > 2; ap_contrib@2 should follow."""
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = _basic_enriched_input(spark)
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[2]
    ).orderBy("pos").collect()

    assert [r["top_k@2"] for r in result] == [1.0, 1.0, 0.0]
    # pos 3 was a hit (label=1) but cut off by top_k@2 → ap_contrib@2 = 0
    assert result[2]["ap_contrib@2"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k add_row_contributions -v`

Expected: FAIL with `ImportError: cannot import name 'add_row_contributions'`

- [ ] **Step 3: Implement `add_row_contributions` (without ndcg_contrib yet)**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def add_row_contributions(
    df: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> SparkDataFrame:
    """Add per-row contribution columns for ranking metrics.

    Requires upstream columns: pos, total_rel.

    Adds (always):
        cum_rel:      cumulative positive count up to & including this position
        prec_at_pos:  cum_rel / pos
        dcg_term:     label / log2(pos + 1)

    Adds (per K in k_values):
        top_k@{K}:        1.0 if pos <= K else 0.0
        ap_contrib@{K}:   prec_at_pos * label * top_k@{K}
        ndcg_contrib@{K}: dcg_term * top_k@{K} / iDCG@{K}    (added in Task 5)
    """
    w_cum = (
        Window.partitionBy(*group_cols)
        .orderBy(F.col("pos"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn("cum_rel", F.sum(F.col(label_col)).over(w_cum))
    df = df.withColumn("prec_at_pos", F.col("cum_rel") / F.col("pos"))
    df = df.withColumn(
        "dcg_term", F.col(label_col) / F.log2(F.col("pos") + F.lit(1))
    )

    for k in k_values:
        df = df.withColumn(
            f"top_k@{k}", (F.col("pos") <= F.lit(k)).cast("double")
        )
        df = df.withColumn(
            f"ap_contrib@{k}",
            F.col("prec_at_pos") * F.col(label_col) * F.col(f"top_k@{k}"),
        )
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k add_row_contributions -v`

Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): add_row_contributions — base columns + ap_contrib"
```

---

## Task 5: `add_row_contributions` — extend with `ndcg_contrib@K` and iDCG

iDCG@K is computed inline with Spark's `aggregate(sequence(1, least(total_rel, K)), 0.0, lambda)` (Spark 3.1+), avoiding UDF and avoiding any collect.

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (extend existing function)
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_add_row_contributions_ndcg_contrib_perfect_ranking(spark):
    """Two positives at pos 1,2; K=3, total_rel=2.

    iDCG@3 = 1/log2(2) + 1/log2(3) = 1.0 + 0.6309... = 1.6309...
    nDCG contributions only at pos 1,2 (label=1): 1.0/iDCG and (1/log2(3))/iDCG.
    Sum of ndcg_contrib@3 over query = iDCG/iDCG = 1.0  → perfect ranking nDCG=1.
    """
    import math
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1, 1, 2),
            ("20240331", "C0", "B", 0.5, 1, 2, 2),
            ("20240331", "C0", "C", 0.1, 0, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()

    idcg3 = 1.0 / math.log2(2) + 1.0 / math.log2(3)
    assert abs(result[0]["ndcg_contrib@3"] - (1.0 / math.log2(2)) / idcg3) < 1e-9
    assert abs(result[1]["ndcg_contrib@3"] - (1.0 / math.log2(3)) / idcg3) < 1e-9
    assert result[2]["ndcg_contrib@3"] == 0.0  # label=0
    total = sum(r["ndcg_contrib@3"] for r in result)
    assert abs(total - 1.0) < 1e-9


def test_add_row_contributions_ndcg_contrib_outside_top_k(spark):
    """K=1: only first row contributes; positive at pos 2 is cut off."""
    import math
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 0, 1, 1),
            ("20240331", "C0", "B", 0.5, 1, 2, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[1]
    ).orderBy("pos").collect()

    # iDCG@1 with total_rel=1: 1/log2(2) = 1.0
    # pos 1 (label=0): dcg_term=0, ndcg_contrib@1 = 0
    # pos 2 (label=1): top_k@1=0 → ndcg_contrib@1 = 0
    assert result[0]["ndcg_contrib@1"] == 0.0
    assert result[1]["ndcg_contrib@1"] == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k ndcg_contrib -v`

Expected: FAIL — `KeyError: 'ndcg_contrib@3'` (column not present)

- [ ] **Step 3: Extend `add_row_contributions` to add `ndcg_contrib@K`**

Modify `add_row_contributions` in `src/recsys_tfb/evaluation/metrics_spark.py` so the per-K loop body becomes:

```python
    for k in k_values:
        df = df.withColumn(
            f"top_k@{k}", (F.col("pos") <= F.lit(k)).cast("double")
        )
        df = df.withColumn(
            f"ap_contrib@{k}",
            F.col("prec_at_pos") * F.col(label_col) * F.col(f"top_k@{k}"),
        )
        # iDCG@K = sum_{i=1}^{min(total_rel, K)} 1 / log2(i + 1)
        # Computed inline via Spark's aggregate(sequence(...)) higher-order function.
        # No UDF, no collect-and-broadcast.
        idcg_at_k = F.aggregate(
            F.sequence(F.lit(1), F.least(F.col("total_rel"), F.lit(k))),
            F.lit(0.0),
            lambda acc, i: acc + F.lit(1.0) / F.log2(i.cast("double") + F.lit(1.0)),
        )
        df = df.withColumn(
            f"ndcg_contrib@{k}",
            F.when(
                idcg_at_k > 0,
                F.col("dcg_term") * F.col(f"top_k@{k}") / idcg_at_k,
            ).otherwise(F.lit(0.0)),
        )
    return df
```

- [ ] **Step 4: Run all add_row_contributions tests to verify pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k add_row_contributions -v`

Expected: 4 PASSED (the 2 from Task 4 + 2 new)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): add ndcg_contrib@K with inline iDCG via Spark aggregate()"
```

---

## Task 6: `aggregate_overall`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def _full_enriched(spark, k_values=(3,)):
    """End-to-end enriched DF for 2 customers, 3 products, ready for aggregators."""
    from recsys_tfb.evaluation.metrics_spark import (
        add_query_aggregates,
        add_row_contributions,
        rank_within_query,
    )

    raw = spark.createDataFrame(
        [
            # C0: A(score 0.9, label 1), B(0.5, 0), C(0.1, 1)
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C0", "C", 0.1, 1),
            # C1: B(0.8, 1), C(0.6, 0), A(0.3, 0)
            ("20240331", "C1", "A", 0.3, 0),
            ("20240331", "C1", "B", 0.8, 1),
            ("20240331", "C1", "C", 0.6, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    group_cols = ["snap_date", "cust_id"]
    df = rank_within_query(raw, group_cols, "score")
    df = add_query_aggregates(df, group_cols, "label")
    df = df.filter(F.col("total_rel") > 0)
    df = add_row_contributions(df, group_cols, "label", list(k_values))
    return df


def test_aggregate_overall_returns_expected_keys(spark):
    from pyspark.sql import functions as F  # noqa: F401 (used inside _full_enriched)
    from recsys_tfb.evaluation.metrics_spark import aggregate_overall

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_overall(enriched, ["snap_date", "cust_id"], "label", [3])
    assert set(result.keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def test_aggregate_overall_known_values(spark):
    """Hand-computed values.

    C0: ranking A(1) B(2) C(3), labels [1,0,1], total_rel=2
        AP@3 = (1/1 + 2/3) / 2 = 5/6
        precision@3 = 2/3, recall@3 = 2/2 = 1
    C1: ranking B(1) C(2) A(3), labels [1,0,0], total_rel=1
        AP@3 = 1/1 / 1 = 1.0
        precision@3 = 1/3, recall@3 = 1
    Overall = mean over queries:
        map@3 = (5/6 + 1.0) / 2 = 11/12
        precision@3 = (2/3 + 1/3) / 2 = 0.5
        recall@3 = 1.0
    """
    import math
    from recsys_tfb.evaluation.metrics_spark import aggregate_overall

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_overall(enriched, ["snap_date", "cust_id"], "label", [3])
    assert abs(result["map@3"] - 11 / 12) < 1e-9
    assert abs(result["precision@3"] - 0.5) < 1e-9
    assert abs(result["recall@3"] - 1.0) < 1e-9
    # nDCG@3 sanity: must be between 0 and 1
    assert 0 < result["ndcg@3"] <= 1.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_overall -v`

Expected: FAIL with `ImportError: cannot import name 'aggregate_overall'`

- [ ] **Step 3: Implement `aggregate_overall`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def aggregate_overall(
    enriched: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-query metrics → cross-query mean.

    Per-query formulas:
        ap@K        = sum(ap_contrib@K) / total_rel
        ndcg@K      = sum(ndcg_contrib@K)              -- already iDCG-normalized
        precision@K = sum(label * top_k@K) / K
        recall@K    = sum(label * top_k@K) / total_rel

    Overall metric@K = mean across queries.
    Returns a flat dict {"map@K": ..., "ndcg@K": ..., "precision@K": ..., "recall@K": ...}.
    """
    per_query_aggs = [F.first("total_rel").alias("total_rel")]
    for k in k_values:
        per_query_aggs.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(f"ndcg_contrib@{k}").alias(f"_ndcg_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )
    per_query = enriched.groupBy(*group_cols).agg(*per_query_aggs)

    for k in k_values:
        per_query = (
            per_query.withColumn(
                f"ap_{k}", F.col(f"_ap_sum_{k}") / F.col("total_rel")
            )
            .withColumn(f"ndcg_{k}", F.col(f"_ndcg_sum_{k}"))
            .withColumn(f"precision_{k}", F.col(f"_hits_{k}") / F.lit(k))
            .withColumn(f"recall_{k}", F.col(f"_hits_{k}") / F.col("total_rel"))
        )

    final_aggs = []
    for k in k_values:
        final_aggs.extend(
            [
                F.mean(f"ap_{k}").alias(f"map@{k}"),
                F.mean(f"ndcg_{k}").alias(f"ndcg@{k}"),
                F.mean(f"precision_{k}").alias(f"precision@{k}"),
                F.mean(f"recall_{k}").alias(f"recall@{k}"),
            ]
        )
    row = per_query.agg(*final_aggs).collect()[0].asDict()
    return {k: float(v) for k, v in row.items()}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_overall -v`

Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): aggregate_overall — per-query → cross-query mean"
```

---

## Task 7: `aggregate_by_row_dimension`

For per-product / per-product-segment: filter to label=1 rows, groupBy(dim_cols), mean of contributions.

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_aggregate_by_row_dimension_keyed_by_dim_value(spark):
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    # 3 products, but only A, B, C had label=1 somewhere; check keys are strings.
    assert set(result.keys()) == {"A", "B", "C"}
    for prod, metrics in result.items():
        assert set(metrics.keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def test_aggregate_by_row_dimension_known_values(spark):
    """Same fixture as aggregate_overall.

    Per-product label=1 rows:
      A: only C0 (label=1 at pos 1) → prec_at_pos=1.0 → map@3 = 1.0
      B: only C1 (label=1 at pos 1) → prec_at_pos=1.0 → map@3 = 1.0
      C: only C0 (label=1 at pos 3) → prec_at_pos=2/3 → map@3 = 2/3
    """
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    assert abs(result["A"]["map@3"] - 1.0) < 1e-9
    assert abs(result["B"]["map@3"] - 1.0) < 1e-9
    assert abs(result["C"]["map@3"] - 2 / 3) < 1e-9
    # precision@K == recall@K == mean(top_k@K) for matched rows (matches pandas semantic)
    for prod in result:
        assert result[prod]["precision@3"] == result[prod]["recall@3"]


def test_aggregate_by_row_dimension_filters_to_label_1(spark):
    """label=0 rows contribute nothing; A's metrics should not be diluted by C1's A (label=0)."""
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    # A only has label=1 at C0 pos 1, so map@3 must be exactly 1.0 (not diluted).
    assert result["A"]["map@3"] == 1.0


def test_aggregate_by_row_dimension_multi_column_key(spark):
    """Multi-column dim → key is '_'.join of values."""
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    # Add a segment column to the input.
    enriched = _full_enriched(spark, k_values=[3])
    enriched = enriched.withColumn(
        "seg", F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent"))
    )
    result = aggregate_by_row_dimension(enriched, ["prod_name", "seg"], "label", [3])
    # Only label=1 rows: (A, mass), (B, affluent), (C, mass)
    assert set(result.keys()) == {"A_mass", "B_affluent", "C_mass"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_by_row_dimension -v`

Expected: FAIL with `ImportError: cannot import name 'aggregate_by_row_dimension'`

- [ ] **Step 3: Implement `aggregate_by_row_dimension`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def aggregate_by_row_dimension(
    enriched: SparkDataFrame,
    dim_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-product / per-product-segment metrics.

    Filters to label=1 rows, groupBy(dim_cols), takes mean of contribution columns.

    Returns {dim_key: {metric_name: value}}.
    dim_key is the dim column value (stringified) for single-column groupings,
    or '_'.join(values) for multi-column groupings.

    Per-dimension formulas (over label=1 rows in the dim):
        map@K       = mean(ap_contrib@K)
        ndcg@K      = mean(ndcg_contrib@K)
        precision@K = mean(top_k@K)        -- same value as recall@K (matches pandas semantic)
        recall@K    = mean(top_k@K)
    """
    rel = enriched.filter(F.col(label_col) == 1)
    aggs = []
    for k in k_values:
        aggs.extend(
            [
                F.mean(f"ap_contrib@{k}").alias(f"map@{k}"),
                F.mean(f"ndcg_contrib@{k}").alias(f"ndcg@{k}"),
                F.mean(f"top_k@{k}").alias(f"hit_rate@{k}"),
            ]
        )
    rows = rel.groupBy(*dim_cols).agg(*aggs).collect()

    result: dict = {}
    for row in rows:
        if len(dim_cols) == 1:
            raw_key = row[dim_cols[0]]
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
        else:
            key = "_".join(str(row[c]) for c in dim_cols)
        metrics: dict = {}
        for k in k_values:
            hit_rate = float(row[f"hit_rate@{k}"])
            metrics[f"map@{k}"] = float(row[f"map@{k}"])
            metrics[f"ndcg@{k}"] = float(row[f"ndcg@{k}"])
            metrics[f"precision@{k}"] = hit_rate
            metrics[f"recall@{k}"] = hit_rate
        result[key] = metrics
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_by_row_dimension -v`

Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): aggregate_by_row_dimension for per-product/per-product-segment"
```

---

## Task 8: `aggregate_by_query_dimension`

For per-segment: per-query metrics first, then groupBy(seg).mean — equal customer weight (matches pandas semantic).

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_aggregate_by_query_dimension_equal_customer_weight(spark):
    """C0 in 'mass', C1 in 'affluent'.

    Per-query AP@3:  C0 = 5/6,  C1 = 1.0  (from aggregate_overall fixture).
    Per-segment:
        mass     → mean over {C0} = 5/6
        affluent → mean over {C1} = 1.0
    """
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_query_dimension

    enriched = _full_enriched(spark, k_values=[3])
    enriched = enriched.withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    result = aggregate_by_query_dimension(
        enriched, "seg", ["snap_date", "cust_id"], "label", [3]
    )
    assert set(result.keys()) == {"mass", "affluent"}
    assert abs(result["mass"]["map@3"] - 5 / 6) < 1e-9
    assert abs(result["affluent"]["map@3"] - 1.0) < 1e-9
    for seg in result:
        assert set(result[seg].keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_by_query_dimension -v`

Expected: FAIL with `ImportError: cannot import name 'aggregate_by_query_dimension'`

- [ ] **Step 3: Implement `aggregate_by_query_dimension`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def aggregate_by_query_dimension(
    enriched: SparkDataFrame,
    dim_col: str,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict:
    """Per-segment metrics with equal customer weighting.

    Two-stage:
        1. groupBy(group_cols).agg(per-query formulas + first(dim_col))  -- one row per query
        2. groupBy(dim_col).mean(per-query metrics)                       -- equal customer weight

    Matches the pandas per_segment semantic (equal customer weight, not row-level mean).
    """
    per_query_aggs = [
        F.first("total_rel").alias("total_rel"),
        F.first(dim_col).alias(dim_col),
    ]
    for k in k_values:
        per_query_aggs.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(f"ndcg_contrib@{k}").alias(f"_ndcg_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )
    per_query = enriched.groupBy(*group_cols).agg(*per_query_aggs)

    metric_aliases = []
    for k in k_values:
        per_query = (
            per_query.withColumn(
                f"map@{k}", F.col(f"_ap_sum_{k}") / F.col("total_rel")
            )
            .withColumn(f"ndcg@{k}", F.col(f"_ndcg_sum_{k}"))
            .withColumn(f"precision@{k}", F.col(f"_hits_{k}") / F.lit(k))
            .withColumn(f"recall@{k}", F.col(f"_hits_{k}") / F.col("total_rel"))
        )
        metric_aliases.extend(
            [f"map@{k}", f"ndcg@{k}", f"precision@{k}", f"recall@{k}"]
        )

    final_aggs = [F.mean(m).alias(m) for m in metric_aliases]
    rows = per_query.groupBy(dim_col).agg(*final_aggs).collect()

    result: dict = {}
    for row in rows:
        raw_key = row[dim_col]
        key = raw_key if isinstance(raw_key, str) else str(raw_key)
        result[key] = {m: float(row[m]) for m in metric_aliases}
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k aggregate_by_query_dimension -v`

Expected: PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): aggregate_by_query_dimension for per-segment (equal customer weight)"
```

---

## Task 9: `compute_all_metrics` orchestrator (with integration tests)

This wires Stage A → Stage B into the top-level function, plus result-dict assembly and edge cases (early return when no positives, optional segment column).

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def _make_parameters(k_values=(3,), segment_columns=()):
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
            "k_values": list(k_values),
            "segment_columns": list(segment_columns),
        },
    }


def _make_eval_predictions(spark, with_segment=False):
    rows = [
        # C0: A(0.9, 1), B(0.5, 0), C(0.1, 1)
        ("20240331", "C0", "A", 0.9, 1, "mass"),
        ("20240331", "C0", "B", 0.5, 0, "mass"),
        ("20240331", "C0", "C", 0.1, 1, "mass"),
        # C1: B(0.8, 1), C(0.6, 0), A(0.3, 0)
        ("20240331", "C1", "A", 0.3, 0, "affluent"),
        ("20240331", "C1", "B", 0.8, 1, "affluent"),
        ("20240331", "C1", "C", 0.6, 0, "affluent"),
    ]
    schema_cols = ["snap_date", "cust_id", "prod_name", "score", "label"]
    if with_segment:
        schema_cols = schema_cols + ["cust_segment_typ"]
    else:
        rows = [r[:5] for r in rows]
    return spark.createDataFrame(rows, schema=schema_cols)


def test_compute_all_metrics_returns_expected_keys(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert set(result.keys()) == {
        "overall",
        "per_product",
        "per_segment",
        "per_product_segment",
        "macro_avg",
        "n_queries",
        "n_excluded_queries",
    }


def test_compute_all_metrics_per_product_map_known_values(spark):
    """Mirrors pandas test_per_product_map_known_values:
        per_product["A"].map@3 == 1.0
        per_product["B"].map@3 == 1.0
        per_product["C"].map@3 == 2/3
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters(k_values=[3])
    result = compute_all_metrics(eval_df, params)
    pp = result["per_product"]
    assert abs(pp["A"]["map@3"] - 1.0) < 1e-9
    assert abs(pp["B"]["map@3"] - 1.0) < 1e-9
    assert abs(pp["C"]["map@3"] - 2 / 3) < 1e-9


def test_compute_all_metrics_no_segment_column(spark):
    """No segment column in df → per_segment / per_product_segment are empty."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert result["per_segment"] == {}
    assert result["per_product_segment"] == {}
    assert "by_segment" not in result["macro_avg"]
    assert "by_product_segment" not in result["macro_avg"]
    assert "by_product" in result["macro_avg"]


def test_compute_all_metrics_with_segment_column(spark):
    """Segment column present → per_segment / per_product_segment populated."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert set(result["per_segment"].keys()) == {"mass", "affluent"}
    assert "by_segment" in result["macro_avg"]
    assert "by_product_segment" in result["macro_avg"]


def test_compute_all_metrics_excluded_queries_counted(spark):
    """A query with no positives is excluded; n_excluded_queries reflects that."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    # C2 has no positives.
    eval_df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C2", "A", 0.9, 0),
            ("20240331", "C2", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = _make_parameters(k_values=[2])
    result = compute_all_metrics(eval_df, params)
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 1


def test_compute_all_metrics_default_k_values_resolves_all(spark):
    """k_values defaults to [5, 'all']; 'all' resolves to n_products."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters()
    params["evaluation"].pop("k_values")  # use the default
    result = compute_all_metrics(eval_df, params)
    # 3 products → 'all' resolves to 3; together with default 5, sorted unique = [3, 5]
    overall_keys = set(result["overall"].keys())
    assert "map@3" in overall_keys
    assert "map@5" in overall_keys


def test_compute_all_metrics_all_queries_excluded(spark):
    """No positives anywhere → early return with empty dicts and counts."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 0),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C1", "A", 0.9, 0),
            ("20240331", "C1", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = _make_parameters(k_values=[2])
    result = compute_all_metrics(eval_df, params)
    assert result["overall"] == {}
    assert result["per_product"] == {}
    assert result["per_segment"] == {}
    assert result["per_product_segment"] == {}
    assert result["macro_avg"] == {}
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k compute_all_metrics -v`

Expected: FAIL with `ImportError: cannot import name 'compute_all_metrics'`

- [ ] **Step 3: Implement `compute_all_metrics`**

Append to `src/recsys_tfb/evaluation/metrics_spark.py`:

```python
def compute_all_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Spark-native orchestrator. Returns dict matching pandas compute_all_metrics shape.

    Stages:
        A1. rank_within_query
        A2. add_query_aggregates
        A3. add_row_contributions (after filtering total_rel > 0)
        B1. aggregate_overall
        B2. aggregate_by_row_dimension (per_product / per_product_segment)
        B3. aggregate_by_query_dimension (per_segment)
        C.  macro_average per dim
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {})
    k_values_raw = eval_params.get("k_values", [5, "all"])
    segment_columns = eval_params.get("segment_columns", [])

    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = _resolve_k_values(k_values_raw, n_products)

    n_queries_total = eval_predictions.select(*group_cols).distinct().count()

    df = rank_within_query(eval_predictions, group_cols, score_col)
    df = add_query_aggregates(df, group_cols, label_col)

    df_with_pos = df.filter(F.col("total_rel") > 0)
    n_queries_with_pos = df_with_pos.select(*group_cols).distinct().count()

    if n_queries_with_pos == 0:
        logger.warning("No queries with positive labels found")
        return {
            "overall": {},
            "per_product": {},
            "per_segment": {},
            "per_product_segment": {},
            "macro_avg": {},
            "n_queries": n_queries_total,
            "n_excluded_queries": n_queries_total - n_queries_with_pos,
        }

    enriched = add_row_contributions(df_with_pos, group_cols, label_col, k_values)
    enriched = enriched.cache()

    try:
        overall = aggregate_overall(enriched, group_cols, label_col, k_values)
        per_product = aggregate_by_row_dimension(
            enriched, [item_col], label_col, k_values
        )

        per_segment: dict = {}
        per_product_segment: dict = {}
        active_seg_col = None
        for seg_col in segment_columns:
            if seg_col in enriched.columns:
                active_seg_col = seg_col
                break
        if active_seg_col is not None:
            per_segment = aggregate_by_query_dimension(
                enriched, active_seg_col, group_cols, label_col, k_values
            )
            per_product_segment = aggregate_by_row_dimension(
                enriched, [item_col, active_seg_col], label_col, k_values
            )

        macro_avg: dict = {}
        macro_avg["by_product"] = _macro_average(per_product)
        if per_segment:
            macro_avg["by_segment"] = _macro_average(per_segment)
        if per_product_segment:
            macro_avg["by_product_segment"] = _macro_average(per_product_segment)

        return {
            "overall": overall,
            "per_product": per_product,
            "per_segment": per_segment,
            "per_product_segment": per_product_segment,
            "macro_avg": macro_avg,
            "n_queries": n_queries_total,
            "n_excluded_queries": n_queries_total - n_queries_with_pos,
        }
    finally:
        enriched.unpersist()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -k compute_all_metrics -v`

Expected: 7 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py
git commit -m "feat(evaluation): compute_all_metrics orchestrator + integration tests"
```

---

## Task 10: Rewire `nodes_spark.compute_metrics` as thin wrapper

Replace the existing ~150-line `compute_metrics` in `pipelines/evaluation/nodes_spark.py` with a thin call into `metrics_spark.compute_all_metrics`. Remove imports that are no longer used.

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`

- [ ] **Step 1: Verify current state — review the diff target**

Run: `grep -n "^def compute_metrics\|^def generate_report\|^def prepare_eval_data\|^def _render_html_report" src/recsys_tfb/pipelines/evaluation/nodes_spark.py`

Expected lines to roughly match: `prepare_eval_data` near top, `compute_metrics` next, `generate_report` and `_render_html_report` below.

- [ ] **Step 2: Replace `compute_metrics` body**

Open `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` and replace the entire function body (signature + docstring + everything until the next `def`):

```python
def compute_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics using the Spark-native pipeline.

    Thin wrapper over `evaluation.metrics_spark.compute_all_metrics`. All
    row-level work stays in Spark; only small aggregated dicts are collected.
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    result = compute_all_metrics(eval_predictions, parameters)
    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        result["n_queries"],
        result["n_excluded_queries"],
    )
    return result
```

- [ ] **Step 3: Remove now-unused imports at the top of `nodes_spark.py`**

After the rewrite, `Window` and most `functions as F` usage from `compute_metrics` are gone (`prepare_eval_data` still uses `F.col`; `_render_html_report` does not use Spark). Verify imports.

Run: `grep -n "Window\|from pyspark" src/recsys_tfb/pipelines/evaluation/nodes_spark.py`

If `Window` is no longer referenced anywhere in the file, delete its import line. Keep `from pyspark.sql import functions as F` (still used by `prepare_eval_data`).

Run this verification command:

```bash
.venv/bin/python -c "import ast, sys; tree = ast.parse(open('src/recsys_tfb/pipelines/evaluation/nodes_spark.py').read()); names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}; print('Window used:', 'Window' in names)"
```

If `Window used: False`, remove the `Window` import. Otherwise keep it.

- [ ] **Step 4: Run the full evaluation test suite to verify no regressions**

Run: `.venv/bin/python -m pytest tests/test_evaluation/ tests/test_pipelines/test_evaluation/ -v`

Expected: all PASS (existing tests still green; the parity test from Task 11 is not yet added).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py
git commit -m "refactor(evaluation): compute_metrics becomes thin wrapper over metrics_spark"
```

---

## Task 11: Spark / pandas parity test

Verifies the new Spark engine returns numerically equivalent results to the pandas engine on the same data.

**Files:**
- Modify: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def _make_random_eval_data(n_customers=30, products=("A", "B", "C", "D"), seed=42):
    """Random fixture for parity testing. Mirrors test_metrics.py _make_test_data."""
    import numpy as np
    import pandas as pd

    rng = np.random.RandomState(seed)
    pred_rows = []
    label_rows = []
    snap_date = "20240331"
    for i in range(n_customers):
        cust_id = f"C{i:04d}"
        seg = ["mass", "affluent", "hnw"][i % 3]
        scores = rng.rand(len(products))
        for j, prod in enumerate(products):
            pred_rows.append(
                {
                    "snap_date": snap_date,
                    "cust_id": cust_id,
                    "prod_name": prod,
                    "score": float(scores[j]),
                    "rank": 0,
                }
            )
            label_rows.append(
                {
                    "snap_date": snap_date,
                    "cust_id": cust_id,
                    "prod_name": prod,
                    "label": int(rng.rand() > 0.7),
                    "cust_segment_typ": seg,
                }
            )
    preds = pd.DataFrame(pred_rows)
    preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)
    labels = pd.DataFrame(label_rows)
    return preds, labels


def _assert_metrics_close(a: dict, b: dict, rtol: float = 1e-6, path: str = ""):
    """Recursively assert two nested metric dicts are numerically close."""
    import math

    assert set(a.keys()) == set(b.keys()), (
        f"Key mismatch at {path or '<root>'}: {set(a.keys())} vs {set(b.keys())}"
    )
    for k in a:
        va, vb = a[k], b[k]
        if isinstance(va, dict):
            _assert_metrics_close(va, vb, rtol=rtol, path=f"{path}.{k}")
        else:
            assert math.isclose(va, vb, rel_tol=rtol, abs_tol=1e-12), (
                f"Mismatch at {path}.{k}: pandas={va!r} spark={vb!r}"
            )


def test_spark_pandas_parity_overall_and_per_dimension(spark):
    """Same input data, both engines should produce numerically equivalent dicts."""
    from recsys_tfb.evaluation.metrics import compute_all_metrics as compute_pd
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics as compute_spark

    preds_pd, labels_pd = _make_random_eval_data(n_customers=30, seed=42)
    eval_pd = preds_pd.merge(
        labels_pd, on=["snap_date", "cust_id", "prod_name"]
    )
    eval_spark = spark.createDataFrame(eval_pd)
    params = _make_parameters(
        k_values=[3, 5], segment_columns=["cust_segment_typ"]
    )

    result_pd = compute_pd(preds_pd, labels_pd, k_values=[3, 5])
    result_spark = compute_spark(eval_spark, params)

    _assert_metrics_close(result_pd["overall"], result_spark["overall"])
    _assert_metrics_close(result_pd["per_product"], result_spark["per_product"])
    _assert_metrics_close(result_pd["per_segment"], result_spark["per_segment"])
    _assert_metrics_close(
        result_pd["per_product_segment"], result_spark["per_product_segment"]
    )
    _assert_metrics_close(result_pd["macro_avg"], result_spark["macro_avg"])
    assert result_pd["n_queries"] == result_spark["n_queries"]
    assert result_pd["n_excluded_queries"] == result_spark["n_excluded_queries"]
```

- [ ] **Step 2: Run the test**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py::test_spark_pandas_parity_overall_and_per_dimension -v`

Expected: PASS. If it fails, the error message indicates which dim/metric diverges — investigate before continuing.

- [ ] **Step 3: Commit**

```bash
git add tests/test_evaluation/test_metrics_spark.py
git commit -m "test(evaluation): Spark/pandas parity test on random fixture"
```

---

## Task 12: Final verification

Run the full project test suite to confirm no regressions outside the evaluation tests.

**Files:**
- (No file changes; verification only.)

- [ ] **Step 1: Run the full test suite**

Run: `.venv/bin/python -m pytest tests/ -q`

Expected: all tests pass (Phase 1 produced 655 passing; Phase 2 adds ~24 new tests in `test_metrics_spark.py`, so expect ~679 passing).

- [ ] **Step 2: Sanity-check line counts and module shape**

Run:

```bash
wc -l src/recsys_tfb/evaluation/metrics_spark.py \
     src/recsys_tfb/pipelines/evaluation/nodes_spark.py
```

Expected:
- `metrics_spark.py` ≈ 200–260 lines
- `nodes_spark.py` should be smaller than before (was ~430; now ~280–300; saved ~130 by replacing fat `compute_metrics`)

- [ ] **Step 3: Smoke check the wrapper**

Run:

```bash
.venv/bin/python -c "
from recsys_tfb.pipelines.evaluation.nodes_spark import compute_metrics
import inspect
src = inspect.getsource(compute_metrics)
assert 'from recsys_tfb.evaluation.metrics_spark import compute_all_metrics' in src
assert len(src.splitlines()) < 30
print('compute_metrics is a thin wrapper:', len(src.splitlines()), 'lines')
"
```

Expected output: `compute_metrics is a thin wrapper: <n> lines` where n < 30.

- [ ] **Step 4: No commit (verification only)**

---

## Optional Follow-up: dev-cluster smoke test

Not part of this plan's commits, but recommended before merging:

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb evaluation --env production
```

Verify the evaluation pipeline runs end-to-end against dev-cluster Hive and produces a report. The output dict shape is verified by unit tests; this confirms wiring.

---

## Self-Review Summary

**Spec coverage:** Every section of `2026-05-12-evaluation-spark-metrics-refactor-design.md` is covered:
- §4 Module breakdown → Tasks 1–9
- §5 Data flow → Task 9 (orchestrator)
- §6 Data Schema → Tasks 4–5
- §7 Aggregator formulas → Tasks 6–8
- §8 Edge cases → Task 9 (no positives early return, missing segment column)
- §9 `.cache()` strategy → Task 9 implementation (cache + unpersist in finally)
- §10 Testing Layers 1–4 → Tasks 2–9, 11
- §11 Migration plan → Tasks 10, 12

**No placeholders detected.** All code blocks are concrete and runnable.

**Type/naming consistency verified:** Column names (`pos`, `total_rel`, `cum_rel`, `prec_at_pos`, `dcg_term`, `top_k@K`, `ap_contrib@K`, `ndcg_contrib@K`) are used identically across all tasks. Function signatures match between task definitions and orchestrator usage.

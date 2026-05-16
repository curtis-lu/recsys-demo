# Evaluation Pipeline 重構 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重構 evaluation pipeline 與 `src/recsys_tfb/evaluation/`：產出讀者旅程結構的新版報告、新增產品大類並行評估、清理模組邊界與死碼。

**Architecture:** metrics 計算層新增「折疊成大類粒度後重用同一條 pipeline」與 `dataset_overview` 聚合，輸出向後相容只增不改。報告組裝抽出成 `report_builder.py`（一段一純函式、免 Spark 可單測），pipeline node 變薄、`toPandas` 只在診斷段發生。

**Tech Stack:** PySpark 3.3.2、pandas 1.5.3、plotly、pytest 7.3.1。約束：no UDF、no network、no extra packages、CPU-only。

**Spec:** `docs/superpowers/specs/2026-05-16-evaluation-pipeline-refactor-design.md`

---

## Task 1: 更新 parameters_evaluation.yaml（config schema）

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`
- Test: `tests/test_evaluation/test_parameters_evaluation_yaml.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_evaluation/test_parameters_evaluation_yaml.py
"""Regression: parameters_evaluation.yaml carries the refactor's new keys."""

from pathlib import Path

import yaml


def _load():
    p = Path("conf/base/parameters_evaluation.yaml")
    return yaml.safe_load(p.read_text())["evaluation"]


def test_k_values_is_superset():
    assert _load()["k_values"] == [1, 2, 3, 4, 5, "all"]


def test_product_categories_block():
    pc = _load()["product_categories"]
    assert pc["enabled"] is True
    assert pc["unmapped"] == "singleton"
    assert pc["mapping"]["fund"] == ["fund_stock", "fund_bond", "fund_mix"]


def test_report_display_and_sections():
    rep = _load()["report"]
    assert rep["sections"]["category"] is True
    assert rep["display"]["primary_map_k"] == [1, 3, 5, "all"]
    assert rep["display"]["guardrail_recall_k"] == [1, 2, 3, 4, 5]
    assert rep["diagnostics"]["sample_rows"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_parameters_evaluation_yaml.py -v`
Expected: FAIL (KeyError: 'product_categories' / k_values mismatch)

- [ ] **Step 3: Replace the file content**

Write `conf/base/parameters_evaluation.yaml` with exactly:

```yaml
evaluation:
  # Snap date for evaluation (YYYYMMDD or YYYY-MM-DD).
  snap_date: "20251231"

  # Single k superset. Every @K metric is computed at every k here, for all
  # granularities (fine product overall/per-item/per-segment AND category).
  # Report sections slice the k columns they show via report.display.*.
  # "all" resolves at runtime to the distinct item count of that granularity.
  k_values: [1, 2, 3, 4, 5, "all"]

  # Segment columns already present in the labels DataFrame.
  segment_columns:
    - cust_segment_typ

  # External segment data sources — joined to labels on key_columns.
  segment_sources:
    holding_combo:
      filepath: data/holding_combo.parquet
      key_columns: [cust_id, snap_date]
      segment_column: holding_combo

  # Product major-category parallel evaluation. When enabled, the fine-grained
  # eval_predictions is collapsed to category grain (category score = max child
  # score == best child rank; category label = max child label) and the SAME
  # metric pipeline runs a second time; results nest under the "category" key.
  product_categories:
    enabled: true
    unmapped: singleton          # products not in any list -> own singleton category
    mapping:
      fund: [fund_stock, fund_bond, fund_mix]
      exchange: [exchange_fx, exchange_usd]
      ccard: [ccard_bill, ccard_cash, ccard_ins]

  # Baseline configuration
  baseline:
    type: global_popularity
    segment_column: cust_segment_typ
    lookback_months: 12

  # Report generation options
  report:
    sections:
      dataset_overview: true
      primary_map: true
      guardrail_recall: true
      category: true             # also gated by product_categories.enabled
      per_segment: true
      diagnostics: true
      baseline: true
    display:
      primary_map_k: [1, 3, 5, "all"]
      guardrail_recall_k: [1, 2, 3, 4, 5]
      recall_colorscale: {low: 0.0, high: 1.0}
    diagnostics:
      include_distributions: true
      include_calibration: true
      n_calibration_bins: 10
      sample_rows: null          # toPandas row cap for diagnostics; null = no sampling
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_evaluation/test_parameters_evaluation_yaml.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add conf/base/parameters_evaluation.yaml tests/test_evaluation/test_parameters_evaluation_yaml.py
git commit -m "feat(evaluation): k superset + product_categories + report config"
```

---

## Task 2: metrics_spark — `_build_category_mapping`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (add after `_resolve_k_values`, ~line 91)
- Test: `tests/test_evaluation/test_metrics_spark_category.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_evaluation/test_metrics_spark_category.py
"""Tests for category-level extension of metrics_spark."""

import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(enabled=True, unmapped="singleton"):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]},
        }},
        "evaluation": {"product_categories": {
            "enabled": enabled, "unmapped": unmapped,
            "mapping": {"fund": ["fund_stock", "fund_bond", "fund_mix"]},
        }},
    }


def test_disabled_returns_none():
    p = _params(enabled=False)
    assert ms._build_category_mapping(p) is None


def test_mapping_with_singleton_unmapped():
    m = ms._build_category_mapping(_params())
    assert m["fund_stock"] == "fund"
    assert m["fund_bond"] == "fund"
    assert m["exchange_fx"] == "exchange_fx"   # unmapped -> singleton
    assert m["lonely"] == "lonely"


def test_unknown_product_in_mapping_fails_loud():
    p = _params()
    p["evaluation"]["product_categories"]["mapping"]["x"] = ["not_a_product"]
    with pytest.raises(ValueError, match="not_a_product"):
        ms._build_category_mapping(p)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_metrics_spark_category.py -v`
Expected: FAIL (AttributeError: module has no attribute '_build_category_mapping')

- [ ] **Step 3: Add the implementation**

Insert in `src/recsys_tfb/evaluation/metrics_spark.py` immediately after `_resolve_k_values` (after line 90):

```python
def _build_category_mapping(parameters: dict) -> dict[str, str] | None:
    """Resolve {prod_name: category}. None when categories disabled.

    Fail-loud (ValueError) if a mapped product is not in
    ``schema.categorical_values[item_col]``. Products absent from every
    mapping list become their own singleton category when
    ``unmapped == 'singleton'`` (the only supported mode).
    """
    eval_params = parameters.get("evaluation", {}) or {}
    pc = eval_params.get("product_categories", {}) or {}
    if not pc.get("enabled"):
        return None

    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list(
        (parameters.get("schema", {}).get("columns", {})
         .get("categorical_values", {}) or {}).get(item_col, [])
    )
    known_set = set(known)

    mapping: dict[str, str] = {}
    for category, prods in (pc.get("mapping", {}) or {}).items():
        for prod in prods:
            if prod not in known_set:
                raise ValueError(
                    f"product_categories.mapping references unknown product "
                    f"'{prod}' (not in schema.categorical_values['{item_col}'])"
                )
            mapping[prod] = category

    unmapped = pc.get("unmapped", "singleton")
    if unmapped != "singleton":
        raise ValueError(
            f"product_categories.unmapped='{unmapped}' unsupported; "
            f"only 'singleton' is implemented"
        )
    for prod in known:
        mapping.setdefault(prod, prod)
    return mapping
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_evaluation/test_metrics_spark_category.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark_category.py
git commit -m "feat(evaluation): _build_category_mapping with fail-loud + singleton"
```

---

## Task 3: metrics_spark — `collapse_to_categories`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (add after `_build_category_mapping`)
- Test: `tests/test_evaluation/test_metrics_spark_category.py` (append)

- [ ] **Step 1: Write the failing test (append to the file)**

```python
def _raw(spark):
    # c1 wants fund (via fund_bond) ; c1 fund_stock is top score
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_stock", 0.9, 0, "mass"),
            ("20240331", "c1", "fund_bond",  0.4, 1, "mass"),
            ("20240331", "c1", "exchange_fx", 0.7, 0, "mass"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label",
                "cust_segment_typ"],
    )


def test_collapse_to_categories_grain(spark):
    p = _params()
    p["schema"]["columns"]["categorical_values"]["prod_name"] = [
        "fund_stock", "fund_bond", "fund_mix", "exchange_fx", "lonely"]
    collapsed = ms.collapse_to_categories(_raw(spark), p)
    rows = {r["prod_name"]: r for r in collapsed.collect()}
    # category column reuses item_col name so downstream stays uniform
    assert set(rows) == {"fund", "exchange_fx"}
    # fund score = max(child score) = max(0.9, 0.4) = 0.9
    assert rows["fund"]["score"] == pytest.approx(0.9)
    # fund label = max(child label) = max(0, 1) = 1
    assert rows["fund"]["label"] == 1
    # segment carried
    assert rows["fund"]["cust_segment_typ"] == "mass"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_metrics_spark_category.py::test_collapse_to_categories_grain -v`
Expected: FAIL (AttributeError: no attribute 'collapse_to_categories')

- [ ] **Step 3: Add the implementation**

Append in `metrics_spark.py` after `_build_category_mapping`:

```python
def collapse_to_categories(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Collapse fine-grained predictions to category grain (no UDF).

    For each (time, entity..., category): score = max(child score),
    label = max(child label), segment columns via F.first. The category
    column is emitted under the schema item_col name so the collapsed DF
    is shape-compatible with compute_all_metrics. ``max(score)`` re-ranking
    is equivalent to taking the best child rank (pos is score-desc derived).
    """
    mapping = _build_category_mapping(parameters)
    if mapping is None:
        raise ValueError("collapse_to_categories called with categories disabled")

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {}) or {}
    segment_columns = [
        c for c in (eval_params.get("segment_columns", []) or [])
        if c in eval_predictions.columns
    ]

    spark = eval_predictions.sparkSession
    map_rows = [(p, c) for p, c in mapping.items()]
    map_df = spark.createDataFrame(map_rows, [item_col, "_category"])

    joined = eval_predictions.join(F.broadcast(map_df), on=item_col, how="inner")

    aggs = [
        F.max(F.col(score_col)).alias(score_col),
        F.max(F.col(label_col)).alias(label_col),
    ]
    for seg in segment_columns:
        aggs.append(F.first(F.col(seg)).alias(seg))

    collapsed = (
        joined.groupBy(*group_cols, "_category")
        .agg(*aggs)
        .withColumnRenamed("_category", item_col)
    )
    return collapsed
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_evaluation/test_metrics_spark_category.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark_category.py
git commit -m "feat(evaluation): collapse_to_categories Spark transform (no UDF)"
```

---

## Task 4: metrics_spark — `compute_dataset_overview`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (add after `collapse_to_categories`)
- Test: `tests/test_evaluation/test_metrics_spark_overview.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_evaluation/test_metrics_spark_overview.py
import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {}}


def _df(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1),
            ("20240331", "c1", "B", 0.1, 0),
            ("20240331", "c2", "A", 0.2, 0),
            ("20240331", "c2", "B", 0.8, 1),
            ("20240229", "c1", "A", 0.5, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_dataset_overview_totals(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    t = ov["totals"]
    assert t["n_rows"] == 5
    assert t["n_customers"] == 2
    assert t["n_products"] == 2
    assert t["n_snap_dates"] == 2
    assert t["n_positives"] == 3
    assert t["positive_rate"] == pytest.approx(3 / 5)


def test_dataset_overview_by_snap_and_item(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    assert ov["by_snap_date"]["20240331"]["n_rows"] == 4
    assert ov["by_snap_date"]["20240331"]["n_positives"] == 2
    assert ov["by_item"]["A"]["n_customers"] == 2
    assert ov["by_item"]["A"]["n_positives"] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_metrics_spark_overview.py -v`
Expected: FAIL (no attribute 'compute_dataset_overview')

- [ ] **Step 3: Add the implementation**

Append in `metrics_spark.py` after `collapse_to_categories`:

```python
def compute_dataset_overview(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    item_col_override: str | None = None,
) -> dict:
    """Dataset profiling for the report §1. Pure Spark agg, small collect.

    ``item_col_override`` lets the caller profile the collapsed
    category-grain DF (item column still named after schema item_col, but
    semantics = category).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = item_col_override or schema["item"]
    label_col = schema["label"]
    entity_col = entity_cols[0]

    n_rows = eval_predictions.count()
    n_customers = eval_predictions.select(*entity_cols).distinct().count()
    n_products = eval_predictions.select(item_col).distinct().count()
    n_snap_dates = eval_predictions.select(time_col).distinct().count()
    n_positives = int(
        eval_predictions.agg(F.sum(F.col(label_col))).collect()[0][0] or 0
    )
    positive_rate = (n_positives / n_rows) if n_rows else 0.0
    avg_pos_per_customer = (n_positives / n_customers) if n_customers else 0.0

    def _group(col: str) -> dict:
        rows = (
            eval_predictions.groupBy(col)
            .agg(
                F.count(F.lit(1)).alias("n_rows"),
                F.sum(F.col(label_col)).alias("n_positives"),
                F.countDistinct(*entity_cols).alias("n_customers"),
            )
            .collect()
        )
        out = {}
        for r in rows:
            key = r[col] if isinstance(r[col], str) else str(r[col])
            nr = int(r["n_rows"])
            npos = int(r["n_positives"] or 0)
            out[key] = {
                "n_rows": nr,
                "n_positives": npos,
                "n_customers": int(r["n_customers"]),
                "positive_rate": (npos / nr) if nr else 0.0,
            }
        return out

    return {
        "totals": {
            "n_rows": n_rows,
            "n_customers": n_customers,
            "n_products": n_products,
            "n_snap_dates": n_snap_dates,
            "n_positives": n_positives,
            "positive_rate": positive_rate,
            "avg_positives_per_customer": avg_pos_per_customer,
        },
        "by_snap_date": _group(time_col),
        "by_item": _group(item_col),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_evaluation/test_metrics_spark_overview.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark_overview.py
git commit -m "feat(evaluation): compute_dataset_overview Spark profiling"
```

---

## Task 5: metrics_spark — wire category + dataset_overview into `compute_all_metrics`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py:383-498` (refactor `compute_all_metrics`)
- Test: `tests/test_evaluation/test_metrics_spark_orchestrator.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_evaluation/test_metrics_spark_orchestrator.py
import pytest

from recsys_tfb.evaluation import metrics_spark as ms


def _params(categories=True):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "exchange_fx"]}}},
        "evaluation": {
            "k_values": [1, "all"],
            "product_categories": {
                "enabled": categories, "unmapped": "singleton",
                "mapping": {"fund": ["fund_stock", "fund_bond"]}},
        },
    }


def _df(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "fund_stock", 0.9, 1),
            ("20240331", "c1", "fund_bond", 0.4, 0),
            ("20240331", "c1", "exchange_fx", 0.7, 0),
            ("20240331", "c2", "fund_stock", 0.2, 0),
            ("20240331", "c2", "fund_bond", 0.3, 0),
            ("20240331", "c2", "exchange_fx", 0.8, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_backward_compatible_keys_present(spark):
    r = ms.compute_all_metrics(_df(spark), _params())
    for k in ("overall", "per_segment", "per_item", "per_item_segment",
              "macro_avg", "n_queries", "n_excluded_queries"):
        assert k in r


def test_dataset_overview_and_category_added(spark):
    r = ms.compute_all_metrics(_df(spark), _params())
    assert "dataset_overview" in r
    assert r["dataset_overview"]["totals"]["n_rows"] == 6
    assert "category" in r
    assert set(r["category"]["per_item"]) == {"fund", "exchange_fx"}
    assert "dataset_overview" in r["category"]
    assert "category" not in r["category"]  # no infinite nesting


def test_category_absent_when_disabled(spark):
    r = ms.compute_all_metrics(_df(spark), _params(categories=False))
    assert "category" not in r
    assert "dataset_overview" in r
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_metrics_spark_orchestrator.py -v`
Expected: FAIL (KeyError 'dataset_overview' / 'category')

- [ ] **Step 3: Refactor `compute_all_metrics`**

In `metrics_spark.py`, rename the existing `compute_all_metrics` body to a private core and add a thin public wrapper. Replace the function definition starting at line 383 (`def compute_all_metrics(`) through the end of the file with:

```python
def _compute_core(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """The fine-grained metric bundle (overall/per_item/per_segment/...).

    Body identical to the pre-refactor compute_all_metrics — no category,
    no dataset_overview. Used for both fine-grained and (on a collapsed DF)
    category-grain passes.
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
    segment_columns = eval_params.get("segment_columns", []) or []

    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = _resolve_k_values(k_values_raw, n_products)
    n_queries_total = eval_predictions.select(*group_cols).distinct().count()

    df = rank_within_query(eval_predictions, group_cols, score_col)
    df = add_query_total_rel(df, group_cols, label_col)

    df_with_pos = df.filter(F.col("total_rel") > 0)
    n_queries_with_pos = df_with_pos.select(*group_cols).distinct().count()
    n_excluded_queries = n_queries_total - n_queries_with_pos

    if n_queries_with_pos == 0:
        logger.warning("No queries with positive labels found")
        return {
            **_EMPTY_RESULT,
            "n_queries": n_queries_total,
            "n_excluded_queries": n_excluded_queries,
        }

    enriched = add_row_contributions(df_with_pos, group_cols, label_col, k_values)
    enriched = enriched.cache()
    try:
        active_seg_col: str | None = None
        for seg in segment_columns:
            if seg in enriched.columns:
                active_seg_col = seg
                break

        carry = [active_seg_col] if active_seg_col else []
        per_query = compute_per_query_metrics(
            enriched, group_cols, label_col, k_values, carry_cols=carry
        ).cache()
        try:
            overall = aggregate_overall(per_query, k_values)
            per_item = aggregate_per_item(
                enriched, [item_col], label_col, k_values
            )

            per_segment: dict = {}
            per_item_segment: dict = {}
            if active_seg_col:
                per_segment = aggregate_per_segment(
                    per_query, active_seg_col, k_values
                )
                per_item_segment = aggregate_per_item(
                    enriched, [item_col, active_seg_col], label_col, k_values
                )

            macro_avg: dict = {"by_item": macro_average(per_item)}
            if per_segment:
                macro_avg["by_segment"] = macro_average(per_segment)
            if per_item_segment:
                macro_avg["by_item_segment"] = macro_average(per_item_segment)

            return {
                "overall": overall,
                "per_segment": per_segment,
                "per_item": per_item,
                "per_item_segment": per_item_segment,
                "macro_avg": macro_avg,
                "n_queries": n_queries_total,
                "n_excluded_queries": n_excluded_queries,
            }
        finally:
            per_query.unpersist()
    finally:
        enriched.unpersist()


def compute_all_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Full bundle: fine-grained core + dataset_overview + optional category.

    Backward compatible: every pre-existing top-level key is unchanged;
    ``dataset_overview`` is always added; ``category`` (same shape, plus its
    own dataset_overview, never re-nested) is added only when
    ``product_categories.enabled``.
    """
    result = _compute_core(eval_predictions, parameters)
    result["dataset_overview"] = compute_dataset_overview(
        eval_predictions, parameters
    )

    if _build_category_mapping(parameters) is not None:
        collapsed = collapse_to_categories(eval_predictions, parameters)
        cat = _compute_core(collapsed, parameters)
        cat["dataset_overview"] = compute_dataset_overview(
            collapsed, parameters
        )
        result["category"] = cat

    return result
```

Keep the existing `compute_all_metrics` docstring block content by moving it onto the new `compute_all_metrics` (preserve the Returns description, add the two new keys).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_metrics_spark_orchestrator.py tests/test_evaluation/test_metrics_spark.py -v`
Expected: PASS (new orchestrator tests + all pre-existing metrics_spark tests still green)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark_orchestrator.py
git commit -m "feat(evaluation): compute_all_metrics adds dataset_overview + category"
```

---

## Task 6: segments.py — single Spark `join_segment_sources` + source seam

**Files:**
- Rewrite: `src/recsys_tfb/evaluation/segments.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:51-73`
- Rewrite: `tests/test_evaluation/test_segments.py`

- [ ] **Step 1: Rewrite the test for the Spark API**

Replace the entire contents of `tests/test_evaluation/test_segments.py` with:

```python
"""Tests for evaluation.segments — single Spark segment-source join."""

from recsys_tfb.evaluation.segments import join_segment_sources


def _labels(spark):
    return spark.createDataFrame(
        [("c0", "20240331", 1), ("c1", "20240331", 0),
         ("c2", "20240331", 1)],
        schema=["cust_id", "snap_date", "label"],
    )


def _write(spark, tmp_path, name, rows, cols):
    p = str(tmp_path / f"{name}.parquet")
    spark.createDataFrame(rows, schema=cols).write.parquet(p)
    return p


def test_join_single_source(spark, tmp_path):
    labels = _labels(spark)
    path = _write(spark, tmp_path, "hc",
                  [("c0", "20240331", "x"), ("c1", "20240331", "y"),
                   ("c2", "20240331", "z")],
                  ["cust_id", "snap_date", "holding_combo"])
    cfg = {"holding_combo": {"filepath": path,
                             "key_columns": ["cust_id", "snap_date"],
                             "segment_column": "holding_combo"}}
    out = join_segment_sources(labels, cfg)
    assert "holding_combo" in out.columns
    assert out.filter("holding_combo IS NULL").count() == 0


def test_missing_file_skipped(spark, tmp_path):
    labels = _labels(spark)
    cfg = {"missing": {"filepath": str(tmp_path / "none.parquet"),
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "missing_col"}}
    out = join_segment_sources(labels, cfg)
    assert "missing_col" not in out.columns
    assert out.count() == 3


def test_partial_join_coverage(spark, tmp_path):
    labels = _labels(spark)
    path = _write(spark, tmp_path, "risk",
                  [("c0", "20240331", "high")],
                  ["cust_id", "snap_date", "risk_level"])
    cfg = {"risk": {"filepath": path,
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "risk_level"}}
    out = join_segment_sources(labels, cfg)
    assert out.filter("risk_level IS NOT NULL").count() == 1
    assert out.filter("risk_level IS NULL").count() == 2


def test_multiple_sources(spark, tmp_path):
    labels = _labels(spark)
    p1 = _write(spark, tmp_path, "a",
                [("c0", "20240331", "A"), ("c1", "20240331", "A"),
                 ("c2", "20240331", "A")],
                ["cust_id", "snap_date", "holding_combo"])
    p2 = _write(spark, tmp_path, "b",
                [("c0", "20240331", "M"), ("c1", "20240331", "M"),
                 ("c2", "20240331", "M")],
                ["cust_id", "snap_date", "risk_level"])
    cfg = {
        "holding_combo": {"filepath": p1,
                          "key_columns": ["cust_id", "snap_date"],
                          "segment_column": "holding_combo"},
        "risk_level": {"filepath": p2,
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "risk_level"},
    }
    out = join_segment_sources(labels, cfg)
    assert "holding_combo" in out.columns
    assert "risk_level" in out.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_segments.py -v`
Expected: FAIL (ImportError: cannot import name 'join_segment_sources')

- [ ] **Step 3: Rewrite `segments.py`**

Replace the entire contents of `src/recsys_tfb/evaluation/segments.py` with:

```python
"""External segment-source joining for evaluation (Spark, single impl).

``_read_segment_source`` is the source seam: today it reads a Parquet
file; a future change swaps only this function to read a Hive table
(``spark.table("ml_recsys.<segment_table>")``) without touching the join
logic. See spec "Out of scope / 後續工作".
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame as SparkDataFrame

logger = logging.getLogger(__name__)


def _read_segment_source(
    spark, source_config: dict
) -> SparkDataFrame | None:
    """Read one external segment source. None when the source is absent.

    SEAM: only this function knows the storage backend.
    """
    filepath = source_config["filepath"]
    if not Path(filepath).exists():
        return None
    return spark.read.parquet(filepath)


def join_segment_sources(
    labels: SparkDataFrame,
    segment_sources: dict,
) -> SparkDataFrame:
    """Left-join each external segment column onto ``labels``.

    Missing sources are warned and skipped (non-fatal), preserving the
    pre-refactor behaviour.
    """
    spark = labels.sparkSession
    for seg_name, source_config in segment_sources.items():
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        seg_df = _read_segment_source(spark, source_config)
        if seg_df is None:
            logger.warning(
                "Segment source '%s' not found at %s — skipping",
                seg_name,
                source_config["filepath"],
            )
            continue

        labels = labels.join(
            seg_df.select(key_columns + [segment_column]),
            on=key_columns,
            how="left",
        )
        logger.info("Joined segment source '%s' (%s)", seg_name, segment_column)

    return labels
```

- [ ] **Step 4: Rewire `prepare_eval_data`**

In `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`, replace lines 53-73 (the `# Join external segment sources` block, from `if segment_sources:` through the `except` clause) with:

```python
    # Join external segment sources (single Spark impl; source seam inside).
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        labels = join_segment_sources(labels, segment_sources)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_segments.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/evaluation/segments.py src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_evaluation/test_segments.py
git commit -m "refactor(evaluation): single Spark join_segment_sources + source seam"
```

---

## Task 7: schema-driven visualizations (distributions / calibration / statistics)

**Files:**
- Modify: `src/recsys_tfb/evaluation/statistics.py`
- Modify: `src/recsys_tfb/evaluation/distributions.py`
- Modify: `src/recsys_tfb/evaluation/calibration.py`
- Test: `tests/test_evaluation/test_schema_driven_viz.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_evaluation/test_schema_driven_viz.py
"""Regression: viz/stats helpers accept non-default column names."""

import pandas as pd

from recsys_tfb.evaluation.statistics import compute_product_statistics
from recsys_tfb.evaluation.distributions import plot_score_distributions
from recsys_tfb.evaluation.calibration import plot_calibration_curves


def test_product_statistics_custom_cols():
    df = pd.DataFrame({
        "item": ["A", "A", "B"], "uid": ["c1", "c2", "c1"],
        "y": [1, 0, 1]})
    stats = compute_product_statistics(
        df, item_col="item", entity_col="uid", label_col="y")
    assert "positive_rate" in stats.columns
    assert set(stats.index) == {"A", "B"}


def test_score_distributions_custom_cols():
    df = pd.DataFrame({"item": ["A", "B"], "sc": [0.1, 0.9]})
    figs = plot_score_distributions(df, item_col="item", score_col="sc")
    assert len(figs) == 2


def test_calibration_custom_cols():
    preds = pd.DataFrame({"t": ["d"] * 4, "u": ["c1", "c2", "c3", "c4"],
                          "item": ["A"] * 4, "sc": [0.2, 0.4, 0.6, 0.8]})
    labs = pd.DataFrame({"t": ["d"] * 4, "u": ["c1", "c2", "c3", "c4"],
                         "item": ["A"] * 4, "y": [0, 0, 1, 1]})
    fig = plot_calibration_curves(
        preds, labs, n_bins=2,
        id_cols=("t", "u", "item"), item_col="item",
        score_col="sc", label_col="y")
    assert fig is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_schema_driven_viz.py -v`
Expected: FAIL (TypeError: unexpected keyword argument 'item_col')

- [ ] **Step 3a: Make `statistics.py` schema-driven**

In `src/recsys_tfb/evaluation/statistics.py` change the two function signatures and replace the hard-coded literals with the parameters. Replace `compute_product_statistics`:

```python
def compute_product_statistics(
    labels: pd.DataFrame,
    item_col: str = "prod_name",
    entity_col: str = "cust_id",
    label_col: str = "label",
) -> pd.DataFrame:
    """Per-product statistics at customer granularity."""
    cust_prod = (
        labels.groupby([item_col, entity_col])[label_col].max().reset_index()
    )
    stats = cust_prod.groupby(item_col).agg(
        positive_customers=(label_col, "sum"),
        total_customers=(label_col, "count"),
    )
    stats["negative_customers"] = (
        stats["total_customers"] - stats["positive_customers"]
    )
    stats["positive_rate"] = (
        stats["positive_customers"] / stats["total_customers"]
    )
    pos_per_cust = (
        labels[labels[label_col] == 1].groupby(entity_col).size()
    )
    avg_pos = pos_per_cust.mean() if len(pos_per_cust) > 0 else 0.0
    stats["avg_positive_products_per_customer"] = avg_pos
    return stats[
        ["positive_customers", "negative_customers", "total_customers",
         "positive_rate", "avg_positive_products_per_customer"]
    ]
```

Replace `compute_segment_statistics` signature line with:

```python
def compute_segment_statistics(
    labels: pd.DataFrame,
    segment_column: str = "cust_segment_typ",
    entity_col: str = "cust_id",
    label_col: str = "label",
) -> pd.DataFrame:
```

and inside it replace every literal `"cust_id"` with `entity_col` and every `"label"` with `label_col` (the `segment_column` variable is already parameterized).

- [ ] **Step 3b: Make `distributions.py` schema-driven**

For each of the five functions in `src/recsys_tfb/evaluation/distributions.py`, add keyword params and replace literals. Use these signatures and replace `"prod_name"`→`item_col`, `"score"`→`score_col`, `"rank"`→`rank_col`, `"label"`→`label_col`, and the `["snap_date", "cust_id", "prod_name", "label"]` merge key list →`list(id_cols) + [label_col]`:

```python
def plot_score_distributions(predictions, title_prefix="",
                             item_col="prod_name", score_col="score"): ...
def plot_rank_heatmap(predictions, title_prefix="",
                      item_col="prod_name", rank_col="rank"): ...
def plot_positive_rank_heatmap(predictions, labels, title_prefix="",
                               id_cols=("snap_date", "cust_id", "prod_name"),
                               item_col="prod_name", rank_col="rank",
                               label_col="label"): ...
def plot_positive_rate_rank_heatmap(predictions, labels, title_prefix="",
                                    id_cols=("snap_date", "cust_id", "prod_name"),
                                    item_col="prod_name", rank_col="rank",
                                    label_col="label"): ...
def plot_score_distributions_by_label(predictions, labels, title_prefix="",
                                      id_cols=("snap_date", "cust_id", "prod_name"),
                                      item_col="prod_name", score_col="score",
                                      label_col="label"): ...
```

In each body: `labels[["snap_date", "cust_id", "prod_name", "label"]]` becomes `labels[list(id_cols) + [label_col]]`; `on=["snap_date", "cust_id", "prod_name"]` becomes `on=list(id_cols)`; `merged["label"]` → `merged[label_col]`; `predictions["prod_name"]` → `predictions[item_col]`; `["score"]` → `[score_col]`; `"rank"` groupby → `rank_col`.

- [ ] **Step 3c: Make `calibration.py` schema-driven**

Replace the `plot_calibration_curves` signature and the merge in `src/recsys_tfb/evaluation/calibration.py`:

```python
def plot_calibration_curves(
    predictions, labels, n_bins=10, title_prefix="",
    id_cols=("snap_date", "cust_id", "prod_name"),
    item_col="prod_name", score_col="score", label_col="label",
):
    merged = predictions.merge(
        labels[list(id_cols) + [label_col]],
        on=list(id_cols), how="inner",
    )
    products = sorted(merged[item_col].unique())
    ...
```

Inside the loop replace `subset["label"]`→`subset[label_col]`, `subset["score"]`→`subset[score_col]`, `merged["prod_name"]`→`merged[item_col]`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_schema_driven_viz.py tests/test_evaluation/test_distributions.py tests/test_evaluation/test_calibration.py tests/test_evaluation/test_statistics.py -v`
Expected: PASS (new regression tests + pre-existing tests still green via defaults)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/statistics.py src/recsys_tfb/evaluation/distributions.py src/recsys_tfb/evaluation/calibration.py tests/test_evaluation/test_schema_driven_viz.py
git commit -m "refactor(evaluation): schema-driven viz/stats column names"
```

---

## Task 8: compare.py — remove dead code

**Files:**
- Modify: `src/recsys_tfb/evaluation/compare.py`
- Modify: `tests/test_evaluation/test_compare.py`

- [ ] **Step 1: Update the test to assert the trimmed API**

In `tests/test_evaluation/test_compare.py`, remove any test referencing `plot_comparison_score_distributions`, `per_segment_delta`, or `macro_avg_delta`, and add:

```python
def test_build_comparison_keeps_overall_and_per_item_only():
    from recsys_tfb.evaluation.compare import build_comparison_result
    a = {"overall": {"map@5": 0.5}, "per_item": {"A": {"hit_rate@5": 0.4}}}
    b = {"overall": {"map@5": 0.3}, "per_item": {"A": {"hit_rate@5": 0.1}}}
    c = build_comparison_result(a, b, "M", "B")
    assert c["overall_delta"]["map@5"] == pytest.approx(0.2)
    assert c["per_item_delta"]["A"]["hit_rate@5"] == pytest.approx(0.3)
    assert "per_segment_delta" not in c
    assert "macro_avg_delta" not in c


def test_plot_comparison_score_distributions_removed():
    import recsys_tfb.evaluation.compare as cmp
    assert not hasattr(cmp, "plot_comparison_score_distributions")
```

(ensure `import pytest` is present at the top of the test file)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_compare.py -v`
Expected: FAIL (per_segment_delta present / attribute still exists)

- [ ] **Step 3: Trim `compare.py`**

In `src/recsys_tfb/evaluation/compare.py`: in `build_comparison_result` delete the `per_segment_delta` block (lines 40-42) and the `macro_avg_delta` block (lines 44-49). Delete the now-unused `_compute_nested_delta`? No — it is still used by `per_item_delta`; keep it. Delete the entire `plot_comparison_score_distributions` function (lines 112-161) and remove the now-unused `import pandas as pd` line if pandas is no longer referenced in the file (it is not — remove it).

Resulting `build_comparison_result` return-building section is exactly:

```python
    comparison["overall_delta"] = _compute_delta(
        result_a.get("overall", {}), result_b.get("overall", {})
    )
    comparison["per_item_delta"] = _compute_nested_delta(
        result_a.get("per_item", {}), result_b.get("per_item", {})
    )
    return comparison
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_compare.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/compare.py tests/test_evaluation/test_compare.py
git commit -m "refactor(evaluation): drop dead compare.py code (score-dist + unused deltas)"
```

---

## Task 9: report.py — collapsible sections + drop dead CSS class

**Files:**
- Modify: `src/recsys_tfb/evaluation/report.py`
- Modify: `tests/test_evaluation/test_report.py`

- [ ] **Step 1: Add failing tests**

Append to `tests/test_evaluation/test_report.py` inside `class TestGenerateHtmlReport`:

```python
    def test_collapsible_section_uses_details(self):
        sections = [ReportSection(title="Diag", description="d",
                                  collapsible=True)]
        html = generate_html_report(sections)
        assert "<details" in html
        assert "<summary>Diag</summary>" in html

    def test_non_collapsible_has_no_details(self):
        sections = [ReportSection(title="Main", description="d")]
        html = generate_html_report(sections)
        assert "<details" not in html

    def test_no_dead_metrics_table_class(self):
        sections = [ReportSection(title="T", description="d",
                                  tables=[pd.DataFrame({"a": [1]})])]
        html = generate_html_report(sections)
        assert 'class="metrics-table"' not in html
        assert 'class="dataframe metrics-table"' not in html
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_report.py -v`
Expected: FAIL (TypeError: unexpected 'collapsible' / metrics-table still emitted)

- [ ] **Step 3: Implement**

In `src/recsys_tfb/evaluation/report.py`:

(a) Add field to the dataclass:

```python
@dataclass
class ReportSection:
    """A section in the evaluation report."""

    title: str
    description: str
    figures: list[go.Figure] = field(default_factory=list)
    tables: list[pd.DataFrame] = field(default_factory=list)
    table_titles: list[str] = field(default_factory=list)
    collapsible: bool = False
```

(b) In the `<style>` list add one rule after the `nav a` line:

```python
        "details > summary { font-size: 1.5em; color: #555; cursor: pointer; margin: 24px 0 8px; }",
```

(c) Replace the per-section render loop body. Replace lines 79-98 (`for i, section in enumerate(sections):` block) with:

```python
    for i, section in enumerate(sections):
        section_id = f"section-{i}"
        if section.collapsible:
            html_parts.append(f'<details class="section" id="{section_id}">')
            html_parts.append(f"<summary>{section.title}</summary>")
        else:
            html_parts.append(f'<div class="section" id="{section_id}">')
            html_parts.append(f"<h2>{section.title}</h2>")
        html_parts.append(f'<p class="description">{section.description}</p>')

        for fig in section.figures:
            html_parts.append(
                fig.to_html(full_html=False, include_plotlyjs=False)
            )

        for ti, table in enumerate(section.tables):
            if ti < len(section.table_titles) and section.table_titles[ti]:
                html_parts.append(f"<h3>{section.table_titles[ti]}</h3>")
            html_parts.append(table.to_html(index=True))

        html_parts.append("</details>" if section.collapsible else "</div>")
```

(note: `table.to_html(index=True)` — the `classes="metrics-table"` arg is removed, eliminating the dead class.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_report.py -v`
Expected: PASS (all, including pre-existing)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report.py tests/test_evaluation/test_report.py
git commit -m "feat(evaluation): collapsible report sections; drop dead metrics-table class"
```

---

## Task 10: report_builder.py — section builders (§0, §1, §2)

**Files:**
- Create: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py` (create)

- [ ] **Step 1: Write failing tests**

```python
# tests/test_evaluation/test_report_builder.py
"""Pure-dict tests for report_builder section functions (no Spark)."""

from recsys_tfb.evaluation import report_builder as rb


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"report": {"display": {
            "primary_map_k": [1, 3, "all"],
            "guardrail_recall_k": [1, 2]}}}}


def _metrics():
    return {
        "overall": {"map@1": 0.5, "map@3": 0.6, "map@5": 0.65,
                    "map@10": 0.7, "precision@1": 0.4, "ndcg@1": 0.55,
                    "recall@1": 0.3},
        "per_item": {"A": {"hit_rate@1": 0.2, "hit_rate@2": 0.4,
                           "mean_pos": 3.0},
                     "B": {"hit_rate@1": 0.1, "hit_rate@2": 0.3,
                           "mean_pos": 5.0}},
        "dataset_overview": {
            "totals": {"n_rows": 100, "n_customers": 10, "n_products": 2,
                       "n_snap_dates": 1, "n_positives": 20,
                       "positive_rate": 0.2,
                       "avg_positives_per_customer": 2.0},
            "by_snap_date": {"20240331": {"n_rows": 100, "n_positives": 20,
                                          "n_customers": 10,
                                          "positive_rate": 0.2}},
            "by_item": {"A": {"n_rows": 50, "n_positives": 12,
                              "n_customers": 10, "positive_rate": 0.24},
                        "B": {"n_rows": 50, "n_positives": 8,
                              "n_customers": 10, "positive_rate": 0.16}}},
        "n_queries": 10, "n_excluded_queries": 0,
    }


def test_headline_section_has_map_card():
    s = rb.build_headline_section(_metrics(), _params())
    txt = " ".join(str(t.to_dict()) for t in s.tables)
    assert "map@1" in txt and "map@all" in txt   # "all" resolves via display
    assert "map@5" not in txt                     # not in display list


def test_dataset_overview_section_tables():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert len(s.tables) == 3   # totals / by_snap_date / by_item
    assert s.title


def test_primary_map_section_slices_k():
    s = rb.build_primary_map_section(_metrics(), _params())
    cols = list(s.tables[0].columns) + list(s.tables[0].index)
    joined = " ".join(map(str, cols))
    assert "map@1" in joined and "map@3" in joined
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_report_builder.py -v`
Expected: FAIL (ModuleNotFoundError: report_builder)

- [ ] **Step 3: Create `report_builder.py` with §0–§2 builders**

```python
# src/recsys_tfb/evaluation/report_builder.py
"""Report section assembly. One pure function per section; no Spark.

Each builder takes the small aggregated metrics dict (from
metrics_spark.compute_all_metrics) + parameters and returns a ReportSection
(or None when its config toggle is off). assemble_report wires the enabled
sections into the final HTML.
"""

from __future__ import annotations

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.report import ReportSection, generate_html_report


def _resolve_display_k(raw_k: list, n_products: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes."""
    out = []
    for k in raw_k:
        if isinstance(k, str) and k.lower() == "all":
            out.append(n_products)
        else:
            out.append(int(k))
    return out


def _report_cfg(parameters: dict) -> dict:
    return (parameters.get("evaluation", {}) or {}).get("report", {}) or {}


def _section_on(parameters: dict, name: str) -> bool:
    sections = _report_cfg(parameters).get("sections", {}) or {}
    return bool(sections.get(name, True))


def _n_products(metrics: dict) -> int:
    return int(
        metrics.get("dataset_overview", {})
        .get("totals", {})
        .get("n_products", 0)
    )


def build_headline_section(metrics: dict, parameters: dict) -> ReportSection:
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), _n_products(metrics)
    )
    card = {f"map@{k}": overall.get(f"map@{k}") for k in ks}
    meta = {
        "n_queries": metrics.get("n_queries"),
        "n_excluded_queries": metrics.get("n_excluded_queries"),
    }
    t1 = pd.DataFrame([card]).T
    t1.columns = ["value"]
    t2 = pd.DataFrame([meta]).T
    t2.columns = ["value"]
    return ReportSection(
        title="摘要 Headline",
        description="主指標 mAP@k（細產品 overall）與 run 概況。",
        tables=[t1, t2],
        table_titles=["主指標 mAP@k", "Run 概況"],
    )


def build_dataset_overview_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "dataset_overview"):
        return None
    ov = metrics.get("dataset_overview", {})
    totals = pd.DataFrame([ov.get("totals", {})]).T
    totals.columns = ["value"]
    by_snap = pd.DataFrame(ov.get("by_snap_date", {})).T
    by_item = pd.DataFrame(ov.get("by_item", {})).T
    return ReportSection(
        title="資料概況 Dataset Overview",
        description="總覽、依 snap_date、依產品的筆數／正樣本數／客戶數。",
        tables=[totals, by_snap, by_item],
        table_titles=["總覽", "by snap_date", "by 產品"],
    )


def build_primary_map_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "primary_map"):
        return None
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), _n_products(metrics)
    )
    rows = {}
    for fam in ("map", "precision", "ndcg", "recall"):
        rows[fam] = {f"@{k}": overall.get(f"{fam}@{k}") for k in ks}
    table = pd.DataFrame(rows).T
    return ReportSection(
        title="主指標 mAP@k（細產品 per-query）",
        description=(
            "overall mAP@k 為主軸；precision/ndcg/recall@k 作脈絡。"
            "K = 產品數時 precision 退化為 base rate、recall 恆為 1。"
        ),
        tables=[table],
        table_titles=["per-query 指標 @k"],
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_report_builder.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(evaluation): report_builder headline/overview/primary-map sections"
```

---

## Task 11: report_builder.py — guardrail / category / segment / baseline / glossary + assemble

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Modify: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: Write failing tests (append)**

```python
def test_guardrail_section_renames_hitrate_and_has_heatmap():
    s = rb.build_guardrail_recall_section(_metrics(), _params())
    cols = " ".join(map(str, s.tables[0].columns))
    assert "recall@1 (per-item)" in cols
    assert "hit_rate" not in cols
    assert len(s.figures) == 1            # plotly heatmap


def test_category_section_none_when_absent():
    assert rb.build_category_section(_metrics(), _params()) is None


def test_category_section_present_when_category_key():
    m = _metrics()
    m["category"] = {"overall": {"map@1": 0.7},
                     "per_item": {"fund": {"hit_rate@1": 0.5,
                                           "mean_pos": 2.0}},
                     "dataset_overview": m["dataset_overview"]}
    s = rb.build_category_section(m, _params())
    assert s is not None and s.tables


def test_glossary_section_always_built():
    s = rb.build_glossary_section(_params())
    assert "recall@k (per-item)" in " ".join(
        map(str, s.tables[0].to_dict().values()))


def test_assemble_report_is_html():
    html = rb.assemble_report(_metrics(), _params())
    assert html.startswith("<!DOCTYPE html>")
    assert "摘要 Headline" in html
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_evaluation/test_report_builder.py -v`
Expected: FAIL (no attribute build_guardrail_recall_section / assemble_report)

- [ ] **Step 3: Append builders + assemble to `report_builder.py`**

```python
import plotly.graph_objects as go  # add to imports at top of file


def _per_item_recall_table(per_item: dict, ks: list) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + base."""
    data = {}
    for item, m in per_item.items():
        row = {f"recall@{k} (per-item)": m.get(f"hit_rate@{k}") for k in ks}
        row["mean_pos"] = m.get("mean_pos")
        data[item] = row
    return pd.DataFrame(data).T


def build_guardrail_recall_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "guardrail_recall"):
        return None
    per_item = metrics.get("per_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), _n_products(metrics)
    )
    table = _per_item_recall_table(per_item, ks)
    cs = (disp.get("recall_colorscale", {}) or {})
    z = [[per_item.get(it, {}).get(f"hit_rate@{k}") for k in ks]
         for it in table.index]
    fig = go.Figure(
        data=go.Heatmap(
            z=z, x=[f"recall@{k}" for k in ks], y=list(table.index),
            zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
            colorscale="RdYlGn", texttemplate="%{z:.3f}",
        )
    )
    fig.update_layout(title="per-item recall@k 色階", yaxis_title="產品")
    return ReportSection(
        title="護欄 per_item recall@k（細產品）",
        description=(
            "每產品 recall@k（per-item，即 hit_rate@k 正名）＋色階。"
            "純判讀、無 pass/fail 閾值。完整資料統計見「資料概況」。"
        ),
        figures=[fig],
        tables=[table],
        table_titles=["per-item recall@k"],
    )


def build_category_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "category"):
        return None
    cat = metrics.get("category")
    if not cat:
        return None
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_cat = int(cat.get("dataset_overview", {}).get("totals", {})
                .get("n_products", 0))
    map_ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_cat)
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_cat)
    overall = cat.get("overall", {})
    map_tbl = pd.DataFrame(
        [{f"map@{k}": overall.get(f"map@{k}") for k in map_ks}]
    ).T
    map_tbl.columns = ["value"]
    rec_tbl = _per_item_recall_table(cat.get("per_item", {}), rec_ks)
    return ReportSection(
        title="大類層級 Category",
        description="大類粒度 mAP@k 與 per-item recall@k（大類=子產品最佳 rank）。",
        tables=[map_tbl, rec_tbl],
        table_titles=["大類 mAP@k", "大類 per-item recall@k"],
    )


def build_segment_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_segment"):
        return None
    per_segment = metrics.get("per_segment", {})
    if not per_segment:
        return None
    table = pd.DataFrame(per_segment).T
    return ReportSection(
        title="分群 Per-Segment",
        description="per-query 指標依 segment 切分。",
        tables=[table],
        table_titles=["per-segment 指標"],
    )


def build_diagnostics_section(
    diagnostics_frames: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "diagnostics") or not diagnostics_frames:
        return None
    figs = diagnostics_frames.get("figures", [])
    if not figs:
        return None
    return ReportSection(
        title="診斷 Diagnostics",
        description="score 分布／rank heatmap／calibration（預設收合）。",
        figures=figs,
        collapsible=True,
    )


def build_baseline_section(
    metrics: dict, baseline_metrics: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "baseline") or baseline_metrics is None:
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    delta = pd.DataFrame([comp["overall_delta"]]).T
    delta.columns = ["Delta (Model - Baseline)"]
    return ReportSection(
        title="基準比較 Baseline",
        description="Model vs Baseline：overall 指標 delta。",
        tables=[delta],
        table_titles=["overall delta"],
    )


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)", "P(rank(P)≤k | P 為正)，命中事件等權；護欄"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("ndcg@k", "log 折扣排序品質，正規化 [0,1]"),
    ("mean_pos", "產品為正時平均排名位置（越小越好）"),
    ("base rate", "母體正樣本率"),
]


def build_glossary_section(parameters: dict) -> ReportSection:
    tbl = pd.DataFrame(_GLOSSARY, columns=["指標", "語意"])
    return ReportSection(
        title="詞彙表 Glossary",
        description="指標語意，詳見 docs/metrics_concept_map.html。",
        tables=[tbl],
        table_titles=["指標語意"],
    )


def assemble_report(
    metrics: dict,
    parameters: dict,
    baseline_metrics: dict | None = None,
    diagnostics_frames: dict | None = None,
) -> str:
    """Assemble enabled sections (§0–§8) into the final HTML string."""
    candidates = [
        build_headline_section(metrics, parameters),
        build_dataset_overview_section(metrics, parameters),
        build_primary_map_section(metrics, parameters),
        build_guardrail_recall_section(metrics, parameters),
        build_category_section(metrics, parameters),
        build_segment_section(metrics, parameters),
        build_diagnostics_section(diagnostics_frames, parameters),
        build_baseline_section(metrics, baseline_metrics, parameters),
        build_glossary_section(parameters),
    ]
    sections = [s for s in candidates if s is not None]
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "Snap Date": eval_params.get("snap_date", "unknown"),
        "Total Queries": metrics.get("n_queries"),
        "Excluded Queries": metrics.get("n_excluded_queries"),
    }
    return generate_html_report(
        sections, title="Model Evaluation Report", metadata=metadata
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_evaluation/test_report_builder.py -v`
Expected: PASS (all)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(evaluation): report_builder guardrail/category/segment/baseline/glossary + assemble"
```

---

## Task 12: nodes_spark — slim generate_report, delete `_render_html_report`

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Test: `tests/test_pipelines/test_evaluation/test_generate_report.py` (create; create `__init__.py` if the dir is new)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_pipelines/test_evaluation/test_generate_report.py
"""generate_report: dict-driven HTML, toPandas only when diagnostics on."""

from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report


def _params(diagnostics=False):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": "20240331", "report": {
            "sections": {"diagnostics": diagnostics},
            "display": {"primary_map_k": [1], "guardrail_recall_k": [1]},
            "diagnostics": {"include_distributions": diagnostics,
                            "include_calibration": False,
                            "sample_rows": None}}},
    }


def _eval_pred(spark):
    return spark.createDataFrame(
        [("20240331", "c1", "A", 0.9, 1, 1),
         ("20240331", "c1", "B", 0.1, 0, 2),
         ("20240331", "c2", "A", 0.2, 0, 2),
         ("20240331", "c2", "B", 0.8, 1, 1)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def _metrics():
    return {
        "overall": {"map@1": 0.5},
        "per_item": {"A": {"hit_rate@1": 0.5, "mean_pos": 1.5}},
        "per_segment": {},
        "dataset_overview": {
            "totals": {"n_rows": 4, "n_customers": 2, "n_products": 2,
                       "n_snap_dates": 1, "n_positives": 2,
                       "positive_rate": 0.5,
                       "avg_positives_per_customer": 1.0},
            "by_snap_date": {}, "by_item": {}},
        "n_queries": 2, "n_excluded_queries": 0,
    }


def test_generate_report_html_no_diagnostics(spark):
    html = generate_report(_eval_pred(spark), _metrics(), _params(False), None)
    assert html.startswith("<!DOCTYPE html>")
    assert "摘要 Headline" in html
    assert "<details" not in html      # diagnostics off


def test_generate_report_with_diagnostics(spark):
    html = generate_report(_eval_pred(spark), _metrics(),
                           _params(True), None)
    assert "<details" in html          # collapsible diagnostics present
```

Also create empty `tests/test_pipelines/test_evaluation/__init__.py` if missing.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipelines/test_evaluation/test_generate_report.py -v`
Expected: FAIL (current generate_report calls removed `_render_html_report`)

- [ ] **Step 3: Replace `generate_report` and delete `_render_html_report`**

In `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`:

(a) Replace the imports block (lines 12-28) with only what is still used:

```python
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.calibration import plot_calibration_curves
from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_rank_heatmap,
    plot_score_distributions,
    plot_score_distributions_by_label,
)
from recsys_tfb.evaluation.report_builder import assemble_report
```

(b) Replace `generate_report` (lines 139-153) and delete `_render_html_report` entirely (lines 156-338) with:

```python
def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
) -> str:
    """Build the HTML report. Metrics dicts drive §0–§8; only the
    diagnostics section (when enabled) needs row-level pandas, collected
    here with minimal columns and an optional sample cap.
    """
    schema = get_schema(parameters)
    id_cols = schema["identity_columns"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    item_col = schema["item"]

    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}

    diagnostics_frames = None
    if sections_cfg.get("diagnostics", True):
        sample_rows = diag_cfg.get("sample_rows")
        sdf = eval_predictions
        if sample_rows:
            sdf = sdf.limit(int(sample_rows))
        pred_cols = list(dict.fromkeys(id_cols + [score_col, rank_col]))
        predictions = sdf.select(*pred_cols).toPandas()
        labels = (
            sdf.select(*list(dict.fromkeys(id_cols + [label_col])))
            .distinct()
            .toPandas()
        )
        figs = []
        if diag_cfg.get("include_distributions", True):
            figs += plot_score_distributions(
                predictions, item_col=item_col, score_col=score_col
            )
            figs += plot_score_distributions_by_label(
                predictions, labels, id_cols=tuple(id_cols),
                item_col=item_col, score_col=score_col, label_col=label_col
            )
            figs.append(
                plot_rank_heatmap(
                    predictions, item_col=item_col, rank_col=rank_col
                )
            )
            figs.append(
                plot_positive_rank_heatmap(
                    predictions, labels, id_cols=tuple(id_cols),
                    item_col=item_col, rank_col=rank_col, label_col=label_col
                )
            )
        if diag_cfg.get("include_calibration", True):
            figs.append(
                plot_calibration_curves(
                    predictions, labels,
                    n_bins=diag_cfg.get("n_calibration_bins", 10),
                    id_cols=tuple(id_cols), item_col=item_col,
                    score_col=score_col, label_col=label_col,
                )
            )
        diagnostics_frames = {"figures": figs}

    return assemble_report(
        evaluation_metrics, parameters,
        baseline_metrics=baseline_metrics,
        diagnostics_frames=diagnostics_frames,
    )
```

(c) Confirm remaining unused imports are gone (the old `datetime`, `pandas as pd`, `ReportSection`, `compute_product_statistics`, `compute_segment_statistics`, `build_comparison_result`, `plot_comparison_metrics`, `plot_positive_rate_rank_heatmap` imports are no longer referenced — remove any that linger).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_pipelines/test_evaluation/test_generate_report.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/
git commit -m "refactor(evaluation): slim generate_report, delete _render_html_report"
```

---

## Task 13: Full evaluation test sweep + dev-cluster smoke

**Files:** none (verification only)

- [ ] **Step 1: Run the whole evaluation + pipeline suite**

Run: `.venv/bin/python -m pytest tests/test_evaluation tests/test_pipelines/test_evaluation -v`
Expected: PASS (all). Investigate and fix any regression before continuing — do not skip.

- [ ] **Step 2: Run the metrics_spark legacy suite for backward-compat proof**

Run: `.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py -v`
Expected: PASS (every pre-existing test green — proves additive-only output change).

- [ ] **Step 3: dev-cluster end-to-end smoke (manual)**

Per CLAUDE.md (evaluation 走 client-template 預設 conf):

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb evaluation --env production
```

Expected: pipeline completes; `data/evaluation/<model_version>/<snap_date>/report.html` exists and opens with §0 摘要 / §1 資料概況 / §4 大類層級 present. Confirm `metrics.json` (if saved) contains `dataset_overview` and `category` keys.

- [ ] **Step 4: Commit (only if smoke required a fix)**

```bash
git add -A
git commit -m "fix(evaluation): address dev-cluster smoke findings"
```

---

## Self-Review

- **Spec coverage:** §2 報告結構 → Tasks 10–12; §3 大類計算 → Tasks 2–5; §4 模組邊界 (A1 report_builder=10/11, A2 toPandas=12, A3 schema-driven=7, A4 segments=6, A5/D compare=8, C naming=9/11) ; §5 config=1, 錯誤處理=2 (fail-loud) /6 (missing skip), 測試策略=每 Task TDD + Task 13; §1/§6 Out-of-scope segment Hive 已於 Task 6 seam 預留、未實作。無遺漏。
- **Placeholder scan:** 無 TBD/TODO；每 code step 有完整程式。
- **Type consistency:** `compute_all_metrics`/`_compute_core`/`collapse_to_categories`/`compute_dataset_overview`/`_build_category_mapping` 簽名跨 Task 一致；`assemble_report(metrics, parameters, baseline_metrics, diagnostics_frames)` 與 Task 12 呼叫一致；`ReportSection.collapsible` 於 Task 9 定義、Task 11/12 使用；`join_segment_sources` Task 6 定義並於 prepare_eval_data 使用。

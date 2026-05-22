# segment_sources Hive-table Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make evaluation `segment_sources` read segment columns from Hive tables (default: `cust_segment_typ` from `ml_recsys.sample_pool`), so the `report.html` per-segment section renders, and fail loud when a configured segment column has no source.

**Architecture:** `segments.py`'s source seam reads Hive tables via `spark.table()` (no parquet). The segment join moves from `label_table` to the final `eval_predictions` (a pure enrichment, left-join, deduped to one row per customer-key). A new `core/consistency.py` predicate (invariant A10) rejects, at CLI entry, any `segment_columns` entry not provided by a `segment_sources` entry. The synthetic data generator stops emitting `cust_segment_typ` into `label_table` (production label ETL never did).

**Tech Stack:** Python 3.10, PySpark 3.3.2, pytest 7.3.1.

---

## Conventions

All commands run from the worktree. Shorthand used below:

- `WT` = `/Users/curtislu/projects/recsys_tfb/.worktrees/segment-sources-hive`
- `PY` = `/Users/curtislu/projects/recsys_tfb/.venv/bin/python`
- Test command form: `PYTHONPATH=$WT/src $PY -m pytest <paths> -q` — run with `WT`/`PY` expanded to absolute paths.

Pre-flight before any python use (per repo CLAUDE.md):
`readlink $WT/.venv` must be `/Users/curtislu/projects/recsys_tfb/.venv`; `$PY -V` must be `Python 3.10.9`.

## File Structure

- `scripts/generate_synthetic_data.py` — drop `cust_segment_typ` from `generate_label_table`.
- `data/label_table.parquet` — regenerated artifact (no `cust_segment_typ`).
- `src/recsys_tfb/evaluation/segments.py` — source seam → `spark.table()`; `join_segment_sources` gains fail-loud, column check, collision drop, `dropDuplicates`.
- `conf/base/parameters_evaluation.yaml` — `segment_sources` Hive form + `cust_segment_typ` entry + updated comments.
- `src/recsys_tfb/core/consistency.py` — new predicate `segment_columns_without_source` (A10), wired into `validate_config_consistency`.
- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — `join_segment_sources` call relocated to after `eval_predictions` is built.
- Tests: `tests/test_evaluation/test_segments.py` (rewritten), `tests/test_core/test_consistency.py` (new predicate tests), `tests/test_pipelines/test_evaluation/test_nodes_spark.py` (updated + new test).

---

### Task 1: Fix synthetic `label_table` — drop `cust_segment_typ`

`generate_label_table` emits `cust_segment_typ` into `label_table`, but the production label ETL (`conf/sql/etl/label/label_*.sql`) does not, and the generator docstring claims to mirror that SQL. `generate_sample_pool` computes its own segments and only pulls `[snap_date, cust_id, prod_name, label]` from `label_table`, so it is unaffected.

**Files:**
- Modify: `scripts/generate_synthetic_data.py` (`generate_label_table`, ~lines 309-362)
- Regenerate: `data/label_table.parquet`

- [ ] **Step 1: Remove the segment pre-compute block in `generate_label_table`**

Delete these 4 lines (the `# Pre-compute segments` block, ~lines 309-312):

```python
    # Pre-compute segments: use customer index modulo (consistent with generation)
    max_customers = int(INITIAL_CUSTOMERS * (1 + MONTHLY_GROWTH_RATE) ** (len(SNAP_DATES) - 1)) + 10
    all_segments_rng = np.random.default_rng(RANDOM_SEED)
    all_segments = all_segments_rng.choice(SEGMENTS, size=max_customers, p=SEGMENT_PROBS)
```

- [ ] **Step 2: Remove the per-snap `segments` slice**

Delete this line inside the `for snap_date in SNAP_DATES:` loop (~line 323):

```python
        segments = all_segments[:n_cust]
```

- [ ] **Step 3: Remove `kept_segments` and the `cust_segment_typ` column**

Delete this line (~line 351):

```python
            kept_segments = segments[kept_idx]
```

In the `prod_df = pd.DataFrame({...})` dict (~lines 354-362), delete the `cust_segment_typ` entry so the dict reads exactly:

```python
                prod_df = pd.DataFrame({
                    "snap_date": snap_dt,
                    "cust_id": kept_cust_ids,
                    "apply_start_date": apply_start,
                    "apply_end_date": apply_end,
                    "label": group_labels[prod][kept_idx],
                    "prod_name": prod,
                })
```

- [ ] **Step 4: Regenerate the synthetic data**

Run: `cd $WT && PYTHONPATH=$WT/src $PY scripts/generate_synthetic_data.py`
Expected: script prints table shapes; `Label table:` summary no longer needs a segment line.

- [ ] **Step 5: Verify `label_table.parquet` no longer has `cust_segment_typ`**

Run: `$PY -c "import pyarrow.parquet as pq; print(pq.read_schema('$WT/data/label_table.parquet').names)"`
Expected: `['snap_date', 'cust_id', 'apply_start_date', 'apply_end_date', 'label', 'prod_name']` — no `cust_segment_typ`.

- [ ] **Step 6: Discard incidental rewrites of the other two parquet files**

The generator rewrites all three files; `feature_table`/`sample_pool` content is unchanged. Run:
`cd $WT && git checkout -- data/feature_table.parquet data/sample_pool.parquet`
(If `git status` shows them unchanged already, this is a no-op.)

- [ ] **Step 7: Run the product-consistency lint to confirm data still valid**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_pipelines/test_source_etl/test_product_consistency.py -q`
Expected: PASS (item lists unaffected by removing a segment column).

- [ ] **Step 8: Commit**

```bash
cd $WT
git add scripts/generate_synthetic_data.py data/label_table.parquet
git commit -m "fix(synthetic-data): drop cust_segment_typ from label_table

Production label ETL (label_*.sql) never produced this column; the
generator docstring claims to mirror that SQL. Segment columns now come
from segment_sources, not label_table."
```

---

### Task 2: Migrate `segment_sources` to Hive tables

Rewrite the `segments.py` source seam to read Hive tables and harden `join_segment_sources` (fail-loud, column check, collision drop, dedup). Update the default config to the Hive form.

**Files:**
- Modify: `src/recsys_tfb/evaluation/segments.py` (full rewrite)
- Modify: `conf/base/parameters_evaluation.yaml` (lines 14-23)
- Test: `tests/test_evaluation/test_segments.py` (full rewrite)

- [ ] **Step 1: Rewrite the failing test file**

Replace the entire contents of `tests/test_evaluation/test_segments.py` with:

```python
"""Tests for evaluation.segments — Hive-table segment-source join."""

import pytest

from recsys_tfb.evaluation.segments import join_segment_sources


def _df(spark):
    """Base frame the segment sources are joined onto."""
    return spark.createDataFrame(
        [("c0", "20240331", 1), ("c1", "20240331", 0), ("c2", "20240331", 1)],
        schema=["cust_id", "snap_date", "label"],
    )


def _view(spark, name, rows, cols):
    """Register rows as a temp view that spark.table() can resolve."""
    spark.createDataFrame(rows, schema=cols).createOrReplaceTempView(name)
    return name


def test_join_single_source(spark):
    df = _df(spark)
    _view(spark, "hc_tbl",
          [("c0", "20240331", "x"), ("c1", "20240331", "y"),
           ("c2", "20240331", "z")],
          ["cust_id", "snap_date", "holding_combo"])
    cfg = {"holding_combo": {"table": "hc_tbl",
                             "key_columns": ["cust_id", "snap_date"],
                             "segment_column": "holding_combo"}}
    out = join_segment_sources(df, cfg)
    assert "holding_combo" in out.columns
    assert out.count() == 3
    assert out.filter("holding_combo IS NULL").count() == 0


def test_dedup_prevents_fanout(spark):
    """A finer-grained source (multiple rows per key) must not fan out the
    input — dropDuplicates(key_columns) collapses it to one row per key."""
    df = _df(spark)
    # sample_pool-like: one row per (cust, snap, product); segment repeats.
    _view(spark, "pool_tbl",
          [("c0", "20240331", "mass", "A"), ("c0", "20240331", "mass", "B"),
           ("c1", "20240331", "rich", "A"), ("c1", "20240331", "rich", "B"),
           ("c2", "20240331", "mass", "A"), ("c2", "20240331", "mass", "B")],
          ["cust_id", "snap_date", "cust_segment_typ", "prod_name"])
    cfg = {"cust_segment_typ": {"table": "pool_tbl",
                                "key_columns": ["cust_id", "snap_date"],
                                "segment_column": "cust_segment_typ"}}
    out = join_segment_sources(df, cfg)
    assert out.count() == 3  # not 6


def test_missing_table_raises(spark):
    df = _df(spark)
    cfg = {"gone": {"table": "no_such_table",
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "seg"}}
    with pytest.raises(ValueError, match="no_such_table"):
        join_segment_sources(df, cfg)


def test_missing_column_raises(spark):
    df = _df(spark)
    _view(spark, "bad_tbl",
          [("c0", "20240331", "x")],
          ["cust_id", "snap_date", "other_col"])
    cfg = {"seg": {"table": "bad_tbl",
                   "key_columns": ["cust_id", "snap_date"],
                   "segment_column": "seg_col"}}
    with pytest.raises(ValueError, match="seg_col"):
        join_segment_sources(df, cfg)


def test_collision_drops_preexisting_column(spark):
    """When df already carries the segment_column, segment_sources is
    authoritative: the pre-existing column is dropped before the join."""
    df = spark.createDataFrame(
        [("c0", "20240331", 1, "STALE"), ("c1", "20240331", 0, "STALE")],
        schema=["cust_id", "snap_date", "label", "cust_segment_typ"],
    )
    _view(spark, "auth_tbl",
          [("c0", "20240331", "mass"), ("c1", "20240331", "rich")],
          ["cust_id", "snap_date", "cust_segment_typ"])
    cfg = {"cust_segment_typ": {"table": "auth_tbl",
                                "key_columns": ["cust_id", "snap_date"],
                                "segment_column": "cust_segment_typ"}}
    out = join_segment_sources(df, cfg)
    assert out.columns.count("cust_segment_typ") == 1
    vals = {r["cust_id"]: r["cust_segment_typ"] for r in out.collect()}
    assert vals == {"c0": "mass", "c1": "rich"}


def test_partial_join_is_left(spark):
    """Customers absent from the source get NULL (left join)."""
    df = _df(spark)
    _view(spark, "sparse_tbl",
          [("c0", "20240331", "high")],
          ["cust_id", "snap_date", "risk_level"])
    cfg = {"risk": {"table": "sparse_tbl",
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "risk_level"}}
    out = join_segment_sources(df, cfg)
    assert out.count() == 3
    assert out.filter("risk_level IS NOT NULL").count() == 1
    assert out.filter("risk_level IS NULL").count() == 2


def test_multiple_sources(spark):
    df = _df(spark)
    _view(spark, "m_a",
          [("c0", "20240331", "A"), ("c1", "20240331", "A"),
           ("c2", "20240331", "A")],
          ["cust_id", "snap_date", "holding_combo"])
    _view(spark, "m_b",
          [("c0", "20240331", "M"), ("c1", "20240331", "M"),
           ("c2", "20240331", "M")],
          ["cust_id", "snap_date", "risk_level"])
    cfg = {
        "holding_combo": {"table": "m_a",
                          "key_columns": ["cust_id", "snap_date"],
                          "segment_column": "holding_combo"},
        "risk_level": {"table": "m_b",
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "risk_level"},
    }
    out = join_segment_sources(df, cfg)
    assert "holding_combo" in out.columns
    assert "risk_level" in out.columns
    assert out.count() == 3
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_evaluation/test_segments.py -q`
Expected: FAIL — old `segments.py` reads `source_config["filepath"]` (KeyError) and has no fail-loud/dedup behavior.

- [ ] **Step 3: Rewrite `segments.py`**

Replace the entire contents of `src/recsys_tfb/evaluation/segments.py` with:

```python
"""External segment-source joining for evaluation (Spark, Hive-table sources).

``_read_segment_source`` is the source seam: it reads a Hive table via
``spark.table(...)``. Each ``segment_sources`` entry declares a ``table``
(Hive-qualified name), ``key_columns`` and ``segment_column``. A configured
source that cannot be read fails loud — never a silent skip.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _read_segment_source(
    spark: SparkSession, source_config: dict
) -> SparkDataFrame:
    """Read one segment source from its Hive table.

    SEAM: only this function knows the storage backend. ``spark.table``
    failure (e.g. table absent) raises — the caller wraps it with context.
    """
    return spark.table(source_config["table"])


def join_segment_sources(
    df: SparkDataFrame,
    segment_sources: dict,
) -> SparkDataFrame:
    """Left-join each segment column from its Hive table onto ``df``.

    For each entry: read the Hive ``table``, select ``key_columns +
    segment_column``, dedupe to one row per ``key_columns`` (the segment is a
    customer-grained attribute; the source table may be finer-grained), and
    left-join onto ``df``. Fails loud on a missing table or missing column. A
    ``segment_column`` already present on ``df`` is dropped first —
    ``segment_sources`` is the authoritative source for that column.
    """
    spark = df.sparkSession
    for seg_name, source_config in segment_sources.items():
        table = source_config["table"]
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        try:
            seg_df = _read_segment_source(spark, source_config)
        except Exception as e:  # noqa: BLE001 — re-raised with context below
            raise ValueError(
                f"segment source {seg_name!r}: cannot read Hive table "
                f"{table!r}. A configured segment source must exist."
            ) from e

        missing = [
            c for c in key_columns + [segment_column] if c not in seg_df.columns
        ]
        if missing:
            raise ValueError(
                f"segment source {seg_name!r}: Hive table {table!r} is "
                f"missing column(s) {missing}. Expected key_columns + "
                f"segment_column = {key_columns + [segment_column]}; table "
                f"has {seg_df.columns}."
            )

        # segment_sources is authoritative: drop any pre-existing same-named
        # column on df so the join does not produce an ambiguous reference.
        if segment_column in df.columns:
            logger.info(
                "join_segment_sources: dropping pre-existing column %r from "
                "the input; segment source %r is authoritative",
                segment_column, seg_name,
            )
            df = df.drop(segment_column)

        # dropDuplicates(key_columns) guarantees at most one row per key ->
        # the left join cannot fan out df.
        seg = seg_df.select(key_columns + [segment_column]).dropDuplicates(
            key_columns
        )
        df = df.join(seg, on=key_columns, how="left")
        logger.info(
            "Joined segment source %r (%s) from %s",
            seg_name, segment_column, table,
        )

    return df
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_evaluation/test_segments.py -q`
Expected: PASS — all 7 tests.

- [ ] **Step 5: Update the default config**

In `conf/base/parameters_evaluation.yaml`, replace lines 14-23 (the `segment_columns` comment + block and the `segment_sources` comment + block) with:

```yaml
  # Segment columns to slice per-segment metrics by. Each must be delivered by
  # a segment_sources entry below (config-consistency invariant A10).
  segment_columns:
    - cust_segment_typ

  # External segment data sources — Hive tables left-joined onto eval_predictions
  # on key_columns. Each entry: `table` (Hive-qualified), `key_columns`,
  # `segment_column`. A configured source that cannot be read fails loud.
  segment_sources:
    cust_segment_typ:
      table: ml_recsys.sample_pool
      key_columns: [cust_id, snap_date]
      segment_column: cust_segment_typ
    # External Hive-table example (kept commented — no such table by default;
    # an active entry with no real table would fail evaluation loud):
    # holding_combo:
    #   table: ml_recsys.holding_combo
    #   key_columns: [cust_id, snap_date]
    #   segment_column: holding_combo
```

- [ ] **Step 6: Verify the evaluation YAML test still passes**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_evaluation/test_parameters_evaluation_yaml.py -q`
Expected: PASS (this test does not assert on `segment_sources`/`filepath`).

- [ ] **Step 7: Commit**

```bash
cd $WT
git add src/recsys_tfb/evaluation/segments.py tests/test_evaluation/test_segments.py conf/base/parameters_evaluation.yaml
git commit -m "feat(evaluation): segment_sources reads Hive tables, fail-loud

_read_segment_source uses spark.table(); join_segment_sources dedupes to
one row per key (no fan-out), raises on missing table/column, and drops a
pre-existing same-named column (segment_sources is authoritative). Default
config points cust_segment_typ at ml_recsys.sample_pool."
```

---

### Task 3: Config-consistency invariant A10

Add a predicate so a `segment_columns` entry with no providing `segment_sources` entry raises `ConfigConsistencyError` at CLI entry — instead of the per-segment report section silently never rendering.

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_core/test_consistency.py`:

```python
def test_segment_columns_without_source_flags_uncovered():
    from recsys_tfb.core.consistency import segment_columns_without_source
    params = {"evaluation": {
        "segment_columns": ["cust_segment_typ"],
        "segment_sources": {"hc": {"segment_column": "holding_combo"}},
    }}
    assert segment_columns_without_source(params) == ["cust_segment_typ"]


def test_segment_columns_without_source_ok_when_covered():
    from recsys_tfb.core.consistency import segment_columns_without_source
    params = {"evaluation": {
        "segment_columns": ["cust_segment_typ"],
        "segment_sources": {"cs": {"segment_column": "cust_segment_typ"}},
    }}
    assert segment_columns_without_source(params) == []


def test_segment_columns_without_source_empty_when_no_segment_columns():
    from recsys_tfb.core.consistency import segment_columns_without_source
    assert segment_columns_without_source({"evaluation": {}}) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_core/test_consistency.py -q -k segment_columns_without_source`
Expected: FAIL — `ImportError: cannot import name 'segment_columns_without_source'`.

- [ ] **Step 3: Add the predicate to `consistency.py`**

In `src/recsys_tfb/core/consistency.py`, insert this function immediately before `def validate_config_consistency(`:

```python
def segment_columns_without_source(parameters: dict) -> list[str]:
    """evaluation.segment_columns entries with no providing segment_source (A10).

    Every column in ``evaluation.segment_columns`` must be delivered by some
    ``evaluation.segment_sources`` entry's ``segment_column``. Otherwise the
    metric layer silently produces no per_segment results and the report
    drops the per-segment section without warning. Returns sorted offending
    columns; empty list means OK.
    """
    ev = parameters.get("evaluation", {}) or {}
    seg_cols = ev.get("segment_columns", []) or []
    sources = (ev.get("segment_sources", {}) or {}).values()
    provided = {(cfg or {}).get("segment_column") for cfg in sources}
    return sorted(c for c in seg_cols if c not in provided)
```

- [ ] **Step 4: Wire it into `validate_config_consistency`**

In `validate_config_consistency`, immediately before the final `if errors:` block, add:

```python
    seg_no_src = segment_columns_without_source(parameters)
    if seg_no_src:
        errors.append(
            f"evaluation.segment_columns entries {seg_no_src} have no "
            f"evaluation.segment_sources entry providing them (no "
            f"segment_source has a matching segment_column). The per-segment "
            f"report section would silently never render. Add a "
            f"segment_sources entry for each, or remove them from "
            f"segment_columns."
        )
```

- [ ] **Step 5: Add A10 to the module docstring legend**

In the `consistency.py` module docstring, immediately after the A9 bullet (which ends `weight_unknown_items`` (product-only check, mirrors A5).`), add:

```
* A10 — an ``evaluation.segment_columns`` entry has no ``evaluation.
  segment_sources`` entry providing it (matching ``segment_column``); the
  per-segment report section would silently never render. Predicate:
  ``segment_columns_without_source``.
```

- [ ] **Step 6: Run the new tests to verify they pass**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_core/test_consistency.py -q -k segment_columns_without_source`
Expected: PASS — all 3 tests.

- [ ] **Step 7: Run the full consistency suites (regression + wiring)**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS — the default config (updated in Task 2 to provide `cust_segment_typ` via `segment_sources`) satisfies A10.

- [ ] **Step 8: Commit**

```bash
cd $WT
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): A10 — segment_columns must be covered by segment_sources

Rejects at CLI entry any evaluation.segment_columns entry with no
providing segment_sources entry, instead of the per-segment report
section silently never rendering."
```

---

### Task 4: Relocate the segment join in `prepare_eval_data`

Move `join_segment_sources` from joining onto `label_table` (before the labels↔predictions join) to joining onto the final `eval_predictions`. The label side stays minimal; the segment columns become a pure enrichment of the eval table.

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (`prepare_eval_data`)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

- [ ] **Step 1: Write the new failing test and fix the stale one**

In `tests/test_pipelines/test_evaluation/test_nodes_spark.py`, replace the whole function `test_prepare_eval_data_dedupes_label_when_predictions_carry_it` (its `def` line through its last `assert`) with these two functions:

```python
def test_prepare_eval_data_dedupes_label_when_predictions_carry_it(spark):
    """In --post-training mode the predictions source (training_eval_predictions)
    already carries a `label` column. The merge join keys on identity_cols only,
    so without dedup `label` survives on both sides -> AnalysisException:
    reference 'label' is ambiguous. prepare_eval_data must drop the label_table
    side's `label` and keep the predictions' own label.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "label": [1, 0],  # authoritative — scored against at training time
        "model_version": ["v1"] * 2,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "label": [0, 1],  # differing values, to prove which side wins
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)

    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters)

    # Exactly one `label` column survives -> no ambiguous reference.
    assert result.columns.count("label") == 1
    result_pdf = result.select("prod_name", "label").toPandas()
    # Predictions' own label is kept (label_table's differing values discarded).
    by_prod = result_pdf.set_index("prod_name")["label"]
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


def test_prepare_eval_data_joins_segment_sources(spark):
    """segment_sources Hive tables are left-joined onto eval_predictions (after
    the predictions x labels join), enriching it with the segment column
    without changing its row count."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions = spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "rank": [1, 2],
        "model_version": ["v1"] * 2,
    }))
    labels = spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["A", "B"],
        "label": [1, 0],
    }))
    # sample_pool-like source: finer-grained (one row per product).
    spark.createDataFrame(pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31"] * 2,
        "cust_segment_typ": ["mass", "mass"],
        "prod_name": ["A", "B"],
    })).createOrReplaceTempView("seg_pool")

    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {
            "snap_date": "2025-01-31",
            "segment_sources": {"cust_segment_typ": {
                "table": "seg_pool",
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "cust_segment_typ"}},
        },
    }
    result = prepare_eval_data(predictions, labels, parameters).toPandas()
    assert len(result) == 2  # no fan-out from the finer-grained source
    assert set(result["cust_segment_typ"]) == {"mass"}
```

- [ ] **Step 2: Run the tests to verify the new one fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py -q -k "dedupes_label or joins_segment_sources"`
Expected: `test_prepare_eval_data_joins_segment_sources` FAILS — the current code joins segment sources onto `label_table` before the labels filter; with the relocated design not yet in place the assertions on `eval_predictions` enrichment do not hold as intended. `test_prepare_eval_data_dedupes_label...` PASSES.

- [ ] **Step 3: Remove the old segment-join block from `prepare_eval_data`**

In `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`, find this block (just after `eval_params = parameters.get("evaluation", {})`):

```python
    eval_params = parameters.get("evaluation", {})
    segment_sources = eval_params.get("segment_sources", {})

    labels = label_table

    # Join external segment sources (single Spark impl; source seam inside).
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        labels = join_segment_sources(labels, segment_sources)
```

Replace it with:

```python
    eval_params = parameters.get("evaluation", {})

    labels = label_table
```

- [ ] **Step 4: Add the relocated segment-join block after `eval_predictions` is built**

Still in `prepare_eval_data`, find the line `logger.info("Eval data prepared via Spark join")` (near the end, just before `return eval_predictions`). Immediately BEFORE that `logger.info` line, insert:

```python
    # Join segment sources onto the final eval table (Hive-table sources;
    # source seam inside segments). Done here — not on label_table — so the
    # label side stays minimal and segment columns are a pure enrichment.
    segment_sources = eval_params.get("segment_sources", {})
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        eval_predictions = join_segment_sources(eval_predictions, segment_sources)

```

- [ ] **Step 5: Run the evaluation node tests to verify they pass**

Run: `PYTHONPATH=$WT/src $PY -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py -q`
Expected: PASS — all tests, including `test_prepare_eval_data_joins_segment_sources`.

- [ ] **Step 6: Commit**

```bash
cd $WT
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "refactor(evaluation): join segment_sources onto eval_predictions

Segment columns are joined onto the final eval table rather than
label_table — the label side stays minimal and segment columns are a
pure left-join enrichment."
```

---

### Task 5: Full regression of the evaluation suite

- [ ] **Step 1: Run the evaluation + evaluation-pipeline test suites**

Run (may take several minutes — run in background if needed per repo CLAUDE.md):
`PYTHONPATH=$WT/src $PY -m pytest tests/test_evaluation tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS — all tests.

- [ ] **Step 2: If any test fails, fix it before proceeding**

Investigate failures with the systematic-debugging approach; do not paper over them. Re-run until green.

- [ ] **Step 3: Final commit if Step 2 made changes**

```bash
cd $WT
git add -A
git commit -m "test(evaluation): fix regressions from segment_sources Hive migration"
```

---

## Self-Review

**Spec coverage:** Every spec section maps to a task — §1/§6 segment_sources Hive form + default config → Task 2; §2 join relocation → Task 4; §3 dedup → Task 2 (`test_dedup_prevents_fanout`); §4 synthetic-data fix → Task 1; §5 guard model: fail-loud read + column check + collision drop → Task 2, A10 config check → Task 3.

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every command has expected output.

**Type consistency:** `join_segment_sources(df, segment_sources)` signature is consistent across `segments.py`, both call sites in `nodes_spark.py` (one removed, one added), and all tests. `segment_columns_without_source(parameters) -> list[str]` is consistent between definition, wiring, and tests. Config entry keys (`table`, `key_columns`, `segment_column`) are consistent across `segments.py`, the YAML, and all test `cfg` dicts.

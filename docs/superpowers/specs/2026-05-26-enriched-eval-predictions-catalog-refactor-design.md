# Enriched eval_predictions — catalog-driven persist (refactor) — Design

**Status:** Approved (brainstorming complete, locked 2026-05-26). Next: implementation plan.

**Predecessor:** `docs/superpowers/plans/2026-05-24-evaluation-multi-model-compare.md`
(Task 8 originally introduced `persist_eval_predictions` writing directly via
`spark.write.saveAsTable("ml_recsys.eval_predictions")`, with the explicit
decision that "Hive writes are side-effects of pipeline nodes" and the table
"does NOT need a catalog entry". This spec reverses that decision.)

---

## 1. Goal

Bring the evaluation pipeline's persisted output (`ml_recsys.eval_predictions`,
read back in `--compare-only` mode) under the same `HiveTableDataset` +
`${hive.db}` catalog abstraction the rest of the project's Hive tables use
(`training_eval_predictions`, `score_table`, etc.), and rename the table to
`enriched_eval_predictions` to capture its actual abstraction level (raw
predictions enriched with rank + label join + segment join — distinct from
`training_eval_predictions`'s raw per-product per-customer scores).

The current implementation hard-codes `"ml_recsys.eval_predictions"` in node
bodies (write *and* read), manually orchestrates dynamic-partition overwrite
and `model_version` column injection, and uses a sentinel string return value
to fake a DAG edge. Every single one of those concerns is already handled
declaratively by `HiveTableDataset` for sibling tables. The asymmetry — same
class of operation written two completely different ways in adjacent files —
is the bug. This spec eliminates it.

## 2. Problem context

### 2.1 Two Hive tables, two write styles, same project

`pipelines/training/nodes.py::predict_and_write_test_predictions`
(line 575-726) writes the `training_eval_predictions` Hive table via the
catalog runner `@` prefix:

```python
# pipelines/training/pipeline.py:108-115
Node(
    predict_and_write_test_predictions,
    inputs=[
        "model", "test_parquet_handle",
        "preprocessor", "parameters",
        "@training_eval_predictions",  # catalog handle for chunked save
    ],
    outputs="predict_manifest",
)
```

The node body calls `training_eval_predictions.save(out_pdf)` per partition;
`HiveTableDataset.save()` handles CREATE TABLE IF NOT EXISTS, dynamic-partition
overwrite, and partition-column injection internally
(`io/hive_table_dataset.py:142-186`).

`pipelines/evaluation/comparison_nodes.py::persist_eval_predictions`
(line 107-144) writes the `eval_predictions` Hive table entirely outside the
catalog:

```python
df = eval_predictions.withColumn("model_version", F.lit(mv))
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
try:
    (df.write.mode("overwrite")
          .partitionBy(schema["time"], "model_version")
          .format("parquet").saveAsTable("ml_recsys.eval_predictions"))
except Exception as e:
    msg = str(e).lower()
    if "hive" in msg or "metastore" in msg:
        logger.warning("persist_eval_predictions skipped (no Hive): %s", e)
        return f"persisted-skipped:{snap_date}:{mv}"
    raise
return f"persisted:{snap_date}:{mv}"
```

Every single line of imperative work — CREATE TABLE, `partitionOverwriteMode`,
`withColumn` for partition col, the `saveAsTable` call itself — is already
implemented inside `HiveTableDataset` for the sibling table. The `try/except`
no-Hive degrade is also dead code (the only test exercising this path uses a
real local Hive warehouse).

### 2.2 What `eval_predictions` actually is (vs. `training_eval_predictions`)

The two tables are not interchangeable — they sit at different abstraction
levels along the prediction → report pipeline:

| Property | `training_eval_predictions` | `eval_predictions` (current) |
|---|---|---|
| Abstraction level | Raw model output | Report-ready (post-join, post-rank) |
| Customer scope | Only customers with ≥1 positive label (pass-0 filter) | Full ranked candidate set |
| Partition layout | (snap_date × prod_name × model_version) | (snap_date × model_version) |
| `rank` column | Absent (Spark mAP recomputes) | Present (rank_within_query precomputed) |
| `label` column | Direct from predict-time data | LEFT JOIN with label_table + fillna(0) |
| Segment columns | None | `join_segment_sources` enrichment |
| Producer / consumer | training writes; training reads + evaluation (`--post-training`) | evaluation writes; evaluation reads (`--compare-only`) |

So they have distinct semantic roles (`training_eval_predictions` =
source-of-truth for scores; `eval_predictions` = source-of-truth for reports).
Renaming the latter to `enriched_eval_predictions` makes the relationship
explicit at the table-name level.

### 2.3 Design intent — what the cache is for

`persist_eval_predictions` exists so that `--compare-only` mode can run report
comparison without re-running the upstream evaluation pipeline (which would
recompute the most expensive part: `prepare_eval_data`'s four operations —
filter to (snap_date, model_version), LEFT JOIN with label_table,
`join_segment_sources`, `rank_within_query` window function). The cache does
**not** save `compute_all_metrics` work on the eval side: comparison metrics
are mathematically distinct (computed on the restricted common subset of
(cust_id, prod_name) pairs), so the original full-set `evaluation_metrics`
cannot be reused. The cache's value is in skipping data prep, not metric
computation.

## 3. Scope

**In scope:**

- Add catalog entry `enriched_eval_predictions: HiveTableDataset` to
  `conf/base/catalog.yaml`.
- Rewrite `persist_eval_predictions` as an identity pass-through; route the
  write through the catalog (framework auto-save).
- Replace `load_eval_predictions_from_hive` with
  `validate_enriched_eval_predictions_present` — a small validator node that
  runs after the catalog auto-load, filters to the configured snap_date, and
  raises `DataConsistencyError("(B4) ...")` if empty.
- Update `pipeline.py` for all three modes (default / `--compare` /
  `--compare-only`) to use the new node names and catalog routing.
- Update affected tests (4 string-level renames; delete 2 obsolete B4
  tests; add 5 new tests covering validator behavior, catalog round-trip,
  and trivial persist unit — full breakdown in §6).
- Drop the dead `try/except` no-Hive degrade in the existing persist node.
- Drop the existing dev-cluster Hive table `ml_recsys.eval_predictions` (no
  external consumers; data is regenerated on next `--env production`
  evaluation run).

**Out of scope (deferred to follow-up):**

- **Schema evolution.** `columns: "auto"` accepts whatever shape the
  DataFrame has on first write. When `evaluation.segment_sources` config
  changes (adds/removes a segment column), the table must currently be
  dropped and recreated. A future follow-up extends `HiveTableDataset` with
  `ALTER TABLE ADD COLUMNS` so schema can evolve in place; this spec does
  not implement it.
- **Project-wide consistency gate.** The class of bug this spec fixes
  ("node body bypasses `HiveTableDataset` and writes Hive directly") can be
  prevented going forward by a grep- or AST-based unit test that forbids
  `saveAsTable(` / `insertInto(` literals inside `src/recsys_tfb/pipelines/`.
  Specified here as principle, not implemented now.
- **Renaming or relocating any other Hive tables** in the project. Only
  `ml_recsys.eval_predictions` is touched.

## 4. Locked design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Catalog entry name: **`enriched_eval_predictions`** (key in `catalog.yaml`) | Captures the abstraction level ("raw + enrichment for report"); distinct from sibling `training_eval_predictions`. In-memory `eval_predictions` (output of `prepare_eval_data`) stays unchanged to avoid renaming five consumer nodes. |
| D2 | Hive physical table name: **`ml_recsys.enriched_eval_predictions`** (renamed from `ml_recsys.eval_predictions`) | catalog key == Hive table name (project convention, mirrors `training_eval_predictions`). Dev-cluster drop+rerun, no migration. |
| D3 | `columns: "auto"` (inferred from DataFrame at first write) | Reflects the design intent — the schema *should* track changes in `segment_sources` / calibration toggle / mode. Schema rigidity would defeat the cache's "report-ready" purpose. |
| D4 | `partition_filter: {model_version: ${model_version}}` (static, run-singleton) | Mirrors `training_eval_predictions`. Catalog injects `model_version` via `_apply_partition_filter_cols` on write; filters and drops on read. `persist_eval_predictions` no longer manually adds the column. |
| D5 | `partition_cols: [snap_date]` (dynamic, data-driven) | snap_date is the data-side time grain; downstream metrics need the column. Putting it in `partition_filter` would let the catalog drop it on load. |
| D6 | `persist_eval_predictions` becomes an identity pass-through | All write-side machinery (DDL, partition overwrite mode, column injection, write path qualification) lives in the catalog layer. The node exists solely as the named DAG edge. |
| D7 | Replace `load_eval_predictions_from_hive` with `validate_enriched_eval_predictions_present` (small validator, pass-through pattern) | Echoes the project's existing `validate_predictions` pattern (`pipelines/inference/nodes_spark.py:145`). Catalog handles the load; this node owns only the B4 invariant assertion. |
| D8 | Drop `try/except` no-Hive degrade in persist | Dead code; the one test exercising persist+load uses a real local Hive warehouse. `HiveTableDataset` itself has no such branch. |
| D9 | Drop sentinel-string return from persist; sign down `parameters` arg | Framework auto-save uses the returned DataFrame; no DAG-edge faking needed. `parameters` is unused after dropping the literal table name and partition-mode plumbing. |

## 5. Architecture — components & interfaces

### 5.1 catalog entry (`conf/base/catalog.yaml`)

Insert after the existing `training_eval_predictions` block:

```yaml
# --- Evaluation Pipeline - Cached, report-ready eval_predictions ---
# Written by evaluation/comparison_nodes.py::persist_eval_predictions (the
# in-memory eval_predictions from prepare_eval_data, after label join, rank,
# and segment enrichment). Read back by --compare-only mode via catalog
# auto-load + assert_enriched_eval_predictions_present (B4 validator).
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

Physical DDL emitted on first write (`HiveTableDataset._build_create_ddl`):

```sql
CREATE TABLE ml_recsys.enriched_eval_predictions (
    <auto-inferred non-partition columns from DataFrame>
)
PARTITIONED BY (model_version STRING, snap_date STRING)
STORED AS PARQUET
```

Partition path order becomes `model_version=<mv>/snap_date=<date>/` (was
`snap_date=<date>/model_version=<mv>/` under the old `partitionBy(time,
model_version)` direct write). Since this is a rename+rebuild, no migration.

### 5.2 `persist_eval_predictions` (new body)

`src/recsys_tfb/pipelines/evaluation/comparison_nodes.py`:

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

Removed (vs. before):

- `CREATE DATABASE IF NOT EXISTS ml_recsys` — catalog handles via
  `${hive.db}` (database created via `scripts/setup_hive_dev.py` for dev or
  one-off DDL for production).
- `withColumn("model_version", F.lit(mv))` — catalog handles via
  `_apply_partition_filter_cols` (`io/hive_table_dataset.py:227-229`).
- `spark.conf.set("partitionOverwriteMode", "dynamic")` — catalog handles
  (`io/hive_table_dataset.py:164-166`).
- `.partitionBy(...).saveAsTable(literal)` — framework auto-save invokes
  `HiveTableDataset.save()` which calls `insertInto(qualified_name)`.
- `try/except Hive metastore not available` — dead code (D8).
- Sentinel string return `f"persisted:{snap_date}:{mv}"` — D9.
- `df.count()` log — `HiveTableDataset.save()` already logs partitions
  written (`io/hive_table_dataset.py:181-186`).

Signature narrowed: `parameters: dict` argument removed.

### 5.3 `validate_enriched_eval_predictions_present` (new validator)

`src/recsys_tfb/pipelines/evaluation/comparison_nodes.py` (replaces
`load_eval_predictions_from_hive`):

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

### 5.4 `pipeline.py` three-mode structure

`src/recsys_tfb/pipelines/evaluation/pipeline.py`:

**Default mode (no flags) — 5 nodes:**

```python
nodes = [
    Node(prepare_eval_data, inputs=[predictions_input, "label_table", "parameters"],
         outputs="eval_predictions"),
    Node(compute_metrics, inputs=["eval_predictions", "parameters"],
         outputs="evaluation_metrics"),
    Node(compute_baseline_metrics, inputs=["eval_predictions", "label_table", "parameters"],
         outputs="baseline_metrics"),
    Node(generate_report, inputs=["eval_predictions", "evaluation_metrics",
                                  "parameters", "baseline_metrics"],
         outputs="evaluation_report"),
    Node(persist_eval_predictions, inputs=["eval_predictions"],
         outputs="enriched_eval_predictions"),  # ← catalog HiveTableDataset auto-saved
]
```

`predictions_input = "training_eval_predictions" if post_training else "ranked_predictions"`
(unchanged).

**`--compare` mode — appends 3 nodes:** `load_compare_predictions`,
`restrict_to_common`, `generate_comparison_report` (unchanged signatures and
wiring).

**`--compare-only` mode — 4 nodes:**

```python
return Pipeline([
    Node(
        validate_enriched_eval_predictions_present,
        inputs=["enriched_eval_predictions", "parameters"],  # ← catalog auto-load
        outputs="eval_predictions",
    ),
    Node(load_compare_predictions, inputs=["parameters"],
         outputs="compare_predictions_raw"),
    Node(restrict_to_common,
         inputs=["eval_predictions", "compare_predictions_raw",
                 "label_table", "parameters"],
         outputs=["eval_predictions_common", "compare_predictions_common",
                  "compare_coverage_partial"]),
    Node(generate_comparison_report,
         inputs=["eval_predictions_common", "compare_predictions_common",
                 "compare_coverage_partial", "parameters"],
         outputs="evaluation_comparison_report"),
])
```

### 5.5 Runner / DAG consistency

Verified against `core/runner.py:30-43` (input availability validation) and
`core/runner.py:151-162` (intermediate dataset release):

- `--compare-only`: `enriched_eval_predictions` is in catalog (D1) → enters
  the runner's `available` set → first node's input check passes →
  `catalog.load("enriched_eval_predictions")` invokes `HiveTableDataset.load()`
  with `WHERE model_version='<mv>'`, drops the `model_version` column, returns
  the SparkDataFrame.
- Default / `--compare`: persist node's `outputs="enriched_eval_predictions"`
  matches the catalog entry → runner calls `catalog.save("enriched_eval_predictions", df)`
  → `HiveTableDataset.save()` runs `_apply_partition_filter_cols`,
  `_ensure_table_exists`, and `insertInto`. Output is a "terminal" (not
  consumed in this pipeline) so it's not released as an intermediate.

No runner code changes.

## 6. Tests

### 6.1 Affected existing tests

| File | Location | Change |
|---|---|---|
| `test_pipelines/test_evaluation/test_pipeline.py:23` | `test_pipeline_outputs` (default) | `s/eval_predictions_persisted_sentinel/enriched_eval_predictions/` |
| `test_pipelines/test_evaluation/test_pipeline.py:54` | `test_pipeline_outputs_same_as_default` (post_training) | same |
| `test_pipelines/test_evaluation/test_pipeline.py:94-99` | `test_pipeline_node_names` (compare_only) | `s/load_eval_predictions_from_hive/validate_enriched_eval_predictions_present/` |
| `test_pipelines/test_evaluation_compare_pipeline.py:35` | `test_compare_only_mode_skips_compute_nodes` | same |
| `test_pipelines/test_evaluation_compare_pipeline.py:62-93` | `test_b4_load_from_hive_fails_loud_on_missing_partition` | **rewrite** (validator unit test) |
| `test_pipelines/test_evaluation_compare_pipeline.py:96-127` | `test_b4_load_from_hive_returns_partition_when_present` | **rewrite** (catalog round-trip + validator pass) |

### 6.2 New / rewritten tests

**B4 validator — three behaviors:**

1. `test_b4_validator_raises_when_partition_empty` — validator receives an
   empty DataFrame (simulates "no rows after catalog filter applied"); expect
   `DataConsistencyError, match="B4"`.
2. `test_b4_validator_raises_when_snap_date_filter_yields_empty` — validator
   receives a DataFrame whose `snap_date` values don't include the configured
   `evaluation.snap_date`; expect `DataConsistencyError, match="B4"`.
3. `test_b4_validator_passes_when_partition_present` — validator returns the
   snap_date-filtered DataFrame unchanged otherwise.

**Catalog round-trip integration:**

4. `test_persist_and_catalog_load_roundtrip` — instantiate `HiveTableDataset`
   with the spec's catalog config; call `persist_eval_predictions(df)`, then
   `ds.save(returned)`, then `ds.load()`; verify `model_version` is dropped
   from the loaded DF and the data round-trips intact. Uses the existing
   local-Hive warehouse fixture pattern (`_warehouse_table_dir`).

**persist trivial unit:**

5. `test_persist_eval_predictions_returns_input_df` — assert `persist(df) is df`
   (identity pass-through; catalog is responsible for the actual write).

### 6.3 Existing tests that don't need changes

- Node-count assertions across default / `--compare` / `--compare-only` modes
  (5 / 8 / 4 nodes respectively) — unchanged.
- `persist_eval_predictions` *node name* assertions — unchanged (function
  name kept).

## 7. Migration

### Dev-cluster

The existing `ml_recsys.eval_predictions` table on dev-cluster has no external
consumers (internal to the evaluation pipeline). One-time drop:

```bash
scripts/dev_admin.sh -c "DROP TABLE IF EXISTS ml_recsys.eval_predictions"
```

Next `--env production` evaluation run materializes
`ml_recsys.enriched_eval_predictions` via the new catalog path.

### Production

Same pattern: a one-time drop of the legacy table on first deployment.
(No data preservation needed; the table is per-run idempotent and trivially
regenerated.)

## 8. Open issues & deferred follow-ups

1. **Schema evolution.** When `segment_sources` config changes, the
   `columns: "auto"` table needs to be dropped+recreated. Follow-up:
   extend `HiveTableDataset` with `ALTER TABLE ADD COLUMNS` triggered when
   the inferred DataFrame schema is a superset of the existing table schema.
2. **Consistency gate (no-direct-saveAsTable rule).** A grep- or AST-based
   unit test that forbids `saveAsTable(` / `insertInto(` literals in
   `src/recsys_tfb/pipelines/` would prevent this class of bug from
   recurring. Specified as a principle here (§3 out-of-scope); future PR.
3. **`compute_all_metrics` recomputation in `--compare-only`.** The cache
   saves `prepare_eval_data` cost but not `compute_all_metrics` (which must
   recompute on the restricted common subset). If `--compare-only` proves
   too slow in practice, the optimization is incremental/vectorized
   `compute_all_metrics`, not a different cache layout.

## 9. Invariant legend addition (optional, not implemented in this spec)

If/when the consistency-gate follow-up lands, the new invariant codes:

- **C1** (proposed) — `src/recsys_tfb/pipelines/**/*.py` must not contain
  literal `.saveAsTable(` or `.insertInto(` calls; all Hive writes go through
  catalog `HiveTableDataset` entries.

This spec does not register C1 in `core/consistency.py`; the follow-up will.

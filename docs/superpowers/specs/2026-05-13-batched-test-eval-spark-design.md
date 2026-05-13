# Batched Test Eval + Spark mAP Design

**Date:** 2026-05-13
**Status:** Draft (pending user review)
**Author:** Claude (with curtis-lu)

## Problem

`evaluate_model` + `compute_test_mAP` in the training pipeline run entirely in pandas on the driver. For production scale (10M customers × 22 products ≈ 220M rows × ~1500 features), this OOMs the 128GB driver at two points:

1. `evaluate_model:557` — `extract_Xy(eval_parquet_handle, ...)` materializes the full X matrix in pandas (TB-class).
2. `compute_test_mAP` — the merged `predictions × labels` pandas DataFrame and its per-customer AP computation are also pandas-bound on the same driver.

The current `write_test_predictions` already iterates per `prod_name` to write Hive, but the chunking does not help OOM because predictions_pdf is already fully materialized in pandas by the time write happens.

## Goal

Refactor the training pipeline's test-set evaluation path so that:

1. Test-set prediction is **batched at the `(snap_date, prod_name)` partition boundary** — peak driver memory bounded by a single partition's worth of customers × features.
2. Predictions are written to Hive one partition at a time, **without the partition-overwrite double-write footgun**.
3. `compute_test_mAP` runs **Spark-native** by reading from the Hive table, so the per-customer AP computation never touches pandas.
4. The `training_eval_predictions` table becomes the single source of truth — predictions, label, calibration info, and identity all in one place — so downstream evaluation `--post-training` does not need to re-join `label_table`.

Non-goals (for this spec):

- Changes to inference pipeline (it serves business need for full-customer scoring; OOM doesn't apply because it's already Spark).
- Touching `finalize_model` / `calibrate_model` / `tune_hyperparameters` data paths.
- Changing the `score_uncalibrated` semantics (always raw model output; equals `score` when no calibration).

## Architecture

```
test_parquet_handle (Hive: test_model_input — already contains label)
  │
  ├──→ predict_and_write_test_predictions  [REPLACES evaluate_model + write_test_predictions]
  │      Pass 0 (once): label-only scan of the parquet to build, per snap_date,
  │        the set of cust_ids with ≥1 positive across all prods in that
  │        snap_date. Reads only (cust_id, snap_date, label) — negligible memory.
  │      Pass 1: For each (snap_date, prod_name) partition:
  │        1. Load only that partition's rows from the parquet handle
  │        2. Filter rows to customers in the positive-set for this snap_date
  │        3. extract X for the filtered slice
  │        4. model.predict → score (+ predict_uncalibrated if CalibratedModelAdapter)
  │        5. Build spark DF with (cust_id, score, score_uncalibrated, label)
  │           + partition cols (snap_date, prod_name) — model_version injected by catalog
  │        6. catalog.save() — HiveTableDataset writes that one partition
  │      Returns: manifest dict {snap_dates: [...], prods: [...], model_version, n_rows_written}
  │
  ▼
training_eval_predictions (Hive)
  schema: cust_id, score, score_uncalibrated, rank, label
  partition: snap_date, prod_name, model_version
  semantics: only customers with ≥1 positive label in test window
  │
  ├──→ compute_test_mAP_spark  [Spark-native]
  │      inputs: training_eval_predictions (Spark DF), predict_manifest, parameters
  │      Reuses evaluation.metrics_spark.compute_all_metrics(...)
  │      Returns: dict for log_experiment (same shape as today)
  │
  ▼
log_experiment (unchanged)
```

## Detailed Design

### 1. `training_eval_predictions` catalog entry

Add `label` column; add `partition_filter: model_version: ${model_version}` so `HiveTableDataset` injects model_version automatically and downstream loads are pre-filtered.

```yaml
training_eval_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: training_eval_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: score_uncalibrated, type: DOUBLE}  # raw model output; equals score when calibration off
    - {name: label, type: INT}  # NEW — copied from test_model_input
  # NOTE: rank removed — not consumed by compute_test_mAP_spark or downstream
  # prepare_eval_data; Spark mAP recomputes it internally. See spec §2.
  partition_filter:
    model_version: ${model_version}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
```

**Schema migration note:** existing rows from prior training runs lack the `label` column. Strategy: since this table is rewritten on every training run (overwriting the partitions for the current `model_version`), we accept that historical partitions for *other* model_versions will be schema-incompatible. Add a NOTE in the catalog comment; if a real migration is needed for archival reads, it goes outside this spec.

### 2. New node: `predict_and_write_test_predictions`

Replaces both `evaluate_model` and `write_test_predictions`. Lives in `src/recsys_tfb/pipelines/training/nodes.py`.

**Signature:**

```python
def predict_and_write_test_predictions(
    model: ModelAdapter,
    test_parquet_handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    training_eval_predictions: HiveTableDataset,  # catalog handle, for chunked save
) -> dict:
    """Per-partition test prediction + Hive write.

    Iterates (snap_date, prod_name) partitions of the test parquet, filters to
    customers with ≥1 positive label in the test window, predicts, and writes
    each partition to training_eval_predictions in a single insertInto so that
    dynamic-partition overwrite mode does not double-write.

    Returns a manifest dict for downstream compute_test_mAP to depend on
    (DAG ordering — actual data is read back from Hive).
    """
```

**Key behaviors:**

- **Partition enumeration:** use pyarrow.dataset to list `(snap_date, prod_name)` combos in the parquet without reading payload (`ds.partitioning` if partitioned; otherwise `select(distinct)` on the loaded pdf — but that defeats the purpose, so we will require the parquet to be partitioned on these columns or we filter by `(snap_date, prod_name)` against `ds.to_table(filter=...)`).
- **Positive-customer set (Pass 0):** before the main loop, do a single column-projected scan of the test parquet reading only `(cust_id, snap_date, label)` columns — this is small (10M × 22 × 3 cols ≈ a few GB even at full scale, but most parquets compress this well and we collect only `label==1` rows). Group by `snap_date` to get `set[cust_id]` per snap_date. This is the positive set the per-partition filter (Pass 1) uses. Doing it once avoids the per-prod-chunk semantic bug where a customer who has a positive in prod_A but not prod_B would be incorrectly dropped when processing prod_B.
- **Per-partition predict (Pass 1):** within each `(snap_date, prod_name)` slice, drop rows whose `cust_id` is not in the snap_date's positive set; then extract X → `model.predict(X)` → assign `score_uncalibrated` (raw) → if `CalibratedModelAdapter`, also `model.predict_uncalibrated(X)` for the raw column.
- **No rank column:** rank is not written; it's recomputed by Spark mAP downstream (`rank_within_query` in `evaluation/metrics_spark.py`). Compatibility check: `prepare_eval_data` in `pipelines/evaluation/nodes_spark.py` reads `training_eval_predictions` but does not select `rank` (verified by grep in the implementation plan).
- **Write:** call `training_eval_predictions.save(spark_df)` per partition. Because each save's DataFrame contains exactly one `(snap_date, prod_name)` partition's worth of rows, dynamic-partition overwrite cleanly overwrites that single partition. Subsequent saves overwrite different partitions and do not collide.

**Decision: drop `rank` from the written schema**

Today's `evaluate_model` ranks score *within `(snap_date, cust_id)` across all products*. If we chunk per-prod, we cannot compute this in a single pass. Alternatives considered:

- **(a)** Drop `rank` from `training_eval_predictions` — verified that neither `compute_test_mAP` nor downstream `prepare_eval_data` reads `rank`; Spark mAP recomputes rank internally via `rank_within_query`. **(Chosen.)**
- **(b)** After all per-prod writes complete, run a Spark pass to compute rank and overwrite. Doubles the I/O for a column nobody reads.
- **(c)** Compute rank within the per-prod batch only — rank always = 1. Misleading.

### 3. `compute_test_mAP_spark`

Replaces the pandas `compute_test_mAP`. Lives in `src/recsys_tfb/pipelines/training/nodes.py`.

**Signature:**

```python
def compute_test_mAP_spark(
    training_eval_predictions: SparkDataFrame,  # catalog-loaded, already filtered to current model_version via partition_filter
    predict_manifest: dict,                      # forces DAG ordering on the write node
    parameters: dict,
) -> dict:
    """Spark-native mAP over the per-test-snap_date partitions just written.

    Reuses evaluation.metrics_spark.compute_all_metrics(...). Returns the same
    dict shape consumed by log_experiment today, including the optional
    'uncalibrated' sub-dict when calibration was applied.
    """
```

**Body sketch:**

```python
from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

schema = get_schema(parameters)
item_col = schema["item"]

# n_prods is derived from the data — same approach as today's pandas compute_test_mAP
# (which uses test_predictions_pdf[item_col].nunique()). map@all is keyed by n_prods.
n_prods = training_eval_predictions.select(item_col).distinct().count()
map_key = f"map@{n_prods}"

# Detect calibration: score != score_uncalibrated for any row
calibration_applied = training_eval_predictions.filter(
    F.col("score") != F.col("score_uncalibrated")
).limit(1).count() > 0

# Calibrated metrics (or only metrics, if no calibration)
cal = compute_all_metrics(training_eval_predictions, parameters)
result = {
    "overall_map": cal["overall"].get(map_key, 0.0),
    "per_product_ap": {p: v.get(map_key, 0.0) for p, v in cal["per_product"].items()},
    "n_queries": cal["n_queries"],
    "n_excluded_queries": cal["n_excluded_queries"],
}

if calibration_applied:
    # Run a second pass on score_uncalibrated by aliasing it as score
    uncal_df = training_eval_predictions.withColumnRenamed("score", "_score_calibrated") \
                                        .withColumnRenamed("score_uncalibrated", "score")
    uncal = compute_all_metrics(uncal_df, parameters)
    result["uncalibrated"] = {
        "overall_map": uncal["overall"].get(map_key, 0.0),
        "per_product_ap": {p: v.get(map_key, 0.0) for p, v in uncal["per_product"].items()},
    }
    result["calibration_method"] = parameters.get("training", {}).get("calibration", {}).get("method", "isotonic")
```

### 4. Pipeline.py wiring

`src/recsys_tfb/pipelines/training/pipeline.py`:

```python
nodes.extend([
    Node(
        predict_and_write_test_predictions,
        inputs=[
            "model", "test_parquet_handle", "preprocessor", "parameters",
            "training_eval_predictions",  # catalog handle for chunked save
        ],
        outputs="predict_manifest",
    ),
    Node(
        compute_test_mAP_spark,
        inputs=["training_eval_predictions", "predict_manifest", "parameters"],
        outputs="evaluation_results",
    ),
    Node(
        log_experiment,
        inputs=[
            "model", "best_params", "best_iteration",
            "evaluation_results", "parameters",
        ],
        outputs=None,
    ),
])
```

`write_test_predictions` is removed. `evaluate_model` and `compute_test_mAP` (pandas versions) are removed.

**Open question:** does the framework support passing a catalog handle (`training_eval_predictions`) as an *input* to a node so the function can call `.save()` directly? Or does the framework only consume catalog entries as data (auto-load)? — to be verified in the plan phase by reading `src/recsys_tfb/core/catalog.py` and runner code.

If framework does not support catalog-handle-as-input, fallback: inject the dataset via a thin factory inside the node body (read catalog from `parameters` or via the registry), or perform the chunked write with a direct `HiveTableDataset` instantiation. The DAG still gets `outputs="predict_manifest"` for ordering, and the catalog entry stays as the **load** declaration for compute_test_mAP_spark.

### 5. Refactor: extract `_pdf_to_X` helper from `extract_Xy`

`extract_Xy_with_groups` is **not** modified.

`extract_Xy` (src/recsys_tfb/io/extract.py:71-141) currently does two coupled steps:

- **Step A** — `read_parquet`: `pdf = handle.to_pandas()`
- **Step B** — `pdf → X numpy`: `slice_features` + `encode_categoricals` (for deferred identity cats) + `to_numpy`

The new `predict_and_write_test_predictions` node needs Step B only — it reads each partition's pdf itself via pyarrow (so Step A's "read whole parquet" is the wrong shape), then applies the Pass 0 positive-set filter, then needs to turn the filtered pdf into X for `model.predict`.

**Refactor:**

```python
# src/recsys_tfb/io/extract.py

def _pdf_to_X(
    pdf: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> np.ndarray:
    """Step B: already-loaded pdf → X numpy.

    Encapsulates slice_features + encode_categoricals (deferred identity cats)
    + to_numpy. Used by extract_Xy after its parquet read and by
    predict_and_write_test_predictions after its per-partition pyarrow read
    and positive-set filter.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    # ... size summary log ...

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes

    with log_step(logger, "to_numpy"):
        X = X_df.values
    return X


def extract_Xy(handle, preprocessor_metadata, parameters):
    """Step A + Step B; Step B delegated to _pdf_to_X."""
    schema = get_schema(parameters)
    label_col = schema["label"]
    _log_parquet_metadata(handle)
    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values
    return X, y
```

`extract_Xy_with_groups` does NOT switch to `_pdf_to_X` in this PR — leaving it alone reduces test churn. It already works correctly for its only caller (`tune_hyperparameters`), which reads the val parquet whole.

**Existing tests:**

- `tests/test_io/test_extract.py` — current `extract_Xy` tests verify log events, size summaries, encoded categoricals. After refactor, the log events fire from `_pdf_to_X` (slice_features, encode_categoricals, to_numpy) instead of from `extract_Xy` directly. Tests assert by `caplog` on `recsys_tfb.io.extract` logger — same module, so existing assertions stay green. **Verify in the plan phase** that no test asserts the call-site of the `log_step` events.

**Partition-directory read inside `predict_and_write_test_predictions`:**

The new node reads each `(snap_date, prod_name)` partition via pyarrow directly. The training cache root (`_materialize_parquet_handle`) lays out the test parquet with Hive-style partitioning, so:

```python
import pyarrow.dataset as pads

ds = pads.dataset(test_parquet_handle.path, format="parquet")
# Pass 0: label-only scan for positive customer set per snap_date
labels_table = ds.to_table(columns=[entity_col, time_col, label_col])
# build positive_set[snap_date] = set(cust_id with label==1)

# Pass 1: per-partition read
for snap_date, prod_name in distinct_partitions:
    partition_table = ds.to_table(
        filter=(pads.field(time_col) == snap_date) & (pads.field(item_col) == prod_name)
    )
    partition_pdf = partition_table.to_pandas()
    partition_pdf = partition_pdf[
        partition_pdf[entity_col].isin(positive_set[snap_date])
    ]
    X = _pdf_to_X(partition_pdf, preprocessor_metadata, parameters)
    y_score = model.predict(X)
    # ... build spark DF + catalog.save() ...
```

**Risk:** if the cached test parquet is NOT laid out as a directory-of-partitions (just a single .parquet file with all rows interleaved), pyarrow filter still works but does not skip row groups efficiently — overhead is one full-file scan per partition. Plan phase must verify cache layout in `cache_test_model_input` / `_materialize_parquet_handle`. If layout is single-file, either (i) accept the scan overhead (still beats loading 220M rows into pandas once), or (ii) extend the cache step to write partitioned-by-`(snap_date, prod_name)`. Decision deferred to plan.

## Testing Strategy

- **Unit (pandas-free, no Spark):**
  - `predict_and_write_test_predictions` with mocked model + small synthetic parquet → verify manifest content + that `HiveTableDataset.save` is called once per partition.
  - `compute_test_mAP_spark` with synthetic Spark DF → verify dict shape and that calibration branch fires only when score ≠ score_uncalibrated.
- **Integration (dev-cluster):**
  - Full training pipeline run on synthetic `data/{feature_table,label_table,sample_pool}.parquet` (the existing dev fixtures) with `--env production` against local dev-cluster.
  - Verify `training_eval_predictions` partitions written = `len(test_snap_dates) × n_prods`.
  - Verify mAP numerical match against a regression baseline (current pandas mAP on the same fixtures, captured before this change).
- **mAP numerical equivalence:**
  - Spark mAP must equal pandas mAP to 1e-6 on the same data — the existing `evaluation/metrics_spark.compute_all_metrics` already has parity tests; we rely on those.

## Migration / Rollout

- Single PR, single commit-or-few-commits.
- No backwards-compat shim: `training_eval_predictions` table schema changes (drop `rank`, add `label`). Drop the dev-cluster table once before re-running:
  - `scripts/dev_admin.sh -c "DROP TABLE IF EXISTS ml_recsys.training_eval_predictions"` (or via `scripts/nuke_ml_recsys.py` if rerunning fresh).
- Downstream `evaluation/nodes_spark.py::prepare_eval_data`: verify it does not select `rank` from `training_eval_predictions`. If it does, drop that selection — `rank_within_query` recomputes it.

## Risks

- **Framework constraint on catalog handles as node inputs** — if not supported, fallback in §4 adds complexity but doesn't block the design.
- **Partition slicing on the parquet handle** — if `_materialize_parquet_handle` flattens partitions in cache, we can't read a single partition; would need to fall back to loading the whole parquet and filtering in pandas (still bounded since we then filter to positive customers immediately, but defeats the OOM win).
- **Spark mAP performance** — `compute_all_metrics` already exists and is exercised by `evaluation` pipeline; reusing it carries no novel risk.

## Open Questions for Review

1. ~~Drop `rank` from `training_eval_predictions` schema~~ — **Decided: drop.**
2. Catalog-handle-as-input vs in-node direct `HiveTableDataset` instantiation — preference?
3. ~~Partition slicing approach~~ — **Decided: don't modify `extract_Xy_with_groups`; extract `_pdf_to_X` helper from `extract_Xy`; new node reads partitions via pyarrow filter and applies positive-set filter. See §5.**
4. Schema migration acceptance — drop+rewrite for this PR is acceptable in dev; for prod, do we need a migration script?
5. Cache layout: is `cache_test_model_input` storing the test parquet as a directory-of-partitions or a single file? Affects whether pyarrow filter pushes down or does full-file scan per partition (§5 Risk).

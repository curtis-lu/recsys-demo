# Graph Report - recsys_tfb  (2026-05-06)

## Corpus Check
- 148 files · ~257,456 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2017 nodes · 4635 edges · 38 communities detected
- Extraction: 50% EXTRACTED · 50% INFERRED · 0% AMBIGUOUS · INFERRED: 2301 edges (avg confidence: 0.69)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 47|Community 47]]
- [[_COMMUNITY_Community 48|Community 48]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]

## God Nodes (most connected - your core abstractions)
1. `CalibratedModelAdapter` - 94 edges
2. `ModelAdapter` - 94 edges
3. `DataCatalog` - 85 edges
4. `MemoryDataset` - 80 edges
5. `TableConfig` - 72 edges
6. `SQLRunner` - 72 edges
7. `HiveTableDataset` - 69 edges
8. `Node` - 62 edges
9. `Runner` - 61 edges
10. `get_schema()` - 56 edges

## Surprising Connections (you probably didn't know these)
- `Tests for inference pipeline validation (sanity checks).` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `Build a valid ranked_predictions and matching scoring_dataset.` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `Re-rank by score descending within each group.` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `Pipeline` --uses--> `Look up a pipeline by name and return it via the module's create_pipeline().`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/pipeline.py → src/recsys_tfb/pipelines/__init__.py
- `Pipeline` --uses--> `Return all registered pipeline names.`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/pipeline.py → src/recsys_tfb/pipelines/__init__.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.03
Nodes (102): AbstractDataset, Abstract base class for all dataset implementations., DataCatalog, MemoryDataset, Execution metadata for a pipeline run., RunContext, Shared executor for the feature/label/sample_pool ETL sub-commands.      ``stage, Run the feature ETL pipeline (feature_aum/sav/ccard/info/concat/table). (+94 more)

### Community 1 - "Community 1"
Cohesion: 0.02
Nodes (122): ABC, feature_importance(), get_adapter(), log_to_mlflow(), ModelAdapter, predict(), ModelAdapter ABC and adapter registry., Create and return an adapter instance for the given algorithm name. (+114 more)

### Community 2 - "Community 2"
Cohesion: 0.03
Nodes (62): AbstractDataset, exists(), load(), save(), In-memory dataset for intermediate pipeline results., Release the in-memory data to free memory., Manage dataset instances, providing unified load/save/exists interface., Register a dataset programmatically. (+54 more)

### Community 3 - "Community 3"
Cohesion: 0.04
Nodes (84): AuditWriter, Audit logging for source ETL pipeline execution.  Writes audit records to a Hive, Write a summary audit record for the entire snap_date run., Write ETL audit records to Hive and Python structured logging., Create the audit table if it doesn't exist., Insert a single audit record into the Hive audit table., CheckResult, OutputChecker (+76 more)

### Community 4 - "Community 4"
Cohesion: 0.03
Nodes (88): build_comparison_result(), _compute_delta(), _compute_nested_delta(), plot_comparison_metrics(), plot_comparison_score_distributions(), Comparison logic for evaluating two models or model vs baseline., Create overlay histograms and side-by-side boxplots comparing two models.      R, Compute deltas (A - B) for all metrics at all levels.      Args:         result_ (+80 more)

### Community 5 - "Community 5"
Cohesion: 0.03
Nodes (52): Shared functions for the dataset building pipeline., Validate that train, calibration, val, and test snap_dates are mutually non-over, validate_date_splits(), apply_preprocessor_to_features(), build_model_input(), compute_baseline_metrics(), compute_baselines(), fit_preprocessor_metadata() (+44 more)

### Community 6 - "Community 6"
Cohesion: 0.04
Nodes (57): _apply(), ConfigLoader, _deep_merge(), _flatten_params(), Return the merged content of a specific parameters file.          Args:, Flatten nested dict into dotted keys, e.g. {'hive': {'db': 'x'}} → {'hive.db': ', Load and merge YAML config files from base and environment directories., Load all YAML files from a directory, keyed by stem name. (+49 more)

### Community 7 - "Community 7"
Cohesion: 0.03
Nodes (50): _aggregate_metric_lists(), _aggregate_per_dimension(), compute_all_metrics(), compute_ap(), compute_ap_at_k(), compute_mrr(), compute_mrr_at_k(), compute_ndcg() (+42 more)

### Community 8 - "Community 8"
Cohesion: 0.03
Nodes (65): generate_global_popularity_baseline(), generate_segment_popularity_baseline(), Baseline generators for model comparison.  Provides global and segment popularit, Generate a segment-level popularity baseline.      Computes positive rate per (s, Generate a global popularity baseline.      Computes overall positive rate per p, _join_token(), pandas_bucket(), ratio_to_threshold() (+57 more)

### Community 9 - "Community 9"
Cohesion: 0.05
Nodes (33): _base_params(), Tests for recsys_tfb.core.versioning module (three-layer versioning)., _sample_schema(), TestBuildManifestMetadata, TestComputeBaseDatasetVersion, TestComputeCalibrationVariantId, TestComputeModelVersion, TestComputeTrainVariantId (+25 more)

### Community 10 - "Community 10"
Cohesion: 0.05
Nodes (45): _get_preprocessing_config(), Backend-agnostic helpers for preprocessing., Extract drop_columns and categorical_columns from parameters.      Returns:, Check that all required columns exist. Raises ValueError if missing., Log warning for drop_columns that don't exist in the DataFrame., _validate_columns(), _warn_missing_drop_columns(), apply_preprocessor() (+37 more)

### Community 11 - "Community 11"
Cohesion: 0.06
Nodes (39): plot_positive_rank_heatmap(), plot_positive_rate_rank_heatmap(), plot_rank_heatmap(), plot_score_distributions(), plot_score_distributions_by_label(), Score and rank distribution visualizations., Plot score distributions per product.      Returns:         List of two Figures:, Positive rate at each (product, rank) position heatmap.      Cell value = count( (+31 more)

### Community 12 - "Community 12"
Cohesion: 0.05
Nodes (13): get_pipeline(), list_pipelines(), Look up a pipeline by name and return it via the module's create_pipeline()., Return all registered pipeline names., create_pipeline(), Baselines pipeline definition., Tests for training pipeline definition., TestBaselinesPipeline (+5 more)

### Community 13 - "Community 13"
Cohesion: 0.06
Nodes (23): apply_preprocessor(), build_scoring_dataset(), predict_scores(), rank_predictions(), Select validation identity keys (full population, optional random cust_id sampli, select_val_keys(), apply_preprocessor(), Apply preprocessor to Spark inference scoring dataset.      Returns identity + f (+15 more)

### Community 14 - "Community 14"
Cohesion: 0.07
Nodes (26): compute_metrics(), generate_report(), prepare_eval_data(), Join ranked predictions with labels using Spark.      For external segment sourc, Generate HTML report from Spark evaluation results.      Collects the eval_predi, Compute ranking metrics using Spark SQL.      Uses window functions to compute A, Run all three nodes sequentially., TestComputeMetrics (+18 more)

### Community 15 - "Community 15"
Cohesion: 0.08
Nodes (20): main(), Bootstrap dev Hive: write synthetic parquet as ml_recsys.{feature,label,sample_p, format_yaml_output(), _load_pandas(), _load_spark(), main(), _print_summary(), Suggest categorical columns from a dataset.  Given a parquet file or Hive table, (+12 more)

### Community 16 - "Community 16"
Cohesion: 0.09
Nodes (23): find_best_version(), get_current_best_version(), _is_version_dir(), list_versions(), main(), print_version_table(), promote(), Promote a versioned model to best/ for inference use.  Usage:     python scripts (+15 more)

### Community 17 - "Community 17"
Cohesion: 0.11
Nodes (20): build_segment_metrics_table(), compute_segment_metrics(), load_and_join_segment_sources(), _plot_dimension_charts(), plot_segment_charts(), Segment-level metrics and visualizations., Plot grouped bar charts for segment-level metrics.      Args:         segment_me, Create grouped bar charts from dimension metrics. (+12 more)

### Community 18 - "Community 18"
Cohesion: 0.11
Nodes (17): ConsoleFormatter, generate_run_id(), get_current_context(), JsonFormatter, Structured logging framework for pipeline execution.  Provides RunContext for ex, Configure the root logger from config and bind the RunContext.      Args:, Generate a run ID in the format ``YYYYMMDD_HHMMSS_{6 hex chars}``., Return the current RunContext, or None if not set. (+9 more)

### Community 19 - "Community 19"
Cohesion: 0.1
Nodes (8): get_schema_for_hash(), Centralized column schema for all pipelines.  Provides get_schema() to retrieve, Return canonical schema dict intended for version hashing.      Same resolution, Validate the shape of ``parameters["schema"]``.      Enforces:     - Scalar keys, validate_schema_config(), Tests for schema config validation and source_etl consistency checks., TestGetSchemaForHash, TestValidateSchemaConfig

### Community 20 - "Community 20"
Cohesion: 0.14
Nodes (14): validate_predictions(), _make_valid_data(), Tests for inference pipeline validation (sanity checks)., Re-rank by score descending within each group., Build a valid ranked_predictions and matching scoring_dataset., _rerank(), TestCompleteness, TestMultipleFailures (+6 more)

### Community 21 - "Community 21"
Cohesion: 0.13
Nodes (12): _block_vdclient_magic_import(), _install_fake_vdclient_magic(), Tests for resolve_vdclient_placeholders., Install a fake vdclient_magic module with spark_ports(cluster) -> tuple., Make ``import vdclient_magic`` raise ImportError., TestResolve, TestResolveEnv, Resolve ${...} placeholders in spark config values.  Two placeholder forms are s (+4 more)

### Community 22 - "Community 22"
Cohesion: 0.19
Nodes (9): compute_product_statistics(), compute_segment_statistics(), Dataset statistics for evaluation reports., Per-segment statistics at customer granularity.      Returns DataFrame indexed b, Per-product statistics at customer granularity.      Returns DataFrame indexed b, _make_labels(), Tests for evaluation.statistics module., TestProductStatistics (+1 more)

### Community 23 - "Community 23"
Cohesion: 0.15
Nodes (9): _make_base_and_train_variant(), Training pipeline resolves base + train_variant via latest symlinks., Create minimal conf dirs with catalog and optional parameter files., Training pipeline accepts --base-dataset-version and --train-variant., Inference reads base/train_variant from model manifest; outputs under model hash, Create base dataset dir with one train_variant and corresponding latest symlinks, Dataset pipeline computes hash-based base_dataset_version and train_variant_id., _setup_conf() (+1 more)

### Community 24 - "Community 24"
Cohesion: 0.24
Nodes (7): plot_calibration_curves(), Calibration curve visualizations., Plot calibration curves per product.      Args:         predictions: DataFrame w, _make_data(), Tests for evaluation.calibration module., Products with too few samples should be skipped., TestPlotCalibrationCurves

### Community 25 - "Community 25"
Cohesion: 0.24
Nodes (6): _make_parameters(), Cross-validation: Spark metrics vs pandas metrics on the same data.  Requires a, Per-product metrics should match between backends., n_queries and n_excluded_queries should match., Spark and pandas backends should produce matching metrics., TestSparkPandasCrossValidation

### Community 26 - "Community 26"
Cohesion: 0.33
Nodes (5): generate_feature_table(), generate_label_table(), 擴展合成資料產生器，支援多 snap_dates、額外欄位、8 類產品。, 產生合成標籤資料表。      Args:         rng: numpy 隨機數產生器。         snap_dates: 快照日期清單，預設為, 產生合成特徵資料表（22 欄位）。      Args:         rng: numpy 隨機數產生器（確保可重複性）。         snap_dat

### Community 27 - "Community 27"
Cohesion: 0.33
Nodes (1): Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen

### Community 28 - "Community 28"
Cohesion: 0.67
Nodes (1): Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run inside

### Community 47 - "Community 47"
Cohesion: 1.0
Nodes (1): Save data to the dataset.

### Community 48 - "Community 48"
Cohesion: 1.0
Nodes (1): Check if the dataset exists.

### Community 54 - "Community 54"
Cohesion: 1.0
Nodes (1): Train the model. After calling, the adapter holds the trained model.

### Community 55 - "Community 55"
Cohesion: 1.0
Nodes (1): Return probability scores as a 1-D numpy array.

### Community 56 - "Community 56"
Cohesion: 1.0
Nodes (1): Save the model to the given filepath using the algorithm's native format.

### Community 57 - "Community 57"
Cohesion: 1.0
Nodes (1): Load a model from the given filepath into this adapter.

### Community 58 - "Community 58"
Cohesion: 1.0
Nodes (1): Return {feature_name: importance_score}.

### Community 59 - "Community 59"
Cohesion: 1.0
Nodes (1): Log the model artifact using the algorithm's MLflow integration.

### Community 60 - "Community 60"
Cohesion: 1.0
Nodes (1): # NOTE: no _SUCCESS file -> partial

## Knowledge Gaps
- **290 isolated node(s):** `Shared SparkSession for all tests.`, `Create minimal conf dirs with catalog and optional parameter files.`, `Create base dataset dir with one train_variant and corresponding latest symlinks`, `Dataset pipeline computes hash-based base_dataset_version and train_variant_id.`, `Training pipeline resolves base + train_variant via latest symlinks.` (+285 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 27`** (6 nodes): `Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (3 nodes): `main()`, `Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run inside`, `nuke_ml_recsys.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 47`** (1 nodes): `Save data to the dataset.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 48`** (1 nodes): `Check if the dataset exists.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 54`** (1 nodes): `Train the model. After calling, the adapter holds the trained model.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 55`** (1 nodes): `Return probability scores as a 1-D numpy array.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 56`** (1 nodes): `Save the model to the given filepath using the algorithm's native format.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 57`** (1 nodes): `Load a model from the given filepath into this adapter.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 58`** (1 nodes): `Return {feature_name: importance_score}.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 59`** (1 nodes): `Log the model artifact using the algorithm's MLflow integration.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 60`** (1 nodes): `# NOTE: no _SUCCESS file -> partial`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `get_schema()` connect `Community 8` to `Community 1`, `Community 5`, `Community 7`, `Community 10`, `Community 11`, `Community 13`, `Community 14`, `Community 19`, `Community 20`?**
  _High betweenness centrality (0.196) - this node is a cross-community bridge._
- **Why does `exists()` connect `Community 2` to `Community 0`, `Community 4`, `Community 6`, `Community 9`, `Community 11`, `Community 14`, `Community 15`, `Community 16`, `Community 17`, `Community 18`?**
  _High betweenness centrality (0.103) - this node is a cross-community bridge._
- **Why does `SQLRunner` connect `Community 0` to `Community 3`, `Community 6`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Are the 81 inferred relationships involving `CalibratedModelAdapter` (e.g. with `.test_fit_calibrator_isotonic()` and `.test_fit_calibrator_sigmoid()`) actually correct?**
  _`CalibratedModelAdapter` has 81 INFERRED edges - model-reasoned connections that need verification._
- **Are the 91 inferred relationships involving `ModelAdapter` (e.g. with `.test_cannot_instantiate()` and `TestModelAdapterABC`) actually correct?**
  _`ModelAdapter` has 91 INFERRED edges - model-reasoned connections that need verification._
- **Are the 75 inferred relationships involving `DataCatalog` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`DataCatalog` has 75 INFERRED edges - model-reasoned connections that need verification._
- **Are the 71 inferred relationships involving `MemoryDataset` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`MemoryDataset` has 71 INFERRED edges - model-reasoned connections that need verification._
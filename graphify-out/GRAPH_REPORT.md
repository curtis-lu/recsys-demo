# Graph Report - recsys_tfb  (2026-05-07)

## Corpus Check
- 148 files · ~275,247 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2079 nodes · 4756 edges · 70 communities detected
- Extraction: 50% EXTRACTED · 50% INFERRED · 0% AMBIGUOUS · INFERRED: 2394 edges (avg confidence: 0.68)
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
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 49|Community 49]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 64|Community 64]]
- [[_COMMUNITY_Community 65|Community 65]]
- [[_COMMUNITY_Community 66|Community 66]]
- [[_COMMUNITY_Community 67|Community 67]]
- [[_COMMUNITY_Community 68|Community 68]]
- [[_COMMUNITY_Community 69|Community 69]]
- [[_COMMUNITY_Community 70|Community 70]]
- [[_COMMUNITY_Community 71|Community 71]]
- [[_COMMUNITY_Community 72|Community 72]]
- [[_COMMUNITY_Community 73|Community 73]]
- [[_COMMUNITY_Community 74|Community 74]]
- [[_COMMUNITY_Community 75|Community 75]]
- [[_COMMUNITY_Community 76|Community 76]]
- [[_COMMUNITY_Community 77|Community 77]]
- [[_COMMUNITY_Community 78|Community 78]]
- [[_COMMUNITY_Community 79|Community 79]]
- [[_COMMUNITY_Community 80|Community 80]]
- [[_COMMUNITY_Community 81|Community 81]]
- [[_COMMUNITY_Community 82|Community 82]]
- [[_COMMUNITY_Community 83|Community 83]]
- [[_COMMUNITY_Community 84|Community 84]]
- [[_COMMUNITY_Community 85|Community 85]]
- [[_COMMUNITY_Community 86|Community 86]]
- [[_COMMUNITY_Community 87|Community 87]]
- [[_COMMUNITY_Community 88|Community 88]]
- [[_COMMUNITY_Community 89|Community 89]]
- [[_COMMUNITY_Community 90|Community 90]]
- [[_COMMUNITY_Community 91|Community 91]]
- [[_COMMUNITY_Community 92|Community 92]]
- [[_COMMUNITY_Community 93|Community 93]]

## God Nodes (most connected - your core abstractions)
1. `DataCatalog` - 98 edges
2. `CalibratedModelAdapter` - 94 edges
3. `ModelAdapter` - 94 edges
4. `MemoryDataset` - 93 edges
5. `SQLRunner` - 85 edges
6. `Runner` - 74 edges
7. `TableConfig` - 72 edges
8. `HiveTableDataset` - 69 edges
9. `Node` - 62 edges
10. `get_schema()` - 56 edges

## Surprising Connections (you probably didn't know these)
- `Tests for inference pipeline validation (sanity checks).` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `Build a valid ranked_predictions and matching scoring_dataset.` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `Re-rank by score descending within each group.` --uses--> `ValidationError`  [INFERRED]
  tests/test_pipelines/test_inference/test_validation.py → .worktrees/training-cache-model-input/src/recsys_tfb/pipelines/inference/validation.py
- `MemoryDataset` --uses--> `Execute all nodes in the pipeline in topological order.`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/catalog.py → src/recsys_tfb/core/runner.py
- `ConfigLoader` --uses--> `Create or return the SparkSession.      Two call modes:      1. Pipeline entrypo`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/config.py → src/recsys_tfb/utils/spark.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.03
Nodes (126): AbstractDataset, AbstractDataset, Abstract base class for all dataset implementations., DataCatalog, MemoryDataset, In-memory dataset for intermediate pipeline results., Release the in-memory data to free memory., Manage dataset instances, providing unified load/save/exists interface. (+118 more)

### Community 1 - "Community 1"
Cohesion: 0.02
Nodes (122): feature_importance(), get_adapter(), log_to_mlflow(), ModelAdapter, predict(), Create and return an adapter instance for the given algorithm name., Abstract base class for model adapters.      All algorithm-specific adapters inh, train() (+114 more)

### Community 2 - "Community 2"
Cohesion: 0.04
Nodes (85): AuditWriter, Audit logging for source ETL pipeline execution.  Writes audit records to a Hive, Write a summary audit record for the entire snap_date run., Write ETL audit records to Hive and Python structured logging., Create the audit table if it doesn't exist., Insert a single audit record into the Hive audit table., CheckResult, OutputChecker (+77 more)

### Community 3 - "Community 3"
Cohesion: 0.02
Nodes (87): _get_preprocessing_config(), Backend-agnostic helpers for preprocessing., Extract drop_columns and categorical_columns from parameters.      Returns:, Check that all required columns exist. Raises ValueError if missing., Log warning for drop_columns that don't exist in the DataFrame., _validate_columns(), _warn_missing_drop_columns(), log_step() (+79 more)

### Community 4 - "Community 4"
Cohesion: 0.03
Nodes (66): build_comparison_result(), _compute_delta(), _compute_nested_delta(), plot_comparison_metrics(), plot_comparison_score_distributions(), Comparison logic for evaluating two models or model vs baseline., Create overlay histograms and side-by-side boxplots comparing two models.      R, Compute deltas (A - B) for all metrics at all levels.      Args:         result_ (+58 more)

### Community 5 - "Community 5"
Cohesion: 0.03
Nodes (66): _join_token(), pandas_bucket(), ratio_to_threshold(), Deterministic CRC32-based hashing utilities for sampling.  Both PySpark's F.crc3, Convert a [0, 1] sampling ratio into an integer bucket threshold., Build a Spark Column of bucket indices in [0, HASH_BUCKETS).      Datetime/date, Build a numpy array of bucket indices in [0, HASH_BUCKETS).      Mirrors :func:`, spark_bucket() (+58 more)

### Community 6 - "Community 6"
Cohesion: 0.04
Nodes (49): ABC, exists(), load(), ModelAdapter ABC and adapter registry., save(), generate_report(), 讀取 pipeline 產出，產生繁體中文驗證報告。      Args:         scenario_name: 情境名稱。         work_, ParquetDataset (+41 more)

### Community 7 - "Community 7"
Cohesion: 0.03
Nodes (73): Return all registered dataset names., promote_model(), 情境測試共用 fixtures 與 helpers。, 在情境工作目錄下執行 model promote。      Args:         work_dir: 工作目錄路徑。      Raises:, 為情境建立隔離的工作目錄。      Args:         scenario_name: 情境名稱（如 "scenario_1"）。         fe, 在指定工作目錄下用 subprocess 執行 pipeline CLI。      Args:         work_dir: 工作目錄路徑。, run_pipeline(), setup_workdir() (+65 more)

### Community 8 - "Community 8"
Cohesion: 0.04
Nodes (36): _base_params(), Tests for recsys_tfb.core.versioning module (three-layer versioning)., _sample_schema(), TestBuildManifestMetadata, TestComputeBaseDatasetVersion, TestComputeCalibrationVariantId, TestComputeFeatureTableFingerprint, TestComputeModelVersion (+28 more)

### Community 9 - "Community 9"
Cohesion: 0.06
Nodes (26): _escape_sql_value(), _format_col(), HiveTableDataset, _infer_columns_from_spark(), Hive table dataset with INSERT OVERWRITE PARTITION semantics.  Supports both ext, Read/write a Hive table via Spark, with dynamic-partition insert-overwrite., Ensure DataFrame has static partition columns with the filter values.          -, _to_spark() (+18 more)

### Community 10 - "Community 10"
Cohesion: 0.05
Nodes (48): Return the merged content of a specific parameters file.          Args:, Shared SparkSession for all tests., spark(), Exception, get_pipeline(), list_pipelines(), Look up a pipeline by name and return it via the module's create_pipeline()., Return all registered pipeline names. (+40 more)

### Community 11 - "Community 11"
Cohesion: 0.06
Nodes (39): plot_positive_rank_heatmap(), plot_positive_rate_rank_heatmap(), plot_rank_heatmap(), plot_score_distributions(), plot_score_distributions_by_label(), Score and rank distribution visualizations., Plot score distributions per product.      Returns:         List of two Figures:, Positive rate at each (product, rank) position heatmap.      Cell value = count( (+31 more)

### Community 12 - "Community 12"
Cohesion: 0.07
Nodes (28): compute_baseline_metrics(), compute_baselines(), compute_metrics(), generate_report(), prepare_eval_data(), Compute ranking metrics on baseline predictions using Spark SQL.      Collects t, Compute baseline predictions using Spark SQL.      Supports global_popularity an, Join ranked predictions with labels using Spark.      For external segment sourc (+20 more)

### Community 13 - "Community 13"
Cohesion: 0.07
Nodes (8): create_pipeline(), Baselines pipeline definition., Tests for training pipeline definition., TestBaselinesPipeline, TestDatasetPipeline, TestEvaluationPipeline, TestInferencePipeline, TestTrainingPipeline

### Community 14 - "Community 14"
Cohesion: 0.09
Nodes (18): format_yaml_output(), _load_pandas(), _load_spark(), main(), _print_summary(), Suggest categorical columns from a dataset.  Given a parquet file or Hive table,, Format categorical columns as a flat YAML snippet.      Example output:, Suggest categorical columns from a dataset and write a YAML snippet. (+10 more)

### Community 15 - "Community 15"
Cohesion: 0.11
Nodes (20): build_segment_metrics_table(), compute_segment_metrics(), load_and_join_segment_sources(), _plot_dimension_charts(), plot_segment_charts(), Segment-level metrics and visualizations., Plot grouped bar charts for segment-level metrics.      Args:         segment_me, Create grouped bar charts from dimension metrics. (+12 more)

### Community 16 - "Community 16"
Cohesion: 0.11
Nodes (17): ConsoleFormatter, generate_run_id(), get_current_context(), JsonFormatter, Structured logging framework for pipeline execution.  Provides RunContext for ex, Configure the root logger from config and bind the RunContext.      Args:, Generate a run ID in the format ``YYYYMMDD_HHMMSS_{6 hex chars}``., Return the current RunContext, or None if not set. (+9 more)

### Community 17 - "Community 17"
Cohesion: 0.1
Nodes (8): get_schema_for_hash(), Centralized column schema for all pipelines.  Provides get_schema() to retrieve, Return canonical schema dict intended for version hashing.      Same resolution, Validate the shape of ``parameters["schema"]``.      Enforces:     - Scalar keys, validate_schema_config(), Tests for schema config validation and source_etl consistency checks., TestGetSchemaForHash, TestValidateSchemaConfig

### Community 18 - "Community 18"
Cohesion: 0.11
Nodes (11): _apply(), _deep_merge(), _flatten_params(), Flatten nested dict into dotted keys, e.g. {'hive': {'db': 'x'}} → {'hive.db': ', Recursively merge override into base. Non-dict values are replaced., Return catalog configuration dict.          If runtime_params is provided, subst, Return merged dict of all parameters*.yaml files., _substitute() (+3 more)

### Community 19 - "Community 19"
Cohesion: 0.09
Nodes (17): generate_global_popularity_baseline(), generate_segment_popularity_baseline(), Baseline generators for model comparison.  Provides global and segment popularit, Generate a segment-level popularity baseline.      Computes positive rate per (s, Generate a global popularity baseline.      Computes overall positive rate per p, compute_baseline_metrics(), compute_baselines(), Compute baseline predictions using popularity-based methods.      Reads baseline (+9 more)

### Community 20 - "Community 20"
Cohesion: 0.14
Nodes (14): validate_predictions(), _make_valid_data(), Tests for inference pipeline validation (sanity checks)., Re-rank by score descending within each group., Build a valid ranked_predictions and matching scoring_dataset., _rerank(), TestCompleteness, TestMultipleFailures (+6 more)

### Community 21 - "Community 21"
Cohesion: 0.13
Nodes (12): _block_vdclient_magic_import(), _install_fake_vdclient_magic(), Tests for resolve_vdclient_placeholders., Install a fake vdclient_magic module with spark_ports(cluster) -> tuple., Make ``import vdclient_magic`` raise ImportError., TestResolve, TestResolveEnv, Resolve ${...} placeholders in spark config values.  Two placeholder forms are s (+4 more)

### Community 22 - "Community 22"
Cohesion: 0.14
Nodes (11): _make_base_and_train_variant(), _mock_spark_with_feature_table_schema(), Dataset pipeline computes hash-based base_dataset_version and train_variant_id., Create minimal conf dirs with catalog and optional parameter files., Training pipeline resolves base + train_variant via latest symlinks., Training pipeline accepts --base-dataset-version and --train-variant., Inference reads base/train_variant from model manifest; outputs under model hash, Create minimal conf dirs with catalog and optional parameter files. (+3 more)

### Community 23 - "Community 23"
Cohesion: 0.11
Nodes (15): base_dir(), _find_base_dir(), _find_train_variant_dir(), 情境 2：訓練期間往前挪移一個月。  驗證將 val_snap_dates 改為 ["2025-06-30"]、test_snap_dates 改為 ["202, train_model_input 的 snap_dates 不應包含 val (2025-06-30) 和 test (2025-07-31)。, train_dev 的 snap_dates 應為 train 日期的子集（共用日期，按 cust_id 切分）。, val_model_input 的 snap_dates 應恰為 [2025-06-30]。, base_dataset_version 應為 8 字元 hash。 (+7 more)

### Community 24 - "Community 24"
Cohesion: 0.19
Nodes (9): compute_product_statistics(), compute_segment_statistics(), Dataset statistics for evaluation reports., Per-segment statistics at customer granularity.      Returns DataFrame indexed b, Per-product statistics at customer granularity.      Returns DataFrame indexed b, _make_labels(), Tests for evaluation.statistics module., TestProductStatistics (+1 more)

### Community 25 - "Community 25"
Cohesion: 0.24
Nodes (7): plot_calibration_curves(), Calibration curve visualizations., Plot calibration curves per product.      Args:         predictions: DataFrame w, _make_data(), Tests for evaluation.calibration module., Products with too few samples should be skipped., TestPlotCalibrationCurves

### Community 26 - "Community 26"
Cohesion: 0.24
Nodes (6): _make_parameters(), Cross-validation: Spark metrics vs pandas metrics on the same data.  Requires a, Per-product metrics should match between backends., n_queries and n_excluded_queries should match., Spark and pandas backends should produce matching metrics., TestSparkPandasCrossValidation

### Community 27 - "Community 27"
Cohesion: 0.33
Nodes (5): generate_feature_table(), generate_label_table(), 擴展合成資料產生器，支援多 snap_dates、額外欄位、8 類產品。, 產生合成標籤資料表。      Args:         rng: numpy 隨機數產生器。         snap_dates: 快照日期清單，預設為, 產生合成特徵資料表（22 欄位）。      Args:         rng: numpy 隨機數產生器（確保可重複性）。         snap_dat

### Community 28 - "Community 28"
Cohesion: 0.33
Nodes (1): Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen

### Community 30 - "Community 30"
Cohesion: 0.67
Nodes (1): Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run inside

### Community 49 - "Community 49"
Cohesion: 1.0
Nodes (1): Save data to the dataset.

### Community 50 - "Community 50"
Cohesion: 1.0
Nodes (1): Check if the dataset exists.

### Community 56 - "Community 56"
Cohesion: 1.0
Nodes (1): Train the model. After calling, the adapter holds the trained model.

### Community 57 - "Community 57"
Cohesion: 1.0
Nodes (1): Return probability scores as a 1-D numpy array.

### Community 58 - "Community 58"
Cohesion: 1.0
Nodes (1): Save the model to the given filepath using the algorithm's native format.

### Community 59 - "Community 59"
Cohesion: 1.0
Nodes (1): Load a model from the given filepath into this adapter.

### Community 60 - "Community 60"
Cohesion: 1.0
Nodes (1): Return {feature_name: importance_score}.

### Community 61 - "Community 61"
Cohesion: 1.0
Nodes (1): Log the model artifact using the algorithm's MLflow integration.

### Community 62 - "Community 62"
Cohesion: 1.0
Nodes (1): Create base dataset dir with one train_variant and corresponding latest symlinks

### Community 63 - "Community 63"
Cohesion: 1.0
Nodes (1): Dataset pipeline computes hash-based base_dataset_version and train_variant_id.

### Community 64 - "Community 64"
Cohesion: 1.0
Nodes (1): Training pipeline resolves base + train_variant via latest symlinks.

### Community 65 - "Community 65"
Cohesion: 1.0
Nodes (1): Training pipeline accepts --base-dataset-version and --train-variant.

### Community 66 - "Community 66"
Cohesion: 1.0
Nodes (1): Inference reads base/train_variant from model manifest; outputs under model hash

### Community 67 - "Community 67"
Cohesion: 1.0
Nodes (1): When keys include prod_name, join label_table on full identity key.

### Community 68 - "Community 68"
Cohesion: 1.0
Nodes (1): When keys don't include prod_name, expand to all products.

### Community 69 - "Community 69"
Cohesion: 1.0
Nodes (1): prod_name is an identity column — encoding is deferred to training.

### Community 70 - "Community 70"
Cohesion: 1.0
Nodes (1): Hash only the train-sampling subset of dataset params.

### Community 71 - "Community 71"
Cohesion: 1.0
Nodes (1): Hash only the calibration-sampling subset of dataset params.

### Community 72 - "Community 72"
Cohesion: 1.0
Nodes (1): Compute model version ID from training params and dataset variant IDs.

### Community 73 - "Community 73"
Cohesion: 1.0
Nodes (1): Write metadata as manifest.json in the version directory.

### Community 74 - "Community 74"
Cohesion: 1.0
Nodes (1): Read and return manifest.json from a version directory.      Raises FileNotFound

### Community 75 - "Community 75"
Cohesion: 1.0
Nodes (1): Create or update a symlink at *link* pointing to *target*.      If *link* alread

### Community 76 - "Community 76"
Cohesion: 1.0
Nodes (1): Resolve which base dataset version to use.      If *version* is provided, return

### Community 77 - "Community 77"
Cohesion: 1.0
Nodes (1): Resolve a train/calibration variant ID under a base dataset directory.      ``va

### Community 78 - "Community 78"
Cohesion: 1.0
Nodes (1): Return the short git HEAD commit hash, or None if not in a repo.

### Community 79 - "Community 79"
Cohesion: 1.0
Nodes (1): Build a manifest metadata dict with standard fields.      ``parent_version`` and

### Community 80 - "Community 80"
Cohesion: 1.0
Nodes (1): Apply preprocessor to Spark inference scoring dataset.      Returns identity + f

### Community 81 - "Community 81"
Cohesion: 1.0
Nodes (1): Build a manifest metadata dict with standard fields.      ``parent_version`` and

### Community 82 - "Community 82"
Cohesion: 1.0
Nodes (1): Hash non-sampling dataset params together with the canonical schema.      The re

### Community 83 - "Community 83"
Cohesion: 1.0
Nodes (1): Hash only the train-sampling subset of dataset params.

### Community 84 - "Community 84"
Cohesion: 1.0
Nodes (1): Compute model version ID from training params and dataset variant IDs.

### Community 85 - "Community 85"
Cohesion: 1.0
Nodes (1): Write metadata as manifest.json in the version directory.

### Community 86 - "Community 86"
Cohesion: 1.0
Nodes (1): Read and return manifest.json from a version directory.      Raises FileNotFound

### Community 87 - "Community 87"
Cohesion: 1.0
Nodes (1): Create or update a symlink at *link* pointing to *target*.      If *link* alread

### Community 88 - "Community 88"
Cohesion: 1.0
Nodes (1): Resolve which base dataset version to use.      If *version* is provided, return

### Community 89 - "Community 89"
Cohesion: 1.0
Nodes (1): Resolve a train/calibration variant ID under a base dataset directory.      ``va

### Community 90 - "Community 90"
Cohesion: 1.0
Nodes (1): Resolve which model version to use.      If *version* is provided, return it dir

### Community 91 - "Community 91"
Cohesion: 1.0
Nodes (1): Return the short git HEAD commit hash, or None if not in a repo.

### Community 92 - "Community 92"
Cohesion: 1.0
Nodes (1): Build a manifest metadata dict with standard fields.      ``parent_version`` and

### Community 93 - "Community 93"
Cohesion: 1.0
Nodes (1): # NOTE: no _SUCCESS file -> partial

## Knowledge Gaps
- **323 isolated node(s):** `Shared SparkSession for all tests.`, `Create minimal conf dirs with catalog and optional parameter files.`, `Create minimal conf dirs with catalog and optional parameter files.`, `Create base dataset dir with one train_variant and corresponding latest symlinks`, `Dataset pipeline computes hash-based base_dataset_version and train_variant_id.` (+318 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 28`** (6 nodes): `Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (3 nodes): `main()`, `Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run inside`, `nuke_ml_recsys.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 49`** (1 nodes): `Save data to the dataset.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50`** (1 nodes): `Check if the dataset exists.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 56`** (1 nodes): `Train the model. After calling, the adapter holds the trained model.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 57`** (1 nodes): `Return probability scores as a 1-D numpy array.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 58`** (1 nodes): `Save the model to the given filepath using the algorithm's native format.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 59`** (1 nodes): `Load a model from the given filepath into this adapter.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 60`** (1 nodes): `Return {feature_name: importance_score}.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 61`** (1 nodes): `Log the model artifact using the algorithm's MLflow integration.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 62`** (1 nodes): `Create base dataset dir with one train_variant and corresponding latest symlinks`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 63`** (1 nodes): `Dataset pipeline computes hash-based base_dataset_version and train_variant_id.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 64`** (1 nodes): `Training pipeline resolves base + train_variant via latest symlinks.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 65`** (1 nodes): `Training pipeline accepts --base-dataset-version and --train-variant.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 66`** (1 nodes): `Inference reads base/train_variant from model manifest; outputs under model hash`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 67`** (1 nodes): `When keys include prod_name, join label_table on full identity key.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 68`** (1 nodes): `When keys don't include prod_name, expand to all products.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 69`** (1 nodes): `prod_name is an identity column — encoding is deferred to training.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 70`** (1 nodes): `Hash only the train-sampling subset of dataset params.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 71`** (1 nodes): `Hash only the calibration-sampling subset of dataset params.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 72`** (1 nodes): `Compute model version ID from training params and dataset variant IDs.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 73`** (1 nodes): `Write metadata as manifest.json in the version directory.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 74`** (1 nodes): `Read and return manifest.json from a version directory.      Raises FileNotFound`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 75`** (1 nodes): `Create or update a symlink at *link* pointing to *target*.      If *link* alread`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 76`** (1 nodes): `Resolve which base dataset version to use.      If *version* is provided, return`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 77`** (1 nodes): `Resolve a train/calibration variant ID under a base dataset directory.      ``va`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 78`** (1 nodes): `Return the short git HEAD commit hash, or None if not in a repo.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 79`** (1 nodes): `Build a manifest metadata dict with standard fields.      ``parent_version`` and`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 80`** (1 nodes): `Apply preprocessor to Spark inference scoring dataset.      Returns identity + f`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 81`** (1 nodes): `Build a manifest metadata dict with standard fields.      ``parent_version`` and`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 82`** (1 nodes): `Hash non-sampling dataset params together with the canonical schema.      The re`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 83`** (1 nodes): `Hash only the train-sampling subset of dataset params.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 84`** (1 nodes): `Compute model version ID from training params and dataset variant IDs.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 85`** (1 nodes): `Write metadata as manifest.json in the version directory.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 86`** (1 nodes): `Read and return manifest.json from a version directory.      Raises FileNotFound`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 87`** (1 nodes): `Create or update a symlink at *link* pointing to *target*.      If *link* alread`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 88`** (1 nodes): `Resolve which base dataset version to use.      If *version* is provided, return`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 89`** (1 nodes): `Resolve a train/calibration variant ID under a base dataset directory.      ``va`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 90`** (1 nodes): `Resolve which model version to use.      If *version* is provided, return it dir`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 91`** (1 nodes): `Return the short git HEAD commit hash, or None if not in a repo.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 92`** (1 nodes): `Build a manifest metadata dict with standard fields.      ``parent_version`` and`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 93`** (1 nodes): `# NOTE: no _SUCCESS file -> partial`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `get_schema()` connect `Community 3` to `Community 1`, `Community 4`, `Community 5`, `Community 11`, `Community 12`, `Community 17`, `Community 19`, `Community 20`?**
  _High betweenness centrality (0.181) - this node is a cross-community bridge._
- **Why does `SQLRunner` connect `Community 0` to `Community 2`, `Community 10`?**
  _High betweenness centrality (0.128) - this node is a cross-community bridge._
- **Why does `compute_all_metrics()` connect `Community 4` to `Community 1`, `Community 3`, `Community 11`, `Community 12`, `Community 15`, `Community 19`?**
  _High betweenness centrality (0.117) - this node is a cross-community bridge._
- **Are the 88 inferred relationships involving `DataCatalog` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`DataCatalog` has 88 INFERRED edges - model-reasoned connections that need verification._
- **Are the 81 inferred relationships involving `CalibratedModelAdapter` (e.g. with `.test_fit_calibrator_isotonic()` and `.test_fit_calibrator_sigmoid()`) actually correct?**
  _`CalibratedModelAdapter` has 81 INFERRED edges - model-reasoned connections that need verification._
- **Are the 91 inferred relationships involving `ModelAdapter` (e.g. with `.test_cannot_instantiate()` and `TestModelAdapterABC`) actually correct?**
  _`ModelAdapter` has 91 INFERRED edges - model-reasoned connections that need verification._
- **Are the 84 inferred relationships involving `MemoryDataset` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`MemoryDataset` has 84 INFERRED edges - model-reasoned connections that need verification._
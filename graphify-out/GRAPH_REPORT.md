# Graph Report - recsys_tfb  (2026-05-04)

## Corpus Check
- 147 files · ~371,121 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1984 nodes · 4491 edges · 33 communities detected
- Extraction: 51% EXTRACTED · 49% INFERRED · 0% AMBIGUOUS · INFERRED: 2207 edges (avg confidence: 0.69)
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
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 53|Community 53]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]

## God Nodes (most connected - your core abstractions)
1. `CalibratedModelAdapter` - 94 edges
2. `ModelAdapter` - 94 edges
3. `DataCatalog` - 85 edges
4. `MemoryDataset` - 80 edges
5. `SQLRunner` - 69 edges
6. `TableConfig` - 67 edges
7. `Node` - 62 edges
8. `Runner` - 61 edges
9. `get_schema()` - 56 edges
10. `Pipeline` - 53 edges

## Surprising Connections (you probably didn't know these)
- `Pipeline` --uses--> `Look up a pipeline by name and return it via the module's create_pipeline().`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/pipeline.py → src/recsys_tfb/pipelines/__init__.py
- `Pipeline` --uses--> `Return all registered pipeline names.`  [INFERRED]
  .worktrees/training-cache-model-input/src/recsys_tfb/core/pipeline.py → src/recsys_tfb/pipelines/__init__.py
- `Hive table dataset with INSERT OVERWRITE PARTITION semantics.  Supports both ext` --uses--> `AbstractDataset`  [INFERRED]
  src/recsys_tfb/io/hive_table_dataset.py → .worktrees/training-cache-model-input/src/recsys_tfb/io/base.py
- `Read/write a Hive table via Spark, with dynamic-partition insert-overwrite.` --uses--> `AbstractDataset`  [INFERRED]
  src/recsys_tfb/io/hive_table_dataset.py → .worktrees/training-cache-model-input/src/recsys_tfb/io/base.py
- `Dataset for reading and writing Parquet files.      Supports pandas and PySpark` --uses--> `AbstractDataset`  [INFERRED]
  src/recsys_tfb/io/parquet_dataset.py → .worktrees/training-cache-model-input/src/recsys_tfb/io/base.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.03
Nodes (113): ABC, AbstractDataset, AbstractDataset, load(), Abstract base class for all dataset implementations., DataCatalog, MemoryDataset, In-memory dataset for intermediate pipeline results. (+105 more)

### Community 1 - "Community 1"
Cohesion: 0.02
Nodes (138): feature_importance(), get_adapter(), log_to_mlflow(), ModelAdapter, predict(), ModelAdapter ABC and adapter registry., Create and return an adapter instance for the given algorithm name., Abstract base class for model adapters.      All algorithm-specific adapters inh (+130 more)

### Community 2 - "Community 2"
Cohesion: 0.03
Nodes (80): AuditWriter, Audit logging for source ETL pipeline execution.  Writes audit records to a Hive, Write a summary audit record for the entire snap_date run., Write ETL audit records to Hive and Python structured logging., Create the audit table if it doesn't exist., Insert a single audit record into the Hive audit table., CheckResult, OutputChecker (+72 more)

### Community 3 - "Community 3"
Cohesion: 0.02
Nodes (80): _get_preprocessing_config(), Backend-agnostic helpers for preprocessing., Extract drop_columns and categorical_columns from parameters.      Returns:, Check that all required columns exist. Raises ValueError if missing., Log warning for drop_columns that don't exist in the DataFrame., _validate_columns(), _warn_missing_drop_columns(), log_step() (+72 more)

### Community 4 - "Community 4"
Cohesion: 0.02
Nodes (90): _join_token(), pandas_bucket(), ratio_to_threshold(), Deterministic CRC32-based hashing utilities for sampling.  Both PySpark's F.crc3, Convert a [0, 1] sampling ratio into an integer bucket threshold., Build a Spark Column of bucket indices in [0, HASH_BUCKETS).      Datetime/date, Build a numpy array of bucket indices in [0, HASH_BUCKETS).      Mirrors :func:`, spark_bucket() (+82 more)

### Community 5 - "Community 5"
Cohesion: 0.03
Nodes (66): build_comparison_result(), _compute_delta(), _compute_nested_delta(), plot_comparison_metrics(), plot_comparison_score_distributions(), Comparison logic for evaluating two models or model vs baseline., Create overlay histograms and side-by-side boxplots comparing two models.      R, Compute deltas (A - B) for all metrics at all levels.      Args:         result_ (+58 more)

### Community 6 - "Community 6"
Cohesion: 0.03
Nodes (73): Return all registered dataset names., generate_report(), promote_model(), 情境測試共用 fixtures 與 helpers。, 在情境工作目錄下執行 model promote。      Args:         work_dir: 工作目錄路徑。      Raises:, 讀取 pipeline 產出，產生繁體中文驗證報告。      Args:         scenario_name: 情境名稱。         work_, 為情境建立隔離的工作目錄。      Args:         scenario_name: 情境名稱（如 "scenario_1"）。         fe, 在指定工作目錄下用 subprocess 執行 pipeline CLI。      Args:         work_dir: 工作目錄路徑。 (+65 more)

### Community 7 - "Community 7"
Cohesion: 0.04
Nodes (42): _base_params(), Tests for recsys_tfb.core.versioning module (three-layer versioning)., _sample_schema(), TestBuildManifestMetadata, TestComputeBaseDatasetVersion, TestComputeCalibrationVariantId, TestComputeModelVersion, TestComputeTrainVariantId (+34 more)

### Community 8 - "Community 8"
Cohesion: 0.05
Nodes (36): save(), _format_col(), HiveTableDataset, _infer_columns_from_spark(), Hive table dataset with INSERT OVERWRITE PARTITION semantics.  Supports both ext, Read/write a Hive table via Spark, with dynamic-partition insert-overwrite., _to_spark(), ParquetDataset (+28 more)

### Community 9 - "Community 9"
Cohesion: 0.04
Nodes (53): exists(), compute_metrics(), generate_report(), prepare_eval_data(), Join ranked predictions with labels using Spark.      For external segment sourc, Generate HTML report from Spark evaluation results.      Collects the eval_predi, Compute ranking metrics using Spark SQL.      Uses window functions to compute A, find_best_version() (+45 more)

### Community 10 - "Community 10"
Cohesion: 0.06
Nodes (48): _apply(), ConfigLoader, _deep_merge(), _flatten_params(), Return the merged content of a specific parameters file.          Args:, Flatten nested dict into dotted keys, e.g. {'hive': {'db': 'x'}} → {'hive.db': ', Load and merge YAML config files from base and environment directories., Load all YAML files from a directory, keyed by stem name. (+40 more)

### Community 11 - "Community 11"
Cohesion: 0.05
Nodes (46): plot_calibration_curves(), Calibration curve visualizations., Plot calibration curves per product.      Args:         predictions: DataFrame w, plot_positive_rank_heatmap(), plot_positive_rate_rank_heatmap(), plot_rank_heatmap(), plot_score_distributions(), plot_score_distributions_by_label() (+38 more)

### Community 12 - "Community 12"
Cohesion: 0.05
Nodes (13): get_pipeline(), list_pipelines(), Look up a pipeline by name and return it via the module's create_pipeline()., Return all registered pipeline names., create_pipeline(), Baselines pipeline definition., Tests for training pipeline definition., TestBaselinesPipeline (+5 more)

### Community 13 - "Community 13"
Cohesion: 0.09
Nodes (18): format_yaml_output(), _load_pandas(), _load_spark(), main(), _print_summary(), Suggest categorical columns from a dataset.  Given a parquet file or Hive table,, Format categorical columns as a flat YAML snippet.      Example output:, Suggest categorical columns from a dataset and write a YAML snippet. (+10 more)

### Community 14 - "Community 14"
Cohesion: 0.11
Nodes (20): build_segment_metrics_table(), compute_segment_metrics(), load_and_join_segment_sources(), _plot_dimension_charts(), plot_segment_charts(), Segment-level metrics and visualizations., Plot grouped bar charts for segment-level metrics.      Args:         segment_me, Create grouped bar charts from dimension metrics. (+12 more)

### Community 15 - "Community 15"
Cohesion: 0.11
Nodes (17): ConsoleFormatter, generate_run_id(), get_current_context(), JsonFormatter, Structured logging framework for pipeline execution.  Provides RunContext for ex, Configure the root logger from config and bind the RunContext.      Args:, Generate a run ID in the format ``YYYYMMDD_HHMMSS_{6 hex chars}``., Return the current RunContext, or None if not set. (+9 more)

### Community 16 - "Community 16"
Cohesion: 0.1
Nodes (8): get_schema_for_hash(), Centralized column schema for all pipelines.  Provides get_schema() to retrieve, Return canonical schema dict intended for version hashing.      Same resolution, Validate the shape of ``parameters["schema"]``.      Enforces:     - Scalar keys, validate_schema_config(), Tests for schema config validation and source_etl consistency checks., TestGetSchemaForHash, TestValidateSchemaConfig

### Community 17 - "Community 17"
Cohesion: 0.09
Nodes (17): generate_global_popularity_baseline(), generate_segment_popularity_baseline(), Baseline generators for model comparison.  Provides global and segment popularit, Generate a segment-level popularity baseline.      Computes positive rate per (s, Generate a global popularity baseline.      Computes overall positive rate per p, compute_baseline_metrics(), compute_baselines(), Compute baseline predictions using popularity-based methods.      Reads baseline (+9 more)

### Community 18 - "Community 18"
Cohesion: 0.13
Nodes (12): _block_vdclient_magic_import(), _install_fake_vdclient_magic(), Tests for resolve_vdclient_placeholders., Install a fake vdclient_magic module with spark_ports(cluster) -> tuple., Make ``import vdclient_magic`` raise ImportError., TestResolve, TestResolveEnv, Resolve ${...} placeholders in spark config values.  Two placeholder forms are s (+4 more)

### Community 19 - "Community 19"
Cohesion: 0.19
Nodes (9): compute_product_statistics(), compute_segment_statistics(), Dataset statistics for evaluation reports., Per-segment statistics at customer granularity.      Returns DataFrame indexed b, Per-product statistics at customer granularity.      Returns DataFrame indexed b, _make_labels(), Tests for evaluation.statistics module., TestProductStatistics (+1 more)

### Community 20 - "Community 20"
Cohesion: 0.15
Nodes (9): _make_base_and_train_variant(), Training pipeline resolves base + train_variant via latest symlinks., Create minimal conf dirs with catalog and optional parameter files., Training pipeline accepts --base-dataset-version and --train-variant., Inference reads base/train_variant from model manifest; outputs under model hash, Create base dataset dir with one train_variant and corresponding latest symlinks, Dataset pipeline computes hash-based base_dataset_version and train_variant_id., _setup_conf() (+1 more)

### Community 21 - "Community 21"
Cohesion: 0.24
Nodes (6): _make_parameters(), Cross-validation: Spark metrics vs pandas metrics on the same data.  Requires a, Per-product metrics should match between backends., n_queries and n_excluded_queries should match., Spark and pandas backends should produce matching metrics., TestSparkPandasCrossValidation

### Community 22 - "Community 22"
Cohesion: 0.33
Nodes (5): generate_feature_table(), generate_label_table(), 擴展合成資料產生器，支援多 snap_dates、額外欄位、8 類產品。, 產生合成標籤資料表。      Args:         rng: numpy 隨機數產生器。         snap_dates: 快照日期清單，預設為, 產生合成特徵資料表（22 欄位）。      Args:         rng: numpy 隨機數產生器（確保可重複性）。         snap_dat

### Community 23 - "Community 23"
Cohesion: 0.33
Nodes (1): Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen

### Community 43 - "Community 43"
Cohesion: 1.0
Nodes (1): Save data to the dataset.

### Community 44 - "Community 44"
Cohesion: 1.0
Nodes (1): Check if the dataset exists.

### Community 50 - "Community 50"
Cohesion: 1.0
Nodes (1): Train the model. After calling, the adapter holds the trained model.

### Community 51 - "Community 51"
Cohesion: 1.0
Nodes (1): Return probability scores as a 1-D numpy array.

### Community 52 - "Community 52"
Cohesion: 1.0
Nodes (1): Save the model to the given filepath using the algorithm's native format.

### Community 53 - "Community 53"
Cohesion: 1.0
Nodes (1): Load a model from the given filepath into this adapter.

### Community 54 - "Community 54"
Cohesion: 1.0
Nodes (1): Return {feature_name: importance_score}.

### Community 55 - "Community 55"
Cohesion: 1.0
Nodes (1): Log the model artifact using the algorithm's MLflow integration.

### Community 56 - "Community 56"
Cohesion: 1.0
Nodes (1): # NOTE: no _SUCCESS file -> partial

## Knowledge Gaps
- **289 isolated node(s):** `Shared SparkSession for all tests.`, `Create minimal conf dirs with catalog and optional parameter files.`, `Create base dataset dir with one train_variant and corresponding latest symlinks`, `Dataset pipeline computes hash-based base_dataset_version and train_variant_id.`, `Training pipeline resolves base + train_variant via latest symlinks.` (+284 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 23`** (6 nodes): `Preprocessing module: fit/transform/apply logic shared across pipelines.  Backen`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 43`** (1 nodes): `Save data to the dataset.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 44`** (1 nodes): `Check if the dataset exists.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50`** (1 nodes): `Train the model. After calling, the adapter holds the trained model.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 51`** (1 nodes): `Return probability scores as a 1-D numpy array.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 52`** (1 nodes): `Save the model to the given filepath using the algorithm's native format.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 53`** (1 nodes): `Load a model from the given filepath into this adapter.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 54`** (1 nodes): `Return {feature_name: importance_score}.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 55`** (1 nodes): `Log the model artifact using the algorithm's MLflow integration.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 56`** (1 nodes): `# NOTE: no _SUCCESS file -> partial`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `get_schema()` connect `Community 3` to `Community 1`, `Community 4`, `Community 5`, `Community 9`, `Community 11`, `Community 16`, `Community 17`?**
  _High betweenness centrality (0.176) - this node is a cross-community bridge._
- **Why does `SQLRunner` connect `Community 0` to `Community 2`, `Community 10`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **Why does `DataCatalog` connect `Community 0` to `Community 1`, `Community 6`, `Community 8`, `Community 9`, `Community 10`, `Community 12`?**
  _High betweenness centrality (0.115) - this node is a cross-community bridge._
- **Are the 81 inferred relationships involving `CalibratedModelAdapter` (e.g. with `.test_fit_calibrator_isotonic()` and `.test_fit_calibrator_sigmoid()`) actually correct?**
  _`CalibratedModelAdapter` has 81 INFERRED edges - model-reasoned connections that need verification._
- **Are the 91 inferred relationships involving `ModelAdapter` (e.g. with `.test_cannot_instantiate()` and `TestModelAdapterABC`) actually correct?**
  _`ModelAdapter` has 91 INFERRED edges - model-reasoned connections that need verification._
- **Are the 75 inferred relationships involving `DataCatalog` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`DataCatalog` has 75 INFERRED edges - model-reasoned connections that need verification._
- **Are the 71 inferred relationships involving `MemoryDataset` (e.g. with `TestResolveCachePath` and `TestIsSparkDataframe`) actually correct?**
  _`MemoryDataset` has 71 INFERRED edges - model-reasoned connections that need verification._
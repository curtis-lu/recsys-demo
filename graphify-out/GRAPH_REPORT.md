# Graph Report - config-consistency-validation  (2026-05-17)

## Corpus Check
- 156 files · ~347,027 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2041 nodes · 4906 edges · 42 communities detected
- Extraction: 51% EXTRACTED · 49% INFERRED · 0% AMBIGUOUS · INFERRED: 2407 edges (avg confidence: 0.67)
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
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 64|Community 64]]

## God Nodes (most connected - your core abstractions)
1. `ParquetHandle` - 122 edges
2. `SQLRunner` - 85 edges
3. `ModelAdapter` - 85 edges
4. `CalibratedModelAdapter` - 84 edges
5. `DataCatalog` - 82 edges
6. `MemoryDataset` - 77 edges
7. `LightGBMAdapter` - 72 edges
8. `HiveTableDataset` - 69 edges
9. `TableConfig` - 67 edges
10. `LgbDatasetHandle` - 61 edges

## Surprising Connections (you probably didn't know these)
- `Tests for io.extract.extract_Xy.` --uses--> `ParquetHandle`  [INFERRED]
  tests/test_io/test_extract.py → src/recsys_tfb/io/handles.py
- `Six rows across three customers; c1 + c2 have positives, c3 does not.` --uses--> `ParquetHandle`  [INFERRED]
  tests/test_io/test_extract.py → src/recsys_tfb/io/handles.py
- `When the metadata probe raises (e.g. bogus path), log WARNING and     let extrac` --uses--> `ParquetHandle`  [INFERRED]
  tests/test_io/test_extract.py → src/recsys_tfb/io/handles.py
- `_pdf_to_X turns an already-loaded pdf into X numpy, applying the     same slice_` --uses--> `ParquetHandle`  [INFERRED]
  tests/test_io/test_extract.py → src/recsys_tfb/io/handles.py
- `When no categorical_columns overlap with identity_columns, the     encode_catego` --uses--> `ParquetHandle`  [INFERRED]
  tests/test_io/test_extract.py → src/recsys_tfb/io/handles.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.02
Nodes (158): ABC, feature_importance(), get_adapter(), log_to_mlflow(), ModelAdapter, predict(), prepare_train_inputs(), ModelAdapter ABC and adapter registry. (+150 more)

### Community 1 - "Community 1"
Cohesion: 0.03
Nodes (103): DataCatalog, MemoryDataset, _apply(), ConfigLoader, _deep_merge(), _flatten_params(), Flatten nested dict into dotted keys, e.g. {'hive': {'db': 'x'}} → {'hive.db': ', Load and merge YAML config files from base and environment directories. (+95 more)

### Community 2 - "Community 2"
Cohesion: 0.03
Nodes (60): AbstractDataset, AbstractDataset, exists(), load(), Abstract base class for all dataset implementations., save(), In-memory dataset for intermediate pipeline results., Release the in-memory data to free memory. (+52 more)

### Community 3 - "Community 3"
Cohesion: 0.04
Nodes (80): AuditWriter, Audit logging for source ETL pipeline execution.  Writes audit records to a Hive, Write a summary audit record for the entire snap_date run., Write ETL audit records to Hive and Python structured logging., Create the audit table if it doesn't exist., Insert a single audit record into the Hive audit table., CheckResult, OutputChecker (+72 more)

### Community 4 - "Community 4"
Cohesion: 0.03
Nodes (113): add_query_total_rel(), add_row_contributions(), aggregate_overall(), aggregate_per_item(), aggregate_per_segment(), _build_category_mapping(), collapse_to_categories(), compute_all_metrics() (+105 more)

### Community 5 - "Community 5"
Cohesion: 0.02
Nodes (72): generate_global_popularity_baseline(), generate_segment_popularity_baseline(), Baseline generators for model comparison.  Provides global and segment popularit, Generate a segment-level popularity baseline.      Computes positive rate per (s, Generate a global popularity baseline.      Computes overall positive rate per p, _join_token(), ratio_to_threshold(), Deterministic CRC32-based hashing utilities for sampling.  PySpark's F.crc32 use (+64 more)

### Community 6 - "Community 6"
Cohesion: 0.03
Nodes (66): _get_preprocessing_config(), Backend-agnostic helpers for preprocessing., Extract drop_columns and categorical_columns from parameters.      Returns:, Check that all required columns exist. Raises ValueError if missing., Log warning for drop_columns that don't exist in the DataFrame., _validate_columns(), _warn_missing_drop_columns(), log_step() (+58 more)

### Community 7 - "Community 7"
Cohesion: 0.04
Nodes (58): Return the merged content of a specific parameters file.          Args:, Per-test SparkSession resolved via get_or_create_spark_session.      Function-sc, spark(), Exception, get_pipeline(), list_pipelines(), Look up a pipeline by name and return it via the module's create_pipeline()., Return all registered pipeline names. (+50 more)

### Community 8 - "Community 8"
Cohesion: 0.04
Nodes (39): _base_params(), Tests for recsys_tfb.core.versioning module (three-layer versioning)., _sample_schema(), TestBuildManifestMetadata, TestComputeBaseDatasetVersion, TestComputeCalibrationVariantId, TestComputeFeatureTableFingerprint, TestComputeModelVersion (+31 more)

### Community 9 - "Community 9"
Cohesion: 0.08
Nodes (53): assemble_report(), build_baseline_section(), build_category_section(), build_dataset_overview_section(), build_diagnostics_section(), build_glossary_section(), build_guardrail_recall_section(), build_headline_section() (+45 more)

### Community 10 - "Community 10"
Cohesion: 0.06
Nodes (38): plot_calibration_curves(), Calibration curve visualizations., Plot calibration curves per product.      Args:         predictions: DataFrame w, plot_positive_rank_heatmap(), plot_positive_rate_rank_heatmap(), plot_rank_heatmap(), plot_score_distributions(), plot_score_distributions_by_label() (+30 more)

### Community 11 - "Community 11"
Cohesion: 0.06
Nodes (45): build_comparison_result(), _compute_delta(), _compute_nested_delta(), Comparison logic for evaluating two models or model vs baseline., Compute deltas (A - B) for all metrics at all levels.      Args:         result_, Compute metric-level delta (A - B)., Compute delta for each sub-key in a nested metrics dict., extract_Xy() (+37 more)

### Community 12 - "Community 12"
Cohesion: 0.08
Nodes (33): config_role_conflicts(), ConfigConsistencyError, ConsistencyError, DataConsistencyError, inference_products_mismatch(), item_missing_from_categorical(), override_unknown_items(), _prepare_model_input() (+25 more)

### Community 13 - "Community 13"
Cohesion: 0.06
Nodes (10): create_pipeline(), Baselines pipeline definition., post_training=True — read from training_eval_predictions., Default (post_training=False) — monitoring scenario., TestBaselinesPipeline, TestDatasetPipeline, TestEvaluationPipelineDefault, TestEvaluationPipelinePostTraining (+2 more)

### Community 14 - "Community 14"
Cohesion: 0.08
Nodes (21): ConsoleFormatter, generate_run_id(), get_current_context(), _human_bytes(), JsonFormatter, log_data_volume(), Structured logging framework for pipeline execution.  Provides RunContext for ex, Configure the root logger from config and bind the RunContext.      Args: (+13 more)

### Community 15 - "Community 15"
Cohesion: 0.09
Nodes (23): find_best_version(), get_current_best_version(), _is_version_dir(), list_versions(), main(), print_version_table(), promote(), Promote a versioned model to best/ for inference use.  Usage:     python scripts (+15 more)

### Community 16 - "Community 16"
Cohesion: 0.09
Nodes (10): get_schema_for_hash(), Centralized column schema for all pipelines.  Provides get_schema() to retrieve, # NOTE: this checks only schema.item (the single identity categorical in, Return canonical schema dict intended for version hashing.      Same resolution, Validate the shape of ``parameters["schema"]``.      Enforces:     - Scalar keys, validate_schema_config(), Tests for schema config validation and source_etl consistency checks., TestGetSchemaForHash (+2 more)

### Community 17 - "Community 17"
Cohesion: 0.12
Nodes (18): Validate inference output with sanity checks. Raises ValidationError on failure., validate_predictions(), _make_valid_data(), Tests for inference pipeline validation (Spark backend)., Re-rank by score descending within each (snap_date, cust_id) group., Build a valid ranked_predictions and matching scoring_dataset (Spark)., _rerank(), TestCompleteness (+10 more)

### Community 18 - "Community 18"
Cohesion: 0.12
Nodes (14): format_yaml_output(), _load_spark(), main(), _print_summary(), Suggest categorical columns from a dataset.  Given a Hive table or HDFS parquet, Suggest categorical columns from a dataset and write a YAML snippet., Infer categorical columns from a Spark DataFrame.      String and boolean column, Format categorical columns as a flat YAML snippet.      Example output: (+6 more)

### Community 19 - "Community 19"
Cohesion: 0.11
Nodes (14): _make_base_and_train_variant(), _mock_spark_with_feature_table_schema(), Dataset pipeline computes hash-based base_dataset_version and train_variant_id., Build a SparkSession-like mock whose ``table(fqn).schema.fields``     returns th, Training pipeline resolves base + train_variant via latest symlinks., _run_pipeline calls inject_cache_source_tables with substitution_params, Training pipeline accepts --base-dataset-version and --train-variant., Inference reads base/train_variant from model manifest; outputs under model hash (+6 more)

### Community 20 - "Community 20"
Cohesion: 0.13
Nodes (9): compute_ap(), compute_mean_ap(), Single-query ranking metrics on numpy arrays.  Scope is intentionally narrow: on, Compute Average Precision for a single query.      Returns None if there are no, Mean of per-group Average Precision.      A "group" represents one query (e.g. o, Tests for evaluation.metrics — numpy-only HPO primitives.  Scope: only ``compute, TestComputeAP, TestComputeMeanAP (+1 more)

### Community 21 - "Community 21"
Cohesion: 0.13
Nodes (8): collect_dataset_snap_dates(), Shared functions for the dataset building pipeline., Return sorted union of train/cal/val/test snap_dates as pd.Timestamps.      Sing, Validate that train/calibration/val/test snap_date sets are mutually disjoint., validate_date_splits(), Tests for backend-agnostic dataset pipeline helpers (nodes_shared)., TestCollectDatasetSnapDates, TestValidateDateSplits

### Community 22 - "Community 22"
Cohesion: 0.13
Nodes (21): ccard_prods(), _diff_msg(), exchange_prods(), _extract_cte_body(), _extract_prod_literals(), fund_prods(), inference_prods(), Lint test: prod_name 在 yaml configs 與 ETL SQL 必須保持一致。  Six places that hard-code (+13 more)

### Community 23 - "Community 23"
Cohesion: 0.16
Nodes (10): copy_hdfs_to_local(), get_hive_table_location(), HDFS↔driver-local file-copy utilities.  Pure mechanics, agnostic to caller. No k, Return the HDFS Location URI of a Hive table via DESCRIBE FORMATTED.      Args:, Copy an HDFS path (file or directory) to a driver-local path.      Uses Spark's, _make_fake_spark(), Tests for recsys_tfb.utils.hdfs., Build a MagicMock spark simulating the JVM bridge surface we use. (+2 more)

### Community 24 - "Community 24"
Cohesion: 0.19
Nodes (9): compute_product_statistics(), compute_segment_statistics(), Dataset statistics for evaluation reports., Per-product statistics at customer granularity., Per-segment statistics at customer granularity.      Returns DataFrame indexed b, _make_labels(), Tests for evaluation.statistics module., TestProductStatistics (+1 more)

### Community 25 - "Community 25"
Cohesion: 0.19
Nodes (14): _assign_customer_demographics(), _compute_label_prob(), generate_feature_table(), _generate_financial_features(), generate_label_table(), generate_sample_pool(), main(), Generate synthetic feature_table and label_table Parquet files for local dev.  P (+6 more)

### Community 26 - "Community 26"
Cohesion: 0.26
Nodes (12): join_segment_sources(), External segment-source joining for evaluation (Spark, single impl).  ``_read_se, Read one external segment source. None when the source is absent.      SEAM: onl, Left-join each external segment column onto ``labels``.      Missing sources are, _read_segment_source(), _labels(), Tests for evaluation.segments — single Spark segment-source join., test_join_single_source() (+4 more)

### Community 27 - "Community 27"
Cohesion: 0.53
Nodes (5): _load(), Regression: parameters_evaluation.yaml carries the refactor's new keys., test_k_values_is_superset(), test_product_categories_block(), test_report_display_and_sections()

### Community 28 - "Community 28"
Cohesion: 0.33
Nodes (1): Preprocessing: fit/transform/apply logic for Spark pipelines.  - ``._spark``   —

### Community 29 - "Community 29"
Cohesion: 0.5
Nodes (1): validate_config_consistency must run in _load_config_and_setup.

### Community 30 - "Community 30"
Cohesion: 0.67
Nodes (1): Bootstrap dev Hive: write synthetic parquet as ml_recsys.{feature,label,sample_p

### Community 31 - "Community 31"
Cohesion: 0.67
Nodes (1): Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run via th

### Community 50 - "Community 50"
Cohesion: 1.0
Nodes (1): Load data from the dataset.

### Community 51 - "Community 51"
Cohesion: 1.0
Nodes (1): Save data to the dataset.

### Community 52 - "Community 52"
Cohesion: 1.0
Nodes (1): Check if the dataset exists.

### Community 58 - "Community 58"
Cohesion: 1.0
Nodes (1): Encode non-identity categoricals in Spark feature_table at customer-month granul

### Community 59 - "Community 59"
Cohesion: 1.0
Nodes (1): Merge Spark keys + labels + pre-encoded features into model_input.      Equivale

### Community 60 - "Community 60"
Cohesion: 1.0
Nodes (1): Apply preprocessor to Spark inference scoring dataset.      Returns identity + f

### Community 61 - "Community 61"
Cohesion: 1.0
Nodes (1): Base for all consistency failures (subclasses ValueError by design).

### Community 62 - "Community 62"
Cohesion: 1.0
Nodes (1): Config self-contradiction detectable without data (Layer 1).

### Community 63 - "Community 63"
Cohesion: 1.0
Nodes (1): Config disagrees with the actual data (Layer 2).

### Community 64 - "Community 64"
Cohesion: 1.0
Nodes (1): Canonical sorted list of valid item values (the single source).      Reads ``sch

## Knowledge Gaps
- **270 isolated node(s):** `Per-test SparkSession resolved via get_or_create_spark_session.      Function-sc`, `Build a SparkSession-like mock whose ``table(fqn).schema.fields``     returns th`, `Create minimal conf dirs with catalog and optional parameter files.`, `Create base dataset dir with one train_variant and corresponding latest symlinks`, `Dataset pipeline computes hash-based base_dataset_version and train_variant_id.` (+265 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 28`** (6 nodes): `Preprocessing: fit/transform/apply logic for Spark pipelines.  - ``._spark``   —`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`, `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (4 nodes): `validate_config_consistency must run in _load_config_and_setup.`, `test_load_config_calls_validate_config_consistency()`, `test_validate_config_consistency_imported()`, `test_consistency_cli_wiring.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (3 nodes): `setup_hive_dev.py`, `main()`, `Bootstrap dev Hive: write synthetic parquet as ml_recsys.{feature,label,sample_p`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 31`** (3 nodes): `main()`, `Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.  Run via th`, `nuke_ml_recsys.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50`** (1 nodes): `Load data from the dataset.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 51`** (1 nodes): `Save data to the dataset.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 52`** (1 nodes): `Check if the dataset exists.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 58`** (1 nodes): `Encode non-identity categoricals in Spark feature_table at customer-month granul`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 59`** (1 nodes): `Merge Spark keys + labels + pre-encoded features into model_input.      Equivale`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 60`** (1 nodes): `Apply preprocessor to Spark inference scoring dataset.      Returns identity + f`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 61`** (1 nodes): `Base for all consistency failures (subclasses ValueError by design).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 62`** (1 nodes): `Config self-contradiction detectable without data (Layer 1).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 63`** (1 nodes): `Config disagrees with the actual data (Layer 2).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 64`** (1 nodes): `Canonical sorted list of valid item values (the single source).      Reads ``sch`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DataCatalog` connect `Community 1` to `Community 2`, `Community 11`, `Community 13`, `Community 7`?**
  _High betweenness centrality (0.120) - this node is a cross-community bridge._
- **Why does `get_schema()` connect `Community 5` to `Community 0`, `Community 4`, `Community 6`, `Community 10`, `Community 11`, `Community 12`, `Community 16`, `Community 17`?**
  _High betweenness centrality (0.114) - this node is a cross-community bridge._
- **Why does `SQLRunner` connect `Community 1` to `Community 3`, `Community 6`, `Community 7`?**
  _High betweenness centrality (0.094) - this node is a cross-community bridge._
- **Are the 119 inferred relationships involving `ParquetHandle` (e.g. with `TestModelAdapterABC` and `TestLightGBMAdapter`) actually correct?**
  _`ParquetHandle` has 119 INFERRED edges - model-reasoned connections that need verification._
- **Are the 74 inferred relationships involving `SQLRunner` (e.g. with `TestValidateOrder` and `TestDryRun`) actually correct?**
  _`SQLRunner` has 74 INFERRED edges - model-reasoned connections that need verification._
- **Are the 82 inferred relationships involving `ModelAdapter` (e.g. with `TestModelAdapterABC` and `TestLightGBMAdapter`) actually correct?**
  _`ModelAdapter` has 82 INFERRED edges - model-reasoned connections that need verification._
- **Are the 70 inferred relationships involving `CalibratedModelAdapter` (e.g. with `TestCalibratedModelAdapter` and `Tests for CalibratedModelAdapter.`) actually correct?**
  _`CalibratedModelAdapter` has 70 INFERRED edges - model-reasoned connections that need verification._
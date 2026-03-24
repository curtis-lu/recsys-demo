### Requirement: Inference pipeline definition
The inference module SHALL expose a `create_pipeline(backend: str = "pandas") -> Pipeline` function that accepts a `backend` parameter and returns a Pipeline with 5 nodes wired in sequence: build_scoring_dataset -> apply_preprocessor -> predict_scores -> rank_predictions -> validate_predictions. Node functions SHALL be imported from `nodes_pandas.py` or `nodes_spark.py` based on the backend parameter.

#### Scenario: Pipeline node count and order
- **WHEN** `create_pipeline()` is called with any backend value
- **THEN** the returned Pipeline SHALL contain exactly 5 nodes in the correct dependency order, identical regardless of backend

#### Scenario: Pipeline inputs from catalog
- **WHEN** the inference pipeline executes
- **THEN** it SHALL read feature_table, preprocessor, and model from the DataCatalog

### Requirement: Catalog inputs
Inference pipeline SHALL 從 dataset 版本目錄讀取 `preprocessor`（路徑為 `data/dataset/${dataset_version}/preprocessor.pkl`），從 model 目錄讀取 `model`（路徑為 `data/models/${model_version}/model.pkl`）。

#### Scenario: preprocessor 從 dataset 版本目錄讀取
- **WHEN** inference pipeline 執行 apply_preprocessor node
- **THEN** preprocessor SHALL 從 `data/dataset/{dataset_version}/preprocessor.pkl` 載入

#### Scenario: model 從 model 版本目錄讀取
- **WHEN** inference pipeline 執行 predict_scores node
- **THEN** model SHALL 從 `data/models/{model_version}/model.pkl` 載入

### Requirement: Final output
Inference pipeline 的 `ranked_predictions` 和 `scoring_dataset` SHALL 寫入版本化路徑。

#### Scenario: ranked_predictions 寫入版本化路徑
- **WHEN** inference pipeline 完成
- **THEN** ranked_predictions SHALL 寫入 `data/inference/{model_version}/{snap_date}/ranked_predictions.parquet`

#### Scenario: scoring_dataset 寫入版本化路徑
- **WHEN** inference pipeline 完成
- **THEN** scoring_dataset SHALL 寫入 `data/inference/{model_version}/{snap_date}/scoring_dataset.parquet`

#### Scenario: Pipeline final output
- **WHEN** the inference pipeline completes
- **THEN** ranked_predictions SHALL be saved to the DataCatalog

#### Scenario: Backend parameter selects node source
- **WHEN** `create_pipeline(backend="spark")` is called
- **THEN** all node functions SHALL be imported from `nodes_spark.py`

### Requirement: End-to-end inference execution
The inference pipeline SHALL be executable via `python -m recsys_tfb -p inference -e local` after the training pipeline has produced model and preprocessor artifacts.

#### Scenario: Successful batch scoring
- **WHEN** model.pkl and preprocessor.pkl exist from a prior training run, and feature_table.parquet contains data for the configured snap_dates
- **THEN** the pipeline SHALL produce a ranked_predictions Parquet file with all customers scored across all configured products

#### Scenario: Missing model artifact
- **WHEN** model.pkl does not exist and inference pipeline is executed
- **THEN** the pipeline SHALL fail with a clear error indicating the model is not found

### Requirement: Inference pipeline includes validation node
The inference pipeline SHALL include a `validate_predictions` node as the final step, after `rank_predictions`.

#### Scenario: Pipeline node count
- **WHEN** creating the inference pipeline
- **THEN** the pipeline contains 5 nodes: build_scoring_dataset, apply_preprocessor, predict_scores, rank_predictions, validate_predictions

#### Scenario: Validation node inputs and outputs
- **WHEN** the validate_predictions node is defined
- **THEN** it takes inputs ["ranked_predictions", "scoring_dataset", "parameters"] and outputs "validated_predictions"

#### Scenario: Both backends include validation
- **WHEN** creating the pipeline with backend "pandas" or "spark"
- **THEN** both backends import and register the validate_predictions function

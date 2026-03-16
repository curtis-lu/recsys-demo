## MODIFIED Requirements

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

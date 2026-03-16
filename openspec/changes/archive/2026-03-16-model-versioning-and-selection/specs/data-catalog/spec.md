## MODIFIED Requirements

### Requirement: Training pipeline catalog entries
DataCatalog SHALL 包含以下 training pipeline 輸出的定義：
- `best_params`：JSONDataset，儲存 Optuna 搜索的最佳超參數
- `evaluation_results`：JSONDataset，儲存 mAP 等評估指標
- `model`：PickleDataset，儲存訓練完成的 LightGBM Booster
- `preprocessor`：PickleDataset，儲存資料前處理器
- `category_mappings`：JSONDataset，儲存類別對應表

所有 training artifacts 的路徑 SHALL 使用 `${model_version}` 模板變數（如 `data/models/${model_version}/model.pkl`），由 ConfigLoader 的 `runtime_params` 在執行時解析為實際版本目錄。

#### Scenario: best_params 持久化
- **WHEN** tune_hyperparameters 節點執行完畢
- **THEN** best_params SHALL 以 JSON 格式存入 `data/models/${model_version}/best_params.json`

#### Scenario: evaluation_results 持久化
- **WHEN** evaluate_model 節點執行完畢
- **THEN** evaluation_results SHALL 以 JSON 格式存入 `data/models/${model_version}/evaluation_results.json`

#### Scenario: model 持久化
- **WHEN** train_model 節點執行完畢
- **THEN** model SHALL 以 pickle 格式存入 `data/models/${model_version}/model.pkl`

#### Scenario: 各環境路徑可設定
- **WHEN** 使用 production 環境設定
- **THEN** 所有 training artifact 的路徑 SHALL 切換至 HDFS 路徑（如 `hdfs:///data/recsys/models/${model_version}/model.pkl`）

### Requirement: Inference pipeline catalog entries
DataCatalog SHALL include entries for inference pipeline datasets:
- `scoring_dataset`: ParquetDataset for the intermediate scoring dataset
- `ranked_predictions`: ParquetDataset for the final ranked output

#### Scenario: scoring_dataset persistence
- **WHEN** build_scoring_dataset node completes
- **THEN** scoring_dataset SHALL be saved to data/inference/scoring_dataset.parquet via ParquetDataset

#### Scenario: ranked_predictions persistence
- **WHEN** rank_predictions node completes
- **THEN** ranked_predictions SHALL be saved to data/inference/ranked_predictions.parquet via ParquetDataset

#### Scenario: Inference reads training artifacts from best
- **WHEN** inference pipeline loads model and preprocessor
- **THEN** `${model_version}` SHALL 被解析為 `"best"`，從 `data/models/best/` 目錄讀取

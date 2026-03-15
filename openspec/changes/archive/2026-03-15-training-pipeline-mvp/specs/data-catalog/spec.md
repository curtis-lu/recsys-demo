## ADDED Requirements

### Requirement: Training pipeline catalog entries
DataCatalog SHALL 包含以下 training pipeline 輸出的定義：
- `best_params`：JSONDataset，儲存 Optuna 搜索的最佳超參數
- `evaluation_results`：JSONDataset，儲存 mAP 等評估指標
- `model`：PickleDataset，儲存訓練完成的 LightGBM Booster（已存在）

#### Scenario: best_params 持久化
- **WHEN** tune_hyperparameters 節點執行完畢
- **THEN** best_params SHALL 以 JSON 格式存入 data/models/best_params.json

#### Scenario: evaluation_results 持久化
- **WHEN** evaluate_model 節點執行完畢
- **THEN** evaluation_results SHALL 以 JSON 格式存入 data/models/evaluation_results.json

#### Scenario: 各環境路徑可設定
- **WHEN** 使用 production 環境設定
- **THEN** 所有 training artifact 的路徑 SHALL 切換至 HDFS 路徑

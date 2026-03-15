## ADDED Requirements

### Requirement: MLflow 本地追蹤
系統 SHALL 預設使用本地 file store（`mlruns/` 目錄）作為 MLflow tracking backend，無需網路連線。

#### Scenario: 離線環境記錄
- **WHEN** 在無網路環境執行 log_experiment
- **THEN** 實驗資料 SHALL 成功寫入本地 mlruns/ 目錄

#### Scenario: 可切換 tracking URI
- **WHEN** parameters 設定 mlflow.tracking_uri 為遠端 server URL
- **THEN** 記錄 SHALL 寫入指定遠端 server

### Requirement: 實驗結果可查詢
MLflow 記錄的每次訓練 run SHALL 包含足夠資訊以比較不同實驗：超參數、mAP 指標、模型 artifact。

#### Scenario: 比較多次實驗
- **WHEN** 執行兩次 training pipeline（不同超參數設定）
- **THEN** MLflow UI 中 SHALL 可查看並比較兩次 run 的 params 與 metrics

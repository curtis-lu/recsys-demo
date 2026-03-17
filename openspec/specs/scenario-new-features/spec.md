### Requirement: 驗證新增特徵欄位全 pipeline 通過
情境 3 測試 SHALL 使用含額外欄位（`txn_count_l1m`, `avg_txn_amt_l1m`）的 feature_table，執行 dataset → training → promote → inference 全流程。

#### Scenario: X_train 包含新欄位
- **WHEN** dataset pipeline 以含新欄位的 feature_table 完成
- **THEN** `X_train` 的欄位包含 `txn_count_l1m` 和 `avg_txn_amt_l1m`

#### Scenario: preprocessor 記錄新欄位
- **WHEN** dataset pipeline 完成
- **THEN** `preprocessor["feature_columns"]` 包含 `txn_count_l1m` 和 `avg_txn_amt_l1m`

#### Scenario: 模型使用新欄位訓練
- **WHEN** training pipeline 完成
- **THEN** 模型的 feature 數量 = 原 6 numeric + 2 new numeric + prod_name encoded = 9 個特徵

#### Scenario: inference 使用新欄位評分
- **WHEN** inference pipeline 完成
- **THEN** `scoring_dataset` 包含 `txn_count_l1m` 和 `avg_txn_amt_l1m` 欄位，`ranked_predictions` 行數正確

#### Scenario: 無需修改設定
- **WHEN** 新欄位不在 `drop_columns` 清單中
- **THEN** 無需修改 `parameters_dataset.yaml`，新欄位自動被包含在模型特徵中

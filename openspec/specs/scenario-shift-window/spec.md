### Requirement: 驗證訓練視窗前移後 data split 正確性
情境 2 測試 SHALL 使用基礎 6 個月資料，將 `train_dev_snap_dates` 改為 `["2024-03-31"]`、`val_snap_dates` 改為 `["2024-04-30"]`，執行 dataset + training pipeline。

#### Scenario: train_set 不含 train_dev 和 val 的 snap_dates
- **WHEN** dataset pipeline 以新設定完成
- **THEN** `train_set` 的 snap_dates 不包含 2024-03-31 和 2024-04-30

#### Scenario: train_dev_set 使用正確 snap_dates
- **WHEN** dataset pipeline 完成
- **THEN** `train_dev_set` 的 snap_dates 恰為 [2024-03-31]

#### Scenario: val_set 使用正確 snap_dates
- **WHEN** dataset pipeline 完成
- **THEN** `val_set` 的 snap_dates 恰為 [2024-04-30]

#### Scenario: 各 split 行數大於零
- **WHEN** dataset pipeline 完成
- **THEN** `train_set`、`train_dev_set`、`val_set` 行數均 > 0

#### Scenario: 模型訓練完成
- **WHEN** training pipeline 完成
- **THEN** `data/models/{model_version}/model.txt` 存在

#### Scenario: dataset_version 與基礎設定不同
- **WHEN** 以修改後的 `parameters_dataset.yaml` 計算 dataset_version
- **THEN** 產出的 dataset_version hash 與未修改設定時不同（因參數內容變了）

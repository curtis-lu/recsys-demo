## ADDED Requirements

### Requirement: 驗證推論新 snap_date 的正確性
情境 1 測試 SHALL 使用基礎 6 個月資料（訓練用前 3 個月），並以 `snap_dates: ["2024-04-30"]` 執行 inference pipeline。

#### Scenario: 推論結果 snap_date 為新日期
- **WHEN** 以 `inference.snap_dates=["2024-04-30"]` 執行 inference pipeline
- **THEN** `ranked_predictions` 的 `snap_date` 全部為 2024-04-30

#### Scenario: 每位客戶有完整產品排名
- **WHEN** inference pipeline 完成
- **THEN** 每位客戶恰好有 5 個產品排名，排名為 1~5 連續整數

#### Scenario: 客戶數符合預期
- **WHEN** inference pipeline 完成
- **THEN** `ranked_predictions` 的唯一客戶數等於 feature_table 在 2024-04-30 的唯一客戶數（200）

#### Scenario: 輸出路徑使用實際 model hash
- **WHEN** inference pipeline 完成
- **THEN** 推論產出位於 `data/inference/{actual_model_hash}/{snap_date}/` 而非 `data/inference/best/`

### Requirement: 完整 pipeline 端對端執行
情境 1 SHALL 依序執行 dataset → training → promote → inference，確認全鏈路通暢。

#### Scenario: 全 pipeline 執行不報錯
- **WHEN** 依序執行 dataset、training、promote、inference
- **THEN** 每個步驟 subprocess 回傳碼為 0，且各步驟產出檔案存在

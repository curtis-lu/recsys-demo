## ADDED Requirements

### Requirement: 驗證新增產品後 category_mappings 和推論的正確性
情境 4 測試 SHALL 使用含 7 個產品（原 5 + ploan, mloan）的 label_table，inference 設定的 products 清單同步更新為 7 個，執行 dataset → training → promote → inference 全流程。

#### Scenario: category_mappings 包含新產品
- **WHEN** dataset pipeline 以含 7 產品的 label_table 完成
- **THEN** `category_mappings["prod_name"]` 包含 `ploan` 和 `mloan`，共 7 個產品

#### Scenario: train_set 包含新產品
- **WHEN** dataset pipeline 完成
- **THEN** `train_set` 的 `prod_name` 唯一值為 7 個

#### Scenario: 每位客戶推論 7 個產品
- **WHEN** inference pipeline 以 `products: [bond, fx, mix, mloan, ploan, stock, usd]` 完成
- **THEN** 每位客戶在 `ranked_predictions` 中恰好有 7 個產品排名

#### Scenario: 排名完整連續
- **WHEN** inference pipeline 完成
- **THEN** 每位客戶的排名為 1~7 連續整數

#### Scenario: prod_code 唯一值正確
- **WHEN** inference pipeline 完成
- **THEN** `ranked_predictions` 的 `prod_code` 唯一值恰為 7 個

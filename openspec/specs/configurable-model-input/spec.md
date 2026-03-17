## Requirements

### Requirement: prepare_model_input 欄位設定可透過 YAML 配置
`prepare_model_input()` 函數 SHALL 從 `parameters` dict 讀取 `drop_columns` 和 `categorical_columns` 設定。若 YAML 未提供，SHALL 使用程式碼內的預設值（向後相容）。

#### Scenario: YAML 有提供欄位設定
- **WHEN** `parameters_dataset.yaml` 包含 `dataset.prepare_model_input.drop_columns` 和 `dataset.prepare_model_input.categorical_columns`
- **THEN** `prepare_model_input()` SHALL 使用 YAML 中指定的欄位清單

#### Scenario: YAML 未提供欄位設定（向後相容）
- **WHEN** `parameters_dataset.yaml` 沒有 `dataset.prepare_model_input` 區塊
- **THEN** `prepare_model_input()` SHALL 使用預設值：`drop_columns=["snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"]`、`categorical_columns=["prod_name"]`

#### Scenario: preprocessor 記錄實際使用的設定
- **WHEN** `prepare_model_input()` 執行完成
- **THEN** 回傳的 `preprocessor` dict SHALL 包含 `drop_columns` 和 `categorical_columns` 鍵，記錄實際使用的值（不論來自 YAML 或預設值）

#### Scenario: pandas 和 spark 後端行為一致
- **WHEN** 分別使用 pandas 和 spark 後端執行 `prepare_model_input()`，輸入資料相同
- **THEN** 兩者 SHALL 產出相同的 `preprocessor` dict 和相同的 feature columns

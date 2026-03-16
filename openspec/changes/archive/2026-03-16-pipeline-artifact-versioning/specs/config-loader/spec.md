## MODIFIED Requirements

### Requirement: Runtime parameter substitution
`get_catalog_config(runtime_params={...})` SHALL 支援多個 template variables 的替換，包括 `${model_version}`、`${dataset_version}`、`${snap_date}`。

#### Scenario: 多個 template variables 同時替換
- **WHEN** runtime_params 包含 `{"model_version": "best", "dataset_version": "a1b2c3d4", "snap_date": "20240331"}`
- **THEN** catalog 中所有 filepath 的 `${model_version}`、`${dataset_version}`、`${snap_date}` SHALL 被替換為對應值

#### Scenario: 未提供的 template variable 保留原樣
- **WHEN** runtime_params 只包含 `{"dataset_version": "a1b2c3d4"}` 但 filepath 中有 `${model_version}`
- **THEN** `${model_version}` SHALL 保留為原始字串不替換

## ADDED Requirements

### Requirement: 取得特定參數檔內容
ConfigLoader SHALL 提供 `get_parameters_by_name(name: str) -> dict` 方法，回傳指定 parameters 檔的合併後內容（base + env overlay）。

#### Scenario: 取得 dataset 參數
- **WHEN** 呼叫 `get_parameters_by_name("parameters_dataset")`
- **THEN** SHALL 回傳 parameters_dataset.yaml 的合併後完整內容（用於 hash 計算）

#### Scenario: 取得 training 參數
- **WHEN** 呼叫 `get_parameters_by_name("parameters_training")`
- **THEN** SHALL 回傳 parameters_training.yaml 的合併後完整內容

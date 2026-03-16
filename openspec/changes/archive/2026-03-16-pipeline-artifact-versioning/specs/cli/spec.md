## MODIFIED Requirements

### Requirement: Run command
`python -m recsys_tfb run --pipeline <name>` SHALL 根據 pipeline 類型計算版本 ID 並注入 runtime_params。

#### Scenario: 執行 dataset pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p dataset`
- **THEN** 系統 SHALL 計算 dataset_version hash，以 `runtime_params={"dataset_version": hash}` 注入 catalog，pipeline 完成後寫入 manifest 並更新 latest symlink

#### Scenario: 執行 training pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p training`
- **THEN** 系統 SHALL 解析 dataset_version（預設 latest），計算 model_version hash，以 `runtime_params={"model_version": hash, "dataset_version": dataset_version}` 注入 catalog，pipeline 完成後寫入 manifest

#### Scenario: 執行 inference pipeline
- **WHEN** 執行 `python -m recsys_tfb run -p inference`
- **THEN** 系統 SHALL 以 model_version="best" 讀取 model manifest 解析 dataset_version，以 `runtime_params={"model_version": "best", "dataset_version": resolved, "snap_date": date}` 注入 catalog，pipeline 完成後寫入 manifest

## ADDED Requirements

### Requirement: Dataset version CLI 選項
CLI SHALL 支援 `--dataset-version` 選項，允許手動指定要使用的 dataset 版本。

#### Scenario: 指定 dataset 版本執行 training
- **WHEN** 執行 `python -m recsys_tfb run -p training --dataset-version a1b2c3d4`
- **THEN** 系統 SHALL 使用 `a1b2c3d4` 作為 dataset_version 而非 latest

#### Scenario: 未指定時使用 latest
- **WHEN** 執行 `python -m recsys_tfb run -p training`（不帶 --dataset-version）
- **THEN** 系統 SHALL 解析 `data/dataset/latest` symlink 取得 dataset_version

#### Scenario: 指定的版本不存在
- **WHEN** 執行 `python -m recsys_tfb run -p training --dataset-version nonexistent`
- **THEN** 系統 SHALL 輸出錯誤訊息指出版本目錄不存在，以非零 exit code 結束

### Requirement: 版本 log 輸出
CLI SHALL 在每個 pipeline 啟動時 log 輸出所有相關的版本 ID。

#### Scenario: Dataset pipeline 版本 log
- **WHEN** dataset pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Dataset version: {dataset_version}`

#### Scenario: Training pipeline 版本 log
- **WHEN** training pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Model version: {model_version}` 和 `Dataset version: {dataset_version}`

#### Scenario: Inference pipeline 版本 log
- **WHEN** inference pipeline 啟動
- **THEN** 系統 SHALL log 輸出 `Model version: best`、`Dataset version: {dataset_version}`

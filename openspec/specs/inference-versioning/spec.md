## Requirements

### Requirement: Inference 產出版本化目錄
Inference pipeline 的產出 SHALL 儲存在 `data/inference/${model_version}/${snap_date}/` 版本目錄中，其中 `${model_version}` 為實際 model hash（非 `"best"` 字串）。

#### Scenario: 執行 inference 建立版本目錄
- **WHEN** 執行 `python -m recsys_tfb run -p inference`，best symlink 指向 model hash `a1b2c3d4`，parameters_inference.yaml 中 snap_dates 包含 "2024-03-31"
- **THEN** 產出 SHALL 寫入 `data/inference/a1b2c3d4/20240331/`（使用實際 hash，snap_date 去除 "-"）

#### Scenario: 不同 snap_date 各自獨立
- **WHEN** 以不同 snap_dates 執行 inference pipeline
- **THEN** 每個 snap_date SHALL 有獨立的子目錄，不互相覆蓋

#### Scenario: 同一 model_version 和 snap_date 重跑
- **WHEN** 以相同 model_version 和 snap_date 重跑 inference
- **THEN** SHALL 覆蓋同一目錄的產出

### Requirement: Inference 自動解析 dataset_version
Inference pipeline 執行時 SHALL 自動從 model manifest 讀取對應的 `dataset_version`，用於解析 preprocessor 路徑。

#### Scenario: 從 best model manifest 解析 dataset_version
- **WHEN** 執行 inference pipeline 且 model_version 為 "best"
- **THEN** 系統 SHALL 讀取 `data/models/best/manifest.json` 中的 `dataset_version`，將 preprocessor 路徑解析為 `data/dataset/{dataset_version}/preprocessor.pkl`

#### Scenario: model manifest 不存在時 fallback
- **WHEN** 執行 inference 但 model manifest 不存在（如舊版 model）
- **THEN** 系統 SHALL 使用 dataset 的 latest 版本作為 fallback，並輸出 warning log

### Requirement: Inference manifest 自動寫入
Inference pipeline 成功完成後 SHALL 在每個 snap_date 子目錄中寫入 manifest.json。

#### Scenario: manifest 包含實際 model hash
- **WHEN** inference pipeline 成功完成，best symlink 指向 model hash `a1b2c3d4`
- **THEN** manifest.json 的 `version` 和 `model_version` 欄位 SHALL 為 `"a1b2c3d4"`（實際 hash），非 `"best"`

### Requirement: Inference output latest symlink
Inference pipeline 成功完成後 SHALL 在 `data/inference/` 下維護 `latest` symlink。

#### Scenario: 更新 latest symlink
- **WHEN** inference pipeline 成功完成，output 寫入 `data/inference/a1b2c3d4/20240331/`
- **THEN** `data/inference/latest` symlink SHALL 指向 `data/inference/a1b2c3d4/20240331/`


## MODIFIED Requirements

### Requirement: Inference manifest includes run_id
The inference pipeline manifest.json SHALL include a `run_id` field.

#### Scenario: run_id in inference manifest
- **WHEN** the inference pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id

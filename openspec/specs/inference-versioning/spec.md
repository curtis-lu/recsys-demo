## Requirements

### Requirement: Inference 產出版本化目錄
Inference pipeline 的產出 SHALL 儲存在 `data/inference/${model_version}/${snap_date}/` 版本目錄中。

#### Scenario: 執行 inference 建立版本目錄
- **WHEN** 執行 `python -m recsys_tfb run -p inference`，model_version 為 "best"，parameters_inference.yaml 中 snap_dates 包含 "2024-03-31"
- **THEN** 產出 SHALL 寫入 `data/inference/best/20240331/`（snap_date 去除 "-"）

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

#### Scenario: manifest 包含完整追溯資訊
- **WHEN** inference pipeline 成功完成
- **THEN** manifest.json SHALL 包含 version、pipeline="inference"、created_at、model_version、dataset_version、parameters（parameters_inference.yaml 完整內容）

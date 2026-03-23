## Requirements

### Requirement: Catalog 模板變數解析版本化路徑
系統 SHALL 在 catalog.yaml 中使用 `${model_version}` 模板變數定義 model artifacts 路徑（如 `data/models/${model_version}/model.pkl`）。CLI 執行時透過 ConfigLoader 的 `runtime_params` 機制解析此變數：training 時替換為參數 hash（8 字元 hex），其他 pipeline 替換為 `"best"`。

#### Scenario: 訓練完成後建立版本目錄
- **WHEN** 執行 `python -m recsys_tfb run -p training`
- **THEN** 系統 SHALL 將 `${model_version}` 解析為 hash（由 parameters_training.yaml 內容 + dataset_version 計算），在 `data/models/{hash}/` 下建立 artifacts（model.pkl、best_params.json、evaluation_results.json）

#### Scenario: Training 不寫入 best 目錄
- **WHEN** training pipeline 執行完成
- **THEN** `data/models/best` symlink 或目錄 SHALL 不被 training 寫入或修改

#### Scenario: 相同參數和 dataset 產生相同版本 ID
- **WHEN** 以相同 parameters_training.yaml 和相同 dataset_version 執行 training 兩次
- **THEN** 兩次 SHALL 寫入同一版本目錄（hash 相同），第二次覆蓋第一次

#### Scenario: 不同 dataset 產生不同版本 ID
- **WHEN** 以相同 parameters_training.yaml 但不同 dataset_version 執行 training
- **THEN** SHALL 建立不同的版本目錄

#### Scenario: Inference 解析為 best
- **WHEN** 執行 `python -m recsys_tfb run -p inference`
- **THEN** `${model_version}` SHALL 被解析為 `"best"`，catalog 路徑為 `data/models/best/`

#### Scenario: 版本 ID 記錄在 log 中
- **WHEN** training pipeline 啟動
- **THEN** 系統 SHALL 以 log 輸出本次訓練的版本 ID（hash）

### Requirement: 跨版本 mAP 比較報告
系統 SHALL 在訓練完成後，掃描所有版本目錄並輸出 mAP 比較報告。

#### Scenario: 比較多個版本的 mAP
- **WHEN** `data/models/` 下有多個版本目錄
- **THEN** 系統 SHALL 以 log 輸出所有版本的 overall_map，按 mAP 降序排列，並標示推薦版本

#### Scenario: 顯示當前 best 版本
- **WHEN** `data/models/best` symlink 存在且指向包含 evaluation_results.json 的目錄
- **THEN** 比較報告 SHALL 標示當前 best 版本的 mAP 供對照

#### Scenario: 單一版本時正常運作
- **WHEN** 只有一個版本目錄
- **THEN** 系統 SHALL 正常輸出該版本的 mAP，不報錯

#### Scenario: 同時支援新舊版本格式
- **WHEN** `data/models/` 下同時有 hash 格式目錄和舊的 `YYYYMMDD_HHMMSS` 格式目錄
- **THEN** 比較報告 SHALL 掃描兩種格式的目錄，忽略 `best` symlink 和其他非版本目錄

### Requirement: 比較報告回傳結構化結果
`compare_model_versions` 函式 SHALL 回傳一個 dict，包含 versions（所有版本的 mAP 清單）、recommended_version（mAP 最高的版本 ID）、current_best_version（當前 best 中的版本 ID，若無則為 None）。

#### Scenario: 回傳值結構
- **WHEN** compare_model_versions 執行完成
- **THEN** 回傳 dict SHALL 包含 `versions`（list of dict with version_id 和 overall_map）、`recommended_version`（str）、`current_best_version`（str or None）

### Requirement: Training manifest 自動寫入
Training pipeline 成功完成後 SHALL 在版本目錄中寫入 manifest.json。

#### Scenario: manifest 包含完整追溯資訊
- **WHEN** training pipeline 成功完成
- **THEN** manifest.json SHALL 包含 version、pipeline="training"、created_at、git_commit、dataset_version、parameters（parameters_training.yaml 完整內容）、artifacts（產出檔案清單）

#### Scenario: pipeline 失敗不寫入 manifest
- **WHEN** training pipeline 執行失敗
- **THEN** SHALL 不寫入 manifest.json


## MODIFIED Requirements

### Requirement: Model manifest includes run_id
The training pipeline manifest.json SHALL include a `run_id` field.

#### Scenario: run_id in model manifest
- **WHEN** the training pipeline completes and writes manifest.json
- **THEN** the manifest SHALL include `"run_id": "<run_id>"` matching the current execution's run_id

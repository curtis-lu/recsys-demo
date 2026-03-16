## ADDED Requirements

### Requirement: Catalog 模板變數解析版本化路徑
系統 SHALL 在 catalog.yaml 中使用 `${model_version}` 模板變數定義 model artifacts 路徑（如 `data/models/${model_version}/model.pkl`）。CLI 執行時透過 ConfigLoader 的 `runtime_params` 機制解析此變數：training 時替換為時間戳（`YYYYMMDD_HHMMSS`），其他 pipeline 替換為 `"best"`。

#### Scenario: 訓練完成後建立版本目錄
- **WHEN** 執行 `python -m recsys_tfb run -p training`
- **THEN** 系統 SHALL 將 `${model_version}` 解析為訓練時間戳，在 `data/models/{timestamp}/` 下建立所有 5 個 artifacts（model.pkl、preprocessor.pkl、best_params.json、evaluation_results.json、category_mappings.json）

#### Scenario: Training 不寫入 best 目錄
- **WHEN** training pipeline 執行完成
- **THEN** `data/models/best/` 目錄 SHALL 不被 training 寫入或修改

#### Scenario: 多次訓練不覆蓋歷史版本
- **WHEN** training pipeline 執行兩次（不同時間）
- **THEN** 兩個版本目錄 SHALL 同時存在，互不覆蓋

#### Scenario: Inference 解析為 best
- **WHEN** 執行 `python -m recsys_tfb run -p inference`
- **THEN** `${model_version}` SHALL 被解析為 `"best"`，catalog 路徑為 `data/models/best/`

#### Scenario: 版本 ID 記錄在 log 中
- **WHEN** training pipeline 啟動
- **THEN** 系統 SHALL 以 log 輸出本次訓練的版本 ID（時間戳）

### Requirement: 跨版本 mAP 比較報告
系統 SHALL 在訓練完成後，掃描所有版本目錄並輸出 mAP 比較報告。

#### Scenario: 比較多個版本的 mAP
- **WHEN** `data/models/` 下有多個版本目錄
- **THEN** 系統 SHALL 以 log 輸出所有版本的 overall_map，按 mAP 降序排列，並標示推薦版本

#### Scenario: 顯示當前 best 版本
- **WHEN** `data/models/best/` 目錄存在且包含 evaluation_results.json
- **THEN** 比較報告 SHALL 標示當前 best 版本的 mAP 供對照

#### Scenario: 單一版本時正常運作
- **WHEN** 只有一個版本目錄
- **THEN** 系統 SHALL 正常輸出該版本的 mAP，不報錯

#### Scenario: 忽略非版本目錄
- **WHEN** `data/models/` 下有 `best/` 或其他非時間戳命名的子目錄
- **THEN** 比較報告 SHALL 忽略這些目錄，只處理符合 `YYYYMMDD_HHMMSS` 格式的版本目錄

### Requirement: 比較報告回傳結構化結果
`compare_model_versions` 函式 SHALL 回傳一個 dict，包含 versions（所有版本的 mAP 清單）、recommended_version（mAP 最高的版本 ID）、current_best_version（當前 best/ 中的版本 ID，若無則為 None）。

#### Scenario: 回傳值結構
- **WHEN** compare_model_versions 執行完成
- **THEN** 回傳 dict SHALL 包含 `versions`（list of dict with version_id 和 overall_map）、`recommended_version`（str）、`current_best_version`（str or None）

## Context

Training pipeline 目前將 model、preprocessor、best_params、evaluation_results、category_mappings 存入固定路徑（`data/models/*.pkl/json`），每次訓練覆蓋。MLflow 有記錄實驗 metrics 但模型不從 MLflow 載入。Inference pipeline 直接從 catalog 載入固定路徑的 model 和 preprocessor。

需要在不改動 inference pipeline 程式碼的前提下，支援多版本模型並存與人工把關的模型部署流程。

## Goals / Non-Goals

**Goals:**
- 每次訓練自動將所有 artifacts 存入帶時間戳的版本目錄
- 訓練完成後產出跨版本 mAP 比較報告
- 提供 CLI script 讓使用者人工確認後 promote 指定版本到 `best/`
- Inference pipeline 零改動，從 `best/` 讀取
- Training 不自動寫入 `best/`，確保人工把關流程

**Non-Goals:**
- 不建立 MLflow Model Registry 整合
- 不做自動部署（auto-promote）
- 不支援版本清理（retention policy）
- 不修改 DataCatalog 核心類別

## Decisions

### 1. 版本化方式：時間戳目錄

每次訓練在 `data/models/{YYYYMMDD_HHMMSS}/` 下存入所有 5 個 artifacts。

**替代方案**：MLflow run_id → 需依賴 MLflow API 來載入，增加複雜度且 production 環境可能沒有 MLflow 服務。

**替代方案**：遞增編號 → 需要讀取現有最大編號，在並行訓練時可能衝突。

### 2. Training 與 Inference 分離 catalog 路徑：模板變數

Catalog.yaml 中 model artifacts 路徑使用 `${model_version}` 模板變數（如 `data/models/${model_version}/model.pkl`）。ConfigLoader 的 `get_catalog_config()` 新增 `runtime_params` 參數，CLI 執行時傳入 `{"model_version": timestamp}` (training) 或 `{"model_version": "best"}` (其他)。

**效果**：版本化意圖在 catalog.yaml 中一目瞭然，不需要讀 CLI 原始碼才知道實際寫入路徑。Training 透過 catalog 直接寫入版本目錄 `data/models/{timestamp}/`，不接觸 `best/`。Inference 從 `data/models/best/` 讀取。

**替代方案（已放棄）**：CLI 用 `str.replace("models/best/", ...)` 覆寫 → 讀 catalog.yaml 的人不知道 training 實際寫到別處，不直觀。

**替代方案**：在 training pipeline 加 node 手動寫檔 → 繞過 catalog 系統，不夠乾淨。

**替代方案**：Training 用不同名稱的 output（如 `_trained_model`）→ 破壞 pipeline 語義清晰度。

### 3. Model selection：報告制，不自動 promote

`compare_model_versions` node 只掃描版本目錄、讀取 evaluation_results.json、輸出比較 log。不做任何檔案複製。

**理由**：使用者明確要求人工把關流程，避免自動更新 production 模型帶來的風險。

### 4. Promote 機制：獨立 Python script

`scripts/promote_model.py` 接受版本時間戳（或不指定則自動選 mAP 最高），複製 artifacts 到 `best/`。

**替代方案**：Makefile → 太簡單，難以加入驗證邏輯。

**替代方案**：獨立 pipeline → 過度工程，promote 是一次性操作不需要 DAG。

## Risks / Trade-offs

- **磁碟空間增長** → 每次訓練產生一個版本目錄。短期可接受，長期可加入版本清理功能（non-goal for now）。
- **時間戳衝突** → 同一秒內啟動兩次訓練可能產生相同時間戳。機率極低，且目前不支援並行訓練。
- **首次使用需 promote** → `best/` 目錄不存在時 inference 會失敗。使用者需在首次訓練後執行 promote script 才能使用 inference。

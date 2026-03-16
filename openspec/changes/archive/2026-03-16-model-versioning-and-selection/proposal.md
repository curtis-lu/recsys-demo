## Why

目前 training pipeline 每次訓練都會覆蓋 `data/models/model.pkl` 等固定路徑的 artifacts，導致無法保留歷史版本、無法比較不同訓練結果、無法回退到較佳的模型。此外，inference pipeline 缺乏明確的模型選擇機制，無法確保使用的是經過人工驗證的最佳模型。

## What Changes

- 每次訓練自動將所有 artifacts 存入帶時間戳的版本化目錄（如 `data/models/20260316_153000/`）
- 新增 `compare_model_versions` node，在訓練完成後輸出所有版本的 mAP 比較報告
- 新增獨立的 `promote_model.py` script，人工確認後將指定版本複製到 `data/models/best/` 供 inference 使用
- Catalog 中 model artifacts 的路徑改為使用 `${model_version}` 模板變數，由 ConfigLoader `runtime_params` 在執行時解析（training → 時間戳，inference → `"best"`）

## Capabilities

### New Capabilities
- `model-versioning`: 訓練時自動版本化儲存所有 artifacts 到時間戳目錄，並產出跨版本 mAP 比較報告
- `model-promotion`: 獨立 CLI script 將指定模型版本（或自動選 mAP 最高）promote 到 best/ 目錄供 inference 使用

### Modified Capabilities
- `training-pipeline`: 新增 `compare_model_versions` node（版本化儲存透過 catalog 模板變數實現，不需額外 node）
- `data-catalog`: model artifacts 路徑改用 `${model_version}` 模板變數
- `config-loader`: `get_catalog_config()` 新增 `runtime_params` 支援模板變數替換

## Impact

- **程式碼**: `training/nodes.py`（新增 1 個函式）、`training/pipeline.py`（新增 1 個 node）、`core/config.py`（新增 `runtime_params` 支援）、`__main__.py`（簡化版本化邏輯）、新增 `scripts/promote_model.py`
- **設定檔**: `conf/*/catalog.yaml` 中 model artifacts 路徑改用 `${model_version}` 模板變數
- **儲存**: `data/models/` 目錄結構改變，每次訓練會新增一個版本子目錄
- **Inference**: 無程式碼改動，但首次使用前需先執行 promote script 建立 `best/` 目錄

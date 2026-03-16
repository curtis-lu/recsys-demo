## Why

Dataset pipeline 產出（data/dataset/）和 inference pipeline 產出（data/inference/）目前使用固定路徑，每次執行都會覆蓋前一次結果，無法保留歷史版本。Model 雖有時間戳版本目錄，但缺乏追溯資訊（對應的參數設定、使用的 dataset 版本），且 preprocessor.pkl、category_mappings.json 等相依的上游物件歸類在 model 目錄下而非它們實際產生的 dataset 目錄。需要統一的版本管理機制，讓每個 pipeline 的產出都可追溯、可重現。

## What Changes

- **BREAKING**: Model 版本 ID 從時間戳 (`YYYYMMDD_HHMMSS`) 改為參數 hash（8 字元 hex），hash 範圍包含 `parameters_training.yaml` 內容 + `dataset_version`
- **BREAKING**: `preprocessor` 和 `category_mappings` 的 catalog 路徑從 `data/models/${model_version}/` 移至 `data/dataset/${dataset_version}/`
- 新增 dataset 版本化：產出路徑加入 `${dataset_version}` template variable，版本 ID 為 `parameters_dataset.yaml` 內容的 hash
- 新增 inference 版本化：產出路徑改為 `data/inference/${model_version}/${snap_date}/`
- 每個版本目錄自動寫入 `manifest.json`，記錄參數快照、上游版本、git commit、建立時間
- Dataset 新增 `latest` symlink 機制，自動指向最新產生的版本
- Model 的 `best` 從目錄複製改為 symlink
- 新增 `VersionManager` 模組集中管理 hash 計算、manifest 生成、symlink 維護
- CLI 新增 `--dataset-version` 選項，可手動指定 dataset 版本
- Inference 自動從 model manifest 解析對應的 `dataset_version`

## Capabilities

### New Capabilities
- `dataset-versioning`: Dataset pipeline 產出的版本化管理，包含 hash-based 版本 ID、manifest、latest symlink
- `inference-versioning`: Inference pipeline 產出的版本化管理，以 model_version/snap_date 組織
- `version-manager`: 集中式版本管理模組，封裝 hash 計算、manifest 生成、symlink 維護、版本解析

### Modified Capabilities
- `model-versioning`: 版本 ID 從時間戳改為參數 hash；manifest.json 追溯資訊
- `model-promotion`: promote 改用 symlink；支援 hash 格式版本 ID；複製 manifest.json
- `data-catalog`: 新增 `${dataset_version}` 和 `${snap_date}` template variables；preprocessor/category_mappings 路徑變更
- `config-loader`: 支援多個 runtime_params template variables
- `cli`: 新增 `--dataset-version` 選項；版本解析邏輯變更
- `dataset-pipeline`: 產出路徑加入版本目錄
- `inference-pipeline`: 產出路徑加入版本目錄

## Impact

- **Catalog config** (`conf/base/catalog.yaml`, `conf/production/catalog.yaml`, `conf/local/catalog.yaml`): 所有 dataset/inference 相關路徑需加入 template variables
- **CLI** (`src/recsys_tfb/__main__.py`): 版本計算邏輯重構，改用 VersionManager
- **New module** (`src/recsys_tfb/core/versioning.py`): VersionManager 實作
- **Promote script** (`scripts/promote_model.py`): 改用 symlink、支援新版本格式
- **Training nodes** (`src/recsys_tfb/pipelines/training/nodes.py`): `compare_model_versions` 需支援 hash 格式目錄
- **現有 model 版本目錄**: 舊的 `YYYYMMDD_HHMMSS` 目錄保留但不再由新邏輯產生
- **測試**: 需更新所有涉及版本路徑的測試

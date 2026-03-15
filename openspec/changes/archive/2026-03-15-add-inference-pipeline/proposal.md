## Why

Training pipeline (Strategy 1 MVP) 已完成，能產出訓練好的 LightGBM 模型與 preprocessor。目前缺少將模型應用於新資料的批次評分能力，無法產出客戶產品推薦排名供行銷 PM 使用。建立 Inference Pipeline 是讓 MVP 端對端可用的關鍵一步。

## What Changes

- 新增 `inference` pipeline 模組，包含 4 個 node 函數：建立評分資料集、套用前處理、預測評分、排名
- 新增 `parameters_inference.yaml` 設定檔，定義評分的 snap_date 與產品列表
- 在 catalog 中新增 inference 相關的 dataset 定義（scoring_dataset、ranked_predictions）
- 在 pipeline registry 中註冊 `inference` pipeline
- 輸出格式：按 snap_date 與 prod_code 的 Parquet 檔案，包含 cust_id、prod_code、score、rank

## Capabilities

### New Capabilities
- `inference-nodes`: 推論 pipeline 的 4 個純函數節點——build_scoring_dataset、apply_preprocessor、predict_scores、rank_predictions
- `inference-pipeline`: 推論 pipeline 定義，串接 4 個 nodes 並整合至 pipeline registry

### Modified Capabilities
- `pipeline-registry`: 新增 `"inference"` 進入 `_REGISTRY`
- `data-catalog`: 新增 scoring_dataset 與 ranked_predictions 的 catalog 定義

## Impact

- 新增檔案：`src/recsys_tfb/pipelines/inference/` 目錄（__init__.py、nodes.py、pipeline.py）、`conf/base/parameters_inference.yaml`
- 修改檔案：`src/recsys_tfb/pipelines/__init__.py`、`conf/base/catalog.yaml`
- 依賴現有 training artifacts：model.pkl、preprocessor.pkl
- 不修改 dataset 或 training pipeline 的任何程式碼

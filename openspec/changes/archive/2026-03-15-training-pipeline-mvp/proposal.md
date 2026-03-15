## Why

Dataset building pipeline 已完成，可產出 X_train / X_train_dev / X_val 等模型就緒資料。目前 training pipeline 為空殼，無法進行模型訓練與評估。需建立 MVP 訓練流程（Strategy 1：單一二元分類器 + mAP 指標），以驗證端到端可行性並產出初版模型。

## What Changes

- 新增 LightGBM 模型訓練節點：使用 train set 訓練，train_dev set 做 early stopping
- 新增 Optuna 超參數搜索節點：以 train_dev mAP 為目標函數，自動搜尋最佳超參數
- 新增模型評估節點：計算 mAP（mean Average Precision）指標，支援 overall 與 per-product 切片
- 新增 MLflow 實驗追蹤：記錄超參數、指標、模型 artifact
- 新增 training pipeline DAG：串接上述節點，通過 catalog 持久化模型與評估結果
- 新增 training 相關 YAML 設定（parameters_training.yaml, catalog entries）
- 新增 training pipeline 測試

## Capabilities

### New Capabilities

- `training-nodes`: LightGBM 訓練、Optuna 超參數搜索、mAP 評估的純函數節點
- `training-pipeline`: 訓練 pipeline DAG 定義與 catalog 配置
- `mlflow-tracking`: MLflow 實驗追蹤整合（參數、指標、模型記錄）

### Modified Capabilities

- `data-catalog`: 新增 training pipeline 輸出的 catalog entries（evaluation_results, best_params 等）

## Impact

- **新增檔案**: `src/recsys_tfb/pipelines/training/nodes.py`, `src/recsys_tfb/pipelines/training/pipeline.py`, `conf/base/parameters_training.yaml`, 測試檔案
- **修改檔案**: `conf/base/catalog.yaml`（新增 entries）, `src/recsys_tfb/pipelines/training/__init__.py`
- **新增依賴**: LightGBM 4.6.0, Optuna 4.5.0, MLflow 3.1.0（皆已在 requirements 中）
- **下游影響**: 訓練完成後可接續 inference pipeline 開發


## Why

Training 和 Inference pipeline 目前直接依賴 LightGBM API（`lgb.Booster`、`lgb.Dataset`、`lgb.train()`），使得切換或新增演算法需要大幅修改 pipeline nodes。建立 ModelAdapter 抽象層，將演算法特有邏輯封裝在 adapter 中，讓 pipeline nodes 透過統一介面操作模型，為未來支援 XGBoost、ranking model 等演算法打下基礎。

## What Changes

- 新增 `ModelAdapter` ABC，定義 `train`/`predict`/`save`/`load`/`feature_importance`/`log_to_mlflow` 統一介面
- 新增 `LightGBMAdapter` 實作，封裝所有 LightGBM 特有邏輯（`lgb.Dataset` 建立、`lgb.train()`、early stopping）
- 新增 `ModelAdapterDataset` I/O adapter，取代現有 `LightGBMDataset`，支援 `model_meta.json` sidecar 記錄演算法資訊
- **BREAKING**: `LightGBMDataset` 被 `ModelAdapterDataset` 取代，catalog.yaml 需更新 model entry 的 type
- 重構 training nodes（`tune_hyperparameters`、`train_model`、`evaluate_model`、`log_experiment`），從直接使用 `lgb.*` 改為透過 adapter 介面
- 重構 inference nodes（pandas/spark `predict_scores`），model 參數型別從 `lgb.Booster` 改為 `ModelAdapter`
- `parameters_training.yaml` 新增 `algorithm` 和 `algorithm_params` 設定，將 hard-coded 的 objective/metric 移至 config
- 不含 Calibration（延後至獨立 phase）
- 不含 XGBoostAdapter（僅確保 ABC 設計不對 LightGBM 隱式依賴）

## Capabilities

### New Capabilities
- `model-adapter`: ModelAdapter ABC 介面定義與演算法 adapter 實作（LightGBMAdapter），封裝 train/predict/save/load/feature_importance/log_to_mlflow
- `model-adapter-io`: ModelAdapterDataset I/O adapter，支援 model_meta.json sidecar，自動根據 meta 選擇正確 adapter 載入模型

### Modified Capabilities
- `training-nodes`: 重構為使用 ModelAdapter 介面，不再直接依賴 lgb.* API；新增 algorithm/algorithm_params config 支援
- `inference-nodes`: model 參數型別從 lgb.Booster 改為 ModelAdapter
- `spark-inference-nodes`: 同上，Spark 版本的 model 參數型別變更
- `mlflow-tracking`: 從 mlflow.lightgbm.log_model() 改為 adapter.log_to_mlflow()

## Impact

- **程式碼**：`src/recsys_tfb/models/`（新增）、`pipelines/training/nodes.py`、`pipelines/inference/nodes_pandas.py`、`pipelines/inference/nodes_spark.py`、`io/model_adapter_dataset.py`（新增）
- **設定**：`conf/base/catalog.yaml`（model entry type 變更）、`conf/base/parameters_training.yaml`（新增 algorithm/algorithm_params）
- **移除**：`io/lightgbm_dataset.py` 被 `model_adapter_dataset.py` 取代
- **測試**：需新增 model adapter 單元測試、更新 training/inference 相關測試
- **相依性**：無新增外部套件，LightGBM 仍為唯一 ML 框架依賴

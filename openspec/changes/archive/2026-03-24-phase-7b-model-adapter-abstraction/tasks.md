## 1. ModelAdapter ABC 與 Registry

- [x] 1.1 建立 `src/recsys_tfb/models/__init__.py` 和 `src/recsys_tfb/models/base.py`，定義 ModelAdapter ABC（train, predict, save, load, feature_importance, log_to_mlflow 抽象方法）與 ADAPTER_REGISTRY + get_adapter() 工廠函數
- [x] 1.2 建立 `src/recsys_tfb/models/lightgbm_adapter.py`，實作 LightGBMAdapter：train() 內部建立 lgb.Dataset 並呼叫 lgb.train()、predict() 回傳 np.ndarray、save/load 使用原生格式、feature_importance() 回傳 dict、log_to_mlflow() 呼叫 mlflow.lightgbm.log_model()
- [x] 1.3 撰寫 `tests/test_models/` 單元測試：ABC 不可實例化、LightGBMAdapter train/predict/save/load/feature_importance 基本功能、get_adapter registry 正確回傳與錯誤處理

## 2. ModelAdapterDataset I/O

- [x] 2.1 建立 `src/recsys_tfb/io/model_adapter_dataset.py`，實作 ModelAdapterDataset：save 時寫 model 檔 + model_meta.json sidecar、load 時讀 meta 自動選擇 adapter、向後相容（無 meta 時 fallback LightGBM）
- [x] 2.2 更新 `src/recsys_tfb/io/__init__.py`，匯出 ModelAdapterDataset
- [x] 2.3 移除 `src/recsys_tfb/io/lightgbm_dataset.py`，更新所有 import 引用
- [x] 2.4 撰寫 ModelAdapterDataset 單元測試：save/load round-trip、meta sidecar 內容驗證、無 meta fallback

## 3. Training Nodes 重構

- [x] 3.1 更新 `conf/base/parameters_training.yaml`，新增 `training.algorithm: lightgbm` 和 `training.algorithm_params: {objective: binary, metric: binary_logloss, verbosity: -1}`，移除 nodes.py 中 hard-coded 的 objective/metric
- [x] 3.2 重構 `tune_hyperparameters()`：使用 get_adapter() 建立 adapter，每個 trial 呼叫 adapter.train() + adapter.predict()，移除直接的 lgb.Dataset/lgb.train 呼叫
- [x] 3.3 重構 `train_model()`：使用 get_adapter() 建立 adapter，回傳型別改為 ModelAdapter，合併 algorithm_params + best_params 傳給 adapter.train()
- [x] 3.4 重構 `evaluate_model()`：參數型別改為 ModelAdapter，使用 adapter.predict()
- [x] 3.5 重構 `log_experiment()`：使用 adapter.log_to_mlflow() 取代 mlflow.lightgbm.log_model()，額外記錄 algorithm param
- [x] 3.6 更新 training 相關測試，確保使用 adapter 介面

## 4. Inference Nodes 重構

- [x] 4.1 更新 `pipelines/inference/nodes_pandas.py` 的 `predict_scores()`：model 參數型別改為 ModelAdapter
- [x] 4.2 更新 `pipelines/inference/nodes_spark.py` 的 `predict_scores()`：model 參數型別改為 ModelAdapter
- [x] 4.3 更新 inference 相關測試（已相容，MockModel 使用 predict() 介面）

## 5. Config 與 Catalog 更新

- [x] 5.1 更新 `conf/base/catalog.yaml`：model entry type 改為 ModelAdapterDataset
- [x] 5.2 更新 `conf/local/catalog.yaml`（若存在）：同步修改 model entry
- [x] 5.3 更新 `conf/production/catalog.yaml`（若存在）：同步修改 model entry

## 6. 驗證與文件

- [x] 6.1 執行完整測試套件 `pytest tests/ -v`，確認所有測試通過（406 passed）
- [x] 6.2 執行端到端測試：`python -m recsys_tfb --pipeline training --env local` 訓練成功
- [x] 6.3 執行端到端測試：`python -m recsys_tfb --pipeline inference --env local` 推論成功
- [x] 6.4 更新 `CLAUDE.md`：Phase 7b 標記為完成、Calibration 移至獨立 phase、更新 models/ 目錄結構說明

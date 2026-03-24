## Why

目前的資料切割為 3-way（train / train-dev / val），無法支援機率校準所需的獨立 calibration set，也缺少與 validation 分離的 test set 用於最終評估。此外，train 和 train-dev 目前以日期切分，導致兩者分佈不一致，不利於 early stopping 偵測 overfitting。需要重構為 5-way split 以支援完整的 ML 實驗流程，同時增加 per-group 自訂抽樣比例的彈性。

## What Changes

- **BREAKING**: `split_keys` 函數移除，改由 `split_train_keys` + `select_calibration_keys` + `select_val_keys` + `select_test_keys` 取代
- **BREAKING**: `train_dev_snap_dates` 參數移除，改用 `train_dev_ratio`（train & train-dev 共用日期，按 cust_id 比例切分）
- **BREAKING**: `prepare_model_input` 輸出從 8 個擴展為 10 個（不含 calibration）或 12 個（含 calibration）
- 新增 `test_snap_dates` 參數，test set 為全量不抽樣
- 新增 `enable_calibration` flag + `calibration_snap_dates` + `calibration_sample_ratio`
- 新增 `sample_ratio_overrides`：支援 per-group 自訂抽樣比例（多欄位以 `"|"` 組合為 key）
- `val_sample_ratio` 邏輯從 `prepare_model_input` 移至 `select_val_keys`（抽樣提前到 key selection 階段）
- Pipeline 條件式建構：`create_pipeline(backend, enable_calibration)` 依 flag 決定是否包含 calibration nodes
- `get_pipeline` 增加 `**kwargs` 傳遞，CLI 從 parameters 讀取 `enable_calibration` 傳入

## Capabilities

### New Capabilities

- `stratified-sampling-overrides`: 支援 per-group 自訂抽樣比例（sample_ratio_overrides），多欄位 sample_group_keys 以 `"|"` 組合序列化為 key
- `calibration-split`: Optional calibration dataset split，獨立日期與抽樣比例，由 enable_calibration flag 控制
- `test-split`: 獨立 test dataset split，全量不抽樣，用於最終評估
- `train-dev-cust-split`: Train 與 train-dev 共用日期，依 cust_id 比例切分（train_dev_ratio），確保分佈一致

### Modified Capabilities

- `dataset-nodes`: split_keys 移除，新增 split_train_keys / select_calibration_keys / select_val_keys / select_test_keys；select_sample_keys 過濾至 train dates 並支援 overrides；prepare_model_input 擴展為 4-set / 5-set
- `dataset-pipeline`: Pipeline 條件式建構（enable_calibration），node 拓撲重構
- `spark-dataset-nodes`: 所有 pandas node 的 Spark backend 對應實作
- `val-sampling`: val_sample_ratio 邏輯從 prepare_model_input 移至 select_val_keys
- `pipeline-registry`: get_pipeline 增加 **kwargs 傳遞
- `cli`: __main__.py 從 parameters 讀取 enable_calibration 傳入 get_pipeline

## Impact

- **Code**: `nodes_pandas.py`, `nodes_spark.py`, `pipeline.py`（dataset pipeline）, `pipelines/__init__.py`, `__main__.py`
- **Config**: `parameters_dataset.yaml`（新增/移除參數）, `catalog.yaml` / `local/catalog.yaml` / `production/catalog.yaml`（新增 catalog entries）
- **Tests**: `test_nodes.py`（dataset pipeline 測試全面更新）
- **Downstream（不受影響）**: Training pipeline 繼續消費 X_train / y_train / X_train_dev / y_train_dev / X_val / y_val；Inference pipeline 消費 preprocessor + model
- **Downstream（未來受益）**: Phase 7c (Calibration) 將消費 X_calibration / y_calibration；Phase 8 (Evaluation) 將消費 X_test / y_test / test_set

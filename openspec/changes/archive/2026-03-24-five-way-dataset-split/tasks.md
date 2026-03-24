## 1. Parameters & Config

- [x] 1.1 更新 `conf/base/parameters_dataset.yaml`：移除 `train_dev_snap_dates`，新增 `train_dev_ratio`, `enable_calibration`, `calibration_snap_dates`, `calibration_sample_ratio`, `test_snap_dates`, `sample_ratio_overrides`
- [x] 1.2 更新 `conf/base/catalog.yaml`：新增 `calibration_keys`, `test_keys`, `calibration_set`, `test_set`, `X_calibration`, `y_calibration`, `X_test`, `y_test` entries
- [x] 1.3 更新 `conf/local/catalog.yaml`：新增對應的 local (pandas backend) catalog entries
- [x] 1.4 更新 `conf/production/catalog.yaml`：新增對應的 production (spark backend) catalog entries

## 2. Pandas Node 實作

- [x] 2.1 新增日期驗證 helper `_validate_date_splits(parameters)` 於 `nodes_pandas.py`
- [x] 2.2 重構 `select_sample_keys`：過濾至 train dates + 支援 `sample_ratio_overrides`（per-group effective ratio + `rand() < ratio` 抽樣）
- [x] 2.3 新增 `split_train_keys(sample_keys, parameters)` → (train_keys, train_dev_keys)：按 cust_id 比例切分
- [x] 2.4 新增 `select_calibration_keys(sample_pool, label_table, parameters)` → calibration_keys：獨立日期 + 分層抽樣
- [x] 2.5 新增 `select_val_keys(label_table, parameters)` → val_keys：全量或可選純隨機抽樣
- [x] 2.6 新增 `select_test_keys(label_table, parameters)` → test_keys：全量不抽樣
- [x] 2.7 重構 `prepare_model_input`：擴展為 4-set 版本（+test_set），移除 val_sample_ratio 邏輯
- [x] 2.8 新增 `prepare_model_input_with_calibration`：5-set 版本（+calibration_set），共用 `_transform` helper
- [x] 2.9 移除舊的 `split_keys` 函數

## 3. Spark Node 實作

- [x] 3.1 Mirror `_validate_date_splits` 至 `nodes_spark.py`
- [x] 3.2 重構 Spark `select_sample_keys`：過濾至 train dates + 支援 overrides
- [x] 3.3 新增 Spark `split_train_keys`
- [x] 3.4 新增 Spark `select_calibration_keys`
- [x] 3.5 新增 Spark `select_val_keys`
- [x] 3.6 新增 Spark `select_test_keys`
- [x] 3.7 重構 Spark `prepare_model_input` 為 4-set 版本
- [x] 3.8 新增 Spark `prepare_model_input_with_calibration` 5-set 版本
- [x] 3.9 移除舊的 Spark `split_keys` 函數

## 4. Pipeline 定義 & Registry & CLI

- [x] 4.1 更新 `pipeline.py`：`create_pipeline(backend, enable_calibration)` 條件式建構
- [x] 4.2 更新 `pipelines/__init__.py`：`get_pipeline` 增加 `**kwargs` 傳遞
- [x] 4.3 更新 `__main__.py`：dataset pipeline 時從 parameters 讀取 `enable_calibration` 傳入 `get_pipeline`

## 5. 測試

- [x] 5.1 更新 test parameters fixture（新增 train_dev_ratio, test_snap_dates, enable_calibration, sample_ratio_overrides 等）
- [x] 5.2 更新 TestSelectSampleKeys：驗證 train dates 過濾、sample_ratio_overrides
- [x] 5.3 新增 TestSplitTrainKeys：cust_id ratio 切分、不重疊、確定性、all keys preserved
- [x] 5.4 新增 TestSelectCalibrationKeys：日期過濾、分層抽樣、deterministic
- [x] 5.5 新增 TestSelectValKeys：全量、可選隨機抽樣、by cust_id
- [x] 5.6 新增 TestSelectTestKeys：全量不抽樣
- [x] 5.7 更新 TestPrepareModelInput：4-set 版本（+test_set）、確認不再有 val_sample_ratio 邏輯
- [x] 5.8 新增 TestPrepareModelInputWithCalibration：5-set 版本
- [x] 5.9 新增 TestDateValidation：日期不重疊驗證、重疊時 raise ValueError
- [x] 5.10 新增 TestSampleRatioOverrides：單欄位/多欄位 override、fallback to default

## 6. 驗證

- [x] 6.1 執行 `pytest tests/ -v` 確認所有測試通過
- [x] 6.2 執行 `python -m recsys_tfb --pipeline dataset --env local`（enable_calibration=false）確認產出 4 組
- [x] 6.3 修改 parameters 設 `enable_calibration: true` + `calibration_snap_dates`，重跑確認產出 5 組

## 1. LightGBMDataset Adapter

- [x] 1.1 建立 `src/recsys_tfb/io/lightgbm_dataset.py`：實作 `LightGBMDataset(AbstractDataset)`，使用 `lgb.Booster.save_model()` / `lgb.Booster(model_file=...)` 進行 save/load
- [x] 1.2 在 `src/recsys_tfb/io/__init__.py` 新增 `LightGBMDataset` export
- [x] 1.3 在 `src/recsys_tfb/core/catalog.py` 的 `_DATASET_REGISTRY` 新增 `"LightGBMDataset"` 項目
- [x] 1.4 建立 `tests/test_io/test_lightgbm_dataset.py`：測試 save/load roundtrip、exists、輸出為文字檔
- [x] 1.5 修改 `conf/base/catalog.yaml`：model entry 從 `PickleDataset` / `model.pkl` 改為 `LightGBMDataset` / `model.txt`
- [x] 1.6 修改 `tests/test_cli.py`：更新 model 相關的 catalog config 和 assertion（`model.pkl` → `model.txt`）

## 2. Parameter Snapshot

- [x] 2.1 修改 `src/recsys_tfb/__main__.py` dataset post-run 區塊：`write_manifest()` 後新增 `parameters_dataset.json` 寫入
- [x] 2.2 修改 `src/recsys_tfb/__main__.py` training post-run 區塊：新增 `parameters_training.json` 寫入
- [x] 2.3 修改 `src/recsys_tfb/__main__.py` inference post-run 區塊：新增 `parameters_inference.json` 寫入

## 3. X/y Artifacts 改用 Parquet

- [x] 3.1 修改 `src/recsys_tfb/pipelines/dataset/nodes_pandas.py` 的 `prepare_model_input()`：y_train/y_train_dev/y_val 改為回傳 `pd.DataFrame({"label": ...})` 並更新 type hints
- [x] 3.2 修改 `src/recsys_tfb/pipelines/dataset/nodes_spark.py` 的 `prepare_model_input()`：同上
- [x] 3.3 修改 `src/recsys_tfb/pipelines/training/nodes.py` 的 `tune_hyperparameters()`：type hints 改為 `pd.DataFrame`，lgb.Dataset 和 _compute_ap 呼叫改用 `["label"].values`
- [x] 3.4 修改 `src/recsys_tfb/pipelines/training/nodes.py` 的 `train_model()`：同上
- [x] 3.5 修改 `src/recsys_tfb/pipelines/training/nodes.py` 的 `evaluate_model()`：type hint 改為 `pd.DataFrame`，_compute_map 和 per-product AP 呼叫改用 `["label"].values`
- [x] 3.6 修改 `conf/base/catalog.yaml`：X_train/y_train/X_train_dev/y_train_dev/X_val/y_val 從 PickleDataset (.pkl) 改為 ParquetDataset (.parquet) with backend: pandas

## 4. Tests 更新

- [x] 4.1 修改 `tests/test_pipelines/test_dataset/test_nodes.py`：y_train 的 type assertion 從 `np.ndarray` 改為 `pd.DataFrame`，驗證 columns == ["label"]
- [x] 4.2 修改 `tests/test_pipelines/test_training/test_nodes.py`：`synthetic_data` fixture 的 `make_labels` 改為回傳 `pd.DataFrame({"label": ...})`，以及 `y_val_extended` 等測試資料
- [x] 4.3 修改 `tests/test_pipelines/test_dataset/test_nodes_spark.py`：同 4.1 的 type assertion 修改（如適用）

## 5. 驗證

- [x] 5.1 執行 `pytest tests/ -v` 確認全部測試通過
- [x] 5.2 執行 `python -m recsys_tfb run --pipeline dataset --env local` 確認 parquet 檔案正確產出
- [x] 5.3 執行 `python -m recsys_tfb run --pipeline training --env local` 確認 model.txt 和 parameters JSON 正確產出

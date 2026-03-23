## 1. Catalog Memory Release

- [x] 1.1 在 `MemoryDataset` 新增 `release()` 方法（`core/catalog.py`）
- [x] 1.2 在 `DataCatalog` 新增 `get_dataset(name)` 方法（`core/catalog.py`）
- [x] 1.3 在 `Runner` 新增 `_build_last_consumer_map()` 靜態方法，分析 DAG 依賴計算每個 dataset 的最後消費者（`core/runner.py`）
- [x] 1.4 在 `Runner.run()` 的 node 執行迴圈中，於每個 node 完成後釋放不再需要的 MemoryDataset（`core/runner.py`）
- [x] 1.5 新增 `dataset_released` structured log event，記錄 dataset_name 和 node 名稱（`core/runner.py`、`core/logging.py`）
- [x] 1.6 撰寫 memory release 相關測試：release() 方法、get_dataset()、last consumer map、多消費者場景、log event（`tests/test_core/`）

## 2. Sample Pool 分離

- [x] 2.1 更新 `generate_synthetic_data.py`，新增產出 `data/sample_pool.parquet`（含 snap_date、cust_id、cust_segment_typ）
- [x] 2.2 在 `conf/base/catalog.yaml` 新增 `sample_pool` ParquetDataset 定義
- [x] 2.3 更新 `nodes_pandas.py` 的 `select_sample_keys`，輸入從 `label_table` 改為 `sample_pool`
- [x] 2.4 更新 `nodes_spark.py` 的 `select_sample_keys`，輸入從 `label_table` 改為 `sample_pool`
- [x] 2.5 更新 `pipelines/dataset/pipeline.py`，將 `select_sample_keys` node 的 inputs 從 `label_table` 改為 `sample_pool`
- [x] 2.6 更新 dataset pipeline 相關測試，新增 `sample_pool` fixture 並修改呼叫（`tests/test_pipelines/test_dataset/`）

## 3. Val Sampling

- [x] 3.1 在 `conf/base/parameters_dataset.yaml` 新增 `val_sample_ratio: 1.0` 參數
- [x] 3.2 更新 `nodes_pandas.py` 的 `prepare_model_input`，加入 val set 可選抽樣邏輯
- [x] 3.3 更新 `nodes_spark.py` 的 `prepare_model_input`，加入 val set 可選抽樣邏輯
- [x] 3.4 撰寫 val sampling 相關測試：預設全量、抽樣比例、分層抽樣、deterministic、group keys fallback（`tests/test_pipelines/test_dataset/`）

## 4. 整合驗證

- [x] 4.1 執行 `python scripts/generate_synthetic_data.py` 產生包含 sample_pool 的假資料
- [x] 4.2 執行 `pytest tests/ -v` 確認所有測試通過（351 passed，7 pre-existing promote_model failures）
- [x] 4.3 執行 `python -m recsys_tfb --pipeline dataset --env local` 端到端驗證，確認 log 中出現 `dataset_released` 事件
- [x] 4.4 執行 `python -m recsys_tfb --pipeline training --env local` 確認 training pipeline 仍正常運作

## 1. 配置與呼叫鏈

- [x] 1.1 在 `conf/base/parameters.yaml` 新增 `backend: pandas` 參數
- [x] 1.2 新增 `conf/production/parameters.yaml`，設定 `backend: spark`
- [x] 1.3 修改 `src/recsys_tfb/pipelines/__init__.py`：`get_pipeline(name, backend)` 接受並傳遞 backend 參數
- [x] 1.4 修改 `src/recsys_tfb/__main__.py`：先載入 config 取 backend，再呼叫 `get_pipeline(name, backend)`

## 2. Dataset Pipeline 雙模式

- [x] 2.1 將 `src/recsys_tfb/pipelines/dataset/nodes.py` 重命名為 `nodes_pandas.py`
- [x] 2.2 新增 `src/recsys_tfb/pipelines/dataset/nodes_spark.py`：實作 `select_sample_keys`（Window 分層抽樣）
- [x] 2.3 在 `nodes_spark.py` 實作 `split_keys`（PySpark filter）
- [x] 2.4 在 `nodes_spark.py` 實作 `build_dataset`（PySpark join）
- [x] 2.5 在 `nodes_spark.py` 實作 `prepare_model_input`（.toPandas() 轉換 + category encoding）
- [x] 2.6 修改 `src/recsys_tfb/pipelines/dataset/pipeline.py`：`create_pipeline(backend)` 根據 backend 選擇 import 來源

## 3. Inference Pipeline 雙模式

- [x] 3.1 將 `src/recsys_tfb/pipelines/inference/nodes.py` 重命名為 `nodes_pandas.py`
- [x] 3.2 新增 `src/recsys_tfb/pipelines/inference/nodes_spark.py`：實作 `build_scoring_dataset`（crossJoin + filter）
- [x] 3.3 在 `nodes_spark.py` 實作 `apply_preprocessor`（broadcast join category encoding，保留 identity 欄位）
- [x] 3.4 在 `nodes_spark.py` 實作 `predict_scores`（按 snap_date 分塊 .toPandas() + model.predict()）
- [x] 3.5 在 `nodes_spark.py` 實作 `rank_predictions`（Window + row_number）
- [x] 3.6 修改 `src/recsys_tfb/pipelines/inference/pipeline.py`：`create_pipeline(backend)` 根據 backend 選擇 import 來源

## 4. I/O 層與 Training Pipeline

- [x] 4.1 修改 `src/recsys_tfb/io/parquet_dataset.py`：save() 加入自動型別轉換（pandas ↔ Spark）
- [x] 4.2 修改 `src/recsys_tfb/pipelines/training/pipeline.py`：`create_pipeline(backend)` 簽名一致但始終用 pandas node

## 5. 測試

- [x] 5.1 更新 `tests/test_pipelines/test_dataset/test_nodes.py`：修正 import 路徑為 `nodes_pandas`
- [x] 5.2 新增 `tests/test_pipelines/test_dataset/test_nodes_spark.py`：用 Spark fixture 測試四個 Spark node 函數
- [x] 5.3 新增 `tests/test_pipelines/test_inference/test_nodes_spark.py`：用 Spark fixture 測試四個 Spark inference node 函數
- [x] 5.4 新增 `tests/test_io/test_parquet_dataset.py`（或更新）：測試 save 自動型別轉換
- [x] 5.5 執行 `pytest` 確認所有測試通過

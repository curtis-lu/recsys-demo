## Why

所有 pipeline node 函數目前只有 pandas 實作，但生產環境 catalog 設定 `backend: spark`，使 `ParquetDataset.load()` 回傳 `pyspark.sql.DataFrame`，導致 `--env production` 直接報錯。生產環境的 feature_table / label_table 可能超過 128GB，無法全量 `.toPandas()` 轉換。需要讓使用者可以透過配置選擇 pandas 或 Spark 處理資料。

## What Changes

- 新增 `parameters.yaml` 頂層 `backend` 參數（`"pandas"` 或 `"spark"`），驅動 pipeline 選擇哪套 node 實作
- 新增 `conf/production/parameters.yaml`，設定 `backend: spark`
- Dataset pipeline：將現有 `nodes.py` 重命名為 `nodes_pandas.py`，新增 `nodes_spark.py`（PySpark 原生實作）
- Inference pipeline：同上模式，將 `nodes.py` 重命名為 `nodes_pandas.py`，新增 `nodes_spark.py`
- 修改 `pipeline.py`：`create_pipeline(backend)` 根據 backend 參數選擇 import 哪套 node
- 修改呼叫鏈：`__main__.py` → `get_pipeline(name, backend)` → `create_pipeline(backend)`
- `ParquetDataset.save()` 加入自動型別轉換（pandas ↔ Spark 防禦性處理）
- Training pipeline 不需要 Spark 版本（LightGBM 硬性要求 pandas/numpy）

## Capabilities

### New Capabilities
- `spark-dataset-nodes`: Dataset pipeline 的 PySpark 原生 node 實作（select_sample_keys、split_keys、build_dataset、prepare_model_input）
- `spark-inference-nodes`: Inference pipeline 的 PySpark 原生 node 實作（build_scoring_dataset、apply_preprocessor、predict_scores、rank_predictions）
- `backend-selection`: 透過 `backend` 參數在 pipeline 層級選擇 pandas 或 Spark node 實作

### Modified Capabilities
- `dataset-pipeline`: `create_pipeline` 新增 `backend` 參數，根據配置動態選擇 node 來源
- `inference-pipeline`: 同上
- `pipeline-registry`: `get_pipeline` 新增 `backend` 參數傳遞
- `cli`: `run` 命令先載入 config 取得 backend，再建立 pipeline
- `io-datasets`: `ParquetDataset.save()` 加入自動型別轉換

## Impact

- **程式碼**：`src/recsys_tfb/pipelines/dataset/`、`src/recsys_tfb/pipelines/inference/`、`src/recsys_tfb/pipelines/__init__.py`、`src/recsys_tfb/__main__.py`、`src/recsys_tfb/io/parquet_dataset.py`
- **配置**：`conf/base/parameters.yaml`、新增 `conf/production/parameters.yaml`
- **測試**：現有 dataset node 測試更新 import 路徑，新增 Spark 版本測試
- **依賴**：PySpark 已在技術棧中，無新增依賴
- **相容性**：現有 pandas 行為完全保留（`backend: pandas` 為預設值），不影響 local 開發

## Why

Dataset Building Pipeline 在生產環境（128GB RAM）下執行全量資料（1000萬客戶 × 22 產品 × 12 個月）時，所有中間產物同時保留在記憶體中，造成記憶體壓力。同時，現有抽樣機制僅能從 label table 提取 key，缺乏以獨立客戶主檔進行彈性分層抽樣的能力。驗證集（val set）目前為全量且無法在訓練階段抽樣，在開發環境中拖慢迭代速度。

## What Changes

- **Runner 自動釋放記憶體**：每個 node 執行完畢後，自動釋放不再被下游 node 使用的 `MemoryDataset`，降低記憶體尖峰用量。新增 `dataset_released` structured log event。
- **Sample pool 獨立化**：`select_sample_keys` 的輸入從 `label_table` 改為獨立的 `sample_pool` table（ETL 產出的客戶主檔），包含 `cust_id`、`snap_date` 及分層欄位（如 `cust_segment_typ`）。**BREAKING**：pipeline 輸入變更，需重新產生假資料。
- **Val set 抽樣**：在 `prepare_model_input` 階段新增可選的 val set 抽樣（`val_sample_ratio` 參數），全量 val set 仍保留在磁碟上供完整評估使用，僅 numpy 轉換時抽樣以降低記憶體。

## Capabilities

### New Capabilities
- `catalog-memory-release`: Runner 自動偵測並釋放不再需要的 MemoryDataset，基於 pipeline DAG 的最後消費者分析
- `sample-pool`: 獨立的抽樣池 table 支援，取代從 label table 提取抽樣 key 的方式
- `val-sampling`: prepare_model_input 階段的 val set 可選抽樣機制

### Modified Capabilities

（無既有 spec 需修改）

## Impact

- **核心框架**：`core/runner.py`（記憶體釋放邏輯）、`core/catalog.py`（MemoryDataset.release()）、`core/logging.py`（新 log 欄位）
- **Dataset Building Pipeline**：`pipelines/dataset/nodes_pandas.py`、`nodes_spark.py`、`pipeline.py`（sample pool 輸入變更 + val 抽樣）
- **Config**：`catalog.yaml`（新增 sample_pool dataset）、`parameters_dataset.yaml`（新增 val_sample_ratio）
- **假資料**：`scripts/generate_synthetic_data.py`（產出 sample_pool.parquet）
- **測試**：需更新 dataset pipeline 相關測試，新增 memory release 測試

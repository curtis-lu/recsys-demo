## Why

Dataset building pipeline 需要增強以支援更靈活的抽樣策略、正確的三組資料集切分（train / train-dev / val），以及更好的可觀測性。目前 `select_sample_keys` 的分層抽樣欄位寫死為 `snap_date`，無法依客群分層；`split_keys` 只產出兩組資料集（train / val），缺少訓練過程中用於調參的 train-dev 集；category_mappings 埋在 preprocessor.pkl 中無法直接檢視。

## What Changes

- **select_sample_keys** 的 group by 欄位改為由 YAML 設定 (`sample_group_keys`)，支援多欄位分層抽樣（如 `["snap_date", "cust_segment_typ"]`）
- **split_keys** 從兩組切分改為三組切分：train（時間內、抽樣）、train-dev（時間外、抽樣）、val（時間外、全量），三組 snap_dates 互不重疊
- **prepare_model_input** 接受三組資料集，並將 `category_mappings` 另存為獨立 JSON 檔案
- 新增 **JSONDataset** I/O 類別，用於儲存 category_mappings
- 更新合成資料產生器，label_table 新增 `cust_segment_typ` 欄位
- 更新 YAML 設定：新增 `sample_group_keys`、`train_dev_snap_dates`

## Capabilities

### New Capabilities
- `json-dataset`: JSONDataset I/O 類別，支援 JSON 格式的讀寫，用於 category_mappings 等結構化資料的持久化與檢視

### Modified Capabilities
- `dataset-nodes`: select_sample_keys 支援 YAML 設定的 group by 欄位；split_keys 改為三組切分；prepare_model_input 處理三組資料集並輸出 category_mappings
- `dataset-pipeline`: Pipeline 從 5 nodes 改為 7 nodes，新增 train-dev 資料集的建構節點，category_mappings 作為獨立輸出
- `synthetic-data`: label_table 新增 cust_segment_typ 欄位
- `data-catalog`: 註冊 JSONDataset 類別，新增 category_mappings catalog 條目

## Impact

- **程式碼**：`nodes.py`, `pipeline.py`, `generate_synthetic_data.py`, `catalog.py`, `io/__init__.py`
- **設定檔**：`parameters_dataset.yaml`, `catalog.yaml`
- **新增檔案**：`io/json_dataset.py`
- **測試**：`test_nodes.py` 所有測試需更新 fixtures 和 assertions
- **下游影響**：training pipeline 需改用 `X_train_dev` / `y_train_dev` 作為訓練過程驗證集，`X_val` / `y_val` 作為最終評估集

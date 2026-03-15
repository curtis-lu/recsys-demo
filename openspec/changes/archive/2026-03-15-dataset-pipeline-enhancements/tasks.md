## 1. 合成資料更新

- [x] 1.1 更新 `scripts/generate_synthetic_data.py`：label_table 新增 `cust_segment_typ` 欄位（mass/affluent/hnw，依客戶 index 決定）
- [x] 1.2 執行 `python scripts/generate_synthetic_data.py` 重新產生 parquet 檔案

## 2. JSONDataset I/O 類別

- [x] 2.1 新增 `src/recsys_tfb/io/json_dataset.py`：實作 JSONDataset（load/save/exists），save 時 indent=2 並自動建立父目錄
- [x] 2.2 在 `src/recsys_tfb/io/__init__.py` 加入 JSONDataset 匯出
- [x] 2.3 在 `src/recsys_tfb/core/catalog.py` 的 `_DATASET_REGISTRY` 中註冊 JSONDataset

## 3. 設定檔更新

- [x] 3.1 更新 `conf/base/parameters_dataset.yaml`：新增 `sample_group_keys`、`train_dev_snap_dates`，保留 `val_snap_dates`
- [x] 3.2 更新 `conf/base/catalog.yaml`：新增 `category_mappings` 條目（type: JSONDataset）
- [x] 3.3 更新 `conf/local/catalog.yaml`：同步新增 `category_mappings` 條目（如有需要）

## 4. Dataset Nodes 修改

- [x] 4.1 修改 `select_sample_keys`：從 parameters 讀取 `sample_group_keys`，以其做 groupby 分層抽樣，輸出仍為 `["snap_date", "cust_id"]`
- [x] 4.2 修改 `split_keys`：新增 `label_table` 參數，三組切分（train / train_dev / val），val 從完整 label_table 取全量 keys
- [x] 4.3 修改 `prepare_model_input`：接受三組資料集（train_set, train_dev_set, val_set），`cust_segment_typ` 加入 drop_cols，回傳 category_mappings 作為獨立輸出

## 5. Pipeline 定義更新

- [x] 5.1 更新 `src/recsys_tfb/pipelines/dataset/pipeline.py`：split_keys 輸入新增 label_table，輸出改為三組 keys；新增 build_train_dev_dataset node；prepare_model_input 輸入輸出對應更新

## 6. 測試更新

- [x] 6.1 更新 `tests/test_pipelines/test_dataset/test_nodes.py` 的 `label_table` fixture：新增 `cust_segment_typ` 欄位
- [x] 6.2 更新 `parameters` fixture：新增 `sample_group_keys` 和 `train_dev_snap_dates`
- [x] 6.3 更新 `TestSelectSampleKeys`：新增多欄位分層抽樣測試
- [x] 6.4 更新 `TestSplitKeys`：改為三組切分測試，驗證 val 為全量、日期互不重疊
- [x] 6.5 更新 `TestPrepareModelInput`：三組資料集、cust_segment_typ 排除、category_mappings 獨立輸出
- [x] 6.6 新增 JSONDataset 測試（`tests/test_io/test_json_dataset.py`）
- [x] 6.7 執行全部測試確認通過

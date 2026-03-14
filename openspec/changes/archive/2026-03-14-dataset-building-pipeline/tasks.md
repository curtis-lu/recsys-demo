## 1. 合成假資料

- [x] 1.1 建立 `scripts/generate_synthetic_data.py`，產生 feature_table.parquet 和 label_table.parquet 到 `data/` 目錄，schema 對齊 SQL ETL 產出（feature: snap_date, cust_id, total_aum, fund_aum, in_amt_sum_l1m, out_amt_sum_l1m, in_amt_ratio_l1m, out_amt_ratio_l1m；label: snap_date, cust_id, apply_start_date, apply_end_date, label, prod_name），使用固定 random seed，3 個 snap_date，每個 200 客戶，5 個產品類別，label=1 約 10%
- [x] 1.2 執行 script 產生 Parquet 檔案並驗證 schema 正確

## 2. Dataset Building Nodes

- [x] 2.1 建立 `src/recsys_tfb/pipelines/dataset/nodes.py`，實作 `select_sample_keys(label_table, parameters)` — 依 snap_date 分層抽樣，回傳 (snap_date, cust_id) unique keys DataFrame
- [x] 2.2 實作 `split_keys(sample_keys, parameters)` — 依 val_snap_dates 參數做時間切分，回傳 dict {"train_keys": df, "val_keys": df}
- [x] 2.3 實作 `build_dataset(keys, feature_table, label_table)` — join keys 與 features/labels，回傳完整特徵 + label DataFrame
- [x] 2.4 實作 `prepare_model_input(train_set, val_set, parameters)` — 轉換為 X_train, y_train, X_val, y_val arrays + preprocessor dict，prod_name 做 category encoding

## 3. Pipeline 定義與設定

- [x] 3.1 更新 `src/recsys_tfb/pipelines/dataset/pipeline.py`，組裝 nodes 為 Pipeline（正確的 inputs/outputs 接線）
- [x] 3.2 更新 `src/recsys_tfb/pipelines/dataset/__init__.py` 匯出 create_pipeline
- [x] 3.3 新增 `conf/base/parameters_dataset.yaml`（sample_ratio, val_snap_dates, random_seed 等參數）
- [x] 3.4 更新 `conf/base/catalog.yaml` 和 `conf/local/catalog.yaml`，新增 preprocessor 路徑

## 4. 測試

- [x] 4.1 建立 `tests/test_pipelines/test_dataset/test_nodes.py`，測試每個 node 函數（抽樣比例、時間切分正確性、join 結果、輸出格式）
- [x] 4.2 建立 `tests/test_pipelines/test_dataset/test_pipeline.py`，測試 pipeline 定義（節點數量、inputs/outputs 正確性）
- [x] 4.3 執行全部測試確認通過（含既有的 38 個框架測試）

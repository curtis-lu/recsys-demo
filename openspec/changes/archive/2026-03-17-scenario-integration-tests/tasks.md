## 1. 前置修正

- [x] 1.1 修正 `scripts/promote_model.py` 的 `REQUIRED_ARTIFACTS`：`"model.pkl"` → `"model.txt"`

## 2. 資料產生模組

- [x] 2.1 建立 `tests/scenarios/__init__.py`
- [x] 2.2 建立 `tests/scenarios/data_generator.py`：實作 `generate_feature_table(rng, snap_dates, num_customers, extra_columns)` 函式
- [x] 2.3 在 `data_generator.py` 實作 `generate_label_table(rng, snap_dates, num_customers, products)` 函式
- [x] 2.4 定義模組常數：`BASE_SNAP_DATES`（6 個月）、`BASE_PRODUCTS`（5 個）、`EXTENDED_PRODUCTS`（7 個）、`NUM_CUSTOMERS`

## 3. 共用 Fixtures 與 Helpers

- [x] 3.1 建立 `tests/scenarios/conftest.py`：實作 `setup_workdir(scenario_name, feature_table, label_table, config_overrides)` helper 函式
- [x] 3.2 在 `conftest.py` 實作 `run_pipeline(work_dir, pipeline_name, env_name)` helper — subprocess 呼叫 CLI
- [x] 3.3 在 `conftest.py` 實作 `promote_model(work_dir)` helper — 呼叫 promote_model.py 並指定 --models-dir
- [x] 3.4 在 `conftest.py` 實作 `generate_report(scenario_name, work_dir, output_path)` helper — 讀取 pipeline 產出並產生繁體中文驗證報告

## 4. 情境 1：推論新一週的資料

- [x] 4.1 建立 `tests/scenarios/test_scenario_1_new_inference.py`
- [x] 4.2 實作 test function：setup 工作目錄（base 6 個月資料、training 加速設定、inference snap_dates=["2024-04-30"]）
- [x] 4.3 執行 dataset → training → promote → inference 全流程
- [x] 4.4 Assert：ranked_predictions snap_date = 2024-04-30、每客戶 5 產品、排名 1~5、客戶數 = 200、輸出路徑含 model hash
- [x] 4.5 產生驗證報告至 `tests/scenarios/output/scenario_1/report.txt`

## 5. 情境 2：訓練視窗前移

- [x] 5.1 建立 `tests/scenarios/test_scenario_2_shift_window.py`
- [x] 5.2 實作 test function：setup 工作目錄（base 6 個月資料、train_dev=["2024-03-31"]、val=["2024-04-30"]）
- [x] 5.3 執行 dataset → training
- [x] 5.4 Assert：train_set snap_dates 不含 03-31/04-30、train_dev snap_dates = [03-31]、val snap_dates = [04-30]、各 split 行數 > 0、model.txt 存在
- [x] 5.5 產生驗證報告至 `tests/scenarios/output/scenario_2/report.txt`

## 6. 情境 3：新增特徵欄位

- [x] 6.1 建立 `tests/scenarios/test_scenario_3_new_features.py`
- [x] 6.2 實作 test function：setup 工作目錄（extra_columns=True 的 feature_table、base label_table）
- [x] 6.3 執行 dataset → training → promote → inference
- [x] 6.4 Assert：X_train 含 txn_count_l1m/avg_txn_amt_l1m、preprocessor.feature_columns 含新欄、scoring_dataset 含新欄、ranked_predictions 行數正確
- [x] 6.5 產生驗證報告至 `tests/scenarios/output/scenario_3/report.txt`

## 7. 情境 4：新增產品

- [x] 7.1 建立 `tests/scenarios/test_scenario_4_new_products.py`
- [x] 7.2 實作 test function：setup 工作目錄（7 產品 label_table、inference products 含 ploan/mloan）
- [x] 7.3 執行 dataset → training → promote → inference
- [x] 7.4 Assert：category_mappings 含 7 產品、train_set prod_name 唯一值 = 7、每客戶 7 產品排名、排名 1~7、prod_code 唯一值 = 7
- [x] 7.5 產生驗證報告至 `tests/scenarios/output/scenario_4/report.txt`

## 8. 收尾

- [x] 8.1 更新 `.gitignore` 加入 `tests/scenarios/output/`
- [x] 8.2 逐一執行 `pytest tests/scenarios/ -v -s` 確認全部通過
- [x] 8.3 檢視 4 份驗證報告確認內容完整可讀

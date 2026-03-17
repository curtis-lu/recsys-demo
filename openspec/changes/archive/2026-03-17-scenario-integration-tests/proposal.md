## Why

目前 pipeline 缺乏端對端的情境測試，無法在程式變更後快速驗證營運常見操作（推論新資料、調整訓練視窗、新增欄位/產品）是否正常運作。需要建立一套可重複執行、自動化判定且產出可人工檢視報告的情境測試框架，以確保 pipeline 在實際營運情境下的正確性。

## What Changes

- 新增擴展合成資料產生模組，支援多個 snap_dates（6 個月）、可選額外特徵欄位、可選額外產品
- 新增 4 個 pytest 情境測試，各自驗證一種營運情境：
  1. 推論新一週的資料（新 snap_date）
  2. 訓練期間往前挪移一個月（調整 train_dev/val dates）
  3. 新增特徵欄位（feature_table schema 變更）
  4. 新增產品 ploan, mloan（label_table 產品擴充）
- 新增共用 pytest fixtures，負責工作目錄隔離、設定檔組裝、pipeline 執行
- 新增繁體中文驗證報告產生器，供人工事後檢視
- **修正** `scripts/promote_model.py` 中 `REQUIRED_ARTIFACTS` 的 `model.pkl` → `model.txt`（對應 LightGBMDataset 實際輸出）

## Capabilities

### New Capabilities
- `scenario-data-generator`: 擴展合成資料產生模組，支援多 snap_dates、額外欄位、額外產品等變體
- `scenario-test-fixtures`: pytest 共用 fixtures，提供工作目錄隔離、設定覆蓋、pipeline 執行、model promote 封裝
- `scenario-validation-report`: 繁體中文驗證報告產生器，讀取 pipeline 產出並輸出結構化摘要
- `scenario-new-inference`: 情境 1 測試 — 驗證推論新 snap_date 的正確性
- `scenario-shift-window`: 情境 2 測試 — 驗證訓練視窗前移後 data split 的正確性
- `scenario-new-features`: 情境 3 測試 — 驗證新增特徵欄位全 pipeline 通過
- `scenario-new-products`: 情境 4 測試 — 驗證新增產品後 category_mappings 和推論的正確性

### Modified Capabilities
- `model-promotion`: 修正 `REQUIRED_ARTIFACTS` 中的 `model.pkl` → `model.txt`

## Impact

- **新增檔案**：`tests/scenarios/` 目錄下約 7 個檔案（conftest.py、data_generator.py、4 個 test 檔、__init__.py）
- **修改檔案**：`scripts/promote_model.py`（1 行修正）、`.gitignore`（加入 `tests/scenarios/output/`）
- **不影響**：現有 `conf/`、`data/`、pipeline 程式碼、既有測試
- **執行時間**：全部情境約 5-10 分鐘（每個情境用 n_trials=3, num_iterations=100 加速訓練）
- **依賴**：無新增外部依賴，使用現有 pytest + pandas + lightgbm

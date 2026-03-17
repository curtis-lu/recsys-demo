## ADDED Requirements

### Requirement: 產生繁體中文驗證報告
`conftest.py` 或獨立 helper SHALL 提供 `generate_report(scenario_name, work_dir, output_path)` 函式，讀取 pipeline 產出並產生結構化繁體中文報告。

#### Scenario: 產生含 dataset pipeline 摘要的報告
- **WHEN** 工作目錄含已完成的 dataset pipeline 產出
- **THEN** 報告包含：dataset 版本、各 split（train/train_dev/val）行數與欄位數、snap_dates、X_train 形狀、preprocessor feature_columns、category_mappings

#### Scenario: 產生含 training pipeline 摘要的報告
- **WHEN** 工作目錄含已完成的 training pipeline 產出
- **THEN** 報告包含：model 版本、best_params、overall_map、per_product_ap

#### Scenario: 產生含 inference pipeline 摘要的報告
- **WHEN** 工作目錄含已完成的 inference pipeline 產出
- **THEN** 報告包含：推論路徑、ranked_predictions 行數、唯一客戶數、唯一產品數、snap_dates、分數範圍、排名範圍、前 10 筆樣本

### Requirement: 報告輸出至檔案
報告 SHALL 同時輸出至 stdout 和指定的 `report.txt` 檔案路徑。

#### Scenario: 報告寫入 report.txt
- **WHEN** 呼叫 `generate_report(scenario_name, work_dir, output_path="tests/scenarios/output/scenario_1/report.txt")`
- **THEN** 報告內容寫入該檔案，可供事後人工檢視

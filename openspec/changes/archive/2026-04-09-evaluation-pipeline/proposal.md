## Why

目前 evaluation 功能散落在 `src/recsys_tfb/evaluation/` 模組與 `scripts/evaluate_model.py` script 中，未納入 pipeline registry 與 CLI 執行流程。這導致評估流程缺乏版本追蹤、無法透過統一 CLI 執行、不支援 Spark backend、且 baseline 計算與模型評估耦合在一起。需要將 evaluation 正式 pipeline 化，使其與 dataset/training/inference pipeline 風格一致。

## What Changes

- 新增 `evaluation` pipeline：讀取 inference 已產出的 `ranked_predictions`，join label_table，計算排名指標，產出 metrics.json 與 report.html
- 新增 `baselines` pipeline：獨立計算 popularity baseline，只依賴 snap_date，不綁 model_version
- 兩條 pipeline 都支援 pandas/spark 雙 backend
- 註冊到 pipeline registry，支援 `python -m recsys_tfb evaluation` 和 `--pipeline baselines` CLI 執行
- Evaluation report 內建可選的 baseline 比較（有 baseline artifact 就納入，沒有就只產模型報告）
- 新增 `conf/base/parameters_evaluation.yaml` 集中管理評估參數
- 新增 catalog entries，evaluation 產出路徑為 `data/evaluation/${model_version}/${snap_date}/`，baselines 為 `data/baselines/${snap_date}/`
- 刪除 `scripts/evaluate_model.py`，核心邏輯已在 evaluation modules 中，pipeline nodes 直接重用

## Capabilities

### New Capabilities
- `evaluation-pipeline`: 模型評估 pipeline，從 inference 結果計算排名指標並產出報告
- `baselines-pipeline`: 獨立的 baseline 計算 pipeline，產出 popularity baseline predictions 與 metrics

### Modified Capabilities
（無既有 spec 需要修改）

## Impact

- **新增檔案**：`src/recsys_tfb/pipelines/evaluation/`、`src/recsys_tfb/pipelines/baselines/`、`conf/base/parameters_evaluation.yaml`
- **修改檔案**：`src/recsys_tfb/pipelines/__init__.py`（registry）、`src/recsys_tfb/__main__.py`（CLI）、`conf/base/catalog.yaml`（新 entries）
- **刪除檔案**：`scripts/evaluate_model.py`
- **不影響**：training pipeline、inference pipeline 的程式碼與產出不會被修改，evaluation 只讀取 inference 的輸出
- **依賴關係**：evaluation pipeline 讀取 inference 的 `ranked_predictions`（透過 catalog），但不會改動 inference pipeline 本身。baselines pipeline 完全獨立，無跨 pipeline 依賴

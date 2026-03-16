## 1. Catalog 路徑更新

- [x] 1.1 修改 `conf/base/catalog.yaml` 中 model、preprocessor、best_params、evaluation_results、category_mappings 的 filepath 從 `data/models/` 改為 `data/models/best/`
- [x] 1.2 修改 `conf/production/catalog.yaml` 中對應的 HDFS 路徑到 `hdfs:///data/recsys/models/best/`
- [x] 1.3 執行現有測試確認不 break：`pytest tests/`

## 2. CLI 覆寫 Training Catalog 路徑

- [x] 2.1 在 `src/recsys_tfb/__main__.py` 的 `run()` 函式中，當 pipeline 為 training 時：產生時間戳、覆寫 catalog config 中 `models/best/` 路徑為 `models/{timestamp}/`、log 版本 ID
- [x] 2.2 新增 CLI 覆寫邏輯的測試：驗證 training 時路徑被覆寫、inference 時路徑不變

## 3. 版本比較 Node

- [x] 3.1 在 `src/recsys_tfb/pipelines/training/nodes.py` 新增 `compare_model_versions` 函式：掃描版本目錄、讀取 evaluation_results.json、log 比較報告、回傳結構化 dict
- [x] 3.2 在 `tests/pipelines/training/test_nodes.py` 新增 `compare_model_versions` 測試：多版本排名、單一版本、忽略非版本目錄、標示 current best

## 4. Training Pipeline 更新

- [x] 4.1 修改 `src/recsys_tfb/pipelines/training/pipeline.py`，在 log_experiment 之後新增 compare_model_versions Node
- [x] 4.2 更新 training pipeline 測試驗證新的 5 節點 DAG 結構

## 5. Promote Script

- [x] 5.1 建立 `scripts/promote_model.py`：支援指定版本或自動選最佳、驗證 artifacts 完整性、複製到 best/、輸出摘要、支援 --models-dir 參數
- [x] 5.2 建立 `tests/scripts/test_promote_model.py`：測試指定版本 promote、自動選最佳、版本不存在錯誤、artifacts 不完整錯誤

## 6. 端對端驗證

- [x] 6.1 執行 training pipeline 兩次，驗證兩個版本目錄並存、best/ 未被寫入、比較報告正確
- [x] 6.2 執行 promote script，驗證 best/ 目錄正確建立
- [x] 6.3 執行 inference pipeline，驗證從 best/ 正確載入模型（model 載入成功，preprocessor 尚未由 pipeline 產出）

## 1. 設定與模組結構

- [ ] 1.1 建立 `src/recsys_tfb/pipelines/inference/__init__.py`（空檔案）
- [ ] 1.2 建立 `conf/base/parameters_inference.yaml`，包含 snap_dates 與 products 列表（從合成資料的 label_table 查詢實際產品名稱）
- [ ] 1.3 在 `conf/base/catalog.yaml` 新增 scoring_dataset 和 ranked_predictions 的 ParquetDataset 定義

## 2. Inference Nodes 實作

- [ ] 2.1 實作 `build_scoring_dataset(feature_table, parameters)` — 篩選 snap_date、cross-join 產品列表、附加特徵欄位
- [ ] 2.2 實作 `apply_preprocessor(scoring_dataset, preprocessor)` — drop columns、categorical encoding、驗證 feature_columns 順序一致
- [ ] 2.3 實作 `predict_scores(model, X_score, scoring_dataset)` — 呼叫 model.predict、組裝 [snap_date, cust_id, prod_code, score] 輸出
- [ ] 2.4 實作 `rank_predictions(score_table, parameters)` — 按 (snap_date, cust_id) 分組、依 score 降序排名、輸出 [snap_date, cust_id, prod_code, score, rank]

## 3. Pipeline 定義與註冊

- [ ] 3.1 建立 `src/recsys_tfb/pipelines/inference/pipeline.py`，串接 4 個 nodes
- [ ] 3.2 在 `src/recsys_tfb/pipelines/__init__.py` 的 `_REGISTRY` 加入 `"inference"` 項目

## 4. 測試

- [ ] 4.1 建立 `tests/test_pipelines/test_inference/__init__.py`
- [ ] 4.2 建立 `tests/test_pipelines/test_inference/test_nodes.py` — 測試 4 個 node 函數（cross-join 正確性、preprocessor 套用一致性、分數範圍、排名唯一性）
- [ ] 4.3 建立 `tests/test_pipelines/test_inference/test_pipeline.py` — Pipeline 整合測試（節點數量、輸入輸出串接）

## 5. 端對端驗證

- [ ] 5.1 執行 `pytest tests/` 確認所有測試通過
- [ ] 5.2 執行 `python -m recsys_tfb -p inference -e local` 驗證完整 pipeline
- [ ] 5.3 檢查 `data/inference/ranked_predictions.parquet` 輸出正確性（欄位、分數範圍、排名連續性）

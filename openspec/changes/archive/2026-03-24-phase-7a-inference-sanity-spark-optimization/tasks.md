## 1. Inference Sanity Checks

- [x] 1.1 新建 `validation.py`，定義 `ValidationError` 例外類別
- [x] 1.2 在 `nodes_pandas.py` 新增 `validate_predictions` 函式（6 項 sanity checks）
- [x] 1.3 在 `nodes_spark.py` 新增 `validate_predictions` 函式（Spark 原生操作）
- [x] 1.4 在 `pipeline.py` 註冊 validate_predictions 節點（pandas/spark 雙後端）

## 2. Spark 優化 — Inference Pipeline

- [x] 2.1 `predict_scores` 改為按 (snap_date, prod_name) 雙欄位分片
- [x] 2.2 `predict_scores` 分開 select 特徵列和 identity 列，避免重複列處理
- [x] 2.3 `predict_scores` 注入 model_version 欄位（供分區輸出使用）
- [x] 2.4 `build_scoring_dataset` 移除 .count() actions
- [x] 2.5 `rank_predictions` 移除 .count() 和 .dropDuplicates().count() actions

## 3. Spark 優化 — Dataset Building Pipeline

- [x] 3.1 `select_sample_keys` 移除所有 .count() 呼叫
- [x] 3.2 `split_keys` 移除三次 .count() 呼叫
- [x] 3.3 `build_dataset` 移除 .count() 呼叫

## 4. ParquetDataset 分區寫入

- [x] 4.1 ParquetDataset 新增 `partition_cols` 建構式參數
- [x] 4.2 Spark 後端 save 使用 `partitionBy()`
- [x] 4.3 Pandas 後端 save 使用 pyarrow `write_to_dataset`
- [x] 4.4 更新 base catalog.yaml（新增 score_table、validated_predictions）
- [x] 4.5 更新 production catalog.yaml（新增分區輸出設定）

## 5. 測試

- [x] 5.1 新建 `test_validation.py`（12 個測試案例，含 pandas validate_predictions）
- [x] 5.2 新增 ParquetDataset partition_cols 測試（pandas + spark）
- [x] 5.3 更新 `test_pipeline.py`（node count 4→5、outputs 新增 validated_predictions）
- [x] 5.4 全部測試通過（382 passed）

## 6. 文件更新

- [x] 6.1 更新 CLAUDE.md — Phase 7 拆分為 7a/7b，更新 roadmap 表格

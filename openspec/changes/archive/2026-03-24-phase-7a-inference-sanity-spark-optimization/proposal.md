## Why

推論 pipeline 缺乏輸出品質驗證，異常分數或不完整結果可能直接進入下游行銷系統。同時，Spark 後端的 nodes 中有大量不必要的 `.count()` action 和粗粒度的 `toPandas()` 轉換，在 10M 客戶規模下造成效能瓶頸和記憶體壓力。ParquetDataset 也不支援分區寫入，無法滿足 production Hive table 的分區需求。

## What Changes

- 新增 `validate_predictions` pipeline 節點，對推論輸出執行 6 項 sanity checks（row_count_match、score_range、no_missing、completeness、rank_consistency、no_duplicates），失敗即拋出 `ValidationError` 中斷 pipeline
- Inference pipeline 的 `predict_scores` 改為按 `(snap_date, prod_name)` 雙欄位分片 toPandas，記憶體壓力降低約 22 倍
- 移除 Inference 和 Dataset Building pipeline Spark nodes 中所有不必要的 `.count()` action
- ParquetDataset 新增通用 `partition_cols` 參數，支援 pandas（pyarrow）和 Spark 分區寫入
- Production catalog 新增 `score_table` 和 `validated_predictions` 的分區輸出設定

## Capabilities

### New Capabilities
- `inference-validation`: 推論輸出 sanity checks（6 項驗證 + ValidationError + pandas/spark 雙實作）

### Modified Capabilities
- `spark-inference-nodes`: predict_scores 改為 (snap_date, prod_name) 分片、移除 .count() actions
- `spark-dataset-nodes`: 移除所有不必要的 .count() actions
- `io-datasets`: ParquetDataset 新增 partition_cols 分區寫入支援
- `inference-pipeline`: 新增 validate_predictions 節點，pipeline 從 4 個節點增為 5 個

## Impact

- **修改檔案**：`nodes_spark.py`（inference + dataset）、`nodes_pandas.py`（inference）、`pipeline.py`（inference）、`parquet_dataset.py`、`catalog.yaml`（base + production）、`CLAUDE.md`
- **新增檔案**：`validation.py`、`test_validation.py`
- **向後相容**：`partition_cols` 預設為 None，不影響現有行為。inference pipeline 輸出從 `ranked_predictions` 延伸為 `validated_predictions`

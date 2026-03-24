## Context

Phase 6 完成後，推論 pipeline 可正常產出排序結果，但缺乏輸出驗證機制。Spark 後端在 10M 客戶 x 22 產品的規模下，多處 `.count()` action 和粗粒度 `toPandas()` 造成效能與記憶體問題。Production 環境需要以 Hive partition 格式輸出推論結果。

**限制條件**：PySpark 3.3.2、生產環境禁止 UDF、無網路、CPU-only（4 core, 128GB RAM）。

## Goals / Non-Goals

**Goals:**

- 推論輸出品質驗證，異常時中斷 pipeline
- 減少 Spark 後端不必要的 action 開銷
- 降低 `predict_scores` 的每次 toPandas 記憶體壓力
- ParquetDataset 支援分區寫入（通用能力）

**Non-Goals:**

- ModelAdapter 抽象化（Phase 7b）
- Probability calibration（Phase 7b）
- 分佈漂移偵測（PSI/KS 等進階監控）
- Dataset Building pipeline 的 Spark 節點重構（僅移除 .count()）

## Decisions

### 1. validate_predictions 作為獨立 pipeline 節點（非嵌入式驗證）

**選擇**：新增獨立節點，放在 `rank_predictions` 之後，pass-through 模式返回原始 DataFrame。

**替代方案**：在 `predict_scores` 和 `rank_predictions` 內部加入 assert。

**理由**：符合 Kedro 設計哲學（分離邏輯與驗證），可獨立測試，未來可配置跳過。

### 2. predict_scores 按 (snap_date, prod_name) 雙欄位分片

**選擇**：從原本的 snap_date 單欄位分片改為 (snap_date, prod_name) 雙欄位。

**替代方案**：一次全量 toPandas（簡潔但 OOM 風險高）；可配置 batch_size（靈活但過度工程）。

**理由**：10M 客戶 / 22 產品 ≈ 每片 45 萬筆，在 128GB RAM 下安全。分片鍵與業務語義對齊，不需額外參數。

### 3. 全面移除 Spark nodes 中的 .count()

**選擇**：移除所有非驗證用途的 `.count()` 呼叫，包括日誌用途。

**替代方案**：保留但改為可配置 debug_mode；保留關鍵 .count()。

**理由**：`.count()` 是昂貴的 Spark action，每次呼叫觸發完整 DAG 計算。日誌資訊可從 Spark UI 或下游節點取得。

### 4. ParquetDataset partition_cols 作為通用能力

**選擇**：在 ParquetDataset 建構式新增 `partition_cols` 參數，pandas 用 pyarrow、Spark 用 `partitionBy()`。

**替代方案**：新建 HiveDataset adapter（更多代碼維護）。

**理由**：DataCatalog 已使用 `cls(**entry)` 建立 dataset，YAML 中的額外參數自動傳遞，不需修改 catalog.py。

### 5. 失敗策略：Error + 中斷執行

**選擇**：任何 sanity check 失敗即拋出 `ValidationError`，中斷 pipeline。

**替代方案**：Warning + 繼續；可配置每項 check 的嚴重程度。

**理由**：推論結果直接影響行銷決策，不可靠的結果不應進入下游。簡單明確的失敗策略優於複雜的配置。

## Risks / Trade-offs

- **[Spark validate_predictions 中的 .count()]** validate_predictions 節點需要 `.count()` 來比對 row count 和檢查完整性。這是驗證必要的 action，無法避免。 → 已接受，驗證正確性優先於效能。
- **[monotonically_increasing_id 不安全]** 曾考慮用 `monotonically_increasing_id` 對齊 identity 和 features，但多分區下不保證順序。 → 改用分開 select + toPandas 對齊，犧牲少量效能換取正確性。
- **[production catalog 的 model_version 分區欄位]** DataFrame 中原本不含 `model_version`，需要在 Spark 版 predict_scores 中注入。 → 僅在 parameters 包含 model_version 時注入，不影響 pandas 後端。


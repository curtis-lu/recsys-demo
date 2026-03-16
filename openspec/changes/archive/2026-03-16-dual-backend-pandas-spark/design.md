## Context

目前所有 pipeline node 函數只有 pandas 實作。生產環境的 `conf/production/catalog.yaml` 設定 `backend: spark`，使 `ParquetDataset.load()` 回傳 `pyspark.sql.DataFrame`，與 node 函數預期的 `pd.DataFrame` 不相容。

生產環境的 feature_table / label_table 可能超過 128GB，無法全量 `.toPandas()` 轉換。Inference pipeline 處理 10M 客戶 × 22 產品 × 500 特徵，更不可能放入 pandas。

現有架構的資料流：`__main__.py` → `get_pipeline(name)` → `create_pipeline()` → Runner 用 `catalog.load()` 取資料 → 呼叫 node 函數。Catalog 已根據 YAML 配置回傳不同型別，但 node 函數無法處理 Spark DataFrame。

## Goals / Non-Goals

**Goals:**
- 使用者可透過 `parameters.yaml` 的 `backend` 參數選擇 pandas 或 Spark 處理
- 分開兩套 node 實作（`nodes_pandas.py` + `nodes_spark.py`），各自乾淨獨立
- Pipeline 接線（Node 定義）不變，差異僅在 node 函數內部實作
- 現有 pandas 行為完全保留（`backend: pandas` 為預設值）
- Spark 版 `prepare_model_input` 在抽樣 + split 後的小量資料上做 `.toPandas()` 轉換，輸出 pandas/numpy 給 LightGBM

**Non-Goals:**
- Training pipeline 不做 Spark 版本（LightGBM 硬性要求 pandas/numpy，輸入來自 PickleDataset）
- 不建立 DataFrame 抽象層或 adapter pattern（兩套實作更清晰直接）
- 不修改 Catalog 的 load/save 介面

## Decisions

### 1. 配置驅動而非自動偵測

**決策**：在 `parameters.yaml` 新增頂層 `backend` 參數，`create_pipeline(backend)` 根據此值選擇 import 哪套 node。

**替代方案**：讓 node 函數用 `isinstance()` 自動偵測輸入型別。
**捨棄原因**：自動偵測在每個函數中加入 if/else 分支，破壞程式碼清晰度，且無法預先確認整條 pipeline 的型別一致性。

### 2. 兩套檔案而非雙模函數

**決策**：`nodes_pandas.py` + `nodes_spark.py` 分開兩個檔案，函數簽名完全一致。

**替代方案**：單一檔案中每個函數內部 if/else 分流。
**捨棄原因**：混合兩套 API 使每個函數都變成兩倍長，難以獨立測試和維護。分開檔案可各自有專屬的 import、測試、review。

### 3. `prepare_model_input` 的轉換策略

**決策**：Spark 版 `prepare_model_input` 開頭對三個 Spark DF 呼叫 `.toPandas()`，之後邏輯與 pandas 版完全相同。

**理由**：此時資料已經過 `select_sample_keys`（抽樣）和 `split_keys`（split），量級可控。LightGBM 需要 pandas DataFrame / numpy array 作為輸入，這是無法避免的轉換點。將轉換放在最後一個 dataset node（而非 training pipeline 入口）可保持 training pipeline 完全不受影響。

### 4. Inference `predict_scores` 分塊預測

**決策**：按 `snap_date` 分塊，每塊 `.toPandas()` → `model.predict()` → 收集結果 → `spark.createDataFrame()`。若單一 snap_date 太大，再按 `cust_id` hash 分桶。

**理由**：LightGBM `predict()` 需要 pandas/numpy。10M × 22 = 2.2 億行無法一次轉換。按 snap_date 分塊是自然分割（每週推論通常只有 1 個日期），再按 hash 分桶可進一步控制記憶體用量。

### 5. 呼叫鏈修改：先載入 config 再建 pipeline

**決策**：`__main__.py` 中調整順序為：載入 config → 取 `backend` → `get_pipeline(name, backend)` → 建 catalog → 執行。

**理由**：現有 `get_pipeline` 不接受參數，需擴展。pipeline 建立需要知道 backend 才能選擇正確的 node 模組。

### 6. `ParquetDataset.save()` 自動型別轉換

**決策**：save 時偵測傳入資料型別，若與 backend 不符則自動轉換。

**理由**：`prepare_model_input`（Spark 版）的上游 node 產出 Spark DF，下游 catalog 可能設為 `backend: spark`。但 `prepare_model_input` 自身輸出 pandas（存到 PickleDataset，不受影響）。防禦性轉換確保邊界情況不會 crash。

### 7. Spark 分層抽樣用 Window 函數

**決策**：`select_sample_keys` 的 Spark 版用 `Window.partitionBy(*group_keys).orderBy(F.rand(seed))` + `row_number()` 實作分層抽樣。

**替代方案**：`sampleBy()` — 只支援單一 column。`df.sample(fraction)` — 非分層。
**捨棄原因**：Window 方案支援多 group key、精確控制每組比例，且可透過 `F.rand(seed)` 維持可重現性。

## Risks / Trade-offs

- **`F.rand(seed)` 跨 Spark 版本可重現性**：`F.rand` 的隨機序列可能因 Spark 版本或 partition 數不同而改變。→ 接受此限制；生產環境 Spark 版本固定（3.3.2）。
- **`prepare_model_input` .toPandas() OOM 風險**：若 `sample_ratio` 設太高且資料量巨大，轉換可能 OOM。→ 使用者自行控制 `sample_ratio`；文件中說明此約束。
- **兩套程式碼維護成本**：新增 feature 時需同步修改兩套 node。→ 兩套函數簽名一致，測試覆蓋兩套，降低遺漏風險。
- **Inference 分塊預測的效能**：逐塊 `.toPandas()` + `createDataFrame` 有序列化開銷。→ 分塊數量有限（通常 1-20 塊），可接受。

## Context

目前 evaluation 功能由 `src/recsys_tfb/evaluation/` 模組提供核心邏輯（metrics、baselines、report、distributions 等），`scripts/evaluate_model.py` 作為 CLI 入口串接這些模組。所有 evaluation 程式碼為 pandas-only，無 Spark 支援。

專案已有 dataset/training/inference 三條 pipeline，使用自建 Kedro-inspired 框架（Node/Pipeline/Runner），透過 `get_pipeline(name, backend)` registry 與 `__main__.py` CLI 執行。Inference pipeline 產出 `ranked_predictions.parquet`（含 snap_date, cust_id, prod_name, score, rank），已經過 6 項驗證。

## Goals / Non-Goals

**Goals:**
- 新增 `evaluation` 和 `baselines` 兩條 pipeline，風格與既有 pipeline 一致
- 重用 inference 已產出的 `ranked_predictions`，不重跑推論
- 支援 pandas/spark 雙 backend
- Spark backend 使用 **Spark SQL 全程計算排名指標**（AP、nDCG、MRR、Precision@K、Recall@K），不 collect 大表到 driver
- Pandas backend 重用既有 `evaluation/` 模組的核心計算邏輯
- Evaluation 產出對應 `model_version` + `snap_date`，baselines 只對應 `snap_date`
- Evaluation report 可選整合 baseline 比較

**Non-Goals:**
- 不修改 training/inference pipeline 的任何程式碼或產出
- 不新增 evaluation_version hash
- 不支援多 snap_date 批次處理
- 不新增獨立 compare pipeline（compare 功能內建於 evaluation report）

## Decisions

### 1. Spark 策略：全程 Spark SQL 指標計算

**選擇**：`nodes_spark.py` 使用 Spark SQL 實作完整的排名指標計算。`nodes_pandas.py` 保留重用現有 `compute_all_metrics` 等函數。兩個 backend 各自獨立實作，結果一致。

**Spark SQL 計算流程**：
1. Join ranked_predictions + label_table → 帶 label 的完整表
2. Window functions 計算 per-row 指標貢獻：
   - `ROW_NUMBER() OVER (PARTITION BY snap_date, cust_id ORDER BY score DESC)` → 排名位置
   - `SUM(label) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` → 累計相關數
   - `cumulative_relevant / position` → precision@position
   - `label / LOG2(position + 1)` → DCG 貢獻
3. 逐 query 聚合（per snap_date, cust_id）：
   - `AVG(CASE WHEN label = 1 THEN precision_at_pos END)` → AP
   - `SUM(dcg_contribution) / ideal_dcg` → nDCG
   - `1.0 / MIN(CASE WHEN label = 1 THEN position END)` → MRR
   - 以及 @K 版本（加 `WHERE position <= K` 條件）
4. 再按 product/segment/overall 維度聚合 → 直接得到最終指標

**理由**：Spark SQL 語法清晰易讀，window functions 天然適合排名指標計算，且無需 collect 大表。複雜邏輯用 SQL 表達比 DataFrame API 更直觀。

### 2. 節點拆分：三節點架構

- `prepare_eval_data`：join predictions + labels + segments → 輸出 `eval_predictions`
- `compute_metrics`：計算指標 → 輸出 `evaluation_metrics` (JSON)
- `generate_report`：產出 HTML report → 輸出 `evaluation_report`

**理由**：職責清晰，中間產物可獨立檢視。

### 3. Baselines 雙 backend

- `nodes_spark.py`：用 Spark SQL groupby 計算 per-product / per-segment popularity rate，cross join 客戶列表產出 baseline predictions
- `nodes_pandas.py`：重用現有 `baselines.py` 的 `generate_global_popularity_baseline` / `generate_segment_popularity_baseline`

### 4. Baseline 解耦

`baselines` 為完全獨立 pipeline。`evaluation` 的 `generate_report` 節點可選讀取 baseline artifacts：有 baseline_metrics 就納入報告比較，沒有就只產模型報告。

**實作**：`generate_report` 的 baseline 輸入在 catalog 中標記為可選（檔案不存在時 load 為 None）。

### 5. CLI 版本解析

- `evaluation`：需要 `model_version`（預設跟 `data/models/best` symlink）+ `snap_date`（從 `parameters_evaluation.yaml`）。Runtime params: `model_version`、`dataset_version`、`snap_date`。
- `baselines`：只需要 `snap_date`（從 `parameters_evaluation.yaml`）。Runtime params 只含 `snap_date`。

### 6. 刪除 evaluate_model.py script

Pipeline 化完成後刪除 `scripts/evaluate_model.py`，核心邏輯在 `evaluation/` 模組中，pipeline nodes 直接重用。

### 7. 對其他 pipeline 的影響

**不修改** training/inference pipeline 的任何程式碼。Evaluation 透過 catalog 讀取 inference 的 `ranked_predictions`（同一個 catalog entry，純讀取）。Pipeline registry 新增 entries 不影響既有 pipeline。CLI 新增分支不影響既有分支。

### 8. 參數化設計原則

所有可配置項 SHALL 集中在 `parameters_evaluation.yaml`，避免 hard-coded 值：
- 指標相關：`k_values`、metric 類型清單
- 分段相關：`segment_columns`、`segment_sources`（外部 Parquet 路徑與 join key）
- Baseline 相關：`baseline_type`、`baseline_segment_column`、`lookback_months`（歷史回溯期間）
- Report 相關：`include_baseline_comparison`、`include_calibration`、`include_distributions`、`n_calibration_bins`
- Schema 欄位名：繼續沿用 `parameters.yaml` 的 `schema.columns`（透過 `get_schema()`），不在 nodes 中 hard-code 欄位名
- Snap date：由 `parameters_evaluation.yaml` 設定，不寫死在程式碼中

## Risks / Trade-offs

- **[Risk] Spark SQL 指標與 pandas 版結果一致性** → 需要 cross-validation 測試，對同一份資料兩個 backend 的指標輸出必須在容許誤差內一致
- **[Risk] baseline_metrics 可選載入機制** → DataCatalog 可能不支援「檔案不存在返回 None」。需確認 catalog 實作，必要時在節點中用 try/except 或 catalog.exists() 處理
- **[Trade-off] 兩套指標實作的維護成本** → Spark SQL 和 pandas 各自獨立實作排名指標，需確保同步更新。但這與 inference pipeline 的雙 backend 模式一致，是專案既有做法
- **[Trade-off] Spark SQL 可讀性** → AP/nDCG 的 SQL 表達較長，但比 DataFrame API 的鏈式呼叫更易讀。可考慮用 CTE 拆分步驟

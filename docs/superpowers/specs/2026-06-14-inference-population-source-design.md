# Inference 推論母體來源表設計（`inference_population`）

- 日期：2026-06-14
- 狀態：設計待 review
- 主題：為 inference 建立明確的「推論母體」來源表，把「誰該被推論（membership）」與「他們有什麼特徵（feature enrichment）」分開

## 背景與問題

inference 目前**沒有明確的推論母體**。`build_scoring_dataset` 用
`feature_table.filter(snap_dates).select(join_key).dropDuplicates()` 隱式地把
feature_table 的客戶集合當成母體，再 cross-join 候選 item、left-join 把特徵接回來。

這有三個結構性問題：

1. **membership 與 enrichment 被混在同一張表**。「誰該被推論」是業務決策（本週在範圍內、未流失、符合資格的客戶），不該等同於「誰剛好有 feature 列」。
2. **母體 grain 在 source_etl 產出階段沒有受到保證**。沒有任何 invariant（`consistency.py` A1–A14、資料閘 B1/B5）保證 feature_table 對 `(time, entity)` 唯一。
3. **兩個 silent bug**：
   - feature ETL 漏了某客戶 → 他無聲地不在推論母體內，不報錯。
   - feature_table 有重複 `(time, entity)` 列 → 經 `how="left"` join 被**重複評分**（`dropDuplicates` 只去重 cross-join 左邊，join 仍會 fan-out，所以現行去重是**不完整**的，且在 feature_table 守 grain 時是**多餘的 shuffle**）。

**對照組**：training 端早有正解。`sample_pool` 是明確、由自己的 ETL 產出、grain 受
保證的母體表——`parameters_sample_pool_etl.yaml` 以 `primary_key: [snap_date, cust_id, prod_name]`
＋ `quality_checks: {max_duplicate_key_ratio: 0.0}` 在 **ETL 產出時**強制唯一。inference 是
唯一一條沒有對應母體的 pipeline。本設計就是補上 inference 的對應物。

## 目標 / 非目標

**目標**
- 引入明確的 inference 推論母體來源表 `inference_population`，grain = `(time, entity)`。
- 母體唯一性在 **source_etl 產出階段**保證（沿用既有 ETL 的 `primary_key` + `quality_checks` 機制）。
- `build_scoring_dataset` 以母體為準，feature_table 退回純 enrichment 角色。
- 缺特徵的母體成員「**允許但註記＋回報**」，不阻斷整批。

**非目標**
- 不改 `consistency.py`（唯一性由 ETL 設定機制負責，非新增一致性 invariant）。
- 不重用 `sample_pool` 作母體（時間覆蓋對不上，見下節）。
- 不在本設計把 `feature_present` 註記**下推到** `score_table` / `ranked_predictions` 等輸出表（schema 變動，列為可選 follow-up）。
- 不改下游 `predict_scores` / `rank_predictions` / `validate_predictions` 的邏輯。

## 為什麼不直接重用 `sample_pool`

`sample_pool` 是為 **training snapshots（`train_snap_dates`）** 建的；inference 每週跑的是**當期 snap_date**，通常不在 `sample_pool` 內。模式對、那張表的時間覆蓋不對。因此「比照 sample_pool 的模式」，但用一張**專屬 inference 的**母體表。

## 設計

> 以抽象框架描述（time / entity / item 為 schema 角色欄）。示例 instantiation：time=`snap_date`、entity=`cust_id`、item=`prod_name`。

### 元件 1：來源表 `inference_population`（source_etl 產出）

- **grain**：`(time, entity)`，每位 entity 每個 time 一列＝該批要推論的母體成員。
- **產出**：source_etl 一支 SQL（`inference_population/inference_population.sql`），業務邏輯（誰進母體）寫在這支 SQL，由使用者自定義。
- **唯一性保證**：新增 `parameters_inference_population_etl.yaml`，比照 `sample_pool_etl`：
  ```yaml
  inference_population_etl:
    dry_run: false
    variables:
      target_db: "ml_recsys"
    source_checks: {}
    tables:
      - name: inference_population
        sql_file: inference_population/inference_population.sql
        partition_by:
          snap_date: DATE
        primary_key: [snap_date, cust_id]      # = (time, entity) grain
        quality_checks:
          max_duplicate_key_ratio: 0.0          # 重複即 ETL fail-loud
    audit:
      database: "${target_db}"
      table: etl_audit_log
  ```
- **catalog**：新增 `inference_population`（`HiveTableDataset`, `read_only: true`），比照 `sample_pool`。

### 元件 2：`build_scoring_dataset` 改寫

pipeline 接線：`inputs=["inference_population", "feature_table", "parameters"]`。

```python
# 母體（grain 由 ETL 保證唯一）→ 不再 dropDuplicates
customers = inference_population.filter(F.col(time_col).cast("date").isin(snap_dates)) \
                               .select(*join_key)

# 母體必須覆蓋請求的 snap_dates（fail-loud；對小基數 distinct 即可）
# （missing-snap_date 檢查從 feature_table 移到 inference_population）

# enrichment：用 indicator 欄標記特徵是否存在（不依賴 feature 欄可不可為 null）
ft = feature_table.withColumn("_ft_present", F.lit(True))
scoring = customers.crossJoin(products_df) \
                   .join(ft, on=join_key, how="left")
scoring = scoring.withColumn("feature_present", F.col("_ft_present").isNotNull()) \
                 .drop("_ft_present")
```

效果：
- **membership 以母體為準**，feature_table 只負責 enrichment。
- **移除 `dropDuplicates`**（grain 已由 ETL 保證）→ 順帶解掉先前 audit-fix 觸發的雙重 shuffle。
- **缺特徵成員保留**（left join），且帶有 `feature_present=false` 註記。

### 元件 3：覆蓋語意——允許 + 回報 + 註記

- **註記**：scoring_dataset（join 後的表）多一個 boolean 欄 `feature_present`。缺特徵的母體成員 = `false`，下游可據此自行排除或觀察。
- **回報**：`build_scoring_dataset` 記 log：每 `snap_date` 母體成員數 vs 缺特徵成員數（一次小聚合，cardinality = #snap_dates）。
- **不阻斷**：缺特徵**不** raise。

## 資料流

```
inference_population (time,entity)        feature_table (time,entity,features)
        │ filter snap_dates                       │ + _ft_present=lit(true)
        │ select join_key                         │
        ▼                                         │
   customers ──crossJoin── products(config) ──join(left, join_key)──▶ scoring
                                                                       + feature_present
                                                                       (log 缺特徵回報)
        ▼ (下游不變)
   apply_preprocessor → predict_scores → rank_predictions → validate_predictions → publish
```

## 錯誤處理

| 情況 | 行為 |
|---|---|
| 母體有重複 `(time, entity)` 列 | source_etl **fail-loud**（`quality_checks.max_duplicate_key_ratio: 0.0`） |
| 請求的 snap_date 不在母體 | `build_scoring_dataset` **fail-loud**（`ValueError`，列出缺的日期） |
| 母體成員在 feature_table 缺特徵 | **允許**：保留該成員、`feature_present=false`、log 回報 |

## 與 audit-fix PR 的關係（排序）

- 現行 `feat/inference-eval-audit-fixes` PR（model feature 對齊、persisted-history scoping、eval model_version 欄缺失容忍）**先行、獨立**。
- 本設計**後做**，會重寫 `build_scoring_dataset`，**取代** audit-fix 在該函式引入的 missing-snap_date 檢查與 `dropDuplicates`（連帶解掉 #2 雙重 shuffle）。
- 因此本 PR 應 rebase 在 audit-fix 之後，或於其合併後開始，以避免在 `build_scoring_dataset` 上互衝。

## 測試計畫

- `build_scoring_dataset`：
  - 母體驅動 membership：母體有、但 feature_table 沒有的成員，仍出現在 scoring 且 `feature_present=false`。
  - feature_table 有、但母體沒有的客戶，**不**出現在 scoring。
  - 缺特徵回報：log/計數正確；缺特徵成員數正確。
  - 不再依賴 `dropDuplicates`：母體唯一輸入下，輸出列數 = #母體 × #products。
  - 請求 snap_date 不在母體 → raise。
- catalog / parameters 接線：`inference_population` 可被 load；ETL 設定可被解析。
- 既有 inference 下游測試（predict/rank/validate）保持綠燈（介面不變）。

## 開放 / 待確認

1. `feature_present` 是否要**下推**到 `score_table` / `ranked_predictions`（讓 production 消費端也看得到）？目前列為可選 follow-up（涉及 Hive 輸出表 schema 演化）。
2. 範例（合成資料）的 `inference_population.sql` 內容：最小可用版＝對 feature 來源在 snap_date 取 distinct `(time, entity)`；之後由使用者放入真實業務資格邏輯。

## YAGNI

- 不做 `feature_present` 下推（除非確有 production 消費需求）。
- 不做 population ↔ sample_pool 的對齊/共用。
- 不在 inference pipeline 內重做 grain 唯一性掃描（ETL 已保證；避免重複 shuffle）。

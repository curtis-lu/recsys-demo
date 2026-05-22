# 評估指標（Metrics）

本文件說明本專案實際計算與輸出的排序評估指標，皆以程式為準：

- `src/recsys_tfb/evaluation/metrics.py`：HPO 用的 numpy 單機 primitive。
- `src/recsys_tfb/evaluation/metrics_spark.py`：evaluation pipeline 的 Spark-native 指標主體（四層設計）。
- `src/recsys_tfb/evaluation/report_builder.py`：把指標 dict 組成 HTML 報表分段。
- `conf/base/parameters_evaluation.yaml`：`k_values`、`segment_columns`、`product_categories`、`report` 設定。

> 指標的**概念與語意**（per-query vs per-item 兩種切法、命名陷阱、退化情況）見
> [metrics_concept_map.html](metrics_concept_map.html)（瀏覽器開啟）。本文件聚焦
> 「程式實際算什麼、輸出長什麼樣、設定怎麼控制」。

---

## 1. 兩個計算路徑

| 路徑 | 模組 | 何時跑 | 產物 |
|---|---|---|---|
| HPO scoring | `metrics.py`（`compute_ap` / `compute_mean_ap`）| training `tune_hyperparameters` 每個 Optuna trial | 單一 mAP scalar，選最佳 trial |
| 完整評估 | `metrics_spark.py`（`compute_all_metrics`）| training `compute_test_mAP_spark`；evaluation pipeline | dict-shaped overall / per_item / per_segment / category 指標 |

### 1.1 HPO mAP（`metrics.py`）

- `compute_ap(y_true, y_score)`：單一 query 的 Average Precision；無正例回 `None`（AP 未定義）。
- `compute_mean_ap(groups, y_true, y_score)`：對每個 query group 算 AP，**跳過無正例的 group**，再對 group 等權平均；全跳過或空陣列回 `0.0`。
- query group = `(snap_date, cust_id)`（即 `schema.time + entity`）。training 的 HPO 即用此值在 val 上比較各 trial。

### 1.2 完整評估（`metrics_spark.py`，四層）

| 層 | 函式 | 做什麼 |
|---|---|---|
| Layer 1 row 級 | `rank_within_query`、`add_query_total_rel`、`add_row_contributions` | 加 `pos`（query 內 score desc 排名）、`total_rel`（該 query label 總和）、`cum_rel`、`prec_at_pos`、`dcg_term`，與每個 K 的 `top_k@K`、`ap_contrib@K`、`ndcg_contrib@K`（iDCG 以 Spark `aggregate(sequence(...))` 內聯計算，無 UDF）|
| Layer 2 per-query | `compute_per_query_metrics` | 每 query 一列：`map@K = sum(ap_contrib@K)/total_rel`、`ndcg@K = sum(ndcg_contrib@K)`、`precision@K = hits/K`、`recall@K = hits/total_rel` |
| Layer 3 聚合 | `aggregate_overall`、`aggregate_per_segment`、`aggregate_per_item`、`macro_average` | overall/per_segment 為 **query 等權**平均；per_item 為對「該 item 為正例的 row」做 **row 等權**平均 |
| Layer 4 orchestrator | `compute_all_metrics` | 串接全部 + `dataset_overview`，product_categories 啟用時加 `category` 段 |

**無正例的 query 會被排除**（`total_rel = 0` 時 AP/nDCG 未定義），`n_excluded_queries` 記錄被排除數，`n_queries` 為過濾前總 query 數。

---

## 2. 指標字彙（程式輸出的 key）

`@K` 的 K 來自 `evaluation.k_values`（混合 int 與 `"all"`；`"all"` 在執行期解析為該粒度的 distinct item 數，見 `_resolve_k_values`）。

### 2.1 per-query 家族（出現在 `overall` / `per_segment`，query 等權）

| key | 公式（單 query）| 意義 |
|---|---|---|
| `map@K` | AP@K | 累積精確度，越前面命中越加分 |
| `ndcg@K` | DCG@K / iDCG@K | log 折扣排序品質，正規化到 [0,1] |
| `precision@K` | 命中數 / K | 推前 K 個有幾成準 |
| `recall@K` | 命中數 / total_rel | 想要的撈回幾成（客戶等權）|

> 退化：當 `K >= n_products`（即 `"all"`），`precision@all` 塌成母體正樣本率（base rate，標籤密度健檢，非排序品質）、`recall@all` 對每 query 恆為 `1.0`。輸出仍保留以相容於遍歷 `k_values` 的下游，但解讀時須注意。

### 2.2 per-item 家族（出現在 `per_item` / `per_item_segment`，對 item-正例 row 等權）

| key | 公式 | 意義 |
|---|---|---|
| `hit_rate@K` | mean(top_k@K) over P-正例 row | per-item recall：`P(rank(P) ≤ K \| P 為正例)`（命中事件等權，與 per-query `recall@K` 同條件機率但聚合單位不同，故另命名）|
| `map_attr@K` | mean(ap_contrib@K) | P 對「客戶 AP@K」的平均可加貢獻碎片 |
| `ndcg_attr@K` | mean(ndcg_contrib@K) | P 對「客戶 nDCG@K」的平均可加貢獻碎片 |
| `mean_pos` | mean(pos) over P-正例 row | P 被想要時平均排第幾名（越小越好）|

> `_attr` 後綴 = 它是某 per-query 指標的**可加碎片**；`hit_rate` 無 `_attr` = 它**不是** query recall 的碎片，是重新定義的 item-marginal recall。per_item 刻意**沒有** `precision@K` / `recall@K`（K 分母是 per-query 概念，無法乾淨歸因到單一 item；用 `hit_rate@K` 作 item-level recall 類比）。

---

## 3. training 寫出的 `evaluation_results.json`

`compute_test_mAP_spark`（`pipelines/training/nodes.py`）把 Spark 指標壓成 `log_experiment` 與 `promote_model.py` 用的扁平 dict：

```jsonc
{
  "overall_map": float,            // overall["map@<n_prods>"]，per-query mAP@all
  "per_item_map_attr": { "<prod>": float, ... },  // per_item["<prod>"]["map_attr@<n_prods>"]
  "n_queries": int,
  "n_excluded_queries": int,
  // 有 calibration（score != score_uncalibrated）時才有：
  "uncalibrated": { "overall_map": float, "per_item_map_attr": {...} },
  "calibration_method": "sigmoid" | "isotonic" | ...
}
```

- `scripts/promote_model.py` 自動選版時即比較各版本此檔的 `overall_map`（取最高）。
- `<n_prods>` = `training_eval_predictions` 的 distinct item 數，故 `overall_map` 為 mAP@all。

---

## 4. evaluation 報表分段

`generate_report`（`pipelines/evaluation/nodes_spark.py`）→ `report_builder.assemble_report` 組出 `data/evaluation/<model_version>/<snap_date>/report.html`。分段由 `evaluation.report.sections` 開關，顯示的 K 由 `evaluation.report.display.*` 決定：

| section（`report.sections`）| 對應 builder | 內容 |
|---|---|---|
| `dataset_overview` | `build_dataset_overview_section` | row/customer/product/snap_date/正例率（`compute_dataset_overview`）|
| `primary_map` | `build_primary_map_section` | per-query `map@K`（主指標）|
| `guardrail_recall` | `build_guardrail_recall_section` | `recall@K` 護欄熱圖 |
| `category` | `build_category_section` | product 大類平行評估（見下）；同時受 `product_categories.enabled` 控制 |
| `per_segment` | `build_segment_section` | 依 `segment_columns` / `segment_sources` 分群 |
| `diagnostics` | `build_diagnostics_section` | 分數分佈、校準曲線（`include_calibration`、`n_calibration_bins`）|
| `baseline` | `build_baseline_section` | 與 popularity baseline（evaluation pipeline 內 `compute_baseline_metrics` 計算）的 overall mAP 與 per-item recall delta（baseline section 關閉時自動略過）|

另含 headline 與 glossary 段。

### product 大類平行評估

`product_categories.enabled=true` 時，`compute_all_metrics` 會把 fine-grained 預測 collapse 成 category 粒度（category score = max(子產品 score)、label = max(子產品 label)），用**同一套指標**再算一次，結果掛在回傳 dict 的 `category` key（結構同頂層，含自己的 `dataset_overview`，不再巢狀 `category`）。mapping 來自 `evaluation.product_categories.mapping`；未列入任何 list 的產品在 `unmapped: singleton` 下自成單一 category；mapping 參照未宣告產品會 fail-loud（`ValueError`）。

---

## 5. 設定速查（`conf/base/parameters_evaluation.yaml`）

| 設定 | 作用 |
|---|---|
| `k_values` | 所有 `@K` 指標的 K 超集（含 `"all"`）；報表分段再自行切片 |
| `segment_columns` | labels 內既有的分群欄（如 `cust_segment_typ`）|
| `segment_sources` | 外部分群來源，依 `key_columns` left-join 進 labels（`prepare_eval_data`）|
| `product_categories` | 大類平行評估的 mapping 與 `unmapped` 策略 |
| `baseline` | popularity baseline 的 `lookback_months` 歷史窗口（月）|
| `report.sections` / `report.display` / `report.diagnostics` | 報表分段開關、各段顯示的 K、診斷圖選項 |

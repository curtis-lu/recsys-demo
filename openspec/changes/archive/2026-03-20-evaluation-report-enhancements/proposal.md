## Why

Evaluation report 目前缺少資料集基本統計、無法區分正/負樣本的分佈、Segment Analysis 用圖表過於冗長，且 metrics 命名不一致（部分帶 @K、部分不帶）。需要增強 report 的可讀性與分析維度，讓使用者能更快掌握資料集特性與模型排序品質。

## What Changes

- **新增資料集統計表**：by product 和 by segment 各一張表，包含正/負樣本數、正樣本率、不重複客戶數、每位客戶平均正樣本產品數。分散嵌入對應的 report section。
- **Rank Distribution 新增正樣本視角**：保留現有全量 heatmap，新增「正樣本 rank 次數 heatmap」和「各 rank 位置正樣本率 heatmap」兩張圖。
- **Score Distributions 新增正/負樣本分開的分佈圖**：保留現有全量版，新增一張 grouped boxplot 按 product 呈現正/負樣本各自的 score 分佈。
- **Segment Analysis 改用表格**：保留 segment metrics 計算，但改用類似 Per-Product Metrics 的表格呈現，不再用 bar chart。
- **Metrics 統一為 @K 格式**：**BREAKING** — 所有 metric 統一帶 @K suffix（@5 一組 + @N 一組，N=產品總數），刪掉不帶 @K 的 map、ndcg、mrr。影響 `compute_all_metrics` 回傳結構及所有下游消費者。

## Capabilities

### New Capabilities
- `dataset-statistics`: 計算並呈現 per-product 和 per-segment 的資料集統計表（正/負樣本數、正樣本率、客戶數、平均正樣本產品數）
- `positive-sample-distributions`: 正樣本 rank heatmap（次數 + 正樣本率）和正/負樣本 score distribution（grouped boxplot）

### Modified Capabilities
- `ranking-metrics`: 所有 metric 統一為 @K 格式，刪除不帶 @K 的 map/ndcg/mrr
- `segment-analysis`: Segment Analysis 改用表格呈現，不再使用 bar chart 圖表

## Impact

- `src/recsys_tfb/evaluation/metrics.py` — @K 統一：`_compute_query_metrics`、`_enrich_with_contributions`、`_aggregate_per_dimension` 修改，新增 `compute_ap_at_k`、`compute_mrr_at_k`
- `src/recsys_tfb/evaluation/statistics.py` — 新增檔案
- `src/recsys_tfb/evaluation/distributions.py` — 新增 3 個函式
- `src/recsys_tfb/evaluation/segments.py` — 新增 `build_segment_metrics_table`
- `scripts/evaluate_model.py` — `_run_analysis` 整合所有變更
- `tests/test_evaluation/` — 多個測試檔案需更新 metric key names，新增測試
- 下游影響：任何直接讀取 `metrics.json` 的工具需適配新 key 格式（@K suffix）

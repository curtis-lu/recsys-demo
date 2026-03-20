## Context

Evaluation report 已有 metrics 計算、score/rank distribution、calibration、segment analysis 等功能。前一輪修正了 per-product metrics 的 vectorized decomposition bug。

目前問題：
1. 缺少資料集基本統計（正/負樣本數、客戶數等），使用者難以快速掌握資料集特性
2. Rank/Score distribution 不區分正/負樣本，無法觀察模型是否將正樣本排序靠前
3. Segment Analysis 用 bar chart 呈現，每個 metric 一張圖太冗長
4. Metrics 命名不一致：map、ndcg、mrr 不帶 @K，但 ndcg@N 和 ndcg 語意相同

## Goals / Non-Goals

**Goals:**
- 在 report 中嵌入 per-product 和 per-segment 的資料集統計表
- 新增正樣本 rank heatmap（次數 + 正樣本率）和正/負樣本 score boxplot
- Segment Analysis 改用表格呈現
- 所有 metric 統一為 @K 格式（@5 + @N 兩組）

**Non-Goals:**
- 不改變 `compute_all_metrics` 回傳 dict 的整體結構（keys: overall, per_product 等）
- 不新增 evaluation parameters YAML 設定
- 不調整 compare 子命令的報表格式
- 不新增前端互動功能

## Decisions

### 1. 資料集統計放在獨立模組 `statistics.py`

資料集統計是「資料描述」而非「模型 metrics」，放在 `metrics.py` 會模糊職責。新增 `evaluation/statistics.py` 提供 `compute_product_statistics` 和 `compute_segment_statistics`，回傳 DataFrame。

在 `_run_analysis` 中，product 統計表嵌入 Per-Product Metrics section 作為第二張 table；segment 統計表嵌入對應 Segment Analysis section。

### 2. 正樣本分佈函式接收 predictions + labels

現有 `plot_score_distributions` 和 `plot_rank_heatmap` 只接收 predictions。新函式需要 labels 來區分正/負樣本。保留現有函式不變（向後相容），新增獨立函式：
- `plot_positive_rank_heatmap(predictions, labels)`
- `plot_positive_rate_rank_heatmap(predictions, labels)`
- `plot_score_distributions_by_label(predictions, labels)` → 回傳 grouped boxplot

### 3. Segment Analysis 用 `build_segment_metrics_table` 取代 bar chart

`compute_segment_metrics` 回傳的 dict 結構不變，新增 `build_segment_metrics_table` 將其轉為 DataFrame（rows=segments, columns=metrics）。`_run_analysis` 改用 tables 而非 figures。`plot_segment_charts` 保留在程式碼中但不再被 `_run_analysis` 呼叫。

### 4. Metrics @K 統一策略

刪除不帶 @K 的 map、ndcg、mrr。所有 metric 都帶 @K suffix。需新增：
- `compute_ap_at_k(y_true, y_score, k)` — top-K 內的 AP（只考慮 top-K items 的 precision at relevant positions）
- `compute_mrr_at_k(y_true, y_score, k)` — 若第一個正樣本 rank > K 則回傳 0

`_compute_query_metrics` 改為對每個 K 計算全部 5 個 metrics。`_enrich_with_contributions` 同步移除全量版欄位，只保留 @K 版本。

### 5. Score distribution by label 用 grouped boxplot

22 個產品用 overlaid histogram 會很混亂。Grouped boxplot（x=product, color=label）更清晰，一張圖即可呈現所有產品的正/負分佈差異。

## Risks / Trade-offs

- **BREAKING CHANGE: metric key names** → 所有下游消費者（讀取 metrics.json、測試中 assert key names）都需更新。風險中等，透過全面更新測試來 mitigate。
- **正樣本 rank heatmap 稀疏** → 大部分 product 在大部分 rank 位置的正樣本數很少，heatmap 可能看起來很稀疏。用文字格式 `%{text}` 顯示實際數字，配合 colorscale 視覺化即可。
- **Segment 表格欄位多** → 如果 K 值多，metrics 欄位數量會增加。目前只有 @5 和 @N 兩組（各 5 個 metrics = 10 欄），可接受。

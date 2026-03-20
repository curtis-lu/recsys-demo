## 1. Metrics @K 統一

- [ ] 1.1 新增 `compute_ap_at_k` 和 `compute_mrr_at_k` 函式到 `metrics.py`
- [ ] 1.2 修改 `_compute_query_metrics` 移除不帶 @K 的 map/ndcg/mrr，改為每個 K 計算 map@K/ndcg@K/mrr@K/precision@K/recall@K
- [ ] 1.3 修改 `_enrich_with_contributions` 移除全量版欄位，只保留 @K 版本的 contribution 欄位
- [ ] 1.4 修改 `_aggregate_per_dimension` 的 metric_cols 對應新的 @K 欄位名稱
- [ ] 1.5 更新 `tests/test_evaluation/test_metrics.py` 所有 metric key assertions（map→map@K 等）
- [ ] 1.6 更新 `tests/scripts/test_evaluate_model.py` 中引用 metric key 的部分

## 2. 資料集統計

- [ ] 2.1 新增 `src/recsys_tfb/evaluation/statistics.py`，實作 `compute_product_statistics` 和 `compute_segment_statistics`
- [ ] 2.2 新增 `tests/test_evaluation/test_statistics.py`，測試欄位正確性、已知計數、missing segment column

## 3. 正樣本分佈

- [ ] 3.1 在 `distributions.py` 新增 `plot_positive_rank_heatmap`（正樣本 rank 次數）
- [ ] 3.2 在 `distributions.py` 新增 `plot_positive_rate_rank_heatmap`（各 rank 位置正樣本率）
- [ ] 3.3 在 `distributions.py` 新增 `plot_score_distributions_by_label`（正/負樣本 grouped boxplot）
- [ ] 3.4 在 `tests/test_evaluation/test_distributions.py` 新增 3 個函式的測試

## 4. Segment Analysis 表格化

- [ ] 4.1 在 `segments.py` 新增 `build_segment_metrics_table` 函式
- [ ] 4.2 在 `tests/test_evaluation/test_segments.py` 新增表格函式的測試

## 5. 整合到 evaluate_model.py

- [ ] 5.1 更新 `_run_analysis` 的 imports 和 section 建構邏輯：嵌入資料集統計表、正樣本 rank heatmap、正/負 score 分佈、segment 表格
- [ ] 5.2 執行全量測試確認所有 evaluation 測試通過

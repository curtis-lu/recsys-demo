## 1. 核心指標模組

- [x] 1.1 建立 `src/recsys_tfb/evaluation/__init__.py` 模組骨架
- [x] 1.2 實作 `evaluation/metrics.py` — 單一 query 指標函數：compute_ap, compute_ndcg, compute_precision_at_k, compute_recall_at_k, compute_mrr
- [x] 1.3 實作 `evaluation/metrics.py` — compute_all_metrics 主入口函數，含 predictions-labels join、query grouping、overall/per_product/per_segment/per_product_segment 維度
- [x] 1.4 實作 `evaluation/metrics.py` — macro average 和 micro average 計算（by_product, by_segment, by_product_segment）
- [x] 1.5 建立 `tests/test_evaluation/__init__.py` 和 `tests/test_evaluation/test_metrics.py` — 涵蓋所有 scenario
- [x] 1.6 重構 `training/nodes.py` — 移除 `_compute_ap`/`_compute_map`，改為 `from recsys_tfb.evaluation.metrics import compute_ap`，確認 evaluate_model 行為不變
- [x] 1.7 執行既有 training tests 確認不破壞

## 2. Baseline 產生器

- [x] 2.1 實作 `evaluation/baselines.py` — generate_global_popularity_baseline（含 leakage 防護，限 snap_date 之前資料）
- [x] 2.2 實作 `evaluation/baselines.py` — generate_segment_popularity_baseline
- [x] 2.3 建立 `tests/test_evaluation/test_baselines.py` — 驗證 schema 一致性、排序正確性、leakage 防護、無歷史資料警告

## 3. 視覺化模組

- [x] 3.1 實作 `evaluation/distributions.py` — plot_score_distributions（histogram + boxplot）
- [x] 3.2 實作 `evaluation/distributions.py` — plot_rank_heatmap
- [x] 3.3 實作 `evaluation/calibration.py` — plot_calibration_curves（使用 sklearn.calibration.calibration_curve）
- [x] 3.4 實作 `evaluation/segments.py` — compute_segment_metrics, compute_holding_combo_metrics（含 top_n 過濾）
- [x] 3.5 實作 `evaluation/segments.py` — plot_segment_charts, plot_holding_combo_charts
- [x] 3.6 建立 `tests/test_evaluation/test_distributions.py`, `test_calibration.py`, `test_segments.py`

## 4. 報告產生與比較邏輯

- [x] 4.1 實作 `evaluation/report.py` — ReportSection dataclass, generate_html_report（內嵌 plotly.js）, save_report, save_metrics_json
- [x] 4.2 實作 `evaluation/compare.py` — build_comparison_result（計算所有維度 delta）
- [x] 4.3 實作 `evaluation/compare.py` — plot_comparison_metrics, plot_comparison_score_distributions
- [x] 4.4 建立 `tests/test_evaluation/test_report.py`, `test_compare.py`

## 5. CLI 腳本

- [x] 5.1 實作 `scripts/evaluate_model.py` — Typer app 骨架，analyze 子命令（載入資料、呼叫各模組、產生報告）
- [x] 5.2 實作 `scripts/evaluate_model.py` — compare 子命令（支援 model_b 或 --baseline 擇一）
- [x] 5.3 實作 snap_date 格式轉換（YYYY-MM-DD → YYYYMMDD）及 model_version 別名解析（latest/best）
- [x] 5.4 建立 `tests/scripts/test_evaluate_model.py` — CLI 整合測試

## 6. 收尾

- [x] 6.1 端對端測試：synthetic data → dataset → training → inference → evaluate analyze → evaluate compare
- [x] 6.2 確認離線 HTML 報告在瀏覽器正常顯示
- [x] 6.3 更新 `CLAUDE.md` — 新增 evaluation 模組到 current status 和 roadmap
- [x] 6.4 更新 `plan.md` — Phase 6 進階功能區段標記 evaluation 相關項目為已完成

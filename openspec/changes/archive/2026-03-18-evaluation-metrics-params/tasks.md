## 1. 參數設定檔

- [x] 1.1 新增 `conf/base/parameters_evaluation.yaml`，包含 k_values、segment_columns、segment_sources、holding_combo.top_n

## 2. @K "all" 解析

- [x] 2.1 修改 `evaluation/metrics.py` — `compute_all_metrics` 支援 k_values 含 `"all"` 字串，在 merge 後解析為產品總數 N；預設值改為 `[5, "all"]`
- [x] 2.2 修改 `evaluation/segments.py` — `compute_segment_metrics` 預設值改為 `[5, "all"]`
- [x] 2.3 新增測試：`test_metrics.py` 測試 k_values=[5, "all"] 正確解析為 [5, N]

## 3. 外部 Segment 資料源

- [x] 3.1 在 `evaluation/segments.py` 新增 `load_and_join_segment_sources(labels, segment_sources)` 函數
- [x] 3.2 新增測試：`test_segments.py` 測試 load_and_join_segment_sources（正常載入、檔案不存在跳過、部分 join 覆蓋）

## 4. 移除 Holding Combo 專用 API

- [x] 4.1 從 `evaluation/segments.py` 移除 `compute_holding_combo_metrics` 和 `plot_holding_combo_charts`
- [x] 4.2 從 `evaluation/__init__.py` 移除對應匯出（__init__.py 無顯式匯出，無需修改）
- [x] 4.3 更新 `tests/test_evaluation/test_segments.py` — 移除 holding combo 專用測試，新增 holding_combo 作為 segment 的統一流程測試

## 5. Metrics Summary 分列 micro/macro

- [x] 5.1 修改 `scripts/evaluate_model.py` 的 `_run_analysis` — Metrics Summary section 改為三張表格（Overall、Macro Average、Micro Average）
- [x] 5.2 新增測試：驗證 _run_analysis 回傳的 sections 中 Metrics Summary 包含三張表格且格式正確

## 6. CLI 參數整合

- [x] 6.1 修改 `scripts/evaluate_model.py` — analyze 指令新增 `--params-file` 選項，載入 parameters_evaluation.yaml，整合 segment_columns 和 segment_sources
- [x] 6.2 修改 `scripts/evaluate_model.py` — compare 指令同步整合 parameters_evaluation.yaml 的 k_values
- [x] 6.3 修改 `scripts/evaluate_model.py` — `_run_analysis` 新增 `segment_columns` 參數，統一遍歷所有 segment 維度
- [x] 6.4 更新 `tests/scripts/test_evaluate_model.py` — 測試 CLI 從 YAML 讀取參數、--k-values 覆蓋、多 segment 維度分析

## 7. 驗證

- [x] 7.1 執行全部 evaluation 測試：`pytest tests/test_evaluation/ -v`
- [x] 7.2 執行 CLI 測試：`pytest tests/scripts/test_evaluate_model.py -v`
- [x] 7.3 端到端驗證：執行 analyze 指令確認報告包含三張 Metrics Summary 表格、@5/@N 指標、多 segment 維度

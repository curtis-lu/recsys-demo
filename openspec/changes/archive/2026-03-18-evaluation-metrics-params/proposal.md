## Why

Evaluation 模組目前有三個限制影響使用彈性：(1) Metrics Summary 報告只顯示全局 overall 平均，不分列 micro/macro 平均（by product、by segment），難以判斷各維度表現差異；(2) @K 值硬編碼為 [3, 5]，無法透過 YAML 設定調整，且缺少 @N（全部產品）的指標；(3) Holding Combo 分析以獨立函數實作，但本質上只是另一種客群切分方式，應統一為 segment 維度並支援從外部資料源載入，提高擴充彈性。

## What Changes

- **Metrics Summary 分列 micro/macro**：報告的 Metrics Summary section 從只有一張 overall 表格，改為三張表格（Overall、Macro Average、Micro Average），每張表格的欄位為各分析維度（by_product、by_cust_segment_typ、by_holding_combo 等）
- **@K 參數化**：新增 `conf/base/parameters_evaluation.yaml`，集中管理 k_values（預設 `[5, "all"]`，"all" 在 runtime 解析為產品總數 N）、segment 相關設定。CLI `--k-values` 作為 YAML 設定的覆蓋
- **Holding Combo 統一為 Segment**：Holding combo 改為從外部 Parquet 資料源（如 SQL 產出的 `cust_id + snap_date + holding_combo`）載入，透過 `segment_sources` 設定 join 到 labels 上，統一使用 `compute_segment_metrics` 分析。**BREAKING**：移除 `compute_holding_combo_metrics` 和 `plot_holding_combo_charts`

## Capabilities

### New Capabilities
- `evaluation-params`: 集中管理 evaluation 參數的 YAML 設定檔（k_values、segment_columns、segment_sources），以及 runtime 解析 "all" → N 的邏輯

### Modified Capabilities
- `evaluation-metrics`: k_values 預設值改為 `[5, "all"]`，支援 "all" 字串解析為產品總數 N
- `evaluation-segments`: 移除 `compute_holding_combo_metrics` / `plot_holding_combo_charts`，新增 `load_and_join_segment_sources` 支援外部資料源載入；所有 segment 維度統一流程
- `evaluation-cli`: analyze/compare 指令整合 parameters_evaluation.yaml 載入；Metrics Summary 改為分列 micro/macro 三張表格
- `evaluation-report`: Metrics Summary section 結構從單一 overall 表格改為 Overall + Macro Average + Micro Average 三張表格

## Impact

- **修改檔案**：`evaluation/metrics.py`、`evaluation/segments.py`、`scripts/evaluate_model.py`、`evaluation/__init__.py`
- **新增檔案**：`conf/base/parameters_evaluation.yaml`
- **Breaking change**：移除 `compute_holding_combo_metrics`、`plot_holding_combo_charts` 公開 API 及其測試
- **測試影響**：`test_metrics.py`、`test_segments.py`、`test_evaluate_model.py` 需更新
- **依賴**：無新增套件依賴

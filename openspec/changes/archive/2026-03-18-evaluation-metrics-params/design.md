## Context

Evaluation 模組目前的 metrics、segments、CLI 邏輯存在三個限制：報告只顯示 overall 指標不分列 micro/macro、@K 硬編碼為 [3,5]、Holding Combo 以獨立函數處理而非統一 segment 維度。所有設定散落在 CLI flag 和函數預設值中，無集中設定檔。

現有架構中 `compute_all_metrics` 已計算 macro_avg / micro_avg（by_product、by_segment），但報告端 (`_run_analysis`) 未將這些資料呈現。Segment 分析有 `compute_segment_metrics`（通用）和 `compute_holding_combo_metrics`（特化），兩者邏輯高度重疊。

## Goals / Non-Goals

**Goals:**
- Metrics Summary 報告分列 Overall、Macro Average、Micro Average 三張表格
- @K 參數化，預設 [5, "all"]，"all" 在 runtime 解析為產品總數 N
- 集中管理 evaluation 參數於 `parameters_evaluation.yaml`
- Holding combo 改為外部 Parquet 資料源，透過 `segment_sources` 設定 join，統一 segment 分析流程
- 移除 `compute_holding_combo_metrics` / `plot_holding_combo_charts` 冗餘 API

**Non-Goals:**
- 不修改 baseline 比較流程（維持在 compare 指令中）
- 不修改 `compute_all_metrics` 內部的 macro/micro 計算邏輯（已正確實作）
- 不新增 YAML 以外的設定機制
- 不修改 training pipeline 的 metrics 計算

## Decisions

### 1. "all" 解析位置：`compute_all_metrics` 內部

在 `compute_all_metrics` 中 join predictions + labels 後，取得 `prod_code` unique count 作為 N，將 k_values 中的 `"all"` 替換為 N。

**理由**：此處已有完整的 merged DataFrame，可直接取得 N。在 CLI 端解析需要額外讀取資料，且 `compute_segment_metrics` 也需要同樣邏輯，集中在底層更一致。

**替代方案**：在 CLI 端預先解析 — 缺點是 `compute_segment_metrics` 等上層函數也需要知道 N，會導致 N 在多處傳遞。

### 2. 外部 segment 資料源載入：新增 `load_and_join_segment_sources`

在 `segments.py` 新增 `load_and_join_segment_sources(labels, segment_sources)` 函數，遍歷 `segment_sources` 設定逐一載入 Parquet 並 left join。

**理由**：保持 I/O 邏輯與 metrics 計算分離。載入完成後，holding_combo 和其他外部 segment 在 labels 上就只是普通欄位，完全複用 `compute_segment_metrics`。

### 3. Metrics Summary 表格結構

三張表格的 column 為各分析維度（by_product、by_cust_segment_typ、by_holding_combo 等），row 為指標名稱。資料來源直接是 `metrics["macro_avg"]` 和 `metrics["micro_avg"]`。

**理由**：`compute_all_metrics` 已計算這些值，只需在報告端組裝 DataFrame 呈現。

### 4. 參數載入方式：YAML 檔 + CLI 覆蓋

CLI 嘗試載入 `parameters_evaluation.yaml`（透過 `yaml.safe_load`），若不存在則使用內建預設值。`--k-values` CLI flag 優先於 YAML 設定。

**理由**：與現有 `ConfigLoader` 模式一致（其他 pipeline 也是 YAML 設定 + CLI 覆蓋）。評估腳本不經過主 pipeline 框架，直接讀取 YAML 即可。

## Risks / Trade-offs

- **Breaking change**：移除 `compute_holding_combo_metrics` / `plot_holding_combo_charts` — 外部若有直接引用會斷裂。但目前只有 `evaluate_model.py` CLI 和測試使用，風險低。→ 同步清理所有引用點。
- **外部 Parquet 檔不存在**：`segment_sources` 指定的檔案不存在時需明確報錯。→ `load_and_join_segment_sources` 中檢查檔案存在性，不存在時 log warning 並跳過該 segment（不中斷分析）。
- **"all" 在不同 segment 下 N 可能不同**：per-segment 分析時產品數可能因 segment 而異。→ `compute_all_metrics` 以 merged data 的全局產品數為 N，保持一致性。

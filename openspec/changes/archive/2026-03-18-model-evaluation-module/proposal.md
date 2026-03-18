## Why

目前模型評估只有 mAP 和 per-product AP 兩個指標（在 training pipeline 中計算），無法全面了解模型在各產品/客群面向的推論品質。需要系統化的評估工具來：深入分析模型的推論機率分布與排序傾向、確認模型能兼顧各產品而非偏好特定產品、比較不同模型版本或 baseline 的表現差異，以支撐模型迭代與上線決策。

## What Changes

- 新增 `src/recsys_tfb/evaluation/` 模組，包含排序指標計算（mAP, nDCG, precision@K, recall@K, MRR）、分布分析、校準曲線、客群細分、baseline 產生、HTML 報告產生、模型比較等可重用函數
- 新增 `scripts/evaluate_model.py` CLI 腳本（Typer），支援單模型分析和雙模型/baseline 比較兩種模式，產出 Plotly HTML 互動報告
- 重構：提取 `training/nodes.py` 的 `_compute_ap`/`_compute_map` 至 `evaluation/metrics.py`，原處改為 import
- 新增 baseline 機制：全域熱門度排序、客群熱門度排序，作為模型表現的參考基準線
- 指標平均方式同時提供 macro average 和 micro average，維度涵蓋分產品、分客群、分產品×客群

## Capabilities

### New Capabilities
- `evaluation-metrics`: 排序指標計算引擎，支援 mAP, nDCG, precision@K, recall@K, MRR，含 per-product/per-segment/per-product×segment 細分，以及 macro/micro average
- `evaluation-distributions`: 推論機率分布分析（score histogram/boxplot）與排名分布分析（rank heatmap）
- `evaluation-calibration`: 模型校準曲線（predicted probability vs actual positive rate）
- `evaluation-segments`: 客群細分分析（依 cust_segment_typ 及持有產品組合分群）
- `evaluation-baselines`: Baseline 產生器（全域熱門度、客群熱門度排序），輸出與 ranked_predictions 相同 schema
- `evaluation-report`: HTML 互動報告產生器，使用 Plotly 產出自包含離線可用的單一 HTML 檔案
- `evaluation-compare`: 模型比較邏輯，支援模型 vs 模型、模型 vs baseline，計算所有指標 delta
- `evaluation-cli`: CLI 入口（Typer），提供 analyze 和 compare 兩個子命令

### Modified Capabilities
- `training-nodes`: 提取 `_compute_ap`/`_compute_map` 至 `evaluation/metrics.py`，原處改為 import

## Impact

- **新增程式碼**：`src/recsys_tfb/evaluation/` 模組（8 個檔案）、`scripts/evaluate_model.py`
- **修改程式碼**：`src/recsys_tfb/pipelines/training/nodes.py`（import 來源變更，不影響行為）
- **新增測試**：`tests/test_evaluation/` 完整測試套件
- **新增依賴**：plotly（已在 PRD 套件清單中，5.17.0）、sklearn.calibration（已有 scikit-learn 1.5.0）
- **新增產出**：`data/evaluation/` 目錄存放 HTML 報告和 metrics JSON
- **文件更新**：`CLAUDE.md` 和 `plan.md` 的 roadmap/狀態更新

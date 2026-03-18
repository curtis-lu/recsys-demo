## Context

目前模型評估僅在 training pipeline 中計算 mAP 和 per-product AP（`training/nodes.py` 的 `_compute_ap`/`_compute_map`）。inference 輸出為 `ranked_predictions.parquet`（columns: snap_date, cust_id, prod_code, score, rank）。label_table 包含 cust_segment_typ 客群欄位。plotly 5.17.0 和 scikit-learn 1.5.0 已在環境中。

生產環境為離線、無網路、CPU-only（4 core, 128GB RAM），資料規模為 10M 客戶 × 22 產品。

## Goals / Non-Goals

**Goals:**
- 提供可重用的 evaluation 模組，支援腳本和未來 notebook/pipeline 使用
- 支援單模型深度分析和雙模型/baseline 比較
- 計算 5 種排序指標（mAP, nDCG, precision@K, recall@K, MRR），含 macro/micro average
- 產出自包含的 Plotly HTML 互動報告（離線可用）
- 從 training/nodes.py 提取指標計算邏輯，消除重複

**Non-Goals:**
- 不實作 pipeline 節點整合（模組設計為 pipeline-ready，但本次只做腳本入口）
- 不實作 notebook（後續階段）
- 不實作自動化排程或觸發（手動執行）
- 不處理 PySpark 後端的評估（評估在 pandas 上進行，即使推論用 Spark）
- 不實作 SHAP / feature importance 分析

## Decisions

### D1：模組 + 腳本架構（非 pipeline 節點）

將分析邏輯放在 `src/recsys_tfb/evaluation/` 模組，CLI 入口放在 `scripts/evaluate_model.py`。

**理由**：分析函數皆為純函數（DataFrame → dict/Figure），天然符合 Node 介面。未來整合進 pipeline 只需寫 `pipeline.py` + 更新 catalog，不需重構邏輯。但現階段用腳本更靈活，可支援任意版本比較而不受 pipeline DAG 限制。

**替代方案**：直接做成 pipeline 節點 → 工作量大且比較模式不適合 DAG 結構。

### D2：指標計算以 query group 為單位

每個 `(snap_date, cust_id)` 為一個 query，query 內的產品按 score 排序後計算指標。這與現有 `_compute_map` 的 groupby 邏輯一致。

**理由**：排序指標的語義是「對每位客戶的產品排序品質」，以客戶為單位計算後再彙總。

### D3：Macro/Micro average 三個維度

- **分產品**：先算各產品的指標，macro = 等權平均，micro = 合併所有 query 後計算
- **分客群**：先算各客群的指標，macro = 等權平均，micro = 依 query 數加權
- **分產品×客群**：交叉維度

**理由**：macro 給每個群組等權重，適合觀察弱勢群組；micro 反映整體表現，適合業務指標。兩者互補。

### D4：Baseline 的 leakage 防護

計算 baseline 正樣本率時，只使用 target snap_date 之前的 label_table 資料。

**理由**：避免用未來資訊產生 baseline，確保比較公平。若無歷史資料則使用所有可用資料並發出警告。

### D5：HTML 報告內嵌 plotly.js

使用 `plotly.offline.get_plotlyjs()` 取得 JS 內容，嵌入 HTML 中。各圖表用 `to_html(full_html=False, include_plotlyjs=False)`。

**理由**：生產環境無網路，不能用 CDN。內嵌約增加 3MB 但確保離線可用。

### D6：直接讀取 Parquet 檔案而非透過 Catalog

CLI 腳本直接建構路徑 `data/inference/{model_version}/{snap_date}/ranked_predictions.parquet` 讀取，不經過 ConfigLoader/DataCatalog。

**理由**：評估腳本需要同時讀取多個版本的資料，Catalog 的 template 變數替換不支援同時載入多版本。版本解析透過 `core/versioning.resolve_model_version()` 處理 latest/best 別名。

### D7：欄位名稱統一

inference 輸出用 `prod_code`，label_table 用 `prod_name`。在 `compute_all_metrics` 內部將 label_table 的 `prod_name` rename 為 `prod_code` 後 join。

**理由**：統一欄位名讓下游分析函數不需關心來源差異。

## Risks / Trade-offs

- **[記憶體]** 生產環境 10M × 22 = 220M rows 載入 pandas 約需 8-10GB → 128GB RAM 足夠，但須注意同時載入兩個版本時加倍。**緩解**：一次只處理一個 snap_date。
- **[plotly.js 體積]** 內嵌 JS 使 HTML 約 3-4MB → 可接受，比多檔案方便。
- **[持有產品組合分群]** 22 產品的組合數可能爆炸 → **緩解**：只取出現頻率前 N 名的組合，其餘歸為「其他」。
- **[指標定義差異]** nDCG 的 relevance 目前為 binary → 未來若有多級 relevance 需調整。**緩解**：函數設計接受 y_true 為任意非負數值。

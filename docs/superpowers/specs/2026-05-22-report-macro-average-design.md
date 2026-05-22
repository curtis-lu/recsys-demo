# report.html 加 Macro 平均彙總列 — 設計

日期：2026-05-22
分支：`feat/report-macro-avg`

## 目標

在 evaluation pipeline 產出的 `report.html` 中，為 per-item 與 per-segment
的表格各加一列「Macro 平均」彙總列，讓讀者一眼看到整體水準後再往下看個別
產品 / segment。

## 背景與關鍵發現

Macro 平均**已經算好**。`metrics_spark.py` 的 `_compute_core()`
（`metrics_spark.py:612-616`）已產出 `macro_avg` dict：

- `macro_avg["by_item"]` — 對所有產品等權平均的
  `hit_rate@K`、`map_attr@K`、`ndcg_attr@K`、`mean_pos`
- `macro_avg["by_segment"]` — 對所有 segment 等權平均的
  `map@K`、`ndcg@K`、`precision@K`、`recall@K`
- `macro_avg["by_item_segment"]` — 同理（本次不顯示）

`metrics.json` 也已包含這份資料（`save_metrics_json` dump 整個 metrics
dict）。**唯一缺口是 `report_builder.py` 沒有把 `macro_avg` 顯示到
`report.html`。**

因此本變更**純粹是 report 顯示層**：不動 metrics 計算層、不動
`metrics.json`、不改 `macro_avg` 的結構或語意。

### Macro 平均 vs overall（語意區別）

- `overall` 的 `map@K` 等：以 **query 等權**平均的 per-query 指標。
- `macro_avg["by_item"]`：對**每個產品等權**平均的 per-item 指標。
- `macro_avg["by_segment"]`：對**每個 segment 等權**平均的 per-query 指標。

兩者是不同的彙總方式，會放進 glossary 說明，避免讀者誤以為等同。

## 設計決定（已與使用者確認）

1. **顯示位置**：在現有表格加一列彙總列（不新增 section、不另設小表）。
2. **Heatmap**：彙總列**只進表格**，色階圖（heatmap）維持「每產品一列」，
   不加 macro 列 — 避免「產品平均」與個別產品共用同一色階造成判讀偏差。
3. **位置與列名**：彙總列置於表格**頂部**，index 名為 **`Macro 平均`**。
4. **大類 Category section**：大類 per-item recall@k 表格**也加**彙總列，
   與細產品各表術語一致。

## 受影響範圍

只動 `src/recsys_tfb/evaluation/report_builder.py`。

| Section builder | 表格 | 彙總列資料來源 |
|---|---|---|
| `build_guardrail_recall_section` | per-item recall@k + mean_pos | `metrics["macro_avg"]["by_item"]` |
| `build_per_item_attr_section` | map_attr@k 表、ndcg_attr@k 表（各加一列） | `metrics["macro_avg"]["by_item"]` |
| `build_segment_section` | per-segment 指標表 | `metrics["macro_avg"]["by_segment"]` |
| `build_category_section` | 大類 per-item recall@k 表 | `metrics["category"]["macro_avg"]["by_item"]` |

**不動**：

- `build_headline_section` / `build_primary_map_section` — 顯示的是
  `overall`，本來就是單列彙總，不需要 macro 列。
- `build_baseline_section` 的 per-item recall delta 表 — 不在本次範圍。
- `metrics_spark.py`、`report.py`、`metrics.json` 輸出。

## 核心做法

**關鍵觀察**：`macro_avg["by_item"]` 的內層 dict
（`hit_rate@K` / `map_attr@K` / `ndcg_attr@K` / `mean_pos`）與
`per_item[item]` 的 key 結構完全相同；`macro_avg["by_segment"]` 與
`per_segment[seg]` 同理。所以「Macro 平均」列就是「一個多出來的
item / segment」，能套用現有欄位組裝邏輯，不必另寫一份。

### per-item 表格（recall / map_attr / ndcg_attr）

`_per_item_metric_table` 新增一個 optional 參數
`macro_metrics: dict | None = None`：

- 給值時，用**同一套** `metric_key` / `col_fmt` / `k` / `extra_cols`
  邏輯為這個 dict 組出一列，以 `"Macro 平均"` 為 index 名
  **prepend 到表格最上方**。
- 不給值（`None`）時行為與現狀完全相同。

欄位組裝邏輯維持單一真實來源，不複製。

`_per_item_recall_table` 透傳這個參數給 `_per_item_metric_table`。

### per-segment 表格

`build_segment_section` 目前是 `pd.DataFrame(per_segment).T`。改成在
`macro_avg["by_segment"]` 非空時，把
`{"Macro 平均": macro_by_segment, **per_segment}` 一起轉成 DataFrame —
dict 插入序保證 `Macro 平均` 在頂列。

### Heatmap 與表格解耦

問題：`_per_item_heatmap` 目前以 `table.index` 當 y 軸列序；若表格多了
`Macro 平均` 列，heatmap 會跟著多一列（值為 `None`）。

解法：`_per_item_heatmap` 改成從 `per_item` 的 keys 取 y 軸列序，
而非 `table.index`。`Macro 平均` 不在 `per_item` 裡 → heatmap 自動排除。

各 builder 內的順序：

1. 先建表（**不含** macro 列）
2. 用該表 + `per_item` 建 heatmap
3. 再以 `macro_metrics` 重建含 macro 列的表（或在表上 prepend）

實作上最簡單：先 `_per_item_metric_table(..., macro_metrics=None)` 給
heatmap 用，再 `_per_item_metric_table(..., macro_metrics=<dict>)` 給
表格用；兩次呼叫成本極低（純 pandas，無 Spark）。

## 邊界情況

- `macro_avg` 為空 dict（無正樣本 → `_EMPTY_RESULT`；或無 segment column
  → `macro_avg` 無 `by_segment` key）：對應表格**不加** macro 列，
  輸出與現狀相同。builder 用 `.get(...)` 安全取值。
- `per_segment` 為空：`build_segment_section` 本來就回傳 `None`，不受影響。
- `metrics` dict 缺 `macro_avg` key（理論上不會，但防禦性）：
  `metrics.get("macro_avg", {})` → 視為空，不加列。

## 其他小幅調整

- `_GLOSSARY` 新增一條 `Macro 平均`：說明「對所有產品（或 segment）等權
  平均，與 query 等權的 `overall` 不同」。
- 受影響 4 個 section 的 `description` 各補一句，說明頂列為 macro 平均。

## 測試

走 TDD。在 `tests/test_evaluation` 既有 `report_builder` 測試檔新增 case：

1. `build_guardrail_recall_section` 產出的 recall 表頂列 index 為
   `Macro 平均`，各欄值等於該欄所有產品的算術平均。
2. `build_per_item_attr_section` 的 map_attr 表與 ndcg_attr 表頂列同上。
3. `build_segment_section` 的 per-segment 表頂列為 `Macro 平均`。
4. `build_category_section` 的大類 recall 表頂列為 `Macro 平均`。
5. heatmap 的 y 軸**不含** `Macro 平均`（列數 = 產品數）。
6. `macro_avg` 為空時，表格不含 `Macro 平均` 列（回歸現狀）。

優先只跑 `report_builder` 相關測試檔，不跑整包 `tests/test_evaluation`。

## 不做（YAGNI）

- 不顯示 `macro_avg["by_item_segment"]`（per-segment×per-item 交叉，
  目前 report 沒有對應表格）。
- 不在 heatmap 加 macro 列。
- 不動 baseline section 的 delta 表。
- 不改 metrics 計算 / `metrics.json` 結構。

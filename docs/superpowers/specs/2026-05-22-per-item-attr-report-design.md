# per_item 歸因 Attribution 段落 — Design

日期：2026-05-22
分支：`feat/per-item-attr-report`

## 背景與目標

`compute_all_metrics` 產出的 `metrics["per_item"]` 對每個產品已帶有
`hit_rate@K`、`map_attr@K`、`ndcg_attr@K`、`mean_pos` 四類值。但目前
`report.html` 的「護欄 per_item recall@k」段只用到 `hit_rate@K`（顯示為
`recall@k (per-item)`）與 `mean_pos`；`map_attr@K` / `ndcg_attr@K` 從未呈現。

目標：把 `map_attr@K` / `ndcg_attr@K` 加進 `report.html`，讓使用者看到
**每個產品對主指標 mAP@k / nDCG@k 各貢獻多少**。

本變更**只動 report 層**（`report_builder.py` / `evaluation.yaml` /
測試），不碰 metrics 計算層。

## 名詞定義

- **`ap_contrib@k`**（metrics 層既有，per-row）：一筆「(客戶, 產品) 且該產品為
  此客戶正解」紀錄的單筆貢獻 = 該產品排名進前 k 時的累積精度（排越前、前面
  混入的非正解越少 → 越高；未進前 k → 0）。一位客戶的 `AP@k` = 他所有正解
  產品的 `ap_contrib@k` 加總 ÷ 正解數 `total_rel`。
- **`map_attr@k`**（per-item 聚合）：某產品在「它為該客戶正解」的所有客戶上，
  `ap_contrib@k` 的平均 → 即該產品平均替 `AP@k` 加了多少分。**不是該產品
  自己的 mAP@k**。
- **`ndcg_attr@k`**：同 `map_attr@k`，單筆貢獻改用 log 折扣、已用 iDCG
  正規化的 `ndcg_contrib@k`。

## 設計決策（已與使用者確認）

| 項目 | 決定 |
|---|---|
| 放置位置 | 新增獨立段落「per_item 歸因 Attribution（細產品）」 |
| K 值來源 | 重用 `display.primary_map_k`（`[1,3,5,"all"]`），與主指標 mAP@k 段同調 |
| 表格佈局 | 兩張表：`map_attr` 一張、`ndcg_attr` 一張，rows = 產品、cols = `@k` |
| 色階 heatmap | 兩張表各配一個 RdYlGn heatmap，色階自動縮放（不設固定 `zmin/zmax`） |
| 實作方式 | 方案 A：抽參數化共用 helper，現有 `_per_item_recall_table` 改為呼叫它 |

## §1 共用 helper（重構）

在 `report_builder.py` 抽兩個參數化 helper，取代 `_per_item_recall_table`
裡寫死的邏輯：

```python
def _per_item_metric_table(per_item, ks, n_prod, metric_key, col_fmt,
                           extra_cols=None) -> pd.DataFrame:
    """Rows = items；每個 k 一欄（欄名由 col_fmt.format(k=k) 決定），
    再接 optional 的非 @k 欄 extra_cols（dict: 欄名 -> per_item 內鍵）。"""

def _per_item_heatmap(table, per_item, ks, n_prod, metric_key,
                      x_fmt, title, zmin=None, zmax=None) -> go.Figure:
    """RdYlGn heatmap；z 取自 per_item[item][f"{metric_key}@{lookup(k)}"]；
    x 軸標籤由 x_fmt.format(k=k) 決定；zmin/zmax 為 None 時 Plotly 自動縮放。"""
```

- `metric_key` ∈ `"hit_rate"` / `"map_attr"` / `"ndcg_attr"`，對應 `per_item`
  dict 既有鍵；`@k` 後綴透過既有 `_k_to_lookup(k, n_prod)` 轉換。
- `_per_item_recall_table` 改為呼叫
  `_per_item_metric_table(per_item, ks, n_prod, "hit_rate",
  "recall@{k} (per-item)", extra_cols={"mean_pos": "mean_pos"})`，
  **輸出 DataFrame 完全不變**。
- `build_guardrail_recall_section` 的 heatmap 改用
  `_per_item_heatmap(..., "hit_rate", "recall@{k}", zmin=cs.low, zmax=cs.high)`，
  **輸出 figure 完全不變**。
- 既有 caller（`build_guardrail_recall_section` / `build_category_section` /
  `build_baseline_section`）行為不變，由 `test_report_builder.py` 既有測試守住。

## §2 新段落 builder

新增 `build_per_item_attr_section(metrics, parameters) -> ReportSection | None`：

```python
def build_per_item_attr_section(metrics, parameters):
    if not _section_on(parameters, "per_item_attr"):
        return None
    per_item = metrics.get("per_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod)

    map_tbl  = _per_item_metric_table(per_item, ks, n_prod, "map_attr",  "map_attr@{k}")
    ndcg_tbl = _per_item_metric_table(per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}")
    map_fig  = _per_item_heatmap(map_tbl,  per_item, ks, n_prod, "map_attr",
                                 "map_attr@{k}", "per-item map_attr@k 色階")
    ndcg_fig = _per_item_heatmap(ndcg_tbl, per_item, ks, n_prod, "ndcg_attr",
                                 "ndcg_attr@{k}", "per-item ndcg_attr@k 色階")
    return ReportSection(
        title="per_item 歸因 Attribution（細產品）",
        description=<見下>,
        figures=[map_fig, ndcg_fig],
        tables=[map_tbl, ndcg_tbl],
        table_titles=["per-item map_attr@k", "per-item ndcg_attr@k"],
    )
```

段落 `description`（白話 + 公式逐步說明）：

> **per_item 歸因 Attribution** — 每個產品對主指標 mAP@k / nDCG@k 各貢獻多少。
> 算法：對每筆「(客戶, 產品) 且該產品是這位客戶的正解」的紀錄，先算單筆貢獻
> `ap_contrib@k` = 該產品排名進前 k 時的**累積精度**（排越前、前面混入的非
> 正解越少 → 越高；沒進前 k → 0）。一位客戶的 `AP@k` = 他所有正解產品的
> `ap_contrib@k` 加總 ÷ 正解數 `total_rel`。`map_attr@k` = 某產品在「它為
> 該客戶正解」的所有客戶上，`ap_contrib@k` 的平均 → 即這個產品平均替 `AP@k`
> 加了多少分。`ndcg_attr@k` 同理，把單筆貢獻換成 log 折扣的 `ndcg_contrib@k`。

要點：
- **段落順序**：在 `assemble_report` 的 `candidates` list 中插在
  `build_guardrail_recall_section` 之後（兩個都是 per_item 段，相鄰）。
- **空 `per_item`**：不另加 guard，與 `build_guardrail_recall_section` 一致
  （空 dict → 空表，不會炸）。

## §3 config 與詞彙表

**`parameters/evaluation.yaml`**：在 `evaluation.report.sections` 區塊加
`per_item_attr: true`。`_section_on` 預設即回 `True`，加這行純為可發現性與
與其他段一致。

**`_GLOSSARY`** 新增兩列：

| 指標 | 語意 |
|---|---|
| `map_attr@k` | 某產品為正解時 `ap_contrib@k` 的平均；`ap_contrib@k` = 該產品進前 k 時的累積精度。客戶該買它、模型排越前 → 值越高。**非該產品自己的 mAP@k**，是 mAP@k 拆到單一產品的貢獻 |
| `ndcg_attr@k` | 同 `map_attr@k`，單筆貢獻改用 `ndcg_contrib@k`（log 折扣排序品質，已用 iDCG 正規化）。越高越好 |

## §4 測試

`tests/test_evaluation/test_report_builder.py`（純 dict、無 Spark）：

- 擴充 `_metrics()` fixture：`per_item` 的 A/B 兩產品各補 `map_attr@1`、
  `map_attr@3`、`map_attr@2`、`ndcg_attr@1`、`ndcg_attr@3`、`ndcg_attr@2`
  （`primary_map_k=[1,3,"all"]`、`n_products=2` → `"all"` 經 `_k_to_lookup`
  解析為 `2`，故鍵用 `@1/@3/@2`）。
- 新測試：
  - `test_per_item_attr_section_built` — 回傳 2 tables、2 figures；
    `map` 表欄含 `map_attr@1`、index = `{A, B}`。
  - `test_per_item_attr_section_off` — `sections.per_item_attr=False` →
    回傳 `None`。
  - `test_per_item_attr_heatmap_autoscale` — heatmap figure 未設定固定
    `zmin/zmax`（為 `None`）。
- 回歸：既有 guardrail / category / baseline 測試在 §1 重構後須**原封不動
  通過**。

驗證指令（worktree + 絕對 venv python）：

```
PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
  -m pytest tests/test_evaluation/test_report_builder.py -q
```

## 不做（YAGNI）

- 不加 attribution 專用 colorscale config（沿用 RdYlGn + 自動縮放）。
- 不動 metrics 計算層（`metrics_spark.py`）。
- 不加 baseline delta 的 attribution 比較。
- 不碰 category 段（`build_category_section`）。
- 不做先前討論過的「conditional per-query metric over P-positive queries」。

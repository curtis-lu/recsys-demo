# Evaluation Report — Baseline Section: 顯示基本統計與並列 metrics

- **Date**: 2026-05-23
- **Branch**: `feat/baseline-section-stats`
- **Scope**: `src/recsys_tfb/evaluation/report_builder.py`, `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (+ tests)

## 動機

目前 `build_baseline_section` 只顯示 `Delta (Model − Baseline)`(overall metrics + per-item recall@k 兩張表)。
使用者反映:**只看 delta 很難驗證合理性** —— 不知道 baseline 本身的絕對值、也看不出 baseline 排序的組成是否符合預期。

舉例:`map@1` delta = +0.0018,模型贏多少?是「model 0.49 vs baseline 0.49」還是「0.49 vs 0.10」?光看 delta 完全分不出來。

## 目標

- 讓 reader **在 report.html 內**就能驗證:
  1. Baseline 的 popularity 排序是否合理(哪個產品最熱門)
  2. Model 與 baseline 各 metric 的**絕對值**,而非只有差距

## 不在範圍

- baseline 的 lookback window / fallback 訊息(可未來再加)
- 評估集 sanity stats(model 與 baseline 評估集相同,無需在 baseline section 重複)
- 改 baseline 演算法(popularity = `sum(label)` over lookback window)
- 修 model 與 baseline 評估集差異(無差異)
- `mean_pos` 等其他 per-item 指標(本次未提出)

## 設計

### Baseline section 改寫為三大區塊

```
基準比較 Baseline

[1] popularity 排名組成               ← 新增
    product        count   rank
    exchange_usd   310     1
    exchange_fx    239     2
    ...

[2] overall metrics  (Model / Baseline / Delta)   ← 由 delta-only 改為三欄
    metric        Model    Baseline   Delta
    map@1         0.4889   0.4871    +0.0018
    map@3         0.7766   0.7766     0.0000
    ndcg@1        0.6820   0.6774    +0.0046
    precision@1   ...
    recall@5      ...

[3a] per-item recall@k  (Model / Baseline / Delta)   ← 原 delta-only 改三欄
     rows = 產品(含 Macro 平均),columns = recall@k 的 M/B/Δ 三欄交織
     k from `display.guardrail_recall_k` (default [1,2,3,4,5]) → 15 cols

[3b] per-item map_attr@k  (Model / Baseline / Delta)   ← 新增
     rows 同上,k from `display.primary_map_k` (default [1,3,5,"all"]) → 12 cols

[3c] per-item ndcg_attr@k  (Model / Baseline / Delta)  ← 新增
     rows 同上,k from `display.primary_map_k` (default [1,3,5,"all"]) → 12 cols
```

Per-item 三張表都採同一展開模式(M / B / Δ 交織),易讀。

### 資料流改動

唯一新增的資料:`purchase_counts`(每產品歷史購買次數)。
已在 `compute_purchase_counts`(`src/recsys_tfb/evaluation/baselines.py`)算過,目前算完即丟。

**API 改動**:`compute_baseline_metrics` 的回傳擴充一個 key。

```python
# Before
def compute_baseline_metrics(...) -> Optional[dict]:
    return {"overall": {...}, "per_item": {...}}

# After
def compute_baseline_metrics(...) -> Optional[dict]:
    return {
        "overall": {...},          # 不變
        "per_item": {...},          # 不變
        "purchase_counts": {        # 新增
            prod_name: int,         # 跨 eval snap_dates 加總(單 snap 即該 snap 值)
        },
    }
```

**多 snap_date 行為**:`evaluation.snap_date` 目前是單值;若未來改多 snap,`purchase_counts` 跨 snap 加總(global popularity 的自然加總)。

### Report 渲染改動

`build_baseline_section`(`report_builder.py:397`)改寫:

1. **[1] popularity 表**:從 `baseline_metrics["purchase_counts"]` 構造,sort by count desc,加 `rank` 欄。若缺(舊資料/向後相容)→ 跳過 [1] 表,[2][3] 照常。
2. **[2] overall 表**:把現有 delta-only 換成三欄。資料源:`comp["result_a"]["overall"]`(Model)、`comp["result_b"]["overall"]`(Baseline)、`comp["overall_delta"]`(Δ)。
3. **[3] per-item 三表**:對 `["hit_rate", "map_attr", "ndcg_attr"]` 三族 metric 各產一表,交織 M/B/Δ 三欄。資料源:`comp["result_a"]["per_item"]`、`comp["result_b"]["per_item"]`、`comp["per_item_delta"]`。

`build_comparison_result`(`compare.py`)**不需要改** —— 它已經保留 `result_a` / `result_b` 整包。

### 邊界處理

| 情境 | 行為 |
|---|---|
| `baseline_metrics is None`(section 關閉或 baseline 失敗)| 整個 section 返回 `None`(現有行為,不變)|
| `purchase_counts` 缺(向後相容)| 跳過 [1] 表,[2][3] 照常 |
| `per_item` 缺或為空 | 跳過 [3a][3b][3c] 三表 |
| 某產品在 Model 出現但 Baseline 缺(或反之)| `_compute_delta` 用 `.get(k, 0.0)` 補 0(現有行為,不變);M/B 欄該格顯示 `NaN`(pandas 自然行為)|
| `Macro 平均` 列 | 與現有 per-item section 一致;`_macro_row()` 抽出共用 helper(若已存在則直接 reuse)|

### 表格欄寬考量

[3a] 15 欄、[3b]/[3c] 各 12 欄。沿用現有 HTML table 樣式,使用者已熟悉;若有橫向超寬問題,後續可加 CSS overflow-x: auto,本次不做。

## 測試

`tests/test_evaluation/test_report_builder.py`(現有檔)新增/更新:

1. **`test_baseline_section_renders_popularity_table`** — 給有 `purchase_counts` 的 `baseline_metrics`,assert section 含三張(或四張)表、第一張 columns 為 `["count", "rank"]`、排序 desc by count、rank 從 1 起。
2. **`test_baseline_section_without_purchase_counts`** — 向後相容:`baseline_metrics` 不含 `purchase_counts` 時,只渲染 [2][3] 不報錯。
3. **`test_baseline_section_overall_three_cols`** — overall 表 columns 為 `["Model", "Baseline", "Delta"]`,值取自 result_a/result_b/delta。
4. **`test_baseline_section_per_item_three_families`** — per-item 表數量 = 3(recall/map_attr/ndcg_attr),每張 columns 命名含 `M`/`B`/`Δ`。
5. **`test_compute_baseline_metrics_returns_purchase_counts`**(Spark)— 用 small fixture,assert 回傳 dict 含 `purchase_counts: dict[prod, int]`、加總正確。

避免新增 long-running Spark 測試(CLAUDE.md:測試效能注意);(5) 用既有 spark fixture + 小資料即可。

## 風險與緩解

| 風險 | 緩解 |
|---|---|
| `compute_baseline_metrics` 回傳新 key,downstream consumer 假設 schema | 全 repo grep `compute_baseline_metrics` 與 `baseline_metrics[` 使用點;目前只在 evaluation pipeline 內流向 `build_baseline_section`,影響面小 |
| MLflow 或 manifest 序列化 baseline_metrics 時 schema 變化 | 確認 `data/evaluation/.../manifest.json` 不寫 baseline_metrics(目前只寫 parameters);若有則加 schema version |
| Per-item 表變寬,終端視覺擁擠 | report 是 HTML 不是終端;HTML table 本就可橫向滾(瀏覽器原生)|

## 檔案地圖

| 檔案 | 改動 |
|---|---|
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | `compute_baseline_metrics` 多算/多回傳 `purchase_counts` |
| `src/recsys_tfb/evaluation/report_builder.py` | 改寫 `build_baseline_section`:新增 popularity 表、overall 改 3 欄、per-item 擴成 3 族 × 3 欄 |
| `tests/test_evaluation/test_report_builder.py` | 新增 4 個 test case |
| `tests/test_evaluation/test_nodes_spark.py`(或同等) | 新增 1 個 Spark test for `purchase_counts` |

`src/recsys_tfb/evaluation/compare.py` 不動。
`src/recsys_tfb/evaluation/baselines.py` 不動(`compute_purchase_counts` 既有,只需在 node 內保留結果即可)。

## 驗收條件

1. 重跑 `python -m recsys_tfb evaluation --env production --post-training`,`data/evaluation/<v>/<snap>/report.html` 的「基準比較 Baseline」段含:popularity 排名表(8 列) + overall 三欄表 + 3 張 per-item 三欄表。
2. 新增/更新測試全綠。
3. `compute_baseline_metrics` 對舊測試的 assertion 仍通過(向後相容)。

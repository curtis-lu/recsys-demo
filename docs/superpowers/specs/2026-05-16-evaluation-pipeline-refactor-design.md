# Evaluation Pipeline 重構設計 (spec)

- 日期：2026-05-16
- 分支：`feat/evaluation-pipeline-refactor`
- 範圍：`src/recsys_tfb/evaluation/`、`src/recsys_tfb/pipelines/evaluation/nodes_spark.py`、`conf/base/parameters_evaluation.yaml`、新版 `evaluation_report.html`
- 指標語意基準：`docs/metrics_concept_map.html`

## 1. 目標與決策前提

- **主指標**：mAP@k（k = 1, 3, 5, all），per-query、細產品粒度。
- **護欄指標**：per_item recall@k（k = 1~5），呈現方式為**排序表 + 條件色階，不設 pass/fail 閾值**（人工判讀）。
- **產品大類**：與細產品**並行**的大類層級評估（細產品 22 類照算，另輸出大類粒度同套指標）。大類對應表寫在 parameters yaml、可開關。
- **彈性優先**：k 值集合、大類對應、各段 on/off、顯示用 k、色階端點皆走 config。
- 報告整體採「**讀者旅程 / 結論優先**」單一線性結構（已於設計階段選定）。

## 2. 報告內容規格（新版 evaluation_report.html，結構 A）

| 段 | 標題 | 內容 |
|---|---|---|
| §0 | 摘要卡片 | run metadata（model_version、snap_date、generated_at、n_queries、n_excluded）＋主指標卡片：細產品 overall mAP@1/3/5/all。純數值。 |
| §1 | 資料概況 | **總覽**：總筆數、客戶數(distinct cust_id)、產品數、snap_date 數、整體正樣本數、整體 positive rate、每客戶平均正樣本數(total_rel 平均＋分位)、n_excluded_queries。**by snap_date 表**：筆數／正樣本數／客戶數。**by prod_name 表**：筆數／正樣本數／客戶數／positive rate。**by 大類表**（大類開啟時）：同上。 |
| §2 | 主指標 mAP@k（細產品 per-query） | overall mAP@k 主表（顯示 k 由 `display.primary_map_k`）；附 precision/ndcg/HR@k 脈絡欄；`@all` 退化小字註記。 |
| §3 | 護欄 per_item recall@k（細產品） | 22 細產品 × recall@(1~5)，可排序＋條件色階；僅保留一欄 base rate 作判讀輔助，完整資料統計指向 §1（不重複 n_positives／客戶數）。欄位顯示名 `recall@k (per-item)`，連結 §8。 |
| §4 | 大類層級 | 大類 mAP@k + 大類 per_item recall@k＋色階；表旁註記每個大類由哪些子產品組成。`product_categories.enabled` 連動。 |
| §5 | 分群 Per-Segment | 每 segment 欄一表：mAP@k／recall@k by segment，segment 樣本統計併入同表（移除舊版 N 個重複的「Segment Dataset Statistics: X」段）。 |
| §6 | 診斷（可摺疊 `<details>`，預設收合） | score 分布／rank heatmaps／calibration，由 config 開關。 |
| §7 | 基準比較（optional） | 有 baseline 且開關開啟時：Model vs Baseline 的 overall mAP@k 與 per-item recall@k delta。 |
| §8 | 詞彙表 Glossary | 每指標一句語意 → 對應概念地圖定義。 |

原則：**§1 獨佔資料剖析**；指標段只放指標＋最低限度判讀輔助欄，避免重複段落。

## 3. metrics_spark.py 大類計算擴充

- 不複製指標邏輯。大類評估 = 將細產品 `eval_predictions` 折疊成大類粒度同形 DataFrame，再餵進**同一條 Layer1→3 pipeline**。
- 新增 `collapse_to_categories(eval_predictions, parameters) -> SparkDataFrame`（純 Spark、無 UDF）：
  - 由 config 對應表建 `(prod_name, category)` mapping DF，broadcast join。
  - `groupBy(snap_date, cust_id, category)`：大類 score = `max(子 score)`、大類 label = `max(子 label)`、segment 欄 = `first()`。
  - 不搬子 rank；折疊後由既有 `rank_within_query` 在大類粒度重算名次。
  - **等價性**：query 內 pos 由 score desc 決定，故 `max(子 score)` 重排 ⟺「取子產品最佳 rank」，且免 UDF。
- 輸出形狀（巢狀、對稱）：`compute_all_metrics` 結果新增 `category` key，子結構與頂層同形（`overall/per_item/per_segment/.../n_queries/n_excluded_queries`），`enabled` 才有。
- 新增 `dataset_overview` 子 dict（§1 統計，純 Spark 聚合後收斂）。
- **單一 k 超集**：metrics 層只算一份 `k_values` 聯集（預設 `{1,2,3,4,5,"all"}`），所有粒度（細產品 overall/per-item/per-segment＋大類）共用；報告各段再從 `display.*` 切要顯示的 k。指標層不依指標家族分叉 k。
- 約束：全程 groupBy/agg＋broadcast join，無 UDF、無網路、CPU 友善。

## 4. evaluation/ 模組邊界重整

| 模組 | 重構後職責 | 動作 |
|---|---|---|
| `metrics_spark.py` | Layer1–3 + `compute_all_metrics`；新增 `collapse_to_categories`；輸出加 `category`、`dataset_overview` | 擴充 |
| `report.py` | 低階 HTML primitives；移除死的 `metrics-table` CSS class | 微調 |
| `report_builder.py`（新） | §0–§8 段落組裝，一段一純函式 `(metrics dict 切片, parameters) -> ReportSection`，免 Spark 可單測；外加 `assemble_report(...) -> html` | 新增 |
| `distributions.py`／`calibration.py`／`statistics.py` | schema-driven：欄名由 `get_schema(parameters)` 傳入 | 改簽名 |
| `segments.py` | 收斂成單一 Spark `join_segment_sources(labels_sdf, segment_sources)`；**來源讀取隔離成薄 seam**（目前 `spark.read.parquet`，未來局部換 `spark.table`）；刪 pandas 重複版＋更新 test | 去重 |
| `compare.py` | 只留 §7 用到的 overall mAP@k delta + per-item recall@k delta；刪 `plot_comparison_score_distributions` 與未用 nested deltas | 瘦身 |
| `metrics.py`／`baselines.py` | 不動 | — |
| pipelines/evaluation/`nodes_spark.py` | `_render_html_report`（180 行）刪除，邏輯拆進 `report_builder`；`generate_report` 變薄；`prepare_eval_data` 僅把 inline segment 載入改呼叫 `segments.join_segment_sources`（簽名/輸出不變，不接手大類折疊） | 瘦身 |

**重構後資料流**

```
eval_predictions (Spark)
  → compute_metrics → { overall, per_item, per_segment, dataset_overview,
                         category:{…}, macro_avg, n_queries, … }   小 dict，全在 Spark 聚合完
  → generate_report
       · §0–§5,§7：純由 dict 組（無 toPandas）
       · §6 診斷：啟用才抽最小欄位 toPandas（可抽樣）→ 分布/calibration/heatmap
       · report_builder.assemble_report(…) → html
  → save_report / save_metrics_json
```

命名修正（C）：per-item 內部 key 不動（降風險），於 `report_builder` 顯示層正名為 `recall@k (per-item)`，§8 詞彙表釐清。

## 5. config / 錯誤處理 / 測試

### parameters_evaluation.yaml

```yaml
evaluation:
  k_values: [1, 2, 3, 4, 5, "all"]          # 單一 k 超集
  product_categories:
    enabled: true
    unmapped: singleton
    mapping:
      fund:     [fund_stock, fund_bond, fund_mix]
      exchange: [exchange_fx, exchange_usd]
      ccard:    [ccard_bill, ccard_cash, ccard_ins]
      # …其餘照 schema.categorical_values.prod_name 補齊
  segment_columns: [cust_segment_typ]        # 不變
  segment_sources: { holding_combo: {…} }    # 不變（來源 seam，未來換 Hive）
  report:
    sections:   { dataset_overview, primary_map, guardrail_recall,
                  category, per_segment, diagnostics, baseline }  # 各段 on/off
    display:
      primary_map_k:      [1, 3, 5, "all"]
      guardrail_recall_k: [1, 2, 3, 4, 5]
      recall_colorscale:  { low: 0.0, high: 1.0 }
    diagnostics:
      include_distributions: true
      include_calibration: true
      n_calibration_bins: 10
      sample_rows: null                      # 診斷段 toPandas 抽樣上限，null=全量
    baseline: { … 既有不變 … }
```

缺鍵走安全預設（全開、k 取超集）→ 向後相容。

### 錯誤處理

- 大類 mapping 引用不存在 prod_name → fail-loud（啟動時對照 `schema.categorical_values.prod_name`）。
- `product_categories.enabled: false` → 不跑 collapse、不輸出 `category`、§4 略過。
- segment_source 檔不存在 → warn 並略過（現行行為）。
- baseline 未提供 → §7 略過（現行行為）。

### 測試策略

- `report_builder` 每段純函式：小 metrics dict fixture 單測（免 Spark），延續 `test_report.py` 風格。
- `collapse_to_categories`：conftest session-scoped `spark`，斷言大類 score/label/rank 與 singleton。
- `dataset_overview` 聚合：小 Spark DF 斷言 by snap_date／產品／大類。
- schema-driven 視覺化：傳非預設欄名 A3 迴歸測試。
- `segments.join_segment_sources`：沿用 `test_segments.py` 四案例改測 Spark 單一實作。
- 既有 `test_compute_test_map_spark.py` 等不受影響（`compute_all_metrics` 既有頂層 key 不改名不移除，只新增 `category`／`dataset_overview`）。
- 端到端：dev-cluster `--env production` 跑 evaluation 冒煙（走 client-template 預設 conf）。

向後相容承諾：`compute_all_metrics` 既有頂層 key 不改名、不移除，只新增；per-item 命名只在顯示層正名。

## 6. Out of scope / 後續工作

- `segment_sources` 由 parquet 改為 Hive table、並新增簡易 source_etl pipeline 產該表 —— **本次不做**。本次只建立「單一函式 + 來源讀取隔離 seam」，讓未來切換為局部改動。
- 不順帶做與本目標無關的重構。

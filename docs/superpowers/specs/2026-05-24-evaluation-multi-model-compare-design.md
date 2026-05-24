# Evaluation 多模型比較設計 (Multi-Model Comparison)

**Date**: 2026-05-24
**Status**: Draft (awaiting user review)
**Scope**: evaluation pipeline 擴展，支援跟其他模型版本或別專案模型的 2-way 比較

## Goal

讓 `evaluation` pipeline 除了現有「Model vs popularity baseline」比較外，再支援：

1. **跟其他版本的同套模型比較**（換 `model_version`、同 prod_name universe）
2. **跟別專案既有模型比較**（外部 Hive 表、可能 prod_name 不同 / customer set 不同）

並在 common universe 上重新排序、重新計算指標，產出獨立的 `report_comparison.html`。

## Non-Goals

- N-way 比較（>= 3 個模型同列）— 第一版固定 2-way
- 跟外部 Parquet 檔比較 — 第一版限 Hive table（或同套 ranked_predictions）
- 從別人的 evaluation metrics artifact 讀數字比較 — 雙方都從 row-level predictions 進來、確保 common universe 重排重算
- 跨 snap_date 趨勢分析、`per_segment` 章節在 compare 報告中、diagnostics 章節在 compare 報告中、popularity composition — 全部明確不做
- `--use-cached-eval` 跳過 prepare 的 ergonomic flag — Phase 2 deferred
- `--no-persist-eval` 跳過 Hive 寫入的逃生門 — 第一版不引入
- N-way 比較版型重構 — 第一版用既有 `_per_item_metric_compare_table` (M/B/Δ)

## Decisions

整體設計分七段，按段落收斂於下：

### §1 — 模組邊界

```
src/recsys_tfb/
├── evaluation/
│   ├── comparison/                         # 新增 sub-package
│   │   ├── __init__.py
│   │   ├── sources.py                      # Spark：讀 A/B predictions、套 column mapping
│   │   ├── alignment.py                    # 純函式：算 common cust × prod universe
│   │   ├── restrict.py                     # Spark：filter + rank_within_query
│   │   └── report.py                       # 純函式：組裝 report_comparison.html
│   ├── compare.py                          # 不動（dict-driven A vs B delta，已是 generic）
│   ├── report.py                           # 不動（generate_html_report 復用）
│   ├── report_builder.py                   # 不動
│   ├── metrics_spark.py                    # 不動（compute_all_metrics 被 comparison 復用）
│   └── ...
└── pipelines/evaluation/
    ├── pipeline.py                         # 改：create_pipeline 接 compare_source / compare_only
    ├── nodes_spark.py                      # 不動（既有節點）
    └── comparison_nodes.py                 # 新增：load / restrict / generate_compare_report / persist 等節點
```

**模組責任**

| File | 責任 | 對應 Pipeline Node |
|---|---|---|
| `comparison/sources.py` | 讀 A/B raw predictions、column rename、prod_mapping rename + N:1 collapse | `load_compare_predictions` |
| `comparison/alignment.py` | 純函式：給 A/B DataFrames，回 `(common_cust_set, common_prod_set)` | `restrict_to_common` 上半 |
| `comparison/restrict.py` | 套 common 集合 filter + 重 `rank_within_query`，雙方各回一份 `*_common` DF | `restrict_to_common` 下半 |
| `comparison/report.py` | 給雙方 metrics dict + coverage 資訊，組 4 章 `ReportSection`、call `generate_html_report` | `generate_comparison_report` |

**邊界原則**

- `comparison/` 內部完全不知道 Pipeline / Node / Catalog — 只接 Spark DataFrame in / out + 純 dict 函式
- `pipelines/evaluation/comparison_nodes.py` 是 Pipeline-aware 層、把 `parameters['evaluation']['compare']` 解析成 `sources.load_compare_predictions(...)` 等呼叫的 thin shim
- `compare.py` 不動 — `build_comparison_result(metrics_a_dict, metrics_b_dict, label_a, label_b)` 已是 dict-level diff 工具，正好就是需要的

### §2 — Config schema

`conf/base/parameters_evaluation.yaml` 新增 `compare_sources` dict，key = source 名稱（CLI 用 `--compare <key>` dispatch）：

```yaml
evaluation:
  # ... 既有設定 ...

  compare_sources:
    # 同套 stack、不同 model_version
    v_prev:
      kind: model_version
      model_version: "2026-01-31_abcdef12_34567890"
      label: "v_prev (上一版)"

    # 別專案外部 Hive
    ext_proj_x:
      kind: external_hive
      table: other_project.predictions
      label: "External Project X"
      columns:
        cust_id: customer_id
        snap_date: as_of_date
        prod_name: item_code
        score: pred_score
      prod_mapping:
        ext_fund_a: fund_stock
        ext_fund_b: fund_bond
        ext_fund_mix: fund_mix
        ext_usd: exchange_usd
        ext_fx: exchange_fx
        ext_card_bill: ccard_bill
        ext_card_cash: ccard_cash
        ext_card_ins: ccard_ins
      unmapped_policy: fail   # fail (預設) | drop
```

**Grammar 規則**

- `kind` ∈ `{model_version, external_hive}`；A11 invariant 驗
- `label` 必填：報告上顯示的對手名稱（不只是 source key）
- `columns` 僅 `external_hive` 有意義；`model_version` 路線**禁止**寫 `columns` / `prod_mapping`（fail loud）
- `prod_mapping` 是「外部 prod_name → 本系統 prod_name」的 N:1 映射
- `unmapped_policy: fail` (預設) = 外部出現未涵蓋 prod 時 raise；`drop` = filter 掉

### §3 — CLI / Pipeline wiring

**CLI（`src/recsys_tfb/__main__.py`）**

```bash
# 既有用法不動：
python -m recsys_tfb evaluation --env production

# 一次出兩份 report
python -m recsys_tfb evaluation --env production --compare ext_proj_x

# 只跑 compare（讀已 persist 的 eval_predictions）
python -m recsys_tfb evaluation --env production --compare-only ext_proj_x
```

- `--compare` / `--compare-only` 互斥 (A13)
- 兩者帶值必須是 `compare_sources` 的 key (A12)
- CLI dispatch 把選定的 source dict 寫到 `parameters['evaluation']['compare']`、再呼叫 `create_pipeline(compare_source=..., compare_only=...)`

**Pipeline（`pipelines/evaluation/pipeline.py`）**

三種模式的節點組合：

```python
def create_pipeline(
    post_training: bool = False,
    compare_source: dict | None = None,
    compare_only: bool = False,
) -> Pipeline:
    # predictions_input: 既有 convention
    #   "training_eval_predictions" if post_training else "ranked_predictions"

    if compare_only:
        # 模式 3：只跑 compare，讀 Hive eval_predictions
        # CLI A12 已保證 compare_source is not None 在此分支
        nodes = [
            Node(load_eval_predictions_from_hive, ["parameters"],
                 outputs="eval_predictions"),
            Node(load_compare_predictions, ["parameters"],
                 outputs="compare_predictions_raw"),
            Node(restrict_to_common,
                 ["eval_predictions", "compare_predictions_raw",
                  "label_table", "parameters"],
                 outputs=["eval_predictions_common", "compare_predictions_common"]),
            Node(generate_comparison_report,
                 ["eval_predictions_common", "compare_predictions_common", "parameters"],
                 outputs="evaluation_comparison_report"),
        ]
        return Pipeline(nodes)

    # 既有 4 節點
    nodes = [
        Node(prepare_eval_data, [predictions_input, "label_table", "parameters"],
             outputs="eval_predictions"),
        Node(compute_metrics, ["eval_predictions", "parameters"],
             outputs="evaluation_metrics"),
        Node(compute_baseline_metrics, ["eval_predictions", "label_table", "parameters"],
             outputs="baseline_metrics"),
        Node(generate_report,
             ["eval_predictions", "evaluation_metrics", "parameters", "baseline_metrics"],
             outputs="evaluation_report"),
        # 新增 persist 節點：永遠寫，作為 --compare-only 與 ad-hoc 分析的 source of truth
        # outputs 是 sentinel — 框架要求節點有 output 作 DAG edge，但下游不消費
        Node(persist_eval_predictions, ["eval_predictions", "parameters"],
             outputs="eval_predictions_persisted_sentinel"),
    ]
    if compare_source is not None:
        nodes += [
            Node(load_compare_predictions, ["parameters"],
                 outputs="compare_predictions_raw"),
            Node(restrict_to_common,
                 ["eval_predictions", "compare_predictions_raw",
                  "label_table", "parameters"],
                 outputs=["eval_predictions_common", "compare_predictions_common"]),
            Node(generate_comparison_report,
                 ["eval_predictions_common", "compare_predictions_common", "parameters"],
                 outputs="evaluation_comparison_report"),
        ]
    return Pipeline(nodes)
```

**Catalog（`conf/base/catalog.yaml`）**

- 既有 `evaluation_report` 不動
- 新增 `evaluation_comparison_report`：`report_comparison.html`（同目錄）
- 新增 `eval_predictions` Hive dataset：`ml_recsys.eval_predictions`、partition `(snap_date, model_version)`、INSERT OVERWRITE PARTITION 語意（既有 `HiveTableDataset` 已支援）
- `eval_predictions_persisted` 是 placeholder（不映射實體檔）、純為了讓 persist 節點接得上 DAG

### §4 — Data flow & 模組行為

**整體 flow（`--compare X` 模式）**

```
ranked_predictions     label_table   parameters
        │                  │             │
        ▼                  ▼             │
  prepare_eval_data ───────────────────► │
        │                                │
        ▼ eval_predictions               │
        ├──────► compute_metrics ───────► evaluation_metrics
        ├──────► compute_baseline_metrics ──► baseline_metrics
        ├──────► generate_report ────────────► report.html
        ├──────► persist_eval_predictions ──► Hive ml_recsys.eval_predictions
        │
        ▼
   load_compare_predictions(parameters)
        │  kind=model_version → ranked_predictions filter
        │  kind=external_hive → spark.table + column rename
        │  套 prod_mapping (rename + groupBy max(score) collapse)
        ▼ compare_predictions_raw
   restrict_to_common(eval_predictions, compare_predictions_raw, parameters)
        │  alignment.common_universe → (common_cust, common_prod)
        │  雙方 filter to common + rank_within_query
        ▼ (eval_predictions_common, compare_predictions_common)
   generate_comparison_report(...)
        │  compute_all_metrics(A_common) → metrics_a
        │  compute_all_metrics(B_common) → metrics_b
        │  build_comparison_result(metrics_a, metrics_b, label_a, label_b) → comp
        │  comparison.report.assemble(comp, coverage_info, parameters) → HTML
        ▼ report_comparison.html
```

**`sources.load_compare_predictions(parameters, spark)`**

- 讀 `parameters['evaluation']['compare']`（CLI dispatch 寫入）
- `kind == "model_version"`：`spark.table("ranked_predictions").filter(model_version == X & snap_date == eval_snap_date)`
- `kind == "external_hive"`：`spark.table(table).filter(snap_date_col == eval_snap_date)`、然後 `select(*[col(ext).alias(internal) for internal, ext in columns.items()])` 統一 schema
- 套 `prod_mapping`：
  - 先 `.replace(prod_mapping, subset=[prod_name])` rename
  - 若 `unmapped_policy == "fail"`：collect distinct prod_name set、跟 mapping key set 比，有缺就 raise (B2)
  - `unmapped_policy == "drop"`：filter `prod_name in mapping_values` 並 log warning
- N:1 collapse：rename 後同一位 cust 可能對同一本系統 prod 有多筆 → `groupBy(cust_id, snap_date, prod_name).agg(max(score))`（同 `product_categories` collapse 邏輯）

**`alignment.common_universe(a, b, schema) -> (set[cust], set[prod])`**

- 純 Python set 運算：兩邊各 `select(<cust>).distinct().rdd.flatMap(...)` collect 後做 set intersection
- 客戶 intersection、產品 intersection 分開算（不是 (cust, prod) 對的 intersection）
- 任一 empty → raise (B3)

**`restrict.restrict_to_common(a, b, label_table, common_cust, common_prod, schema) -> (a_common, b_common)`**

- Filter 用 inner join with single-column DF（broadcast hint 在 cust 大時更穩、不靠 `.isin`）
- candidate set 縮了 → 重 `rank_within_query(df, [snap_date, cust_id], score)`、覆寫舊 rank 欄
- A 端 (`eval_predictions`) 上游 `prepare_eval_data` 已 LEFT JOIN 過 label、本步驟對 A **保留既有 label 欄**、不重 join
- B 端 (`compare_predictions_raw`) **沒有** label → 本步驟對 B 從 `label_table` LEFT JOIN（同 `prepare_eval_data` 邏輯）、missing fillna(0)、確保「比的是同一個 ground truth」

**`comparison.report.assemble(comp, coverage_info, parameters)`**

- 復用 `evaluation/report.py::generate_html_report`、`ReportSection` dataclass
- `coverage_info = {"n_cust_A_full": ..., "n_cust_B_full": ..., "n_cust_common": ..., "n_prod_A_full": ..., "n_prod_B_full": ..., "n_prod_common": ..., "dropped_prods_A": [...], "dropped_prods_B": [...]}`
- 章節結構見 §5

**Spark 注意點**

- 模式 1 / 2 下 `eval_predictions` 有 4 個 consumer (compute_metrics / compute_baseline_metrics / generate_report / persist_eval_predictions)，現有 evaluation pipeline 已是這個負擔 + 1 (compute_baseline + persist 都是新增的消費者，前者已 merge)
- 是否要在 `prepare_eval_data` 末加 `.cache()` 待之後 perf 確認再優化 — 不在此設計範圍
- `compute_all_metrics(B_common, parameters)` 餵 `parameters['evaluation']['k_values']`，但 `"all"` 在 common universe 上是「common universe 內 all」、跟 single-model 不同；compare 報告上下文使讀者自然理解

### §5 — `report_comparison.html` 章節

**章節 1：Compare 概頁**

兩張表：

- 雙方 metadata：label / kind / model_version 或 Hive table / snap_date / n_cust(full) / n_prod(full) / generated_at
- coverage 三欄：A_full / B_full / common (報告使用的 universe)、含 n_cust / n_prod / n_query
- 被 drop 的 prods：A 端、B 端各列 count + 名稱清單

**章節 2：Overall metrics (M/B/Δ)**

跟既有 `build_baseline_section` 的 overall 表完全同款、欄名換 `label_a` / `label_b`、數字是 common universe 上重算。

**章節 3：Per-item M/B/Δ**

復用既有的 `_per_item_metric_compare_table` helper，三張表：

- per-item recall@k (M/B/Δ)
- per-item map_attr@k (M/B/Δ)
- per-item ndcg_attr@k (M/B/Δ)

頂列 `Macro 平均`（既有機制）；rows 是 common prods。

**章節 4：Category M/B/Δ**

當 `evaluation.product_categories.enabled = true` 才加：

- A_common / B_common 各 collapse 到大類（既有 `collapse_to_categories`）、雙方 compute_all_metrics、build_comparison_result
- 大類同樣取 intersection — 只有「雙方 mapped 後都覆蓋」的大類才列

**章節 5：詞彙表**

完全沿用 `build_glossary_section`。

**明確排除的章節**

- `headline mAP@k`（§1 概頁已涵蓋）
- `dataset_overview`（跟 report.html 重複）
- `per_segment`（compare 場景需重新設計 segment 對齊；deferred）
- `diagnostics`（single-model 概念）
- `popularity composition`（跨模型比無意義）

**TOC / 樣式**

完全復用 `report.py` 既有 vertical TOC + Back-to-Top；降低使用者切換認知成本。

### §6 — Fail-loud invariants

延續 `core/consistency.py` 既有 A1-A6 / B1 體系：

| ID | Predicate | When | Behavior |
|---|---|---|---|
| **A11** | `compare_source_well_formed` | parameters load | `kind` ∈ enum、必填欄齊、`model_version` 路線不得有 `columns` / `prod_mapping` |
| **A12** | `compare_source_key_exists` | CLI 解析 `--compare`/`--compare-only` | `X` 必須是 `compare_sources` 的 key |
| **A13** | `compare_mutual_exclusive` | CLI parse | `--compare` 跟 `--compare-only` 不得同帶 |
| **B2** | `compare_unmapped_products` | `load_compare_predictions` 執行 | `unmapped_policy=fail` 遇缺 mapping → `DataConsistencyError`；`drop` → log warning + filter |
| **B3** | `compare_common_universe_nonempty` | `restrict_to_common` 執行 | `common_cust` 或 `common_prod` 為空 → `DataConsistencyError` |
| **B4** | `compare_eval_predictions_partition_exists` | `load_eval_predictions_from_hive` | Hive 對應 `(snap_date, model_version)` partition 不存在 → fail loud，附訊息「請先以 `--compare X` 或不帶 flag 跑一次 evaluation」 |

**Schema mismatch（external_hive 路線）**

- `cust_id` / `snap_date` / `prod_name` / `score` 四欄缺一 → raise
- snap_date filter 後 isEmpty → 「外部表在 snap_date X 無資料」
- score 非 numeric → raise

**不做**

- score scale 對齊 / normalize（ranking metric 只看排名）
- 客戶 id 型別對齊預警（join 不上會在 B3 fail loud）
- prod_mapping 對 A 端 unknown prod 預警（跟 A6 既有重疊）

**半成功處理**

- `--compare X` 模式下 compare 章節失敗、**不阻斷** report.html 產出（compare 在 DAG 葉節點、不被 generate_report 依賴）
- 任一 fail loud 都是 `ConfigConsistencyError`（CLI 階段）或 `DataConsistencyError`（pipeline 執行）、延續 collect-all 哲學

### §7 — 測試策略

**新增測試檔案**

```
tests/test_evaluation/
  test_comparison_sources.py
  test_comparison_alignment.py
  test_comparison_restrict.py
  test_comparison_report.py

tests/test_pipelines/
  test_evaluation_compare_pipeline.py

tests/test_core/
  test_consistency_compare.py
```

**每檔要驗的核心 case**

- `sources`: 兩 kind 路線、N:1 collapse 後 `(cust, snap, prod)` 唯一 + score = max、unmapped fail / drop 兩語意
- `alignment`: cust / prod intersection 正確、單邊空 → raise
- `restrict`: row 全在 common (cust × prod)、rank 覆寫為 1..n_common、label LEFT JOIN + fillna(0)
- `report`: 4 章都渲染、coverage 數字正確、M/B/Δ 齊、Δ = (A − B)、categories 條件啟用、雙方 label 正確顯示
- `pipeline`: 三模式各驗一遍（無 flag / --compare / --compare-only）、A13 互斥、B4 partition 不存在
- `consistency`: A11/A12/A13/B2 各分支

**效能注意**

- alignment 純 Python 最快；其他 Spark 測試用最小 fixture（3-5 cust × 4-5 prod）
- 不為了覆蓋率塞大資料；不複測 unit 已驗過的行為
- 估增 ~25-35 個測試、總時間估 +2-4 分鐘

**Regression 守護**

- 既有 `test_evaluation_pipeline*.py` 不動、加「不帶 --compare 時 pipeline 結構等同既有」assertion
- 既有 `test_baseline_section_*` 不動

**不寫的測試**

- dev-cluster integration smoke（依 CLAUDE.md SOP 人工 ad-hoc）
- prod 規模 perf benchmark
- HTML pixel-level snapshot（既有 dict-driven 風格）

## Open Questions

- `eval_predictions` 的 4 個 consumer 是否需要在 `prepare_eval_data` 末加 `.cache()` — 待 perf 觀察、不在此設計範圍
- `--use-cached-eval` flag（讀 Hive 跳 prepare）— Phase 2 deferred、目前的 dev iteration loop 上 prepare_eval_data 不是 bottleneck

## Out of Scope

- N-way 比較版型重構
- 從別人的 evaluation metrics artifact 讀數字比較
- 跨 snap_date 趨勢分析、per_segment / diagnostics / popularity composition 在 compare 報告中
- `--use-cached-eval` / `--no-persist-eval` flag

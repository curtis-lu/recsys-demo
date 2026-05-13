# Evaluation Spark Metrics Refactor — Design

**Date**: 2026-05-12
**Status**: Approved for implementation
**Phase**: Phase 2 of evaluation-pipeline simplification (Phase 1 已完成 MRR / micro_avg / 死代碼移除)

## 1. Motivation

`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:compute_metrics` 目前是一個 ~150 行的「fat function」，混合了 Spark window 聚合、pandas 收回後再聚合、以及多種 metric 維度組裝。問題：

1. **Fat function**：單一函式同時負責 join / per-query / per-product / per-segment / per-product-segment / macro / I/O。
2. **擴充困難**：欄位命名不一致（`precision_at_pos` vs `precision`、`r_per_query` vs `total_rel`、`ndcg_k_contrib@K` vs `dcg_contrib`），加新 metric / 新 dimension 要改多處。
3. **可讀性差**：高資訊密度、註解不足、Spark/pandas 邏輯交錯。
4. **效能問題**：`enriched_pd = df.filter(F.col("total_rel") > 0).toPandas()` 把 row-level 資料拉回 driver，跟「Spark-native 計算」的目標相違。

## 2. Goals & Non-Goals

### Goals
- Spark-native：row-level 資料完全停留 Spark，只 collect small aggregated dicts。
- 模組化、職責單一：每個函式只做一件事，輸入輸出顯式。
- 對外契約不變：`compute_metrics` 仍接 `(eval_predictions: SparkDataFrame, parameters: dict)`，仍回相同形狀的 result dict，下游 `generate_report` / `compute_baseline_metrics` 不需動。
- 命名一致：欄位採統一命名（見 §6 Data Schema）。
- 直接從 Hive 來源（`training_eval_predictions` / `ranked_predictions`，由 `prepare_eval_data` 產生的 SparkDataFrame）端到端在 Spark 算完。

### Non-Goals
- 不動 `evaluation/metrics.py`（pandas 實作，目前由 training / baselines / segments 使用）。Phase 1 已清除 MRR / micro_avg / 死代碼。
- 不動 `compare.py` / `segments.py` / `report.py` / `statistics.py` / `calibration.py` / `distributions.py`。
- 不動 pipeline 接線（`pipeline.py`）。
- 不動 `prepare_eval_data` / `generate_report` / `_render_html_report`。
- 不修改 metric 語意（per-product 的 `precision@K` 與 `recall@K` 仍同值，沿用目前 pandas semantic — 留給未來另案）。
- 不加新 metric 或新 dimension（只讓未來加它們變容易）。

## 3. Files Changed

| 動作 | 路徑 | 說明 |
|---|---|---|
| 新增 | `src/recsys_tfb/evaluation/metrics_spark.py` | ~250 行，Spark-native 計算 |
| 修改 | `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | `compute_metrics` 縮為 ~15 行 wrapper，移除所有 pandas helper import |
| 新增 | `tests/test_evaluation/test_metrics_spark.py` | unit + integration + parity tests |

## 4. Module Breakdown

`evaluation/metrics_spark.py` 拆成 **7 個單一職責函式** + 1 個 orchestrator。

### Stage A — Pipeline stages (`SparkDataFrame → SparkDataFrame`, 可疊加)

```python
def rank_within_query(df, group_cols, score_col) -> SparkDataFrame:
    """加 `pos`：query 內依 score desc 的 1-based rank。"""

def add_query_aggregates(df, group_cols, label_col) -> SparkDataFrame:
    """加 `total_rel`：每 query 正樣本數（Window sum）。"""

def add_row_contributions(df, group_cols, label_col, k_values) -> SparkDataFrame:
    """加 row-level contribution 欄位：
       恆有：cum_rel, prec_at_pos, dcg_term
       per-K：top_k@K, ap_contrib@K, ndcg_contrib@K
    iDCG@K 用 Spark `aggregate(sequence(1, least(total_rel, K)), 0.0, …)` inline。
    """
```

### Stage B — Aggregators (`SparkDataFrame → dict`, **僅此處 collect**, 結果 small)

```python
def aggregate_overall(enriched, group_cols, label_col, k_values) -> dict:
    """per-query metrics → cross-query mean。Collect 1 row。"""

def aggregate_by_query_dimension(enriched, dim_col, group_cols, label_col, k_values) -> dict:
    """per-query metrics → groupBy(dim_col).mean。Equal customer weight。
    對應 pandas per_segment 語意。Collect ~數個 segment row。"""

def aggregate_by_row_dimension(enriched, dim_cols, label_col, k_values) -> dict:
    """filter(label==1) → groupBy(dim_cols).mean(contributions)。
    對應 pandas per_product / per_product_segment 語意。
    Collect ~22 個 product 或 ~22×N 個 product×segment。"""

def macro_average(per_dim: dict) -> dict:
    """純 python dict mean。直接 import `metrics._macro_average` 重用。"""
```

### Stage C — Orchestrator (~50 行純組裝)

```python
def compute_all_metrics(eval_predictions: SparkDataFrame, parameters: dict) -> dict:
    """讀 schema/k_values → Stage A pipeline → Stage B aggregators → 拼結果。
    Returns dict same shape as pandas `compute_all_metrics`."""
```

### `nodes_spark.py:compute_metrics` (~15 行 wrapper)

```python
def compute_metrics(eval_predictions: SparkDataFrame, parameters: dict) -> dict:
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
    result = compute_all_metrics(eval_predictions, parameters)
    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        result["n_queries"], result["n_excluded_queries"],
    )
    return result
```

### 職責邊界

| Layer | 職責 | 不該做 |
|---|---|---|
| Stage A | 加欄位、純 transformation | 不 collect、不 agg、不寫 dict |
| Stage B | groupBy + agg + collect、組 python dict | 不算 contribution |
| Stage C | 讀 parameters、串 A→B、組最終 dict | 沒有公式、沒有 Window/groupBy 邏輯 |
| `nodes_spark.compute_metrics` | logging、回傳 | 沒有任何計算 |

## 5. Data Flow

```
eval_predictions (SparkDataFrame, prepare_eval_data 已 join label)
  columns: [snap_date, cust_id, prod_name, score, rank, label, model_version,
            (optional) segment_cols...]
      │
      ▼
rank_within_query(df, [snap_date, cust_id], "score")
  ► +pos
      │
      ▼
add_query_aggregates(df, [snap_date, cust_id], "label")
  ► +total_rel
      │  -- count n_queries_total (distinct group keys, before filter)
      │  -- filter(total_rel > 0); count n_queries_with_pos
      ▼
add_row_contributions(df, [snap_date, cust_id], "label", k_values)
  ► +cum_rel, +prec_at_pos, +dcg_term
  ► +top_k@K, +ap_contrib@K, +ndcg_contrib@K  (per K)
  .cache()  -- 後面 4 個 aggregator 重用
      │
      ├──► aggregate_overall ──────────────► overall
      ├──► aggregate_by_row_dimension([prod]) ──► per_product
      ├──► aggregate_by_query_dimension(seg) ─► per_segment
      └──► aggregate_by_row_dimension([prod, seg]) ► per_product_segment
                       │
                       ▼
              macro_average for each per_dim
                       │
                       ▼
         Final dict {overall, per_product, per_segment,
                     per_product_segment, macro_avg,
                     n_queries, n_excluded_queries}
```

## 6. Data Schema (Enriched DataFrame Columns)

| 欄位 | 型別 | 公式 | 階段 |
|---|---|---|---|
| `pos` | int | `row_number() over Window(group, score desc)` | rank_within_query |
| `total_rel` | int | `sum(label) over Window(group)` | add_query_aggregates |
| `cum_rel` | int | `sum(label) over Window(group, pos, rowsUnboundedPreceding..currentRow)` | add_row_contributions |
| `prec_at_pos` | double | `cum_rel / pos` | add_row_contributions |
| `dcg_term` | double | `label / log2(pos + 1)` | add_row_contributions |
| `top_k@K` | double | `(pos ≤ K) cast double` | add_row_contributions |
| `ap_contrib@K` | double | `prec_at_pos × label × top_k@K` | add_row_contributions |
| `ndcg_contrib@K` | double | `dcg_term × top_k@K / iDCG@K`（iDCG@K=0 時取 0） | add_row_contributions |

`iDCG@K` per row（同 query 內所有 row 相同值）：

```python
F.aggregate(
    F.sequence(F.lit(1), F.least(F.col("total_rel"), F.lit(k))),
    F.lit(0.0),
    lambda acc, i: acc + F.lit(1.0) / F.log2(i.cast("double") + F.lit(1.0)),
)
```

## 7. Aggregator Output Formulas

### `aggregate_overall`

```
per_query_ap@K        = sum(ap_contrib@K)         / total_rel
per_query_ndcg@K      = sum(ndcg_contrib@K)        -- 已含 iDCG 標準化
per_query_precision@K = sum(label × top_k@K)      / K
per_query_recall@K    = sum(label × top_k@K)      / total_rel

overall metric@K = mean over queries of per_query metric@K
```

> Note：`label × top_k@K` 不另存 row-level column，aggregator 內以
> `F.sum(F.col(label_col) * F.col(f"top_k@{k}"))` 計算。原因：此乘積只在
> overall 端用到、不參與 per-dimension 聚合，多存一欄空間浪費。

### `aggregate_by_row_dimension` (per-product / per-product-segment)

```
filter label = 1, groupBy(dim_cols):
  dim_map@K       = mean(ap_contrib@K)
  dim_ndcg@K      = mean(ndcg_contrib@K)
  dim_precision@K = mean(top_k@K)        -- 與 dim_recall@K 同值，沿用 pandas 語意
  dim_recall@K    = mean(top_k@K)
```

### `aggregate_by_query_dimension` (per-segment, equal customer weight)

```
1. groupBy(group_cols + [seg_col]).agg(per-query metric formulas)  -- 得 per-query metrics + seg
2. groupBy(seg_col).mean(per-query metrics)                         -- equal customer weight
```

## 8. Edge Cases

| 情境 | 處理 |
|---|---|
| 沒有任何正樣本的 query | `total_rel = 0`，`add_row_contributions` 前 filter，計入 `n_excluded_queries` |
| 整體無正樣本 | early return：`per_product = per_segment = per_product_segment = {}`、`overall = {}` |
| segment 欄位缺 | `per_segment = per_product_segment = {}`；`macro_avg` 不含 `by_segment` / `by_product_segment` key |
| `iDCG@K = 0` | 只可能 `total_rel = 0` 時發生，已被前面 filter；公式內仍以 `when(idcg>0, …, 0.0)` 防禦 |
| `k_values` 含 `"all"` | reuse `metrics._resolve_k_values`（純 list 操作），先 `n_products = df.select(item_col).distinct().count()` |
| multiple segment columns | 與目前一致，只用 `segment_columns` 中第一個出現在 df.columns 的欄位 |

## 9. `.cache()` 策略

`add_row_contributions` 輸出的 enriched df 被 4 個 aggregator 共用，必須 `.cache()`，orchestrator 結束時 `.unpersist()`。

## 10. Testing Strategy

新增 `tests/test_evaluation/test_metrics_spark.py`，4 層：

### Layer 1 — Stage A 函式 (unit)

小 fixture (~10 rows, 2 customers, 3 products)，驗欄位 + 數值：

- `test_rank_within_query_assigns_1_based_pos`
- `test_rank_within_query_sorts_by_score_desc`
- `test_add_query_aggregates_total_rel_per_query`
- `test_add_row_contributions_adds_expected_columns`
- `test_add_row_contributions_idcg_handles_total_rel_lt_k`
- `test_ap_contrib_formula`（手算對照）
- `test_ndcg_contrib_formula`（手算對照，K < total_rel 與 K ≥ total_rel 兩情境）

### Layer 2 — Aggregator 函式 (unit)

已 enriched 的 fixture：

- `test_aggregate_overall_returns_expected_keys`
- `test_aggregate_overall_known_values`（2 客戶 × 3 產品手算）
- `test_aggregate_by_row_dimension_keyed_by_dim_value`
- `test_aggregate_by_row_dimension_filters_to_label_1`
- `test_aggregate_by_query_dimension_equal_customer_weight`

### Layer 3 — End-to-end `compute_all_metrics` (integration)

對齊 pandas `test_per_product_map_known_values` 的 hand-computed fixture：

- `test_compute_all_metrics_returns_expected_keys`
- `test_compute_all_metrics_per_product_map_known_values`（per_product["A"].map@3 == 1.0、["C"].map@3 == 2/3）
- `test_compute_all_metrics_excluded_queries_counted`
- `test_compute_all_metrics_no_segment_column`
- `test_compute_all_metrics_with_segment_column`
- `test_compute_all_metrics_default_k_values_resolves_all`

### Layer 4 — Spark / pandas parity (cross-validation)

```python
def test_spark_pandas_parity_overall_and_per_product(spark):
    """同份資料，兩個 engine 數值等價。"""
    pred_pd, label_pd = _make_test_data(n_customers=30, seed=42)
    eval_pd = pred_pd.merge(label_pd, on=["snap_date", "cust_id", "prod_name"])
    eval_spark = spark.createDataFrame(eval_pd)
    parameters = {
        "schema": {  # 使用 core.schema 預設值即可
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
        },
        "evaluation": {"k_values": [3, 5], "segment_columns": ["cust_segment_typ"]},
    }

    result_pandas = compute_all_metrics(pred_pd, label_pd, k_values=[3, 5])
    result_spark = metrics_spark.compute_all_metrics(eval_spark, parameters)

    # rtol 1e-6：Spark 與 pandas 浮點累加順序不同，1e-9 對 nDCG 會 flaky
    _assert_metrics_close(result_pandas["overall"], result_spark["overall"], rtol=1e-6)
    _assert_metrics_close(result_pandas["per_product"], result_spark["per_product"], rtol=1e-6)
    _assert_metrics_close(result_pandas["per_segment"], result_spark["per_segment"], rtol=1e-6)
    assert result_pandas["n_queries"] == result_spark["n_queries"]
    assert result_pandas["n_excluded_queries"] == result_spark["n_excluded_queries"]
```

`_assert_metrics_close` 為本檔內 helper：遞迴比對 nested dict 的數值（rtol/atol 透過 `math.isclose`）。

### Spark fixture

repo 已有 conftest 提供 `spark` fixture，test module scope 重用 SparkSession。

### 不寫

- 不在 dev-cluster 跑（local SparkSession 即可）
- 不測 `nodes_spark.compute_metrics` wrapper 行為
- 不測 `_resolve_k_values`（pandas 端已測）

## 11. Migration Plan (Outline — 詳細留給 implementation plan)

1. 新增 `metrics_spark.py` 與 Stage A / B / C 函式（不接 pipeline）
2. 寫 Layer 1 / 2 / 3 / 4 測試，pytest 全綠
3. 改 `nodes_spark.py:compute_metrics` 為 thin wrapper
4. 跑全 evaluation tests + parity test 驗證
5. dev-cluster local 跑 evaluation pipeline 做一次 smoke test，比對 result dict 與舊版近似（rtol 1e-9）

## 12. Open Questions

無 — 所有設計分支點已於 brainstorming 過程取得用戶決議。

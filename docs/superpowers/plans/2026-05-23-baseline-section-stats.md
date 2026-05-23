# Baseline Section Stats + Side-by-Side Metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand evaluation `report.html` 的「基準比較 Baseline」段，從只顯示 delta 改為:popularity 排名組成表 + overall metrics M/B/Δ + per-item recall@k / map_attr@k / ndcg_attr@k 各一張 M/B/Δ。

**Architecture:** 一個資料管線改動(`compute_baseline_metrics` 多回 `purchase_counts`),其餘為純 report 渲染改動(`build_baseline_section` + 新 helper `_per_item_metric_compare_table`)。`compare.py` / `baselines.py` / pipeline wiring 都不動。

**Tech Stack:** Python 3.10、pandas 1.5.3、pytest 7.3.1、PySpark 3.3.2(僅一個既有 Spark 測試需更新 assert)。

**Spec:** `docs/superpowers/specs/2026-05-23-baseline-section-stats-design.md`

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats`
**Branch:** `feat/baseline-section-stats`

---

## Pre-flight(每次新 session 都要做)

- [ ] **Step P-1: 確認 worktree venv symlink 正確**

Run:
```bash
readlink /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats/.venv
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
```
Expected:
```
/Users/curtislu/projects/recsys_tfb/.venv
Python 3.10.9
```
任一不符 → 依 `docs/worktree-venv-setup.md` §修復後再繼續。

- [ ] **Step P-2: 確認分支正確**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats branch --show-current
```
Expected: `feat/baseline-section-stats`

---

## File Map(全部相對 worktree 根)

| 檔案 | 動作 | 責任 |
|---|---|---|
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | Modify (`compute_baseline_metrics`) | 多算/多回傳 `purchase_counts: dict[prod, int]`(跨 eval snap_dates 加總) |
| `src/recsys_tfb/evaluation/report_builder.py` | Modify (`build_baseline_section`) + Add helper `_per_item_metric_compare_table` | 改寫成三大區塊;新增可重用的「M/B/Δ 交織」per-item helper |
| `tests/test_evaluation/test_report_builder.py` | Modify + Add tests | 既有 `test_baseline_section_has_per_item_recall_delta` / `test_baseline_section_no_per_item_delta_omits_table` 兩個 case 對 baseline section 形狀做 assertion,要改/新增 |
| `tests/test_pipelines/test_evaluation/test_nodes_spark.py` | Modify + Add tests | 既有 `test_returns_overall_and_per_item` 的 `set(...keys) == {"overall","per_item"}` assert 要擴成 `{...,"purchase_counts"}` ;另新增正向 assert |

`src/recsys_tfb/evaluation/compare.py`、`src/recsys_tfb/evaluation/baselines.py`、`src/recsys_tfb/pipelines/evaluation/pipeline.py` 都不動。

---

## Task 1:`compute_baseline_metrics` 回傳新增 `purchase_counts`

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (lines 161–206 的 `compute_baseline_metrics`)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py` (class `TestComputeBaselineMetrics` lines 377–448)

### 設計

`compute_purchase_counts` 已回 Spark DataFrame `(time_col, item_col, score_col)`,其中 `score_col` 為次數(double)。我們把它在 driver 端 collect → 跨 `time_col` 加總成 `dict[prod_name, int]`,塞進回傳 dict。

`evaluation.snap_date` 目前是單值,所以「跨 snap_dates 加總」實務上就是該 snap 的值;預留 future-proof。

回傳 schema 由 `{"overall": {...}, "per_item": {...}}` 變成 `{"overall": {...}, "per_item": {...}, "purchase_counts": {prod: int}}`。

- [ ] **Step 1-1: 改既有 Spark 測試 — 把 keys assert 從 `{"overall","per_item"}` 擴成 `{"overall","per_item","purchase_counts"}`,並驗 `purchase_counts` 內容**

編輯 `tests/test_pipelines/test_evaluation/test_nodes_spark.py` line 425–436(`test_returns_overall_and_per_item`):

```python
    def test_returns_overall_and_per_item(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes_spark import (
            compute_baseline_metrics,
        )

        result = compute_baseline_metrics(
            self._eval_predictions(spark),
            self._label_table(spark),
            self._parameters(),
        )
        assert set(result.keys()) == {"overall", "per_item", "purchase_counts"}
        assert "A" in result["per_item"]
        # purchase_counts comes from _label_table fixture (snap=2024-06-30
        # falls inside the [2024-01-31, 2025-01-31) lookback window for the
        # 2025-01-31 eval snap). A=3 positives (h0/h1/h2 all label=1),
        # B=1 (only h0 label=1), C=0.
        assert result["purchase_counts"] == {"A": 3, "B": 1, "C": 0}
```

- [ ] **Step 1-2: Run failing test**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestComputeBaselineMetrics::test_returns_overall_and_per_item -v
```
Expected: FAIL with `AssertionError` on `set(result.keys()) == {...}` — keys 仍是 `{"overall", "per_item"}`。

- [ ] **Step 1-3: 修改 `compute_baseline_metrics` 多算/多回傳 `purchase_counts`**

編輯 `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`,把整個函式 body(line 165–206)改成:

```python
    """Popularity-baseline metrics, aligned row-for-row with eval_predictions.

    Re-scores each eval_predictions row with the product's historical
    purchase count, then runs the slim metrics path (overall + per_item).
    Returns None when the baseline report section is disabled — the second
    metrics pass is then skipped entirely.

    Returns dict with keys:
      - overall:        dict[str, float]   slim metrics
      - per_item:       dict[str, dict]    per-product slim metrics
      - purchase_counts: dict[str, int]    per-product popularity count
            aggregated across eval snap_dates (sum). Drives the report's
            popularity-composition table; consumers must treat absence
            as backward-compatible (older results may omit it).
    """
    from recsys_tfb.evaluation.baselines import (
        build_baseline_frame,
        compute_purchase_counts,
    )
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    eval_params = parameters.get("evaluation", {}) or {}
    sections = (eval_params.get("report", {}) or {}).get("sections", {}) or {}
    if not sections.get("baseline", True):
        logger.info(
            "Baseline report section disabled — skipping baseline metrics"
        )
        return None

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    lookback_months = (eval_params.get("baseline", {}) or {}).get(
        "lookback_months", 12
    )

    snap_dates = [
        str(r[time_col])
        for r in eval_predictions.select(time_col).distinct().collect()
    ]
    counts = compute_purchase_counts(
        label_table, snap_dates, lookback_months, parameters
    )
    # Aggregate per-product count across eval snap_dates (sum). Single-snap
    # evaluation reduces to that snap's value. cast to int for clean JSON
    # serialisation in manifests / reports.
    purchase_counts = {
        str(r[item_col]): int(r[score_col])
        for r in counts.groupBy(item_col)
        .agg(F.sum(F.col(score_col)).alias(score_col))
        .collect()
    }
    baseline_frame = build_baseline_frame(eval_predictions, counts, parameters)
    metrics = compute_overall_per_item(baseline_frame, parameters)
    metrics["purchase_counts"] = purchase_counts
    logger.info(
        "Baseline metrics computed (overall + per_item) for snap_dates=%s; "
        "purchase_counts has %d products",
        snap_dates, len(purchase_counts),
    )
    return metrics
```

- [ ] **Step 1-4: Run test to verify pass**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py::TestComputeBaselineMetrics -v
```
Expected: PASS(整個 class 兩個 case)。

- [ ] **Step 1-5: 跑同目錄其他既有 baseline / nodes Spark 測試確保沒退化**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py \
  tests/test_evaluation/test_baselines.py -q
```
Expected: 全綠。

- [ ] **Step 1-6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats add \
  src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats commit -m \
  "feat(evaluation): compute_baseline_metrics returns purchase_counts"
```

---

## Task 2:Baseline section 新增 popularity 排名組成表

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py` (`build_baseline_section` lines 397–435)
- Test: `tests/test_evaluation/test_report_builder.py`

### 設計

`build_baseline_section` 在組裝 tables list 之前,若 `baseline_metrics["purchase_counts"]` 存在且非空,先 build 一張表並 prepend 到 tables/table_titles 開頭:

```
product (index) | count | rank
```

排序 desc by count;rank 從 1 起。缺 `purchase_counts` 鍵或為空 → 跳過此表,不影響其餘渲染(向後相容)。

- [ ] **Step 2-1: 寫失敗測試 — popularity 表存在、欄位/排序正確**

新增到 `tests/test_evaluation/test_report_builder.py` 結尾:

```python
def test_baseline_section_renders_popularity_table():
    """purchase_counts -> popularity composition table prepended."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.4},
        "per_item": {"A": {"hit_rate@1": 0.1}},
        "purchase_counts": {"A": 50, "B": 200, "C": 10},
    }
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" in s.table_titles
    idx = s.table_titles.index("popularity 排名組成")
    tbl = s.tables[idx]
    # Sorted desc by count, with rank starting at 1.
    assert list(tbl.columns) == ["count", "rank"]
    assert list(tbl.index) == ["B", "A", "C"]
    assert list(tbl["count"]) == [200, 50, 10]
    assert list(tbl["rank"]) == [1, 2, 3]


def test_baseline_section_omits_popularity_when_purchase_counts_absent():
    """Backward compat: no purchase_counts -> no popularity table, others stay."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4},
            "per_item": {"A": {"hit_rate@1": 0.1}}}
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" not in s.table_titles


def test_baseline_section_omits_popularity_when_purchase_counts_empty():
    """Empty purchase_counts dict -> no popularity table."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4},
            "per_item": {"A": {"hit_rate@1": 0.1}},
            "purchase_counts": {}}
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" not in s.table_titles
```

- [ ] **Step 2-2: Run failing test**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py::test_baseline_section_renders_popularity_table -v
```
Expected: FAIL with `AssertionError: 'popularity 排名組成' not in s.table_titles`。

- [ ] **Step 2-3: 在 `build_baseline_section` 加 popularity 表**

編輯 `src/recsys_tfb/evaluation/report_builder.py` `build_baseline_section`(line 397 起),在 `tables = [delta]` 那行之前插入 popularity 表組建,並把它 prepend 到 tables/table_titles。改後完整函式:

```python
def build_baseline_section(
    metrics: dict, baseline_metrics: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "baseline") or baseline_metrics is None:
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    delta = pd.DataFrame([comp["overall_delta"]]).T
    delta.columns = ["Delta (Model - Baseline)"]
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    # _k_to_lookup handles a hypothetical "all" in guardrail_recall_k
    # (defaults are numeric, but keep parity with the §3 guardrail).
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    pid = comp.get("per_item_delta", {}) or {}
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    # [1] popularity composition (prepended when purchase_counts available).
    # Backward compat: silently omit when key missing or dict empty so older
    # baseline_metrics shapes still render.
    pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
    if pcounts:
        sorted_items = sorted(
            pcounts.items(), key=lambda kv: kv[1], reverse=True
        )
        pop_df = pd.DataFrame(
            {
                "count": [v for _, v in sorted_items],
                "rank": list(range(1, len(sorted_items) + 1)),
            },
            index=[k for k, _ in sorted_items],
        )
        tables.append(pop_df)
        table_titles.append("popularity 排名組成")

    tables.append(delta)
    table_titles.append("overall delta")

    if pid and (baseline_metrics or {}).get("per_item"):
        rec_rows = {
            item: {
                f"recall@{k} (per-item) Δ":
                    md.get(f"hit_rate@{_k_to_lookup(k, n_prod)}")
                for k in rec_ks
            }
            for item, md in pid.items()
        }
        tables.append(pd.DataFrame(rec_rows).T)
        table_titles.append("per-item recall@k delta")
    return ReportSection(
        title="基準比較 Baseline",
        description="Model vs Baseline:popularity 排名組成 + overall mAP@k 與 per-item recall@k delta。",
        tables=tables,
        table_titles=table_titles,
    )
```

- [ ] **Step 2-4: Run tests to verify pass**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -k baseline -v
```
Expected: 既有 `test_baseline_section_has_per_item_recall_delta` / `test_baseline_section_no_per_item_delta_omits_table` 仍 PASS、3 個新 case PASS。

> Note: 既有 `test_baseline_section_no_per_item_delta_omits_table` assert `s.table_titles == ["overall delta"]` — 它的 fixture 沒給 `purchase_counts`,所以新邏輯不會加 popularity 表,該 assert 仍成立。

- [ ] **Step 2-5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats add \
  src/recsys_tfb/evaluation/report_builder.py \
  tests/test_evaluation/test_report_builder.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats commit -m \
  "feat(evaluation): baseline section shows popularity composition table"
```

---

## Task 3:Overall metrics 由 delta-only 改為 Model / Baseline / Delta 三欄

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py` (`build_baseline_section`)
- Test: `tests/test_evaluation/test_report_builder.py`

### 設計

`comp["result_a"]["overall"]` 是 Model overall metrics dict、`comp["result_b"]["overall"]` 是 Baseline、`comp["overall_delta"]` 是 Δ。三者 key 集合相同(`_compute_delta` 取 union 並補 0)。

換掉 `delta` DataFrame:

```python
overall_a = comp["result_a"].get("overall", {}) or {}
overall_b = comp["result_b"].get("overall", {}) or {}
overall_delta = comp["overall_delta"]
all_keys = sorted(set(overall_a) | set(overall_b) | set(overall_delta))
overall_tbl = pd.DataFrame(
    {
        "Model": [overall_a.get(k) for k in all_keys],
        "Baseline": [overall_b.get(k) for k in all_keys],
        "Delta": [overall_delta.get(k) for k in all_keys],
    },
    index=all_keys,
)
```

Title 換 `"overall metrics"`。既有 `"overall delta"` title 已過時。

- [ ] **Step 3-1: 寫失敗測試 — overall 表為三欄、值正確**

新增到 `tests/test_evaluation/test_report_builder.py`:

```python
def test_baseline_section_overall_table_has_model_baseline_delta_cols():
    """overall table: rows = metric keys, cols = [Model, Baseline, Delta]."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.40, "ndcg@1": 0.50},
        "per_item": {"A": {"hit_rate@1": 0.1}},
    }
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "overall metrics" in s.table_titles
    idx = s.table_titles.index("overall metrics")
    tbl = s.tables[idx]
    assert list(tbl.columns) == ["Model", "Baseline", "Delta"]
    # Model fixture has overall["map@1"]=0.5, ndcg@1=0.55 (see _metrics()).
    assert tbl.loc["map@1", "Model"] == 0.5
    assert tbl.loc["map@1", "Baseline"] == 0.40
    assert abs(tbl.loc["map@1", "Delta"] - (0.5 - 0.40)) < 1e-9


def test_baseline_section_overall_table_includes_keys_unique_to_one_side():
    """Keys only in Model OR Baseline still appear, missing side as NaN."""
    m = _metrics()  # has 'precision@1', 'recall@1'
    base = {"overall": {"map@1": 0.4, "extra_key@1": 0.9}}  # no precision/recall
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("overall metrics")
    tbl = s.tables[idx]
    assert "extra_key@1" in tbl.index   # baseline-only key still listed
    assert "precision@1" in tbl.index   # model-only key still listed
```

- [ ] **Step 3-2: 把既有 delta-only 的 assert 改成新 title**

編輯 `tests/test_evaluation/test_report_builder.py` 既有 `test_baseline_section_no_per_item_delta_omits_table`(line 159–165):

```python
def test_baseline_section_no_per_item_delta_omits_table():
    m = _metrics()
    base = {"overall": {"map@1": 0.4}}          # no per_item -> per_item_delta empty
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert s.table_titles == ["overall metrics"]
    assert len(s.tables) == 1
```

(原本是 `"overall delta"` → 改成 `"overall metrics"`)

- [ ] **Step 3-3: Run failing test**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py::test_baseline_section_overall_table_has_model_baseline_delta_cols -v
```
Expected: FAIL with `AssertionError: 'overall metrics' not in s.table_titles`(目前 title 仍是 `overall delta`)。

- [ ] **Step 3-4: 改 `build_baseline_section` 把 overall 表從 delta-only 換成三欄**

編輯 `src/recsys_tfb/evaluation/report_builder.py`,把 Task 2 後的 `build_baseline_section` 內

```python
    delta = pd.DataFrame([comp["overall_delta"]]).T
    delta.columns = ["Delta (Model - Baseline)"]
```

換成

```python
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    overall_keys = sorted(set(overall_a) | set(overall_b) | set(overall_delta))
    overall_tbl = pd.DataFrame(
        {
            "Model": [overall_a.get(k) for k in overall_keys],
            "Baseline": [overall_b.get(k) for k in overall_keys],
            "Delta": [overall_delta.get(k) for k in overall_keys],
        },
        index=overall_keys,
    )
```

並把後續

```python
    tables.append(delta)
    table_titles.append("overall delta")
```

換成

```python
    tables.append(overall_tbl)
    table_titles.append("overall metrics")
```

最後把 `description` 也同步更新:

```python
        description="Model vs Baseline:popularity 排名組成 + overall metrics(M/B/Δ)與 per-item recall@k delta。",
```

- [ ] **Step 3-5: Run all baseline-section tests to verify pass**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -k baseline -v
```
Expected: 全 PASS(含改後的既有 case + Task 2 新增 3 個 + Task 3 新增 2 個)。

- [ ] **Step 3-6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats add \
  src/recsys_tfb/evaluation/report_builder.py \
  tests/test_evaluation/test_report_builder.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats commit -m \
  "feat(evaluation): baseline overall metrics show Model/Baseline/Delta"
```

---

## Task 4:Per-item recall / map_attr / ndcg_attr 各一張 M/B/Δ 表

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py` (add helper `_per_item_metric_compare_table` + use in `build_baseline_section`)
- Test: `tests/test_evaluation/test_report_builder.py`

### 設計

新 helper `_per_item_metric_compare_table` 產一張表,rows=items(+ 可選 Macro row),columns 交織為 `<base> M`、`<base> B`、`<base> Δ` 三組 × 每個 k。

```python
def _per_item_metric_compare_table(
    per_item_a: dict,
    per_item_b: dict,
    per_item_delta: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_base_fmt: str,           # e.g. "recall@{k}"
    macro_a: dict | None = None,
    macro_b: dict | None = None,
) -> pd.DataFrame:
    """Three columns per k: '<base> M', '<base> B', '<base> Δ'.
    Macro row is prepended when BOTH macro_a and macro_b are provided
    (Δ is computed from macro_a − macro_b, not from per_item_delta).
    """
    def _row(m_a: dict, m_b: dict, m_d: dict | None) -> dict:
        row: dict = {}
        for k in ks:
            lk = _k_to_lookup(k, n_prod)
            key = f"{metric_key}@{lk}"
            base = col_base_fmt.format(k=k)
            a = m_a.get(key)
            b = m_b.get(key)
            if m_d is not None:
                d = m_d.get(key)
            else:
                d = (a or 0.0) - (b or 0.0) if (a is not None or b is not None) else None
            row[f"{base} M"] = a
            row[f"{base} B"] = b
            row[f"{base} Δ"] = d
        return row

    data: dict = {}
    if macro_a is not None and macro_b is not None:
        data[_MACRO_LABEL] = _row(macro_a, macro_b, None)
    all_items = list(per_item_a.keys()) + [
        i for i in per_item_b.keys() if i not in per_item_a
    ]
    for item in all_items:
        data[item] = _row(
            per_item_a.get(item, {}),
            per_item_b.get(item, {}),
            per_item_delta.get(item, {}),
        )
    return pd.DataFrame(data).T
```

在 `build_baseline_section` 內,把現有 `pid` 那段 per-item recall delta 表整個換成三張新表(recall / map_attr / ndcg_attr),`ks` 依 spec:recall 用 `guardrail_recall_k`,attr 用 `primary_map_k`。

Macro 來源:Model 的 `metrics["macro_avg"]["by_item"]`、Baseline 的 `baseline_metrics["macro_avg"]["by_item"]`(若不存在則 helper 跳過 Macro row)。

- [ ] **Step 4-1: 在 `_metrics()` fixture 旁加 baseline fixture builder + 寫失敗測試**

新增到 `tests/test_evaluation/test_report_builder.py`(放在現有 helper 後即可):

```python
def _baseline_metrics_full():
    """Baseline metrics dict mirroring _metrics() per_item / macro shape."""
    return {
        "overall": {"map@1": 0.4, "map@3": 0.5, "ndcg@1": 0.45,
                    "precision@1": 0.3, "recall@1": 0.25},
        "per_item": {
            "A": {"hit_rate@1": 0.15, "hit_rate@2": 0.30, "mean_pos": 3.5,
                  "map_attr@1": 0.40, "map_attr@2": 0.45, "map_attr@3": 0.50,
                  "ndcg_attr@1": 0.35, "ndcg_attr@2": 0.40, "ndcg_attr@3": 0.42},
            "B": {"hit_rate@1": 0.08, "hit_rate@2": 0.20, "mean_pos": 5.5,
                  "map_attr@1": 0.25, "map_attr@2": 0.28, "map_attr@3": 0.30,
                  "ndcg_attr@1": 0.20, "ndcg_attr@2": 0.22, "ndcg_attr@3": 0.25}},
        "macro_avg": {"by_item": {
            "hit_rate@1": 0.115, "hit_rate@2": 0.25, "mean_pos": 4.5,
            "map_attr@1": 0.325, "map_attr@2": 0.365, "map_attr@3": 0.40,
            "ndcg_attr@1": 0.275, "ndcg_attr@2": 0.31, "ndcg_attr@3": 0.335,
        }},
    }


def test_baseline_section_has_three_per_item_compare_tables():
    """recall / map_attr / ndcg_attr each get a M/B/Δ-interleaved table."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    # Old delta-only title must be gone.
    assert "per-item recall@k delta" not in s.table_titles
    # Three new titles present.
    for title in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
        "per-item ndcg_attr@k (M/B/Δ)",
    ):
        assert title in s.table_titles


def test_baseline_section_per_item_recall_table_three_cols_per_k():
    """recall table: cols = recall@1 M/B/Δ, recall@2 M/B/Δ (params has guardrail_recall_k=[1,2])."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("per-item recall@k (M/B/Δ)")
    tbl = s.tables[idx]
    assert list(tbl.columns) == [
        "recall@1 M", "recall@1 B", "recall@1 Δ",
        "recall@2 M", "recall@2 B", "recall@2 Δ",
    ]
    # Macro row first.
    assert list(tbl.index)[0] == "Macro 平均"
    # Spot-check A: Model hit_rate@1=0.2, Baseline=0.15, Δ from per_item_delta.
    assert tbl.loc["A", "recall@1 M"] == 0.2
    assert tbl.loc["A", "recall@1 B"] == 0.15
    assert abs(tbl.loc["A", "recall@1 Δ"] - (0.2 - 0.15)) < 1e-9
    # Macro row Δ from macro_a − macro_b.
    assert abs(
        tbl.loc["Macro 平均", "recall@1 Δ"] - (0.15 - 0.115)
    ) < 1e-9


def test_baseline_section_per_item_attr_tables_use_primary_map_k():
    """map_attr / ndcg_attr cols come from primary_map_k = [1, 3, 'all'];
    'all' resolves to n_products (=2 in fixture) for lookup."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("per-item map_attr@k (M/B/Δ)")
    tbl = s.tables[idx]
    assert list(tbl.columns) == [
        "map_attr@1 M", "map_attr@1 B", "map_attr@1 Δ",
        "map_attr@3 M", "map_attr@3 B", "map_attr@3 Δ",
        "map_attr@all M", "map_attr@all B", "map_attr@all Δ",
    ]
    # n_prod=2 means @all → lookup @2. Model A map_attr@2=0.55, Base=0.45.
    assert tbl.loc["A", "map_attr@all M"] == 0.55
    assert tbl.loc["A", "map_attr@all B"] == 0.45


def test_baseline_section_omits_per_item_compare_when_no_baseline_per_item():
    """No baseline per_item -> per-item compare tables skipped (overall stays)."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4}}  # no per_item
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    for title in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
        "per-item ndcg_attr@k (M/B/Δ)",
    ):
        assert title not in s.table_titles
```

也要移除既有 `test_baseline_section_has_per_item_recall_delta`(line 115–124)它測的是被取代掉的舊行為,新測試已涵蓋更精確的行為:

```python
# delete the entire test_baseline_section_has_per_item_recall_delta function
```

- [ ] **Step 4-2: Run failing tests**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py::test_baseline_section_has_three_per_item_compare_tables -v
```
Expected: FAIL — `per-item recall@k (M/B/Δ)` not in titles(目前仍是 `per-item recall@k delta`)。

- [ ] **Step 4-3: 在 `report_builder.py` 加 helper `_per_item_metric_compare_table`**

把 helper 加在現有 `_per_item_recall_table`(約 line 211)上方(任何 `def build_*` 之前都行,跟其他 `_per_item_*` helper 靠一起):

```python
def _per_item_metric_compare_table(
    per_item_a: dict,
    per_item_b: dict,
    per_item_delta: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_base_fmt: str,
    macro_a: dict | None = None,
    macro_b: dict | None = None,
) -> pd.DataFrame:
    """Per-item table with Model/Baseline/Δ interleaved per k.

    Rows = items (Macro 平均 prepended when BOTH macro_a and macro_b are
    given). Columns = ``f"{base} M"``, ``f"{base} B"``, ``f"{base} Δ"`` for
    each ``k``, where ``base = col_base_fmt.format(k=k)``.

    Δ for item rows is read from ``per_item_delta`` (already computed
    upstream by build_comparison_result); Δ for the Macro row is computed
    here as ``macro_a − macro_b`` since macro values aren't part of the
    per-item delta dict.
    """
    def _row(m_a: dict, m_b: dict, m_d: dict | None) -> dict:
        row: dict = {}
        for k in ks:
            lk = _k_to_lookup(k, n_prod)
            key = f"{metric_key}@{lk}"
            base = col_base_fmt.format(k=k)
            a = m_a.get(key)
            b = m_b.get(key)
            if m_d is not None:
                d = m_d.get(key)
            else:
                if a is None and b is None:
                    d = None
                else:
                    d = (a or 0.0) - (b or 0.0)
            row[f"{base} M"] = a
            row[f"{base} B"] = b
            row[f"{base} Δ"] = d
        return row

    data: dict = {}
    if macro_a is not None and macro_b is not None:
        data[_MACRO_LABEL] = _row(macro_a, macro_b, None)
    all_items = list(per_item_a.keys()) + [
        i for i in per_item_b.keys() if i not in per_item_a
    ]
    for item in all_items:
        data[item] = _row(
            per_item_a.get(item, {}),
            per_item_b.get(item, {}),
            per_item_delta.get(item, {}),
        )
    return pd.DataFrame(data).T
```

- [ ] **Step 4-4: 改 `build_baseline_section` 用新 helper 產三張 per-item 表**

替換掉現有的 per-item recall delta 區塊。改後完整 `build_baseline_section`:

```python
def build_baseline_section(
    metrics: dict, baseline_metrics: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "baseline") or baseline_metrics is None:
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    attr_ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )

    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    # [1] popularity composition (omitted when missing/empty for back-compat).
    pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
    if pcounts:
        sorted_items = sorted(
            pcounts.items(), key=lambda kv: kv[1], reverse=True
        )
        pop_df = pd.DataFrame(
            {
                "count": [v for _, v in sorted_items],
                "rank": list(range(1, len(sorted_items) + 1)),
            },
            index=[k for k, _ in sorted_items],
        )
        tables.append(pop_df)
        table_titles.append("popularity 排名組成")

    # [2] overall metrics: Model / Baseline / Delta.
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    overall_keys = sorted(set(overall_a) | set(overall_b) | set(overall_delta))
    overall_tbl = pd.DataFrame(
        {
            "Model": [overall_a.get(k) for k in overall_keys],
            "Baseline": [overall_b.get(k) for k in overall_keys],
            "Delta": [overall_delta.get(k) for k in overall_keys],
        },
        index=overall_keys,
    )
    tables.append(overall_tbl)
    table_titles.append("overall metrics")

    # [3] per-item compare tables — only when baseline has per_item.
    per_item_a = comp["result_a"].get("per_item", {}) or {}
    per_item_b = comp["result_b"].get("per_item", {}) or {}
    per_item_delta = comp.get("per_item_delta", {}) or {}
    macro_a = (metrics.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (baseline_metrics.get("macro_avg", {}) or {}).get("by_item")
    if per_item_b:
        for metric_key, col_fmt, ks, title in (
            ("hit_rate", "recall@{k}", rec_ks,
             "per-item recall@k (M/B/Δ)"),
            ("map_attr", "map_attr@{k}", attr_ks,
             "per-item map_attr@k (M/B/Δ)"),
            ("ndcg_attr", "ndcg_attr@{k}", attr_ks,
             "per-item ndcg_attr@k (M/B/Δ)"),
        ):
            tbl = _per_item_metric_compare_table(
                per_item_a, per_item_b, per_item_delta,
                ks, n_prod, metric_key, col_fmt,
                macro_a=macro_a, macro_b=macro_b,
            )
            tables.append(tbl)
            table_titles.append(title)

    return ReportSection(
        title="基準比較 Baseline",
        description=(
            "Model vs Baseline:popularity 排名組成 + overall metrics(M/B/Δ)+ "
            "per-item recall/map_attr/ndcg_attr(M/B/Δ)。"
        ),
        tables=tables,
        table_titles=table_titles,
    )
```

- [ ] **Step 4-5: Run all baseline-section tests**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -k baseline -v
```
Expected: 全 PASS。

- [ ] **Step 4-6: Run full test_report_builder.py 確認沒影響其他 section**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -q
```
Expected: 全綠。

- [ ] **Step 4-7: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats add \
  src/recsys_tfb/evaluation/report_builder.py \
  tests/test_evaluation/test_report_builder.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats commit -m \
  "feat(evaluation): baseline per-item recall/map_attr/ndcg_attr show M/B/Delta"
```

---

## Task 5:Integration verify(實跑 evaluation pipeline,人眼看 report.html)

> 不寫額外測試;這是 manual sanity check,驗證合成資料 end-to-end 跑得通、report 三個區塊都長出來。

**Files:** 無新增

### Pre-conditions

- dev-cluster 已 up,`ml_recsys.{feature_table,label_table,sample_pool}` 與 `ml_recsys.training_eval_predictions` 已存在(本次 main 開發過程已建好,若被清掉需先重跑 dataset + training + promote)
- `data/models/best` symlink 指向有效 model version
- `conf/base/parameters_evaluation.yaml` 的 `snap_date` 跟 `training_eval_predictions` 內的 snap_date 對齊(本次 main 工作流已調為 `2026-01-31`)

- [ ] **Step 5-1: 跑 evaluation pipeline(local[*] 模式)**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/baseline-section-stats
source ~/dev-cluster/scripts/client-env.sh
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb \
  evaluation --env production --post-training 2>&1 | tee /tmp/eval_verify.log
```
Expected: `Pipeline 'evaluation' completed successfully`,在 stdout 看到 `compute_baseline_metrics ... purchase_counts has 8 products`。

(`local[*]` 是必須的:dev-cluster 的 standalone worker container 沒 python3,`compute_metrics` 會炸 `PythonRDD`。詳見 `dev-cluster-spark` skill SOP-6 + SOP-3-C。)

- [ ] **Step 5-2: 抽 report.html 確認三個區塊存在**

```bash
.venv/bin/python - <<'EOF'
import re, html
from pathlib import Path
p = sorted(Path("data/evaluation").rglob("report.html"),
           key=lambda p: p.stat().st_mtime)[-1]
print("Report:", p)
t = p.read_text()
cells = [html.unescape(re.sub(r"<[^>]+>","",c)).strip()
         for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", t, re.S)]
joined = " | ".join(cells)
i = joined.find("基準比較 Baseline")
# Print 一段含 baseline section 標題與表頭的內容
print(joined[i: i + 1200])
EOF
```
Expected: 輸出包含 `popularity 排名組成` / `count` / `rank`、`overall metrics` / `Model` / `Baseline` / `Delta`、`per-item recall@k (M/B/Δ)` / `recall@1 M`、`per-item map_attr@k (M/B/Δ)`、`per-item ndcg_attr@k (M/B/Δ)`。

- [ ] **Step 5-3: 視覺檢查(瀏覽器開 report.html)**

```bash
open "$(ls -td data/evaluation/*/*/ | head -1)report.html"
```

人眼確認:
1. popularity 排名組成表 8 列、count desc、rank 1–8
2. overall metrics 三欄、數值合理(Model ≈ Baseline 是合成資料預期)
3. 三張 per-item 表都有 Macro 平均列、欄位 M/B/Δ 交織、寬表可橫向滾

若上述都 OK 進 Step 5-4;若視覺有問題回報,可能要回 Task 3/4 微調。

- [ ] **Step 5-4: 跑相關測試一次總綠**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py \
  tests/test_evaluation/test_baselines.py \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py -q
```
Expected: 全綠、無 warning 退化。若有測試 fail → 回對應 Task。

- [ ] **Step 5-5(no commit needed)**

Integration verify 不產生程式碼變更;如有任何 quick fix 則回對應 Task 走 TDD,不要直接 commit。

---

## Self-Review Notes(plan 寫完後自查結果)

| 檢查項 | 結果 |
|---|---|
| Spec 每節都有對應 task | popularity 表 → T2;overall M/B/Δ → T3;per-item recall/map_attr/ndcg_attr 三表 → T4;`compute_baseline_metrics` schema 擴充 → T1;邊界處理(`purchase_counts` 缺、per_item 缺)→ T2/T4 都有 test |
| 無 placeholder | 所有 code block 都是完整可貼上、所有 command 都是完整可執行 |
| 型別/命名一致 | helper 命名 `_per_item_metric_compare_table`(沿用 `_per_item_metric_table` 風格);table titles 在 T2/T3/T4 與測試 assert 同字串;`metric_key` 用 `hit_rate`/`map_attr`/`ndcg_attr` 與既有 `_per_item_metric_table` 用法一致 |
| 範圍 | 4 個程式 task + 1 個 manual verify,單一 feature,不混入其他改動 |

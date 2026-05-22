# report.html Macro 平均彙總列 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `report.html` 的 per-item 與 per-segment 表格頂部各加一列「Macro 平均」彙總列。

**Architecture:** 純 `report_builder.py` 顯示層改動。`metrics_spark.py` 的 `_compute_core()` 已產出 `macro_avg`（`by_item` / `by_segment`），本計畫只把它讀進 report。`macro_avg["by_item"]` 與 `per_item[item]` 的 key 結構相同，所以「Macro 平均」列等同「多一個 item」，套用既有欄位組裝邏輯即可。Heatmap 不顯示 macro 列 — 各 builder 先用「無 macro 的表」建 heatmap，再另建「含 macro 的表」供顯示，`_per_item_heatmap` 本身不動。

**Tech Stack:** Python 3.10、pandas 1.5.3、plotly、pytest 7.3.1。

---

## File Structure

- **Modify:** `src/recsys_tfb/evaluation/report_builder.py`
  - 新增模組常數 `_MACRO_LABEL`
  - `_per_item_metric_table` / `_per_item_recall_table` 新增 `macro_metrics` 參數
  - `build_guardrail_recall_section` / `build_per_item_attr_section` / `build_segment_section` / `build_category_section` 接上 macro 列
  - `_GLOSSARY` 新增一條
- **Modify:** `tests/test_evaluation/test_report_builder.py`
  - `_metrics()` fixture 補 `macro_avg`
  - 新增測試 case

**不動：** `metrics_spark.py`、`report.py`、`_per_item_heatmap`、`metrics.json` 輸出。

## 環境注意

本計畫在 worktree `.worktrees/report-macro-avg`（branch `feat/report-macro-avg`，已 rebase 至含 PR #35 的 main）執行。跑測試一律：

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -q
```

該測試檔為 pure-dict（無 Spark），秒級完成。

---

## Task 1: 共用 helper 加 macro 參數 + 接上護欄 recall section

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 在 `_metrics()` fixture 補 `macro_avg`**

在 `tests/test_evaluation/test_report_builder.py` 的 `_metrics()` 回傳 dict 中，於 `"n_queries"` 那行之前加入 `macro_avg`（值為 `per_item` 中 A、B 的算術平均）：

```python
        "macro_avg": {
            "by_item": {
                "hit_rate@1": 0.15, "hit_rate@2": 0.35, "mean_pos": 4.0,
                "map_attr@1": 0.4, "map_attr@2": 0.435, "map_attr@3": 0.475,
                "ndcg_attr@1": 0.35, "ndcg_attr@2": 0.38, "ndcg_attr@3": 0.4,
            },
        },
        "n_queries": 10, "n_excluded_queries": 0,
```

（原本就是 `"n_queries": 10, "n_excluded_queries": 0,` 一行，把 `macro_avg` 區塊插在它前面。）

- [ ] **Step 2: 寫失敗測試**

在 `tests/test_evaluation/test_report_builder.py` 末尾加入：

```python
def test_guardrail_section_has_macro_row():
    s = rb.build_guardrail_recall_section(_metrics(), _params())
    table = s.tables[0]
    # 頂列為 Macro 平均
    assert list(table.index)[0] == "Macro 平均"
    # 值為各產品等權平均：hit_rate@1 → recall@1 (per-item) 欄
    assert table.loc["Macro 平均", "recall@1 (per-item)"] == 0.15
    assert table.loc["Macro 平均", "mean_pos"] == 4.0
    # heatmap 不含 macro 列
    assert "Macro 平均" not in list(s.figures[0].data[0].y)


def test_guardrail_section_no_macro_when_absent():
    m = _metrics()
    del m["macro_avg"]
    s = rb.build_guardrail_recall_section(m, _params())
    assert "Macro 平均" not in list(s.tables[0].index)
```

- [ ] **Step 3: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py::test_guardrail_section_has_macro_row -q
```
Expected: FAIL — `KeyError: 'Macro 平均'`（macro 列尚未加入）。

- [ ] **Step 4: 實作 helper 參數**

在 `report_builder.py`，於 `_k_to_lookup` 之後（約 `:40`）加入模組常數：

```python
_MACRO_LABEL = "Macro 平均"
```

把 `_per_item_metric_table` 整個函式替換為：

```python
def _per_item_metric_table(
    per_item: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_fmt: str,
    extra_cols: dict[str, str] | None = None,
    macro_metrics: dict | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{_k_to_lookup(k, n_prod)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.

    ``macro_metrics``: when given and non-empty, an equal-weight-average
    metrics dict (same key shape as a per_item value) is prepended as the
    top row labelled ``_MACRO_LABEL``.
    """
    def _row(m: dict) -> dict:
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        return row

    data: dict = {}
    if macro_metrics:
        data[_MACRO_LABEL] = _row(macro_metrics)
    for item, m in per_item.items():
        data[item] = _row(m)
    return pd.DataFrame(data).T
```

把 `_per_item_recall_table` 整個函式替換為：

```python
def _per_item_recall_table(
    per_item: dict, ks: list, n_prod: int, macro_metrics: dict | None = None
) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + mean_pos."""
    return _per_item_metric_table(
        per_item, ks, n_prod, "hit_rate", "recall@{k} (per-item)",
        extra_cols={"mean_pos": "mean_pos"}, macro_metrics=macro_metrics,
    )
```

- [ ] **Step 5: 接上 `build_guardrail_recall_section`**

把 `build_guardrail_recall_section` 整個函式替換為：

```python
def build_guardrail_recall_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "guardrail_recall"):
        return None
    per_item = metrics.get("per_item", {})
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    # heatmap 用無 macro 列的表；顯示用含 macro 列的表
    table_plain = _per_item_recall_table(per_item, ks, n_prod)
    cs = disp.get("recall_colorscale", {}) or {}
    fig = _per_item_heatmap(
        table_plain, per_item, ks, n_prod, "hit_rate", "recall@{k}",
        "per-item recall@k 色階",
        zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
    )
    table = _per_item_recall_table(
        per_item, ks, n_prod, macro_metrics=macro_item
    )
    return ReportSection(
        title="護欄 per_item recall@k（細產品）",
        description=(
            "每產品 recall@k（per-item，即 hit_rate@k 正名）＋色階。"
            "頂列「Macro 平均」為各產品等權平均。"
            "純判讀、無 pass/fail 閾值。完整資料統計見「資料概況」。"
        ),
        figures=[fig],
        tables=[table],
        table_titles=["per-item recall@k"],
    )
```

- [ ] **Step 6: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py -q
```
Expected: PASS（含既有測試 + 兩個新測試）。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(eval): add macro-average row to per-item recall table"
```

---

## Task 2: per_item attribution section 接上 macro 列

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_evaluation/test_report_builder.py` 末尾加入：

```python
def test_per_item_attr_section_has_macro_rows():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    map_tbl, ndcg_tbl = s.tables[0], s.tables[1]
    assert list(map_tbl.index)[0] == "Macro 平均"
    assert list(ndcg_tbl.index)[0] == "Macro 平均"
    # map_attr@1 各產品平均 = (0.5 + 0.3) / 2 = 0.4
    assert map_tbl.loc["Macro 平均", "map_attr@1"] == 0.4
    # ndcg_attr@1 各產品平均 = (0.45 + 0.25) / 2 = 0.35
    assert ndcg_tbl.loc["Macro 平均", "ndcg_attr@1"] == 0.35
    # heatmap 不含 macro 列
    assert "Macro 平均" not in list(s.figures[0].data[0].y)
    assert "Macro 平均" not in list(s.figures[1].data[0].y)
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py::test_per_item_attr_section_has_macro_rows -q
```
Expected: FAIL — `assert 'A' == 'Macro 平均'`（頂列仍是產品 A）。

- [ ] **Step 3: 接上 `build_per_item_attr_section`**

把 `build_per_item_attr_section` 整個函式替換為：

```python
def build_per_item_attr_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_item_attr"):
        return None
    per_item = metrics.get("per_item", {})
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )
    # heatmap 用無 macro 列的表；顯示用含 macro 列的表
    map_tbl_plain = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}"
    )
    ndcg_tbl_plain = _per_item_metric_table(
        per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}"
    )
    map_fig = _per_item_heatmap(
        map_tbl_plain, per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        "per-item map_attr@k 色階",
    )
    ndcg_fig = _per_item_heatmap(
        ndcg_tbl_plain, per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}",
        "per-item ndcg_attr@k 色階",
    )
    map_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        macro_metrics=macro_item,
    )
    ndcg_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}",
        macro_metrics=macro_item,
    )
    return ReportSection(
        title="per_item 歸因 Attribution（細產品）",
        description=(
            "每個產品對主指標 mAP@k / nDCG@k 各貢獻多少。算法：對每筆"
            "「(客戶, 產品) 且該產品是這位客戶的正解」的紀錄，先算單筆貢獻 "
            "ap_contrib@k = 該產品排名進前 k 時的累積精度（排越前、前面混入"
            "的非正解越少 → 越高；沒進前 k → 0）。一位客戶的 AP@k = 他所有"
            "正解產品的 ap_contrib@k 加總 ÷ 正解數 total_rel。map_attr@k = "
            "某產品在「它為該客戶正解」的所有客戶上，ap_contrib@k 的平均 → "
            "即這個產品平均替 AP@k 加了多少分。ndcg_attr@k 同理，把單筆貢獻"
            "換成 log 折扣的 ndcg_contrib@k。頂列「Macro 平均」為各產品等權平均。"
        ),
        figures=[map_fig, ndcg_fig],
        tables=[map_tbl, ndcg_tbl],
        table_titles=["per-item map_attr@k", "per-item ndcg_attr@k"],
    )
```

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py -q
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(eval): add macro-average row to per-item attribution tables"
```

---

## Task 3: per-segment section 接上 macro 列

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_evaluation/test_report_builder.py` 末尾加入：

```python
def test_segment_section_has_macro_row():
    m = _metrics()
    m["per_segment"] = {
        "young": {"map@1": 0.6, "ndcg@1": 0.7},
        "old": {"map@1": 0.4, "ndcg@1": 0.5},
    }
    m["macro_avg"]["by_segment"] = {"map@1": 0.5, "ndcg@1": 0.6}
    s = rb.build_segment_section(m, _params())
    assert list(s.tables[0].index)[0] == "Macro 平均"
    assert s.tables[0].loc["Macro 平均", "map@1"] == 0.5


def test_segment_section_no_macro_when_absent():
    m = _metrics()
    m["per_segment"] = {"young": {"map@1": 0.6}}
    # macro_avg 無 by_segment key
    s = rb.build_segment_section(m, _params())
    assert "Macro 平均" not in list(s.tables[0].index)
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py::test_segment_section_has_macro_row -q
```
Expected: FAIL — 頂列為 `young` 而非 `Macro 平均`。

- [ ] **Step 3: 接上 `build_segment_section`**

把 `build_segment_section` 整個函式替換為：

```python
def build_segment_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_segment"):
        return None
    per_segment = metrics.get("per_segment", {})
    if not per_segment:
        return None
    macro_seg = metrics.get("macro_avg", {}).get("by_segment", {})
    rows = (
        {_MACRO_LABEL: macro_seg, **per_segment}
        if macro_seg
        else dict(per_segment)
    )
    table = pd.DataFrame(rows).T
    return ReportSection(
        title="分群 Per-Segment",
        description=(
            "per-query 指標依 segment 切分。"
            "頂列「Macro 平均」為各 segment 等權平均。"
        ),
        tables=[table],
        table_titles=["per-segment 指標"],
    )
```

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py -q
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(eval): add macro-average row to per-segment table"
```

---

## Task 4: 大類 Category section 接上 macro 列

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_evaluation/test_report_builder.py` 末尾加入：

```python
def test_category_section_recall_table_has_macro_row():
    m = _metrics()
    m["category"] = {
        "overall": {"map@1": 0.7},
        "per_item": {
            "fund": {"hit_rate@1": 0.5, "hit_rate@2": 0.6, "mean_pos": 2.0},
            "loan": {"hit_rate@1": 0.3, "hit_rate@2": 0.4, "mean_pos": 4.0},
        },
        "macro_avg": {
            "by_item": {
                "hit_rate@1": 0.4, "hit_rate@2": 0.5, "mean_pos": 3.0,
            },
        },
        "dataset_overview": m["dataset_overview"],
    }
    s = rb.build_category_section(m, _params())
    # tables[1] 為大類 per-item recall@k 表
    rec_tbl = s.tables[1]
    assert list(rec_tbl.index)[0] == "Macro 平均"
    assert rec_tbl.loc["Macro 平均", "recall@1 (per-item)"] == 0.4
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py::test_category_section_recall_table_has_macro_row -q
```
Expected: FAIL — 頂列為 `fund` 而非 `Macro 平均`。

- [ ] **Step 3: 接上 `build_category_section`**

在 `build_category_section` 中，找到這一行：

```python
    rec_tbl = _per_item_recall_table(cat.get("per_item", {}), rec_ks, n_cat)
```

替換為：

```python
    cat_macro_item = cat.get("macro_avg", {}).get("by_item", {})
    rec_tbl = _per_item_recall_table(
        cat.get("per_item", {}), rec_ks, n_cat, macro_metrics=cat_macro_item
    )
```

同函式的 `description` 由：

```python
        description="大類粒度 mAP@k 與 per-item recall@k（大類=子產品最佳 rank）。",
```

替換為：

```python
        description=(
            "大類粒度 mAP@k 與 per-item recall@k（大類=子產品最佳 rank）。"
            "recall@k 表頂列「Macro 平均」為各大類等權平均。"
        ),
```

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py -q
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(eval): add macro-average row to category recall table"
```

---

## Task 5: Glossary 新增 Macro 平均 條目

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_evaluation/test_report_builder.py` 末尾加入：

```python
def test_glossary_has_macro_average_entry():
    s = rb.build_glossary_section(_params())
    terms = list(s.tables[0]["指標"])
    assert "Macro 平均" in terms
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py::test_glossary_has_macro_average_entry -q
```
Expected: FAIL — `assert 'Macro 平均' in [...]`。

- [ ] **Step 3: 新增 glossary 條目**

在 `report_builder.py` 的 `_GLOSSARY` list 中，於 `("mean_pos", ...)` 條目之後加入：

```python
    ("Macro 平均",
     "對所有產品（或 segment）等權平均；與 query 等權的 overall 不同"),
```

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py -q
```
Expected: PASS（全檔測試通過）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "docs(eval): add Macro 平均 glossary entry"
```

---

## Task 6: 收尾驗證

**Files:** 無（純驗證）

- [ ] **Step 1: 全 report 相關測試**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_report_builder.py tests/test_evaluation/test_report.py -q
```
Expected: 全 PASS。

- [ ] **Step 2: 更新 graphify 知識圖**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg && python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 3: 確認 diff 範圍**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/report-macro-avg diff main..HEAD --stat
```
Expected: 只動 `report_builder.py`、`test_report_builder.py`，加上 `docs/superpowers/` 兩個檔與 graphify 產出。

---

## 完成後

實作完成、測試全綠後，用 `superpowers:finishing-a-development-branch` skill 決定合併方式（PR / merge）。

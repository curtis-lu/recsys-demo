# per_item 歸因 Attribution 段落 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `report.html` 新增「per_item 歸因 Attribution」段落，呈現每個產品對 mAP@k / nDCG@k 的貢獻度（`map_attr@k` / `ndcg_attr@k`）。

**Architecture:** 純 report 層變更。先把「per-item 指標表 + heatmap」抽成參數化共用 helper（現有 `_per_item_recall_table` 改為呼叫它），再用該 helper 組出新段落的兩張表與兩個 heatmap。metrics 計算層不動 —— `metrics["per_item"]` 早已帶 `map_attr@K` / `ndcg_attr@K`。

**Tech Stack:** Python 3.10, pandas 1.5.3, plotly, pytest 7.3.1。

設計來源：`docs/superpowers/specs/2026-05-22-per-item-attr-report-design.md`

---

## 環境前置（每次跑測試前）

本工作在 worktree `.worktrees/per-item-attr-report`。所有測試指令一律：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/per-item-attr-report
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <args>
```

下文 `pytest` 均指上述完整形式。

---

## File Structure

- Modify: `src/recsys_tfb/evaluation/report_builder.py`
  - 新增 `_per_item_metric_table`、`_per_item_heatmap` helper
  - `_per_item_recall_table` 改為呼叫 helper
  - `build_guardrail_recall_section` heatmap 改為呼叫 helper
  - 新增 `build_per_item_attr_section`
  - `assemble_report` candidates 插入新段落
  - `_GLOSSARY` 新增兩列
- Modify: `conf/base/parameters_evaluation.yaml` — `report.sections` 加 `per_item_attr`
- Modify: `tests/test_evaluation/test_report_builder.py` — 擴充 fixture + 新測試
- Modify: `tests/test_evaluation/test_parameters_evaluation_yaml.py` — 斷言新 section key

---

## Task 1: 抽共用 helper（regression-guarded 重構）

此任務為純重構：輸出不變，由 `test_report_builder.py` 既有 15 個測試守住。無新 failing test，改以「重構前後測試皆綠」為安全網。

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py:141-189`
- Test: `tests/test_evaluation/test_report_builder.py`（既有，不改）

- [ ] **Step 1: 跑既有測試建立基線**

Run: `pytest tests/test_evaluation/test_report_builder.py -q`
Expected: PASS，`15 passed`。

- [ ] **Step 2: 新增兩個 helper**

在 `report_builder.py` 中，於現有 `_per_item_recall_table`（第 141 行）**之前**插入：

```python
def _per_item_metric_table(
    per_item: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_fmt: str,
    extra_cols: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{_k_to_lookup(k, n_prod)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.
    """
    data = {}
    for item, m in per_item.items():
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        data[item] = row
    return pd.DataFrame(data).T


def _per_item_heatmap(
    table: pd.DataFrame,
    per_item: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    x_fmt: str,
    title: str,
    zmin: float | None = None,
    zmax: float | None = None,
) -> go.Figure:
    """RdYlGn heatmap; z from ``per_item[item][f"{metric_key}@{lookup(k)}"]``,
    rows ordered by ``table.index``. ``zmin``/``zmax`` left None -> Plotly
    autoscales the colour range.
    """
    z = [
        [per_item.get(it, {}).get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
         for k in ks]
        for it in table.index
    ]
    fig = go.Figure(
        data=go.Heatmap(
            z=z, x=[x_fmt.format(k=k) for k in ks], y=list(table.index),
            zmin=zmin, zmax=zmax,
            colorscale="RdYlGn", texttemplate="%{z:.3f}",
        )
    )
    fig.update_layout(title=title, yaxis_title="產品")
    return fig
```

- [ ] **Step 3: `_per_item_recall_table` 改為呼叫 helper**

把現有（第 141-151 行）：

```python
def _per_item_recall_table(per_item: dict, ks: list, n_prod: int) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + base."""
    data = {}
    for item, m in per_item.items():
        row = {
            f"recall@{k} (per-item)": m.get(f"hit_rate@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
        row["mean_pos"] = m.get("mean_pos")
        data[item] = row
    return pd.DataFrame(data).T
```

整段替換為：

```python
def _per_item_recall_table(per_item: dict, ks: list, n_prod: int) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + mean_pos."""
    return _per_item_metric_table(
        per_item, ks, n_prod, "hit_rate", "recall@{k} (per-item)",
        extra_cols={"mean_pos": "mean_pos"},
    )
```

- [ ] **Step 4: `build_guardrail_recall_section` heatmap 改為呼叫 helper**

在 `build_guardrail_recall_section` 中，把現有的 heatmap 組裝區塊（`cs = disp.get("recall_colorscale", ...)` 起，到 `fig.update_layout(...)` 止）：

```python
    cs = disp.get("recall_colorscale", {}) or {}
    z = [
        [per_item.get(it, {}).get(f"hit_rate@{_k_to_lookup(k, n_prod)}")
         for k in ks]
        for it in table.index
    ]
    fig = go.Figure(
        data=go.Heatmap(
            z=z, x=[f"recall@{k}" for k in ks], y=list(table.index),
            zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
            colorscale="RdYlGn", texttemplate="%{z:.3f}",
        )
    )
    fig.update_layout(title="per-item recall@k 色階", yaxis_title="產品")
```

整段替換為：

```python
    cs = disp.get("recall_colorscale", {}) or {}
    fig = _per_item_heatmap(
        table, per_item, ks, n_prod, "hit_rate", "recall@{k}",
        "per-item recall@k 色階",
        zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
    )
```

- [ ] **Step 5: 跑既有測試確認重構無回歸**

Run: `pytest tests/test_evaluation/test_report_builder.py -q`
Expected: PASS，仍為 `15 passed`（guardrail / category / baseline 行為不變）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/per-item-attr-report
git add src/recsys_tfb/evaluation/report_builder.py
git commit -m "refactor(evaluation): extract shared per-item table/heatmap helpers"
```

---

## Task 2: 新增 per_item 歸因段落

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（新增 `build_per_item_attr_section`、`assemble_report` candidates）
- Test: `tests/test_evaluation/test_report_builder.py`（擴充 `_metrics()` fixture + 3 個新測試）

- [ ] **Step 1: 擴充測試 fixture 並寫 failing 測試**

在 `tests/test_evaluation/test_report_builder.py` 中，把 `_metrics()` 的 `per_item` 區塊（現為）：

```python
        "per_item": {"A": {"hit_rate@1": 0.2, "hit_rate@2": 0.4,
                           "mean_pos": 3.0},
                     "B": {"hit_rate@1": 0.1, "hit_rate@2": 0.3,
                           "mean_pos": 5.0}},
```

替換為（補 `map_attr` / `ndcg_attr`；`_params()` 的 `primary_map_k=[1,3,"all"]`、`n_products=2`，故 `"all"` 經 `_k_to_lookup` 解析為 `2`，鍵用 `@1/@3/@2`）：

```python
        "per_item": {"A": {"hit_rate@1": 0.2, "hit_rate@2": 0.4,
                           "mean_pos": 3.0,
                           "map_attr@1": 0.5, "map_attr@3": 0.6,
                           "map_attr@2": 0.55,
                           "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.5,
                           "ndcg_attr@2": 0.48},
                     "B": {"hit_rate@1": 0.1, "hit_rate@2": 0.3,
                           "mean_pos": 5.0,
                           "map_attr@1": 0.3, "map_attr@3": 0.35,
                           "map_attr@2": 0.32,
                           "ndcg_attr@1": 0.25, "ndcg_attr@3": 0.3,
                           "ndcg_attr@2": 0.28}},
```

並在檔案結尾新增三個測試：

```python
def test_per_item_attr_section_built():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    assert s is not None
    assert len(s.tables) == 2 and len(s.figures) == 2
    map_tbl = s.tables[0]
    assert set(map_tbl.index) == {"A", "B"}
    cols = " ".join(map(str, map_tbl.columns))
    assert "map_attr@1" in cols and "map_attr@3" in cols
    ndcg_cols = " ".join(map(str, s.tables[1].columns))
    assert "ndcg_attr@1" in ndcg_cols


def test_per_item_attr_section_off():
    p = _params()
    p["evaluation"]["report"].setdefault("sections", {})["per_item_attr"] = False
    assert rb.build_per_item_attr_section(_metrics(), p) is None


def test_per_item_attr_heatmap_autoscale():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    for fig in s.figures:
        hm = fig.data[0]
        assert hm.zmin is None and hm.zmax is None
```

- [ ] **Step 2: 跑新測試確認 fail**

Run: `pytest tests/test_evaluation/test_report_builder.py -k per_item_attr -q`
Expected: FAIL，`AttributeError: module ... has no attribute 'build_per_item_attr_section'`。

- [ ] **Step 3: 實作 `build_per_item_attr_section`**

在 `report_builder.py` 中，於 `build_guardrail_recall_section` 之後新增：

```python
def build_per_item_attr_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_item_attr"):
        return None
    per_item = metrics.get("per_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )
    map_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}"
    )
    ndcg_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}"
    )
    map_fig = _per_item_heatmap(
        map_tbl, per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        "per-item map_attr@k 色階",
    )
    ndcg_fig = _per_item_heatmap(
        ndcg_tbl, per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}",
        "per-item ndcg_attr@k 色階",
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
            "換成 log 折扣的 ndcg_contrib@k。"
        ),
        figures=[map_fig, ndcg_fig],
        tables=[map_tbl, ndcg_tbl],
        table_titles=["per-item map_attr@k", "per-item ndcg_attr@k"],
    )
```

- [ ] **Step 4: 把新段落接進 `assemble_report`**

在 `assemble_report` 的 `candidates` list 中，於 `build_guardrail_recall_section(metrics, parameters),` 那行之後插入一行：

```python
        build_per_item_attr_section(metrics, parameters),
```

插入後該段應為：

```python
        build_guardrail_recall_section(metrics, parameters),
        build_per_item_attr_section(metrics, parameters),
        build_category_section(metrics, parameters),
```

- [ ] **Step 5: 跑新測試確認 pass**

Run: `pytest tests/test_evaluation/test_report_builder.py -k per_item_attr -q`
Expected: PASS，`3 passed`。

- [ ] **Step 6: 跑整個檔案確認無回歸**

Run: `pytest tests/test_evaluation/test_report_builder.py -q`
Expected: PASS，`18 passed`（原 15 + 新 3）。

- [ ] **Step 7: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/per-item-attr-report
git add src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(evaluation): add per_item attribution section to report"
```

---

## Task 3: config 與詞彙表

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml:42-49`
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（`_GLOSSARY`）
- Test: `tests/test_evaluation/test_parameters_evaluation_yaml.py`、`tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫 failing 測試**

在 `tests/test_evaluation/test_parameters_evaluation_yaml.py` 的 `test_report_display_and_sections` 中，於 `assert rep["sections"]["category"] is True` 那行之後加一行：

```python
    assert rep["sections"]["per_item_attr"] is True
```

在 `tests/test_evaluation/test_report_builder.py` 結尾新增：

```python
def test_glossary_has_attr_entries():
    s = rb.build_glossary_section(_params())
    terms = set(s.tables[0]["指標"])
    assert "map_attr@k" in terms
    assert "ndcg_attr@k" in terms
```

- [ ] **Step 2: 跑測試確認 fail**

Run: `pytest tests/test_evaluation/test_parameters_evaluation_yaml.py::test_report_display_and_sections tests/test_evaluation/test_report_builder.py::test_glossary_has_attr_entries -q`
Expected: FAIL —— yaml 測試 `KeyError: 'per_item_attr'`；glossary 測試 `AssertionError`。

- [ ] **Step 3: 在 yaml 加 section toggle**

在 `conf/base/parameters_evaluation.yaml` 的 `sections:` 區塊，把 `guardrail_recall: true` 那行之後加一行（縮排對齊同層）：

```yaml
      per_item_attr: true
```

加入後該區塊應為：

```yaml
    sections:
      dataset_overview: true
      primary_map: true
      guardrail_recall: true
      per_item_attr: true
      category: true             # also gated by product_categories.enabled
      per_segment: true
      diagnostics: true
      baseline: true
```

- [ ] **Step 4: 在 `_GLOSSARY` 加兩列**

在 `report_builder.py` 的 `_GLOSSARY` list 中，於 `("ndcg@k", ...)` 那列之後插入兩列：

```python
    ("map_attr@k",
     "某產品為正解時 ap_contrib@k 的平均；ap_contrib@k = 該產品進前 k 時的"
     "累積精度。客戶該買它、模型排越前 → 值越高。非該產品自己的 mAP@k，"
     "是 mAP@k 拆到單一產品的貢獻"),
    ("ndcg_attr@k",
     "同 map_attr@k，單筆貢獻改用 ndcg_contrib@k（log 折扣排序品質，已用 "
     "iDCG 正規化）。越高越好"),
```

- [ ] **Step 5: 跑測試確認 pass**

Run: `pytest tests/test_evaluation/test_parameters_evaluation_yaml.py tests/test_evaluation/test_report_builder.py -q`
Expected: PASS（`test_parameters_evaluation_yaml.py` 全綠、`test_report_builder.py` 為 `19 passed`）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/per-item-attr-report
git add conf/base/parameters_evaluation.yaml src/recsys_tfb/evaluation/report_builder.py tests/test_evaluation/test_parameters_evaluation_yaml.py tests/test_evaluation/test_report_builder.py
git commit -m "feat(evaluation): add per_item_attr section toggle and glossary entries"
```

---

## 收尾驗證

- [ ] **跑 evaluation report 相關測試全綠**

Run: `pytest tests/test_evaluation/test_report_builder.py tests/test_evaluation/test_parameters_evaluation_yaml.py tests/test_pipelines/test_evaluation/test_generate_report.py -q`
Expected: PASS，無 failure。

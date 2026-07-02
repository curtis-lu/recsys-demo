# Training Diagnostics P2b-2: Cases 案例圖 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 為每個 (item × top@1 象限) 的全格最高/最低分案例各產一張單列 signed SHAP 貢獻橫條圖 + 完整 4-象限稽核 `cases_manifest.json`;移除已無消費者的 `examples`;文件同步。

**Architecture:** `select_shap_population`(Spark)加第二輸出 `case_rows`(每格 max/min score 各一列,role=high/low),與既有 profile 抽樣輸出**依目的解耦**;新 pandas 節點 `compute_quadrant_cases` 以一次小 SHAP 畫圖 + 建 manifest。`compute_quadrant_profiles` 一行不動。

**Tech Stack:** PySpark 3.3.2(Window/row_number/crc32/unionByName,無 UDF)、LightGBM 4.6.0、SHAP 0.42.1、matplotlib(Agg)、pandas 1.5.3、numpy 1.25.0。

**Spec:** `docs/superpowers/specs/2026-07-02-training-diagnostics-p2b2-cases-design.md`

**Worktree / 執行環境(每個 Bash 指令都以此開頭):**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2
```
跑測試一律:`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
(裸 pytest / 裸 .venv python 會抓到 main 的 src)。commit 訊息結尾附:
`Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29`

---

## File Structure

| 檔案 | 職責 | 動作 |
|---|---|---|
| `src/recsys_tfb/pipelines/training/diagnostics_spark.py` | Spark 選樣:profile 抽樣 + 全格極值 case_rows | 修改(擴為 2 輸出) |
| `src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py` | 象限診斷:聚合 profile(不動)+ 新 cases 圖/manifest | 修改(加 `compute_quadrant_cases` + 2 helper) |
| `src/recsys_tfb/pipelines/training/diagnostics/paths.py` | 診斷產物路徑 | 修改(加 `cases_dir`) |
| `src/recsys_tfb/pipelines/training/diagnostics/__init__.py` | re-export | 修改(export `compute_quadrant_cases`) |
| `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py` | 全域/per-item/正例 SHAP | 修改(移除 `examples`) |
| `src/recsys_tfb/pipelines/training/pipeline.py` | training DAG | 修改(2 輸出、新節點、log 依賴) |
| `src/recsys_tfb/pipelines/training/nodes.py` | `log_experiment` | 修改(加 `cases_manifest` 參數 + metric) |
| `conf/base/catalog.yaml` | catalog | 修改(加 `cases_manifest`) |
| `conf/base/parameters_training.yaml` | config | 修改(加 `case_top_k`、移除 `n_examples`) |
| `docs/pipelines/training.md`、`README.md` | 文件 | 修改 |
| 對應 `tests/test_pipelines/test_training/*` | 測試 | 修改 |

---

## Task 1: `select_shap_population` 擴充 — 第二輸出 `case_rows`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics_spark.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics_spark.py`

### 背景（實作者必讀）
現況 `select_shap_population(training_eval_predictions, test_model_input, parameters, predict_manifest=None)` 回傳**單一** pandas `shap_population`(每 (item×象限) 用 `crc32(_ck)` 抽 `quadrant_sample_per_cell` 列)。本 task 改為回傳 **`(shap_population, case_rows)` tuple**:第一個維持 P2b-1 行為不變;第二個是每格「全格最高分列 + 全格最低分列」(role=high/low),帶 `quadrant/role/rank/score/label/group` 欄 + 特徵。

`training_eval_predictions` 欄:`snap_date, cust_id, prod_name, score, label`。`test_model_input` 欄:`snap_date, cust_id, prod_name, <features…>, label`(**注意也有 `label`**,join 前須先 drop 以免 ambiguous column)。`_ck = concat_ws("|", group_cols + [item_col])`。**不對稱 tiebreak**:high 用 `orderBy(score DESC, _ck ASC)`、low 用 `orderBy(score ASC, _ck DESC)`,確保同分格 high/low 抓不同列,唯有真正單行格才落同一列。

- [ ] **Step 1: 先更新既有 3 個測試(改成解包 tuple),讓它們反映新契約**

在 `tests/test_pipelines/test_training/test_diagnostics_spark.py`:

`test_quadrant_assignment_and_features_joined` 內把
```python
    pdf = select_shap_population(preds, feats, _params())
```
改為
```python
    pdf, _cases = select_shap_population(preds, feats, _params())
```

`test_per_cell_cap_and_determinism` 內把
```python
    a = select_shap_population(preds, feats, p)
    b = select_shap_population(preds, feats, p)
```
改為
```python
    a, _ = select_shap_population(preds, feats, p)
    b, _ = select_shap_population(preds, feats, p)
```

`test_disabled_returns_none` 內把
```python
    assert select_shap_population(preds, feats, _params(enabled=False)) is None
```
改為
```python
    assert select_shap_population(preds, feats, _params(enabled=False)) == (None, None)
```

- [ ] **Step 2: 新增 case_rows 行為測試(失敗)**

在同檔末尾加入(沿用檔頭既有的 `_PRED_COLS` / `_FEAT_COLS` / `_params`):

```python
def test_case_rows_extremes_role_and_features(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    # c1/c2/c3 三位客戶,item A 都排第1(score 高於 B)→ (A, TP/FP) 依 label 分。
    # (A, TP) 有 3 列(c1,c2,c4 label1),分數 0.9/0.7/0.5 → high=c1, low=c4。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.1, 0),
         ("2024-01-31", "c2", "A", 0.7, 1), ("2024-01-31", "c2", "B", 0.1, 0),
         ("2024-01-31", "c4", "A", 0.5, 1), ("2024-01-31", "c4", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2), ("2024-01-31", "c2", "B", 1.3, 2.3),
         ("2024-01-31", "c4", "A", 1.4, 2.4), ("2024-01-31", "c4", "B", 1.5, 2.5)],
        _FEAT_COLS)
    _pop, cases = select_shap_population(preds, feats, _params())
    a_tp = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")]
    roles = {r.role: r.cust_id for r in a_tp.itertuples()}
    assert roles["high"] == "c1"          # 全格最高分
    assert roles["low"] == "c4"           # 全格最低分
    assert {"f0", "f1"} <= set(cases.columns)          # 特徵 join 進來
    assert {"quadrant", "role", "rank", "score", "label"} <= set(cases.columns)
    # 每個 rendered 案例都能對到分數(供 manifest)
    assert float(a_tp[a_tp.role == "high"]["score"].iloc[0]) == 0.9


def test_case_rows_single_row_cell_marks_same_row(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    # (A, TP) 只有 c1 一列 → high 與 low 落在同一 group-key。
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1), ("2024-01-31", "c1", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0), ("2024-01-31", "c1", "B", 1.1, 2.1)],
        _FEAT_COLS)
    _pop, cases = select_shap_population(preds, feats, _params())
    a_tp = cases[(cases.prod_name == "A") & (cases.quadrant == "TP")]
    hi = a_tp[a_tp.role == "high"].iloc[0]
    lo = a_tp[a_tp.role == "low"].iloc[0]
    assert (hi.snap_date, hi.cust_id) == (lo.snap_date, lo.cust_id)   # 同一列


def test_case_rows_disabled_returns_none_tuple(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    preds = spark.createDataFrame([("2024-01-31", "c1", "A", 0.9, 1)], _PRED_COLS)
    feats = spark.createDataFrame([("2024-01-31", "c1", "A", 1.0, 2.0)], _FEAT_COLS)
    assert select_shap_population(preds, feats, _params(enabled=False)) == (None, None)
```

- [ ] **Step 3: 跑測試確認失敗**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics_spark.py -q
```
Expected: 既有 3 測試因解包 `None`(舊碼回單值)而 fail;3 個新測試 fail(`cases` 相關)。

- [ ] **Step 4: 實作 — 改 `select_shap_population` 回 tuple + 建 case_rows**

把 `src/recsys_tfb/pipelines/training/diagnostics_spark.py` 的 `select_shap_population` 整個函式替換為:

```python
def select_shap_population(
    training_eval_predictions, test_model_input, parameters, predict_manifest=None
):
    """回傳 ``(shap_population, case_rows)``。

    shap_population:每 (item×象限) ``crc32`` 抽樣 profile 樣本(P2b-1,不變)。
    case_rows:每 (item×象限) 全格最高/最低分各一列(``role=high/low``),帶
    ``quadrant/role/rank/score/label`` + group 欄 + 特徵,供 ``compute_quadrant_cases``
    畫單列案例圖。rank/象限/選樣/join 全在 Spark(executor);driver 只 toPandas 小族群。

    ``quadrant_enabled=false`` → ``(None, None)``。best-effort:選樣失敗亦回 ``(None, None)``
    (不中斷訓練)。``predict_manifest`` 僅作 in-DAG 排序依賴(與 ``compute_test_mAP_spark``
    同慣例;三個資料輸入皆無 node producer,不掛此依賴會被 topo-sort 排到 predict 前讀到
    未寫入的預測)。
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from recsys_tfb.core.schema import get_schema

    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None, None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    group_cols = [time_col] + entity_cols

    try:
        # rank:item_col 作 tie-break,讓象限指派在同分時可重現。
        w_rank = Window.partitionBy(*group_cols).orderBy(
            F.col("score").desc(), F.col(item_col))
        ranked = training_eval_predictions.withColumn("_rank", F.row_number().over(w_rank))

        is_top = F.col("_rank") <= F.lit(top_k_decision)
        is_pos = F.col(label_col) == F.lit(1)
        quadrant = (
            F.when(is_top & is_pos, F.lit("TP"))
            .when(is_top & ~is_pos, F.lit("FP"))
            .when(~is_top & is_pos, F.lit("FN"))
            .otherwise(F.lit("TN"))
        )
        ck = F.concat_ws("|", *[F.col(c).cast("string") for c in group_cols + [item_col]])
        labeled = ranked.withColumn("quadrant", quadrant).withColumn("_ck", ck)

        # ---- 輸出 1:profile 抽樣(crc32 每格取 <= per_cell;P2b-1 行為不變)----
        w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
            F.crc32(F.col("_ck")), F.col("_ck"))
        sampled = (
            labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
            .where(F.col("_cell_rn") <= F.lit(per_cell))
        )
        keyset = sampled.select(*group_cols, item_col, "quadrant")
        pop_pdf = keyset.join(
            test_model_input, on=group_cols + [item_col], how="inner").toPandas()

        # ---- 輸出 2:全格極值案例(role=high/low)----
        # 不對稱 tiebreak:同分格 high/low 落不同列;真正單行格才落同一列。
        w_high = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col("score").desc(), F.col("_ck").asc())
        w_low = Window.partitionBy(item_col, "quadrant").orderBy(
            F.col("score").asc(), F.col("_ck").desc())
        highs = (labeled.withColumn("_rn", F.row_number().over(w_high))
                 .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("high")))
        lows = (labeled.withColumn("_rn", F.row_number().over(w_low))
                .where(F.col("_rn") == F.lit(1)).withColumn("role", F.lit("low")))
        extremes = highs.unionByName(lows).select(
            *group_cols, item_col, "quadrant", "role",
            F.col("_rank").alias("rank"), F.col("score").alias("score"),
            F.col(label_col).alias("label"))
        # test_model_input 也有 label 欄 → drop 以免 join 後 ambiguous(label 非特徵)。
        feats_only = (test_model_input.drop(label_col)
                      if label_col in test_model_input.columns else test_model_input)
        case_pdf = extremes.join(
            feats_only, on=group_cols + [item_col], how="inner").toPandas()
    except Exception as e:  # best-effort:選樣失敗不中斷訓練(spec §12)
        logger.warning("select_shap_population failed: %s", e)
        return None, None

    logger.info(
        "select_shap_population: pop_rows=%d case_rows=%d items=%d per_cell=%d",
        len(pop_pdf), len(case_pdf),
        pop_pdf[item_col].nunique() if len(pop_pdf) else 0, per_cell,
    )
    return pop_pdf, case_pdf
```

同時把檔頭 docstring 末句更新為:「P2b-2 已擴為第二輸出 `case_rows`(全格極值案例)。」(把原「P2b-2 會擴充…」那句改成完成式)。

- [ ] **Step 5: 跑測試確認通過**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics_spark.py -q
```
Expected: 全數 PASS(3 更新 + 3 新增,共 6)。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
git add src/recsys_tfb/pipelines/training/diagnostics_spark.py \
  tests/test_pipelines/test_training/test_diagnostics_spark.py && \
git commit -F - <<'EOF'
feat(training-diag): select_shap_population 加第二輸出 case_rows(全格極值案例)

每 (item×象限) 全格最高/最低分各一列(role=high/low,不對稱 tiebreak),帶
rank/score/label + 特徵,供 P2b-2 cases 圖。profile 抽樣輸出不變;label 於
join 前 drop 以免 ambiguous。無 UDF、best-effort 回 (None,None)。

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29
EOF
```

---

## Task 2: `compute_quadrant_cases` + `cases_dir` + 單列橫條圖

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/paths.py`
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py`
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/__init__.py`
- Test: `tests/test_pipelines/test_training/test_shap_cases.py`

- [ ] **Step 1: 先加 `cases_dir` helper(無測試,供後續使用)**

在 `src/recsys_tfb/pipelines/training/diagnostics/paths.py` 的 `per_item_summary_dir` 之後、`safe_name` 之前插入:

```python
def cases_dir(parameters: dict) -> Path:
    """Resolve（並建立）diagnostics/cases/ —— 每 (item×象限) 極值案例圖 + manifest。"""
    d = diagnostics_dir(parameters) / "cases"
    d.mkdir(parents=True, exist_ok=True)
    return d
```

- [ ] **Step 2: 寫 `compute_quadrant_cases` 的失敗測試**

在 `tests/test_pipelines/test_training/test_shap_cases.py` 末尾(在既有 Task 3 wiring 測試「之前」,即 `# ---- Task 3: wiring` 註解上方)插入:

```python
# ---- P2b-2: compute_quadrant_cases ----

from recsys_tfb.pipelines.training.diagnostics.shap_cases import compute_quadrant_cases


def _case_rows(specs):
    """specs: list of (item, quadrant, role, cust, score, rank, label)。加 f0/f1 特徵。"""
    rng = np.random.RandomState(3)
    rows = []
    for (item, q, role, cust, score, rank, label) in specs:
        rows.append(("2024-01-31", cust, item, q, role, rank, float(score), int(label),
                     rng.randn(), rng.randn()))
    return pd.DataFrame(rows, columns=["snap_date", "cust_id", "prod_name", "quadrant",
                                       "role", "rank", "score", "label", "f0", "f1"])


def _cases_params():
    return {"schema": {"item": "prod_name", "label": "label",
                       "time": "snap_date", "entity": ["cust_id"]},
            "model_version": "testmv_cases",
            "diagnostics": {"shap": {"quadrant_enabled": True, "case_top_k": 2}}}


def test_quadrant_cases_manifest_complete_grid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    # A:TP 兩列(high/low 不同 cust)、TN 兩列;FP/FN 無列(空格)。
    rows = _case_rows([
        ("A", "TP", "high", "c1", 0.9, 1, 1), ("A", "TP", "low", "c2", 0.5, 1, 1),
        ("A", "TN", "high", "c3", 0.4, 2, 0), ("A", "TN", "low", "c4", 0.1, 2, 0)])
    out = compute_quadrant_cases(adapter, rows, _PREP, _cases_params())
    assert set(out) == {"A"}
    assert set(out["A"]) == {"TP", "FP", "FN", "TN"}          # 完整 4 象限
    assert out["A"]["TP"]["high"]["rendered"] is True
    assert out["A"]["TP"]["low"]["rendered"] is True
    assert out["A"]["FP"]["high"]["reason"] == "empty"        # 空格
    assert out["A"]["FP"]["low"]["reason"] == "empty"
    assert out["A"]["TP"]["high"]["cust"] == "c1"
    assert out["A"]["TP"]["high"]["score"] == 0.9
    # PNG 實際落地
    base = tmp_path / "data/models/testmv_cases/diagnostics/cases/A"
    assert (base / "TP_high.png").exists()
    assert (base / "TP_low.png").exists()
    assert not (base / "FP_high.png").exists()


def test_quadrant_cases_single_row_cell(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    # A/TP 單行格:high 與 low 同一 cust(c1)。
    rows = _case_rows([
        ("A", "TP", "high", "c1", 0.9, 1, 1), ("A", "TP", "low", "c1", 0.9, 1, 1)])
    out = compute_quadrant_cases(adapter, rows, _PREP, _cases_params())
    assert out["A"]["TP"]["high"]["rendered"] is True
    assert out["A"]["TP"]["low"]["rendered"] is False
    assert out["A"]["TP"]["low"]["reason"] == "single_row_same_as_high"
    base = tmp_path / "data/models/testmv_cases/diagnostics/cases/A"
    assert (base / "TP_high.png").exists()
    assert not (base / "TP_low.png").exists()               # 不產重複檔


def test_quadrant_cases_empty_or_disabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    assert compute_quadrant_cases(adapter, None, _PREP, _cases_params()) == {}
    empty = _case_rows([])
    assert compute_quadrant_cases(adapter, empty, _PREP, _cases_params()) == {}
    rows = _case_rows([("A", "TP", "high", "c1", 0.9, 1, 1),
                       ("A", "TP", "low", "c2", 0.5, 1, 1)])
    p = _cases_params(); p["diagnostics"]["shap"]["quadrant_enabled"] = False
    assert compute_quadrant_cases(adapter, rows, _PREP, p) == {}
```

- [ ] **Step 3: 跑測試確認失敗**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_shap_cases.py -q
```
Expected: 3 個新測試 fail(`ImportError: cannot import name 'compute_quadrant_cases'`)。

- [ ] **Step 4: 實作 `compute_quadrant_cases` + 2 helper**

在 `src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py`:先把 top of file 的 imports 補上 numpy(檔案目前有 `import pandas as pd`,加 `import numpy as np`)。在 `compute_quadrant_profiles` 之後追加:

```python
def _render_case(shap_row, feature_cols, top_k, item, quadrant, role, meta_row, out_dir):
    """畫單列 signed SHAP 橫條圖,回傳存檔 Path;失敗回 None(per-chart 隔離)。"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from .paths import safe_name

    try:
        order = np.argsort(np.abs(shap_row))[::-1][:top_k]
        # barh 由下往上,反轉讓最大貢獻在最上方
        feats = [feature_cols[i] for i in order][::-1]
        vals = [float(shap_row[i]) for i in order][::-1]
        colors = ["tab:red" if v > 0 else "tab:blue" for v in vals]
        fig = plt.figure(figsize=(8, max(2.0, 0.4 * len(feats))))
        try:
            ax = fig.add_subplot(111)
            ax.barh(range(len(feats)), vals, color=colors)
            ax.set_yticks(range(len(feats)))
            ax.set_yticklabels(feats, fontsize=8)
            ax.axvline(0, color="black", linewidth=0.6)
            ax.set_xlabel("signed SHAP (log-odds)")
            ax.set_title(
                f"{item} · {quadrant} · {role} · score={float(meta_row['score']):.3f}"
                f" · rank={int(meta_row['rank'])} · label={int(meta_row['label'])}",
                fontsize=9)
            for y, v in enumerate(vals):
                ax.text(v, y, f" {v:+.3f}", va="center",
                        ha="left" if v >= 0 else "right", fontsize=7)
            fig.tight_layout()
            png_path = out_dir / safe_name(item) / f"{quadrant}_{role}.png"
            png_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(png_path, dpi=100)
            return png_path
        finally:
            plt.close(fig)
    except Exception as e:
        logger.warning("case chart failed (%s/%s/%s): %s", item, quadrant, role, e)
        return None


def _manifest_entry(meta_row, png_path, diag_dir, time_col, entity_cols):
    base = {"snap_date": str(meta_row[time_col]),
            "cust": "|".join(str(meta_row[c]) for c in entity_cols),
            "rank": int(meta_row["rank"]), "score": float(meta_row["score"]),
            "label": int(meta_row["label"])}
    if png_path is None:
        return {"rendered": False, "reason": "render_failed", **base}
    return {"rendered": True, "png": str(png_path.relative_to(diag_dir)), **base}


def compute_quadrant_cases(model, case_rows, preprocessor: dict, parameters: dict) -> dict:
    """per-(item×象限) 全格極值案例的單列 signed SHAP 橫條圖 + 完整稽核 manifest。

    ``case_rows`` 為 ``select_shap_population`` 的第二輸出(每 item×象限 role=high/low
    各一列)。回傳
    ``{"<item>": {"<quadrant>": {"high"/"low": {rendered, png|reason, snap_date, cust,
    rank, score, label}}}}``。None / 空 / ``quadrant_enabled=false`` → ``{}``。
    單次 SHAP over 那幾十列極值。best-effort:整體失敗 log + 回 ``{}``;單張圖失敗只記該筆
    ``reason=render_failed``,不影響其餘。空格記 ``reason=empty``;單行格 low 記
    ``reason=single_row_same_as_high``(不產重複檔)。
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if case_rows is None or len(case_rows) == 0:
        logger.warning("quadrant cases: empty case_rows; skipping")
        return {}

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    from .paths import cases_dir, diagnostics_dir

    case_top_k = int(cfg.get("case_top_k", 15))
    schema = get_schema(parameters)
    item_col = schema["item"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    feature_cols = list(preprocessor["feature_columns"])

    try:
        pdf = case_rows.reset_index(drop=True)
        X = _pdf_to_X(pdf, preprocessor, parameters)
        log_data_volume(logger, "cases.X", X)
        shap_values = feature_attributions(model, X, feature_cols)

        cdir = cases_dir(parameters)
        ddir = diagnostics_dir(parameters)
        items = pdf[item_col].values
        quads = pdf["quadrant"].values
        roles = pdf["role"].values

        def _gkey(i):
            return tuple(str(pdf.iloc[i][c]) for c in [time_col] + entity_cols)

        manifest: dict = {}
        for item in pd.unique(items):
            item_entry: dict = {}
            for q in _QUADRANTS:
                idx = np.where((items == item) & (quads == q))[0]
                if len(idx) == 0:
                    item_entry[q] = {
                        "high": {"rendered": False, "reason": "empty"},
                        "low": {"rendered": False, "reason": "empty"}}
                    continue
                by_role = {roles[i]: i for i in idx}
                hi, lo = by_role.get("high"), by_role.get("low")
                cell: dict = {}
                # high(非空格必有)
                hi_png = _render_case(shap_values[hi], feature_cols, case_top_k,
                                      item, q, "high", pdf.iloc[hi], cdir)
                cell["high"] = _manifest_entry(pdf.iloc[hi], hi_png, ddir,
                                               time_col, entity_cols)
                # low:單行格(與 high 同列)→ 不重畫
                if lo is not None and _gkey(hi) != _gkey(lo):
                    lo_png = _render_case(shap_values[lo], feature_cols, case_top_k,
                                          item, q, "low", pdf.iloc[lo], cdir)
                    cell["low"] = _manifest_entry(pdf.iloc[lo], lo_png, ddir,
                                                  time_col, entity_cols)
                else:
                    cell["low"] = {"rendered": False,
                                   "reason": "single_row_same_as_high"}
                item_entry[q] = cell
            manifest[str(item)] = item_entry
    except Exception as e:  # best-effort:診斷失敗不中斷訓練
        logger.warning("quadrant cases failed: %s", e)
        return {}

    n_rendered = sum(1 for it in manifest.values() for cell in it.values()
                     for r in cell.values() if r.get("rendered"))
    logger.info("quadrant cases: items=%d rendered=%d", len(manifest), n_rendered)
    return manifest
```

- [ ] **Step 5: export `compute_quadrant_cases`**

在 `src/recsys_tfb/pipelines/training/diagnostics/__init__.py` 找到 export `compute_quadrant_profiles` 的地方,一併加入 `compute_quadrant_cases`(from `.shap_cases` import 與 `__all__` 兩處都要,對齊既有寫法)。

- [ ] **Step 6: 跑測試確認通過**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_shap_cases.py -q
```
Expected: 全數 PASS(既有 profile 測試 + 3 新 cases 測試)。

- [ ] **Step 7: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
git add src/recsys_tfb/pipelines/training/diagnostics/paths.py \
  src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py \
  src/recsys_tfb/pipelines/training/diagnostics/__init__.py \
  tests/test_pipelines/test_training/test_shap_cases.py && \
git commit -F - <<'EOF'
feat(training-diag): compute_quadrant_cases — 單列 signed SHAP 案例圖 + manifest

每 (item×象限) 全格極值案例畫 signed 橫條圖(紅推高/藍拉低,case_top_k),寫
cases/<item>/{象限}_{high,low}.png;回傳完整 4-象限稽核 manifest(空格 empty、
單行格 single_row)。單次 SHAP、per-chart best-effort、cust 不 hash。

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29
EOF
```

---

## Task 3: 接線 — pipeline / catalog / config / nodes.py + 結構測試

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Modify: `conf/base/catalog.yaml`
- Modify: `conf/base/parameters_training.yaml`
- Test: `tests/test_pipelines/test_training/test_shap_cases.py`(wiring)、`tests/test_pipelines/test_training/test_pipeline.py`(結構)

- [ ] **Step 1: 更新結構測試(失敗)**

在 `tests/test_pipelines/test_training/test_pipeline.py`:

`test_pipeline_node_count` 內 `assert len(pipeline.nodes) == 17` → `== 18`。
`test_calibration_pipeline_node_count` 內 `assert len(pipeline.nodes) == 19` → `== 20`。

`test_pipeline_outputs` 的 `expected` 集合裡,把
```python
            "shap_population", "quadrant_profiles",
```
改為
```python
            "shap_population", "case_rows", "quadrant_profiles", "cases_manifest",
```

E2E 測試(約 L329 起)的 `skipped_node_names` 集合加入 `"compute_quadrant_cases"`;並在
`catalog.add("quadrant_profiles", MemoryDataset({}))` 之後加一行:
```python
        catalog.add("cases_manifest", MemoryDataset({}))
```
(compute_quadrant_cases 因依賴被跳過的 `case_rows` 而跳過,故 stub 其輸出讓 log_experiment 可載入。)

- [ ] **Step 2: 更新 wiring 測試(失敗)**

在 `tests/test_pipelines/test_training/test_shap_cases.py` 的 `test_pipeline_wires_quadrant_nodes` 內,於既有 assert 後加:
```python
    assert "compute_quadrant_cases" in fns
    assert "cases_manifest" in log_node.inputs
    sp = next(n for n in pipe.nodes if n.func.__name__ == "select_shap_population")
    assert sp.outputs == ["shap_population", "case_rows"]
```

新增 catalog 測試(在 `test_catalog_has_quadrant_profiles` 之後):
```python
def test_catalog_has_cases_manifest():
    from pathlib import Path

    import yaml

    catalog_path = Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml"
    cat = yaml.safe_load(catalog_path.read_text())
    assert cat["cases_manifest"]["type"] == "JSONDataset"
    assert "cases/cases_manifest.json" in cat["cases_manifest"]["filepath"]
```

新增 config 測試(同檔末):
```python
def test_config_has_case_top_k():
    from pathlib import Path

    import yaml

    p = Path(__file__).resolve().parents[3] / "conf" / "base" / "parameters_training.yaml"
    cfg = yaml.safe_load(p.read_text())["diagnostics"]["shap"]
    assert cfg["case_top_k"] == 15
```

- [ ] **Step 3: 跑測試確認失敗**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_pipeline.py \
  tests/test_pipelines/test_training/test_shap_cases.py -q
```
Expected: 結構 / wiring / catalog / config 新斷言 fail。

- [ ] **Step 4: catalog — 加 `cases_manifest`**

在 `conf/base/catalog.yaml` 的 `quadrant_profiles` 區塊之後加:
```yaml

cases_manifest:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/cases/cases_manifest.json
```

- [ ] **Step 5: config — 加 `case_top_k`**

在 `conf/base/parameters_training.yaml` 的 `quadrant_min_rows: 10` 那行之後加:
```yaml
    case_top_k: 15                  # 單列 case 案例圖顯示的特徵數（|SHAP| 最大的前 N）
```

- [ ] **Step 6: pipeline — 2 輸出 + 新節點 + log 依賴**

在 `src/recsys_tfb/pipelines/training/pipeline.py`:

import 區的
```python
from recsys_tfb.pipelines.training.diagnostics import (
    compute_feature_importance,
    compute_feature_statistics,
    compute_quadrant_profiles,
    compute_shap_diagnostics,
)
```
加入 `compute_quadrant_cases`(維持字母序):
```python
from recsys_tfb.pipelines.training.diagnostics import (
    compute_feature_importance,
    compute_feature_statistics,
    compute_quadrant_cases,
    compute_quadrant_profiles,
    compute_shap_diagnostics,
)
```

`select_shap_population` 節點的 `outputs="shap_population"` 改為 `outputs=["shap_population", "case_rows"]`。

在 `compute_quadrant_profiles` 節點之後、`log_experiment` 節點之前插入新節點:
```python
        # P2b-2 象限案例:每 (item×象限) 全格極值案例的單列 signed SHAP 圖 + manifest。
        # 與 profile 依目的解耦(讀 case_rows,自己一次小 SHAP);compute_quadrant_profiles 不動。
        Node(
            compute_quadrant_cases,
            inputs=["model", "case_rows", "preprocessor_view", "parameters"],
            outputs="cases_manifest",
        ),
```

`log_experiment` 節點的 inputs 末端(在 `"quadrant_profiles"` 之後)加 `"cases_manifest"`:
```python
            inputs=[
                "model", "best_params", "best_iteration", "evaluation_results",
                "feature_statistics", "feature_importance", "shap_diagnostics",
                "parameters", "quadrant_profiles", "cases_manifest",
            ],
```
並把該節點上方的排序註解補一句:「`cases_manifest` 亦置末(default None),保證 cases PNG/manifest 於 log_artifacts 前寫好。」

- [ ] **Step 7: nodes.py — `log_experiment` 加參數 + metric**

在 `src/recsys_tfb/pipelines/training/nodes.py` 的 `log_experiment` 簽名末端(`quadrant_profiles: dict = None,` 之後)加:
```python
    cases_manifest: dict = None,
```

在 `n_quadrant_cells` metric 區塊(約 L967-969)之後加:
```python
                if cases_manifest:
                    n_cases = sum(
                        1 for it in cases_manifest.values() for cell in it.values()
                        for r in cell.values() if r.get("rendered")
                    )
                    mlflow.log_metric("n_cases_rendered", n_cases)
```

- [ ] **Step 8: 跑測試確認通過**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_pipeline.py \
  tests/test_pipelines/test_training/test_shap_cases.py \
  tests/test_pipelines/test_training/test_resume_contracts.py -q
```
Expected: 全數 PASS(結構、wiring、catalog、config、resume contracts 不受影響)。

- [ ] **Step 9: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
git add src/recsys_tfb/pipelines/training/pipeline.py \
  src/recsys_tfb/pipelines/training/nodes.py \
  conf/base/catalog.yaml conf/base/parameters_training.yaml \
  tests/test_pipelines/test_training/test_pipeline.py \
  tests/test_pipelines/test_training/test_shap_cases.py && \
git commit -F - <<'EOF'
feat(training-diag): 接線 P2b-2 cases 節點(pipeline/catalog/config/log_experiment)

select_shap_population 改 2 輸出;新增 compute_quadrant_cases 節點 →
cases_manifest(JSONDataset);config 加 case_top_k=15;log_experiment 末加
cases_manifest 輸入(排序 edge)+ n_cases_rendered metric。結構/wiring 測試更新。

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29
EOF
```

---

## Task 4: 收掉 `examples` + 移除死碼 `n_examples`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py`
- Modify: `conf/base/parameters_training.yaml`
- Test: `tests/test_pipelines/test_training/test_diagnostics.py`

### 背景
`compute_shap_diagnostics` 回傳 dict 的 `"examples"`(`{high, low, per_item_high}`,每筆全特徵 SHAP dict)自 P1 移除 waterfall 後**無任何程式消費者**,由 P2b-2 的 `cases/` 取代。`n_examples` config 僅供此區塊,一併移除。

- [ ] **Step 1: 更新既有測試以反映 examples 已移除(失敗)**

在 `tests/test_pipelines/test_training/test_diagnostics.py`:

L139 `assert set(out) >= {"global", "per_item", "examples"}`
→ 改為
```python
    assert set(out) >= {"global", "per_item", "item_idiosyncrasy"}
    assert "examples" not in out
```

刪除 L148-L150 三行:
```python
    assert {"high", "low"} <= set(out["examples"])
    items_in_examples = {e["item"] for e in out["examples"]["per_item_high"]}
    assert {"A", "B", "rare"} <= items_in_examples
```

L121 註解「單次 SHAP 供 global/per_item/examples 三用」→ 改為「單次 SHAP 供 global/per_item 兩用」。

（L112、L324、L416 的 params 內 `"n_examples": 1` 是無害殘留;順手刪除該 key 以保持乾淨,但非必要——若刪,確認該行仍是合法 dict。）

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics.py -q
```
Expected: 更新的斷言 fail(舊碼仍產 `examples`)。

- [ ] **Step 3: 移除 `examples` 產生碼**

在 `src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py` 的 `compute_shap_diagnostics`:

刪除 `n_examples = int(cfg.get("n_examples", 5))` 這行(約 L105)。

刪除整個代表性個例區塊(約 L199-L213):
```python
    # ---- 代表性個例（全域 high/low + 每 item 一筆高分）----
    def _example(i):
        return {"item": str(items[i]), "score": float(scores[i]),
                "shap": {feature_cols[j]: _to_native(shap_values[i, j]) for j in range(len(feature_cols))}}

    hi = np.argsort(scores)[::-1][:n_examples]
    lo = np.argsort(scores)[:n_examples]
    per_item_high = []
    for item in pd.unique(items):
        pos = np.where(items == item)[0]
        best = pos[np.argmax(scores[pos])]
        per_item_high.append(_example(best))
    examples = {"high": [_example(i) for i in hi],
                "low": [_example(i) for i in lo],
                "per_item_high": per_item_high}
```

回傳語句(約 L246-247)由
```python
    return {"global": {"top_features": global_top}, "per_item": per_item, "examples": examples,
            "item_idiosyncrasy": item_idiosyncrasy}
```
改為
```python
    return {"global": {"top_features": global_top}, "per_item": per_item,
            "item_idiosyncrasy": item_idiosyncrasy}
```

（`scores = model.predict(X)` 仍保留:`per_item` 的 `score_min/max/mean` 用得到。）

- [ ] **Step 4: 移除 config `n_examples`**

在 `conf/base/parameters_training.yaml` 刪除
```yaml
    n_examples: 5           # shap_diagnostics.json 中全域 high/low 案例摘要數
```
這行(約 L153)。

- [ ] **Step 5: 跑測試確認通過**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_diagnostics.py -q
```
Expected: 全數 PASS。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
git add src/recsys_tfb/pipelines/training/diagnostics/shap_per_item.py \
  conf/base/parameters_training.yaml \
  tests/test_pipelines/test_training/test_diagnostics.py && \
git commit -F - <<'EOF'
refactor(training-diag): 移除無消費者的 examples 區塊 + 死碼 n_examples

compute_shap_diagnostics 的 examples(high/low/per_item_high)自 P1 移除
waterfall 後已無消費者,由 P2b-2 cases/ 取代;一併移除 config n_examples。
回傳改為 {global, per_item, item_idiosyncrasy}。

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29
EOF
```

---

## Task 5: 文件 — README + training.md 補象限診斷家族、清 examples

**Files:**
- Modify: `docs/pipelines/training.md`
- Modify: `README.md`

### 背景
P2b-1 的 `per_quadrant.json` / 象限聚合 profile **從未寫入文件**;README/training.md 仍把 `examples` 當現役。本 task 一併補齊象限診斷家族(profiles + cases)並清 examples。無自動化測試,以 grep 驗證。

- [ ] **Step 1: `docs/pipelines/training.md` — 清 examples 描述**

- L208 `diagnostics.shap` 摘要列:把「案例數」改為「per-(item×象限) profile 與極值案例圖」。
  原:`| `diagnostics.shap` | 控制 SHAP 開關、抽樣量、top K、案例數、計算預算與 per-item 強化（方向、採購者對照、偏離度） |`
  改:`| `diagnostics.shap` | 控制 SHAP 開關、抽樣量、top K、計算預算、per-item 強化（方向、採購者對照、偏離度）與象限診斷（per-(item×象限) profile 與極值案例圖） |`
- L234 整列刪除(`控制全域 high/low 案例數 | `n_examples` | …`)。
- L418 那句(「beeswarm 同時呈現 SHAP 幅度與方向；高分、低分與 per-item 高分案例摘要則寫在 `shap_diagnostics.json` 的 `examples` 區塊。」)把 `examples` 那半句刪掉,只保留 beeswarm 敘述;改為:「beeswarm 同時呈現 SHAP 幅度與方向。象限案例圖見下方象限診斷小節。」

- [ ] **Step 2: `docs/pipelines/training.md` — 新增象限診斷小節**

在 `diagnostics.shap` 設定詳細說明小節(§`shap_diagnostics.json` 欄位表之後,約 L245 之後)新增一個小節:

```markdown
#### 象限診斷（top@1 TP/FP/FN/TN）

象限診斷聚焦「模型的 top@1 決策」：對每個 query group（time × entity），以最高分候選為決策。
依 `label` 分四象限——`TP`（排第 1 且採用）、`FP`（排第 1 未採用）、`FN`（未排第 1 但採用）、
`TN`（未排第 1 未採用）。由 `quadrant_enabled` 開關（沿用同一組 `quadrant_*` 設定）。

| 產物 | 內容 | 位置 |
| --- | --- | --- |
| `per_quadrant.json` | 每 (item×象限) 聚合的 signed SHAP profile（平均驅動特徵 + 方向 + `low_coverage`），每格抽樣 `quadrant_sample_per_cell` 列 | `diagnostics/per_quadrant.json` |
| 案例圖 | 每 (item×象限) 全格最高分、最低分各一列的單列 signed SHAP 貢獻橫條圖（`case_top_k` 個特徵，紅=推高分、藍=拉低分） | `diagnostics/cases/<item>/{TP,FP,FN,TN}_{high,low}.png` |
| `cases_manifest.json` | 完整 4-象限稽核表：每 item × 4 象限 × {high,low} 一筆，含 `snap_date/cust/rank/score/label` 與 PNG 路徑；空格記 `reason=empty`、單行格 low 記 `reason=single_row_same_as_high` | `diagnostics/cases/cases_manifest.json` |

**top@1 本質**：多數 item 從不會被排到第 1，因此其 `TP/FP` 格常為空（manifest 記 `empty`），
`FN/TN` 才飽滿——這本身即是「該 item 幾乎不被列為首選」的重要訊號，而非缺漏。
案例圖用來看「某位客戶在某象限被排高／排低，具體靠哪些特徵」，與 `per_quadrant.json` 的
「平均驅動特徵」互補。SHAP 值在 log-odds（margin）尺上；正值把分數推高、負值拉低。
```

同時在 config 旋鈕表(L229-235 區)加一列:
```markdown
| 控制案例圖特徵數 | `case_top_k` | 單列 case 圖顯示 |SHAP| 最大的前 N 個特徵；預設 15，太擠可再降 |
```

- [ ] **Step 3: `docs/pipelines/training.md` — 節點表 / 產物表 補象限**

- 節點表(約 L392 附近,`compute_shap_diagnostics` 那列之後)加列:
```markdown
| 象限選樣 | `select_shap_population` | training_eval_predictions、test_model_input | top@1 象限 + 每格抽樣（profile）與全格極值（cases） | `shap_population`、`case_rows` |
| 象限 profile | `compute_quadrant_profiles` | model、shap_population | per-(item×象限) 聚合 signed profile | `quadrant_profiles`（`per_quadrant.json`） |
| 象限案例 | `compute_quadrant_cases` | model、case_rows | 每 (item×象限) 極值案例單列 SHAP 圖 | `cases_manifest`、PNG |
```
- 產物表(約 L411,「模型診斷」那列)把描述補上 `per_quadrant.json`、`cases/` 與 `cases_manifest.json`。

- [ ] **Step 4: `README.md` — 更新診斷句**

L144 那句(「…並可輸出特徵統計、feature importance 與 SHAP 診斷（含 per-item 帶方向的特徵 profile、採購者對照與跨 item 偏離度 `item_idiosyncrasy`）…」)在括號內末端補上:「、象限（TP/FP/FN/TN）per-(item×象限) 聚合 profile 與極值案例 SHAP 圖」。

- [ ] **Step 5: grep 驗證**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
echo "== examples 應已無現役描述(只允許 plans/specs 歷史檔) ==" && \
grep -nE "n_examples|examples 區塊" docs/pipelines/training.md README.md || echo "OK: 無殘留" ; \
echo "== 象限產物應出現 ==" && \
grep -nE "per_quadrant|cases_manifest|case_top_k|象限" docs/pipelines/training.md | head && \
grep -nE "象限" README.md
```
Expected: 第一個 grep 無 training.md/README 命中(印 "OK: 無殘留");第二個 grep 有命中。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
git add docs/pipelines/training.md README.md && \
git commit -F - <<'EOF'
docs: 補象限診斷家族(per_quadrant + cases + manifest)、清 examples

training.md 新增象限診斷小節(top@1 定義、per_quadrant.json、cases 案例圖與
cases_manifest.json、top@1 空格訊號、case_top_k 旋鈕)並移除已收掉的 examples
描述;README 診斷句補象限 profile + 案例圖。回填 P2b-1 遺漏的 per_quadrant 文件。

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29
EOF
```

---

## 最終驗證(所有 task 完成後,real-run 前)

- [ ] **跑相關測試全綠**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/ -q
```
Expected: 全數 PASS。

- [ ] **Real-run（使用者明確要求,完成定義的一部分）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b2 && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```
（Spark cold start ~1–4min,以 background 執行不阻塞。）完成後檢視
`data/models/<model_version>/diagnostics/cases/` 的真實 PNG 與 `cases_manifest.json`,
用 SendUserFile 傳給使用者驗;不合理則回頭修。

---

## Self-Review（已對照 spec）

- **Spec coverage:** §5.1 select_shap_population→Task 1;§5.2/5.3 compute_quadrant_cases→Task 2;
  §5.4 cases_dir→Task 2 Step 1;§6 manifest→Task 2;§7 接線→Task 3;§8 收 examples→Task 4;
  §9 文件→Task 5;§10 測試→各 task TDD;§11 real-run→最終驗證;§12 邊界→Task 1/2 測試涵蓋。無缺口。
- **No placeholders:** 每個 code step 都有完整程式碼與確切指令/預期。
- **Type/name consistency:** `select_shap_population` 回 `(pop_pdf, case_pdf)`;pipeline outputs
  `["shap_population","case_rows"]`;`compute_quadrant_cases(model, case_rows, preprocessor, parameters)`
  → dict;catalog `cases_manifest`;config `case_top_k`;`_render_case`/`_manifest_entry` 前後一致;
  manifest 鍵 `rendered/png/reason/snap_date/cust/rank/score/label` 貫穿測試與實作一致。

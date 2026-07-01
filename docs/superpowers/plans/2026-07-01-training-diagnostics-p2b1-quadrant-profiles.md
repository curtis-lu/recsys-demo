# Training Diagnostics P2b-1 — 象限選樣 + per_quadrant profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 新增 Spark 選樣節點(top@1 象限 + 每 (item×象限) 確定性抽樣)+ pandas 節點算 per-(item×象限) 聚合 signed profile,產出獨立 `per_quadrant.json`。

**Architecture:** `diagnostics_spark.select_shap_population`(Spark,無 UDF)→ 小 pandas → `diagnostics/shap_cases.compute_quadrant_profiles`(單次 SHAP)→ catalog 存 `per_quadrant.json`。`compute_shap_diagnostics` 不動。

**Tech Stack:** PySpark 3.3.2(Window/crc32,無 UDF)、shap 0.42、pandas、pytest(含 `spark` fixture)。

**設計來源:** `docs/superpowers/specs/2026-07-01-training-diagnostics-p2b1-quadrant-profiles-design.md`

**測試執行(worktree,絕對 venv python + PYTHONPATH):**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-p2b
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```
(Spark 測試首次有 cold start,秒~數十秒;純 python 測試秒級。)

---

## File Structure

- **Create** `src/recsys_tfb/pipelines/training/diagnostics_spark.py` — `select_shap_population`(Spark;放子套件外,保持 `diagnostics/` 純 python)。
- **Create** `src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py` — `compute_quadrant_profiles`(pandas)。
- **Create** `tests/test_pipelines/test_training/test_diagnostics_spark.py` — 選樣節點 Spark 測試。
- **Create** `tests/test_pipelines/test_training/test_shap_cases.py` — profile 純 python 測試。
- **Modify** `conf/base/catalog.yaml` — 加 `quadrant_profiles` JSONDataset。
- **Modify** `conf/base/parameters_training.yaml` — 加 4 個 quadrant config 鍵。
- **Modify** `src/recsys_tfb/pipelines/training/pipeline.py` — 2 新節點 + log_experiment 加 `quadrant_profiles` 輸入。
- **Modify** `src/recsys_tfb/pipelines/training/nodes.py` — `log_experiment` 加 `quadrant_profiles` 參數 + scalar。

回歸網:`tests/test_pipelines/test_training/` 全綠(pipeline 結構/catalog 測試若斷言確切集合,依新增更新,不弱化)。

---

## Task 1: `select_shap_population`(Spark 選樣節點)

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics_spark.py`
- Test: `tests/test_pipelines/test_training/test_diagnostics_spark.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_training/test_diagnostics_spark.py`:
```python
"""Tests for select_shap_population (Spark 選樣:rank/象限/每格抽樣/join)."""


def _params(per_cell=30, top_k=1, enabled=True):
    return {"schema": {"time": "snap_date", "entity": ["cust_id"],
                       "item": "prod_name", "label": "label"},
            "diagnostics": {"shap": {"quadrant_enabled": enabled,
                                     "quadrant_top_k_decision": top_k,
                                     "quadrant_sample_per_cell": per_cell}}}


_PRED_COLS = ["snap_date", "cust_id", "prod_name", "score", "label"]
_FEAT_COLS = ["snap_date", "cust_id", "prod_name", "f0", "f1"]


def test_quadrant_assignment_and_features_joined(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1),   # rank1 adopted -> TP
         ("2024-01-31", "c1", "B", 0.2, 0),   # rank2 not     -> TN
         ("2024-01-31", "c2", "A", 0.8, 0),   # rank1 not     -> FP
         ("2024-01-31", "c2", "B", 0.3, 1)],  # rank2 adopted -> FN
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0),
         ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2),
         ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    pdf = select_shap_population(preds, feats, _params())
    q = {(r.cust_id, r.prod_name): r.quadrant for r in pdf.itertuples()}
    assert q[("c1", "A")] == "TP"
    assert q[("c1", "B")] == "TN"
    assert q[("c2", "A")] == "FP"
    assert q[("c2", "B")] == "FN"
    assert {"f0", "f1"} <= set(pdf.columns)        # 特徵 join 進來
    assert len(pdf) == 4


def test_per_cell_cap_and_determinism(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    # (A, TP) 有 2 列;per_cell=1 → 只留 1,且兩次結果相同
    preds = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 0.9, 1),
         ("2024-01-31", "c1", "B", 0.1, 0),
         ("2024-01-31", "c2", "A", 0.9, 1),
         ("2024-01-31", "c2", "B", 0.1, 0)],
        _PRED_COLS)
    feats = spark.createDataFrame(
        [("2024-01-31", "c1", "A", 1.0, 2.0),
         ("2024-01-31", "c1", "B", 1.1, 2.1),
         ("2024-01-31", "c2", "A", 1.2, 2.2),
         ("2024-01-31", "c2", "B", 1.3, 2.3)],
        _FEAT_COLS)
    p = _params(per_cell=1)
    a = select_shap_population(preds, feats, p)
    b = select_shap_population(preds, feats, p)
    tp_a = a[(a.prod_name == "A") & (a.quadrant == "TP")]
    tp_b = b[(b.prod_name == "A") & (b.quadrant == "TP")]
    assert len(tp_a) == 1
    assert list(tp_a["cust_id"]) == list(tp_b["cust_id"])   # 確定性


def test_disabled_returns_none(spark):
    from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
    preds = spark.createDataFrame([("2024-01-31", "c1", "A", 0.9, 1)], _PRED_COLS)
    feats = spark.createDataFrame([("2024-01-31", "c1", "A", 1.0, 2.0)], _FEAT_COLS)
    assert select_shap_population(preds, feats, _params(enabled=False)) is None
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_spark.py -q`
Expected: FAIL(`ModuleNotFoundError: diagnostics_spark`)。

- [ ] **Step 3: 實作**

`src/recsys_tfb/pipelines/training/diagnostics_spark.py`:
```python
"""Spark-side 選樣:top@1 象限 + 每 (item×象限) 確定性抽樣,交給 pandas SHAP 診斷。

放此(非 diagnostics/ 純 python 子套件)因為需要 Spark。全 native Spark,無 UDF。
P2b-2 會擴充為也標記每格 max/min 極值案例(role=high/low)。
"""

import logging

logger = logging.getLogger(__name__)


def select_shap_population(training_eval_predictions, test_model_input, parameters):
    """回傳每 (item×象限) 抽樣的小 pandas(特徵 + item + quadrant),供 per_quadrant SHAP。

    ``quadrant_enabled=false`` → None。rank/象限/抽樣/join 全在 Spark(executor);
    driver 只 toPandas 小族群。
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from recsys_tfb.core.schema import get_schema

    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        logger.info("select_shap_population: quadrant_enabled=false; skipping")
        return None

    top_k_decision = int(cfg.get("quadrant_top_k_decision", 1))
    per_cell = int(cfg.get("quadrant_sample_per_cell", 30))

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    group_cols = [time_col] + entity_cols

    w_rank = Window.partitionBy(*group_cols).orderBy(F.col("score").desc())
    ranked = training_eval_predictions.withColumn("_rank", F.row_number().over(w_rank))

    is_top = F.col("_rank") <= F.lit(top_k_decision)
    is_pos = F.col(label_col) == F.lit(1)
    quadrant = (
        F.when(is_top & is_pos, F.lit("TP"))
        .when(is_top & ~is_pos, F.lit("FP"))
        .when(~is_top & is_pos, F.lit("FN"))
        .otherwise(F.lit("TN"))
    )
    labeled = ranked.withColumn("quadrant", quadrant)

    # 確定性每格抽樣:crc32(key) 排序(key 為 tiebreaker),取 <= per_cell
    ck = F.concat_ws("|", *[F.col(c).cast("string") for c in group_cols + [item_col]])
    labeled = labeled.withColumn("_ck", ck)
    w_cell = Window.partitionBy(item_col, "quadrant").orderBy(
        F.crc32(F.col("_ck")), F.col("_ck"))
    sampled = (
        labeled.withColumn("_cell_rn", F.row_number().over(w_cell))
        .where(F.col("_cell_rn") <= F.lit(per_cell))
    )
    keyset = sampled.select(*group_cols, item_col, "quadrant")

    joined = keyset.join(test_model_input, on=group_cols + [item_col], how="inner")
    pdf = joined.toPandas()
    logger.info(
        "select_shap_population: rows=%d items=%d per_cell=%d",
        len(pdf), pdf[item_col].nunique() if len(pdf) else 0, per_cell,
    )
    return pdf
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_diagnostics_spark.py -q`
Expected: PASS(3 passed;首次含 Spark cold start)。

- [ ] **Step 5: Commit**
```bash
git add src/recsys_tfb/pipelines/training/diagnostics_spark.py tests/test_pipelines/test_training/test_diagnostics_spark.py
git commit -m "feat(diagnostics): select_shap_population Spark node (top@1 quadrant + per-cell sampling, no UDF)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 2: `compute_quadrant_profiles`(pandas)

**Files:**
- Create: `src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py`
- Test: `tests/test_pipelines/test_training/test_shap_cases.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_training/test_shap_cases.py`:
```python
"""Tests for compute_quadrant_profiles (per-item×quadrant signed profile,純 python)."""
import numpy as np
import pandas as pd

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.diagnostics.shap_cases import compute_quadrant_profiles


def _trained_adapter(seed=1):
    rng = np.random.RandomState(seed)
    Xtr = rng.randn(400, 2)
    ytr = (Xtr[:, 0] > 0).astype(float)
    adapter = LightGBMAdapter()
    adapter.train(Xtr, ytr, None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 15, "early_stopping_rounds": 0})
    return adapter


def _pop_from_counts(counts, seed=0):
    rng = np.random.RandomState(seed)
    rows = []
    for (item, q), c in counts.items():
        for _ in range(c):
            rows.append((rng.randn(), rng.randn(), item, q))
    return pd.DataFrame(rows, columns=["f0", "f1", "prod_name", "quadrant"])


def _params(min_rows=10):
    return {"schema": {"item": "prod_name", "label": "label",
                       "time": "snap_date", "entity": ["cust_id"]},
            "diagnostics": {"shap": {"quadrant_enabled": True, "top_k": 2,
                                     "quadrant_min_rows": min_rows}}}


_PREP = {"feature_columns": ["f0", "f1"], "categorical_columns": [], "category_mappings": {}}


def test_quadrant_profiles_structure():
    adapter = _trained_adapter()
    pop = _pop_from_counts({(i, q): 15 for i in ("A", "B")
                            for q in ("TP", "FP", "FN", "TN")})
    out = compute_quadrant_profiles(adapter, pop, _PREP, _params())
    assert set(out) == {"A", "B"}
    for item in ("A", "B"):
        assert set(out[item]) == {"TP", "FP", "FN", "TN"}
        for q, cell in out[item].items():
            assert cell["n_sampled"] == 15
            assert cell["low_coverage"] is False           # 15 >= 10
            assert len(cell["top_features"]) == 2
            assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
                       for r in cell["top_features"])


def test_quadrant_profiles_low_coverage_and_empty_cell():
    adapter = _trained_adapter()
    # A/TP=3(low)、A/FN=12、A/FP=12、A/TN=0(empty→缺席);B 各 12
    pop = _pop_from_counts({("A", "TP"): 3, ("A", "FN"): 12, ("A", "FP"): 12,
                            ("B", "TP"): 12, ("B", "FP"): 12, ("B", "FN"): 12, ("B", "TN"): 12})
    out = compute_quadrant_profiles(adapter, pop, _PREP, _params(min_rows=10))
    assert out["A"]["TP"]["low_coverage"] is True          # 3 < 10
    assert out["A"]["FN"]["low_coverage"] is False         # 12 >= 10
    assert "TN" not in out["A"]                            # 空格不出現


def test_quadrant_profiles_empty_or_disabled():
    adapter = _trained_adapter()
    assert compute_quadrant_profiles(adapter, None, _PREP, _params()) == {}
    empty = _pop_from_counts({})
    assert compute_quadrant_profiles(adapter, empty, _PREP, _params()) == {}
    pop = _pop_from_counts({("A", "TP"): 5})
    p = _params(); p["diagnostics"]["shap"]["quadrant_enabled"] = False
    assert compute_quadrant_profiles(adapter, pop, _PREP, p) == {}
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_shap_cases.py -q`
Expected: FAIL(`ModuleNotFoundError: shap_cases`)。

- [ ] **Step 3: 實作**

`src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py`:
```python
"""P2 象限診斷:per-(item×象限) 聚合 signed profile。P2b-2 續加案例圖。"""

import logging

import pandas as pd

from recsys_tfb.core.logging import log_data_volume

from .attribution import feature_attributions
from .shap_per_item import _signed_profile

logger = logging.getLogger(__name__)

_QUADRANTS = ("TP", "FP", "FN", "TN")


def compute_quadrant_profiles(model, shap_population, preprocessor: dict, parameters: dict) -> dict:
    """per-(item×象限) 平均 signed profile。

    回傳 ``{"<item>": {"<quadrant>": {"top_features":[…], "n_sampled":int,
    "low_coverage":bool}}}``。``shap_population`` 為 ``select_shap_population`` 的小
    pandas(特徵 + item + quadrant)。None / 空 / ``quadrant_enabled=false`` → ``{}``。
    單次 SHAP。best-effort:失敗 log + 回 ``{}``,不中斷訓練。
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if shap_population is None or len(shap_population) == 0:
        logger.warning("quadrant profiles: empty population; skipping")
        return {}

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    top_k = int(cfg.get("top_k", 30))
    quadrant_min_rows = int(cfg.get("quadrant_min_rows", 10))
    item_col = get_schema(parameters)["item"]
    feature_cols = list(preprocessor["feature_columns"])

    try:
        pdf = shap_population.reset_index(drop=True)
        X = _pdf_to_X(pdf, preprocessor, parameters)
        log_data_volume(logger, "quadrant.X", X)
        shap_values = feature_attributions(model, X, feature_cols)
        items = pdf[item_col].values
        quads = pdf["quadrant"].values
        out: dict = {}
        for item in pd.unique(items):
            for q in _QUADRANTS:
                mask = (items == item) & (quads == q)
                n = int(mask.sum())
                if n == 0:
                    continue
                prof, _ = _signed_profile(shap_values[mask], feature_cols, top_k)
                out.setdefault(str(item), {})[q] = {
                    "top_features": prof,
                    "n_sampled": n,
                    "low_coverage": bool(n < quadrant_min_rows),
                }
    except Exception as e:  # best-effort:診斷失敗不中斷訓練
        logger.warning("quadrant profiles failed: %s", e)
        return {}
    logger.info("quadrant profiles: items=%d", len(out))
    return out
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_shap_cases.py -q`
Expected: PASS(3 passed)。

- [ ] **Step 5: Commit**
```bash
git add src/recsys_tfb/pipelines/training/diagnostics/shap_cases.py tests/test_pipelines/test_training/test_shap_cases.py
git commit -m "feat(diagnostics): compute_quadrant_profiles (per-item×quadrant signed profile)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Task 3: 接線(catalog + pipeline + log_experiment + config)

**Files:**
- Modify: `conf/base/catalog.yaml`, `conf/base/parameters_training.yaml`
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`, `src/recsys_tfb/pipelines/training/nodes.py`
- Test: `tests/test_pipelines/test_training/` 既有 pipeline/catalog 測試(依需要更新)

- [ ] **Step 1: 寫/調整測試**

在 `tests/test_pipelines/test_training/test_shap_cases.py` 末尾加接線測試(用既有 catalog/pipeline 讀取慣例;若 repo 已有 `test_pipeline.py` 的結構測試,改該處):
```python
def test_pipeline_wires_quadrant_nodes():
    from recsys_tfb.pipelines.training.pipeline import create_pipeline
    pipe = create_pipeline()
    fns = {n.func.__name__ for n in pipe.nodes}
    assert "select_shap_population" in fns
    assert "compute_quadrant_profiles" in fns
    # log_experiment 依賴 quadrant_profiles(排序保證 per_quadrant.json 先寫)
    log_node = next(n for n in pipe.nodes if n.func.__name__ == "log_experiment")
    assert "quadrant_profiles" in log_node.inputs


def test_catalog_has_quadrant_profiles():
    import yaml
    with open("conf/base/catalog.yaml") as f:
        cat = yaml.safe_load(f)
    assert cat["quadrant_profiles"]["type"] == "JSONDataset"
    assert "per_quadrant.json" in cat["quadrant_profiles"]["filepath"]
```
（`Node` 的屬性名以 repo 實作為準:若非 `.func`/`.inputs`,改用實際屬性——實作前先看 `src/recsys_tfb/core/node.py`。）

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_shap_cases.py -q -k "pipeline or catalog"`
Expected: FAIL(節點/ catalog 尚未加)。

- [ ] **Step 3: 實作接線**

(a) `conf/base/catalog.yaml` 在 `shap_diagnostics:` 條目後加:
```yaml
quadrant_profiles:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/per_quadrant.json
```

(b) `conf/base/parameters_training.yaml` 在 `diagnostics.shap` 區塊末尾(`per_item_beeswarm` 之後)加:
```yaml
    # 象限診斷（P2b:TP/FP/FN/TN top@1;per-(item×象限) profile → per_quadrant.json）
    quadrant_enabled: true          # 關閉則不跑象限選樣/profile
    quadrant_top_k_decision: 1      # 象限「判正」名次界線（top@K）
    quadrant_sample_per_cell: 30    # 每 (item×象限) profile 抽樣目標數
    quadrant_min_rows: 10           # 某格 < 此 → low_coverage
```

(c) `src/recsys_tfb/pipelines/training/pipeline.py`:
- import 區加:
```python
from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
```
- 既有 `from recsys_tfb.pipelines.training.diagnostics import (...)` 內加入 `compute_quadrant_profiles`。
- 在 `compute_shap_diagnostics` 節點之後、`log_experiment` 節點之前,插入兩個節點:
```python
        Node(
            select_shap_population,
            inputs=["training_eval_predictions", "test_model_input", "parameters"],
            outputs="shap_population",
        ),
        Node(
            compute_quadrant_profiles,
            inputs=["model", "shap_population", "preprocessor_view", "parameters"],
            outputs="quadrant_profiles",
        ),
```
- `log_experiment` 節點的 `inputs` list 末尾(在 `"parameters"` 之前)加 `"quadrant_profiles"`。

(d) `src/recsys_tfb/pipelines/training/nodes.py` `log_experiment`:
- 簽名在 `shap_diagnostics: dict,` 之後加 `quadrant_profiles: dict = None,`(預設 None → 既有直接呼叫的測試不破)。
- 在 feature_statistics scalar 區塊之後、diagnostics artifacts 上傳之前,加:
```python
                if quadrant_profiles:
                    n_cells = sum(len(v) for v in quadrant_profiles.values())
                    mlflow.log_metric("n_quadrant_cells", n_cells)
```
（`quadrant_profiles` 亦作 DAG 排序依賴,確保 `per_quadrant.json` 已由 catalog 寫入後才 `log_artifacts(diag_dir)`。）

- [ ] **Step 4: 跑測試(接線 + 全 training-dir 回歸)**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/ -q
```
Expected: PASS。若既有 pipeline 結構測試(如 `TestPipeline` / `TestResumeContracts` / catalog regression)斷言確切節點集合或 catalog 鍵集合,依「新增 2 節點 + quadrant_profiles catalog + log_experiment 多一輸入」更新其預期,**不弱化其他斷言**;log_experiment 既有測試因新參數有 default None 應仍綠(若以 positional 傳滿參數則補上)。報告任何更新的測試。

- [ ] **Step 5: Commit**
```bash
git add conf/base/catalog.yaml conf/base/parameters_training.yaml src/recsys_tfb/pipelines/training/pipeline.py src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_shap_cases.py
git commit -m "feat(diagnostics): wire quadrant selection+profiles into training pipeline (per_quadrant.json)

Claude-Session: https://claude.ai/code/session_01XP8TtjbbbrDGmNjKB2AV29"
```

---

## Real-run 驗證(controller 執行,非 subagent;spec §8)

實作 + 全 training-dir 測試綠後,由 controller 本機實跑:
1. pre-flight:`cd worktree; readlink .venv; .venv/bin/python -V; PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py --check-isolation`(首次 `scripts/local_spark_setup.py` 建 synthetic data)。
2. `export SPARK_CONF_DIR=$PWD/conf/spark-local`;背景跑 training pipeline(缺料自動補上游 dataset→train→predict→象限)。
3. 打開 `data/models/<model_version>/diagnostics/per_quadrant.json`,把真實內容(四象限覆蓋、signed features、low_coverage 分佈)貼給使用者。
4. 不合理則回頭修再驗——以實跑結果為準。

---

## Self-Review(plan 對 spec)

- Spec §3.1 選樣節點→Task 1;§3.2 profile→Task 2;§3.3 接線(catalog JSON + pipeline + log_experiment 排序/scalar)→Task 3;§4 config→Task 3(b);§7 測試→Task 1/2/3;§8 real-run→末段。
- 型別/簽名:`select_shap_population(training_eval_predictions, test_model_input, parameters)`、`compute_quadrant_profiles(model, shap_population, preprocessor, parameters)` 定義與 pipeline 接線一致。
- 無 UDF(Window/row_number/crc32/when 皆 native)。`diagnostics/` 子套件維持純 python(Spark 節點在 `diagnostics_spark.py`)。
- 風險:pipeline 結構/catalog 既有測試可能需更新預期(Task 3 Step 4 已註明);`Node` 屬性名以 `core/node.py` 為準(Task 3 Step 1 已註明先看)。

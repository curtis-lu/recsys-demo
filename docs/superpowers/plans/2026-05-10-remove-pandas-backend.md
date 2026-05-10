# Remove pandas Pipeline Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 移除所有 pipeline 層 `backend="pandas"` 程式碼，固定為 `backend="spark"`，並清理對應的 dispatch、tests、scripts 與設定。

**Architecture:** 把 `pipelines/__init__.get_pipeline` 與 5 個 `pipeline.py::create_pipeline` 的 `backend` 參數整個拿掉；直接 `from .nodes_spark import …`。`evaluation/nodes_spark.generate_report` 目前委派到 `nodes_pandas.generate_report`，先把該函式整段搬到 `nodes_spark.py`，再刪 6 個 `nodes_pandas.py` / `helpers_pandas.py` / `preprocessing/_pandas.py`。`__main__.py` 與 `core/logging.RunContext.backend` 一併移除。`ParquetDataset` 雙後端與 `evaluation/*.py` 報表模組（pandas-only）保留。

**Tech Stack:** Python 3.10+ / PySpark 3.3.2 / pytest 7.3.1 / Typer 0.20.1 / Ploomber 0.23.3 / dev-cluster (Spark Standalone + HDFS + Hive Metastore on Docker).

---

## File Structure 變更

### 刪除（6 檔，~1,167 LOC）
- `src/recsys_tfb/pipelines/baselines/nodes_pandas.py`（99 LOC）
- `src/recsys_tfb/pipelines/dataset/nodes_pandas.py`（184）
- `src/recsys_tfb/pipelines/dataset/helpers_pandas.py`（97）
- `src/recsys_tfb/pipelines/inference/nodes_pandas.py`（214）
- `src/recsys_tfb/pipelines/evaluation/nodes_pandas.py`（310）— ⚠️ `generate_report` 先搬遷
- `src/recsys_tfb/preprocessing/_pandas.py`（263）

### 修改（dispatch & 預設值）
- `src/recsys_tfb/pipelines/__init__.py`：`get_pipeline` 拿掉 `backend` 參數
- `src/recsys_tfb/pipelines/{dataset,training,inference,evaluation,baselines}/pipeline.py`：`create_pipeline` 拿掉 `backend` 參數，直接 `from .nodes_spark import …`
- `src/recsys_tfb/__main__.py`：刪除 `backend = params.get("backend", ...)`、所有 `RunContext(..., backend=...)`、所有 `runtime_params["backend"]` 與 dict 中 `"backend": backend` 條目、`get_pipeline` 呼叫的 `backend=` 參數
- `src/recsys_tfb/core/logging.py:29`：`RunContext` 移除 `backend` 欄位
- `src/recsys_tfb/preprocessing/__init__.py`：docstring 把 `_pandas` 段拿掉
- `src/recsys_tfb/pipelines/dataset/_hashing.py`：刪除 `pandas_bucket`，更新模組 docstring
- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:381`：`generate_report` 不再 import `nodes_pandas`，改成獨立函式（pandas DataFrame → HTML 純報表邏輯）
- `conf/base/parameters.yaml`：移除 `backend: spark`（不再讀取）
- `scripts/suggest_categorical_cols.py`：移除 `--backend pandas` 分支與 `pandas` typer option

### 測試（刪除 / 改寫）
- 刪除：
  - `tests/test_pipelines/test_baselines/test_nodes.py`
  - `tests/test_pipelines/test_evaluation/test_nodes.py`
  - `tests/test_pipelines/test_evaluation/test_spark_cross_validation.py`
  - `tests/test_pipelines/test_dataset/test_nodes.py`
  - `tests/test_pipelines/test_inference/test_nodes.py`
  - `tests/test_pipelines/test_dataset/test_hashing.py` 中的 `test_pandas_bucket_*`、`test_pandas_and_spark_bucket_match`
  - `tests/test_pipelines/test_dataset/test_nodes_spark.py::TestSplitTrainKeys::test_cross_backend_consistency`
- 改寫：
  - `tests/test_pipelines/test_inference/test_validation.py`：改用 `spark` fixture + `spark.createDataFrame`，imports 改 `from recsys_tfb.pipelines.inference.nodes_spark import validate_predictions`
- 修正 fixture：
  - `tests/test_core/test_catalog.py:13`：`"backend": "pandas"` 改 `"backend": "spark"`
- 保留：`tests/test_io/test_parquet_dataset.py`（user 要求 ParquetDataset 雙後端保留）

### 保留（與本次無關）
- `src/recsys_tfb/io/parquet_dataset.py`
- `src/recsys_tfb/evaluation/*.py`（calibration、compare、distributions、baselines、segments、report、statistics、metrics）
- `src/recsys_tfb/pipelines/dataset/nodes_shared.py`
- `src/recsys_tfb/io/handles.py`、`io/extract.py`

---

## Pre-Flight

每次跑 `pytest` 前先確認 venv 有起：
```bash
ls -d .venv && source ~/dev-cluster/scripts/client-env.sh
```
（CLAUDE.md 規範：執行前確認虛擬環境；conftest.py 起 SparkSession 需要 `~/dev-cluster` env）

CLI 格式：`python -m recsys_tfb <pipeline> [--options]`（無 `run` 子指令、無 `--pipeline` flag）。

---

## Task 1: 把 evaluation `generate_report` 從 nodes_pandas 搬進 nodes_spark

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` (lines 369-388)
- Reference: `src/recsys_tfb/pipelines/evaluation/nodes_pandas.py` (lines 1-34, 132-310)

- [ ] **Step 1.1: 確認 nodes_spark 對 nodes_pandas 的引用點**

```bash
grep -n "nodes_pandas\|generate_report" src/recsys_tfb/pipelines/evaluation/nodes_spark.py
```
Expected: 只有 line 381 的 `from recsys_tfb.pipelines.evaluation.nodes_pandas import generate_report as generate_report_pandas` + line 386 委派呼叫。

- [ ] **Step 1.2: 把 nodes_pandas 頂部所需的 import 與 `generate_report` 函式整段搬到 nodes_spark.py 末尾**

把 `nodes_pandas.py` lines 4-33 的 imports 整理進 `nodes_spark.py` 頂部（避免重複）；lines 132-310 的 `generate_report` 函式整段複製到 `nodes_spark.py`，覆蓋掉舊的 stub 版本。新版本要刪除 `from recsys_tfb.pipelines.evaluation.nodes_pandas import …`。

`nodes_spark.py` 新的 `generate_report` 函式輪廓：

```python
def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
) -> str:
    """Generate HTML report from Spark evaluation results.

    Collects the eval_predictions to pandas (post-aggregation, manageable size)
    and runs the pandas-based report rendering inline.
    """
    eval_pd = eval_predictions.toPandas()
    return _render_html_report(eval_pd, evaluation_metrics, parameters, baseline_metrics)


def _render_html_report(
    eval_predictions: pd.DataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
) -> str:
    # ... 原本 nodes_pandas.generate_report 的 body ...
```

**做法**：直接在 `nodes_spark.py` 引入需要的 imports（`pd`、`datetime`、`Path`、`Optional`、`get_schema`、`plot_calibration_curves`、`build_comparison_result`、`plot_comparison_metrics`、`plot_*` 系列、`compute_all_metrics` 不需要、`ReportSection`、`generate_html_report`、`compute_segment_metrics`、`build_segment_metrics_table`、`load_and_join_segment_sources`、`compute_product_statistics`、`compute_segment_statistics`），並把 `generate_report` body 直接寫入 `_render_html_report`，由 `generate_report` 呼叫。

- [ ] **Step 1.3: 跑 evaluation pipeline 構造測試**

```bash
.venv/bin/pytest tests/test_pipelines/test_evaluation/test_pipeline.py -v
```
Expected: PASS（pipeline 仍以 spark backend 構造成功，`generate_report` 從 nodes_spark 取得）。

- [ ] **Step 1.4: 跑 spark 跨驗證測試（暫時還在）以確認新搬的程式可被 pandas-style 呼叫等價**

```bash
.venv/bin/pytest tests/test_pipelines/test_evaluation/test_spark_cross_validation.py -v
```
Expected: PASS（後續 Task 4 才刪這檔）。

- [ ] **Step 1.5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py
git commit -m "refactor(evaluation): inline generate_report into nodes_spark

Preparation for removing nodes_pandas. The HTML report rendering operates
on a pandas DataFrame (post-toPandas collection), so it is hoisted into
nodes_spark.py as a private _render_html_report helper called by the
public generate_report node."
```

---

## Task 2: pipeline 工廠移除 backend 分支（5 個 pipeline.py + pipelines/__init__.py）

**Files:**
- Modify: `src/recsys_tfb/pipelines/__init__.py`
- Modify: `src/recsys_tfb/pipelines/dataset/pipeline.py`
- Modify: `src/recsys_tfb/pipelines/inference/pipeline.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `src/recsys_tfb/pipelines/baselines/pipeline.py`
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`

- [ ] **Step 2.1: pipelines/__init__.py 拿掉 backend 參數**

把 `get_pipeline` 改成：

```python
from recsys_tfb.core.pipeline import Pipeline

_REGISTRY: dict[str, str] = {
    "dataset": "recsys_tfb.pipelines.dataset",
    "training": "recsys_tfb.pipelines.training",
    "inference": "recsys_tfb.pipelines.inference",
    "evaluation": "recsys_tfb.pipelines.evaluation",
    "baselines": "recsys_tfb.pipelines.baselines",
}


def get_pipeline(name: str, **kwargs) -> Pipeline:
    """Look up a pipeline by name and return it via the module's create_pipeline()."""
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Pipeline '{name}' not found. Available pipelines: {available}"
        )
    import importlib

    module = importlib.import_module(_REGISTRY[name])
    return module.create_pipeline(**kwargs)


def list_pipelines() -> list[str]:
    """Return all registered pipeline names."""
    return sorted(_REGISTRY.keys())
```

- [ ] **Step 2.2: dataset/pipeline.py 拿掉 backend 分支**

把 `create_pipeline` 簽章和 imports 改成：

```python
def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    from recsys_tfb.pipelines.dataset.nodes_spark import (
        apply_preprocessor_to_features,
        build_model_input,
        fit_preprocessor_metadata,
        select_calibration_keys,
        select_test_keys,
        select_train_keys,
        select_val_keys,
        split_train_keys,
    )

    nodes = [
        # ... 原本的 nodes 完全照舊 ...
    ]
    # ... 後續邏輯照舊 ...
```

- [ ] **Step 2.3: inference/pipeline.py 拿掉 backend 分支**

```python
def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.inference.nodes_spark import (
        apply_preprocessor,
        build_scoring_dataset,
        predict_scores,
        rank_predictions,
        validate_predictions,
    )

    return Pipeline(
        [
            # ... 原本的 nodes 完全照舊 ...
        ]
    )
```

- [ ] **Step 2.4: evaluation/pipeline.py 拿掉 backend 分支**

```python
def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )

    return Pipeline(
        [
            # ... 原本的 nodes 完全照舊 ...
        ]
    )
```

- [ ] **Step 2.5: baselines/pipeline.py 拿掉 backend 分支**

```python
def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.baselines.nodes_spark import (
        compute_baseline_metrics,
        compute_baselines,
    )

    return Pipeline(
        [
            # ... 原本的 nodes 完全照舊 ...
        ]
    )
```

- [ ] **Step 2.6: training/pipeline.py 拿掉 backend 參數**

把第 19 行：
```python
def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
```
改成：
```python
def create_pipeline(enable_calibration: bool = False) -> Pipeline:
```
（training pipeline 內部沒用 `backend` 變數，body 不必動。）

- [ ] **Step 2.7: 跑單元測試確認 pipeline factories 仍可被建構**

```bash
.venv/bin/pytest tests/test_pipelines/test_baselines/test_pipeline.py tests/test_pipelines/test_dataset/test_pipeline.py tests/test_pipelines/test_evaluation/test_pipeline.py tests/test_pipelines/test_inference/test_pipeline.py tests/test_pipelines/test_training/test_pipeline.py -v 2>&1 | tail -40
```
Expected: PASS。若 `test_pipeline.py` 內有對 `create_pipeline(backend=...)` 的測試呼叫，下一 step 修。

- [ ] **Step 2.8: 修被影響的 pipeline test 呼叫**

`grep -n "create_pipeline(backend" tests/`，把所有 `create_pipeline(backend="spark")` 改成 `create_pipeline()`，`create_pipeline(backend="pandas")` 直接刪該 case 或同樣改 `create_pipeline()`。

- [ ] **Step 2.9: Commit**

```bash
git add src/recsys_tfb/pipelines/__init__.py src/recsys_tfb/pipelines/*/pipeline.py tests/test_pipelines/
git commit -m "refactor(pipelines): drop backend dispatch, hardcode spark imports

create_pipeline() and get_pipeline() no longer take a backend argument.
All five pipeline factories import directly from nodes_spark."
```

---

## Task 3: 移除 __main__.py 與 RunContext 的 backend

**Files:**
- Modify: `src/recsys_tfb/__main__.py`
- Modify: `src/recsys_tfb/core/logging.py`

- [ ] **Step 3.1: core/logging.py 移除 RunContext.backend**

刪 `src/recsys_tfb/core/logging.py:29` 那行 `backend: str = ""`。檢查同檔內 `JsonFormatter` / `ConsoleFormatter` 沒有印 `ctx.backend`（grep 確認）：

```bash
grep -n "backend" src/recsys_tfb/core/logging.py
```
Expected: 改完後完全無輸出。

- [ ] **Step 3.2: __main__.py 移除所有 backend 讀取**

打開 `src/recsys_tfb/__main__.py`，逐處修改：

1. `_load_config_and_setup` (約 line 74-89)：

```python
def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, RunContext]:
    conf_dir = _find_conf_dir()
    config = ConfigLoader(str(conf_dir), env=env)
    params = config.get_parameters()

    run_context = RunContext(pipeline=pipeline, env=env)
    setup_logging(params, run_context)

    try:
        validate_schema_config(params)
    except ValueError as exc:
        logger.error("Schema config validation failed: %s", exc)
        raise typer.Exit(code=1)

    return config, params, run_context
```
（回傳值由 4-tuple 變 3-tuple）

2. `_execute_pipeline` (約 line 100-101)：

```python
try:
    pipe = get_pipeline(pipeline_name, **pipeline_kwargs)
except KeyError:
    ...
```
（不再傳 `backend=`，且 `runtime_params` 不再含 `"backend"` key — 在 callers 裡也要去除。）

3. 把 5 個指令 (`dataset`, `training`, `inference`, `evaluation`, `baselines`) 的 callsite 改成解 3-tuple：

```python
config, params, run_context = _load_config_and_setup("dataset", env)
```
（line 182, 285, 408, 518, 591, 651 — 注意 `_run_etl` 也有一處）

4. 各指令 dict 內 `"backend": backend` 條目刪除（line 329, 448, 553, 622, 671）。

5. ETL 函式 `_run_etl` 也需把 `config, params, backend, run_context = _load_config_and_setup(...)` 改成 3-tuple，並移除後續對 `backend` 的引用。

完成後 grep 確認：
```bash
grep -n "backend" src/recsys_tfb/__main__.py
```
Expected: 完全無輸出。

- [ ] **Step 3.3: 跑 CLI smoke**

```bash
.venv/bin/python -m recsys_tfb --help
.venv/bin/python -m recsys_tfb dataset --help
```
Expected: 兩個指令都印出 help、無 ImportError、無 `--backend` option。

- [ ] **Step 3.4: 跑 CLI 測試**

```bash
.venv/bin/pytest tests/test_cli.py -v 2>&1 | tail -30
```
Expected: PASS（若有 fixture 還在 mock `runtime_params["backend"]`，下一 step 修）。

- [ ] **Step 3.5: 修 test_cli.py 中相關 fixture 與 assert**

`grep -n "backend" tests/test_cli.py`，把任何 mock `params["backend"] = "..."` 或 assert `runtime_params["backend"]` 的測試項刪除或改寫。

- [ ] **Step 3.6: Commit**

```bash
git add src/recsys_tfb/__main__.py src/recsys_tfb/core/logging.py tests/test_cli.py
git commit -m "refactor(cli): drop backend parameter from CLI and RunContext

backend is no longer read from parameters.yaml or threaded through
RunContext; pipelines are spark-only."
```

---

## Task 4: 刪除 pandas-only 測試檔與跨後端一致性測試

**Files:**
- Delete: `tests/test_pipelines/test_baselines/test_nodes.py`
- Delete: `tests/test_pipelines/test_evaluation/test_nodes.py`
- Delete: `tests/test_pipelines/test_evaluation/test_spark_cross_validation.py`
- Delete: `tests/test_pipelines/test_dataset/test_nodes.py`
- Delete: `tests/test_pipelines/test_inference/test_nodes.py`
- Modify: `tests/test_pipelines/test_dataset/test_hashing.py`（移除 pandas_bucket 測試）
- Modify: `tests/test_pipelines/test_dataset/test_nodes_spark.py`（移除 `test_cross_backend_consistency`）
- Modify: `tests/test_core/test_catalog.py`（line 13 backend 改 spark）

- [ ] **Step 4.1: 整檔刪除 5 個 pandas-only 測試檔**

```bash
git rm tests/test_pipelines/test_baselines/test_nodes.py \
       tests/test_pipelines/test_evaluation/test_nodes.py \
       tests/test_pipelines/test_evaluation/test_spark_cross_validation.py \
       tests/test_pipelines/test_dataset/test_nodes.py \
       tests/test_pipelines/test_inference/test_nodes.py
```

- [ ] **Step 4.2: 從 test_hashing.py 移除 pandas_bucket 測試**

打開 `tests/test_pipelines/test_dataset/test_hashing.py`，刪除：
- `from recsys_tfb.pipelines.dataset._hashing import pandas_bucket`（line 7 區塊內）
- `test_pandas_bucket_is_deterministic`（lines 20-26）
- `test_pandas_bucket_seed_changes_output`（lines 28-33）
- `test_pandas_bucket_site_isolates_sampling`（lines 35-40）
- `test_pandas_and_spark_bucket_match`（lines 42-… ）

保留 `test_ratio_to_threshold_round_trip` 與檔內任何純 spark 測試（如有）。如果整檔最後只剩 `test_ratio_to_threshold_round_trip`，仍保留作為基本確定性回歸測試。

- [ ] **Step 4.3: 從 test_nodes_spark.py 移除 test_cross_backend_consistency**

打開 `tests/test_pipelines/test_dataset/test_nodes_spark.py`，刪掉 lines 179-194 的整個 `test_cross_backend_consistency` 方法（含其 docstring 與 `from recsys_tfb.pipelines.dataset.nodes_pandas import …` 區域 import）。

- [ ] **Step 4.4: 修 test_core/test_catalog.py:13 backend 預設**

```python
# 原: "backend": "pandas",
# 改:
"backend": "spark",
```
（ParquetDataset 雙後端保留，但 fixture 對齊正式情境。）

- [ ] **Step 4.5: 跑剩下的測試確認結構乾淨**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/ tests/test_core/test_catalog.py -v 2>&1 | tail -40
```
Expected: PASS（即便此時 nodes_pandas 仍存在；下一 task 才真的刪檔）。

- [ ] **Step 4.6: Commit**

```bash
git add -A tests/
git commit -m "test: remove pandas-backend test modules and cross-backend assertions"
```

---

## Task 5: 改寫 test_validation.py 為 Spark 版

**Files:**
- Rewrite: `tests/test_pipelines/test_inference/test_validation.py`

`validate_predictions` 在 `nodes_spark.py:145` 接 Spark DataFrame，邏輯與 pandas 版等價（用 `.count()` / `.filter()` / `F.col(...)`）。原本的 pandas 測試用 `pd.DataFrame` 驅動，須改成 Spark。

- [ ] **Step 5.1: 用 spark fixture 重寫 test_validation.py**

照 `tests/test_pipelines/test_inference/test_nodes_spark.py` 的模式（使用 `spark` fixture from `tests/conftest.py`）。把每個原 `pd.DataFrame(...)` 構造的測試資料用 `spark.createDataFrame(rows, schema)` 重新組裝；assert 對應地呼叫 `.count()` 等 Spark API。

關鍵替換：
```python
# 原:
from recsys_tfb.pipelines.inference.nodes_pandas import validate_predictions
# 改:
from recsys_tfb.pipelines.inference.nodes_spark import validate_predictions
```

`_make_valid_data` helper 改為回傳兩個 Spark DataFrame；`_rerank` helper 改用 `F.row_number().over(Window.partitionBy(...).orderBy(F.col("score").desc()))`。

新檔結構參考：

```python
"""Tests for inference pipeline validation (Spark backend)."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.pipelines.inference.nodes_spark import validate_predictions
from recsys_tfb.pipelines.inference.validation import ValidationError


@pytest.fixture
def parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "inference": {"products": ["prod_a", "prod_b"]},
    }


def _make_valid_data(spark, n_customers=3):
    rows = [
        ("2024-01-31", f"C{i:04d}", prod, 0.5 + 0.1 * j, j + 1)
        for i in range(n_customers)
        for j, prod in enumerate(["prod_a", "prod_b"])
    ]
    ranked = spark.createDataFrame(
        rows, ["snap_date", "cust_id", "prod_name", "score", "rank"]
    )
    scoring = ranked.select("snap_date", "cust_id", "prod_name")
    return ranked, scoring


class TestValidatePredicationsPass:
    def test_valid_data_passes(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        result = validate_predictions(ranked, scoring, parameters)
        assert result is not None

# ... 其它 TestRowCountMatch / TestScoreRange / TestNoMissing /
#     TestCompleteness / TestRankConsistency / TestNoDuplicates /
#     TestMultipleFailures 對照 nodes_spark.py 行為一一改寫 ...
```

實作時對照 `src/recsys_tfb/pipelines/inference/nodes_spark.py:145-` 中的每個 check 行為：`row_count_match`、`score_range`、`no_missing`、`completeness`、`rank_consistency`、`no_duplicates`，每個 check 都建構一個會觸發失敗的 Spark DataFrame 並 assert `pytest.raises(ValidationError)`。

- [ ] **Step 5.2: 跑這個檔**

```bash
.venv/bin/pytest tests/test_pipelines/test_inference/test_validation.py -v 2>&1 | tail -40
```
Expected: PASS。

- [ ] **Step 5.3: Commit**

```bash
git add tests/test_pipelines/test_inference/test_validation.py
git commit -m "test(inference): port validate_predictions tests to Spark backend"
```

---

## Task 6: 刪除 6 個 pandas-backend 模組

**Files:**
- Delete: `src/recsys_tfb/pipelines/baselines/nodes_pandas.py`
- Delete: `src/recsys_tfb/pipelines/dataset/nodes_pandas.py`
- Delete: `src/recsys_tfb/pipelines/dataset/helpers_pandas.py`
- Delete: `src/recsys_tfb/pipelines/inference/nodes_pandas.py`
- Delete: `src/recsys_tfb/pipelines/evaluation/nodes_pandas.py`
- Delete: `src/recsys_tfb/preprocessing/_pandas.py`
- Modify: `src/recsys_tfb/preprocessing/__init__.py`（docstring 更新）

- [ ] **Step 6.1: 確認沒有任何剩餘 import**

```bash
grep -rn "from recsys_tfb.pipelines.dataset.helpers_pandas\|from recsys_tfb.pipelines.dataset.nodes_pandas\|from recsys_tfb.pipelines.inference.nodes_pandas\|from recsys_tfb.pipelines.evaluation.nodes_pandas\|from recsys_tfb.pipelines.baselines.nodes_pandas\|from recsys_tfb.preprocessing._pandas" src/ tests/ scripts/ 2>/dev/null
```
Expected: 完全無輸出（若有，倒回對應 task 修）。

- [ ] **Step 6.2: 刪除模組**

```bash
git rm src/recsys_tfb/pipelines/baselines/nodes_pandas.py \
       src/recsys_tfb/pipelines/dataset/nodes_pandas.py \
       src/recsys_tfb/pipelines/dataset/helpers_pandas.py \
       src/recsys_tfb/pipelines/inference/nodes_pandas.py \
       src/recsys_tfb/pipelines/evaluation/nodes_pandas.py \
       src/recsys_tfb/preprocessing/_pandas.py
```

- [ ] **Step 6.3: 更新 preprocessing/__init__.py docstring**

把目前的 docstring 改成：

```python
"""Preprocessing module: fit/transform/apply logic for the Spark pipeline.

- ``._spark``   — Spark backend  (imports pyspark at module level, safe
                  because only ``nodes_spark`` files import it)
- ``._common``  — backend-agnostic helpers
"""
```

- [ ] **Step 6.4: 跑全測試**

```bash
.venv/bin/pytest -q 2>&1 | tail -30
```
Expected: PASS（若失敗，多半是仍有殘留 import；用前一步 grep 找出來）。

- [ ] **Step 6.5: Commit**

```bash
git add -A src/recsys_tfb/
git commit -m "refactor: delete pandas-backend pipeline modules

Removes ~1,167 LOC across baselines/dataset/inference/evaluation pandas
nodes plus preprocessing._pandas. All pipelines now run spark-only."
```

---

## Task 7: 移除 pandas_bucket 與更新 _hashing 文檔

**Files:**
- Modify: `src/recsys_tfb/pipelines/dataset/_hashing.py`

- [ ] **Step 7.1: 確認 pandas_bucket 已無調用方**

```bash
grep -rn "pandas_bucket" src/ tests/ scripts/
```
Expected: 完全無輸出（前面 task 已清掉）。

- [ ] **Step 7.2: 刪 pandas_bucket 函式並改 docstring**

打開 `src/recsys_tfb/pipelines/dataset/_hashing.py`，把整檔改成：

```python
"""Deterministic CRC32-based hashing utilities for sampling.

PySpark's F.crc32 uses the IEEE 802.3 polynomial. All dataset sampling
routines route through this helper so splits are reproducible across
reruns and partition layouts.

Datetime/date columns are normalized to ``yyyy-MM-dd HH:mm:ss`` before
concatenation to ensure deterministic byte-level input.
"""
from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

HASH_BUCKETS = 100_000
_DATETIME_FMT_SPARK = "yyyy-MM-dd HH:mm:ss"


def ratio_to_threshold(ratio: float) -> int:
    """Convert a [0, 1] sampling ratio into an integer bucket threshold."""
    return int(round(ratio * HASH_BUCKETS))


def _join_token(seed: int, site: str) -> str:
    return f"{site}|{seed}"


def spark_bucket(
    df: DataFrame, cols: Iterable[str], seed: int, site: str,
) -> Column:
    """Build a Spark Column of bucket indices in [0, HASH_BUCKETS).

    Datetime/date columns are formatted as ``yyyy-MM-dd HH:mm:ss`` so the
    string-level concatenation is deterministic across runs.
    """
    schema = df.schema
    parts: list[Column] = []
    for c in cols:
        dtype = schema[c].dataType
        if isinstance(dtype, (DateType, TimestampType)):
            parts.append(F.date_format(F.col(c), _DATETIME_FMT_SPARK))
        else:
            parts.append(F.col(c).cast("string"))
    parts.append(F.lit(_join_token(seed, site)))
    return F.crc32(F.concat_ws("|", *parts)) % F.lit(HASH_BUCKETS)
```

（移除 `import zlib`、`import numpy`、`import pandas`、`_DATETIME_FMT_PANDAS`、`pandas_bucket`。）

- [ ] **Step 7.3: 跑 hashing 測試**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/test_hashing.py -v
```
Expected: PASS。

- [ ] **Step 7.4: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/_hashing.py
git commit -m "refactor(dataset): drop pandas_bucket from hashing utils"
```

---

## Task 8: scripts/suggest_categorical_cols.py 拿掉 pandas 分支

**Files:**
- Modify: `scripts/suggest_categorical_cols.py`

- [ ] **Step 8.1: 看現有結構**

```bash
.venv/bin/python -c "import ast, pathlib; print(ast.dump(ast.parse(pathlib.Path('scripts/suggest_categorical_cols.py').read_text()), indent=2))" | head -60
```
（或直接 Read 檔，重點是 typer command 與 `if backend == 'pandas': ... elif backend == 'spark': ...` dispatch）。

- [ ] **Step 8.2: 改寫 typer command**

把 `--backend` option 整個拿掉（或保留為內部 const 但不暴露）；移除 `if backend == "pandas":` 分支與相關 `suggest_categorical_columns_pandas` import；只保留 spark 路徑與相應錯誤訊息。Docstring 範例改成：

```python
"""Suggest categorical columns for a Hive table or HDFS parquet path.

Usage:
    python scripts/suggest_categorical_cols.py edw.cust_profile
    python scripts/suggest_categorical_cols.py /user/hive/.../customer
"""
```

- [ ] **Step 8.3: 跑 script 測試**

```bash
.venv/bin/pytest tests/scripts/test_suggest_categorical_cols.py -v 2>&1 | tail -30
```
Expected: 跟 spark 端的測試 PASS；任何 `test_pandas_*` / `test_unknown_backend_exits_with_error` 對 pandas 期待的 case 須一併刪除或改寫。

- [ ] **Step 8.4: 修對應的測試**

`tests/scripts/test_suggest_categorical_cols.py` 中：
- 刪 `suggest_categorical_columns_pandas` import 與所有 `test_pandas_*`、`test_pandas_categorical_dtype`、`test_pandas_end_to_end_writes_output_file`
- 刪或改 `test_unknown_backend_exits_with_error`（CLI 已無 `--backend`）
- 保留 spark-only tests

- [ ] **Step 8.5: 跑全測試**

```bash
.venv/bin/pytest -q 2>&1 | tail -20
```
Expected: PASS。

- [ ] **Step 8.6: Commit**

```bash
git add scripts/suggest_categorical_cols.py tests/scripts/test_suggest_categorical_cols.py
git commit -m "refactor(scripts): drop pandas branch from suggest_categorical_cols"
```

---

## Task 9: parameters.yaml 移除 backend 設定

**Files:**
- Modify: `conf/base/parameters.yaml`

- [ ] **Step 9.1: 確認 src/ 已無讀取 params['backend']**

```bash
grep -rn "params.get(\"backend\"\|params\[\"backend\"\]\|parameters.get(\"backend\"\|parameters\[\"backend\"\]" src/
```
Expected: 完全無輸出。

- [ ] **Step 9.2: 刪除 line 3 的 `backend: spark`**

直接編輯 `conf/base/parameters.yaml`，刪除：

```yaml
backend: spark
```
這一行（連同前後若有的空行做適度整理）。

- [ ] **Step 9.3: 跑全測試與 CLI smoke**

```bash
.venv/bin/pytest -q 2>&1 | tail -10
.venv/bin/python -m recsys_tfb dataset --help
```
Expected: 全 PASS、CLI help 正常。

- [ ] **Step 9.4: Commit**

```bash
git add conf/base/parameters.yaml
git commit -m "chore(conf): drop backend key from parameters.yaml (no longer read)"
```

---

## Task 10: 端到端煙囪測試（dev-cluster）

**Files:**
- 無修改，僅執行驗證。

- [ ] **Step 10.1: 起 dev-cluster Hive 來源表**

```bash
cd ~/projects/recsys_tfb
scripts/dev_admin.sh scripts/setup_hive_dev.py
```
Expected: 印出 `ml_recsys.feature_table` / `label_table` / `sample_pool` 已建立，無 ERROR。

- [ ] **Step 10.2: 跑 dataset pipeline（distributed cluster conf）**

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb dataset --env production
```
Expected: pipeline 完成、寫出 `ml_recsys.train_model_input` / `val_model_input` / `test_model_input` 等 Hive table；log 中無「ImportError」「nodes_pandas」字樣。

- [ ] **Step 10.3: 跑 training pipeline（local conf）**

```bash
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```
Expected: 訓練完成、產出 model artifact（`model.txt` + 對應 manifest）。

- [ ] **Step 10.4: 跑 inference & evaluation & baselines（distributed conf）**

```bash
unset SPARK_CONF_DIR  # 回到 client-template
source ~/dev-cluster/scripts/client-env.sh
# 注意：inference / evaluation 預設讀 best/ 符號連結，需先人工 promote（CLAUDE.md 規範：promote 不可自行操作）
# 此 step 假設已人工 /scripts/promote_model.py 完成；若無則僅跑 baselines 即可：
.venv/bin/python -m recsys_tfb baselines --env production
```
Expected: `ml_recsys.baseline_predictions` / `baseline_metrics` 寫入；無錯。

- [ ] **Step 10.5: 靜態 grep 確認無遺漏**

```bash
grep -rn "nodes_pandas\|helpers_pandas\|preprocessing._pandas\|pandas_bucket" src/ tests/ scripts/ conf/ 2>/dev/null
```
Expected: 完全無輸出。

```bash
grep -rn "backend.*pandas\|backend=\"pandas\"\|backend='pandas'" src/ tests/ scripts/ conf/ 2>/dev/null
```
Expected: 僅剩 `src/recsys_tfb/io/parquet_dataset.py`、`tests/test_io/test_parquet_dataset.py`（ParquetDataset 雙後端保留）。

- [ ] **Step 10.6: 重建 graphify**

```bash
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: 印出更新數量、無 ERROR。

- [ ] **Step 10.7: Commit graphify-out 變化（若有）**

```bash
git status graphify-out/
git add graphify-out/
git commit -m "chore: rebuild graphify after pandas backend removal" 2>/dev/null || echo "no graphify changes to commit"
```

---

## 完成條件

- 所有 task PASS、所有測試綠
- `grep -r nodes_pandas|helpers_pandas|preprocessing\._pandas|pandas_bucket src/ tests/ scripts/ conf/` 完全無輸出
- CLI `python -m recsys_tfb --help` 與所有子指令 `--help` 都不再出現 backend 字眼
- dev-cluster 上 dataset → training → baselines 全段跑通
- graphify 已重建

---

## Self-Review 結果

**Spec coverage：** Task 1-3 對 generate_report 搬遷 / dispatch 移除 / CLI 整理；Task 4-5 處理 tests；Task 6 刪 pandas 模組；Task 7 處理 _hashing；Task 8 處理 scripts；Task 9 處理 conf；Task 10 端到端驗證。對應 user `AskUserQuestion` 三項決定（保留 ParquetDataset 雙後端、移除 suggest_categorical pandas、整段刪除 cross-validation 測試）皆已涵蓋。

**Placeholder scan：** 無「TBD/TODO/implement later/Add appropriate error handling/Similar to Task N」字樣；所有需修改的檔/行已標明，所有需新寫的程式碼都有完整片段。

**Type consistency：** `RunContext` 移除 `backend` 後 `_load_config_and_setup` 的回傳由 4-tuple 變 3-tuple，所有 caller 都列出（Task 3.2）。`get_pipeline(name, **kwargs)` / `create_pipeline()` 簽章一致；test 端對應修改在 Task 2.8。

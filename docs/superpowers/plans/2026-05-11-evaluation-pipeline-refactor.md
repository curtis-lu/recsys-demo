# Evaluation Pipeline Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把訓練 test-set 預測持久化到 Hive，並讓 evaluation pipeline 透過 `--post-training` flag 切換預測來源（訓練後評估 vs 每月監控）。

**Architecture:** Phase 1 把 training pipeline 的 `evaluate_model` 拆成三個 node（evaluate_model 純 predict+rank、`compute_test_mAP` 算指標、`write_test_predictions` 依 prod_name 批次寫 Hive），新增 `training_eval_predictions` Hive 表。Phase 2 在 `evaluation/pipeline.py::create_pipeline` 加 `post_training` flag 切換 input dataset name，於 `prepare_eval_data` 加 `model_version` partition filter，CLI 新增 `--post-training`。

**Tech Stack:** PySpark 3.3.2, pytest 7.3.1, Typer 0.20.1, Hive metastore (managed table). Pipeline DAG framework：`recsys_tfb.core.{node,pipeline}`. CLI entry：`src/recsys_tfb/__main__.py`.

**Spec:** `docs/superpowers/specs/2026-05-11-evaluation-pipeline-refactor-design.md`

---

## File Structure

**Phase 1（training 寫回）**：

| File | Action | Responsibility |
|---|---|---|
| `~/dev-cluster/client-template-local/spark/conf/hive-site.xml` | Create (symlink) | 讓 training pipeline 看到 Hive metastore |
| `conf/base/catalog.yaml` | Modify | 新增 `training_eval_predictions` entry |
| `src/recsys_tfb/pipelines/training/nodes.py` | Modify | 拆 `evaluate_model`，新增 `write_test_predictions`、`compute_test_mAP` |
| `src/recsys_tfb/pipelines/training/pipeline.py` | Modify | DAG 重新接線 |
| `tests/test_pipelines/test_training/test_nodes.py` | Modify | `TestEvaluateModel` 改為驗證新 tuple 回傳；新增 `TestComputeTestMAP`、`TestWriteTestPredictions` |
| `tests/test_pipelines/test_training/test_pipeline.py` | Modify | node 數從 9 → 11；新增 wiring 檢查 |

**Phase 2（evaluation 切換）**：

| File | Action | Responsibility |
|---|---|---|
| `conf/base/catalog.yaml` | Modify | 新增 `ranked_predictions` entry（修正 standalone 讀取） |
| `conf/base/parameters_evaluation.yaml` | Modify | `evaluation.model_version: null` 預設 |
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | Modify | `prepare_eval_data` 內加 model_version filter |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py` | Modify | `create_pipeline(post_training: bool = False)` factory |
| `src/recsys_tfb/__main__.py` | Modify | evaluation 子命令新增 `--post-training` flag |
| `tests/test_pipelines/test_evaluation/test_pipeline.py` | Modify | post_training=True / False 兩種情境 |
| `tests/test_pipelines/test_evaluation/test_nodes_spark.py` | Create | `TestPrepareEvalData::test_filters_by_model_version` |

---

## Implementation Note (Spec Simplification)

Spec §2.2 提出建立新模組 `_model_version.py` 帶 `resolve_model_version`，但**已存在於 `src/recsys_tfb/core/versioning.py:199`**，且 `__main__.py:592` 的 `evaluation()` 子命令**已經呼叫** `resolve_model_version(models_dir, model_version)` 並把結果填入 `runtime_params["model_version"]`。

實作改採：
- **不**新增 `_model_version.py` 模組
- 沿用 `core/versioning.py::resolve_model_version` + 既有 CLI 解析流程
- `prepare_eval_data` 內直接讀 `parameters["model_version"]`（CLI 已預先解析），對 Spark DataFrame 套 filter

即 spec 設計目標（best symlink fallback + 顯式 override）保留，省一個冗餘模組。

---

## Phase 1: Training Write-back

### Task 1.0: 補 `client-template-local` 的 hive-site.xml symlink

**Files:**
- Create symlink: `~/dev-cluster/client-template-local/spark/conf/hive-site.xml`

- [ ] **Step 1: Verify the prerequisite is needed (target missing, source exists)**

Run:
```bash
ls -la ~/dev-cluster/client-template-local/spark/conf/hive-site.xml 2>&1
ls -la ~/dev-cluster/client-template/spark/conf/hive-site.xml 2>&1
```

Expected: first command outputs `No such file or directory`; second shows an existing file.

- [ ] **Step 2: Create symlink**

Run:
```bash
ln -s ~/dev-cluster/client-template/spark/conf/hive-site.xml \
      ~/dev-cluster/client-template-local/spark/conf/hive-site.xml
```

- [ ] **Step 3: Verify training-spec Spark session sees Hive**

Run:
```bash
SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark \
  .venv/bin/python -c "
from recsys_tfb.utils.spark import get_or_create_spark_session
s = get_or_create_spark_session()
s.sql('SHOW DATABASES').show()
"
```

Expected: lists databases including `ml_recsys`. If `ml_recsys` is missing, run `scripts/dev_admin.sh scripts/setup_hive_dev.py` first per CLAUDE.md.

- [ ] **Step 4: Commit (config file change only — no symlink to commit; this is host-local infrastructure)**

No git commit for this task. Note the action in PR description instead.

---

### Task 1.1: 新增 `training_eval_predictions` catalog entry

**Files:**
- Modify: `conf/base/catalog.yaml`

- [ ] **Step 1: Add catalog entry after the existing `validated_predictions` block**

Open `conf/base/catalog.yaml`, find the block ending around line 228 (`validated_predictions` partition_cols block), insert immediately after:

```yaml
# --- Training Pipeline - Test-set predictions for downstream evaluation reuse ---
# Written by training/nodes.py::write_test_predictions per prod_name (bypasses catalog
# auto-save for memory control). Read by evaluation pipeline when --post-training.
training_eval_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: training_eval_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: rank, type: BIGINT}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
    - {name: model_version, type: STRING}
```

- [ ] **Step 2: Verify catalog loads (structural validation)**

Run:
```bash
.venv/bin/python -c "
from recsys_tfb.core.config import ConfigLoader
loader = ConfigLoader('conf')
catalog = loader.get_catalog_by_env('base')
ds_names = sorted(catalog.keys())
print('training_eval_predictions' in ds_names)
"
```

Expected: `True`. If loader API differs, alternative — `python -c "import yaml; print('training_eval_predictions' in yaml.safe_load(open('conf/base/catalog.yaml')))"` → `True`.

- [ ] **Step 3: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "feat(catalog): add training_eval_predictions Hive table entry"
```

---

### Task 1.2: 重構 `evaluate_model` 為純 predict+rank（回傳 pandas tuple）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:538-598`
- Modify: `tests/test_pipelines/test_training/test_nodes.py:371-490` (TestEvaluateModel)

**Reference current implementation:**
- `evaluate_model` (line 538) returns dict via `_compute_ranking_metrics` (line ~490)
- After refactor: returns `(predictions_pdf, labels_pdf)` tuple
- Rank computation (currently inside `_compute_ranking_metrics`) lifts up to `evaluate_model`

- [ ] **Step 1: Write failing test — `evaluate_model` returns (predictions_pdf, labels_pdf)**

Replace the body of `TestEvaluateModel` class in `tests/test_pipelines/test_training/test_nodes.py` with:

```python
class TestEvaluateModel:
    """evaluate_model returns (predictions_pdf, labels_pdf) tuple after refactor."""

    def test_returns_tuple_of_two_dataframes(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        model = trained_model_after_finalize
        result = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        assert isinstance(result, tuple)
        assert len(result) == 2
        predictions_pdf, labels_pdf = result
        import pandas as pd
        assert isinstance(predictions_pdf, pd.DataFrame)
        assert isinstance(labels_pdf, pd.DataFrame)

    def test_predictions_has_required_columns(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        predictions_pdf, _ = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        for col in schema["identity_columns"]:
            assert col in predictions_pdf.columns
        assert schema["score"] in predictions_pdf.columns
        assert schema["rank"] in predictions_pdf.columns

    def test_labels_has_required_columns(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        _, labels_pdf = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        for col in schema["identity_columns"]:
            assert col in labels_pdf.columns
        assert schema["label"] in labels_pdf.columns

    def test_rank_starts_from_one_per_query(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        predictions_pdf, _ = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        group_cols = [schema["time"]] + schema["entity"]
        min_ranks = predictions_pdf.groupby(group_cols)[schema["rank"]].min()
        assert (min_ranks == 1).all()
```

- [ ] **Step 2: Run tests — verify they fail**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestEvaluateModel -v
```

Expected: tests fail because current `evaluate_model` returns a dict.

- [ ] **Step 3: Refactor `evaluate_model` in `src/recsys_tfb/pipelines/training/nodes.py`**

Replace the existing `evaluate_model` function (line 538-598) with:

```python
def evaluate_model(
    model: ModelAdapter,
    eval_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Predict on the test set and rank within each query group.

    Returns:
        (predictions_pdf, labels_pdf):
          predictions_pdf — identity_columns + [score, rank]
          labels_pdf      — identity_columns + [label]

    Downstream nodes consume the predictions/labels separately:
      - write_test_predictions persists predictions to Hive
      - compute_test_mAP computes the dict consumed by MLflow
    """
    from recsys_tfb.io.extract import extract_Xy

    schema_cfg = get_schema(parameters)
    score_col = schema_cfg["score"]
    rank_col = schema_cfg["rank"]
    label_col = schema_cfg["label"]
    identity_cols = schema_cfg["identity_columns"]
    time_col = schema_cfg["time"]
    entity_cols = schema_cfg["entity"]
    group_cols = [time_col] + entity_cols

    eval_pdf = eval_parquet_handle.to_pandas()

    with log_step(logger, "extract_features"):
        X, _ = extract_Xy(eval_parquet_handle, preprocessor_metadata, parameters)

    with log_step(logger, "predict"):
        y_score = model.predict(X)

    predictions_pdf = eval_pdf[identity_cols].reset_index(drop=True).copy()
    predictions_pdf[score_col] = y_score
    predictions_pdf[rank_col] = (
        predictions_pdf.groupby(group_cols)[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    labels_pdf = eval_pdf[identity_cols + [label_col]].reset_index(drop=True).copy()

    logger.info(
        "evaluate_model: predicted %d rows, %d queries",
        len(predictions_pdf),
        predictions_pdf[group_cols].drop_duplicates().shape[0],
    )
    return predictions_pdf, labels_pdf
```

- [ ] **Step 4: Run tests — verify they pass**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestEvaluateModel -v
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "refactor(training): evaluate_model returns (predictions_pdf, labels_pdf) tuple"
```

---

### Task 1.3: 新增 `compute_test_mAP` node

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add new function)
- Modify: `tests/test_pipelines/test_training/test_nodes.py` (add `TestComputeTestMAP`)

`compute_test_mAP` 承接舊 `evaluate_model` 的 dict-returning 角色，供 `log_experiment` / MLflow 消費。

- [ ] **Step 1: Write failing test — `compute_test_mAP` returns dict with required keys**

Append to `tests/test_pipelines/test_training/test_nodes.py`:

```python
# ---- Tests: compute_test_mAP ----

class TestComputeTestMAP:
    """compute_test_mAP computes ranking metrics from (predictions_pdf, labels_pdf)."""

    def test_returns_dict_with_required_keys(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        model = trained_model_after_finalize
        predictions_pdf, labels_pdf = evaluate_model(
            model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert isinstance(result, dict)
        assert "overall_map" in result
        assert "per_product_ap" in result
        assert "n_queries" in result
        assert "n_excluded_queries" in result

    def test_overall_map_in_valid_range(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        model = trained_model_after_finalize
        predictions_pdf, labels_pdf = evaluate_model(
            model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert 0.0 <= result["overall_map"] <= 1.0

    def test_calibrated_model_includes_uncalibrated_subdict(
        self, calibrated_model, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
        if not isinstance(calibrated_model, CalibratedModelAdapter):
            import pytest
            pytest.skip("fixture didn't yield a CalibratedModelAdapter")
        predictions_pdf, labels_pdf = evaluate_model(
            calibrated_model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert "uncalibrated" in result
        assert "overall_map" in result["uncalibrated"]
        assert "per_product_ap" in result["uncalibrated"]
```

If `calibrated_model` fixture doesn't exist, mark that test with `@pytest.mark.skipif` instead of relying on fixture absence — adjust based on existing conftest.py.

- [ ] **Step 2: Run tests — verify they fail (function doesn't exist)**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestComputeTestMAP -v
```

Expected: ImportError or AttributeError — `compute_test_mAP` not defined.

- [ ] **Step 3: Implement `compute_test_mAP` in `src/recsys_tfb/pipelines/training/nodes.py`**

Add after `evaluate_model`:

```python
def compute_test_mAP(
    test_predictions_pdf: pd.DataFrame,
    test_labels_pdf: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking-aware mAP from test-set predictions; feed log_experiment.

    test_predictions_pdf must contain identity_columns + [score, rank].
    test_labels_pdf must contain identity_columns + [label].
    """
    schema_cfg = get_schema(parameters)
    score_col = schema_cfg["score"]
    label_col = schema_cfg["label"]
    item_col = schema_cfg["item"]
    identity_cols = schema_cfg["identity_columns"]

    merged = test_predictions_pdf.merge(test_labels_pdf, on=identity_cols, how="inner")
    predictions_for_metrics = merged[identity_cols + [score_col]].copy()
    labels_for_metrics = merged[identity_cols + [label_col]].copy()

    metrics = compute_all_metrics(
        predictions_for_metrics, labels_for_metrics, k_values=["all"]
    )

    n_products = predictions_for_metrics[item_col].nunique()
    map_key = f"map@{n_products}"

    overall_map = metrics["overall"].get(map_key, 0.0)
    per_product_ap = {
        prod: vals.get(map_key, 0.0)
        for prod, vals in metrics["per_product"].items()
    }

    evaluation_results = {
        "overall_map": overall_map,
        "per_product_ap": per_product_ap,
        "n_queries": metrics["n_queries"],
        "n_excluded_queries": metrics["n_excluded_queries"],
    }

    logger.info(
        "compute_test_mAP: mAP=%.4f, products=%d, excluded_queries=%d",
        overall_map,
        len(per_product_ap),
        metrics["n_excluded_queries"],
    )
    return evaluation_results
```

Note: the calibrated-model `uncalibrated` sub-dict logic from old `evaluate_model` is now MISSING in this function because we no longer have direct access to the model here. **Resolution:** Add a separate path — `compute_test_mAP` accepts an optional third argument for uncalibrated scores, or accept that uncalibrated comparison moves out of training pipeline (rely on evaluation pipeline running both versions).

For this plan: **scope decision** — drop the uncalibrated comparison from `compute_test_mAP`. Evaluation pipeline already handles calibration analysis separately (it's a richer report). Mark this as a small behavior delta in the PR description. Update the third test in step 1 to instead assert that **no** `uncalibrated` key is present:

Replace `test_calibrated_model_includes_uncalibrated_subdict` with:

```python
def test_calibrated_model_no_uncalibrated_subdict(
    self, calibrated_model, val_h, preprocessor_metadata, training_parameters
):
    """After refactor, uncalibrated comparison moves to evaluation pipeline."""
    from recsys_tfb.pipelines.training.nodes import compute_test_mAP
    predictions_pdf, labels_pdf = evaluate_model(
        calibrated_model, val_h, preprocessor_metadata, training_parameters
    )
    result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
    assert "uncalibrated" not in result
```

- [ ] **Step 4: Run tests — verify they pass**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestComputeTestMAP -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): add compute_test_mAP node — extracts metric calc from evaluate_model"
```

---

### Task 1.4: 新增 `write_test_predictions` node（依 prod_name 批次寫 Hive）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add `write_test_predictions`)
- Modify: `tests/test_pipelines/test_training/test_nodes.py` (add `TestWriteTestPredictions`)

- [ ] **Step 1: Write failing test — `write_test_predictions` calls insertInto once per distinct prod_name**

Append to `tests/test_pipelines/test_training/test_nodes.py`:

```python
# ---- Tests: write_test_predictions ----

class TestWriteTestPredictions:
    """write_test_predictions iterates per prod_name and writes to Hive."""

    @pytest.fixture
    def predictions_pdf(self):
        import pandas as pd
        return pd.DataFrame({
            "cust_id": ["c1", "c2", "c1", "c2"],
            "snap_date": ["2025-12-31"] * 4,
            "prod_name": ["fund_stock", "fund_stock", "ccard_ins", "ccard_ins"],
            "score": [0.9, 0.7, 0.6, 0.4],
            "rank": [1, 2, 1, 2],
        })

    @pytest.fixture
    def parameters_with_model_version(self):
        return {
            "schema": {
                "time_col": "snap_date",
                "entity_cols": ["cust_id"],
                "item_col": "prod_name",
                "label_col": "label",
                "score_col": "score",
                "rank_col": "rank",
            },
            "hive": {"db": "ml_recsys"},
            "model_version": "20260511_153000",
        }

    def test_calls_insertInto_once_per_prod_name(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        mock_write = MagicMock()
        mock_spark.createDataFrame.return_value.withColumn.return_value.write = mock_write

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        # 2 distinct prod_names → 2 insertInto calls
        assert mock_write.insertInto.call_count == 2

    def test_each_chunk_filtered_to_one_prod(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        captured_chunks = []

        def capture_create_df(pdf):
            captured_chunks.append(pdf.copy())
            return MagicMock()
        mock_spark.createDataFrame.side_effect = capture_create_df

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        assert len(captured_chunks) == 2
        for chunk in captured_chunks:
            assert chunk["prod_name"].nunique() == 1

    def test_ensures_table_via_create_if_not_exists(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        mock_spark.createDataFrame.return_value.withColumn.return_value.write = MagicMock()

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        ddl_calls = [
            call_args
            for call_args in mock_spark.sql.call_args_list
            if "CREATE TABLE IF NOT EXISTS" in str(call_args)
        ]
        assert len(ddl_calls) == 1, f"expected 1 CREATE TABLE call, got {len(ddl_calls)}"
        assert "training_eval_predictions" in str(ddl_calls[0])
```

- [ ] **Step 2: Run tests — verify they fail (function doesn't exist)**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestWriteTestPredictions -v
```

Expected: ImportError — `write_test_predictions` not defined.

- [ ] **Step 3: Implement `write_test_predictions` in `src/recsys_tfb/pipelines/training/nodes.py`**

Add after `compute_test_mAP`:

```python
def _build_training_eval_predictions_ddl(table_fqn: str) -> str:
    """CREATE TABLE IF NOT EXISTS DDL — schema matches catalog declaration."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
        cust_id STRING,
        score DOUBLE,
        `rank` BIGINT
    )
    PARTITIONED BY (snap_date STRING, prod_name STRING, model_version STRING)
    STORED AS PARQUET
    """.strip()


def write_test_predictions(
    test_predictions_pdf: pd.DataFrame,
    parameters: dict,
) -> None:
    """Write test-set predictions to Hive, iterating per prod_name for memory control.

    Bypasses catalog auto-save: outputs=None at DAG level. The catalog entry
    `training_eval_predictions` declares the table for downstream evaluation reads;
    this function owns the writes (DDL bootstrap + per-prod insertInto).
    """
    from recsys_tfb.utils.spark import get_or_create_spark_session

    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]
    spark = get_or_create_spark_session()
    model_version = parameters["model_version"]
    hive_db = parameters["hive"]["db"]
    table_fqn = f"{hive_db}.training_eval_predictions"

    spark.sql(_build_training_eval_predictions_ddl(table_fqn))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    distinct_prods = sorted(test_predictions_pdf[item_col].unique())
    logger.info(
        "write_test_predictions: %d prods × ~%d rows = %d total → %s",
        len(distinct_prods),
        len(test_predictions_pdf) // max(len(distinct_prods), 1),
        len(test_predictions_pdf),
        table_fqn,
    )

    for prod in distinct_prods:
        chunk_pdf = test_predictions_pdf[test_predictions_pdf[item_col] == prod]
        chunk_sdf = (
            spark.createDataFrame(chunk_pdf)
                 .withColumn("model_version", F.lit(model_version))
        )
        chunk_sdf.write.insertInto(table_fqn, overwrite=True)
        logger.info("write_test_predictions: wrote prod=%s rows=%d", prod, len(chunk_pdf))
```

Ensure `F` is imported (`from pyspark.sql import functions as F` — check existing imports at top of file).

- [ ] **Step 4: Run tests — verify they pass**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestWriteTestPredictions -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): write_test_predictions persists test set to Hive per prod_name"
```

---

### Task 1.5: 訓練 pipeline DAG 重新接線

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py:105-119`
- Modify: `tests/test_pipelines/test_training/test_pipeline.py`

- [ ] **Step 1: Update failing test — pipeline now has 11 nodes (was 9)**

Edit `tests/test_pipelines/test_training/test_pipeline.py`:

Replace:
```python
def test_pipeline_has_nine_nodes(self):
    pipeline = create_pipeline()
    assert len(pipeline.nodes) == 9
```

With:
```python
def test_pipeline_has_eleven_nodes(self):
    pipeline = create_pipeline()
    assert len(pipeline.nodes) == 11

def test_pipeline_has_write_test_predictions_node(self):
    pipeline = create_pipeline()
    names = [n.name for n in pipeline.nodes]
    assert "write_test_predictions" in names
    assert "compute_test_mAP" in names
```

For the calibration variant — update similarly: was 11 nodes, becomes 13.

```python
def test_calibration_pipeline_has_thirteen_nodes(self):
    pipeline = create_pipeline(enable_calibration=True)
    assert len(pipeline.nodes) == 13
```

- [ ] **Step 2: Run tests — verify they fail**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_pipeline.py -v
```

Expected: failures on node count assertions; the new node-name assertion also fails.

- [ ] **Step 3: Update `src/recsys_tfb/pipelines/training/pipeline.py`**

In `pipeline.py`, locate the `nodes.extend([...])` block (around line 105-119) and replace with:

```python
    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "test_parquet_handle", "preprocessor", "parameters"],
            outputs=["test_predictions_pdf", "test_labels_pdf"],
        ),
        Node(
            write_test_predictions,
            inputs=["test_predictions_pdf", "parameters"],
            outputs=None,
        ),
        Node(
            compute_test_mAP,
            inputs=["test_predictions_pdf", "test_labels_pdf", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=[
                "model", "best_params", "best_iteration",
                "evaluation_results", "parameters",
            ],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
```

Also update the imports at top of `pipeline.py`:

```python
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_test_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    compute_test_mAP,
    evaluate_model,
    finalize_model,
    log_experiment,
    prepare_lgb_train_inputs,
    tune_hyperparameters,
    write_test_predictions,
)
```

- [ ] **Step 4: Run tests — verify they pass**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_training/test_pipeline.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/pipeline.py tests/test_pipelines/test_training/test_pipeline.py
git commit -m "feat(training): wire write_test_predictions + compute_test_mAP into DAG"
```

---

### Task 1.6: 端到端煙霧測試（dev-cluster）

**Files:** None — exercise pipeline on dev-cluster

- [ ] **Step 1: Confirm dev-cluster is up**

Run:
```bash
docker ps --filter "name=spark-master" --format "{{.Names}} {{.Status}}"
```

Expected: spark-master container running. If not, see `~/dev-cluster/README.md`.

- [ ] **Step 2: Run training pipeline end-to-end**

Run:
```bash
source ~/dev-cluster/scripts/client-env.sh
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

Expected: completes without errors; logs show `write_test_predictions: wrote prod=<name> rows=<N>` for each product.

- [ ] **Step 3: Verify Hive partitions exist**

Run:
```bash
scripts/dev_admin.sh -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').enableHiveSupport().getOrCreate()
spark.sql('SHOW PARTITIONS ml_recsys.training_eval_predictions').show(50, False)
"
```

Expected: partitions like `snap_date=YYYY-MM-DD/prod_name=fund_stock/model_version=<version>` for each (snap_date, prod_name) in test set.

- [ ] **Step 4: Verify row counts match test set**

Run:
```bash
scripts/dev_admin.sh -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').enableHiveSupport().getOrCreate()
n_pred = spark.table('ml_recsys.training_eval_predictions').count()
print(f'training_eval_predictions row count: {n_pred}')
"
```

Expected: row count > 0 and matches `len(test_predictions_pdf)` from training log.

- [ ] **Step 5: Commit nothing (smoke test only)**

If smoke test fails, fix and re-test before proceeding to Phase 2. Do NOT push Phase 1 PR until smoke passes.

---

## Phase 2: Evaluation Pipeline Refactor

### Task 2.1: 新增 `ranked_predictions` catalog entry

**Files:**
- Modify: `conf/base/catalog.yaml`

**Purpose:** 修正 evaluation pipeline 無法 standalone 跑的現況問題（catalog 沒有 `ranked_predictions` entry，導致 auto-MemoryDataset fallback）。

- [ ] **Step 1: Add catalog entry after `validated_predictions` block**

Open `conf/base/catalog.yaml`, find the `validated_predictions` block, insert immediately after:

```yaml
# --- Inference Pipeline (cont.) - Evaluation read-side entry ---
# Same Hive table as validated_predictions; declared separately so evaluation
# pipeline can run standalone (without inference in same session).
# core/catalog.py:71 auto-fallbacks unknown names to MemoryDataset, which
# silently breaks standalone evaluation reads — this entry fixes that.
ranked_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: ranked_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: rank, type: BIGINT}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
    - {name: model_version, type: STRING}
```

- [ ] **Step 2: Verify catalog parses**

Run:
```bash
.venv/bin/python -c "
import yaml
with open('conf/base/catalog.yaml') as f:
    d = yaml.safe_load(f)
assert 'ranked_predictions' in d
assert d['ranked_predictions']['table'] == 'ranked_predictions'
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "fix(catalog): add ranked_predictions entry for standalone evaluation reads"
```

---

### Task 2.2: `prepare_eval_data` 加 `model_version` filter

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:38-88` (prepare_eval_data)
- Create: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

- [ ] **Step 1: Write failing test — `prepare_eval_data` filters to parameters["model_version"]**

Create `tests/test_pipelines/test_evaluation/test_nodes_spark.py`:

```python
"""Tests for evaluation pipeline Spark nodes."""

from unittest.mock import MagicMock

import pandas as pd
import pytest


class TestPrepareEvalDataModelVersionFilter:
    """prepare_eval_data filters predictions to parameters['model_version']."""

    @pytest.fixture
    def parameters(self):
        return {
            "schema": {
                "time_col": "snap_date",
                "entity_cols": ["cust_id"],
                "item_col": "prod_name",
                "label_col": "label",
                "score_col": "score",
                "rank_col": "rank",
            },
            "evaluation": {},
            "model_version": "20260511_153000",
        }

    def test_filter_applied_with_model_version(self, parameters):
        from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data

        predictions = MagicMock(name="predictions_sdf")
        filtered = MagicMock(name="filtered_sdf")
        predictions.filter.return_value = filtered

        labels = MagicMock(name="label_sdf")
        labels.sparkSession = MagicMock()
        # Chain on labels.join(...) returning something join-able
        filtered.join.return_value = MagicMock(name="eval_predictions")
        filtered.select.return_value.distinct.return_value = MagicMock()

        try:
            prepare_eval_data(predictions, labels, parameters)
        except Exception:
            pass  # we only care that .filter was called

        # Verify .filter() was called once on predictions
        assert predictions.filter.call_count == 1
        filter_arg = predictions.filter.call_args[0][0]
        # Spark Column repr will include the literal model_version value
        assert "20260511_153000" in str(filter_arg)
```

- [ ] **Step 2: Run test — verify it fails**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py -v
```

Expected: fails — `prepare_eval_data` doesn't currently call `.filter()` for model_version.

- [ ] **Step 3: Update `prepare_eval_data` in `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`**

Locate `prepare_eval_data` (line 38) and insert after the segment_sources block (around line 79), BEFORE the `pred_snap_dates` block:

```python
    # Filter predictions to the resolved model_version (already resolved upstream
    # by __main__.py via core.versioning.resolve_model_version).
    model_version = parameters.get("model_version")
    if model_version is None:
        raise RuntimeError(
            "parameters['model_version'] missing. CLI should resolve via "
            "core.versioning.resolve_model_version before pipeline run."
        )
    logger.info("Filtering predictions to model_version=%s", model_version)
    ranked_predictions = ranked_predictions.filter(F.col("model_version") == model_version)
```

(The local var name inside `prepare_eval_data` stays `ranked_predictions` — same parameter name regardless of whether the underlying catalog entry is `ranked_predictions` or `training_eval_predictions`; the node doesn't care.)

- [ ] **Step 4: Run test — verify it passes**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_nodes_spark.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(evaluation): prepare_eval_data filters predictions by model_version"
```

---

### Task 2.3: `evaluation/pipeline.py` 加 `post_training` flag

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `tests/test_pipelines/test_evaluation/test_pipeline.py`

- [ ] **Step 1: Update tests — pipeline factory takes post_training flag**

Replace the entire `tests/test_pipelines/test_evaluation/test_pipeline.py` content with:

```python
"""Tests for evaluation pipeline definition."""

from recsys_tfb.pipelines.evaluation import create_pipeline


class TestEvaluationPipelineDefault:
    """Default (post_training=False) — monitoring scenario."""

    def test_pipeline_has_three_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 3

    def test_pipeline_reads_ranked_predictions(self):
        pipeline = create_pipeline()
        assert "ranked_predictions" in pipeline.inputs
        assert "training_eval_predictions" not in pipeline.inputs

    def test_pipeline_outputs_unchanged(self):
        pipeline = create_pipeline()
        expected = {"eval_predictions", "evaluation_metrics", "evaluation_report"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == ["prepare_eval_data", "compute_metrics", "generate_report"]


class TestEvaluationPipelinePostTraining:
    """post_training=True — read from training_eval_predictions."""

    def test_pipeline_has_three_nodes(self):
        pipeline = create_pipeline(post_training=True)
        assert len(pipeline.nodes) == 3

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_same_as_default(self):
        pipeline = create_pipeline(post_training=True)
        expected = {"eval_predictions", "evaluation_metrics", "evaluation_report"}
        assert pipeline.outputs == expected
```

- [ ] **Step 2: Run tests — verify they fail**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_pipeline.py -v
```

Expected: `TestEvaluationPipelinePostTraining` fails — `create_pipeline()` doesn't accept `post_training`.

- [ ] **Step 3: Update `src/recsys_tfb/pipelines/evaluation/pipeline.py`**

Replace the file content with:

```python
"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(post_training: bool = False) -> Pipeline:
    """Build the evaluation pipeline.

    Args:
        post_training: When True, read predictions from `training_eval_predictions`
            (post-training evaluation). When False (default), read from
            `ranked_predictions` (monthly monitoring). Pattern matches
            training/pipeline.py::create_pipeline(enable_calibration=...).
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )

    return Pipeline(
        [
            Node(
                prepare_eval_data,
                inputs=[predictions_input, "label_table", "parameters"],
                outputs="eval_predictions",
            ),
            Node(
                compute_metrics,
                inputs=["eval_predictions", "parameters"],
                outputs="evaluation_metrics",
            ),
            Node(
                generate_report,
                inputs=[
                    "eval_predictions",
                    "evaluation_metrics",
                    "parameters",
                    "baseline_metrics",
                ],
                outputs="evaluation_report",
            ),
        ]
    )
```

- [ ] **Step 4: Run tests — verify they pass**

Run:
```bash
.venv/bin/python -m pytest tests/test_pipelines/test_evaluation/test_pipeline.py -v
```

Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/pipeline.py tests/test_pipelines/test_evaluation/test_pipeline.py
git commit -m "feat(evaluation): create_pipeline(post_training) flag switches predictions source"
```

---

### Task 2.4: CLI `--post-training` flag

**Files:**
- Modify: `src/recsys_tfb/__main__.py:580-636` (evaluation subcommand)
- Modify: `tests/test_cli.py` (or create test if no slot exists)

- [ ] **Step 1: Locate existing test slot for evaluation CLI**

Run:
```bash
grep -n "def test_.*evaluation\|evaluation()" /Users/curtislu/projects/recsys_tfb/tests/test_cli.py 2>&1 | head -10
```

Note what's there. If no existing evaluation CLI test, the test added below will be the first.

- [ ] **Step 2: Write failing test — typer flag is recognized**

Append to `tests/test_cli.py`:

```python
class TestEvaluationCLIFlags:
    """evaluation subcommand exposes --post-training flag."""

    def test_post_training_flag_in_help(self):
        from typer.testing import CliRunner
        from recsys_tfb.__main__ import app

        runner = CliRunner()
        result = runner.invoke(app, ["evaluation", "--help"])
        assert result.exit_code == 0
        assert "--post-training" in result.output
```

- [ ] **Step 3: Run test — verify it fails**

Run:
```bash
.venv/bin/python -m pytest tests/test_cli.py::TestEvaluationCLIFlags -v
```

Expected: fail — flag not yet declared.

- [ ] **Step 4: Update `evaluation` subcommand in `src/recsys_tfb/__main__.py`**

Locate the `evaluation` function (line 580) and modify the signature + add `pipeline_kwargs`:

```python
@app.command()
def evaluation(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(
        None, "--model-version", help="Model version to use"
    ),
    post_training: bool = typer.Option(
        False, "--post-training",
        help="Read predictions from training_eval_predictions (default: ranked_predictions for monitoring)",
    ),
):
    """Run the evaluation pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("evaluation", env)
    get_or_create_spark_session(_load_spark_config(config, "evaluation"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}

    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    logger.info(
        "Evaluation — model_version: %s (%s), post_training: %s",
        mv, model_version if model_version else "best", post_training,
    )
    logger.info("Evaluation — snap_date: %s", snap_date)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
    }

    pipeline_kwargs = {"post_training": post_training}
    _execute_pipeline(
        "evaluation", pipeline_kwargs, runtime_params, config, params, env
    )

    # Post run — manifest write unchanged
    version_dir = data_dir / "evaluation" / mv / snap_date
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": mv,
            "pipeline": "evaluation",
            "parameters": params_eval,
            "model_version": mv,
        },
        run_id=run_context.run_id,
        extra_metadata={"snap_date": snap_date, "post_training": post_training},
        symlink_target=data_dir / "evaluation" / "latest",
    )
    logger.info("Pipeline 'evaluation' completed successfully")
```

- [ ] **Step 5: Run test — verify it passes**

Run:
```bash
.venv/bin/python -m pytest tests/test_cli.py::TestEvaluationCLIFlags -v
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): evaluation --post-training flag selects training_eval_predictions"
```

---

### Task 2.5: parameters_evaluation.yaml 預設

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`

`evaluation.model_version` 不需新增 — CLI 已經透過 `--model-version` typer flag 處理（resolve via core/versioning.py）。spec §2.4 提到的「parameters_evaluation.yaml 補 `model_version: null` 預設」實際上 redundant — CLI 端已涵蓋。

跳過此 task，無變動。

如果未來想讓 `--env` 切換時走 yaml 級別配置（例如 dev env 預設指定某 model_version），再開另一個 spec 處理。

- [ ] **Step 1: 文件化此決策**

無檔案變動。記在 PR 描述：「Spec §2.5 的 yaml default 改為依賴 CLI 解析路徑」。

---

### Task 2.6: 端到端煙霧測試（兩種情境）

**Files:** None — exercise pipelines on dev-cluster

**Pre-condition:** Phase 1 已完成（training pipeline 已寫過至少一次，`training_eval_predictions` 有資料）。

- [ ] **Step 1: Run post-training evaluation**

Run:
```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb evaluation --env production --post-training
```

Expected: completes; HTML report written to `data/evaluation/<version>/<snap_date>/`.

- [ ] **Step 2: Verify report content shows post-training predictions**

Run:
```bash
ls -la data/evaluation/$(readlink data/models/best)/*/evaluation_report.html 2>&1 | head -5
```

Expected: at least one HTML file exists. Optionally open in browser and verify the metrics section is populated (not empty queries).

- [ ] **Step 3: Run monitoring evaluation (default behavior — read inference predictions)**

**Pre-condition:** inference pipeline has been run at least once, populating `ml_recsys.ranked_predictions`.

Run:
```bash
.venv/bin/python -m recsys_tfb evaluation --env production
```

Expected: completes; HTML report written for monitoring scenario.

- [ ] **Step 4: Confirm both reports use the same `model_version` partition filter**

Run:
```bash
grep -i "model_version" data/evaluation/*/*/evaluation_report.html 2>&1 | head -5
```

Expected: model_version visible in report metadata (if generate_report includes it; else verify via training log).

- [ ] **Step 5: Smoke test commit (none)**

No commit. If smoke fails, fix and re-test before PR.

---

## Final Steps

### Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (Pipeline 與 SPARK_CONF_DIR 對應表)

- [ ] **Step 1: Update the corresponding table to reflect training now writes Hive**

Locate the table in `CLAUDE.md` (line 32-37) and adjust the training row:

```markdown
| `training` | **`~/dev-cluster/client-template-local/spark`**（需 symlink hive-site.xml） | `local[*]` | LightGBM 是 driver 單機訓練；cache 從 HDFS 拉；**test 預測寫回 Hive `ml_recsys.training_eval_predictions`，需 hive-site.xml symlink** |
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): training now writes Hive — note hive-site.xml symlink prereq"
```

---

### Run full test suite

- [ ] **Step 1: Verify everything still passes**

Run:
```bash
.venv/bin/python -m pytest tests/ -q
```

Expected: all tests pass. If any unrelated tests fail, investigate before opening PR.

- [ ] **Step 2: Squash-merge readiness**

The commits within each Phase tell a coherent story; PR review should be by phase boundaries. Consider opening Phase 1 and Phase 2 as separate PRs if review bandwidth is tight.

---

## Self-Review Notes

**Spec coverage:**
- §1.1–1.4 (Phase 1 node split + write) → Tasks 1.2–1.5 ✓
- §1.5 (hive-site.xml prereq) → Task 1.0 ✓
- §1.6 (MLflow unchanged) → Verified by `log_experiment` consuming `evaluation_results` unchanged ✓
- §2.1 (factory + flag) → Task 2.3 ✓
- §2.2 (model_version resolution) → Task 2.2 + note about reusing existing core/versioning.py ✓
- §2.3 (CLI flag) → Task 2.4 ✓
- §2.4 (catalog dual entry) → Task 2.1 ✓
- §2.5 (yaml defaults) → Task 2.5 (no-op, rationale documented)
- §2.6 (usage examples) → Verified in Task 2.6 smoke ✓
- Phase 3 → out of scope, Task 2.6 confirms monitoring still works ✓

**Behavior delta from spec:**
- Spec §1.6 implied `compute_test_mAP` keeps the uncalibrated comparison sub-dict from old `evaluate_model`. Task 1.3 documents the **drop** (model handle no longer reaches the metric node post-refactor); evaluation pipeline owns richer calibration analysis. Surface this in PR description.

**Placeholder scan:** None found.

**Type consistency:**
- `(predictions_pdf, labels_pdf)` tuple shape consistent across Tasks 1.2/1.3/1.4 ✓
- `parameters["model_version"]` consumed identically in Tasks 1.4/2.2 ✓
- `post_training` flag wired through Task 2.3 (factory) and Task 2.4 (CLI) without type drift ✓

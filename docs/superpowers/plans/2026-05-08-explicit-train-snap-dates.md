# Explicit `train_snap_dates` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `dataset` pipeline 的訓練窗從 `train_snap_date_start/end` range 改為顯式 `train_snap_dates: [...]` list，並在 `fit_preprocessor_metadata` / `apply_preprocessor_to_features` 加上以該 list 為基礎的 snap_date filter，讓 dataset 對未來上游週中+月底並存的 feature_table robust。

**Architecture:** 新增 `collect_dataset_snap_dates` helper 集中回答「dataset 用哪些 snap_date」；fit 用 `train_snap_dates`、apply 用 train ∪ cal ∪ val ∪ test 的 union；缺值嚴格 raise。完全替代 range 表示法（不保留向下相容）。本 plan 只處理 spark backend；pandas backend 規劃移除中，不在範圍內。

**Tech Stack:** Python 3.10+, PySpark 3.3.2, pandas 1.5.3, pytest 7.3.1

**Spec:** `docs/superpowers/specs/2026-05-08-explicit-train-snap-dates-design.md`

---

## File Structure

**Production code (modify):**
- `conf/base/parameters_dataset.yaml` — replace `train_snap_date_start/end` with `train_snap_dates` list
- `src/recsys_tfb/pipelines/dataset/nodes_shared.py` — add `collect_dataset_snap_dates`; simplify `validate_date_splits`
- `src/recsys_tfb/pipelines/dataset/nodes_spark.py` — `select_train_keys` 改 list filter
- `src/recsys_tfb/preprocessing/_spark.py` — `fit_preprocessor_metadata` + `apply_preprocessor_to_features` 改 list filter + 缺值 raise

**Test code:**
- `tests/test_pipelines/test_dataset/test_nodes_shared.py` — **CREATE** (新檔，測 `collect_dataset_snap_dates` + 重構後的 `validate_date_splits`)
- `tests/test_pipelines/test_dataset/test_nodes_spark.py` — fixture 替換 + 新增 filter 行為 / 缺值 raise 測試
- `tests/test_pipelines/test_dataset/test_nodes.py` — 移除 `TestDateValidation` 整個 class（已搬到 `test_nodes_shared.py`，pandas 檔保留純 pandas 行為測試）
- `tests/test_core/test_versioning.py` — `_base_params()` 改新 key；`test_train_snap_date_start_affects_base` 改名 + 改 key
- `tests/scenarios/test_scenario_1_new_inference.py` — fixture 改 list
- `tests/scenarios/test_scenario_2_shift_window.py` — fixture 改 list
- `tests/scenarios/test_scenario_3_new_features.py` — fixture 改 list
- `tests/scenarios/test_scenario_4_new_products.py` — fixture 改 list

**Docs:**
- `CLAUDE.md` — 「12 個月月底快照」改為 list-based 描述

**不動（per spec Non-Goals）:**
- `src/recsys_tfb/pipelines/dataset/nodes_pandas.py`、`src/recsys_tfb/preprocessing/_pandas.py`、`tests/test_pipelines/test_dataset/test_nodes.py` 內非 `TestDateValidation` 的部分、`tests/test_pipelines/test_training/test_pipeline.py:TestTrainingPipelineE2E`（pandas E2E）

---

## Task 1: Add `collect_dataset_snap_dates` helper

**Files:**
- Create: `tests/test_pipelines/test_dataset/test_nodes_shared.py`
- Modify: `src/recsys_tfb/pipelines/dataset/nodes_shared.py`

- [ ] **Step 1.1: Create new test file with failing tests for the helper**

Create `tests/test_pipelines/test_dataset/test_nodes_shared.py`:

```python
"""Tests for backend-agnostic dataset pipeline helpers (nodes_shared)."""

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_shared import (
    collect_dataset_snap_dates,
    validate_date_splits,
)


class TestCollectDatasetSnapDates:
    def test_returns_sorted_union(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-03-31", "2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-06-30"],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert result == [
            pd.Timestamp("2025-01-31"),
            pd.Timestamp("2025-02-28"),
            pd.Timestamp("2025-03-31"),
            pd.Timestamp("2025-04-30"),
            pd.Timestamp("2025-05-31"),
            pd.Timestamp("2025-06-30"),
        ]

    def test_deduplicates_overlapping_entries(self):
        # 不同 split 不應重複；helper 不負責 overlap 檢查（那是 validate_date_splits）
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-02-28"],  # dup with train
                "val_snap_dates": [],
                "test_snap_dates": [],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert result == [pd.Timestamp("2025-01-31"), pd.Timestamp("2025-02-28")]

    def test_returns_pd_timestamp_objects(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": [],
                "test_snap_dates": [],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert all(isinstance(d, pd.Timestamp) for d in result)

    def test_missing_train_snap_dates_raises(self):
        params = {
            "dataset": {
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-06-30"],
            }
        }
        with pytest.raises(KeyError, match="train_snap_dates"):
            collect_dataset_snap_dates(params)

    def test_optional_splits_default_to_empty(self):
        # cal/val/test 缺鍵時用 .get(..., [])，不應 raise
        params = {"dataset": {"train_snap_dates": ["2025-01-31"]}}
        result = collect_dataset_snap_dates(params)
        assert result == [pd.Timestamp("2025-01-31")]
```

- [ ] **Step 1.2: Run tests to confirm they fail**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_shared.py::TestCollectDatasetSnapDates -v`
Expected: FAIL — `ImportError: cannot import name 'collect_dataset_snap_dates' from 'recsys_tfb.pipelines.dataset.nodes_shared'`

- [ ] **Step 1.3: Add helper to `nodes_shared.py`**

Edit `src/recsys_tfb/pipelines/dataset/nodes_shared.py` — add the new helper after the existing `validate_date_splits`. Open the file and append:

```python
def collect_dataset_snap_dates(parameters: dict) -> list[pd.Timestamp]:
    """Return sorted union of train/cal/val/test snap_dates as pd.Timestamps.

    Single source of truth for "which snap_dates does the dataset pipeline use".
    Used by apply_preprocessor_to_features (all splits) — fit_preprocessor_metadata
    deliberately uses only train_snap_dates to prevent val/test leakage into the
    category-mapping fit.
    """
    ds = parameters["dataset"]
    dates: set[pd.Timestamp] = set()
    dates.update(pd.Timestamp(d) for d in ds["train_snap_dates"])
    dates.update(pd.Timestamp(d) for d in ds.get("calibration_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("val_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("test_snap_dates", []))
    return sorted(dates)
```

- [ ] **Step 1.4: Run tests to confirm they pass**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_shared.py::TestCollectDatasetSnapDates -v`
Expected: PASS (5 tests)

- [ ] **Step 1.5: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/nodes_shared.py tests/test_pipelines/test_dataset/test_nodes_shared.py
git commit -m "feat(dataset): add collect_dataset_snap_dates helper"
```

---

## Task 2: Simplify `validate_date_splits` to pure-list form

**Files:**
- Modify: `tests/test_pipelines/test_dataset/test_nodes_shared.py`
- Modify: `src/recsys_tfb/pipelines/dataset/nodes_shared.py`
- Modify: `tests/test_pipelines/test_dataset/test_nodes.py` (remove obsolete `TestDateValidation`)

- [ ] **Step 2.1: Append validate_date_splits tests to `test_nodes_shared.py`**

Add to `tests/test_pipelines/test_dataset/test_nodes_shared.py` (after `TestCollectDatasetSnapDates`):

```python
class TestValidateDateSplits:
    def test_non_overlapping_passes(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-03-31"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        validate_date_splits(params)  # should not raise

    def test_train_cal_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-02-28"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="train & calibration"):
            validate_date_splits(params)

    def test_train_val_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-04-30"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="train & val"):
            validate_date_splits(params)

    def test_cal_val_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="calibration & val"):
            validate_date_splits(params)

    def test_val_test_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="val & test"):
            validate_date_splits(params)

    def test_three_way_overlap_reports_all_pairs(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-04-30"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError) as exc_info:
            validate_date_splits(params)
        msg = str(exc_info.value)
        assert "train & calibration" in msg
        assert "train & val" in msg
        assert "calibration & val" in msg

    def test_empty_calibration_passes(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        validate_date_splits(params)  # should not raise

    def test_missing_optional_keys_pass(self):
        # cal/val/test 完全沒提供時也應通過（用 .get(..., [])）
        params = {"dataset": {"train_snap_dates": ["2025-01-31"]}}
        validate_date_splits(params)  # should not raise
```

- [ ] **Step 2.2: Run tests to confirm they fail (because old impl uses range keys)**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_shared.py::TestValidateDateSplits -v`
Expected: Most tests FAIL or pass for wrong reasons — old implementation uses `train_snap_date_start/end`, doesn't see `train_snap_dates` for overlap check; `test_three_way_overlap_reports_all_pairs` will fail.

- [ ] **Step 2.3: Replace `validate_date_splits` implementation with list-only version**

Edit `src/recsys_tfb/pipelines/dataset/nodes_shared.py` — replace the entire `validate_date_splits` function:

```python
def validate_date_splits(parameters: dict) -> None:
    """Validate that train/calibration/val/test snap_date sets are mutually disjoint."""
    ds = parameters.get("dataset", {})
    sets = {
        "train":       set(str(d) for d in ds.get("train_snap_dates", [])),
        "calibration": set(str(d) for d in ds.get("calibration_snap_dates", [])),
        "val":         set(str(d) for d in ds.get("val_snap_dates", [])),
        "test":        set(str(d) for d in ds.get("test_snap_dates", [])),
    }
    overlaps = []
    names = list(sets.keys())
    for i, a in enumerate(names):
        for b in names[i+1:]:
            common = sets[a] & sets[b]
            if common:
                overlaps.append(f"{a} & {b}: {sorted(common)}")
    if overlaps:
        raise ValueError(f"Date splits overlap: {'; '.join(overlaps)}")
```

Make sure `import pandas as pd` stays at top of file (still needed by `collect_dataset_snap_dates`).

- [ ] **Step 2.4: Run new tests to confirm they pass**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_shared.py::TestValidateDateSplits -v`
Expected: PASS (8 tests)

- [ ] **Step 2.5: Remove obsolete `TestDateValidation` class from `test_nodes.py`**

Edit `tests/test_pipelines/test_dataset/test_nodes.py` — delete the entire `class TestDateValidation:` block (lines ~116-196). Also remove the now-unused import:

```python
# Remove from imports
from recsys_tfb.pipelines.dataset.nodes_shared import validate_date_splits
```

- [ ] **Step 2.6: Run remaining `test_nodes.py` tests to confirm no regressions**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes.py -v`
Expected: PASS for all remaining (non-TestDateValidation) tests; if any fail unrelated to this change, leave for pandas-removal task.

- [ ] **Step 2.7: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/nodes_shared.py tests/test_pipelines/test_dataset/test_nodes_shared.py tests/test_pipelines/test_dataset/test_nodes.py
git commit -m "refactor(dataset): simplify validate_date_splits to pure-list form

Move TestDateValidation tests from pandas test_nodes.py into a new
backend-agnostic test_nodes_shared.py and rewrite implementation to
operate on train/calibration/val/test snap_date sets only (no range
handling). Pandas-specific tests in test_nodes.py untouched."
```

---

## Task 3: Update `parameters_dataset.yaml` to list format

**Files:**
- Modify: `conf/base/parameters_dataset.yaml`

- [ ] **Step 3.1: Replace range with list**

Edit `conf/base/parameters_dataset.yaml` — replace lines 1-4:

```yaml
dataset:
  # --- Train 日期範圍 ---
  train_snap_date_start: "2025-01-31"
  train_snap_date_end: "2025-10-31"
```

with:

```yaml
dataset:
  # --- Train 月底 snap_date list ---
  # 顯式列出（避免上游 cadence 變動時 silently 吞下，符合 ML lineage 慣例）
  # 產生：pd.date_range(start, end, freq="ME").strftime("%Y-%m-%d").tolist()
  train_snap_dates:
    - "2025-01-31"
    - "2025-02-28"
    - "2025-03-31"
    - "2025-04-30"
    - "2025-05-31"
    - "2025-06-30"
    - "2025-07-31"
    - "2025-08-31"
    - "2025-09-30"
    - "2025-10-31"
```

- [ ] **Step 3.2: Verify YAML parses**

Run: `python3 -c "import yaml; print(yaml.safe_load(open('conf/base/parameters_dataset.yaml'))['dataset']['train_snap_dates'])"`
Expected: prints list of 10 date strings

- [ ] **Step 3.3: Commit**

```bash
git add conf/base/parameters_dataset.yaml
git commit -m "chore(conf): switch parameters_dataset to explicit train_snap_dates list"
```

---

## Task 4: Update spark `select_train_keys` to use list

**Files:**
- Modify: `src/recsys_tfb/pipelines/dataset/nodes_spark.py:24-46`
- Modify: `tests/test_pipelines/test_dataset/test_nodes_spark.py:86-109` (parameters fixture) + `:117-128` (test_filters_to_train_dates)

- [ ] **Step 4.1: Update parameters fixture in `test_nodes_spark.py`**

Edit `tests/test_pipelines/test_dataset/test_nodes_spark.py` — replace the `parameters` fixture (lines 86-109):

```python
@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "schema": {
            "categorical_values": {
                "prod_name": ["exchange_fx", "exchange_usd", "fund_stock"],
            },
        },
        "dataset": {
            "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-03-31"],
            "sample_ratio": 0.5,
            "sample_group_keys": ["cust_segment_typ", "prod_name"],
            "sample_ratio_overrides": {},
            "train_dev_ratio": 0.2,
            "enable_calibration": False,
            "calibration_snap_dates": [],
            "calibration_sample_ratio": 1.0,
            "val_snap_dates": ["2024-04-30"],
            "val_sample_ratio": 1.0,
            "test_snap_dates": ["2024-05-31"],
        },
    }
```

- [ ] **Step 4.2: Update `test_filters_to_train_dates` assertions**

Replace the test body (lines 117-128) with:

```python
    def test_filters_to_train_dates(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        pdf = result.toPandas()
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))
        excluded = val_dates | test_dates
        assert not pdf["snap_date"].isin(excluded).any()
        # All dates must be in train_snap_dates
        assert pdf["snap_date"].isin(train_dates).all()
```

- [ ] **Step 4.3: Run tests to confirm they fail (impl still uses range)**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestSelectTrainKeys -v`
Expected: FAIL — `KeyError: 'train_snap_date_start'` raised in `select_train_keys`

- [ ] **Step 4.4: Update `select_train_keys` in `nodes_spark.py`**

Edit `src/recsys_tfb/pipelines/dataset/nodes_spark.py` — replace `select_train_keys` (lines 24-46):

```python
def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys using explicit train_snap_dates list."""
    validate_date_splits(parameters)

    ds = parameters["dataset"]
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(
        sample_pool, parameters, train_dates, ds["sample_ratio"], overrides,
        site="sample_keys",
    )
```

The replacement is a complete function rewrite — `time_col`, `start`, `end`, `pool`, `train_dates_rows` are all gone (the new body computes `train_dates` directly from config). Existing imports at the top of `nodes_spark.py` (including `pd`, `F`, `get_schema`) stay unchanged; `get_schema` and `F` are still used elsewhere in the module.

- [ ] **Step 4.5: Run tests to confirm they pass**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestSelectTrainKeys -v`
Expected: PASS (4 tests)

- [ ] **Step 4.6: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/nodes_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "refactor(dataset): select_train_keys (spark) reads train_snap_dates list"
```

---

## Task 5: Update spark `fit_preprocessor_metadata` (filter + missing-raise)

**Files:**
- Modify: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (add new test class)
- Modify: `src/recsys_tfb/preprocessing/_spark.py:75-155`

- [ ] **Step 5.1: Add failing test for missing-raise behavior**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py`:

```python
class TestFitPreprocessorMissingDates:
    def test_missing_train_snap_date_raises(self, spark, feature_table, parameters):
        # parameters has train_snap_dates including 2024-02-29; feature_table has it.
        # Override to require a date that's not in feature_table.
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-12-31"],
            },
        }
        with pytest.raises(ValueError, match="missing required train_snap_dates"):
            fit_preprocessor_metadata(feature_table, params)

    def test_error_lists_missing_dates(self, spark, feature_table, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-12-31", "2024-11-30"],
            },
        }
        with pytest.raises(ValueError) as exc_info:
            fit_preprocessor_metadata(feature_table, params)
        msg = str(exc_info.value)
        assert "2024-11-30" in msg
        assert "2024-12-31" in msg
```

- [ ] **Step 5.2: Run tests to confirm they fail**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitPreprocessorMissingDates -v`
Expected: FAIL — current impl uses `train_snap_date_start/end` and won't raise (it'll raise KeyError on the missing range key, not the validation message we want)

- [ ] **Step 5.3: Update `fit_preprocessor_metadata` in `preprocessing/_spark.py`**

Edit `src/recsys_tfb/preprocessing/_spark.py` — replace the body of `fit_preprocessor_metadata` (lines 75-155). Keep the docstring and signature; rewrite the body:

```python
def fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata at customer-month granularity, decoupled from sampling.

    Feature-categorical distinct values come from feature_table rows whose
    ``time`` falls in ``train_snap_dates``. Identity categoricals (not present
    in feature_table) come from ``parameters["schema"]["categorical_values"][col]``;
    missing declarations raise ``ValueError``.

    Raises ``ValueError`` if feature_table is missing any required train_snap_date
    (fail-loud principle: dataset must be reproducible from feature_table).

    Only small metadata (distinct category values) is collected to driver.

    Returns:
        (preprocessor_metadata, category_mappings)
    """
    import pandas as pd

    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {})
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Fail-loud if feature_table is missing any required train_snap_date.
    # Cardinality is small (typically 12-52 dates); .distinct().collect() is cheap.
    ft_dates = {
        row[time_col]
        for row in feature_table.select(time_col).distinct().collect()
    }
    ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
    missing = sorted(set(train_dates) - ft_dates)
    if missing:
        raise ValueError(
            "feature_table missing required train_snap_dates: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

    with log_step(logger, "filter_train_window"):
        train_features = feature_table.filter(F.col(time_col).isin(train_dates))

    ft_cols = set(feature_table.columns)
    feature_cat_cols = [c for c in categorical_cols if c in ft_cols]
    identity_cat_cols = [c for c in categorical_cols if c not in ft_cols]

    cat_values = schema.get("categorical_values", {})
    missing_cats = [c for c in identity_cat_cols if c not in cat_values]
    if missing_cats:
        raise ValueError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing_cats}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )

    with log_step(logger, "collect_category_mappings"):
        category_mappings: dict[str, list] = {}
        for col in feature_cat_cols:
            distinct_rows = (
                train_features.select(col)
                .filter(F.col(col).isNotNull())
                .distinct()
                .orderBy(col)
                .collect()
            )
            category_mappings[col] = [row[col] for row in distinct_rows]
        for col in identity_cat_cols:
            category_mappings[col] = list(cat_values[col])

    with log_step(logger, "compute_feature_columns"):
        feature_columns = _compute_feature_columns(
            feature_table.columns,
            identity_cols,
            categorical_cols,
            drop_cols,
            label_col,
        )

    preprocessor_metadata = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Fit preprocessor (Spark): %d features, %d categorical, %d drop",
        len(feature_columns), len(categorical_cols), len(drop_cols),
    )
    return preprocessor_metadata, category_mappings
```

- [ ] **Step 5.4: Run tests to confirm they pass**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitPreprocessorMissingDates tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitAndBuild -v`
Expected: PASS — both new tests pass; existing TestFitAndBuild also passes (fixture has all required dates)

- [ ] **Step 5.5: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "refactor(preprocessing): fit_preprocessor_metadata reads train_snap_dates + raises on missing"
```

---

## Task 6: Update spark `apply_preprocessor_to_features` (filter + missing-raise)

**Files:**
- Modify: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (add new tests)
- Modify: `src/recsys_tfb/preprocessing/_spark.py:158-204`

- [ ] **Step 6.1: Add failing tests for apply filter + missing-raise**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py`:

```python
class TestApplyPreprocessorFilter:
    def test_filters_out_dates_outside_dataset_set(
        self, spark, feature_table, parameters
    ):
        """Test A: feature_table 含週中 row 時，filter 必須排除。"""
        # Add a "mid-week" row that isn't in any split's snap_dates
        midweek_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-15"] * 4),
            "cust_id": ["C001", "C002", "C003", "C004"],
            "total_aum": [999.0] * 4,
            "fund_aum": [99.0] * 4,
            "in_amt_sum_l1m": [9.0] * 4,
            "out_amt_sum_l1m": [9.0] * 4,
            "in_amt_ratio_l1m": [0.99] * 4,
            "out_amt_ratio_l1m": [0.99] * 4,
        })
        ft_with_midweek = feature_table.unionByName(spark.createDataFrame(midweek_pdf))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(ft_with_midweek, preprocessor, parameters)
        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        assert pd.Timestamp("2024-01-15") not in result_dates

    def test_missing_required_snap_date_raises(
        self, spark, feature_table, parameters
    ):
        """Test E2: feature_table 缺 cal/val/test 任一 snap_date 應 raise."""
        # parameters val_snap_dates is 2024-04-30; remove 04-30 rows from feature_table
        ft_short = feature_table.filter(F.col("snap_date") != F.lit(pd.Timestamp("2024-04-30")))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        with pytest.raises(ValueError, match="missing required snap_dates"):
            apply_preprocessor_to_features(ft_short, preprocessor, parameters)


class TestFitApplyFilterScopes:
    def test_apply_includes_all_splits_fit_only_train(
        self, spark, feature_table, parameters
    ):
        """Test B: fit 看 train，apply 看 train ∪ cal ∪ val ∪ test."""
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(feature_table, preprocessor, parameters)

        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))

        # Apply must cover all splits
        assert train_dates.issubset(result_dates)
        assert val_dates.issubset(result_dates)
        assert test_dates.issubset(result_dates)
```

You'll need to import `F` at the top of the test file if not already present:
```python
from pyspark.sql import functions as F
```

- [ ] **Step 6.2: Run tests to confirm they fail**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestApplyPreprocessorFilter tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitApplyFilterScopes -v`
Expected: FAIL — current `apply_preprocessor_to_features` does no filtering; midweek row will appear in output; missing dates won't raise.

- [ ] **Step 6.3: Update `apply_preprocessor_to_features` in `preprocessing/_spark.py`**

Add the import at the top of `_spark.py` (after existing imports):

```python
from recsys_tfb.pipelines.dataset.nodes_shared import collect_dataset_snap_dates
```

Replace the body of `apply_preprocessor_to_features` (lines 158-204). Keep signature/docstring intent; rewrite:

```python
def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table at customer-month granularity.

    Filters feature_table to the union of all dataset snap_dates (train ∪ cal ∪ val ∪ test).
    Raises ``ValueError`` if any required snap_date is missing from feature_table.

    Output: (time + entity) + feature_columns that live in feature_table.
    """
    import pandas as pd

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    base_key = [time_col] + entity_cols
    ft_feature_cols = [c for c in feature_columns if c in feature_table.columns]
    keep_cols = list(dict.fromkeys(base_key + ft_feature_cols))
    missing_base = [c for c in base_key if c not in feature_table.columns]
    if missing_base:
        raise ValueError(f"feature_table missing base-key columns: {missing_base}")
    _warn_missing_drop_columns(feature_table.columns, drop_cols, "feature_table")

    needed_dates = collect_dataset_snap_dates(parameters)

    # Fail-loud if feature_table is missing any required snap_date.
    ft_dates = {
        row[time_col]
        for row in feature_table.select(time_col).distinct().collect()
    }
    ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
    missing = sorted(set(needed_dates) - ft_dates)
    if missing:
        raise ValueError(
            "feature_table missing required snap_dates: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

    with log_step(logger, "select_columns"):
        result = (
            feature_table.filter(F.col(time_col).isin(needed_dates))
            .select(*keep_cols)
        )

    with log_step(logger, "encode_categoricals"):
        encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            for col in encode_cols:
                n_unknown = result.filter(F.col(col) == -1).count()
                if n_unknown > 0:
                    logger.warning(
                        "apply_preprocessor_to_features: %d unknowns in column '%s'",
                        n_unknown, col,
                    )

    logger.info(
        "Preprocessed feature_table (Spark): %d cols (encoded=%d)",
        len(result.columns), len(encode_cols),
    )
    return result
```

- [ ] **Step 6.4: Run tests to confirm they pass**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::TestApplyPreprocessorFilter tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitApplyFilterScopes tests/test_pipelines/test_dataset/test_nodes_spark.py::TestFitAndBuild -v`
Expected: PASS — new tests pass; existing TestFitAndBuild still passes (fixture has all required dates).

- [ ] **Step 6.5: Run full spark dataset test module to catch any regressions**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_spark.py -v`
Expected: PASS for all tests.

- [ ] **Step 6.6: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "refactor(preprocessing): apply_preprocessor_to_features filters by needed snap_dates + raises on missing"
```

---

## Task 7: Update `test_versioning.py` for new key

**Files:**
- Modify: `tests/test_core/test_versioning.py:42-57` (`_base_params`), `:141-146` (test method), `:219-223` (test method)

- [ ] **Step 7.1: Update `_base_params()` to use list**

Edit `tests/test_core/test_versioning.py` — replace `_base_params()` (lines 42-57):

```python
def _base_params() -> dict:
    return {
        "dataset": {
            "train_snap_dates": [
                "2023-01-31", "2023-02-28", "2023-03-31", "2023-04-30",
                "2023-05-31", "2023-06-30", "2023-07-31", "2023-08-31",
                "2023-09-30", "2023-10-31", "2023-11-30", "2023-12-31",
            ],
            "val_snap_dates": ["2024-01-31"],
            "test_snap_dates": ["2024-02-29"],
            "sample_ratio": 0.1,
            "sample_ratio_overrides": {},
            "sample_group_keys": ["cust_segment_typ"],
            "train_dev_ratio": 0.1,
            "calibration_snap_dates": ["2024-02-29"],
            "calibration_sample_ratio": 1.0,
            "calibration_sample_ratio_overrides": {},
        },
    }
```

- [ ] **Step 7.2: Rename + update `test_train_snap_date_start_affects_base`**

Replace lines 141-146:

```python
    def test_train_snap_dates_affects_base(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["train_snap_dates"] = ["2022-01-31"]
        assert compute_base_dataset_version(p1, _sample_schema()) != \
            compute_base_dataset_version(p2, _sample_schema())
```

- [ ] **Step 7.3: Rename + update `test_train_snap_date_does_not_affect_train_variant`**

Replace lines 219-223:

```python
    def test_train_snap_dates_does_not_affect_train_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["train_snap_dates"] = ["2022-01-31"]
        assert compute_train_variant_id(p1) == compute_train_variant_id(p2)
```

- [ ] **Step 7.4: Run versioning tests**

Run: `pytest tests/test_core/test_versioning.py -v`
Expected: PASS for all tests.

- [ ] **Step 7.5: Commit**

```bash
git add tests/test_core/test_versioning.py
git commit -m "test(versioning): switch _base_params to train_snap_dates list"
```

---

## Task 8: Update scenarios test fixtures

**Files:**
- Modify: `tests/scenarios/test_scenario_1_new_inference.py:42-43`
- Modify: `tests/scenarios/test_scenario_2_shift_window.py:38-39`
- Modify: `tests/scenarios/test_scenario_3_new_features.py:43-44`
- Modify: `tests/scenarios/test_scenario_4_new_products.py:51-52`

- [ ] **Step 8.1: Inspect each scenario's date config**

Run:
```bash
grep -n "train_snap_date\|val_snap_dates\|test_snap_dates\|BASE_SNAP_DATES" tests/scenarios/test_scenario_*.py | head -40
```

Expected output: each scenario uses `train_snap_date_start: "2025-01-31"` plus an end (varies per scenario), with val/test being separate snap_dates from `BASE_SNAP_DATES = ["2025-01-31", ..., "2025-06-30"]`.

- [ ] **Step 8.2: Update `test_scenario_1_new_inference.py`**

Edit `tests/scenarios/test_scenario_1_new_inference.py` — replace lines 42-43:

Old:
```python
                "train_snap_date_start": "2025-01-31",
                "train_snap_date_end": "2025-04-30",
```

New:
```python
                "train_snap_dates": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30"],
```

- [ ] **Step 8.3: Update `test_scenario_2_shift_window.py`**

Edit `tests/scenarios/test_scenario_2_shift_window.py` — replace lines 38-39:

Old:
```python
                "train_snap_date_start": "2025-01-31",
                "train_snap_date_end": "2025-05-31",
```

New:
```python
                "train_snap_dates": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30", "2025-05-31"],
```

- [ ] **Step 8.4: Update `test_scenario_3_new_features.py`**

Edit `tests/scenarios/test_scenario_3_new_features.py` — replace lines 43-44:

Old:
```python
                "train_snap_date_start": "2025-01-31",
                "train_snap_date_end": "2025-04-30",
```

New:
```python
                "train_snap_dates": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30"],
```

- [ ] **Step 8.5: Update `test_scenario_4_new_products.py`**

Edit `tests/scenarios/test_scenario_4_new_products.py` — replace lines 51-52:

Old:
```python
                "train_snap_date_start": "2025-01-31",
                "train_snap_date_end": "2025-04-30",
```

New:
```python
                "train_snap_dates": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30"],
```

- [ ] **Step 8.6: Run scenarios (slow — these spin up Spark + run full pipelines)**

Run: `pytest tests/scenarios/ -v -x`
Expected: PASS for all scenarios. If any scenario times out, check that BASE_SNAP_DATES from `data_generator.py` covers all the dates in the new lists (should be `["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30", "2025-05-31", "2025-06-30"]` — already covers 1-4).

If `tests/scenarios/output/` artifacts cause stale failures, delete them first: `rm -rf tests/scenarios/output/`

- [ ] **Step 8.7: Commit**

```bash
git add tests/scenarios/test_scenario_1_new_inference.py tests/scenarios/test_scenario_2_shift_window.py tests/scenarios/test_scenario_3_new_features.py tests/scenarios/test_scenario_4_new_products.py
git commit -m "test(scenarios): switch fixtures to train_snap_dates list"
```

---

## Task 9: Update CLAUDE.md description

**Files:**
- Modify: `CLAUDE.md:10`

- [ ] **Step 9.1: Update the Project Overview line**

Edit `CLAUDE.md` — change line 10:

Old:
```markdown
- **Training**：12 個月月底快照，不定期手動執行
```

New:
```markdown
- **Training**：N 個月底 snapshot（顯式 `train_snap_dates` list 配置），不定期手動執行
```

- [ ] **Step 9.2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): note explicit train_snap_dates list config"
```

---

## Task 10: Final verification

**Files:** none (verification only)

- [ ] **Step 10.1: Run full unit test suite**

Run: `pytest tests/test_pipelines/test_dataset/test_nodes_shared.py tests/test_pipelines/test_dataset/test_nodes_spark.py tests/test_core/test_versioning.py -v`
Expected: PASS — all unit tests covering this change.

- [ ] **Step 10.2: Run scenarios (if not already passed in Task 8)**

Run: `pytest tests/scenarios/ -v`
Expected: PASS for all 4 scenarios.

- [ ] **Step 10.3: Run remaining repo tests to catch unrelated regressions**

Run: `pytest tests/ -x --ignore=tests/test_pipelines/test_dataset/test_nodes.py --ignore=tests/test_pipelines/test_training/test_pipeline.py -v`
Expected: PASS. (Excluded files are pandas-backed E2E that's out of scope per Non-Goals; they may fail on yaml-loaded paths but inline-param paths still work.)

- [ ] **Step 10.4: dev-cluster smoke (optional manual step)**

If dev-cluster is running, run a smoke test to verify hash jumps and pipeline end-to-end:

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb dataset --env production
# Expect: new base_dataset_version directory under data/dataset/<new-hash>/
ls data/dataset/
```

Then continue training:
```bash
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

Verify `data/dataset/latest -> <new-hash>` symlink and `models/latest -> <new-model-hash>`.

- [ ] **Step 10.5: Rebuild graphify (optional, automated by hook)**

Per CLAUDE.md graphify rule, the post-edit hook auto-rebuilds. To force rebuild:

```bash
python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

No commit needed if hook already rebuilt during prior commits.

---

## Self-Review Checklist (post-completion)

Before declaring done, walk the spec sections and confirm each is implemented:

- [ ] §1 yaml config: Task 3
- [ ] §2 helper `collect_dataset_snap_dates`: Task 1
- [ ] §3 `validate_date_splits` simplification: Task 2
- [ ] §4 `select_train_keys` (spark): Task 4
- [ ] §5 `fit_preprocessor_metadata` (spark): Task 5
- [ ] §6 `apply_preprocessor_to_features` (spark): Task 6
- [ ] Tests A–E: Tasks 6 (A, E2), 6 (B), 1 (C), 2 (D), 5 (E1)
- [ ] Hash & cache impact: documented in spec; tested via Task 7
- [ ] Docs: Task 9
- [ ] Scenario regression: Task 8
- [ ] Final verification: Task 10

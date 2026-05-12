# decimal128 → double cast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cast all `feature_columns` of `DecimalType` to `double` inside `build_model_input` and `apply_preprocessor` so downstream `pd.read_parquet` doesn't blow peak memory 10× by materializing `decimal.Decimal` Python objects.

**Architecture:** Add a private helper `_cast_feature_decimals_to_double` in `src/recsys_tfb/preprocessing/_spark.py`. Wire it as a final pre-return step inside both writer paths, wrapped in a `log_step("cast_decimals_to_double")` block plus an INFO summary line. Cache invalidation is a manual `rm` (out of scope to automate).

**Tech Stack:** PySpark 3.3.2, pytest 7.3.1, existing `recsys_tfb.core.logging.log_step`.

**Spec:** [docs/superpowers/specs/2026-05-12-decimal-to-double-cast-design.md](../specs/2026-05-12-decimal-to-double-cast-design.md)

---

## Task 1: Helper `_cast_feature_decimals_to_double` + unit tests (TDD)

**Files:**
- Create: `tests/test_preprocessing/__init__.py`
- Create: `tests/test_preprocessing/test_spark.py`
- Modify: `src/recsys_tfb/preprocessing/_spark.py` (add helper near top, after existing imports)

- [ ] **Step 1: Write failing unit tests**

Create `tests/test_preprocessing/__init__.py` (empty).

Create `tests/test_preprocessing/test_spark.py`:

```python
"""Tests for preprocessing._spark private helpers."""

import pandas as pd
import pytest
from decimal import Decimal
from pyspark.sql import types as T

from recsys_tfb.preprocessing._spark import _cast_feature_decimals_to_double

pytestmark = pytest.mark.spark


@pytest.fixture
def mixed_df(spark):
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("label", T.IntegerType()),
        T.StructField("feature_a", T.DecimalType(38, 6)),
        T.StructField("feature_b", T.IntegerType()),
        T.StructField("feature_c", T.DecimalType(29, 0)),
        T.StructField("non_feature_decimal", T.DecimalType(15, 2)),
    ])
    rows = [
        ("C001", 1, Decimal("1.500000"), 10, Decimal("123"), Decimal("9.99")),
        ("C002", 0, Decimal("2.250000"), 20, Decimal("456"), Decimal("8.88")),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _dtype(df, col):
    return dict(df.dtypes)[col]


def test_cast_feature_decimals_casts_only_feature_decimals(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    out, _ = _cast_feature_decimals_to_double(mixed_df, feature_cols)

    assert _dtype(out, "feature_a") == "double"
    assert _dtype(out, "feature_c") == "double"
    # int feature untouched
    assert _dtype(out, "feature_b") == "int"
    # non-feature decimal untouched (not in feature_cols)
    assert _dtype(out, "non_feature_decimal").startswith("decimal")
    # identity / label untouched
    assert _dtype(out, "cust_id") == "string"
    assert _dtype(out, "label") == "int"


def test_cast_feature_decimals_returns_casted_list(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = _cast_feature_decimals_to_double(mixed_df, feature_cols)
    assert sorted(casted) == ["feature_a", "feature_c"]


def test_cast_feature_decimals_noop_when_no_decimals(spark):
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("feature_a", T.IntegerType()),
        T.StructField("feature_b", T.DoubleType()),
    ])
    df = spark.createDataFrame([("C001", 1, 2.5)], schema=schema)
    out, casted = _cast_feature_decimals_to_double(df, ["feature_a", "feature_b"])

    assert casted == []
    assert out.schema == df.schema


def test_cast_feature_decimals_preserves_values(mixed_df):
    feature_cols = ["feature_a"]
    out, _ = _cast_feature_decimals_to_double(mixed_df, feature_cols)
    rows = out.orderBy("cust_id").collect()
    assert rows[0].feature_a == pytest.approx(1.5)
    assert rows[1].feature_a == pytest.approx(2.25)
```

- [ ] **Step 2: Run tests to verify they fail with ImportError**

Run: `.venv/bin/pytest tests/test_preprocessing/test_spark.py -v`
Expected: `ImportError: cannot import name '_cast_feature_decimals_to_double'`

- [ ] **Step 3: Implement the helper**

Add to `src/recsys_tfb/preprocessing/_spark.py`. Place it AFTER existing imports (which already include `pyspark.sql.types as T`, `pyspark.sql.functions as F`, `DataFrame`) and BEFORE `_compute_feature_columns` at line 52. Verify the imports exist first; add any missing ones.

```python
def _cast_feature_decimals_to_double(
    df: DataFrame,
    feature_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Cast all DecimalType columns within feature_cols to double.

    pandas/pyarrow materializes decimal128 as Python decimal.Decimal objects
    (~70 bytes/value vs 8 bytes/float64), inflating peak memory ~10× and
    OOM-killing extract_Xy in prod. LightGBM consumes float anyway, so cast
    at write time and bake the smaller representation into model_input.

    Identity and label columns are intentionally NOT cast — they should not
    be decimal to begin with, and silent coercion of primary keys / label
    dtype would mask a real schema bug.
    """
    feature_set = set(feature_cols)
    decimal_feature_cols = [
        f.name for f in df.schema.fields
        if f.name in feature_set and isinstance(f.dataType, T.DecimalType)
    ]
    for col in decimal_feature_cols:
        df = df.withColumn(col, F.col(col).cast("double"))
    return df, decimal_feature_cols
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_preprocessing/test_spark.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_preprocessing/__init__.py tests/test_preprocessing/test_spark.py src/recsys_tfb/preprocessing/_spark.py
git commit -m "feat(preprocessing): add _cast_feature_decimals_to_double helper

Cast feature_columns of DecimalType → double so downstream pandas read
doesn't materialize Python decimal.Decimal objects (~10× peak memory).
Identity and label columns left untouched."
```

---

## Task 2: Wire into `build_model_input` + integration test

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py` (`build_model_input`, around line 281)
- Modify: `tests/test_pipelines/test_dataset/test_nodes_spark.py` (add new test)

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_pipelines/test_dataset/test_nodes_spark.py`. Use the existing `feature_table`, `label_table`, `keys` fixtures pattern (look at existing tests around line 200-240 for the setup convention; copy whatever's needed).

```python
def test_build_model_input_casts_decimal_features_to_double(
    spark, label_table, parameters
):
    from decimal import Decimal
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("snap_date", T.TimestampType()),
        T.StructField("cust_id", T.StringType()),
        T.StructField("total_aum", T.DecimalType(38, 6)),
        T.StructField("fund_aum", T.DoubleType()),
        T.StructField("in_amt_sum_l1m", T.DecimalType(29, 0)),
        T.StructField("out_amt_sum_l1m", T.DoubleType()),
        T.StructField("in_amt_ratio_l1m", T.DoubleType()),
        T.StructField("out_amt_ratio_l1m", T.DoubleType()),
    ])
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]:
        snap_ts = pd.Timestamp(snap).to_pydatetime()
        for cid, aum in [("C001", "100.0"), ("C002", "200.0"),
                         ("C003", "300.0"), ("C004", "400.0")]:
            rows.append((snap_ts, cid, Decimal(aum), 10.0, Decimal("5"),
                         3.0, 0.05, 0.03))
    feature_table_decimal = spark.createDataFrame(rows, schema=schema)

    preprocessor = fit_preprocessor_metadata(feature_table_decimal, parameters)
    pft = apply_preprocessor_to_features(feature_table_decimal, preprocessor, parameters)
    train_keys, _ = split_train_keys(
        select_train_keys(feature_table_decimal, label_table, parameters),
        parameters,
    )
    result = build_model_input(train_keys, pft, label_table, preprocessor, parameters)

    feature_cols = preprocessor["feature_columns"]
    out_dtypes = dict(result.dtypes)
    decimal_feature_cols = [c for c in feature_cols if "decimal" in out_dtypes[c]]
    assert decimal_feature_cols == [], (
        f"feature_columns still contain decimal types: {decimal_feature_cols}"
    )
    # And the ones that WERE decimal in the input are now double
    assert out_dtypes["total_aum"] == "double"
    assert out_dtypes["in_amt_sum_l1m"] == "double"
```

Note: if `parameters` fixture isn't already present in this test file, copy from another test in the same file (around lines 60-100 there should be a `parameters` fixture).

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::test_build_model_input_casts_decimal_features_to_double -v`
Expected: FAIL with assertion `feature_columns still contain decimal types: ['total_aum', 'in_amt_sum_l1m']`

- [ ] **Step 3: Wire the helper into `build_model_input`**

In `src/recsys_tfb/preprocessing/_spark.py`, modify `build_model_input`. Current code at line 276-283:

```python
    with log_step(logger, "select_output_columns"):
        required = list(set(identity_cols + [label_col] + feature_columns))
        _validate_columns(dataset.columns, required, "build_model_input")

        output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
        result = dataset.select(*output_cols)

    logger.info("Model input (Spark): %d features", len(feature_columns))
    return result
```

Replace with:

```python
    with log_step(logger, "select_output_columns"):
        required = list(set(identity_cols + [label_col] + feature_columns))
        _validate_columns(dataset.columns, required, "build_model_input")

        output_cols = list(dict.fromkeys(identity_cols + [label_col] + feature_columns))
        result = dataset.select(*output_cols)

    with log_step(logger, "cast_decimals_to_double"):
        result, casted = _cast_feature_decimals_to_double(result, feature_columns)
    logger.info(
        "build_model_input: %d features, cast %d decimal feature columns to double",
        len(feature_columns), len(casted),
    )
    if casted:
        logger.debug("build_model_input: casted columns = %s", casted)
    return result
```

(Note: dropping the old `Model input (Spark): %d features` line and folding the count into the new INFO line — same info, less noise.)

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_pipelines/test_dataset/test_nodes_spark.py::test_build_model_input_casts_decimal_features_to_double -v`
Expected: PASS

- [ ] **Step 5: Run full dataset test suite to verify no regression**

Run: `.venv/bin/pytest tests/test_pipelines/test_dataset/ -v`
Expected: all pass (including the existing `build_model_input` tests).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git commit -m "feat(preprocessing): cast decimal features to double in build_model_input

Prevents ~10× peak-memory blow-up when downstream extract_Xy reads the
parquet cache via pd.read_parquet (decimal128 → Python Decimal objects
otherwise). Existing parquet caches must be manually rm'd to regenerate."
```

---

## Task 3: Wire into `apply_preprocessor` + integration test

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py` (`apply_preprocessor`, around line 318)
- Modify: `tests/test_pipelines/test_inference/test_nodes_spark.py` (add new test)

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_pipelines/test_inference/test_nodes_spark.py`. Look at lines 80-110 for the existing `apply_preprocessor` test pattern (it builds a scoring DF, calls `apply_preprocessor(scoring, preprocessor, parameters)`).

```python
def test_apply_preprocessor_casts_decimal_features_to_double(
    spark, preprocessor, parameters
):
    from decimal import Decimal
    from pyspark.sql import types as T

    # Reuse the same feature_columns as the existing preprocessor fixture,
    # but force one of them to be decimal in the scoring data
    feature_cols = preprocessor["feature_columns"]
    decimal_col = feature_cols[0]

    identity_cols = parameters["schema"]["identity_columns"]
    schema_fields = []
    for c in identity_cols:
        schema_fields.append(
            T.StructField(c, T.TimestampType() if c == "snap_date" else T.StringType())
        )
    for c in feature_cols:
        if c == decimal_col:
            schema_fields.append(T.StructField(c, T.DecimalType(38, 6)))
        else:
            schema_fields.append(T.StructField(c, T.DoubleType()))

    row = []
    for c in identity_cols:
        row.append(pd.Timestamp("2024-01-31").to_pydatetime() if c == "snap_date" else "C001")
    for c in feature_cols:
        row.append(Decimal("1.5") if c == decimal_col else 0.5)

    scoring = spark.createDataFrame([tuple(row)], schema=T.StructType(schema_fields))

    result = apply_preprocessor(scoring, preprocessor, parameters)

    out_dtypes = dict(result.dtypes)
    assert out_dtypes[decimal_col] == "double", (
        f"{decimal_col} still {out_dtypes[decimal_col]}, expected double"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_pipelines/test_inference/test_nodes_spark.py::test_apply_preprocessor_casts_decimal_features_to_double -v`
Expected: FAIL — decimal col still decimal.

- [ ] **Step 3: Wire the helper into `apply_preprocessor`**

In `src/recsys_tfb/preprocessing/_spark.py`, modify `apply_preprocessor`. Current code at line 314-321:

```python
    with log_step(logger, "select_feature_columns"):
        missing = set(feature_columns) - set(result.columns)
        if missing:
            raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")
        result = result.select(*identity_cols, *feature_columns)

    logger.info("Preprocessed scoring data (Spark): %d columns", len(result.columns))
    return result
```

Replace with:

```python
    with log_step(logger, "select_feature_columns"):
        missing = set(feature_columns) - set(result.columns)
        if missing:
            raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")
        result = result.select(*identity_cols, *feature_columns)

    with log_step(logger, "cast_decimals_to_double"):
        result, casted = _cast_feature_decimals_to_double(result, feature_columns)
    logger.info(
        "apply_preprocessor: %d columns, cast %d decimal feature columns to double",
        len(result.columns), len(casted),
    )
    if casted:
        logger.debug("apply_preprocessor: casted columns = %s", casted)
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_pipelines/test_inference/test_nodes_spark.py::test_apply_preprocessor_casts_decimal_features_to_double -v`
Expected: PASS

- [ ] **Step 5: Run full inference test suite**

Run: `.venv/bin/pytest tests/test_pipelines/test_inference/ -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_inference/test_nodes_spark.py
git commit -m "feat(preprocessing): cast decimal features to double in apply_preprocessor

Mirror of build_model_input fix; inference scoring parquet also gets read
back via pandas downstream and would face the same OOM."
```

---

## Task 4: Full-suite regression check + final commit

- [ ] **Step 1: Run full test suite**

Run: `.venv/bin/pytest tests/ -x -q`
Expected: all pass. If anything outside our changes fails, investigate before proceeding.

- [ ] **Step 2: Push branch + open PR (handled by finishing-a-development-branch skill)**

PR description must include:

```
## Summary
- Cast all `feature_columns` of `DecimalType` → `double` in
  `build_model_input` and `apply_preprocessor`.
- Fixes ~10× peak-memory blow-up in `extract_Xy` `read_parquet` step
  (OOM-killed `tune_hyperparameters` in prod cluster with 64GB RAM).

## ⚠️ Cache invalidation required after merge
The existing `/dataset/workspaces/data/recsys_cache/<base_dataset_version>/`
caches still contain decimal columns. The cache hash key does NOT include
the cast logic, so new code will hit old (decimal) caches. After merging
you must:

    rm -rf /dataset/workspaces/data/recsys_cache/<base_dataset_version>/

and let the dataset pipeline regenerate them.

## Test plan
- [x] New unit tests on the helper (4 cases)
- [x] New integration tests on `build_model_input` and `apply_preprocessor`
- [x] Full `pytest tests/` green
- [ ] After merge: nuke cache + re-run dataset pipeline + re-run training
- [ ] Verify `extract_Xy: parquet metadata ... schema_types` log shows
      no `decimal128(*)` entries
- [ ] Verify `read_parquet` step completes (no OOM)
```

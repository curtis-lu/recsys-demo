# Non-numeric Feature Gate (B6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 關閉「未宣告的字串欄靜默變成特徵 → `_pdf_to_X` 的 `to_numpy` 產出 object 矩陣 → driver OOM」的縫隙，方法是一個純 predicate（不變量 B6）掛在三個時間點。

**Architecture:** 在 `core/consistency.py` 新增一個純 predicate `nonnumeric_feature_errors` ＋ 一個 Spark dtype 分類器 `spark_dtype_is_numeric`（single source of truth）。兩個呼叫點各自把自己的 dtype 詞彙分類成 `"numeric"`/`"nonnumeric"` 後餵給它：dataset 側閘（`preprocessing/_spark.py` 的 `validate_data_consistency`，防復發）與 training 讀取 backstop（`io/extract.py`，救現在的 cached parquet）。第三塊獨立：`scripts/suggest_categorical_cols.py` 對高 cardinality 字串欄改建議進 `drop_columns`，關掉源頭。最後同步四份文件。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2、pyarrow 14.0.1、pandas 1.5.3、pytest、typer。**No Spark UDF、no network、no new packages。**

**設計來源：** `docs/superpowers/specs/2026-07-11-nonnumeric-feature-gate-design.md`（Phase 0 only；Phase 1/2 為 gated follow-up，不在本計畫）。

---

## 環境約定（每個指令照抄，勿憑記憶）

```bash
WT=/Users/curtislu/projects/recsys_tfb/.worktrees/nonnumeric-feature-gate
PY=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
# 測試一律： PYTHONPATH=$WT/src $PY -m pytest <path> -q
# git 一律：  git -C "$WT" ...
# 改檔絕對路徑必含 .worktrees/nonnumeric-feature-gate（R1：改錯邊的徵兆是輸出跟 baseline 完全相同）
```

**開工前 baseline（一次）：** 跑一次待動測試檔，記錄 main 既有 fail（known-pitfalls.md §5），避免把既有紅燈歸因給本次改動。

```bash
PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_core/test_consistency.py" "$WT/tests/scripts/test_suggest_categorical_cols.py" "$WT/tests/test_io/test_extract.py" -q 2>&1 | tail -15
```

---

## File Structure

| 檔案 | 責任 | 動作 |
|---|---|---|
| `src/recsys_tfb/core/consistency.py` | B6 純 predicate ＋ Spark dtype 分類器 ＋ legend | Modify（新增，約 B5 之後 ~962 行；legend ~123 行） |
| `src/recsys_tfb/preprocessing/_spark.py` | dataset 側閘接 B6（`validate_data_consistency`，:255） | Modify |
| `src/recsys_tfb/io/extract.py` | training 讀取 backstop（讀 parquet schema 後、讀資料前） | Modify |
| `scripts/suggest_categorical_cols.py` | 高卡字串欄改建議 drop_columns | Modify |
| `tests/test_core/test_consistency.py` | B6 predicate + 分類器 純測試 | Modify |
| `tests/test_pipelines/test_dataset/test_nodes_spark.py` | dataset 閘 Spark 整合測試 | Modify |
| `tests/test_io/test_extract.py` | training backstop 測試 | Modify |
| `tests/scripts/test_suggest_categorical_cols.py` | suggest drop 路由測試 | Modify |
| `docs/operations/training-oom-object-matrix.md` | 讀者＝撞到失敗的工程師：改寫成「收到錯誤怎麼辦」 | Modify（已在 worktree，untracked） |
| `docs/pipelines/dataset.md` | 讀者＝寫 config 的人：加 B6 規則 | Modify |
| `docs/operations/known-pitfalls.md` | 讀者＝未來 session：一條 footgun 條目 | Modify |
| `README.md` | operations 索引補孤兒檔連結 | Modify |

依賴：Task 1 是 Task 2、3 的基礎（共用 predicate）。Task 4 獨立。Task 5 依賴 Task 2、3 的最終錯誤訊息定案。

---

### Task 1: B6 純 predicate ＋ Spark dtype 分類器 ＋ legend

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`（predicate 加在 B5 `categorical_dtype_errors` 之後；legend 加在 Layer-2 段 B5 條目之後）
- Test: `tests/test_core/test_consistency.py`

- [ ] **Step 1: 寫失敗測試**

加到 `tests/test_core/test_consistency.py` 檔尾（import 併入該檔既有的 `from recsys_tfb.core.consistency import (...)` 區塊）：

```python
from recsys_tfb.core.consistency import (
    nonnumeric_feature_errors,
    spark_dtype_is_numeric,
)


class TestSparkDtypeIsNumeric:
    @pytest.mark.parametrize(
        "dt,expected",
        [
            ("int", True), ("bigint", True), ("smallint", True),
            ("double", True), ("float", True), ("boolean", True),
            ("decimal(15,0)", True), ("decimal(38,10)", True),
            ("string", False), ("STRING", False), (" string ", False),
            ("binary", False), ("date", False), ("timestamp", False),
            ("array<string>", False), ("map<string,int>", False),
            ("struct<a:int>", False),
        ],
    )
    def test_classification(self, dt, expected):
        assert spark_dtype_is_numeric(dt) is expected


class TestNonnumericFeatureErrors:
    def test_string_feature_not_encoded_is_flagged(self):
        errs = nonnumeric_feature_errors(
            {"age": "numeric", "cust_segment": "nonnumeric"}, set()
        )
        assert len(errs) == 1
        assert "cust_segment" in errs[0]
        assert "categorical_columns" in errs[0]
        assert "drop_columns" in errs[0]

    def test_nonnumeric_but_will_be_encoded_is_ok(self):
        # prod_name: 在 parquet 是 string，但屬 deferred identity categorical
        errs = nonnumeric_feature_errors(
            {"prod_name": "nonnumeric", "age": "numeric"}, {"prod_name"}
        )
        assert errs == []

    def test_all_numeric_is_ok(self):
        assert nonnumeric_feature_errors({"a": "numeric", "b": "numeric"}, set()) == []

    def test_empty_is_ok(self):
        assert nonnumeric_feature_errors({}, set()) == []

    def test_multiple_offenders_sorted_by_column(self):
        errs = nonnumeric_feature_errors(
            {"zzz": "nonnumeric", "aaa": "nonnumeric"}, set()
        )
        assert len(errs) == 2
        assert "aaa" in errs[0] and "zzz" in errs[1]
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_core/test_consistency.py::TestNonnumericFeatureErrors" "$WT/tests/test_core/test_consistency.py::TestSparkDtypeIsNumeric" -q`
Expected: FAIL — `ImportError: cannot import name 'nonnumeric_feature_errors'`。

- [ ] **Step 3: 實作 predicate ＋ 分類器**

加到 `src/recsys_tfb/core/consistency.py`，緊接 `categorical_dtype_errors`（B5）之後（約 962 行、`return errors` 那個函式尾之後）：

```python
# ---------------------------------------------------------------------------
# B6 — non-numeric feature column that will not be encoded (object-dtype OOM)
# ---------------------------------------------------------------------------

_NONNUMERIC_SPARK_PREFIXES = (
    "string", "binary", "date", "timestamp", "interval",
    "array", "map", "struct",
)


def spark_dtype_is_numeric(simple_string: str) -> bool:
    """True iff a Spark ``DataFrame.dtypes`` simpleString denotes a type that
    survives ``DataFrame.values`` into a numeric numpy matrix (int / float /
    decimal / boolean). String / binary / date / timestamp / complex types force
    ``object`` dtype — the B6 footgun. Pure string classification (no Spark import).
    """
    return not simple_string.strip().lower().startswith(_NONNUMERIC_SPARK_PREFIXES)


def nonnumeric_feature_errors(
    feature_kinds: dict[str, str],
    will_be_encoded: set[str],
) -> list[str]:
    """B6 invariant — the single definition.

    A *feature* column that is non-numeric AND will not be encoded to numeric
    downstream forces ``DataFrame.values`` into ``object`` dtype: every cell
    becomes a boxed Python object (~34 B/cell vs 8 B for float64), exploding
    driver memory (OOM at ``_pdf_to_X`` ``to_numpy``) and later failing
    LightGBM's float cast. Prevented by declaring the column categorical (so it
    is integer-encoded) or dropping it.

    ``feature_kinds`` maps each *feature* column to ``"numeric"`` or
    ``"nonnumeric"``; the caller classifies using its own dtype vocabulary
    (Spark simpleString via :func:`spark_dtype_is_numeric` at the dataset gate,
    or pyarrow types at the training-read backstop). ``will_be_encoded`` is the
    set of feature columns that are non-numeric now but become numeric
    downstream (declared categoricals, incl. deferred identity categoricals).
    Returns collect-all error strings sorted by column; empty means OK.
    """
    errors: list[str] = []
    for col in sorted(feature_kinds):
        if feature_kinds[col] != "numeric" and col not in will_be_encoded:
            errors.append(
                f"feature column {col!r} is non-numeric and is not declared "
                f"categorical, so it would become an un-encoded object-dtype "
                f"model feature (OOM at _pdf_to_X.to_numpy, then a LightGBM "
                f"float-cast error). If {col!r} is a categorical feature, add it "
                f"to dataset.prepare_model_input.categorical_columns (it is then "
                f"integer-encoded); if it is not a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
            )
    return errors
```

- [ ] **Step 4: 更新 legend docstring**

在模組 docstring 的 Layer-2 段、B5 條目之後（約 123 行、`Layer 3 — specified but DEFERRED` 之前）加入：

```
* B6 — a feature column that is non-numeric (string / binary / date / timestamp /
  complex) and is NOT declared categorical (so never integer-encoded): it becomes
  an ``object``-dtype model feature → driver OOM at ``_pdf_to_X`` ``to_numpy`` and
  a downstream LightGBM float-cast error. Predicate: ``nonnumeric_feature_errors``
  (with the ``spark_dtype_is_numeric`` classifier). Wired at TWO call sites — the
  dataset gate ``validate_data_consistency`` (prevents a rebuilt dataset baking it
  in) and a training-read backstop in ``io/extract.py`` (fails fast on an
  already-built parquet, before the expensive pandas read). B4 is unused.
```

- [ ] **Step 5: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_core/test_consistency.py::TestNonnumericFeatureErrors" "$WT/tests/test_core/test_consistency.py::TestSparkDtypeIsNumeric" -q`
Expected: PASS（全綠）。

- [ ] **Step 6: Commit**

```bash
git -C "$WT" add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git -C "$WT" commit -m "feat(consistency): B6 predicate nonnumeric_feature_errors + spark dtype classifier"
```

---

### Task 2: dataset 側閘接 B6（防復發）

**Files:**
- Modify: `src/recsys_tfb/preprocessing/_spark.py`（`validate_data_consistency`，:255；沿用同檔 `_compute_feature_columns`，:112）
- Test: `tests/test_pipelines/test_dataset/test_nodes_spark.py`

- [ ] **Step 1: 寫失敗測試**

先讀既有 fixture：`grep -n "def feature_table\|def sample_pool\|def label_table\|def parameters" "$WT/tests/test_pipelines/test_dataset/test_nodes_spark.py" "$WT/tests/test_pipelines/test_dataset/conftest.py"`，確認 `feature_table` / `sample_pool` / `label_table` / `parameters` fixtures 的欄位與 schema。B6 閘與 B1 同跑，故合成 `sample_pool`/`label_table` 必須滿足 B1（沿用既有乾淨 fixtures 即可）。

加到 `tests/test_pipelines/test_dataset/test_nodes_spark.py`（併入既有 `@pytest.mark.spark` 區塊）：

```python
class TestValidateDataConsistencyB6:
    def test_unencoded_string_feature_raises(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        from pyspark.sql import functions as F
        from recsys_tfb.core.consistency import DataConsistencyError

        # 注入一個未宣告 categorical、也未 drop 的字串特徵欄
        rogue = feature_table.withColumn("rogue_str", F.lit("free_text"))
        with pytest.raises(DataConsistencyError, match="rogue_str"):
            validate_data_consistency(sample_pool, label_table, rogue, parameters)

    def test_clean_feature_table_passes(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 乾淨 feature_table（既有 fixture 無流氓字串欄）→ 不 raise
        assert (
            validate_data_consistency(sample_pool, label_table, feature_table, parameters)
            is None
        )
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_pipelines/test_dataset/test_nodes_spark.py::TestValidateDataConsistencyB6" -q`
Expected: `test_unencoded_string_feature_raises` FAIL（未 raise，因 B6 尚未接線）；`test_clean_feature_table_passes` 可能已 PASS。

- [ ] **Step 3: 接線 B6**

改 `src/recsys_tfb/preprocessing/_spark.py` 的 `validate_data_consistency`（:255）。(a) 擴充 lazy import：

```python
    from recsys_tfb.core.consistency import (
        DataConsistencyError,
        categorical_dtype_errors,
        item_coverage_errors,
        nonnumeric_feature_errors,
        resolved_item_values,
        spark_dtype_is_numeric,
    )
```

(b) 把既有 `errors = item_coverage_errors(...) + categorical_dtype_errors(...)` 那段（約 :301-307）替換成：

```python
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    ft_dtypes = dict(feature_table.dtypes)
    feature_cols = _compute_feature_columns(
        list(feature_table.columns),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )
    # 只檢查來自 feature_table 的欄（identity categoricals 如 prod_name 來自
    # schema.categorical_values、不在 feature_table.dtypes，由 A3 另行驗證）
    feature_kinds = {
        c: ("numeric" if spark_dtype_is_numeric(ft_dtypes[c]) else "nonnumeric")
        for c in feature_cols
        if c in ft_dtypes
    }
    errors = (
        item_coverage_errors(
            item,
            resolved_item_values(parameters),
            _distinct_items(sample_pool),
            _distinct_items(label_table),
        )
        + categorical_dtype_errors(categorical_cols, ft_dtypes)
        + nonnumeric_feature_errors(feature_kinds, set(categorical_cols))
    )
```

（注意：`schema`、`item` 在函式上方已定義；`_get_preprocessing_config` 回傳 `(drop_cols, categorical_cols)`。若原碼此處已有 `_, categorical_cols = _get_preprocessing_config(...)`，改成上面的完整解包並移除重複行。）

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_pipelines/test_dataset/test_nodes_spark.py::TestValidateDataConsistencyB6" -q`
Expected: PASS（兩個都綠）。

- [ ] **Step 5: 弄壞驗證（mutation 下在因果鏈上）**

暫時把 Step 3 (b) 的 `+ nonnumeric_feature_errors(feature_kinds, set(categorical_cols))` 這行**整行刪掉**，重跑 `test_unencoded_string_feature_raises`，應轉 **FAIL**（證明紅燈來自 B6 接線，而非別的閘）。確認後把該行改回，重跑應綠。**禁止用 `git checkout --` 還原**（known-pitfalls.md §5b）。

- [ ] **Step 6: Commit**

```bash
git -C "$WT" add src/recsys_tfb/preprocessing/_spark.py tests/test_pipelines/test_dataset/test_nodes_spark.py
git -C "$WT" commit -m "feat(dataset-gate): wire B6 nonnumeric-feature check into validate_data_consistency"
```

---

### Task 3: training 讀取 backstop（救現在的 cached parquet）

**Files:**
- Modify: `src/recsys_tfb/io/extract.py`（新增 helper；在 `extract_Xy` 與 `extract_Xy_with_groups` 兩處，`_log_parquet_metadata(handle)` 之後、`read_parquet` log_step 之前呼叫）
- Test: `tests/test_io/test_extract.py`

- [ ] **Step 1: 寫失敗測試**

先讀既有 fixture：`grep -n "def parameters\|preprocessor_metadata\|ParquetHandle\|def test_\|to_parquet\|tmp_path" "$WT/tests/test_io/test_extract.py" | head -30`，沿用其 `parameters` 與 `preprocessor_metadata` 建法。加到 `tests/test_io/test_extract.py`：

```python
class TestExtractXyB6Backstop:
    def _write_parquet(self, tmp_path, with_string):
        import pandas as pd
        pdf = pd.DataFrame({
            "f_num": [1.0, 2.0, 3.0],
            "prod_name": ["a", "b", "a"],   # deferred identity cat（合法 string 特徵）
            "label": [0, 1, 0],
        })
        if with_string:
            pdf["rogue_str"] = ["x", "y", "z"]   # 未宣告 categorical 的 string 特徵
        p = tmp_path / "mi.parquet"
        pdf.to_parquet(p)
        return p

    def _meta(self, with_string):
        feats = ["f_num", "prod_name"] + (["rogue_str"] if with_string else [])
        return {
            "feature_columns": feats,
            "categorical_columns": ["prod_name"],   # rogue_str 不在內
            "category_mappings": {"prod_name": ["a", "b"]},
        }

    def test_string_feature_fails_fast(self, tmp_path, parameters):
        from recsys_tfb.core.consistency import DataConsistencyError
        from recsys_tfb.io.extract import extract_Xy
        from recsys_tfb.io.handles import ParquetHandle

        handle = ParquetHandle(path=str(self._write_parquet(tmp_path, True)))
        with pytest.raises(DataConsistencyError, match="rogue_str"):
            extract_Xy(handle, self._meta(True), parameters)

    def test_clean_parquet_proceeds(self, tmp_path, parameters):
        from recsys_tfb.io.extract import extract_Xy
        from recsys_tfb.io.handles import ParquetHandle

        handle = ParquetHandle(path=str(self._write_parquet(tmp_path, False)))
        X, y = extract_Xy(handle, self._meta(False), parameters)
        assert X.shape[0] == 3
```

（若既有測試已有 `parameters` fixture 使 `get_schema(parameters)["identity_columns"]` 含 `prod_name`，直接沿用；否則在測試模組建一個最小 `parameters`，其 `schema.identity_columns` 含 `prod_name`、`schema.label` = `"label"`。）

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_io/test_extract.py::TestExtractXyB6Backstop" -q`
Expected: `test_string_feature_fails_fast` FAIL（未 raise，會一路讀進 pandas）。

- [ ] **Step 3: 實作 helper 並在兩處呼叫**

在 `src/recsys_tfb/io/extract.py` 新增（放在 `_pdf_to_X` 之前）：

```python
def _assert_feature_dtypes_numeric(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> None:
    """B6 training-read backstop — raise before the expensive pandas read if any
    model feature column is a non-numeric parquet type that will NOT be encoded
    downstream (would OOM at _pdf_to_X.to_numpy, then fail LightGBM's float cast).

    Reads parquet schema only (pyarrow metadata, no data). Deferred identity
    categoricals (e.g. prod_name, encoded later in _pdf_to_X) are exempt.
    """
    import pyarrow.dataset as pads
    import pyarrow.types as pat

    from recsys_tfb.core.consistency import (
        DataConsistencyError,
        nonnumeric_feature_errors,
    )

    feature_cols = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    identity_cols = get_schema(parameters)["identity_columns"]
    deferred = {c for c in categorical_cols if c in identity_cols}

    schema = pads.dataset(handle.path, format="parquet").schema
    field_type = {name: schema.field(name).type for name in schema.names}

    def _kind(t) -> str:
        if pat.is_integer(t) or pat.is_floating(t) or pat.is_boolean(t) or pat.is_decimal(t):
            return "numeric"
        return "nonnumeric"

    feature_kinds = {
        c: _kind(field_type[c]) for c in feature_cols if c in field_type
    }
    errors = nonnumeric_feature_errors(feature_kinds, deferred)
    if errors:
        raise DataConsistencyError(
            "train_model_input feature columns include un-encoded non-numeric "
            "type(s) — this OOMs at to_numpy and fails LightGBM's float cast ("
            + str(len(errors))
            + " issue(s)):\n- "
            + "\n- ".join(errors)
        )
```

然後在 `extract_Xy`（:312 `_log_parquet_metadata(handle)` 之後、`with log_step(logger, "read_parquet")` 之前）加一行：

```python
    _log_parquet_metadata(handle)
    _assert_feature_dtypes_numeric(handle, preprocessor_metadata, parameters)
```

在 `extract_Xy_with_groups`（:362 `_log_parquet_metadata(handle)` 之後）加同一行。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_io/test_extract.py::TestExtractXyB6Backstop" -q`
Expected: PASS。

- [ ] **Step 5: 弄壞驗證（mutation 下在呼叫本身）**

暫時把 `extract_Xy` 裡 `_assert_feature_dtypes_numeric(handle, preprocessor_metadata, parameters)` 這**行呼叫刪掉**（不是改 helper 內部），重跑 `test_string_feature_fails_fast`，應 FAIL（證明紅燈來自這個呼叫）。改回後重跑應綠。**禁止 `git checkout --`**。

- [ ] **Step 6: 迴歸既有 extract 測試（確認沒破壞乾淨路徑）**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_io/test_extract.py" -q 2>&1 | tail -8`
Expected: 與 Task 開工 baseline 一致（無新增 fail）。

- [ ] **Step 7: Commit**

```bash
git -C "$WT" add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git -C "$WT" commit -m "feat(extract): B6 training-read backstop — fail fast before pandas read on non-numeric feature cols"
```

---

### Task 4: `suggest_categorical_cols.py` — 高卡字串欄改建議 drop

**Files:**
- Modify: `scripts/suggest_categorical_cols.py`
- Test: `tests/scripts/test_suggest_categorical_cols.py`

- [ ] **Step 1: 寫失敗測試**

`suggest_categorical_columns_spark` 回傳簽名會從 `(cats, implicit, n_rows)` 變成 `(cats, drop_suggestions, implicit, n_rows)`。加到 `tests/scripts/test_suggest_categorical_cols.py`：

```python
@pytest.mark.spark
class TestStringDropRouting:
    def test_high_cardinality_string_routed_to_drop(self, spark):
        rows = [(f"id_{i}", "seg_a") for i in range(60)]
        df = spark.createDataFrame(rows, ["raw_id", "seg"])
        cats, drops, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_string_cardinality=10
        )
        assert cats == ["seg"]                      # 低卡字串仍是 categorical
        assert [c for c, _ in drops] == ["raw_id"]  # 高卡字串導向 drop
        assert dict(drops)["raw_id"] >= 50          # 附 cardinality（approx，容忍誤差）

    def test_low_cardinality_string_stays_categorical(self, spark):
        df = spark.createDataFrame([("a",), ("b",), ("a",)], ["s"])
        cats, drops, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["s"]
        assert drops == []


def test_format_yaml_output_includes_drop_block():
    out = format_yaml_output(["seg"], [("raw_id", 4200)])
    assert "categorical_columns:" in out
    assert "drop_columns:" in out
    assert '- "raw_id"' in out
    assert "4200" in out  # cardinality 註解
```

同時把該檔**既有**測試中所有 `cats, implicit, n_rows = suggest_categorical_columns_spark(...)` 改成 `cats, drops, implicit, n_rows = suggest_categorical_columns_spark(...)`（新增第 2 回傳值；用 `grep -n "= suggest_categorical_columns_spark" "$WT/tests/scripts/test_suggest_categorical_cols.py"` 定位全部呼叫點）。

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/scripts/test_suggest_categorical_cols.py" -q`
Expected: FAIL — 新測試（`max_string_cardinality` 未知參數 / 回傳只有 3 值）＋既有測試解包錯誤。

- [ ] **Step 3: 改 `suggest_categorical_columns_spark`**

替換 `scripts/suggest_categorical_cols.py` 的 `suggest_categorical_columns_spark`（:35-84）為：

```python
def suggest_categorical_columns_spark(
    df: "SparkDataFrame",
    max_numerical_cardinality: int = 20,
    max_string_cardinality: int = 50,
) -> tuple[list[str], list[tuple[str, int]], list[tuple[str, int]], int]:
    """Infer categorical columns (and high-cardinality strings to drop).

    Numeric columns with nunique <= ``max_numerical_cardinality`` are implicit
    categoricals (else left as numeric features). String/boolean columns with
    nunique <= ``max_string_cardinality`` are categoricals; ABOVE that they are
    routed to ``drop_suggestions`` (an un-encoded high-cardinality string would
    become an object-dtype model feature → OOM; see consistency B6). Cardinality
    for BOTH numeric and string/bool columns is computed in the single existing
    aggregation (no extra scan).

    Returns:
        (categorical_columns, drop_suggestions, implicit_numeric_info, n_rows)
        drop_suggestions: list of (column, approx_nunique) sorted by column.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, NumericType, StringType

    string_bool_cols: list[str] = []
    numeric_cols: list[str] = []
    for field in df.schema.fields:
        dt = field.dataType
        if isinstance(dt, (StringType, BooleanType)):
            string_bool_cols.append(field.name)
        elif isinstance(dt, NumericType):
            numeric_cols.append(field.name)

    counted_cols = numeric_cols + string_bool_cols
    agg_exprs = [F.count("*").alias("__n_rows__")] + [
        F.approx_count_distinct(F.col(c), rsd=0.05).alias(c) for c in counted_cols
    ]
    row = df.agg(*agg_exprs).collect()[0]
    n_rows = int(row["__n_rows__"])

    implicit: list[tuple[str, int]] = []
    numeric_categorical: set[str] = set()
    for col in numeric_cols:
        n_distinct = int(row[col])
        if n_distinct <= max_numerical_cardinality:
            numeric_categorical.add(col)
            implicit.append((col, n_distinct))

    string_categorical: set[str] = set()
    drop_suggestions: list[tuple[str, int]] = []
    for col in string_bool_cols:
        n_distinct = int(row[col])
        if n_distinct <= max_string_cardinality:
            string_categorical.add(col)
        else:
            drop_suggestions.append((col, n_distinct))
    drop_suggestions.sort()

    categorical: list[str] = []
    for field in df.schema.fields:
        if field.name in string_categorical or field.name in numeric_categorical:
            categorical.append(field.name)

    return categorical, drop_suggestions, implicit, n_rows
```

- [ ] **Step 4: 改 `format_yaml_output` 加 drop 塊**

替換 `format_yaml_output`（:87-98）為：

```python
def format_yaml_output(
    categorical: list[str],
    drop_suggestions: list[tuple[str, int]] | None = None,
) -> str:
    """Format categorical + suggested drop columns as a YAML snippet.

    Example:
        categorical_columns:
          - "col_a"
        drop_columns:
          - "raw_id"   # nunique=4200 — high-cardinality string, not a categorical
    """
    lines = ["categorical_columns:"]
    for col in categorical:
        lines.append(f'  - "{col}"')
    lines.append("drop_columns:")
    if drop_suggestions:
        for col, n in drop_suggestions:
            lines.append(
                f'  - "{col}"   # nunique={n} — high-cardinality string, not a categorical'
            )
    else:
        lines.append("  # （無高 cardinality 字串欄；此清單供人工確認）")
    return "\n".join(lines) + "\n"
```

- [ ] **Step 5: 接上 CLI 與 summary**

在 `main`（:161）的 options 加：

```python
    max_string_cardinality: int = typer.Option(
        50,
        "--max-string-cardinality",
        help="String/bool columns with nunique > this are suggested into drop_columns (B6 footgun)",
    ),
```

改 `main` body 的呼叫與輸出（:180-196）：

```python
        categorical, drop_suggestions, implicit, n_rows = suggest_categorical_columns_spark(
            sdf, max_cardinality, max_string_cardinality
        )
        n_cols = len(sdf.schema.fields)
    finally:
        spark.stop()

    yaml_content = format_yaml_output(categorical, drop_suggestions)
    output_path = _write_output(stem, yaml_content)
    _print_summary(
        source=source,
        max_cardinality=max_cardinality,
        n_rows=n_rows,
        n_cols=n_cols,
        categorical=categorical,
        implicit=implicit,
        drop_suggestions=drop_suggestions,
        output_path=output_path,
    )
```

在 `_print_summary`（:128 簽名）加參數 `drop_suggestions: list[tuple[str, int]]`，並在 `implicit` 區塊之後（:157 `typer.echo("", err=True)` 之前）加：

```python
    if drop_suggestions:
        typer.echo("", err=True)
        typer.echo(
            "High-cardinality string columns routed to drop_columns "
            "(un-encoded string feature → object-dtype OOM, consistency B6):",
            err=True,
        )
        for col, n in drop_suggestions:
            typer.echo(f"  - {col} (nunique={n})", err=True)
```

- [ ] **Step 6: 更新 module docstring（讀者＝CLI 使用者）**

在檔頭 docstring（:1-11）的 Usage 之後補一句，說明字串欄路由與新選項：

```
High-cardinality string columns (nunique > --max-string-cardinality, default 50)
are NOT emitted as categoricals — they are listed under a ``drop_columns:`` block,
because an un-encoded string feature becomes an object-dtype model feature and
OOMs training (consistency invariant B6). Review both blocks before copying.
```

- [ ] **Step 7: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/scripts/test_suggest_categorical_cols.py" -q`
Expected: PASS（新舊測試全綠）。

- [ ] **Step 8: Commit**

```bash
git -C "$WT" add scripts/suggest_categorical_cols.py tests/scripts/test_suggest_categorical_cols.py
git -C "$WT" commit -m "feat(suggest-categorical): route high-cardinality strings to drop_columns (B6)"
```

---

### Task 5: 文件同步（各依讀者角度）

**Files:**
- Modify: `docs/operations/training-oom-object-matrix.md`（已在 worktree，untracked）
- Modify: `docs/pipelines/dataset.md`
- Modify: `docs/operations/known-pitfalls.md`
- Modify: `README.md`

- [ ] **Step 1: 改寫 OOM 文件（讀者＝撞到失敗、手上有錯誤的工程師）**

重定位為「你收到這則錯誤 → 成因 → 怎麼辦」。具體：
- §1「現象」：把入口症狀從 `train.sh: line 5: 72 Killed` 改成讀者現在實際會看到的
  `DataConsistencyError: train_model_input feature columns include un-encoded
  non-numeric type(s) ...`（backstop 上線後）；保留舊 `Killed` 作為「pre-gate 舊 cached
  dataset 或跳過 backstop 時的樣子」的次要說明。
- §6「怎麼確認」：頂部加一句「training backstop 現在會在讀取前自動列出這些欄；本 snippet
  改為供你決定每欄該 declare 還是 drop 用」。
- 新增一小節「收到這則錯誤怎麼辦」：指向 §5 修法（declare→categorical_columns／
  drop→drop_columns），並標明這會 bump `base_dataset_version`、需重建 dataset（＝spec 的
  Phase 1）。措辭對齊程式碼真實錯誤訊息字串。

- [ ] **Step 2: 改 `docs/pipelines/dataset.md`（讀者＝寫 config 的人）**

- `:38`：把「先使用 `scripts/suggest_categorical_cols.py` 產生候選清單，再決定
  `categorical_columns`」補成「…候選清單（含高 cardinality 字串欄的 `drop_columns` 建議），
  再決定 `categorical_columns` 與 `drop_columns`」。
- 不變量清單（`:171-172` 附近，A1「同一欄不可同時 categorical 與 drop」之後）加一條：
  「(B6) 字串／非數值欄若要當特徵，必須列入 `categorical_columns`（會 integer-encode）；
  否則必須列入 `drop_columns`。兩者皆非 → dataset 建構第一個 node（`validate_data_consistency`）
  fail-fast，training 讀取時亦有 backstop。」

- [ ] **Step 3: 加 `known-pitfalls.md` 條目（讀者＝未來 session）**

在檔尾新增一節（照既有格式：症狀／根因／規則／驗證），指向 OOM 文件與 B6：

```markdown
## 8. 字串特徵欄靜默 → object 矩陣 OOM（已加 B6 閘，2026-07-11）

- **症狀（第一分鐘認出它）**：training `prepare_lgb_train_inputs` 在 `_pdf_to_X` 的
  `to_numpy` 步 `Killed`（OOM）；或（B6 上線後）在讀 parquet 前秒級 `DataConsistencyError:
  ... un-encoded non-numeric type(s)`。本機合成資料永不重現（合成 feature_table 無此欄）。
- **根因**：生產 feature_table 有字串欄，未宣告 `categorical_columns`、也未 `drop_columns`
  → `_compute_feature_columns` 收它為特徵 → `_encode_categoricals` 不編它 → `X_df.values`
  塌縮成 object 矩陣（每格 ~34 B vs float64 8 B，公司規模 22→96 GiB）。錯誤在 training
  （下游），根因在 dataset schema 設定（上游）。
- **規則**：字串特徵欄必須 declare categorical 或 drop。新增不變量走
  `core/consistency.py::nonnumeric_feature_errors`（B6），勿 ad-hoc 散落。修 config 會 bump
  `base_dataset_version`、需重建 dataset。詳見 `docs/operations/training-oom-object-matrix.md`。
- **驗證方式**：`python -c "import pyarrow.parquet as pq, pyarrow as pa; s=pq.read_schema('<train_model_input.parquet>'); print([f.name for f in s if pa.types.is_string(f.type)])"` 對照 preprocessor.json 的 `feature_columns`／`categorical_columns`；差集非空即中招。
```

（把「## 8」改成檔案當前最大節號 +1；先 `grep -c "^## " "$WT/docs/operations/known-pitfalls.md"` 確認。）

- [ ] **Step 4: README operations 索引補孤兒檔連結**

在 `README.md:461` 的 operations 表，troubleshooting／known-pitfalls 那一列（或新增一列）加：
`[training-oom-object-matrix.md](docs/operations/training-oom-object-matrix.md)`，一句話：
「training OOM（字串特徵欄 → object 矩陣）成因與修法」。

- [ ] **Step 5: fresh reader 通讀（品質閘）**

派一個 subagent，分別以「撞到失敗的生產工程師」與「第一次寫 dataset config 的人」身分，只讀
`training-oom-object-matrix.md` 與 `dataset.md` 改動段，回答「只有 repo、沒這段對話的人，照
這段能不能懂、知不知道下一步」。挑到不清楚處就修（memory `feedback_analysis_docs_handbook_style`）。

- [ ] **Step 6: 簡繁檢查 + Commit**

```bash
grep -lP '[产两内应实际约级别转优这说话让码尽后]' "$WT/docs/operations/training-oom-object-matrix.md" "$WT/docs/pipelines/dataset.md" "$WT/docs/operations/known-pitfalls.md" || echo "無簡體"
git -C "$WT" add docs/operations/training-oom-object-matrix.md docs/pipelines/dataset.md docs/operations/known-pitfalls.md README.md
git -C "$WT" commit -m "docs: B6 non-numeric feature gate — OOM guide reframe, dataset invariant, pitfall, README index"
```

---

## 收尾（全部 Task 完成後）

- [ ] **graphify rebuild**（改過 code）：
  `cd "$WT" && $PY -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
- [ ] **針對性迴歸**（只跑動過的測試檔，不跑全量）：
  `PYTHONPATH=$WT/src $PY -m pytest "$WT/tests/test_core/test_consistency.py" "$WT/tests/test_io/test_extract.py" "$WT/tests/scripts/test_suggest_categorical_cols.py" "$WT/tests/test_pipelines/test_dataset/test_nodes_spark.py" -q 2>&1 | tail -12`，與開工 baseline 對照，確認無新增 fail。
- [ ] **fresh-context 驗收**：派 code-reviewer subagent，附本計畫驗收條件與 `git -C "$WT" diff main..HEAD`，範圍＝讀 diff＋只跑上列測試檔＋spot-check，不重跑全量。
- [ ] Phase 0 明確**不做**：不改 config、不重建 dataset、不碰記憶體路徑、不 promote。當前生產 run
  的最終成功屬 Phase 1（需 backstop/snippet 給出的兇手欄名＋每欄領域判斷）。

---

## Self-Review（對照 spec）

- **Spec §4.1 predicate** → Task 1 ✓；**§4.2 dataset 閘** → Task 2 ✓；**§4.3 training fail-fast**
  → Task 3 ✓；**§4.4 suggest 腳本** → Task 4 ✓；**§4.5 文件（4 份＋legend＋script docstring）**
  → legend/​docstring 在 Task 1/4，其餘在 Task 5 ✓。**§6 測試策略（含 mutation 紀律、reader 通讀）**
  → 各 Task Step ＋ Task 5 Step 5 ✓。
- **型別一致**：`nonnumeric_feature_errors(feature_kinds, will_be_encoded)`、
  `spark_dtype_is_numeric(str)`、`suggest_categorical_columns_spark(...) -> (cats, drops, implicit, n_rows)`、
  `format_yaml_output(categorical, drop_suggestions)` 在各 Task 使用一致。
- **無 placeholder**：每個 code step 附完整 code；Spark/parameters fixture 處給明「先 grep 既有
  fixture」的具體指令而非空泛「setup」。
- **未涵蓋於本計畫（刻意）**：Phase 1（config 改＋重建）、Phase 2（Sequence/Arrow）— spec §7 gated。

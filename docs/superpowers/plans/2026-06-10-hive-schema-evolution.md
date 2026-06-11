# HiveTableDataset append-only schema evolution — 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `columns: "auto"` 的 Hive 表在既有表 schema 與 DataFrame 不一致時自動 append-only 演化（ALTER ADD COLUMNS／NULL 補欄／按表序投影），feature_table 加欄後 dataset pipeline 不再需要手動 drop 重建 `recsys_prod_*` 表。

**Architecture:** 只動 `src/recsys_tfb/io/hive_table_dataset.py`：`save()` 在「`columns: "auto"` 且表已存在」時走新的 `_evolve_schema()` 私有方法（diff 表 schema vs df schema → ALTER 加欄／typed-NULL 補欄／同名異型 fail-loud），`self._columns` 改以表 schema 為準使投影按表序。顯式 `columns:` 表與表不存在路徑零行為變更。設計細節與決策理由見 spec：`docs/superpowers/specs/2026-06-10-hive-schema-evolution-design.md`。

**Tech Stack:** PySpark 3.3.2、pytest（mock SparkSession ＋ conftest 真 `spark` fixture）。

**執行環境（每個 Bash 指令都遵守，理由見 CLAUDE.md Worktree SOP）：**

- Worktree root：`/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution`
- 跑測試一律：`cd <worktree-root> && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
- git 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution …`
- 本檔所有相對路徑都相對於 worktree root。

---

### Task 1: `_evolve_schema` — 單元測試與實作

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py`（`save()` 約 142–169 行、class docstring、新增 `_evolve_schema`）
- Test: `tests/test_io/test_hive_table_dataset.py`

**背景知識（實作前必讀）：**
- `save()` 現行流程：`columns: "auto"` 從當次 df 推 schema → `CREATE TABLE IF NOT EXISTS`（表已存在＝no-op）→ `df.select(投影)` → **位置式** `insertInto`。表已存在且 df 變寬 → AnalysisException。
- 欄名比對一律 lowercase（Hive 不分大小寫）。
- 型別比對用 Spark `simpleString()`（如 `"double"`、`"decimal(10,2)"`）；cast 用字串形式 `cast(f.dataType.simpleString())`，避免依賴 DataType 物件（mock 測試也因此可行）。
- 既有 `TestAutoInferColumns` 的 mock spark 上 `catalog.tableExists` 回傳 MagicMock（truthy），新分支會誤入演化路徑——該測試**必須**同步補 `tableExists.return_value = False`（行為定義變更，不是 bug 修補）。

- [ ] **Step 1: 寫失敗的單元測試**

在 `tests/test_io/test_hive_table_dataset.py` 檔尾新增：

```python
def _field(name: str, simple_type: str) -> MagicMock:
    f = MagicMock()
    f.name = name
    f.dataType.simpleString.return_value = simple_type
    return f


def _df_with_fields(*fields) -> MagicMock:
    df = MagicMock(name="DataFrame")
    df.schema.fields = list(fields)
    df.select.return_value = df
    df.withColumn.return_value = df
    df.select.return_value.distinct.return_value.collect.return_value = []
    writer = MagicMock()
    df.write.mode.return_value = writer
    return df


class TestSchemaEvolution:
    """columns: 'auto' 且表已存在時的 append-only 演化（spec D2）。"""

    def _make_ds(self) -> HiveTableDataset:
        return HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            columns="auto",
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )

    def _make_spark_with_table(self, *table_fields) -> MagicMock:
        spark = _make_spark_mock()
        spark.catalog.tableExists.return_value = True
        # 表 schema 含分區欄（spark.table 回傳完整 schema），演化邏輯須自行排除
        part_filter = _field("base_dataset_version", "string")
        part_col = _field("snap_date", "string")
        spark.table.return_value.schema.fields = (
            list(table_fields) + [part_filter, part_col]
        )
        return spark

    def test_new_df_column_triggers_alter_and_table_order_projection(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("score", "double"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"),
            _field("new_feat", "double"),
            _field("score", "double"),
            _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "new_feat", "score", "snap_date"]

        with _patch_spark(spark):
            self._make_ds().save(df)

        # 只發 ALTER，不發 CREATE
        sqls = [c[0][0] for c in spark.sql.call_args_list]
        assert len(sqls) == 1
        assert (
            "ALTER TABLE ml_recsys.train_model_input ADD COLUMNS "
            "(new_feat DOUBLE)" in sqls[0]
        )
        # 投影按表序：既有欄在前、新欄附加、再接分區欄
        df.select.assert_any_call(
            "cust_id", "score", "new_feat", "base_dataset_version", "snap_date"
        )

    def test_df_missing_column_filled_with_typed_null(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("dropped_feat", "double"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"), _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "snap_date"]

        with _patch_spark(spark), \
             patch("pyspark.sql.functions.lit") as mock_lit:
            null_col = MagicMock(name="NullCol")
            mock_lit.return_value.cast.return_value = null_col
            self._make_ds().save(df)

        mock_lit.assert_any_call(None)
        mock_lit.return_value.cast.assert_any_call("double")
        df.withColumn.assert_any_call("dropped_feat", null_col)
        # 缺欄不是錯誤，不發 ALTER 也不發 CREATE
        assert spark.sql.call_args_list == []

    def test_type_conflict_raises_value_error(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("score", "int"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"),
            _field("score", "double"),
            _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "score", "snap_date"]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="(?i)type conflict.*score"):
            self._make_ds().save(df)

    def test_column_name_case_difference_is_not_a_new_column(self):
        spark = self._make_spark_with_table(_field("CUST_ID", "string"))
        df = _df_with_fields(
            _field("cust_id", "string"), _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "snap_date"]

        with _patch_spark(spark):
            self._make_ds().save(df)

        assert spark.sql.call_args_list == []  # 無 ALTER、無 CREATE

    def test_explicit_columns_table_never_checks_existence(self):
        ds = HiveTableDataset(
            database="db",
            table="contract_table",
            columns=[{"name": "a", "type": "STRING"}],
            external=False,
        )
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        spark.catalog.tableExists.assert_not_called()
        # 既有契約路徑不變：CREATE IF NOT EXISTS 照發
        assert "CREATE TABLE IF NOT EXISTS" in spark.sql.call_args_list[0][0][0]
```

同檔修既有測試 `TestAutoInferColumns.test_columns_auto_infers_from_dataframe`：在
`with _patch_spark(spark):` 之前加一行——

```python
        spark.catalog.tableExists.return_value = False
```

並把檔頭 docstring 第一段改為：

```python
"""Tests for HiveTableDataset.

Most tests mock SparkSession because insertInto/catalog.tableExists require
a real Hive metastore. TestSchemaEvolutionIntegration uses the real local
`spark` fixture end-to-end.
"""
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_hive_table_dataset.py::TestSchemaEvolution -q
```

Expected: 5 failed（`test_explicit_columns_table_never_checks_existence` 可能 pass——它驗的是現狀；其餘 4 個必須 fail，多半是 `df.select` 投影斷言或 ALTER 斷言不成立）。

- [ ] **Step 3: 實作**

`src/recsys_tfb/io/hive_table_dataset.py` 兩處修改。

(a) `save()` 中間段，把現行的

```python
        if self._infer_columns and not self._columns:
            self._columns = _infer_columns_from_spark(
                df,
                exclude={c["name"] for c in self._partition_cols}
                | set(self._partition_filter.keys()),
            )

        self._ensure_table_exists(spark)
```

替換為：

```python
        if self._infer_columns and spark.catalog.tableExists(self._qualified_name):
            df = self._evolve_schema(spark, df)
        else:
            if self._infer_columns and not self._columns:
                self._columns = _infer_columns_from_spark(
                    df,
                    exclude={c["name"] for c in self._partition_cols}
                    | set(self._partition_filter.keys()),
                )
            self._ensure_table_exists(spark)
```

(b) 在 `_ensure_table_exists` 之前新增方法：

```python
    def _evolve_schema(self, spark, df):
        """Align an auto-schema DataFrame with the existing table (append-only).

        Policy mirrors source_etl's schema evolution, with one deliberate
        difference: a column the table has but the df lacks is NOT an error
        here — these tables are partition-versioned, so a newer version that
        dropped a feature legitimately writes NULL while older partitions
        keep their values. Same-name type conflicts fail loud: ANSI store
        assignment would silently narrow (e.g. double -> int).

        Side effects: may ALTER TABLE ADD COLUMNS; resets ``self._columns``
        to the table's (post-ALTER) non-partition column order so the
        positional insertInto projection follows the TABLE, not the df.
        """
        from pyspark.sql import functions as F

        part_lower = {c["name"].lower() for c in self._partition_cols} | {
            k.lower() for k in self._partition_filter
        }
        table_fields = [
            f
            for f in spark.table(self._qualified_name).schema.fields
            if f.name.lower() not in part_lower
        ]
        df_fields = [
            f for f in df.schema.fields if f.name.lower() not in part_lower
        ]
        df_types = {f.name.lower(): f.dataType.simpleString() for f in df_fields}

        conflicts = [
            (f.name, df_types[f.name.lower()], f.dataType.simpleString())
            for f in table_fields
            if f.name.lower() in df_types
            and df_types[f.name.lower()] != f.dataType.simpleString()
        ]
        if conflicts:
            detail = "; ".join(
                f"{name}: DataFrame={d} vs table={t}" for name, d, t in conflicts
            )
            raise ValueError(
                f"Type conflict writing to Hive table "
                f"'{self._qualified_name}' ({detail}). Schema evolution never "
                f"casts; fix the upstream dtype or rebuild the table."
            )

        table_lower = {f.name.lower() for f in table_fields}
        new_fields = [f for f in df_fields if f.name.lower() not in table_lower]
        if new_fields:
            cols_sql = ", ".join(
                f"{f.name} {f.dataType.simpleString().upper()}"
                for f in new_fields
            )
            logger.info(
                "Schema evolution on %s: ADD COLUMNS %s",
                self._qualified_name,
                [(f.name, f.dataType.simpleString()) for f in new_fields],
            )
            spark.sql(
                f"ALTER TABLE {self._qualified_name} ADD COLUMNS ({cols_sql})"
            )

        for f in table_fields:
            if f.name.lower() not in df_types:
                df = df.withColumn(
                    f.name, F.lit(None).cast(f.dataType.simpleString())
                )

        self._columns = [
            {"name": f.name, "type": f.dataType.simpleString().upper()}
            for f in table_fields + new_fields
        ]
        return df
```

(c) class docstring（`Read/write a Hive table…` 那段）末尾加一段：

```python
    For ``columns="auto"`` tables that already exist, the schema evolves
    append-only on save: new DataFrame columns are added via ALTER TABLE,
    columns the DataFrame lacks are written as typed NULLs, and same-name
    type conflicts raise. Explicitly declared ``columns`` are a contract
    and never evolve.
```

- [ ] **Step 4: 跑測試確認通過（含全檔回歸）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_hive_table_dataset.py -q
```

Expected: 44 passed（既有 39 ＋ 新 5），0 failed。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution commit -m "feat(io): HiveTableDataset auto 表 append-only schema evolution"
```

---

### Task 2: 真 Spark 整合測試（managed parquet 表 ALTER ＋ NULL 讀回）

**Files:**
- Test: `tests/test_io/test_hive_table_dataset.py`（檔尾新增 class）

**背景知識：**
- conftest 的 `spark` fixture 是真 local Spark session；`get_or_create_spark_session()` 會重用仍存活的 session，所以 `save()` 內部不 patch 也會拿到同一個 session——整合測試**不要** `_patch_spark`。
- 此測試同時驗證 spec 的三件事：表 schema 變寬、舊分區讀回新欄為 NULL（managed parquet 表 ALTER 後讀舊檔）、df 比表窄時 NULL 補欄真實可寫。

- [ ] **Step 1: 寫整合測試**

檔尾新增：

```python
class TestSchemaEvolutionIntegration:
    """Real-Spark end-to-end：ALTER 演化 + 舊分區 NULL 讀回 + 缺欄 NULL 寫入。"""

    def _make_ds(self, version: str) -> HiveTableDataset:
        return HiveTableDataset(
            database="evo_test",
            table="model_input",
            columns="auto",
            partition_filter={"base_dataset_version": version},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )

    def test_add_then_drop_column_across_versions(self, spark):
        spark.sql("CREATE DATABASE IF NOT EXISTS evo_test")
        spark.sql("DROP TABLE IF EXISTS evo_test.model_input")
        try:
            # v1：窄 schema 首寫建表
            narrow = spark.createDataFrame(
                [("c1", 0.5, "2024-01-31")], ["cust_id", "score", "snap_date"]
            )
            self._make_ds("v1").save(narrow)

            # v2：多一欄 → 觸發 ALTER
            wide = spark.createDataFrame(
                [("c2", 0.7, 1.0, "2024-01-31")],
                ["cust_id", "score", "new_feat", "snap_date"],
            )
            self._make_ds("v2").save(wide)

            table_cols = [
                f.name
                for f in spark.table("evo_test.model_input").schema.fields
            ]
            assert "new_feat" in table_cols

            v1_rows = self._make_ds("v1").load().collect()
            assert len(v1_rows) == 1
            assert v1_rows[0]["new_feat"] is None  # 舊分區讀回 NULL

            v2_rows = self._make_ds("v2").load().collect()
            assert v2_rows[0]["new_feat"] == 1.0

            # v3：比表窄的 df → NULL 補欄寫入
            narrow2 = spark.createDataFrame(
                [("c3", 0.9, "2024-01-31")], ["cust_id", "score", "snap_date"]
            )
            self._make_ds("v3").save(narrow2)
            v3_rows = self._make_ds("v3").load().collect()
            assert v3_rows[0]["new_feat"] is None
        finally:
            spark.sql("DROP TABLE IF EXISTS evo_test.model_input")
            spark.sql("DROP DATABASE IF EXISTS evo_test")
```

- [ ] **Step 2: 跑整合測試**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_hive_table_dataset.py::TestSchemaEvolutionIntegration -q
```

Expected: 1 passed（Task 1 已實作完，此測試是驗證、不是紅燈；首次跑含 Spark cold start 約 1–3 分鐘）。若 fail，先讀 stacktrace 區分「實作 bug」vs「local metastore 行為差異」，修實作後重跑 Task 1 全部單元測試確認沒退。

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution add tests/test_io/test_hive_table_dataset.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution commit -m "test(io): schema evolution 真 Spark 整合測試"
```

---

### Task 3: catalog 註解文件債

**Files:**
- Modify: `conf/base/catalog.yaml`（檔頭約 2–6 行、`enriched_eval_predictions` 上方約 249–251 行）

- [ ] **Step 1: 修檔頭 stale 註解**

把

```yaml
# 三層 versioning：base_dataset_version 對應「與抽樣無關」的不變產物；
# train_variant_id 對應「依 train sampling」的產物；calibration_variant_id
# 對應「依 calibration sampling」的產物。三層分別落在不同 HDFS path
# （base / train_variants / calibration_variants），table 名也帶各自的 version
# 後綴。
```

改為

```yaml
# 三層 versioning：base_dataset_version 對應「與抽樣無關」的不變產物；
# train_variant_id 對應「依 train sampling」的產物；calibration_variant_id
# 對應「依 calibration sampling」的產物。Hive 表為固定名稱，版本以
# partition_filter 的 partition column 區分；driver-local artifacts（如
# preprocessor.json）落在 data/dataset/${base_dataset_version}/ 版本化路徑。
```

- [ ] **Step 2: 修 enriched_eval_predictions 的待辦註解**

把

```yaml
# columns: "auto" ── schema 從 DataFrame 推得；若 segment_sources 設定在兩次
# run 之間改變，請先 drop 此 table 再重跑（ALTER TABLE schema evolution 是
# 待辦的 follow-up）。
```

改為

```yaml
# columns: "auto" ── schema 從 DataFrame 推得；segment_sources 設定在兩次 run
# 之間改變時，HiveTableDataset 會自動 append-only 演化（ALTER TABLE ADD
# COLUMNS／缺欄補 NULL），不需先 drop table。
```

- [ ] **Step 3: 驗證 catalog 仍可解析（跑 catalog 回歸測試）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_catalog.py tests/test_core/test_catalog_inference_entries.py -q
```

Expected: all passed（只改註解，任何 fail 都代表 YAML 改壞了）。

- [ ] **Step 4: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution add conf/base/catalog.yaml
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution commit -m "docs(catalog): 修 stale 版本化註解、schema evolution 待辦註解改已支援"
```

---

### Task 4: 收尾驗證

- [ ] **Step 1: 全檔測試最終回歸**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution && PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_hive_table_dataset.py -q
```

Expected: 45 passed（39 既有 ＋ 5 單元 ＋ 1 整合），0 failed。

- [ ] **Step 2: diff 對照 spec 檢查**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/hive-schema-evolution diff main..HEAD --stat
```

確認只動了：`src/recsys_tfb/io/hive_table_dataset.py`、`tests/test_io/test_hive_table_dataset.py`、`conf/base/catalog.yaml`、`docs/superpowers/{specs,plans}/…`。出現其他檔案＝越界，回頭查。

**完成定義：** 上述兩步皆綠；spec 的 D1–D5、測試計畫 6+1 條、文件債兩處全部有對應 commit。

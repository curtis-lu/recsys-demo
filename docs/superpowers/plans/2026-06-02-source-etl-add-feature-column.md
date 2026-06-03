# Source ETL feature_table 加欄位（schema evolution）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 source ETL 在 SELECT 新增欄位時，自動 `ALTER TABLE ADD COLUMNS` 並以「目標表欄序」對齊 `INSERT OVERWRITE` 寫進既有表；偵測到欄位被移除則 fail loud。

**Architecture:** 在 `sql_renderer.py` 加兩個純函式（`build_alter_add_columns`、`build_aligned_select_in_order`），在 `sql_runner.py` 的既有表寫入分支加 schema 對帳：比對 SELECT 欄 vs 既有表欄 → 新欄 ALTER、缺欄 raise，INSERT 投影一律照表欄序（既有欄按表序、新欄 append、partition 最後），確保 positional INSERT OVERWRITE 不錯位。

**Tech Stack:** Python 3.10, PySpark 3.3.2 (Hive), pytest 7.3.1。設計詳見 `docs/superpowers/specs/2026-06-02-source-etl-add-feature-column-design.md`。

**測試執行（worktree SOP）：** 一律用絕對 venv python + worktree 的 src：
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```
本計畫測試皆為純函式 / mock-Spark，秒級，**不需** data symlink、不碰真 Spark。

---

## File Structure

- **Modify** `src/recsys_tfb/pipelines/source_etl/sql_renderer.py`
  新增兩個 `@staticmethod`：`build_alter_add_columns`、`build_aligned_select_in_order`。`build_aligned_select` / `build_hive_ctas` / `build_insert_overwrite` 不動（CTAS 與 dry-run 仍用既有 SELECT 序）。
- **Modify** `src/recsys_tfb/pipelines/source_etl/sql_runner.py`
  既有表分支改走新 helper `_build_existing_table_statements`；`_process_single_table` 改成「probe → CTAS 或 existing-statements → 逐句執行」。
- **Modify** `tests/test_pipelines/test_source_etl/test_sql_renderer.py`
  新增 `TestBuildAlterAddColumns`、`TestBuildAlignedSelectInOrder`。
- **Modify** `tests/test_pipelines/test_source_etl/test_sql_runner.py`
  擴充 `_make_spark_mock`（wire `spark.table().schema` 與 `limit0_df.schema`）；新增 `TestSchemaEvolution`。

---

## Task 1: `build_alter_add_columns`（純函式）

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_renderer.py`
- Test: `tests/test_pipelines/test_source_etl/test_sql_renderer.py`

- [ ] **Step 1: 寫失敗測試**

在 `test_sql_renderer.py` 末尾新增：

```python
class TestBuildAlterAddColumns:
    def test_single_column(self):
        cfg = TableConfig(
            name="feature_table",
            sql_file="feature/feature_table.sql",
            partition_by={"snap_date": "DATE"},
        )
        out = SQLRenderer.build_alter_add_columns(cfg, [("new_feat", "double")], "ml_recsys")
        assert out == "ALTER TABLE ml_recsys.feature_table ADD COLUMNS (new_feat double)"

    def test_multiple_columns_preserve_order(self):
        cfg = TableConfig(
            name="feature_concat",
            sql_file="feature/feature_concat.sql",
            partition_by={"snap_date": "DATE"},
        )
        out = SQLRenderer.build_alter_add_columns(
            cfg, [("col_a", "double"), ("col_b", "string")], "ml_recsys"
        )
        assert "ADD COLUMNS (col_a double, col_b string)" in out

    def test_decimal_type(self):
        cfg = TableConfig(
            name="feature_table",
            sql_file="feature/feature_table.sql",
            partition_by={"snap_date": "DATE"},
        )
        out = SQLRenderer.build_alter_add_columns(cfg, [("amt", "decimal(10,2)")], "ml_recsys")
        assert "amt decimal(10,2)" in out
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/test_pipelines/test_source_etl/test_sql_renderer.py::TestBuildAlterAddColumns -q
```
Expected: FAIL（`AttributeError: ... has no attribute 'build_alter_add_columns'`）

- [ ] **Step 3: 實作**

在 `sql_renderer.py` 的 `SQLRenderer` class 內（`build_insert_overwrite` 之後）新增：

```python
    @staticmethod
    def build_alter_add_columns(
        table_config: TableConfig,
        new_columns: list[tuple[str, str]],
        target_db: str,
    ) -> str:
        """Assemble ``ALTER TABLE <db>.<name> ADD COLUMNS (c1 t1, c2 t2)``.

        ``new_columns`` is an ordered list of ``(name, hive_type)``; types come
        from the SELECT's inferred schema (``dataType.simpleString()``). Caller
        guarantees the list is non-empty. New columns are appended after existing
        non-partition columns (Hive ADD COLUMNS semantics).
        """
        cols = ", ".join(f"{name} {dtype}" for name, dtype in new_columns)
        return (
            f"ALTER TABLE {target_db}.{table_config.name} ADD COLUMNS ({cols})"
        )
```

- [ ] **Step 4: 跑測試確認通過**

Run（同 Step 2 指令）
Expected: PASS（3 passed）

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column add \
  src/recsys_tfb/pipelines/source_etl/sql_renderer.py \
  tests/test_pipelines/test_source_etl/test_sql_renderer.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column commit -q \
  -m "feat(source-etl): build_alter_add_columns for schema evolution"
```

---

## Task 2: `build_aligned_select_in_order`（純函式，按表欄序投影）

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_renderer.py`
- Test: `tests/test_pipelines/test_source_etl/test_sql_renderer.py`

- [ ] **Step 1: 寫失敗測試**

在 `test_sql_renderer.py` 末尾新增：

```python
class TestBuildAlignedSelectInOrder:
    def test_projects_in_target_order_not_select_order(self):
        # SELECT 序是 col_b, col_a；目標表序是 col_a, col_b → 投影須照表序
        out = SQLRenderer.build_aligned_select_in_order(
            select_sql="SELECT col_b, col_a, snap_date FROM t",
            select_columns=["col_b", "col_a", "snap_date"],
            target_nonpartition_order=["col_a", "col_b"],
            partition_by={"snap_date": "DATE"},
        )
        assert out.index("col_a") < out.index("col_b")
        assert "CAST(snap_date AS DATE) AS snap_date" in out

    def test_new_column_appended_last(self):
        out = SQLRenderer.build_aligned_select_in_order(
            select_sql="SELECT col_a, col_new, snap_date FROM t",
            select_columns=["col_a", "col_new", "snap_date"],
            target_nonpartition_order=["col_a", "col_new"],
            partition_by={"snap_date": "DATE"},
        )
        assert out.index("col_a") < out.index("col_new")
        assert out.index("col_new") < out.index("CAST(snap_date")

    def test_multi_partition_casts_in_config_order(self):
        out = SQLRenderer.build_aligned_select_in_order(
            select_sql="SELECT col_a, prod_name, snap_date FROM t",
            select_columns=["col_a", "prod_name", "snap_date"],
            target_nonpartition_order=["col_a"],
            partition_by={"prod_name": "STRING", "snap_date": "DATE"},
        )
        assert out.index("CAST(prod_name") < out.index("CAST(snap_date")

    def test_missing_partition_raises(self):
        with pytest.raises(ValueError, match="Partition columns missing"):
            SQLRenderer.build_aligned_select_in_order(
                select_sql="SELECT col_a FROM t",
                select_columns=["col_a"],
                target_nonpartition_order=["col_a"],
                partition_by={"snap_date": "DATE"},
            )
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/test_pipelines/test_source_etl/test_sql_renderer.py::TestBuildAlignedSelectInOrder -q
```
Expected: FAIL（`AttributeError: ... 'build_aligned_select_in_order'`）

- [ ] **Step 3: 實作**

在 `sql_renderer.py` 的 `SQLRenderer` class 內（`build_aligned_select` 之後）新增。注意 partition 驗證訊息與既有 `build_aligned_select` 一致（"Partition columns missing from SELECT output"）：

```python
    @staticmethod
    def build_aligned_select_in_order(
        select_sql: str,
        select_columns: list[str],
        target_nonpartition_order: list[str],
        partition_by: dict[str, str],
    ) -> str:
        """Like :meth:`build_aligned_select`, but project non-partition columns in
        an explicit *target* order (the existing table's column order) rather than
        the SELECT's own order.

        Used for INSERT OVERWRITE into an existing table: Spark 3.3 INSERT is
        positional, so the projection must match the table's column layout
        (existing columns in table order, newly-added columns appended last,
        partition columns cast last). Validates that every partition column is
        present in the SELECT output (same rule/message as build_aligned_select).
        """
        body = SQLRenderer.strip_header_comments(select_sql)
        part_lower = {k.lower(): (k, v) for k, v in partition_by.items()}
        select_lower = {c.lower(): c for c in select_columns}

        missing = [k for k in part_lower if k not in select_lower]
        if missing:
            raise ValueError(
                f"Partition columns missing from SELECT output: {missing}. "
                f"SELECT has: {select_columns}"
            )

        partition_casts = [
            f"CAST({select_lower[pk.lower()]} AS {dtype}) AS {name}"
            for pk, (name, dtype) in (
                (pk, part_lower[pk.lower()]) for pk in partition_by
            )
        ]
        projection = ",\n    ".join(
            list(target_nonpartition_order) + partition_casts
        )
        return f"SELECT\n    {projection}\nFROM (\n{body}\n) _aligned"
```

- [ ] **Step 4: 跑測試確認通過**

Run（同 Step 2 指令）
Expected: PASS（4 passed）

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column add \
  src/recsys_tfb/pipelines/source_etl/sql_renderer.py \
  tests/test_pipelines/test_source_etl/test_sql_renderer.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column commit -q \
  -m "feat(source-etl): build_aligned_select_in_order (project by table column order)"
```

---

## Task 3: Runner schema-evolution 整合

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:215-244`（`_process_single_table` real-run 區塊）+ 新增 `_build_existing_table_statements`
- Test: `tests/test_pipelines/test_source_etl/test_sql_runner.py`

- [ ] **Step 1: 擴充 `_make_spark_mock` 並寫新測試（先讓既有測試仍綠 + 新測試紅）**

在 `test_sql_runner.py` 頂部 import 加上 pyspark 型別：

```python
from pyspark.sql.types import StructField, StructType, StringType
```

把 `_make_spark_mock` 整個替換為（新增 `existing_columns` 參數；wire `limit0_df.schema` 與 `spark.table().schema`）：

```python
def _make_spark_mock(columns=None, table_exists=False, existing_columns=None):
    cols = columns or ["cust_id", "total_aum", "snap_date"]
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    limit0_df = MagicMock()
    limit0_df.columns = cols
    limit0_df.schema = StructType([StructField(c, StringType()) for c in cols])
    # Existing table schema (only consulted when table_exists). Defaults to the
    # SELECT columns so no evolution is detected for legacy tests.
    et_cols = existing_columns if existing_columns is not None else cols
    spark.table.return_value.schema = StructType(
        [StructField(c, StringType()) for c in et_cols]
    )
    _call_count = [0]

    def _side_effect(*args, **kwargs):
        _call_count[0] += 1
        if _call_count[0] == 1:
            return limit0_df
        return MagicMock()

    spark.sql.side_effect = _side_effect
    return spark
```

在檔案末尾新增新測試類別：

```python
class TestSchemaEvolution:
    def _feature_aum_config(self):
        return {
            "variables": {"target_db": "ml_feature"},
            "tables": [
                {
                    "name": "feature_aum",
                    "sql_file": "feature/feature_aum.sql",
                    "partition_by": {"snap_date": "DATE"},
                    "primary_key": ["snap_date", "cust_id"],
                }
            ],
        }

    def test_new_column_triggers_alter_then_insert_in_table_order(self, tmp_path, sql_dir):
        # 既有表: cust_id, total_aum, snap_date(part)；SELECT 多了 sav_amt
        spark = _make_spark_mock(
            columns=["cust_id", "total_aum", "sav_amt", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
            runner.run(["2026-03-31"])

        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        alter = [s for s in executed if "ALTER TABLE" in s]
        insert = [s for s in executed if "INSERT OVERWRITE" in s]
        assert alter, "ALTER TABLE not executed for new column"
        assert "ADD COLUMNS (sav_amt string)" in alter[0]
        assert "ml_feature.feature_aum" in alter[0]
        assert insert, "INSERT OVERWRITE not executed"
        # 投影照表欄序：既有欄在前、新欄 append 在後
        assert insert[0].index("total_aum") < insert[0].index("sav_amt")
        # ALTER 必須在 INSERT 之前執行
        assert executed.index(alter[0]) < executed.index(insert[0])

    def test_no_alter_when_columns_unchanged(self, tmp_path, sql_dir):
        spark = _make_spark_mock(
            columns=["cust_id", "total_aum", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
            runner.run(["2026-03-31"])
        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        assert not any("ALTER TABLE" in s for s in executed)
        assert any("INSERT OVERWRITE" in s for s in executed)

    def test_removed_column_fails_loud_no_write(self, tmp_path, sql_dir):
        # SELECT 缺了既有表的 total_aum → 必須擋下，不可 ALTER/INSERT
        spark = _make_spark_mock(
            columns=["cust_id", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
            with pytest.raises(SourceETLError, match="Removing columns"):
                runner.run(["2026-03-31"])
        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        assert not any("INSERT OVERWRITE" in s for s in executed)
        assert not any("ALTER TABLE" in s for s in executed)
```

- [ ] **Step 2: 跑測試確認新測試失敗、既有測試仍綠**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/test_pipelines/test_source_etl/test_sql_runner.py -q
```
Expected: `TestSchemaEvolution` 3 個 FAIL（new col 未觸發 ALTER / removed 未擋）；其餘既有測試（含 `TestProcessSingleTableExistingRun`、`TestProcessSingleTableMissingPartition`、`TestFailFast`）**仍 PASS**。
若既有測試掛掉，先修 `_make_spark_mock` 再繼續。

- [ ] **Step 3: 實作 runner 變更**

在 `sql_runner.py` 把 `_process_single_table` 的 real-run 區塊（目前 `# Real run:` 到 `spark.sql(final_sql)` 那段，約 215-244 行）替換為：

```python
        # Real run: probe SELECT columns, then CTAS (new table) or schema-aware
        # INSERT OVERWRITE (existing table).
        table_start = time.monotonic()
        try:
            body = SQLRenderer.strip_header_comments(select_sql)
            probe = spark.sql(f"SELECT * FROM (\n{body}\n) _cols LIMIT 0")
            select_columns = probe.columns

            if not spark.catalog.tableExists(table.name, self._target_db):
                logger.info(
                    "Table %s.%s not found, creating via Hive CTAS",
                    self._target_db, table.name,
                )
                aligned_select = SQLRenderer.build_aligned_select(
                    select_sql, select_columns, table.partition_by
                )
                statements = [
                    SQLRenderer.build_hive_ctas(
                        table, aligned_select, self._target_db
                    )
                ]
            else:
                statements = self._build_existing_table_statements(
                    spark, table, select_sql, select_columns, probe
                )

            final_sql = "\n;\n".join(statements)
            if self._rendered_sql_dir:
                self._write_rendered_sql(run_id, snap_date, table.name, final_sql)

            logger.info("Executing %s ...", table.name)
            for stmt in statements:
                spark.sql(stmt)
            duration = time.monotonic() - table_start
            logger.info("Completed %s in %.1fs", table.name, duration)
```

（其下的 `except Exception as exc:` 區塊不動。）

接著在 `_process_single_table` 方法**之後**新增 helper method：

```python
    def _build_existing_table_statements(
        self, spark, table, select_sql, select_columns, probe
    ) -> list[str]:
        """Reconcile the rendered SELECT against the existing table schema.

        Returns the ordered SQL statements to execute: an optional
        ``ALTER TABLE ADD COLUMNS`` (when the SELECT introduces new non-partition
        columns) followed by an ``INSERT OVERWRITE`` whose projection follows the
        TABLE's column order (existing columns first, new columns appended,
        partition columns cast last) so the positional insert lands correctly.

        Fail-loud (append-only policy): if the SELECT drops a column that the
        existing table has, raise — removing columns needs a versioned rebuild,
        not in-place overwrite.
        """
        fqn = f"{self._target_db}.{table.name}"
        part_lower = {k.lower() for k in table.partition_by}
        existing_nonpart = [
            f.name
            for f in spark.table(fqn).schema.fields
            if f.name.lower() not in part_lower
        ]
        existing_lower = {c.lower() for c in existing_nonpart}
        select_nonpart = [c for c in select_columns if c.lower() not in part_lower]
        select_lower = {c.lower() for c in select_nonpart}

        removed = [c for c in existing_nonpart if c.lower() not in select_lower]
        if removed:
            raise SourceETLError(
                f"Removing columns from existing table {fqn} is not supported in "
                f"source ETL: it breaks positional INSERT OVERWRITE and deployed "
                f"models. Removed columns: {removed}. Use a versioned rebuild "
                f"instead, or keep the column and exclude it downstream via "
                f"prepare_model_input.drop_columns."
            )

        new_cols = [c for c in select_nonpart if c.lower() not in existing_lower]
        statements: list[str] = []
        if new_cols:
            type_by_lower = {
                f.name.lower(): f.dataType.simpleString()
                for f in probe.schema.fields
            }
            new_with_types = [(c, type_by_lower[c.lower()]) for c in new_cols]
            logger.info(
                "Schema evolution on %s: ADD COLUMNS %s", fqn, new_with_types
            )
            statements.append(
                SQLRenderer.build_alter_add_columns(
                    table, new_with_types, self._target_db
                )
            )

        target_order = existing_nonpart + new_cols
        aligned_select = SQLRenderer.build_aligned_select_in_order(
            select_sql, select_columns, target_order, table.partition_by
        )
        statements.append(
            SQLRenderer.build_insert_overwrite(
                table, aligned_select, self._target_db
            )
        )
        return statements
```

- [ ] **Step 4: 跑整個 source_etl 測試確認全綠**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/test_pipelines/test_source_etl/ -q
```
Expected: PASS（含 `TestSchemaEvolution` 3 個 + 既有全部）

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column add \
  src/recsys_tfb/pipelines/source_etl/sql_runner.py \
  tests/test_pipelines/test_source_etl/test_sql_runner.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column commit -q \
  -m "feat(source-etl): schema-evolution write path (ADD COLUMNS + table-order insert)"
```

---

## Task 4: 收尾驗證 + graphify 同步

**Files:** 無新檔；驗證 + 知識圖更新。

- [ ] **Step 1: 跑相關測試全集（source_etl + renderer）確認綠**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/test_pipelines/test_source_etl/ -q
```
Expected: all PASS

- [ ] **Step 2: 確認沒有破壞 dry-run 與 SQL 檔（人工檢視 diff）**

Run:
```
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column diff main -- \
  src/recsys_tfb/pipelines/source_etl/
```
確認：`build_aligned_select` / `build_hive_ctas` / `build_insert_overwrite` 未被改；dry-run 區塊未被改。

- [ ] **Step 3: graphify 圖同步（CLAUDE.md 規定改 code 後執行）**

Run（在 worktree root）:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
"from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 4: 最終 commit（若 graphify 產物有變動且被追蹤）**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column status --short
# 若有 graphify-out 變動且需 commit：
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column add -A
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-etl-add-column commit -q \
  -m "chore: sync graphify graph after source-etl schema evolution"
```

---

## 完成準則（Definition of Done）

- `tests/test_pipelines/test_source_etl/` 全綠，含新增 `TestBuildAlterAddColumns` / `TestBuildAlignedSelectInOrder` / `TestSchemaEvolution`。
- SELECT 新增欄位 → 既有表自動 `ALTER ADD COLUMNS` + 按表欄序 `INSERT OVERWRITE`。
- SELECT 缺既有欄 → `SourceETLError`（fail loud），不寫表。
- CTAS（首次建表）、dry-run、既有 `build_*` 純函式行為不變。
- **未做（spec 明列 deferred）**：版本化 / 改邏輯偵測 / 既有欄型別變更 / 物理減欄 / 歷史回填。

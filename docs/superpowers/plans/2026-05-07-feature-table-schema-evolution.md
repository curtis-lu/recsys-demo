# feature_table Schema Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 加一個 `python -m recsys_tfb migrate_schema` 兩段式 CLI（dry-run 預設 + `--apply`），並在 dataset pipeline 第一個 Node 做 fail-fast schema 比對，讓 user 在 `feature_table` 加新欄位時能明確同步 6 張下游 Hive 表的物理 schema，無 silent ALTER。

**Architecture:** 純函式核心 `src/recsys_tfb/io/schema_evolution.py` 提供 `SchemaDiff` / `diff_schema` / `plan_migrations` / `apply_migrations` 等 building block。`HiveTableDataset` 接受 `tracks_feature_table_schema` kwarg，`catalog.yaml` 對 6 張下游表（`preprocessed_feature_table` + 5 張 `*_model_input`）標註 `tracks_feature_table_schema: true`。dataset pipeline `create_pipeline()` 多接 `tracked_fqns: list[str]`，把它 closure 進新增的第一個 Node `verify_feature_table_schema`，後續節點原本 input `feature_table` 的改成 input `feature_table_verified`（純 passthrough sentinel）。`__main__.py` 加 `migrate_schema` thin command。

**Tech Stack:** Python 3.10、PySpark 3.3.2、Typer 0.20.1、pytest 7.3.1。Spark 互動全部 mock，不依賴 dev-cluster；最終以 manual checklist 跑 dev-cluster 驗收。

**Detection scope（簡化說明）：** verify Node 與 `migrate_schema` 只偵測 (a) 新增欄位、(b) 同名異型 type_changed。對「拿掉欄位」與「重命名」**不**主動 fail —— 因 `feature_table_fingerprint`（PR #2）已讓不同 schema 落到不同 `base_dataset_version` partition，舊 partition 的舊欄位閒置但無害。Spec 原始描述偏保守，這次實作以 fingerprint 隔離為前提收斂偵測範圍，對應在 Task 1 design rationale 註明。

---

## File Structure

| 檔案 | 動作 | 責任 |
|---|---|---|
| `src/recsys_tfb/io/schema_evolution.py` | Create | `SchemaDiff` dataclass、`IncompatibleSchemaChangeError`、`FeatureTableSchemaOutOfSync`、`diff_schema`（純）、`format_plan`（純）、`fetch_table_columns`、`plan_migrations`、`apply_migrations`、`collect_tracked_table_fqns` |
| `src/recsys_tfb/io/hive_table_dataset.py` | Modify | `__init__` 接受 `tracks_feature_table_schema: bool = False` kwarg，存成 attribute |
| `conf/base/catalog.yaml` | Modify | 6 張表加 `tracks_feature_table_schema: true` |
| `src/recsys_tfb/pipelines/dataset/_verify.py` | Create | `_compute_feature_table_expected` helper、`make_verify_node_func(tracked_fqns)` factory |
| `src/recsys_tfb/pipelines/dataset/pipeline.py` | Modify | `create_pipeline()` 加 `tracked_fqns: list[str] | None = None` 參數；非空時插入 verify Node 為第一個；改 `fit_preprocessor_metadata` / `apply_preprocessor_to_features` 兩節點 input 由 `feature_table` 改為 `feature_table_verified` |
| `src/recsys_tfb/__main__.py` | Modify | `dataset()` 計算 `tracked_fqns` 注入 `pipeline_kwargs`；新增 `migrate_schema` 子指令 |
| `tests/test_io/test_schema_evolution.py` | Create | 純函式 + spark-mock 測試 |
| `tests/test_io/test_hive_table_dataset.py` | Modify | 加 `tracks_feature_table_schema` flag round-trip 測試 |
| `tests/test_pipelines/test_dataset/test_pipeline.py` | Modify | 加 verify Node 注入測試（含/不含 tracked_fqns 兩案） |
| `tests/test_pipelines/test_dataset/test_verify.py` | Create | verify Node 行為測試（pass/raise） |
| `tests/test_cli.py` | Modify | 加 `migrate_schema` dry-run / `--apply` / no-diff / incompatible 4 個 case |

---

## Task 1: `schema_evolution.py` 純函式核心（TDD）

**Files:**
- Create: `src/recsys_tfb/io/schema_evolution.py`
- Test: `tests/test_io/test_schema_evolution.py`

設計選擇：`diff_schema` 只關心 `expected` 列表中的每個欄位是否在 `actual` 出現且型別一致。`actual` 中存在但 `expected` 沒有的欄位（identity / label / partition / 舊版遺留欄位）一律忽略。理由：fingerprint 機制讓舊欄位在新 base_v 下自然失效，不需 verify Node 幫 user 清理。

- [ ] **Step 1.1: 寫 `SchemaDiff` 失敗測試**

建立 `tests/test_io/test_schema_evolution.py`：

```python
"""Tests for src/recsys_tfb/io/schema_evolution.py."""

import pytest

from recsys_tfb.io.schema_evolution import (
    FeatureTableSchemaOutOfSync,
    IncompatibleSchemaChangeError,
    SchemaDiff,
    diff_schema,
    format_plan,
)


class TestSchemaDiff:
    def test_empty_diff_is_compatible_and_empty(self):
        d = SchemaDiff(table_fqn="db.t", added=[], type_changed=[])
        assert d.is_empty
        assert d.is_compatible

    def test_added_only_is_compatible_but_not_empty(self):
        d = SchemaDiff(
            table_fqn="db.t",
            added=[("foo", "double")],
            type_changed=[],
        )
        assert not d.is_empty
        assert d.is_compatible

    def test_type_changed_makes_incompatible(self):
        d = SchemaDiff(
            table_fqn="db.t",
            added=[],
            type_changed=[("aum", "double", "string")],
        )
        assert not d.is_empty
        assert not d.is_compatible
```

- [ ] **Step 1.2: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py -v
```

Expected: `ImportError: cannot import name 'SchemaDiff' ...`

- [ ] **Step 1.3: 實作 `SchemaDiff` + 兩個 Exception**

建立 `src/recsys_tfb/io/schema_evolution.py`：

```python
"""Hive output table schema evolution helpers.

Used by the dataset pipeline's verify Node (fail-fast at run start) and the
``migrate_schema`` CLI subcommand (dry-run plan + ``--apply`` execute).
Detection scope is intentionally narrow: only ``added`` columns and
``type_changed`` (same name, different dtype) are considered. Removed and
renamed columns are tolerated because :func:`compute_feature_table_fingerprint`
isolates each schema generation into its own ``base_dataset_version`` partition.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaDiff:
    table_fqn: str
    added: list[tuple[str, str]]
    type_changed: list[tuple[str, str, str]]  # (name, actual_dtype, expected_dtype)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.type_changed)

    @property
    def is_compatible(self) -> bool:
        return not self.type_changed


class IncompatibleSchemaChangeError(RuntimeError):
    """Raised when schema diff contains type-changed columns (same name, different dtype)."""


class FeatureTableSchemaOutOfSync(RuntimeError):
    """Raised by the verify Node when one or more tracked tables need migration."""
```

- [ ] **Step 1.4: 跑測試確認 SchemaDiff 通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestSchemaDiff -v
```

Expected: 3 PASS

- [ ] **Step 1.5: 寫 `diff_schema` 失敗測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
class TestDiffSchema:
    def test_actual_none_returns_empty_diff(self):
        d = diff_schema("db.t", expected=[("foo", "double")], actual=None)
        assert d.is_empty

    def test_aligned_schemas_return_empty_diff(self):
        d = diff_schema(
            "db.t",
            expected=[("foo", "double"), ("bar", "string")],
            actual=[("foo", "double"), ("bar", "string"), ("snap_date", "string")],
        )
        assert d.is_empty
        assert d.is_compatible

    def test_added_columns_in_expected_not_in_actual(self):
        d = diff_schema(
            "db.t",
            expected=[("foo", "double"), ("new_feat", "double")],
            actual=[("foo", "double"), ("snap_date", "string")],
        )
        assert d.added == [("new_feat", "double")]
        assert d.type_changed == []

    def test_type_changed_for_same_name_diff_dtype(self):
        d = diff_schema(
            "db.t",
            expected=[("aum", "string")],
            actual=[("aum", "double"), ("snap_date", "string")],
        )
        assert d.added == []
        assert d.type_changed == [("aum", "double", "string")]
        assert not d.is_compatible

    def test_actual_extras_ignored(self):
        # legacy columns / identity / label / partition cols not in expected → ignored
        d = diff_schema(
            "db.t",
            expected=[("foo", "double")],
            actual=[("foo", "double"), ("legacy_col", "int"), ("label", "int")],
        )
        assert d.is_empty

    def test_added_and_type_changed_simultaneously(self):
        d = diff_schema(
            "db.t",
            expected=[("aum", "string"), ("new_feat", "double")],
            actual=[("aum", "double")],
        )
        assert d.added == [("new_feat", "double")]
        assert d.type_changed == [("aum", "double", "string")]
```

- [ ] **Step 1.6: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestDiffSchema -v
```

Expected: ImportError or NameError on `diff_schema`

- [ ] **Step 1.7: 實作 `diff_schema`**

加到 `src/recsys_tfb/io/schema_evolution.py`：

```python
def diff_schema(
    table_fqn: str,
    expected: list[tuple[str, str]],
    actual: list[tuple[str, str]] | None,
) -> SchemaDiff:
    """Compare per-column expected vs actual.

    For each ``(name, dtype)`` in *expected*:
    - name missing in *actual* → ``added``
    - name present with different dtype → ``type_changed``

    Columns that exist only in *actual* are ignored (identity / label /
    partition columns are out of scope; legacy columns from prior schemas
    persist harmlessly).

    Args:
        table_fqn: Fully-qualified Hive table name (e.g. ``"ml_recsys.train_model_input"``).
        expected: Ordered list of ``(name, dtype_simple_string)`` representing
            the feature_table column subset that should appear in *actual*.
        actual: Current ``(name, dtype)`` list from the Hive table, or ``None``
            if the table does not yet exist.

    Returns:
        :class:`SchemaDiff` (frozen).
    """
    if actual is None:
        return SchemaDiff(table_fqn=table_fqn, added=[], type_changed=[])
    actual_map = dict(actual)
    added: list[tuple[str, str]] = []
    type_changed: list[tuple[str, str, str]] = []
    for name, dtype in expected:
        if name not in actual_map:
            added.append((name, dtype))
        elif actual_map[name] != dtype:
            type_changed.append((name, actual_map[name], dtype))
    return SchemaDiff(table_fqn=table_fqn, added=added, type_changed=type_changed)
```

- [ ] **Step 1.8: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestDiffSchema -v
```

Expected: 6 PASS

- [ ] **Step 1.9: 寫 `format_plan` 失敗測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
class TestFormatPlan:
    def test_no_diffs_returns_no_op_message(self):
        assert format_plan([]) == "(no schema changes)"

    def test_skips_empty_diffs(self):
        empty = SchemaDiff("db.t1", added=[], type_changed=[])
        assert format_plan([empty]) == "(no schema changes)"

    def test_added_renders_alter_table_block(self):
        d = SchemaDiff(
            "ml_recsys.train_model_input",
            added=[("foo", "double"), ("bar", "string")],
            type_changed=[],
        )
        s = format_plan([d])
        assert "ml_recsys.train_model_input" in s
        assert "ALTER TABLE" in s and "ADD COLUMNS" in s and "CASCADE" in s
        assert "foo double" in s
        assert "bar string" in s

    def test_type_changed_renders_incompatible_block(self):
        d = SchemaDiff(
            "ml_recsys.train_model_input",
            added=[],
            type_changed=[("aum", "double", "string")],
        )
        s = format_plan([d])
        assert "INCOMPATIBLE" in s
        assert "aum: double -> string" in s
```

- [ ] **Step 1.10: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestFormatPlan -v
```

Expected: NameError on `format_plan`

- [ ] **Step 1.11: 實作 `format_plan`**

加到 `src/recsys_tfb/io/schema_evolution.py`：

```python
def format_plan(diffs: list[SchemaDiff]) -> str:
    """Render a list of diffs as a human-readable migration plan."""
    lines: list[str] = []
    for d in diffs:
        if d.is_empty:
            continue
        lines.append(f"\n=== {d.table_fqn} ===")
        if d.added:
            cols_sql = ", ".join(f"{n} {t}" for n, t in d.added)
            lines.append(
                f"  ALTER TABLE {d.table_fqn} ADD COLUMNS ({cols_sql}) CASCADE"
            )
        if d.type_changed:
            lines.append("  INCOMPATIBLE type_changed (manual DROP + rebuild required):")
            for name, old, new in d.type_changed:
                lines.append(f"    {name}: {old} -> {new}")
    return "\n".join(lines) if lines else "(no schema changes)"
```

- [ ] **Step 1.12: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py -v
```

Expected: 13 PASS（SchemaDiff 3 + DiffSchema 6 + FormatPlan 4）

- [ ] **Step 1.13: Commit**

```bash
git add src/recsys_tfb/io/schema_evolution.py tests/test_io/test_schema_evolution.py
git commit -m "feat(io): SchemaDiff + diff_schema + format_plan for schema evolution"
```

---

## Task 2: `schema_evolution.py` Spark 互動 + catalog 抽取（TDD）

**Files:**
- Modify: `src/recsys_tfb/io/schema_evolution.py`
- Modify: `tests/test_io/test_schema_evolution.py`

- [ ] **Step 2.1: 寫 `fetch_table_columns` 測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
from unittest.mock import MagicMock

from recsys_tfb.io.schema_evolution import fetch_table_columns


def _mock_spark_with_table(fqn, columns):
    """Mock a SparkSession where spark.catalog.tableExists(fqn)=True and
    spark.table(fqn).schema.fields returns mocks with .name + .dataType.simpleString()."""
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    fields = []
    for name, dtype in columns:
        f = MagicMock()
        f.name = name
        f.dataType.simpleString.return_value = dtype
        fields.append(f)
    spark.table.return_value.schema.fields = fields
    return spark


class TestFetchTableColumns:
    def test_returns_none_when_table_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert fetch_table_columns(spark, "db.t") is None
        spark.catalog.tableExists.assert_called_once_with("db.t")

    def test_returns_columns_with_dtypes(self):
        spark = _mock_spark_with_table("db.t", [("foo", "double"), ("bar", "string")])
        assert fetch_table_columns(spark, "db.t") == [
            ("foo", "double"),
            ("bar", "string"),
        ]
```

- [ ] **Step 2.2: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestFetchTableColumns -v
```

Expected: NameError on `fetch_table_columns`

- [ ] **Step 2.3: 實作 `fetch_table_columns`**

加到 `src/recsys_tfb/io/schema_evolution.py`（檔尾，純函式區後）：

```python
def fetch_table_columns(spark, fqn: str) -> list[tuple[str, str]] | None:
    """Read ``(name, dtype_simple_string)`` for a Hive table, or return ``None``
    if the table does not exist."""
    if not spark.catalog.tableExists(fqn):
        return None
    return [
        (f.name, f.dataType.simpleString())
        for f in spark.table(fqn).schema.fields
    ]
```

- [ ] **Step 2.4: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestFetchTableColumns -v
```

Expected: 2 PASS

- [ ] **Step 2.5: 寫 `plan_migrations` 測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
from recsys_tfb.io.schema_evolution import plan_migrations


class TestPlanMigrations:
    def test_returns_diffs_for_each_request(self):
        spark = MagicMock()

        def _table_exists(fqn):
            return fqn in ("db.t1", "db.t2")
        spark.catalog.tableExists.side_effect = _table_exists

        def _table(fqn):
            obj = MagicMock()
            if fqn == "db.t1":
                fields = []
                f1 = MagicMock(); f1.name = "foo"; f1.dataType.simpleString.return_value = "double"
                fields.append(f1)
            else:  # db.t2
                fields = []
                f1 = MagicMock(); f1.name = "foo"; f1.dataType.simpleString.return_value = "double"
                f2 = MagicMock(); f2.name = "bar"; f2.dataType.simpleString.return_value = "string"
                fields.extend([f1, f2])
            obj.schema.fields = fields
            return obj
        spark.table.side_effect = _table

        diffs = plan_migrations(
            requests=[
                ("db.t1", [("foo", "double"), ("bar", "string")]),  # bar added
                ("db.t2", [("foo", "double"), ("bar", "string")]),  # aligned
            ],
            spark=spark,
        )
        assert len(diffs) == 2
        assert diffs[0].added == [("bar", "string")]
        assert diffs[1].is_empty

    def test_raises_on_incompatible_change(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        f = MagicMock(); f.name = "aum"; f.dataType.simpleString.return_value = "double"
        spark.table.return_value.schema.fields = [f]

        with pytest.raises(IncompatibleSchemaChangeError) as exc_info:
            plan_migrations(
                requests=[("db.t", [("aum", "string")])],
                spark=spark,
            )
        assert "aum: double -> string" in str(exc_info.value)

    def test_missing_table_yields_empty_diff(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        diffs = plan_migrations(
            requests=[("db.t", [("foo", "double")])],
            spark=spark,
        )
        assert len(diffs) == 1
        assert diffs[0].is_empty
```

- [ ] **Step 2.6: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestPlanMigrations -v
```

Expected: NameError on `plan_migrations`

- [ ] **Step 2.7: 實作 `plan_migrations`**

加到 `src/recsys_tfb/io/schema_evolution.py`：

```python
def plan_migrations(
    requests: list[tuple[str, list[tuple[str, str]]]],
    spark,
) -> list[SchemaDiff]:
    """Compute one :class:`SchemaDiff` per (fqn, expected_cols) request.

    Tables that do not yet exist yield an empty diff (the table will be
    created by ``CREATE TABLE IF NOT EXISTS`` on first write).

    Raises:
        IncompatibleSchemaChangeError: if any computed diff has
            ``type_changed`` entries. Message is ``format_plan`` of the
            offending diffs only.
    """
    diffs = [
        diff_schema(fqn, expected, fetch_table_columns(spark, fqn))
        for fqn, expected in requests
    ]
    incompatible = [d for d in diffs if not d.is_compatible]
    if incompatible:
        raise IncompatibleSchemaChangeError(format_plan(incompatible))
    return diffs
```

- [ ] **Step 2.8: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestPlanMigrations -v
```

Expected: 3 PASS

- [ ] **Step 2.9: 寫 `apply_migrations` 測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
from recsys_tfb.io.schema_evolution import apply_migrations


class TestApplyMigrations:
    def test_skips_empty_diffs(self):
        spark = MagicMock()
        empty = SchemaDiff("db.t", added=[], type_changed=[])
        apply_migrations([empty], spark)
        spark.sql.assert_not_called()

    def test_emits_alter_table_add_columns_cascade(self):
        spark = MagicMock()
        d = SchemaDiff(
            "ml_recsys.train_model_input",
            added=[("foo", "double"), ("bar", "string")],
            type_changed=[],
        )
        apply_migrations([d], spark)
        spark.sql.assert_called_once_with(
            "ALTER TABLE ml_recsys.train_model_input "
            "ADD COLUMNS (foo double, bar string) CASCADE"
        )

    def test_runs_each_diff_sequentially(self):
        spark = MagicMock()
        d1 = SchemaDiff("db.a", added=[("x", "int")], type_changed=[])
        d2 = SchemaDiff("db.b", added=[("y", "string")], type_changed=[])
        apply_migrations([d1, d2], spark)
        assert spark.sql.call_count == 2
        assert "db.a" in spark.sql.call_args_list[0][0][0]
        assert "db.b" in spark.sql.call_args_list[1][0][0]
```

- [ ] **Step 2.10: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestApplyMigrations -v
```

Expected: NameError on `apply_migrations`

- [ ] **Step 2.11: 實作 `apply_migrations`**

加到 `src/recsys_tfb/io/schema_evolution.py`：

```python
def apply_migrations(diffs: list[SchemaDiff], spark) -> None:
    """Execute ``ALTER TABLE ... ADD COLUMNS (...) CASCADE`` per diff with
    non-empty ``added`` list.

    Sequential; partial failure does NOT roll back already-applied tables.
    ALTER ADD COLUMNS is metadata-only and idempotent; re-running after a
    failure will skip already-aligned tables.
    """
    for d in diffs:
        if not d.added:
            continue
        cols_sql = ", ".join(f"{n} {t}" for n, t in d.added)
        sql = f"ALTER TABLE {d.table_fqn} ADD COLUMNS ({cols_sql}) CASCADE"
        logger.info("Executing: %s", sql)
        spark.sql(sql)
```

- [ ] **Step 2.12: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestApplyMigrations -v
```

Expected: 3 PASS

- [ ] **Step 2.13: 寫 `collect_tracked_table_fqns` 測試**

加進 `tests/test_io/test_schema_evolution.py`：

```python
from recsys_tfb.io.schema_evolution import collect_tracked_table_fqns


class TestCollectTrackedTableFqns:
    def test_picks_only_marked_hive_table_datasets(self):
        catalog = {
            "feature_table": {  # not marked → skip
                "type": "HiveTableDataset",
                "database": "ml_recsys",
                "table": "feature_table",
            },
            "train_model_input": {  # marked → include
                "type": "HiveTableDataset",
                "database": "ml_recsys",
                "table": "train_model_input",
                "tracks_feature_table_schema": True,
            },
            "preprocessor": {  # different type → skip even if marked
                "type": "PickleDataset",
                "filepath": "x.pkl",
                "tracks_feature_table_schema": True,
            },
        }
        fqns = collect_tracked_table_fqns(catalog)
        assert fqns == ["ml_recsys.train_model_input"]

    def test_returns_empty_when_no_marks(self):
        catalog = {
            "x": {"type": "HiveTableDataset", "database": "db", "table": "x"},
        }
        assert collect_tracked_table_fqns(catalog) == []

    def test_falsey_flag_treated_as_not_marked(self):
        catalog = {
            "x": {
                "type": "HiveTableDataset",
                "database": "db",
                "table": "x",
                "tracks_feature_table_schema": False,
            },
        }
        assert collect_tracked_table_fqns(catalog) == []
```

- [ ] **Step 2.14: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py::TestCollectTrackedTableFqns -v
```

Expected: NameError on `collect_tracked_table_fqns`

- [ ] **Step 2.15: 實作 `collect_tracked_table_fqns`**

加到 `src/recsys_tfb/io/schema_evolution.py`：

```python
def collect_tracked_table_fqns(catalog_config: dict) -> list[str]:
    """Scan a catalog config dict (after ${...} substitution) for HiveTableDataset
    entries marked with ``tracks_feature_table_schema: true``. Returns ordered
    list of fully-qualified Hive names."""
    fqns: list[str] = []
    for entry in catalog_config.values():
        if entry.get("type") != "HiveTableDataset":
            continue
        if entry.get("tracks_feature_table_schema") is not True:
            continue
        fqns.append(f"{entry['database']}.{entry['table']}")
    return fqns
```

- [ ] **Step 2.16: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_schema_evolution.py -v
```

Expected: 全 21 case PASS（13 from Task 1 + 8 new）

- [ ] **Step 2.17: Commit**

```bash
git add src/recsys_tfb/io/schema_evolution.py tests/test_io/test_schema_evolution.py
git commit -m "feat(io): plan_migrations + apply_migrations + collect_tracked_table_fqns"
```

---

## Task 3: `HiveTableDataset` 接受 `tracks_feature_table_schema` kwarg

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py`（建構子簽名）
- Modify: `tests/test_io/test_hive_table_dataset.py`

DataCatalog 透過 `cls(**entry)` 建構 dataset；如果 catalog.yaml 帶這個 key 但 HiveTableDataset 沒接受，會 `TypeError: unexpected keyword argument`。本 task 加 kwarg 接收即可，不需要其他行為。

- [ ] **Step 3.1: 寫測試確認 kwarg 可被接受並保存**

加進 `tests/test_io/test_hive_table_dataset.py`（找到既有 class 加進去；如果不確定位置直接放檔尾）：

```python
class TestTracksFeatureTableSchema:
    def test_default_false(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="t",
            columns=[{"name": "x", "type": "DOUBLE"}],
            external=False,
        )
        assert ds._tracks_feature_table_schema is False

    def test_can_be_set_true(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="t",
            columns=[{"name": "x", "type": "DOUBLE"}],
            external=False,
            tracks_feature_table_schema=True,
        )
        assert ds._tracks_feature_table_schema is True
```

- [ ] **Step 3.2: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestTracksFeatureTableSchema -v
```

Expected: `TypeError: __init__() got an unexpected keyword argument 'tracks_feature_table_schema'`

- [ ] **Step 3.3: 修改 `HiveTableDataset.__init__`**

在 `src/recsys_tfb/io/hive_table_dataset.py:32-45`，建構子簽名加上新 kwarg。對照修改後完整片段：

```python
    def __init__(
        self,
        database: str,
        table: str,
        columns: list[dict] | str | None = None,
        partition_cols: list[dict] | None = None,
        partition_filter: dict | None = None,
        external: bool = True,
        location: str | None = None,
        stored_as: str = "PARQUET",
        write_mode: str = "overwrite",
        table_properties: dict | None = None,
        read_only: bool = False,
        tracks_feature_table_schema: bool = False,
    ):
        self._database = database
        self._table = table
        self._infer_columns = columns == "auto"
        self._columns: list[dict] = [] if self._infer_columns else (columns or [])
        self._partition_cols = partition_cols or []
        self._partition_filter = dict(partition_filter or {})
        self._external = external
        self._location = location
        self._stored_as = stored_as
        self._write_mode = write_mode
        self._table_properties = table_properties or {}
        self._read_only = read_only
        self._tracks_feature_table_schema = tracks_feature_table_schema

        self._validate()
```

- [ ] **Step 3.4: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_io/test_hive_table_dataset.py -v
```

Expected: 既有 case 維持 PASS + 新增 2 case PASS。

- [ ] **Step 3.5: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git commit -m "feat(io): HiveTableDataset accepts tracks_feature_table_schema kwarg"
```

---

## Task 4: `catalog.yaml` 標註 6 張下游表

**Files:**
- Modify: `conf/base/catalog.yaml`（6 個 entry 各加一行）

無單元測試（catalog.yaml 是 declarative config）；Task 6 跟 Task 5 的整合測試會吃到這個修改。

- [ ] **Step 4.1: 在 6 個 HiveTableDataset entry 加 `tracks_feature_table_schema: true`**

修改 `conf/base/catalog.yaml`：

`val_model_input`（line 52 附近）、`test_model_input`（line 63 附近）、`preprocessed_feature_table`（line 119）、`train_model_input`（line 130）、`train_dev_model_input`（line 142）、`calibration_model_input`（line 167）。

每個 entry 在 `external: false` 那一行下方加一行 `tracks_feature_table_schema: true`，注意縮排與該 entry 其他 key 對齊。例如 `train_model_input`：

```yaml
train_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_model_input
  external: false
  tracks_feature_table_schema: true     # ← 新增
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}
```

`preprocessed_feature_table` 縮排略大（4 空白）：

```yaml
preprocessed_feature_table:
    type: HiveTableDataset
    database: ${hive.db}
    table: preprocessed_feature_table
    external: false
    tracks_feature_table_schema: true   # ← 新增
    columns: "auto"
    partition_filter:
      base_dataset_version: ${base_dataset_version}
    partition_cols:
      - {name: snap_date, type: STRING}
```

- [ ] **Step 4.2: 確認 catalog.yaml 仍可被 DataCatalog 載入**

```bash
.venv/bin/python -c "
from recsys_tfb.core.catalog import DataCatalog
from recsys_tfb.core.config import ConfigLoader
loader = ConfigLoader('conf', env='local')
cat = loader.get_catalog_config(runtime_params={
    'base_dataset_version': 'x', 'train_variant_id': 'y',
    'calibration_variant_id': 'z', 'model_version': 'm', 'snap_date': 'd'})
DataCatalog(cat)
print('OK')
"
```

Expected: `OK`（如出現 `TypeError: unexpected keyword argument` 表示 Task 3 沒做完整）。

- [ ] **Step 4.3: 確認 collect_tracked_table_fqns 抓到 6 張**

```bash
.venv/bin/python -c "
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.io.schema_evolution import collect_tracked_table_fqns
loader = ConfigLoader('conf', env='local')
cat = loader.get_catalog_config(runtime_params={
    'base_dataset_version': 'x', 'train_variant_id': 'y',
    'calibration_variant_id': 'z', 'model_version': 'm', 'snap_date': 'd'})
fqns = collect_tracked_table_fqns(cat)
print(len(fqns))
for f in fqns: print(' ', f)
"
```

Expected: `6` 後接 6 行 `ml_recsys.{preprocessed_feature_table, train_model_input, train_dev_model_input, val_model_input, test_model_input, calibration_model_input}`。

- [ ] **Step 4.4: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "feat(catalog): mark 6 downstream tables with tracks_feature_table_schema"
```

---

## Task 5: dataset pipeline verify Node + 改路由

**Files:**
- Create: `src/recsys_tfb/pipelines/dataset/_verify.py`
- Modify: `src/recsys_tfb/pipelines/dataset/pipeline.py`
- Create: `tests/test_pipelines/test_dataset/test_verify.py`
- Modify: `tests/test_pipelines/test_dataset/test_pipeline.py`

設計選擇：`feature_table_verified` 是純 passthrough sentinel —— 值就是 verify Node 收到的 `feature_table` DataFrame 本身。改路由的兩個節點是 `fit_preprocessor_metadata` 與 `apply_preprocessor_to_features`（dataset/pipeline.py 中**僅有的兩個**直接吃 `feature_table` 的節點）。其他 `select_*_keys` 節點 input `sample_pool`，跟 verify 不直接相關。

- [ ] **Step 5.1: 寫 `make_verify_node_func` 測試**

建立 `tests/test_pipelines/test_dataset/test_verify.py`：

```python
"""Tests for dataset pipeline verify Node factory."""

from unittest.mock import MagicMock

import pytest

from recsys_tfb.io.schema_evolution import FeatureTableSchemaOutOfSync
from recsys_tfb.pipelines.dataset._verify import make_verify_node_func


def _mock_feature_table(columns):
    """Mock pyspark DataFrame with .columns, .schema.fields, .sparkSession."""
    df = MagicMock()
    df.columns = [n for n, _ in columns]
    fields = []
    for name, dtype in columns:
        f = MagicMock()
        f.name = name
        f.dataType.simpleString.return_value = dtype
        fields.append(f)
    df.schema.fields = fields
    df.sparkSession = MagicMock()
    df.sparkSession.catalog.tableExists.return_value = False  # tracked tables missing
    return df


_PARAMS_FIXTURE = {
    "schema": {
        "columns": {
            "time": "snap_date",
            "entity": "cust_id",
            "item": "prod_name",
            "label": "label",
        },
        "categorical_values": {"prod_name": ["A", "B"]},
    },
    "dataset": {
        "prepare_model_input": {
            "categorical_columns": ["prod_name"],
            "drop_columns": ["snap_date", "cust_id", "label"],
        },
    },
}


class TestMakeVerifyNodeFunc:
    def test_returns_callable_named_verify_feature_table_schema(self):
        fn = make_verify_node_func(tracked_fqns=["db.t"])
        assert fn.__name__ == "verify_feature_table_schema"

    def test_passes_through_feature_table_when_no_diff(self):
        # tracked tables don't exist yet → empty diffs → no raise
        fn = make_verify_node_func(tracked_fqns=["ml_recsys.train_model_input"])
        ft = _mock_feature_table([
            ("snap_date", "string"),
            ("cust_id", "string"),
            ("aum", "double"),
        ])
        result = fn(ft, _PARAMS_FIXTURE)
        assert result is ft  # passthrough

    def test_raises_when_diff_present(self):
        fqn = "ml_recsys.train_model_input"
        fn = make_verify_node_func(tracked_fqns=[fqn])
        ft = _mock_feature_table([
            ("snap_date", "string"),
            ("cust_id", "string"),
            ("aum", "double"),
            ("new_feat", "double"),  # new column not yet in tracked table
        ])
        # Stub tracked table existence + schema (lacking new_feat)
        ft.sparkSession.catalog.tableExists.return_value = True
        existing = []
        for n, d in [("snap_date", "string"), ("cust_id", "string"), ("aum", "double")]:
            f = MagicMock(); f.name = n; f.dataType.simpleString.return_value = d
            existing.append(f)
        ft.sparkSession.table.return_value.schema.fields = existing

        with pytest.raises(FeatureTableSchemaOutOfSync) as exc_info:
            fn(ft, _PARAMS_FIXTURE)
        msg = str(exc_info.value)
        assert "new_feat double" in msg
        assert "migrate_schema" in msg

    def test_empty_tracked_fqns_passes_through(self):
        fn = make_verify_node_func(tracked_fqns=[])
        ft = _mock_feature_table([("snap_date", "string"), ("aum", "double")])
        result = fn(ft, _PARAMS_FIXTURE)
        assert result is ft
```

- [ ] **Step 5.2: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/test_verify.py -v
```

Expected: ImportError on `make_verify_node_func`

- [ ] **Step 5.3: 實作 `_verify.py`**

建立 `src/recsys_tfb/pipelines/dataset/_verify.py`：

```python
"""Verify Node factory for the dataset pipeline.

Builds a node function that, at run start, compares ``feature_table``'s
schema against each tracked downstream Hive table and fails fast with
guidance to run the ``migrate_schema`` CLI when diffs exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.schema_evolution import (
    FeatureTableSchemaOutOfSync,
    format_plan,
    plan_migrations,
)
from recsys_tfb.preprocessing._common import _get_preprocessing_config
from recsys_tfb.preprocessing._spark import _compute_feature_columns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _expected_feature_cols_subset(
    feature_table: "DataFrame", parameters: dict,
) -> list[tuple[str, str]]:
    """Return the ordered subset of feature_table columns that should appear
    in tracked tables (i.e., feature_columns), each paired with its dtype."""
    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    feature_columns_names = _compute_feature_columns(
        feature_table.columns,
        schema["identity_columns"],
        categorical_cols,
        drop_cols,
        schema["label"],
    )
    name_to_dtype = {f.name: f.dataType.simpleString() for f in feature_table.schema.fields}
    return [(n, name_to_dtype[n]) for n in feature_columns_names if n in name_to_dtype]


def make_verify_node_func(tracked_fqns: list[str]):
    """Build a Node-compatible ``(feature_table, parameters) -> feature_table``
    function that fail-fast raises if any tracked table is out of sync."""

    def verify_feature_table_schema(feature_table, parameters):
        if not tracked_fqns:
            return feature_table
        spark = feature_table.sparkSession
        expected = _expected_feature_cols_subset(feature_table, parameters)
        requests = [(fqn, expected) for fqn in tracked_fqns]
        diffs = plan_migrations(requests, spark)
        non_empty = [d for d in diffs if not d.is_empty]
        if non_empty:
            raise FeatureTableSchemaOutOfSync(
                "feature_table schema 與下列下游表不一致：\n"
                + format_plan(non_empty)
                + "\n\n請先跑：python -m recsys_tfb migrate_schema [--env <env>]"
            )
        return feature_table

    return verify_feature_table_schema
```

- [ ] **Step 5.4: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/test_verify.py -v
```

Expected: 4 PASS

- [ ] **Step 5.5: 寫 pipeline-level 測試**

加進 `tests/test_pipelines/test_dataset/test_pipeline.py`（檔尾）：

```python
class TestPipelineWithTrackedFqns:
    def test_no_tracked_fqns_keeps_existing_node_count(self):
        # Backward compatible: tracked_fqns=None or [] → no extra node
        from recsys_tfb.pipelines.dataset import create_pipeline
        pipe_default = create_pipeline()
        pipe_empty = create_pipeline(tracked_fqns=[])
        assert len(pipe_default.nodes) == len(pipe_empty.nodes) == 10

    def test_tracked_fqns_inserts_verify_first(self):
        from recsys_tfb.pipelines.dataset import create_pipeline
        pipe = create_pipeline(tracked_fqns=["ml_recsys.train_model_input"])
        # 10 base + 1 verify = 11
        assert len(pipe.nodes) == 11
        assert pipe.nodes[0].name == "verify_feature_table_schema"
        assert pipe.nodes[0].inputs == ["feature_table", "parameters"]
        assert pipe.nodes[0].outputs == ["feature_table_verified"]

    def test_downstream_nodes_consume_verified_alias(self):
        from recsys_tfb.pipelines.dataset import create_pipeline
        pipe = create_pipeline(tracked_fqns=["ml_recsys.train_model_input"])
        names_to_nodes = {n.name: n for n in pipe.nodes}
        assert "feature_table_verified" in names_to_nodes["fit_preprocessor_metadata"].inputs
        assert "feature_table_verified" in names_to_nodes["apply_preprocessor_to_features"].inputs
        # Ensure the originals are no longer used by these two nodes
        assert "feature_table" not in names_to_nodes["fit_preprocessor_metadata"].inputs
        assert "feature_table" not in names_to_nodes["apply_preprocessor_to_features"].inputs
```

- [ ] **Step 5.6: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/test_pipeline.py::TestPipelineWithTrackedFqns -v
```

Expected: TypeError or assertion failure（`create_pipeline` 還沒接 `tracked_fqns` 參數）。

- [ ] **Step 5.7: 改 pipeline.py 加 verify Node 與路由**

修改 `src/recsys_tfb/pipelines/dataset/pipeline.py`，整段重寫：

```python
"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.dataset._verify import make_verify_node_func


def create_pipeline(
    backend: str = "pandas",
    enable_calibration: bool = False,
    tracked_fqns: list[str] | None = None,
) -> Pipeline:
    if backend == "spark":
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
    else:
        from recsys_tfb.pipelines.dataset.nodes_pandas import (
            apply_preprocessor_to_features,
            build_model_input,
            fit_preprocessor_metadata,
            select_calibration_keys,
            select_test_keys,
            select_train_keys,
            select_val_keys,
            split_train_keys,
        )

    feature_table_input = (
        "feature_table_verified" if tracked_fqns else "feature_table"
    )

    nodes: list[Node] = []

    if tracked_fqns:
        nodes.append(
            Node(
                make_verify_node_func(tracked_fqns),
                inputs=["feature_table", "parameters"],
                outputs="feature_table_verified",
                name="verify_feature_table_schema",
            )
        )

    nodes.extend([
        # --- Key selection ---
        Node(
            select_train_keys,
            inputs=["sample_pool", "parameters"],
            outputs="sample_keys",
            name="select_sample_keys",
        ),
        Node(
            split_train_keys,
            inputs=["sample_keys", "parameters"],
            outputs=["train_keys", "train_dev_keys"],
        ),
        Node(
            select_val_keys,
            inputs=["sample_pool", "parameters"],
            outputs="val_keys",
        ),
        Node(
            select_test_keys,
            inputs=["sample_pool", "parameters"],
            outputs="test_keys",
        ),
        # --- Fit preprocessor on (verified) feature_table ---
        Node(
            fit_preprocessor_metadata,
            inputs=[feature_table_input, "parameters"],
            outputs=["preprocessor", "category_mappings"],
            name="fit_preprocessor_metadata",
        ),
        # --- Encode non-identity categoricals once; all splits reuse this ---
        Node(
            apply_preprocessor_to_features,
            inputs=[feature_table_input, "preprocessor", "parameters"],
            outputs="preprocessed_feature_table",
            name="apply_preprocessor_to_features",
        ),
        # --- Build model_input per split (join keys + labels + encoded features) ---
        Node(
            build_model_input,
            inputs=[
                "train_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_model_input",
            name="build_train_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "train_dev_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_dev_model_input",
            name="build_train_dev_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "val_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="val_model_input",
            name="build_val_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "test_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="test_model_input",
            name="build_test_model_input",
        ),
    ])

    if enable_calibration:
        nodes.extend([
            Node(
                select_calibration_keys,
                inputs=["sample_pool", "parameters"],
                outputs="calibration_keys",
            ),
            Node(
                build_model_input,
                inputs=[
                    "calibration_keys", "preprocessed_feature_table", "label_table",
                    "preprocessor", "parameters",
                ],
                outputs="calibration_model_input",
                name="build_calibration_model_input",
            ),
        ])

    return Pipeline(nodes)
```

- [ ] **Step 5.8: 跑全部 dataset pipeline 測試確認通過**

```bash
.venv/bin/pytest tests/test_pipelines/test_dataset/ -v
```

Expected: 既有 case + 新增 4 case 全 PASS。注意舊的 `test_pipeline_inputs` 必須維持 `{"feature_table", ...}` 因為 default 不傳 `tracked_fqns` → 路由不變。

- [ ] **Step 5.9: Commit**

```bash
git add src/recsys_tfb/pipelines/dataset/_verify.py src/recsys_tfb/pipelines/dataset/pipeline.py tests/test_pipelines/test_dataset/test_verify.py tests/test_pipelines/test_dataset/test_pipeline.py
git commit -m "feat(dataset): verify_feature_table_schema as first Node when tracked_fqns set"
```

---

## Task 6: `__main__.py` `dataset` cmd 注入 `tracked_fqns`

**Files:**
- Modify: `src/recsys_tfb/__main__.py`（`dataset` 指令裡計算 `tracked_fqns`）

`_execute_pipeline` 內部會再算一次 `catalog_config`，這裡是預先算給 `pipeline_kwargs` 用，是預期的小重複（catalog substitution 很快）。

- [ ] **Step 6.1: 修改 `dataset` 指令，注入 `tracked_fqns`**

在 `src/recsys_tfb/__main__.py:dataset()` 內，於 `pipeline_kwargs = {"enable_calibration": enable_calibration}` **之前**插入 catalog scan：

```python
    # 既有 runtime_params 構造後 ...
    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": "best",
        "snap_date": _NONE_PLACEHOLDER,
        "backend": backend,
    }

    # ↓↓↓ 新增：先算 tracked_fqns 給 create_pipeline ↓↓↓
    from recsys_tfb.io.schema_evolution import collect_tracked_table_fqns
    substitution_params = {**params, **runtime_params}
    catalog_config_for_scan = config.get_catalog_config(
        runtime_params=substitution_params
    )
    tracked_fqns = collect_tracked_table_fqns(catalog_config_for_scan)
    # ↑↑↑

    pipeline_kwargs = {
        "enable_calibration": enable_calibration,
        "tracked_fqns": tracked_fqns,
    }

    _execute_pipeline("dataset", pipeline_kwargs, runtime_params, config, params, env)
```

- [ ] **Step 6.2: 加 CLI 整合測試確保 verify Node 路由有打開**

加進 `tests/test_cli.py:TestCLI`（`test_dataset_pipeline_uses_hash_version` 之後）：

```python
    def test_dataset_pipeline_injects_tracked_fqns(self, tmp_path, monkeypatch):
        """Dataset CLI passes tracked_fqns derived from catalog metadata to create_pipeline."""
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {"sample_ratio": 0.1, "train_dev_ratio": 0.2}},
        )
        # Replace catalog with one HiveTableDataset entry marked tracked
        catalog_path = tmp_path / "conf" / "base" / "catalog.yaml"
        with open(catalog_path) as f:
            cat = yaml.safe_load(f)
        cat["my_tracked"] = {
            "type": "HiveTableDataset",
            "database": "ml_recsys",
            "table": "my_tracked",
            "external": False,
            "columns": [{"name": "x", "type": "DOUBLE"}],
            "tracks_feature_table_schema": True,
        }
        with open(catalog_path, "w") as f:
            yaml.dump(cat, f)

        captured = {}

        def _fake_create_pipeline(*args, **kwargs):
            captured["tracked_fqns"] = kwargs.get("tracked_fqns")
            from recsys_tfb.core.pipeline import Pipeline
            return Pipeline([])

        monkeypatch.setattr(
            "recsys_tfb.pipelines.dataset.create_pipeline", _fake_create_pipeline
        )
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch(
                        "recsys_tfb.utils.spark.get_or_create_spark_session",
                        return_value=_mock_spark_with_feature_table_schema(),
                    ):
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(app, ["dataset"])
            assert captured["tracked_fqns"] == ["ml_recsys.my_tracked"]
        finally:
            os.chdir(old_cwd)
```

- [ ] **Step 6.3: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_cli.py::TestCLI::test_dataset_pipeline_injects_tracked_fqns -v
.venv/bin/pytest tests/test_cli.py::TestCLI::test_dataset_pipeline_uses_hash_version -v
```

Expected: 兩個都 PASS。前者驗證新 wiring，後者驗證沒打破既有行為。

- [ ] **Step 6.4: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): dataset command injects tracked_fqns into create_pipeline"
```

---

## Task 7: `migrate_schema` CLI 子指令

**Files:**
- Modify: `src/recsys_tfb/__main__.py`（新增 `@app.command(name="migrate_schema")`）
- Modify: `tests/test_cli.py`（4 個 case）

- [ ] **Step 7.1: 寫 `migrate_schema` CLI 失敗測試**

加進 `tests/test_cli.py:TestCLI`（檔尾）：

```python
    def test_migrate_schema_no_diff_silent_ok(self, tmp_path):
        """No tracked tables (or all aligned) → exit 0 with friendly stdout."""
        _setup_conf(tmp_path)
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session",
                return_value=_mock_spark_with_feature_table_schema(),
            ):
                result = runner.invoke(app, ["migrate_schema"])
            assert result.exit_code == 0, result.output
            assert "No schema changes needed" in result.output
        finally:
            os.chdir(old_cwd)

    def test_migrate_schema_dry_run_default_prints_plan(self, tmp_path, monkeypatch):
        _setup_conf(tmp_path)
        # Patch plan_migrations to return a non-empty diff
        from recsys_tfb.io.schema_evolution import SchemaDiff

        def _fake_plan(requests, spark):
            return [SchemaDiff("ml_recsys.t", added=[("foo", "double")], type_changed=[])]
        monkeypatch.setattr(
            "recsys_tfb.__main__.plan_migrations", _fake_plan
        )
        # Sentinel for apply_migrations: must NOT be called in dry-run
        called = {"apply": False}
        def _fake_apply(diffs, spark):
            called["apply"] = True
        monkeypatch.setattr(
            "recsys_tfb.__main__.apply_migrations", _fake_apply
        )
        # Make collect_tracked return a non-empty list so plan is invoked
        monkeypatch.setattr(
            "recsys_tfb.__main__.collect_tracked_table_fqns",
            lambda cat: ["ml_recsys.t"],
        )

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session",
                return_value=_mock_spark_with_feature_table_schema(),
            ):
                result = runner.invoke(app, ["migrate_schema"])
            assert result.exit_code == 0, result.output
            assert "ml_recsys.t" in result.output
            assert "foo double" in result.output
            assert "Run with --apply to execute" in result.output
            assert called["apply"] is False
        finally:
            os.chdir(old_cwd)

    def test_migrate_schema_apply_executes(self, tmp_path, monkeypatch):
        _setup_conf(tmp_path)
        from recsys_tfb.io.schema_evolution import SchemaDiff

        monkeypatch.setattr(
            "recsys_tfb.__main__.plan_migrations",
            lambda requests, spark: [
                SchemaDiff("ml_recsys.t", added=[("foo", "double")], type_changed=[])
            ],
        )
        called = {"apply": 0}
        def _fake_apply(diffs, spark):
            called["apply"] += 1
        monkeypatch.setattr(
            "recsys_tfb.__main__.apply_migrations", _fake_apply
        )
        monkeypatch.setattr(
            "recsys_tfb.__main__.collect_tracked_table_fqns",
            lambda cat: ["ml_recsys.t"],
        )

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session",
                return_value=_mock_spark_with_feature_table_schema(),
            ):
                result = runner.invoke(app, ["migrate_schema", "--apply"])
            assert result.exit_code == 0, result.output
            assert called["apply"] == 1
            assert "Applied 1 migration" in result.output
        finally:
            os.chdir(old_cwd)

    def test_migrate_schema_incompatible_change_exits_nonzero(self, tmp_path, monkeypatch):
        _setup_conf(tmp_path)
        from recsys_tfb.io.schema_evolution import IncompatibleSchemaChangeError

        def _raise(requests, spark):
            raise IncompatibleSchemaChangeError("aum: double -> string")
        monkeypatch.setattr(
            "recsys_tfb.__main__.plan_migrations", _raise
        )
        monkeypatch.setattr(
            "recsys_tfb.__main__.collect_tracked_table_fqns",
            lambda cat: ["ml_recsys.t"],
        )

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session",
                return_value=_mock_spark_with_feature_table_schema(),
            ):
                result = runner.invoke(app, ["migrate_schema"])
            assert result.exit_code == 1, result.output
            assert "aum: double -> string" in result.output
        finally:
            os.chdir(old_cwd)
```

- [ ] **Step 7.2: 跑測試確認失敗**

```bash
.venv/bin/pytest tests/test_cli.py -k migrate_schema -v
```

Expected: `Usage Error: No such command 'migrate_schema'` or 4× FAIL with exit code 2。

- [ ] **Step 7.3: 加 module-level import + 在 `__main__.py` 新增 `migrate_schema` 指令**

在 `src/recsys_tfb/__main__.py` 檔頭 import 區塊加：

```python
from recsys_tfb.io.schema_evolution import (
    IncompatibleSchemaChangeError,
    apply_migrations,
    collect_tracked_table_fqns,
    format_plan,
    plan_migrations,
)
```

（保留 import 字母序；放在 `from recsys_tfb.io.*` 區塊位置，如尚無此區塊則放在 `core` imports 後。）

在檔案最末（`@app.command(name="baselines")` 之後）新增：

```python
@app.command(name="migrate_schema")
def migrate_schema_cmd(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    apply: bool = typer.Option(
        False, "--apply",
        help="Execute the migration plan; without this flag the command is dry-run.",
    ),
):
    """Sync Hive output table schemas to the current feature_table schema.

    Default (without --apply) is a dry-run: prints the plan and exits 0.
    With --apply, executes ALTER TABLE ... ADD COLUMNS (...) CASCADE per
    diff. Detects added columns and (incompatible) type changes only.
    """
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, backend, run_context = _load_config_and_setup("migrate_schema", env)
    get_or_create_spark_session(_load_spark_config(config, "migrate_schema"))
    spark = get_or_create_spark_session()

    # Build catalog config with placeholder substitutions just enough to
    # surface tracked tables; ${...} placeholders that don't apply here can
    # be filled with sentinels.
    runtime_params = {
        "base_dataset_version": _NONE_PLACEHOLDER,
        "train_variant_id": _NONE_PLACEHOLDER,
        "calibration_variant_id": _NONE_PLACEHOLDER,
        "model_version": _NONE_PLACEHOLDER,
        "snap_date": _NONE_PLACEHOLDER,
        "backend": backend,
    }
    substitution_params = {**params, **runtime_params}
    catalog_config = config.get_catalog_config(runtime_params=substitution_params)
    tracked_fqns = collect_tracked_table_fqns(catalog_config)

    if not tracked_fqns:
        typer.echo("No schema changes needed. (no tracked tables)")
        raise typer.Exit(0)

    # Compute expected = feature_table feature_columns subset
    hive_db = params.get("hive", {}).get("db", "ml_recsys")
    feature_table_fqn = f"{hive_db}.feature_table"
    ft_df = spark.table(feature_table_fqn)
    from recsys_tfb.pipelines.dataset._verify import _expected_feature_cols_subset
    expected = _expected_feature_cols_subset(ft_df, params)
    requests = [(fqn, expected) for fqn in tracked_fqns]

    try:
        diffs = plan_migrations(requests, spark)
    except IncompatibleSchemaChangeError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=1)

    non_empty = [d for d in diffs if not d.is_empty]
    if not non_empty:
        typer.echo("No schema changes needed.")
        raise typer.Exit(0)

    typer.echo(format_plan(non_empty))
    if not apply:
        typer.echo("\nRun with --apply to execute.")
        raise typer.Exit(0)

    apply_migrations(non_empty, spark)
    typer.echo(f"\nApplied {len(non_empty)} migration(s).")
```

> **Note for implementer**: Sentinel placeholders for `${base_dataset_version}` etc. are intentional — `migrate_schema` does not need real version IDs because `collect_tracked_table_fqns` only reads `database` + `table` keys.

- [ ] **Step 7.4: 跑測試確認通過**

```bash
.venv/bin/pytest tests/test_cli.py -k migrate_schema -v
```

Expected: 4 PASS

- [ ] **Step 7.5: 跑整套 CLI 測試確認沒打破其他指令**

```bash
.venv/bin/pytest tests/test_cli.py -v
```

Expected: 全 PASS。

- [ ] **Step 7.6: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): migrate_schema subcommand with dry-run / --apply gate"
```

---

## Task 8: 端到端 dev-cluster 驗收（Manual checklist）

**Files:** 無 code 變更；本 task 是手動 smoke test，須在 dev-cluster 跑過後才算完工。

> **Why manual**: dev-cluster 啟停需要 docker compose、開銷大、不適合進 CI。同時這條 path 涉及真實 Hive metastore + HDFS，是 spec 規定的最終驗收（spec §「端到端驗收」段）。

- [ ] **Step 8.1: 確認 dev-cluster running**

```bash
cd ~/dev-cluster && docker compose ps
```

Expected: `namenode`, `datanode`, `hive-metastore`, `spark-master`, `spark-worker` 都是 `Up (healthy)`。如果不是，照 dev-cluster README 啟動。

- [ ] **Step 8.2: 重建 dev Hive 資料**

```bash
cd ~/projects/recsys_tfb
scripts/dev_admin.sh scripts/nuke_ml_recsys.py
scripts/dev_admin.sh scripts/setup_hive_dev.py
```

- [ ] **Step 8.3: 跑一次 dataset pipeline 建立 6 張下游表**

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb dataset --env production
```

Expected: 結束後 `SHOW TABLES IN ml_recsys` 應含 `preprocessed_feature_table`、`train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input`。記下此時的 `base_dataset_version`（log 中印出的 8 字元 hash）為 `BASE_V_OLD`。

- [ ] **Step 8.4: 在 setup 腳本加一個新欄位後重灌 feature_table**

編輯 `scripts/setup_hive_dev.py`，在 feature_table 寫入前加一個合成欄位 `test_extra_feat`（DoubleType，值用 `F.rand()`）。重跑：

```bash
scripts/dev_admin.sh scripts/setup_hive_dev.py
```

- [ ] **Step 8.5: 重跑 dataset → verify Node fail-fast**

```bash
.venv/bin/python -m recsys_tfb dataset --env production
```

Expected: pipeline 在第一個 Node `verify_feature_table_schema` 失敗，exit code 1，stderr 含 `feature_table schema 與下列下游表不一致` + `migrate_schema`。

- [ ] **Step 8.6: dry-run migrate_schema 預覽 plan**

```bash
.venv/bin/python -m recsys_tfb migrate_schema --env production
```

Expected: stdout 印 6 張表的 ADD COLUMNS plan，每張都含 `test_extra_feat double`。最後一行：`Run with --apply to execute.`。Exit 0。

- [ ] **Step 8.7: --apply 執行 migration**

```bash
.venv/bin/python -m recsys_tfb migrate_schema --env production --apply
```

Expected: stdout 印 plan 後再印 `Applied 6 migration(s).`。Exit 0。

- [ ] **Step 8.8: 重跑 dataset 應該過 verify Node 並寫新 base_v partition**

```bash
.venv/bin/python -m recsys_tfb dataset --env production
```

Expected: pipeline 完整跑完，新 `base_dataset_version`（記為 `BASE_V_NEW`）≠ `BASE_V_OLD`（fingerprint 變了）。

- [ ] **Step 8.9: 驗證舊 partition 顯示 NULL，新 partition 有實值**

```bash
scripts/dev_admin.sh -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql(f\"\"\"
SELECT base_dataset_version, COUNT(*) AS n_rows,
       SUM(CASE WHEN test_extra_feat IS NULL THEN 1 ELSE 0 END) AS n_null
FROM ml_recsys.train_model_input
GROUP BY base_dataset_version
ORDER BY base_dataset_version
\"\"\").show(truncate=False)
"
```

Expected: 兩個 base_v 各一行，舊 base_v 的 `n_null == n_rows`（CASCADE 後舊 Parquet 沒 test_extra_feat → 全 NULL），新 base_v 的 `n_null == 0`。

- [ ] **Step 8.10: 驗收完成 — 在 PR description 勾選此 task**

無 commit；本 task 結果記錄在後續 PR 描述的 Test Plan 段，列出每步觀察到的實際輸出。

---

## Self-Review

**Spec coverage check:**

| Spec section | Plan task |
|---|---|
| `migrate_schema` 兩段式 CLI | Task 7 |
| `IncompatibleSchemaChangeError` 訊息 | Task 1（exception）+ Task 2（plan_migrations 觸發）+ Task 7（CLI 顯示）|
| Catalog metadata 標註 6 張表 | Task 4 |
| `HiveTableDataset` 接 tracks 旗標 | Task 3 |
| `verify_feature_table_schema` Node | Task 5 |
| Pipeline 改路由 `feature_table_verified` | Task 5 |
| dataset CLI 注入 tracked_fqns | Task 6 |
| ALTER ... ADD COLUMNS CASCADE | Task 2（apply_migrations）|
| 端到端 dev-cluster checklist | Task 8 |
| Spec §3.5「CASCADE 預設」 | Task 2 step 2.11 SQL 帶 CASCADE |
| Spec §3.4「user 改回去無害」 | 由 fingerprint 隔離，無需 plan task |

**Detection scope diff vs spec：** spec 在 §「Error Handling」中列了 `removed: [...]`，本 plan 簡化為只偵測 `added` + `type_changed`。理由詳見 plan 開頭「Detection scope（簡化說明）」段。如果之後決定要嚴格實作 spec 原始描述，建議當成獨立後續工作（需引入「per-table expected schema」hardcode mapping，破壞 verify Node 對 catalog metadata 的單純依賴）。

**Type consistency check:**
- `SchemaDiff` fields 一致：`table_fqn`, `added: list[tuple[str, str]]`, `type_changed: list[tuple[str, str, str]]`（task 1, 2, 5, 7 都用同一 shape）。
- `plan_migrations(requests, spark)` 參數順序在 task 2/5/7 一致。
- `apply_migrations(diffs, spark)` 參數順序一致。
- `collect_tracked_table_fqns(catalog_config)` 簽名一致（task 2/6/7）。
- Custom exceptions：`IncompatibleSchemaChangeError`（task 1 定義、task 2 raise、task 7 catch），`FeatureTableSchemaOutOfSync`（task 1 定義、task 5 raise）。

**Placeholder scan:** 無 TBD / TODO / "fill in details"。每個程式步驟都附完整 code block；每個測試步驟都有完整 test 函式；每個指令都有 expected output。


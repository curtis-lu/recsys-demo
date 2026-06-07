# etl_audit_log 批次化寫入 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `etl_audit_log` 的每筆一條 INSERT 改為「run 內 buffer、結束 flush 一次 coalesced 寫出」，並取消 `snap_date` 分區，解決小碎檔問題。

**Architecture:** `AuditWriter.write_record/write_summary` 改為只 append 到記憶體 buffer（仍即時打結構化 log）；新增 `flush()` 用單一 `createDataFrame(...).coalesce(1).write.mode("append").insertInto(...)` 寫出，audit 失敗只記 log 不致命。`sql_runner.py` 的 `run()` 與 `run_source_checks()` 在最外層 `finally` 呼叫 `flush()`。`etl_audit_log` 改為不分區、`snap_date` 降為一般欄位（source_etl 未上線，無相容包袱）。

**Tech Stack:** Python 3.10、PySpark 3.3.2、pytest 7.3.1。無 UDF、無新套件。

> **環境前置（每個 Bash 步驟都適用）**
> - 工作目錄＝worktree root：`/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write`
> - 測試／CLI 一律：
>   `PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
> - git 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write ...`
> - 切換分支/合併前先 `git -C <wt> checkout -- graphify-out/GRAPH_REPORT.md`（hook 會弄髒它）。

---

## File Structure

- `src/recsys_tfb/pipelines/source_etl/audit.py` — **重寫核心**：欄位單一真實來源（`_AUDIT_COLUMNS` → DDL + `StructType`）、buffer、`flush()`；移除 `_INSERT_AUDIT_SQL` 與手動跳脫；`_CREATE_AUDIT_TABLE_SQL` 改由 `_create_table_sql()` 生成（不分區）。
- `src/recsys_tfb/pipelines/source_etl/sql_runner.py` — `run()` 與 `run_source_checks()` 外層 `finally` 加 `flush()`。
- `tests/test_pipelines/test_source_etl/test_audit.py` — 重寫：驗 buffer/flush、不分區 DDL、不跳脫、空 buffer no-op、flush 失敗不 raise。
- `tests/test_pipelines/test_source_etl/test_sql_runner.py` — 補 flush 斷言（既有 `test_audit_record_written_per_failed_check` + 新增 run() flush 測試）。
- `docs/pipelines/source_etl.md` — §稽核更新為 buffer/flush + 不分區。

---

## Task 1: 重寫 `AuditWriter`（buffer + flush + 不分區 schema）

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/audit.py`（全檔重寫）
- Test: `tests/test_pipelines/test_source_etl/test_audit.py`（全檔重寫）

- [ ] **Step 1: 重寫 test_audit.py（先寫會失敗的測試）**

把 `tests/test_pipelines/test_source_etl/test_audit.py` 全檔換成：

```python
"""Tests for audit writer (buffer + batched flush)."""

from unittest.mock import MagicMock

from recsys_tfb.pipelines.source_etl.audit import AuditWriter
from recsys_tfb.pipelines.source_etl.models import AuditRecord


def _make_writer():
    spark = MagicMock()
    config = {"database": "ml_feature", "table": "etl_audit_log"}
    writer = AuditWriter(spark, config)
    return writer, spark


class TestEnsureTable:
    def test_create_sql_is_unpartitioned_with_snap_date_column(self):
        _, spark = _make_writer()
        sql = spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS ml_feature.etl_audit_log" in sql
        assert "snap_date STRING" in sql
        assert "PARTITIONED BY" not in sql
        assert "STORED AS PARQUET" in sql


class TestBuffering:
    def test_write_record_buffers_without_writing(self):
        writer, spark = _make_writer()
        spark.sql.reset_mock()
        spark.createDataFrame.reset_mock()

        writer.write_record(
            AuditRecord(
                run_id="r1",
                snap_date="2024-01-31",
                table_name="feature_aum",
                status="success",
                row_count=1500000,
                duration_seconds=120.5,
            )
        )

        # buffering only: no Spark write happened yet
        spark.sql.assert_not_called()
        spark.createDataFrame.assert_not_called()

    def test_write_summary_buffers_summary_record(self):
        writer, _ = _make_writer()
        writer.write_summary("r1", "2024-01-31", "success", 600.0)
        # flush has not run; assert on next flush below via TestFlush
        writer.flush  # attribute exists


class TestFlush:
    def test_flush_writes_one_batched_coalesced_append(self):
        writer, spark = _make_writer()
        writer.write_record(
            AuditRecord(run_id="r1", snap_date="2024-01-31",
                        table_name="feature_aum", status="success",
                        row_count=10, duration_seconds=1.0)
        )
        writer.write_record(
            AuditRecord(run_id="r1", snap_date="2024-01-31",
                        table_name="feature_table", status="success",
                        row_count=20, duration_seconds=2.0)
        )
        writer.flush()

        spark.createDataFrame.assert_called_once()
        rows = spark.createDataFrame.call_args[0][0]
        assert len(rows) == 2
        # row tuple order: (run_id, snap_date, table_name, status,
        #                   row_count, duration_seconds, error_message, created_at)
        assert rows[0][0] == "r1"
        assert rows[0][2] == "feature_aum"
        assert rows[0][4] == 10

        df = spark.createDataFrame.return_value
        df.coalesce.assert_called_once_with(1)
        writer_chain = df.coalesce.return_value.write.mode
        writer_chain.assert_called_once_with("append")
        writer_chain.return_value.insertInto.assert_called_once_with(
            "ml_feature.etl_audit_log"
        )

    def test_flush_clears_buffer(self):
        writer, spark = _make_writer()
        writer.write_record(
            AuditRecord(run_id="r1", snap_date="2024-01-31",
                        table_name="t", status="success")
        )
        writer.flush()
        spark.createDataFrame.reset_mock()
        writer.flush()  # second flush: nothing buffered
        spark.createDataFrame.assert_not_called()

    def test_flush_empty_is_noop(self):
        writer, spark = _make_writer()
        writer.flush()
        spark.createDataFrame.assert_not_called()

    def test_error_message_passed_raw_not_escaped(self):
        writer, spark = _make_writer()
        msg = "can't parse\nnext line"
        writer.write_record(
            AuditRecord(run_id="r1", snap_date="2024-01-31",
                        table_name="t", status="failed", error_message=msg)
        )
        writer.flush()
        rows = spark.createDataFrame.call_args[0][0]
        assert rows[0][6] == msg  # raw, no backslash escaping

    def test_flush_failure_logs_and_does_not_raise(self):
        writer, spark = _make_writer()
        spark.createDataFrame.side_effect = RuntimeError("boom")
        writer.write_record(
            AuditRecord(run_id="r1", snap_date="2024-01-31",
                        table_name="t", status="success")
        )
        writer.flush()  # must NOT raise
        # buffer cleared even on failure (no retry/duplication)
        writer.flush()
        assert spark.createDataFrame.call_count == 1
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/test_audit.py -q
```
Expected: FAIL（`AuditWriter` 尚無 `flush`、CREATE 仍含 `PARTITIONED BY`、`write_record` 仍立即 `spark.sql`）。

- [ ] **Step 3: 重寫 audit.py**

把 `src/recsys_tfb/pipelines/source_etl/audit.py` 全檔換成：

```python
"""Audit logging for source ETL pipeline execution.

Records are buffered during a run and written to a Hive table in a single
batched, coalesced ``flush`` (avoids the small-files problem). Each record also
emits an immediate structured log event.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from recsys_tfb.pipelines.source_etl.models import AuditRecord

logger = logging.getLogger(__name__)

# Single source of truth for the audit table columns and their order. Drives both
# the CREATE DDL and the DataFrame schema so the positional ``insertInto`` lands
# in the right columns. Order MUST match the tuple built in ``flush``.
_AUDIT_COLUMNS: list[tuple[str, str, object]] = [
    ("run_id", "STRING", StringType()),
    ("snap_date", "STRING", StringType()),
    ("table_name", "STRING", StringType()),
    ("status", "STRING", StringType()),
    ("row_count", "BIGINT", LongType()),
    ("duration_seconds", "DOUBLE", DoubleType()),
    ("error_message", "STRING", StringType()),
    ("created_at", "TIMESTAMP", TimestampType()),
]

_AUDIT_SCHEMA = StructType(
    [StructField(name, spark_type, True) for name, _, spark_type in _AUDIT_COLUMNS]
)


def _create_table_sql(database: str, table: str) -> str:
    cols = ",\n    ".join(f"{name} {hive_type}" for name, hive_type, _ in _AUDIT_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {database}.{table} (\n"
        f"    {cols}\n"
        f")\n"
        f"STORED AS PARQUET"
    )


class AuditWriter:
    """Buffer ETL audit records and flush them to Hive in one batched write."""

    def __init__(self, spark, audit_config: dict) -> None:
        self._spark = spark
        self._database = audit_config["database"]
        self._table = audit_config["table"]
        self._buffer: list[AuditRecord] = []
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create the audit table if it doesn't exist (unpartitioned)."""
        self._spark.sql(_create_table_sql(self._database, self._table))
        logger.debug(
            "Ensured audit table %s.%s exists", self._database, self._table
        )

    def write_record(self, record: AuditRecord) -> None:
        """Buffer one audit record and emit an immediate structured log event."""
        self._buffer.append(record)
        logger.info(
            "Audit: %s %s [%s] rows=%d duration=%.1fs",
            record.snap_date,
            record.table_name,
            record.status,
            record.row_count,
            record.duration_seconds,
            extra={
                "event": "etl_audit",
                "snap_date": record.snap_date,
                "table_name": record.table_name,
                "status": record.status,
            },
        )

    def write_summary(
        self,
        run_id: str,
        snap_date: str,
        status: str,
        total_duration: float,
    ) -> None:
        """Buffer a summary audit record for the entire snap_date run."""
        self.write_record(
            AuditRecord(
                run_id=run_id,
                snap_date=snap_date,
                table_name="__summary__",
                status=status,
                duration_seconds=total_duration,
            )
        )

    def flush(self) -> None:
        """Write all buffered records in one coalesced append, then clear.

        Audit failures are logged but never raised: ``flush`` runs in a
        ``finally`` and must not mask an in-flight ETL exception.
        """
        if not self._buffer:
            return
        now = datetime.now(timezone.utc)
        rows = [
            (
                r.run_id,
                r.snap_date,
                r.table_name,
                r.status,
                int(r.row_count),
                float(r.duration_seconds),
                r.error_message,
                now,
            )
            for r in self._buffer
        ]
        fqn = f"{self._database}.{self._table}"
        try:
            df = self._spark.createDataFrame(rows, _AUDIT_SCHEMA)
            df.coalesce(1).write.mode("append").insertInto(fqn)
            logger.info("Flushed %d audit records to %s", len(rows), fqn)
        except Exception as exc:  # audit must not crash ETL
            logger.error(
                "Failed to flush %d audit records to %s: %s", len(rows), fqn, exc
            )
        finally:
            self._buffer.clear()
```

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/test_audit.py -q
```
Expected: PASS（全部）。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write checkout -- graphify-out/GRAPH_REPORT.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write add src/recsys_tfb/pipelines/source_etl/audit.py tests/test_pipelines/test_source_etl/test_audit.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write commit -m "refactor(audit): buffer records and flush once (unpartitioned, no small files)"
```

---

## Task 2: 把 `flush()` 接到 `run()` 與 `run_source_checks()`

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:169-196`（`run()` 迴圈外包 finally）
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:411-424`（`run_source_checks()` 包 finally）
- Test: `tests/test_pipelines/test_source_etl/test_sql_runner.py`

- [ ] **Step 1: 新增 run() 的 flush 測試 + 補強既有 source-check 測試（先寫會失敗的測試）**

在 `tests/test_pipelines/test_source_etl/test_sql_runner.py` 末尾**新增**一個 class（沿用檔案頂部既有的 `MagicMock`、`patch`、`SQLRunner`、`SourceETLError`、`CheckResult` import；若缺再補）：

```python
class TestAuditFlush:
    def test_run_flushes_audit_once_on_success(self, sql_dir):
        runner = SQLRunner(_base_config(), sql_dir, dry_run=False, stage="feature_etl")
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        probe = MagicMock()
        probe.columns = ["snap_date", "cust_id"]
        spark.sql.return_value = probe
        audit = MagicMock()
        with patch.object(runner, "_initialize_context", return_value=(spark, audit)), \
             patch.object(OutputChecker, "run_all", lambda self, t, db, d: []):
            runner.run(["2026-03-31"], run_id="r1")
        audit.flush.assert_called_once()

    def test_run_flushes_audit_even_on_failure(self, sql_dir):
        runner = SQLRunner(_base_config(), sql_dir, dry_run=False, stage="feature_etl")
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("spark boom")
        audit = MagicMock()
        with patch.object(runner, "_initialize_context", return_value=(spark, audit)):
            with pytest.raises(SourceETLError):
                runner.run(["2026-03-31"], run_id="r1")
        audit.flush.assert_called_once()

    def test_source_checks_flushes_audit_on_failure(self, sql_dir, monkeypatch):
        runner = SQLRunner(
            _base_config(source_checks={"feat_a": {"partition_key": "snap_date"}}),
            sql_dir, dry_run=False, stage="feature_etl",
        )
        audit = MagicMock()
        monkeypatch.setattr(runner, "_initialize_context", lambda: (MagicMock(), audit))
        from recsys_tfb.pipelines.source_etl import checks as checks_mod
        monkeypatch.setattr(
            checks_mod.SourceChecker, "run_all",
            lambda self, cfgs, d: [CheckResult(False, "bad", table="feat_a",
                                               check="row_count", snap_date=d)],
        )
        with pytest.raises(SourceCheckError):
            runner.run_source_checks(["2025-01-31"], run_id="r1")
        audit.flush.assert_called_once()
```

> **注意對齊既有 helper**：本檔已有 `_base_config(...)`、`_runner(...)`、`sql_dir` fixture、以及 `OutputChecker` / `SourceCheckError` / `SourceChecker` 的 import。若 `_base_config` 不接受 `source_checks=` kwarg，改用既有 `TestRunSourceChecks` 內 `self._runner(sql_dir, {...})` 的建構方式（見該 class），並把 `OutputChecker` 加入檔案頂部 import（`from recsys_tfb.pipelines.source_etl.checks import OutputChecker, SourceChecker, CheckResult`）。

也在既有 `test_audit_record_written_per_failed_check`（約 line 486）的最後一行 `assert {r.snap_date ...}` 之後**補一行**：

```python
        assert audit.flush.call_count == 1
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/test_sql_runner.py -q -k "AuditFlush or audit_record_written"
```
Expected: FAIL（`run()` / `run_source_checks()` 尚未呼叫 `audit.flush`）。

- [ ] **Step 3: 在 `run()` 外層包 finally 呼叫 flush**

把 `src/recsys_tfb/pipelines/source_etl/sql_runner.py` 的 `run()` 迴圈段（目前 169-196 行，從 `tables_to_run = self._get_tables_to_run(restart_from)` 之後的 `for snap_date in target_dates:` 整段）改為：

```python
        spark, audit = self._initialize_context()
        tables_to_run = self._get_tables_to_run(restart_from)

        try:
            for snap_date in target_dates:
                logger.info("Processing snap_date=%s", snap_date)
                run_start = time.monotonic()
                snap_status = "success"
                try:
                    # Execute tables
                    for table in tables_to_run:
                        self._process_single_table(spark, table, snap_date, run_id, audit)
                except SourceETLError:
                    # SQL/Spark execution error: abort the whole run after this
                    # iteration's audit summary is buffered.
                    snap_status = "failed"
                    raise
                finally:
                    total_duration = time.monotonic() - run_start
                    if not self._dry_run and audit:
                        audit.write_summary(
                            run_id, snap_date, snap_status, total_duration
                        )
                    logger.info(
                        "snap_date=%s finished: status=%s, duration=%.1fs",
                        snap_date,
                        snap_status,
                        total_duration,
                    )
        finally:
            if not self._dry_run and audit:
                audit.flush()
```

- [ ] **Step 4: 在 `run_source_checks()` 包 finally 呼叫 flush**

把 `run_source_checks()` 中「`failed = [...]` 到 `logger.info("Source check passed...")`」這段（目前約 411-429 行）改為：

```python
        try:
            failed = [r for r in all_results if not r.passed]
            if failed:
                if audit:
                    for r in failed:
                        audit.write_record(
                            AuditRecord(
                                run_id=run_id,
                                snap_date=r.snap_date,
                                table_name="__source_check__",
                                status="failed",
                                error_message=r.message,
                            )
                        )
                raise SourceCheckError(all_results, self._stage)

            logger.info(
                "Source check passed: %d checks (%s)",
                len(all_results), self._stage,
            )
        finally:
            if audit:
                audit.flush()
```

- [ ] **Step 5: 跑測試確認通過（含整包 source_etl 迴歸）**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_source_etl/ tests/test_cli.py -q
```
Expected: PASS（全部；確認沒打破既有 source-check / dry-run / fail-fast 測試）。

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write checkout -- graphify-out/GRAPH_REPORT.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write add src/recsys_tfb/pipelines/source_etl/sql_runner.py tests/test_pipelines/test_source_etl/test_sql_runner.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write commit -m "feat(source_etl): flush audit buffer once in run() and run_source_checks() finally"
```

---

## Task 3: 更新文件 §稽核

**Files:**
- Modify: `docs/pipelines/source_etl.md`（§輸入/輸出 的「稽核」一行 + §重跑語意 末段）

- [ ] **Step 1: 更新「稽核」描述**

把 `docs/pipelines/source_etl.md` §輸入 / 輸出 中：

```
- **稽核**：每輪寫一筆 summary 到 `${target_db}.etl_audit_log`。
```

改為：

```
- **稽核**：每張表 / 每輪 summary / source-check 失敗都記入 `${target_db}.etl_audit_log`；
  紀錄在 run 期間先 buffer，於 run 結束時**一次** coalesced 寫出（單檔，避免小碎檔）。
  該表**不分區**（`snap_date` 為一般欄位，查詢用 `WHERE snap_date = '...'`）。
```

- [ ] **Step 2: 確認文件其他處沒有與「分區 audit 表」相矛盾的敘述**

Run:
```bash
grep -n "etl_audit_log\|稽核\|audit" docs/pipelines/source_etl.md
```
Expected: 僅上面改過的一行 + §關鍵設定的 `audit:` 一行（後者只講「稽核表位置」，不需改）。若出現其他「分區 / partition」描述 audit 表的句子，一併刪除分區字眼。

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write checkout -- graphify-out/GRAPH_REPORT.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write add docs/pipelines/source_etl.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/etl-audit-batched-write commit -m "docs: etl_audit_log batched flush + unpartitioned in source_etl.md"
```

---

## Task 4: dev-cluster 端到端驗證（人工確認）

> 此 Task 不寫 code，是上線前的真實寫入驗證。需本機 dev-cluster；若當下不便跑，可標記為待辦交回使用者，不阻擋 PR 的單元測試綠燈。

**Files:** 無（只執行與觀察）

- [ ] **Step 1: 重建 audit 表（schema 變更）**

舊 `etl_audit_log` 是分區表，schema 改了必須先 drop 再讓 pipeline 重建。用 dev admin wrapper：

```bash
cd /Users/curtislu/projects/recsys_tfb
scripts/dev_admin.sh -c "spark.sql('DROP TABLE IF EXISTS ml_recsys.etl_audit_log')"
```
（若 `dev_admin.sh` 不支援 `-c` inline，改寫一個一行 drop 的小腳本走 `scripts/dev_admin.sh <script>`；參考 CLAUDE.md §Local dev-cluster testing 的 admin pattern。）

- [ ] **Step 2: 實跑一次 source ETL 並檢查檔案數**

```bash
source ~/dev-cluster/scripts/client-env.sh
cd /Users/curtislu/projects/recsys_tfb
.venv/bin/python -m recsys_tfb feature_etl --env production --target-dates 2025-01-31
```
Expected：pipeline 成功；`etl_audit_log` 該次 run 只新增 **1 個** parquet 檔（不再是每表一檔）。

- [ ] **Step 3: 驗證內容與 created_at**

```bash
scripts/dev_admin.sh -c "spark.sql('SELECT run_id, snap_date, table_name, status, created_at FROM ml_recsys.etl_audit_log ORDER BY created_at').show(50, False)"
```
Expected：每張表一列 + 一列 `__summary__`，`created_at` 皆有值、`snap_date` 為一般欄位且正確。

---

## Self-Review

**Spec coverage：**
- 目標1（批次化、每 run 1 檔）→ Task 1 `flush()` + Task 2 finally 接線 + Task 4 實測。✓
- 目標2（移除手動跳脫）→ Task 1 移除 `_INSERT_AUDIT_SQL`/`replace`，`test_error_message_passed_raw_not_escaped` 守住。✓
- 目標3（fail-fast 仍落地）→ Task 2 `test_run_flushes_audit_even_on_failure` + `test_source_checks_flushes_audit_on_failure`。✓
- 目標4（audit 失敗不致命）→ Task 1 `flush` try/except + `test_flush_failure_logs_and_does_not_raise`。✓
- D1 不分區 schema → Task 1 `_create_table_sql` + `test_create_sql_is_unpartitioned_with_snap_date_column`。✓
- D3 欄序單一真實來源 → Task 1 `_AUDIT_COLUMNS` 同時驅動 DDL 與 `StructType`、flush tuple 同序。✓
- 文件 → Task 3。✓

**Placeholder scan：** 無 TBD/TODO；每個 code step 皆附完整程式碼與確切指令。Task 4 的 `dev_admin.sh -c` 有給 fallback 寫法，非 placeholder。✓

**Type consistency：** `flush` tuple 順序＝`_AUDIT_COLUMNS`＝`_AUDIT_SCHEMA`＝DDL 欄序（run_id, snap_date, table_name, status, row_count[BIGINT/Long], duration_seconds[DOUBLE/Double], error_message, created_at[TIMESTAMP]）。測試 `rows[0][6]`＝error_message、`rows[0][4]`＝row_count 與此一致。`write_record`/`write_summary`/`flush` 簽名跨 Task 一致。✓

# `--source-check` preflight gate ＋ output checks fail-fast 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 source_check 抽成各 ETL 指令的 `--source-check` preflight 旗標（collect-all、失敗 exit 1、不寫表），並把 output quality_checks 改成 fail-fast 中止整個 run，提供可操作的失敗報告與重跑指引。

**Architecture:** 在 `feature_etl`/`label_etl`/`sample_pool_etl` 三個 typer 指令加 `--source-check` 旗標；`SQLRunner` 移除正常 run 內的 source-check gate、新增唯讀的 `run_source_checks()`、把 output check 改成 raise `OutputCheckError`。失敗以攜帶結構化清單的 `SourceCheckError`/`OutputCheckError` 表達，CLI 攔截後印乾淨報告並 exit 1。`CheckResult` 加欄位以產出 expected vs actual 報告。

**Tech Stack:** Python 3.10、Typer 0.20.1、PySpark 3.3.2（測試以 MagicMock 模擬 spark）、pytest 7.3.1。

設計依據：`docs/superpowers/specs/2026-06-07-source-check-subcommand-design.md`。

---

## 約定（每個測試/CLI 指令都這樣跑）

worktree root：`/Users/curtislu/projects/recsys_tfb/.worktrees/source-check`
測試一律用絕對 venv python ＋ worktree 的 `PYTHONPATH`（CLAUDE.md SOP §3）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/source-check
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/source-check/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

下文以 `PYTEST` 代稱上面那串 `PYTHONPATH=… .venv/bin/python -m pytest`。

涉及檔案：
- `src/recsys_tfb/pipelines/source_etl/checks.py`（enrich CheckResult、填欄位）
- `src/recsys_tfb/pipelines/source_etl/sql_runner.py`（新例外、run_source_checks、fail-fast、stage 建構參數、移除 gate）
- `src/recsys_tfb/__main__.py`（`--source-check` 旗標、`_run_etl` 分支）
- `tests/test_pipelines/test_source_etl/test_checks.py`
- `tests/test_pipelines/test_source_etl/test_sql_runner.py`
- `tests/test_cli.py`
- `docs/pipelines/source_etl.md`、`README.md`、`docs/change-guide.md`

---

## Task 1: enrich `CheckResult` ＋ `SourceChecker` 填欄位

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/checks.py:13-19`（CheckResult）、`:28-66`（partition/row_count）、`:68-105`（schema_drift）、`:107-138`（run_all）
- Test: `tests/test_pipelines/test_source_etl/test_checks.py`

- [ ] **Step 1: 寫 failing test**

加到 `test_checks.py`（沿用檔內既有 mock 風格）：

```python
class TestSourceCheckResultFields:
    def test_partition_check_populates_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value="snap_date=2024-02-29")
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        r = checker.check_partition_exists("db.t", "snap_date", "2024-01-31")
        assert r.passed is False
        assert r.table == "db.t"
        assert r.check == "partition_exists"
        assert r.expected == "partition snap_date=2024-01-31"
        assert r.actual == "not found"

    def test_row_count_populates_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 523 if k == "cnt" else None
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        r = checker.check_row_count("db.t", "snap_date", "2024-01-31", min_count=1000)
        assert r.passed is False
        assert r.check == "row_count"
        assert r.expected == ">= 1000"
        assert r.actual == "523"

    def test_run_all_stamps_snap_date(self):
        spark = MagicMock()
        prow = MagicMock()
        prow.__getitem__ = MagicMock(return_value="snap_date=2024-01-31")
        spark.sql.return_value.collect.return_value = [prow]

        checker = SourceChecker(spark)
        cfgs = [SourceCheckConfig(table_name="db.t", partition_key="snap_date")]
        results = checker.run_all(cfgs, "2024-01-31")
        assert all(r.snap_date == "2024-01-31" for r in results)
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_checks.py::TestSourceCheckResultFields -q`
Expected: FAIL（`CheckResult` 無 `table`/`check`/`expected`/`actual`/`snap_date` 屬性）。

- [ ] **Step 3: 改 `CheckResult` 加欄位**

`checks.py` 把 dataclass 改成：

```python
@dataclass
class CheckResult:
    """Result of a single check."""

    passed: bool
    message: str
    metric_value: float | int | None = None
    # 報告用欄位（皆有預設，向後相容）
    table: str = ""
    check: str = ""          # "partition_exists" | "row_count" | "schema_drift"
    snap_date: str = ""
    expected: str = ""
    actual: str = ""
```

- [ ] **Step 4: `SourceChecker` 各 check 填欄位**

`check_partition_exists` 兩個 return 改成帶欄位：

```python
        target = f"{partition_key}={snap_date}"
        expected = f"partition {target}"
        if exists:
            return CheckResult(
                True, f"Partition {target} exists in {table}",
                table=table, check="partition_exists",
                expected=expected, actual="found",
            )
        return CheckResult(
            False, f"Partition {target} not found in {table}",
            table=table, check="partition_exists",
            expected=expected, actual="not found",
        )
```

except 分支也補欄位：

```python
        except Exception as exc:
            return CheckResult(
                False, f"Failed to check partitions for {table}: {exc}",
                table=table, check="partition_exists",
                expected=f"partition {partition_key}={snap_date}", actual=f"error: {exc}",
            )
```

`check_row_count` 的 return：

```python
        return CheckResult(
            passed,
            f"{table} row count: {count} (min: {min_count})",
            metric_value=count,
            table=table, check="row_count",
            expected=f">= {min_count}", actual=str(count),
        )
```

`check_schema_drift` 三個 return：

```python
        if not expected_columns:
            return CheckResult(
                True, f"No schema expectations for {table}",
                table=table, check="schema_drift", expected="(none)", actual="ok",
            )
        ...
        if errors:
            return CheckResult(
                False, f"Schema drift in {table}: {'; '.join(errors)}",
                table=table, check="schema_drift",
                expected="declared columns present & typed",
                actual="; ".join(errors),
            )
        return CheckResult(
            True, f"Schema OK for {table}",
            table=table, check="schema_drift",
            expected="declared columns present & typed", actual="ok",
        )
```

- [ ] **Step 5: `run_all` 蓋 snap_date**

`SourceChecker.run_all` 末段在 return 前統一蓋上 snap_date：

```python
        for r in results:
            r.snap_date = snap_date
        return results
```

- [ ] **Step 6: 跑測試確認 PASS（含既有測試不回歸）**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_checks.py -q`
Expected: PASS（新 class ＋ 既有 `TestSourceChecker*` 全綠）。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/source_etl/checks.py tests/test_pipelines/test_source_etl/test_checks.py
git commit -m "feat(source_etl): enrich CheckResult with report fields (SourceChecker)" --no-verify
```

---

## Task 2: `OutputChecker` 填欄位

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/checks.py:147-255`（四個 output check）、`:257-297`（run_all）
- Test: `tests/test_pipelines/test_source_etl/test_checks.py`

- [ ] **Step 1: 寫 failing test**

```python
class TestOutputCheckResultFields:
    def test_row_count_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 0 if k == "cnt" else None
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        r = checker.check_row_count("db", "t", "2024-01-31", min_count=100)
        assert r.passed is False
        assert r.table == "t"
        assert r.check == "min_row_count"
        assert r.expected == ">= 100"
        assert r.actual == "0"
        assert r.snap_date == "2024-01-31"

    def test_run_all_sets_table_and_snap_date(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 5 if k == "cnt" else None
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        tc = TableConfig(
            name="feature_table", sql_file="x.sql",
            partition_by={"snap_date": "DATE"},
            primary_key=["snap_date", "cust_id"],
            quality_checks={"min_row_count": 1},
        )
        results = checker.run_all(tc, "ml_recsys", "2024-01-31")
        assert all(r.table == "feature_table" for r in results)
        assert all(r.snap_date == "2024-01-31" for r in results)
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_checks.py::TestOutputCheckResultFields -q`
Expected: FAIL（output check 未填 table/check/expected/actual/snap_date）。

- [ ] **Step 3: 四個 output check 填欄位**

`check_row_count`（output 版）return：

```python
        return CheckResult(
            passed,
            f"{db}.{table} row count: {count} (min: {min_count})",
            metric_value=count,
            table=table, check="min_row_count",
            snap_date=snap_date, expected=f">= {min_count}", actual=str(count),
        )
```

`check_duplicate_keys` 兩個 return：

```python
        if total == 0:
            return CheckResult(
                True, f"{db}.{table} has 0 rows, skip dup check", metric_value=0.0,
                table=table, check="max_duplicate_key_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="0 rows",
            )
        ...
        return CheckResult(
            passed,
            f"{db}.{table} duplicate key ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
            table=table, check="max_duplicate_key_ratio",
            snap_date=snap_date, expected=f"<= {max_ratio}", actual=f"{ratio:.4f}",
        )
```

`check_null_ratio` 三個 return（no-columns / 0-cells / 一般）：

```python
        if not columns:
            return CheckResult(
                True, f"{db}.{table} has no columns to check",
                table=table, check="max_null_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="no columns",
            )
        ...
        if total_cells == 0:
            return CheckResult(
                True, f"{db}.{table} has 0 cells, skip null check", metric_value=0.0,
                table=table, check="max_null_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="0 cells",
            )
        ...
        return CheckResult(
            passed,
            f"{db}.{table} null ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
            table=table, check="max_null_ratio",
            snap_date=snap_date, expected=f"<= {max_ratio}", actual=f"{ratio:.4f}",
        )
```

`check_schema_contract` 三個 return：

```python
        if not required_columns:
            return CheckResult(
                True, f"No required columns declared for {db}.{table}",
                table=table, check="schema_contract",
                snap_date=snap_date, expected="(none)", actual="ok",
            )
        ...
        if missing:
            return CheckResult(
                False,
                f"Schema contract failed for {db}.{table}: missing columns {missing}",
                table=table, check="schema_contract",
                snap_date=snap_date, expected="required columns present",
                actual=f"missing {missing}",
            )
        return CheckResult(
            True, f"Schema contract OK for {db}.{table}",
            table=table, check="schema_contract",
            snap_date=snap_date, expected="required columns present", actual="ok",
        )
```

> 注意：`check_schema_contract`/`check_null_ratio`/`check_duplicate_keys` 的簽章已含
> `snap_date`，直接帶入即可；`check_row_count`（output 版）亦同。

- [ ] **Step 4: 跑測試確認 PASS（含既有不回歸）**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_checks.py -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/source_etl/checks.py tests/test_pipelines/test_source_etl/test_checks.py
git commit -m "feat(source_etl): enrich OutputChecker results with report fields" --no-verify
```

---

## Task 3: `SourceCheckError` / `OutputCheckError` ＋ 報告格式

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:29-30`（在 `SourceETLError` 下方新增兩個子類）
- Test: `tests/test_pipelines/test_source_etl/test_sql_runner.py`

- [ ] **Step 1: 寫 failing test**

加到 `test_sql_runner.py`（import 在檔頭一併加）：

```python
from recsys_tfb.pipelines.source_etl.sql_runner import (
    OutputCheckError,
    SourceCheckError,
    SourceETLError,
    SQLRunner,
)
from recsys_tfb.pipelines.source_etl.checks import CheckResult


class TestErrorReports:
    def test_source_check_error_report(self):
        results = [
            CheckResult(True, "ok", table="t1", check="partition_exists",
                        snap_date="2025-01-31", expected="partition snap_date=2025-01-31",
                        actual="found"),
            CheckResult(False, "bad", table="feat_aum", check="partition_exists",
                        snap_date="2025-01-31", expected="partition snap_date=2025-01-31",
                        actual="not found"),
            CheckResult(False, "low", table="feat_aum", check="row_count",
                        snap_date="2025-02-28", expected=">= 1000000", actual="523"),
        ]
        err = SourceCheckError(results, "feature_etl")
        msg = str(err)
        assert "Source check FAILED: 2 of 3 checks failed" in msg
        assert "[FAIL] feat_aum / partition_exists @ 2025-01-31" in msg
        assert "expected partition snap_date=2025-01-31, got: not found" in msg
        assert "expected >= 1000000, got: 523" in msg
        assert "SHOW PARTITIONS feat_aum" in msg            # partition hint
        # 重跑指令只含失敗日期、去重排序
        assert ("python -m recsys_tfb feature_etl --source-check "
                "--target-dates 2025-01-31,2025-02-28") in msg
        assert err.results == results
        assert err.stage == "feature_etl"
        assert isinstance(err, SourceETLError)

    def test_output_check_error_report(self):
        failed = [
            CheckResult(False, "dup", table="feature_table",
                        check="max_duplicate_key_ratio", snap_date="2025-01-31",
                        expected="<= 0.0", actual="0.0123"),
        ]
        err = OutputCheckError("feature_etl", "feature_table", "2025-01-31", failed)
        msg = str(err)
        assert "Output quality check FAILED: feature_table @ 2025-01-31" in msg
        assert "expected <= 0.0, got: 0.0123" in msg
        assert ("python -m recsys_tfb feature_etl --target-dates 2025-01-31 "
                "--restart-from feature_table") in msg
        assert err.table == "feature_table"
        assert isinstance(err, SourceETLError)
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py::TestErrorReports -q`
Expected: FAIL（`ImportError: cannot import name 'SourceCheckError'`）。

- [ ] **Step 3: 在 `sql_runner.py` 新增兩個例外類**

`from recsys_tfb.pipelines.source_etl.checks import (OutputChecker, SourceChecker, CheckResult)`
—— 把 `CheckResult` 加進既有 import（`sql_runner.py:15-18`）。

在 `class SourceETLError` 之後新增：

```python
class SourceCheckError(SourceETLError):
    """Preflight source_checks 失敗：攜帶全部結果，str() 即完整報告。"""

    _HINTS = {
        "partition_exists": "上游分區尚未產出。確認上游已寫入該日：SHOW PARTITIONS {table}",
        "row_count": "上游資料量不足／該日載入不完整。確認上游 ETL 已完成。",
        "schema_drift": "上游 schema 與 expected_columns 不符。對齊上游欄位或更新設定。",
    }

    def __init__(self, results: list[CheckResult], stage: str) -> None:
        self.results = results
        self.stage = stage
        super().__init__(self._format(results, stage))

    @classmethod
    def _format(cls, results: list[CheckResult], stage: str) -> str:
        failed = [r for r in results if not r.passed]
        lines = [f"Source check FAILED: {len(failed)} of {len(results)} checks failed", ""]
        for r in failed:
            lines.append(f"  [FAIL] {r.table} / {r.check} @ {r.snap_date}")
            lines.append(f"         expected {r.expected}, got: {r.actual}")
            hint = cls._HINTS.get(r.check, "")
            if hint:
                lines.append(f"         → {hint.format(table=r.table)}")
        failed_dates = ",".join(sorted({r.snap_date for r in failed if r.snap_date}))
        lines += [
            "",
            "修復上游後重跑（僅失敗日期）：",
            f"  python -m recsys_tfb {stage} --source-check --target-dates {failed_dates}",
        ]
        return "\n".join(lines)


class OutputCheckError(SourceETLError):
    """單一輸出表的 quality_checks 失敗（fail-fast）。"""

    def __init__(
        self, stage: str, table: str, snap_date: str, failed: list[CheckResult]
    ) -> None:
        self.stage = stage
        self.table = table
        self.snap_date = snap_date
        self.failed = failed
        lines = [f"Output quality check FAILED: {table} @ {snap_date}"]
        for r in failed:
            lines.append(f"  [FAIL] {r.table} / {r.check} @ {r.snap_date}")
            lines.append(f"         expected {r.expected}, got: {r.actual}")
        lines += [
            "ETL 已中止。修復後可從該表續跑（跳過先前已寫的表）：",
            f"  python -m recsys_tfb {stage} --target-dates {snap_date} "
            f"--restart-from {table}",
        ]
        super().__init__("\n".join(lines))
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py::TestErrorReports -q`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/source_etl/sql_runner.py tests/test_pipelines/test_source_etl/test_sql_runner.py
git commit -m "feat(source_etl): add SourceCheckError/OutputCheckError with actionable reports" --no-verify
```

---

## Task 4: `SQLRunner` 加 `stage` 建構參數 ＋ `run_source_checks()` ＋ 移除 run() gate

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:36-57`（`__init__`）、`:86-149`（`run()`）、`:342-370`（移除 private `_run_source_checks`，新增 public `run_source_checks`）
- Test: `tests/test_pipelines/test_source_etl/test_sql_runner.py`

- [ ] **Step 1: 寫 failing test**

```python
class TestRunSourceChecks:
    def _runner(self, sql_dir, source_checks):
        config = _base_config()
        config["source_checks"] = source_checks
        # dry_run=False 才會真的查 spark；但我們直接 mock SourceChecker，故 spark 用 mock
        return SQLRunner(config, sql_dir, dry_run=False, stage="feature_etl")

    def test_collect_all_then_raise(self, sql_dir, monkeypatch):
        runner = self._runner(sql_dir, {
            "feat_a": {"partition_key": "snap_date"},
        })
        # 不真的起 spark / audit
        monkeypatch.setattr(runner, "_initialize_context", lambda: (MagicMock(), None))

        from recsys_tfb.pipelines.source_etl import checks as checks_mod
        calls = []

        def fake_run_all(self, cfgs, snap_date):
            calls.append(snap_date)
            ok = CheckResult(True, "ok", table="feat_a", check="partition_exists",
                             snap_date=snap_date)
            bad = CheckResult(False, "bad", table="feat_a", check="row_count",
                              snap_date=snap_date, expected=">= 1", actual="0")
            # 第一天通過、第二天失敗
            return [ok] if snap_date == "2025-01-31" else [ok, bad]

        monkeypatch.setattr(checks_mod.SourceChecker, "run_all", fake_run_all)

        with pytest.raises(SourceCheckError) as ei:
            runner.run_source_checks(["2025-01-31", "2025-02-28"], run_id="r1")
        # 兩天都跑了（collect-all，不在第一個失敗就停）
        assert calls == ["2025-01-31", "2025-02-28"]
        # 例外攜帶全部結果（3 筆）與全部失敗（1 筆）
        assert len(ei.value.results) == 3
        assert sum(1 for r in ei.value.results if not r.passed) == 1

    def test_all_pass_no_raise(self, sql_dir, monkeypatch):
        runner = self._runner(sql_dir, {"feat_a": {"partition_key": "snap_date"}})
        monkeypatch.setattr(runner, "_initialize_context", lambda: (MagicMock(), None))
        from recsys_tfb.pipelines.source_etl import checks as checks_mod
        monkeypatch.setattr(
            checks_mod.SourceChecker, "run_all",
            lambda self, cfgs, d: [CheckResult(True, "ok", snap_date=d)],
        )
        runner.run_source_checks(["2025-01-31"], run_id="r1")  # 不 raise

    def test_no_source_checks_warns_no_raise(self, sql_dir, caplog):
        runner = SQLRunner(_base_config(), sql_dir, dry_run=False, stage="feature_etl")
        runner.run_source_checks(["2025-01-31"], run_id="r1")  # 空設定 → 不 raise
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py::TestRunSourceChecks -q`
Expected: FAIL（`SQLRunner.__init__` 不接受 `stage`；無 `run_source_checks`）。

- [ ] **Step 3: `__init__` 加 `stage` 參數**

`sql_runner.py` `__init__` 簽章與 body：

```python
    def __init__(
        self,
        config: dict,
        sql_dir: Path,
        dry_run: bool = False,
        rendered_sql_dir: Path | None = None,
        stage: str = "source_etl",
    ) -> None:
        ...
        self._renderer = SQLRenderer(sql_dir)
        self._target_db = self._variables.get("target_db", "default")
        self._stage = stage
        self._validate_order()
```

- [ ] **Step 4: 移除 run() 內 source-check gate**

`run()` 內刪除下列區塊（`sql_runner.py` 約 118-122）：

```python
                # Source freshness checks (skip in dry-run)
                if not self._dry_run and self._source_checks:
                    if not self._run_source_checks(spark, snap_date, run_id, audit):
                        snap_status = "skipped_source_check"
                        continue
```

刪除後 `try:` 區塊內第一個動作直接是 `# Execute tables` 迴圈。

- [ ] **Step 5: 用 public `run_source_checks` 取代 private `_run_source_checks`**

把 `sql_runner.py:342-370` 的 `_run_source_checks(self, spark, snap_date, run_id, audit) -> bool`
整個方法換成：

```python
    def run_source_checks(
        self,
        target_dates: list[str],
        run_id: str | None = None,
    ) -> None:
        """Preflight：對全部 target_dates 跑 source_checks，collect-all。

        全部跑完後若有任一失敗，raise SourceCheckError（攜帶結構化失敗清單），
        並對每筆失敗寫 etl_audit_log（table_name="__source_check__"）。不寫任何
        輸出表。
        """
        if run_id is None:
            run_id = generate_run_id()
        if not self._source_checks:
            logger.warning("No source_checks configured for %s; nothing to check.", self._stage)
            return

        spark, audit = self._initialize_context()
        checker = SourceChecker(spark)

        all_results: list[CheckResult] = []
        for snap_date in target_dates:
            all_results.extend(checker.run_all(self._source_checks, snap_date))

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
            "Source check passed: %d/%d checks (%s)",
            len(all_results), len(all_results), self._stage,
        )
```

- [ ] **Step 6: 跑測試確認 PASS（含既有 test_sql_runner 不回歸）**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py -q`
Expected: PASS（`TestRunSourceChecks` 綠；既有 dry-run/order/CTAS/insert/schema-evolution/SQL-error 測試不受影響）。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/source_etl/sql_runner.py tests/test_pipelines/test_source_etl/test_sql_runner.py
git commit -m "feat(source_etl): add run_source_checks() preflight, drop in-run source gate" --no-verify
```

---

## Task 5: output checks 改 fail-fast（run() 中止整個 run）

**Files:**
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:113-149`（run() 的 table 迴圈）、`:189-271`（`_process_single_table`）、`:372-424`（`_run_output_checks`）
- Test: `tests/test_pipelines/test_source_etl/test_sql_runner.py`

- [ ] **Step 1: 寫 failing test**

```python
class TestOutputCheckFailFast:
    def test_output_check_failure_raises_and_stops(self, sql_dir, monkeypatch):
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=False, stage="feature_etl")
        spark = _make_spark_mock(table_exists=False)  # CTAS 路徑、寫表成功
        monkeypatch.setattr(runner, "_initialize_context", lambda: (spark, None))

        # 讓第一張表的 output check 失敗
        from recsys_tfb.pipelines.source_etl import sql_runner as sr_mod

        def fake_run_all(self, table_config, target_db, snap_date):
            return [CheckResult(
                False, "dup too high", table=table_config.name,
                check="max_duplicate_key_ratio", snap_date=snap_date,
                expected="<= 0.0", actual="0.5",
            )]

        monkeypatch.setattr(sr_mod.OutputChecker, "run_all", fake_run_all)

        with pytest.raises(OutputCheckError) as ei:
            runner.run(target_dates=["2025-01-31", "2025-02-28"], run_id="r1")
        # fail-fast：停在第一張表（feature_aum），不續做後續表/日期
        assert ei.value.table == "feature_aum"
        assert ei.value.snap_date == "2025-01-31"
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py::TestOutputCheckFailFast -q`
Expected: FAIL（目前 output check 失敗回傳 -1、`run()` 不 raise → 不會 raise `OutputCheckError`）。

- [ ] **Step 3: `_run_output_checks` 改 raise**

把 `_run_output_checks` 失敗分支改成 raise（保留 success audit 與 row_count 取值）：

```python
        if failed:
            if audit:
                audit.write_record(
                    AuditRecord(
                        run_id=run_id,
                        snap_date=snap_date,
                        table_name=table.name,
                        status="failed",
                        row_count=row_count,
                        duration_seconds=duration,
                        error_message="; ".join(r.message for r in failed),
                    )
                )
            raise OutputCheckError(self._stage, table.name, snap_date, failed)

        if audit:
            audit.write_record(
                AuditRecord(
                    run_id=run_id, snap_date=snap_date, table_name=table.name,
                    status="success", row_count=row_count, duration_seconds=duration,
                )
            )
        return row_count
```

（回傳型別仍 `int`，但失敗時改走 raise，不再回 -1。）

- [ ] **Step 4: `_process_single_table` 末段不再判斷 row_count**

把：

```python
        row_count = self._run_output_checks(
            spark, table, snap_date, run_id, audit, duration
        )
        return row_count >= 0
```

改成：

```python
        self._run_output_checks(spark, table, snap_date, run_id, audit, duration)
        return True
```

（`_run_output_checks` 失敗即 raise，走到這裡代表成功。回傳值留 `True` 以最小化呼叫端改動。）

- [ ] **Step 5: `run()` 的 table 迴圈移除 break/continue**

把 `run()` 內：

```python
                # Execute tables
                for table in tables_to_run:
                    success = self._process_single_table(
                        spark, table, snap_date, run_id, audit
                    )
                    if not success:
                        # Output-quality failure: record and stop this snap_date.
                        snap_status = "failed"
                        break
```

改成：

```python
                # Execute tables
                for table in tables_to_run:
                    self._process_single_table(spark, table, snap_date, run_id, audit)
```

`except SourceETLError:` 區塊（會接住 `OutputCheckError`，因其為子類）維持不變——
設 `snap_status = "failed"` 後 `raise`，使整個 run 中止、後續 snap_date 不執行。

- [ ] **Step 6: 跑測試確認 PASS（含既有不回歸）**

Run: `PYTEST tests/test_pipelines/test_source_etl/test_sql_runner.py -q`
Expected: PASS（`TestOutputCheckFailFast` 綠；`test_sql_error_aborts_remaining_snap_dates` 等既有測試仍綠，因 `OutputCheckError`/`SourceETLError` 都被同一 except 接住）。

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/source_etl/sql_runner.py tests/test_pipelines/test_source_etl/test_sql_runner.py
git commit -m "feat(source_etl): output quality checks fail-fast abort the whole run" --no-verify
```

---

## Task 6: CLI `--source-check` 旗標 ＋ `_run_etl` 分支

**Files:**
- Modify: `src/recsys_tfb/__main__.py:177-285`（`_run_etl` ＋ 三個 ETL 指令）
- Test: `tests/test_cli.py`

- [ ] **Step 1: 寫 failing test**

加到 `test_cli.py`（檔頭已有 `from recsys_tfb.__main__ import app`、`runner = CliRunner()`、`_setup_conf`、`os`、`patch`）：

```python
def _setup_etl_conf(tmp_path, source_checks=None):
    """conf/base + parameters_feature_etl.yaml（最小可跑 _run_etl）。"""
    _setup_conf(tmp_path)
    base_dir = tmp_path / "conf" / "base"
    params = {
        "feature_etl": {
            "variables": {"target_db": "ml_recsys"},
            "source_checks": source_checks or {},
            "tables": [
                {"name": "feature_table", "sql_file": "feature/feature_table.sql",
                 "partition_by": {"snap_date": "DATE"},
                 "primary_key": ["snap_date", "cust_id"]},
            ],
        }
    }
    with open(base_dir / "parameters_feature_etl.yaml", "w") as f:
        yaml.dump(params, f)


class TestSourceCheckCLI:
    def test_flag_in_help(self):
        for cmd in ("feature_etl", "label_etl", "sample_pool_etl"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, result.output
            assert "--source-check" in result.output

    def test_source_check_pass_exit0_no_etl(self, tmp_path):
        _setup_etl_conf(tmp_path, source_checks={"feat_a": {"partition_key": "snap_date"}})
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                inst = MockRunner.return_value
                inst.run_source_checks.return_value = None
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 0, result.output
            inst.run_source_checks.assert_called_once()
            inst.run.assert_not_called()           # 不寫表
        finally:
            os.chdir(old)

    def test_source_check_fail_exit1_no_etl(self, tmp_path):
        from recsys_tfb.pipelines.source_etl.sql_runner import SourceCheckError
        from recsys_tfb.pipelines.source_etl.checks import CheckResult
        _setup_etl_conf(tmp_path, source_checks={"feat_a": {"partition_key": "snap_date"}})
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                inst = MockRunner.return_value
                inst.run_source_checks.side_effect = SourceCheckError(
                    [CheckResult(False, "bad", table="feat_a", check="partition_exists",
                                 snap_date="2025-01-31", expected="x", actual="not found")],
                    "feature_etl",
                )
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 1, result.output
            inst.run.assert_not_called()
        finally:
            os.chdir(old)

    def test_source_check_with_restart_from_errors(self, tmp_path):
        _setup_etl_conf(tmp_path)
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--restart-from", "feature_table",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 1, result.output
            MockRunner.return_value.run_source_checks.assert_not_called()
        finally:
            os.chdir(old)
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `PYTEST tests/test_cli.py::TestSourceCheckCLI -q`
Expected: FAIL（三個 ETL 指令尚無 `--source-check` 旗標）。

- [ ] **Step 3: `_run_etl` 加 `source_check_only` 分支**

`__main__.py` 把 `_run_etl` 簽章與 body 改成（含 restart 衝突 fail-fast、source-check 強制非 dry-run、例外攔截）：

```python
def _run_etl(
    stage: str,
    env: str,
    target_dates: Optional[str],
    restart_from: Optional[str],
    source_check_only: bool = False,
) -> None:
    """Shared executor for the feature/label/sample_pool ETL sub-commands."""
    from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner, SourceCheckError
    from recsys_tfb.utils.spark import get_or_create_spark_session

    # restart-from 對純檢查無意義 → 先報錯（不必起 Spark）
    if source_check_only and restart_from:
        logger.error("--source-check 與 --restart-from 不能同時使用（檢查不寫表，無從續跑）。")
        raise typer.Exit(code=1)

    config, params, run_context = _load_config_and_setup(stage, env)

    spark_configs = _load_spark_config(config, stage)
    get_or_create_spark_session(spark_configs)

    conf_dir = _find_conf_dir()

    params_etl = config.get_parameters_by_name(f"parameters_{stage}")
    etl_config = params_etl.get(stage, params_etl)
    sql_dir = conf_dir / "sql" / "etl"
    dry_run = etl_config.get("dry_run", env == "local")

    if target_dates:
        date_list = [d.strip() for d in target_dates.split(",")]
    else:
        date_list = etl_config.get("target_dates", [])
    if not date_list:
        logger.error("No target_dates provided. Use --target-dates or set in config.")
        raise typer.Exit(code=1)

    rendered_sql_dir_str = etl_config.get("rendered_sql_dir")
    rendered_sql_dir = Path(rendered_sql_dir_str) if rendered_sql_dir_str else None

    runner = SQLRunner(
        config=etl_config,
        sql_dir=sql_dir,
        dry_run=False if source_check_only else dry_run,  # 檢查唯讀、必須實查 Hive
        rendered_sql_dir=rendered_sql_dir,
        stage=stage,
    )

    if source_check_only:
        try:
            runner.run_source_checks(target_dates=date_list, run_id=run_context.run_id)
        except SourceCheckError as exc:
            logger.error("%s", exc)
            raise typer.Exit(code=1)
        logger.info("Source check completed: %s", stage)
        return

    try:
        runner.run(
            target_dates=date_list,
            restart_from=restart_from,
            run_id=run_context.run_id,
        )
    except Exception:
        logger.exception("%s pipeline failed", stage)
        raise typer.Exit(code=1)

    logger.info("Pipeline '%s' completed successfully", stage)
```

- [ ] **Step 4: 三個 ETL 指令加 `--source-check` 旗標並轉傳**

`feature_etl` / `label_etl` / `sample_pool_etl` 各加同一個參數，並把它傳進 `_run_etl`。以 `feature_etl` 為例：

```python
@app.command(name="feature_etl")
def feature_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None, "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None, "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the feature ETL pipeline (feature_aum/sav/ccard/info/concat/table)."""
    _run_etl("feature_etl", env, target_dates, restart_from, source_check_only=source_check)
```

`label_etl`、`sample_pool_etl` 比照（各自保留原本 docstring 與 stage 名；新增同樣的
`source_check` 參數，呼叫 `_run_etl("<stage>", env, target_dates, restart_from,
source_check_only=source_check)`）。

- [ ] **Step 5: 跑測試確認 PASS（含既有 CLI 測試不回歸）**

Run: `PYTEST tests/test_cli.py -q`
Expected: PASS（`TestSourceCheckCLI` 綠；既有 `test_etl_subcommands_advertise_target_dates` 等不受影響）。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): add --source-check preflight flag to ETL subcommands" --no-verify
```

---

## Task 7: 文件（source_etl.md / README / change-guide）

**Files:**
- Modify: `docs/pipelines/source_etl.md:45-72`（關鍵設定 ＋ 重跑語意）、`:74-80`（指令）
- Modify: `README.md`（source_etl 段落）
- Modify: `docs/change-guide.md`（如有觸及 source_check 失敗行為的條目）

- [ ] **Step 1: `source_etl.md` 補 source_checks 設定參考表**

在 §關鍵設定「stage 層級」清單的 `source_checks` 那行下方，插入：

````markdown
### `source_checks` 設定

`source_checks` 是「上游表 → 檢查設定」的 map，由 `--source-check` preflight 模式
執行（見下方指令）。每個 key 是上游表 FQN：

| 欄位 | 型別 | 預設 | 意義 |
|---|---|---|---|
| key（表名） | str | — | 上游表 FQN，如 `feature_store.feat_aum` |
| `partition_key` | str | （必填） | 分區欄名；檢查 `<partition_key>=<snap_date>` 是否存在 |
| `min_row_count` | int | `0` | 該分區最少列數；`0` 表示不檢查列數 |
| `expected_columns` | map{col: type} | `{}` | 期望欄位→型別；缺欄或型別不符即失敗；空表示不檢查 |
| `allow_new_columns` | bool | `true` | `false` 時，上游多出 `expected_columns` 以外的欄位即失敗 |

```yaml
feature_etl:
  source_checks:
    feature_store.feat_aum:
      partition_key: snap_date
      min_row_count: 1000000
      expected_columns:
        cust_id: string
        aum_bal: decimal(18,2)
      allow_new_columns: true
```
````

- [ ] **Step 2: `source_etl.md` 改寫 §重跑語意的「檢查失敗的後果」**

把第 72 行那段（`source_checks` 失敗跳過該 snap_date…）整段替換為：

```markdown
- **檢查 / 執行失敗的後果**：
  - `source_checks`（上游）：只在 `--source-check` preflight 模式跑。會把**全部**
    target_dates × 全部檢查跑完（collect-all），有任一失敗即印出彙整報告（含
    expected vs actual、依檢查類型的修復提示、僅含失敗日期的重跑指令）並以非零碼結束；
    **不寫任何表**。正常 ETL run（不帶 `--source-check`）**不再跑 source_checks**。
  - `quality_checks`（輸出）：任一失敗即 **fail-fast 中止整個 run**、以非零碼結束，並
    印出 `--restart-from <table>` 續跑指引（修復後從失敗那張表續跑、跳過先前已寫的表）。
  - SQL / Spark 執行錯誤（如表不存在 / schema 不符）：中止整個 run。
  - 三者都記入 `etl_audit_log`。
```

同時刪除/修正 §dry-run 那行裡「不跑 source_checks」的描述（改為「不寫表」；source_checks
本就只在 `--source-check` 模式跑，與 dry-run 無關）。

- [ ] **Step 3: `source_etl.md` §指令補 `--source-check` 工作流**

在 §指令 code block 後補：

````markdown
**先 preflight 再正式跑**（建議工作流）：

```bash
# 1) 先驗上游（唯讀、不寫表；全部跑完有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31
# 2) 通過後再實際寫表
python -m recsys_tfb feature_etl              --target-dates 2025-01-31
```

`--source-check` 不可與 `--restart-from` 併用（檢查不寫表，無從續跑）。
````

- [ ] **Step 4: `README.md` source_etl 段落補一句**

在 README 介紹 source_etl 指令處，補一句：「先用 `feature_etl --source-check` 對上游
做 preflight（唯讀、全部跑完、有失敗即 exit 1 並印修復指引），通過後再不帶旗標實際寫表；
輸出品質檢查失敗會 fail-fast 中止整個 run。」（對齊 README 既有行文與識別字）。

- [ ] **Step 5: `docs/change-guide.md` 對齊**

`grep -n "source_check\|source_etl\|quality_check" docs/change-guide.md`；若有描述舊
「跳過該 snap_date 繼續」行為的條目，改為新語意（preflight gate ＋ output fail-fast）。
若無觸及則不動。

- [ ] **Step 6: Commit**

```bash
git add docs/pipelines/source_etl.md README.md docs/change-guide.md
git commit -m "docs: document --source-check preflight and fail-fast output checks" --no-verify
```

---

## Task 8: 全套回歸 ＋ graphify 重建

**Files:** 無新增；驗證與圖更新。

- [ ] **Step 1: 跑 source_etl ＋ CLI 全套測試**

Run:
```
PYTEST tests/test_pipelines/test_source_etl/ tests/test_cli.py -q
```
Expected: 全 PASS。

- [ ] **Step 2: 對 spec 做覆蓋自查**

逐條對 `docs/superpowers/specs/2026-06-07-source-check-subcommand-design.md` §6 受影響檔案
與 §7 測試計畫，確認每項都有對應 commit；缺的補。

- [ ] **Step 3: graphify 重建（改過 code）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/source-check
python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 4: 開 PR（人工觸發 / 經使用者同意後）**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/source-check push -u origin feat/source-check-subcommand
gh pr create --fill --base main
```

> model promote / 對外操作需人工觸發；push / PR 在使用者同意後再做。

---

## 自我複審結果（writing-plans Self-Review）

- **Spec 覆蓋**：D1（Task 6）、D2（Task 4 Step 4 移除 gate）、D3（Task 5）、D4（Task 4
  collect-all）、D5（Task 4 audit ＋ Task 3 報告）、D6（Task 6 restart 衝突）、D7（Task 1/2
  enrich）；文件（Task 7）；測試（各 Task ＋ Task 8）。皆有對應 task。
- **Placeholder 掃描**：無 TBD/TODO；每個 code step 都有完整程式碼與預期輸出。
- **型別一致性**：`SourceCheckError(results, stage)`、`OutputCheckError(stage, table,
  snap_date, failed)`、`SQLRunner(..., stage=...)`、`run_source_checks(target_dates,
  run_id)`、`CheckResult(... table/check/snap_date/expected/actual)` 在各 task 一致。

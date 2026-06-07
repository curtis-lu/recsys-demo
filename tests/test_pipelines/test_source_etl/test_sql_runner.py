"""Tests for SQLRunner."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import DoubleType, StructField, StructType, StringType

from recsys_tfb.pipelines.source_etl.sql_runner import (
    OutputCheckError,
    SourceCheckError,
    SourceETLError,
    SQLRunner,
)
from recsys_tfb.pipelines.source_etl.checks import CheckResult


@pytest.fixture()
def sql_dir(tmp_path):
    """Create minimal SQL files for testing."""
    feat = tmp_path / "feature"
    feat.mkdir()
    (feat / "feature_aum.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT cust_id, total_aum\n"
        "FROM feature_store.feat_aum\n"
        "WHERE snap_date = '${target_date}'\n"
    )
    (feat / "feature_sav.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT cust_id, sav_amt\n"
        "FROM feature_store.feat_sav\n"
        "WHERE snap_date = '${target_date}'\n"
    )
    (feat / "feature_concat.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT a.cust_id, a.total_aum, b.sav_amt\n"
        "FROM ${target_db}.feature_aum a\n"
        "JOIN ${target_db}.feature_sav b ON a.cust_id = b.cust_id\n"
        "WHERE a.snap_date = '${target_date}'\n"
    )
    return tmp_path


def _base_config():
    return {
        "variables": {"target_db": "ml_feature"},
        "tables": [
            {
                "name": "feature_aum",
                "sql_file": "feature/feature_aum.sql",
                "partition_by": {"snap_date": "DATE"},
                "primary_key": ["snap_date", "cust_id"],
            },
            {
                "name": "feature_sav",
                "sql_file": "feature/feature_sav.sql",
                "partition_by": {"snap_date": "DATE"},
            },
            {
                "name": "feature_concat",
                "sql_file": "feature/feature_concat.sql",
                "partition_by": {"snap_date": "DATE"},
                "depends_on": ["feature_aum", "feature_sav"],
            },
        ],
    }


def _make_spark_mock(
    columns=None, table_exists=False, existing_columns=None, select_type_overrides=None
):
    cols = columns or ["cust_id", "total_aum", "snap_date"]
    overrides = select_type_overrides or {}
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    limit0_df = MagicMock()
    limit0_df.columns = cols
    # probe.schema drives ALTER-column type inference; default StringType, but a
    # caller can override specific columns to assert non-string type plumbing.
    limit0_df.schema = StructType(
        [StructField(c, overrides.get(c, StringType())) for c in cols]
    )
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


class TestValidateOrder:
    def test_valid_order(self, sql_dir):
        """Tables in correct dependency order should pass."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        assert len(runner._tables) == 3

    def test_invalid_order(self, sql_dir):
        """Dependency appearing after dependent should raise."""
        config = _base_config()
        # Move feature_concat to the first position
        config["tables"] = [config["tables"][2], config["tables"][0], config["tables"][1]]
        with pytest.raises(ValueError, match="depends on 'feature_aum'"):
            SQLRunner(config, sql_dir, dry_run=True)

    def test_missing_dependency(self, sql_dir):
        """Reference to non-existent table should raise."""
        config = _base_config()
        config["tables"][2]["depends_on"] = ["feature_aum", "feature_sav", "nonexistent"]
        with pytest.raises(ValueError, match="depends on 'nonexistent'"):
            SQLRunner(config, sql_dir, dry_run=True)


class TestDryRun:
    def test_dry_run_renders_sql(self, sql_dir, capsys):
        """Dry run should render SQL without executing."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        runner.run(target_dates=["2024-01-31"])
        # No SparkSession created, no errors

    def test_dry_run_restart_from(self, sql_dir, caplog):
        """Restart from should skip tables before the specified one."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with caplog.at_level("INFO"):
            runner.run(target_dates=["2024-01-31"], restart_from="feature_concat")
        # feature_aum and feature_sav should be skipped
        assert any("Skipping feature_aum" in m for m in caplog.messages)
        assert any("Skipping feature_sav" in m for m in caplog.messages)
        assert any("DRY RUN [feature_concat]" in m for m in caplog.messages)

    def test_dry_run_multiple_dates(self, sql_dir, caplog):
        """Dry run should process multiple snap dates."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with caplog.at_level("INFO"):
            runner.run(target_dates=["2024-01-31", "2024-02-29"])
        # Both dates should be processed
        assert any("2024-01-31" in m for m in caplog.messages)
        assert any("2024-02-29" in m for m in caplog.messages)


class TestRestartFromValidation:
    def test_invalid_restart_from(self, sql_dir):
        """Restart from non-existent table should raise."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with pytest.raises(ValueError, match="not found in tables"):
            runner.run(target_dates=["2024-01-31"], restart_from="nonexistent")


class TestRenderedSqlDir:
    def test_files_written_in_dry_run(self, sql_dir, tmp_path):
        """rendered_sql_dir writes one .sql file per table per snap_date."""
        out_dir = tmp_path / "rendered_sql"
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True, rendered_sql_dir=out_dir)
        runner.run(target_dates=["2024-01-31"], run_id="test_run")

        snap_dir = out_dir / "test_run" / "2024-01-31"
        assert snap_dir.is_dir()
        written = {f.name for f in snap_dir.iterdir()}
        assert written == {"feature_aum.sql", "feature_sav.sql", "feature_concat.sql"}

        content = (snap_dir / "feature_aum.sql").read_text()
        assert "INSERT OVERWRITE TABLE" in content
        assert "PARTITION" in content
        assert "2024-01-31" in content

    def test_no_files_without_rendered_sql_dir(self, sql_dir, tmp_path):
        """Without rendered_sql_dir, no files should be written."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        runner.run(target_dates=["2024-01-31"], run_id="test_run")
        # No rendered_sql directory should be created anywhere
        assert not any((tmp_path / p).exists() for p in ["rendered_sql"])

    def test_multiple_target_dates_separate_dirs(self, sql_dir, tmp_path):
        """Each target_date gets its own subdirectory."""
        out_dir = tmp_path / "rendered_sql"
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True, rendered_sql_dir=out_dir)
        runner.run(target_dates=["2024-01-31", "2024-02-29"], run_id="test_run")

        assert (out_dir / "test_run" / "2024-01-31").is_dir()
        assert (out_dir / "test_run" / "2024-02-29").is_dir()


class TestSourceChecksConfig:
    def test_source_checks_parsed(self, sql_dir):
        """Source checks from config should be parsed into SourceCheckConfig."""
        config = _base_config()
        config["source_checks"] = {
            "feature_store.feat_aum": {
                "partition_key": "snap_date",
                "min_row_count": 1000000,
                "expected_columns": {"cust_id": "string"},
            }
        }
        runner = SQLRunner(config, sql_dir, dry_run=True)
        assert len(runner._source_checks) == 1
        assert runner._source_checks[0].table_name == "feature_store.feat_aum"
        assert runner._source_checks[0].min_row_count == 1000000


def _all_pass_output_checks(self, table_config, target_db, snap_date):
    """Stub for OutputChecker.run_all — returns no failures.

    Tests that focus on SQL generation (CTAS / INSERT OVERWRITE / schema
    evolution) use real Spark mocks that cannot satisfy the schema_contract
    check. Patch with this stub so output-check failures don't obscure the
    SQL-emission assertions.
    """
    return []


class TestProcessSingleTableFirstRun:
    def test_emits_ctas_and_writes_rendered_sql(self, tmp_path, sql_dir):
        """First run (table absent) should emit Hive CTAS and write it to rendered_sql."""
        spark = _make_spark_mock(table_exists=False)
        config = {
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
        runner = SQLRunner(config, sql_dir, rendered_sql_dir=tmp_path / "rendered")
        from recsys_tfb.pipelines.source_etl.checks import OutputChecker
        with patch.object(runner, "_initialize_context", return_value=(spark, None)), \
             patch.object(OutputChecker, "run_all", _all_pass_output_checks):
            runner.run(["2026-03-31"])
        files = list((tmp_path / "rendered").rglob("feature_aum.sql"))
        assert files, "rendered SQL not written"
        content = files[0].read_text()
        assert "STORED AS PARQUET" in content
        assert "INSERT OVERWRITE" not in content


class TestProcessSingleTableExistingRun:
    def test_emits_insert_overwrite(self, tmp_path, sql_dir):
        """Subsequent run (table present) should emit INSERT OVERWRITE with CAST."""
        spark = _make_spark_mock(table_exists=True)
        config = {
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
        runner = SQLRunner(config, sql_dir, rendered_sql_dir=tmp_path / "rendered")
        from recsys_tfb.pipelines.source_etl.checks import OutputChecker
        with patch.object(runner, "_initialize_context", return_value=(spark, None)), \
             patch.object(OutputChecker, "run_all", _all_pass_output_checks):
            runner.run(["2026-03-31"])
        files = list((tmp_path / "rendered").rglob("feature_aum.sql"))
        assert files
        content = files[0].read_text()
        assert "INSERT OVERWRITE" in content
        assert "CAST(snap_date AS DATE)" in content


class TestFailFast:
    def test_sql_error_aborts_remaining_snap_dates(self, sql_dir):
        """An SQL execution failure must raise SourceETLError and prevent
        subsequent snap_dates from being processed."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        limit0_df = MagicMock()
        limit0_df.columns = ["cust_id", "total_aum", "snap_date"]

        call_count = [0]

        def _side_effect(sql, *args, **kwargs):
            call_count[0] += 1
            # First call = column probe (LIMIT 0); succeed.
            if call_count[0] == 1:
                return limit0_df
            # Second call = the real INSERT/CTAS; simulate Spark ParseException.
            raise RuntimeError("ParseException: syntax error near 'foo'")

        spark.sql.side_effect = _side_effect

        config = {
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
        runner = SQLRunner(config, sql_dir)
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
            with pytest.raises(SourceETLError, match="feature_aum"):
                runner.run(["2026-03-31", "2026-04-30"])

        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        assert not any("2026-04-30" in s for s in executed), (
            "second snap_date was processed despite first failing"
        )


class TestProcessSingleTableMissingPartition:
    def test_no_insert_when_partition_col_absent(self, tmp_path, sql_dir):
        """If SELECT output lacks a partition column, no INSERT OVERWRITE is executed."""
        spark = _make_spark_mock(columns=["cust_id", "total_aum"], table_exists=True)
        config = {
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
        runner = SQLRunner(config, sql_dir)
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
            with pytest.raises(SourceETLError, match="Partition columns missing"):
                runner.run(["2026-03-31"])
        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        assert not any("INSERT OVERWRITE" in s for s in executed)


class TestSchemaEvolution:
    # NOTE: _make_spark_mock returns a STATIC existing-table schema, so these
    # tests cover the single-snap_date case. In production the ALTER actually
    # mutates the table, so a later snap_date re-reads the evolved schema via
    # spark.table(fqn) and emits no further ALTER. That per-date no-re-ALTER
    # behavior is a property of "read existing schema each call" (pinned by the
    # single-date tests below) rather than something this static mock can express.
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

    def test_new_column_triggers_alter_then_insert_in_table_order(self, sql_dir):
        # 既有表: cust_id, total_aum, snap_date(part)；SELECT 多了 sav_amt
        spark = _make_spark_mock(
            columns=["cust_id", "total_aum", "sav_amt", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        from recsys_tfb.pipelines.source_etl.checks import OutputChecker
        with patch.object(runner, "_initialize_context", return_value=(spark, None)), \
             patch.object(OutputChecker, "run_all", _all_pass_output_checks):
            runner.run(["2026-03-31"])

        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        alter = [s for s in executed if "ALTER TABLE" in s]
        insert = [s for s in executed if "INSERT OVERWRITE" in s]
        assert alter, "ALTER TABLE not executed for new column"
        assert "ADD COLUMNS (sav_amt string)" in alter[0]
        assert "ml_feature.feature_aum" in alter[0]
        assert insert, "INSERT OVERWRITE not executed"
        # 投影照表欄序：既有欄保序在前、新欄 append 在後
        assert insert[0].index("cust_id") < insert[0].index("total_aum")
        assert insert[0].index("total_aum") < insert[0].index("sav_amt")
        # ALTER 必須在 INSERT 之前執行
        assert executed.index(alter[0]) < executed.index(insert[0])

    def test_new_column_type_inferred_from_probe_schema(self, sql_dir):
        # 新欄的 ALTER 型別取自 SELECT probe schema，不是寫死 string
        spark = _make_spark_mock(
            columns=["cust_id", "total_aum", "sav_amt", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
            select_type_overrides={"sav_amt": DoubleType()},
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        from recsys_tfb.pipelines.source_etl.checks import OutputChecker
        with patch.object(runner, "_initialize_context", return_value=(spark, None)), \
             patch.object(OutputChecker, "run_all", _all_pass_output_checks):
            runner.run(["2026-03-31"])
        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        alter = [s for s in executed if "ALTER TABLE" in s]
        assert alter, "ALTER TABLE not executed for new column"
        assert "ADD COLUMNS (sav_amt double)" in alter[0]

    def test_no_alter_when_columns_unchanged(self, sql_dir):
        spark = _make_spark_mock(
            columns=["cust_id", "total_aum", "snap_date"],
            table_exists=True,
            existing_columns=["cust_id", "total_aum", "snap_date"],
        )
        runner = SQLRunner(self._feature_aum_config(), sql_dir)
        from recsys_tfb.pipelines.source_etl.checks import OutputChecker
        with patch.object(runner, "_initialize_context", return_value=(spark, None)), \
             patch.object(OutputChecker, "run_all", _all_pass_output_checks):
            runner.run(["2026-03-31"])
        executed = [c.args[0] for c in spark.sql.call_args_list if c.args]
        assert not any("ALTER TABLE" in s for s in executed)
        assert any("INSERT OVERWRITE" in s for s in executed)

    def test_removed_column_fails_loud_no_write(self, sql_dir):
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


class TestRunSourceChecks:
    def _runner(self, sql_dir, source_checks):
        config = _base_config()
        config["source_checks"] = source_checks
        return SQLRunner(config, sql_dir, dry_run=False, stage="feature_etl")

    def test_collect_all_then_raise(self, sql_dir, monkeypatch):
        runner = self._runner(sql_dir, {"feat_a": {"partition_key": "snap_date"}})
        monkeypatch.setattr(runner, "_initialize_context", lambda: (MagicMock(), None))

        from recsys_tfb.pipelines.source_etl import checks as checks_mod
        calls = []

        def fake_run_all(self, cfgs, snap_date):
            calls.append(snap_date)
            ok = CheckResult(True, "ok", table="feat_a", check="partition_exists",
                             snap_date=snap_date)
            bad = CheckResult(False, "bad", table="feat_a", check="row_count",
                              snap_date=snap_date, expected=">= 1", actual="0")
            return [ok] if snap_date == "2025-01-31" else [ok, bad]

        monkeypatch.setattr(checks_mod.SourceChecker, "run_all", fake_run_all)

        with pytest.raises(SourceCheckError) as ei:
            runner.run_source_checks(["2025-01-31", "2025-02-28"], run_id="r1")
        assert calls == ["2025-01-31", "2025-02-28"]
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
        runner.run_source_checks(["2025-01-31"], run_id="r1")  # must NOT raise

    def test_no_source_checks_warns_no_raise(self, sql_dir):
        runner = SQLRunner(_base_config(), sql_dir, dry_run=False, stage="feature_etl")
        runner.run_source_checks(["2025-01-31"], run_id="r1")  # must NOT raise

    def test_audit_record_written_per_failed_check(self, sql_dir, monkeypatch):
        runner = self._runner(sql_dir, {"feat_a": {"partition_key": "snap_date"}})
        audit = MagicMock()
        monkeypatch.setattr(runner, "_initialize_context", lambda: (MagicMock(), audit))

        from recsys_tfb.pipelines.source_etl import checks as checks_mod
        monkeypatch.setattr(
            checks_mod.SourceChecker, "run_all",
            lambda self, cfgs, d: [CheckResult(False, "bad", table="feat_a",
                                               check="row_count", snap_date=d,
                                               expected=">= 1", actual="0")],
        )

        with pytest.raises(SourceCheckError):
            runner.run_source_checks(["2025-01-31", "2025-02-28"], run_id="r1")

        # one audit record per failed result (2 dates × 1 fail each)
        assert audit.write_record.call_count == 2
        recs = [c.args[0] for c in audit.write_record.call_args_list]
        assert all(r.table_name == "__source_check__" for r in recs)
        assert all(r.status == "failed" for r in recs)
        assert {r.snap_date for r in recs} == {"2025-01-31", "2025-02-28"}


class TestOutputCheckFailFast:
    def test_output_check_failure_raises_and_stops(self, sql_dir, monkeypatch):
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=False, stage="feature_etl")
        spark = _make_spark_mock(table_exists=False)  # CTAS path, table "writes" ok
        monkeypatch.setattr(runner, "_initialize_context", lambda: (spark, None))

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
        # fail-fast: stop at the first table (feature_aum), no further tables/dates
        assert ei.value.table == "feature_aum"
        assert ei.value.snap_date == "2025-01-31"


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

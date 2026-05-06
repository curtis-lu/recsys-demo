"""Tests for SQLRunner."""

from unittest.mock import MagicMock, patch

import pytest

from recsys_tfb.pipelines.source_etl.sql_runner import SourceETLError, SQLRunner


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


def _make_spark_mock(columns=None, table_exists=False):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    limit0_df = MagicMock()
    limit0_df.columns = columns or ["cust_id", "total_aum", "snap_date"]
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
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
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
        with patch.object(runner, "_initialize_context", return_value=(spark, None)):
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

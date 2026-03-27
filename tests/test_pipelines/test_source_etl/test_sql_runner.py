"""Tests for SQLRunner."""

import pytest

from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner


@pytest.fixture()
def sql_dir(tmp_path):
    """Create minimal SQL files for testing."""
    feat = tmp_path / "feature"
    feat.mkdir()
    (feat / "feature_aum.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT cust_id, total_aum\n"
        "FROM feature_store.feat_aum\n"
        "WHERE snap_date = '${snap_date}'\n"
    )
    (feat / "feature_sav.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT cust_id, sav_amt\n"
        "FROM feature_store.feat_sav\n"
        "WHERE snap_date = '${snap_date}'\n"
    )
    (feat / "feature_concat.sql").write_text(
        "--partition by: snap_date\n\n"
        "SELECT a.cust_id, a.total_aum, b.sav_amt\n"
        "FROM ${target_db}.feature_aum a\n"
        "JOIN ${target_db}.feature_sav b ON a.cust_id = b.cust_id\n"
        "WHERE a.snap_date = '${snap_date}'\n"
    )
    return tmp_path


def _base_config():
    return {
        "variables": {"target_db": "ml_feature"},
        "tables": [
            {
                "name": "feature_aum",
                "sql_file": "feature/feature_aum.sql",
                "partition_by": ["snap_date"],
                "primary_key": ["snap_date", "cust_id"],
            },
            {
                "name": "feature_sav",
                "sql_file": "feature/feature_sav.sql",
                "partition_by": ["snap_date"],
            },
            {
                "name": "feature_concat",
                "sql_file": "feature/feature_concat.sql",
                "partition_by": ["snap_date"],
                "depends_on": ["feature_aum", "feature_sav"],
            },
        ],
    }


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
        runner.run(snap_dates=["2024-01-31"])
        # No SparkSession created, no errors

    def test_dry_run_restart_from(self, sql_dir, caplog):
        """Restart from should skip tables before the specified one."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with caplog.at_level("INFO"):
            runner.run(snap_dates=["2024-01-31"], restart_from="feature_concat")
        # feature_aum and feature_sav should be skipped
        assert any("Skipping feature_aum" in m for m in caplog.messages)
        assert any("Skipping feature_sav" in m for m in caplog.messages)
        assert any("DRY RUN [feature_concat]" in m for m in caplog.messages)

    def test_dry_run_multiple_dates(self, sql_dir, caplog):
        """Dry run should process multiple snap dates."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with caplog.at_level("INFO"):
            runner.run(snap_dates=["2024-01-31", "2024-02-29"])
        # Both dates should be processed
        assert any("2024-01-31" in m for m in caplog.messages)
        assert any("2024-02-29" in m for m in caplog.messages)


class TestRestartFromValidation:
    def test_invalid_restart_from(self, sql_dir):
        """Restart from non-existent table should raise."""
        config = _base_config()
        runner = SQLRunner(config, sql_dir, dry_run=True)
        with pytest.raises(ValueError, match="not found in tables"):
            runner.run(snap_dates=["2024-01-31"], restart_from="nonexistent")


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

"""Tests for source ETL data models."""

import pytest

from recsys_tfb.pipelines.source_etl.models import (
    AuditRecord,
    SourceCheckConfig,
    TableConfig,
)


class TestTableConfig:
    def test_from_dict_full(self):
        data = {
            "name": "feature_aum",
            "sql_file": "feature/feature_aum.sql",
            "partition_by": ["snap_date"],
            "primary_key": ["snap_date", "cust_id"],
            "depends_on": ["feature_info"],
            "quality_checks": {"min_row_count": 1000, "max_null_ratio": 0.05},
        }
        cfg = TableConfig.from_dict(data)
        assert cfg.name == "feature_aum"
        assert cfg.sql_file == "feature/feature_aum.sql"
        assert cfg.partition_by == ["snap_date"]
        assert cfg.primary_key == ["snap_date", "cust_id"]
        assert cfg.depends_on == ["feature_info"]
        assert cfg.quality_checks["min_row_count"] == 1000

    def test_from_dict_minimal(self):
        data = {
            "name": "feature_sav",
            "sql_file": "feature/feature_sav.sql",
            "partition_by": ["snap_date"],
        }
        cfg = TableConfig.from_dict(data)
        assert cfg.name == "feature_sav"
        assert cfg.primary_key == []
        assert cfg.depends_on == []
        assert cfg.quality_checks == {}

    def test_from_dict_missing_required_raises(self):
        with pytest.raises(KeyError):
            TableConfig.from_dict({"name": "x", "sql_file": "x.sql"})


class TestSourceCheckConfig:
    def test_from_dict_full(self):
        data = {
            "partition_key": "snap_date",
            "min_row_count": 500000,
            "expected_columns": {"cust_id": "string", "amt": "double"},
            "allow_new_columns": False,
        }
        cfg = SourceCheckConfig.from_dict("db.table", data)
        assert cfg.table_name == "db.table"
        assert cfg.partition_key == "snap_date"
        assert cfg.min_row_count == 500000
        assert cfg.expected_columns == {"cust_id": "string", "amt": "double"}
        assert cfg.allow_new_columns is False

    def test_from_dict_defaults(self):
        data = {"partition_key": "snap_date"}
        cfg = SourceCheckConfig.from_dict("db.t", data)
        assert cfg.min_row_count == 0
        assert cfg.expected_columns == {}
        assert cfg.allow_new_columns is True


class TestAuditRecord:
    def test_defaults(self):
        rec = AuditRecord(
            run_id="abc",
            snap_date="2024-01-31",
            table_name="feature_aum",
            status="success",
        )
        assert rec.row_count == 0
        assert rec.duration_seconds == 0.0
        assert rec.error_message == ""

    def test_full(self):
        rec = AuditRecord(
            run_id="abc",
            snap_date="2024-01-31",
            table_name="feature_aum",
            status="failed",
            row_count=100,
            duration_seconds=3.5,
            error_message="check failed",
        )
        assert rec.status == "failed"
        assert rec.row_count == 100
        assert rec.error_message == "check failed"

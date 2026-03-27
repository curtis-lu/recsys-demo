"""Tests for audit writer."""

from unittest.mock import MagicMock, call

from recsys_tfb.pipelines.source_etl.audit import AuditWriter
from recsys_tfb.pipelines.source_etl.models import AuditRecord


class TestAuditWriter:
    def _make_writer(self):
        spark = MagicMock()
        config = {"database": "ml_feature", "table": "etl_audit_log"}
        writer = AuditWriter(spark, config)
        return writer, spark

    def test_ensure_table_on_init(self):
        writer, spark = self._make_writer()
        create_call = spark.sql.call_args_list[0]
        sql = create_call[0][0]
        assert "CREATE TABLE IF NOT EXISTS ml_feature.etl_audit_log" in sql
        assert "PARTITIONED BY (snap_date STRING)" in sql

    def test_write_record(self):
        writer, spark = self._make_writer()
        spark.sql.reset_mock()

        record = AuditRecord(
            run_id="run_001",
            snap_date="2024-01-31",
            table_name="feature_aum",
            status="success",
            row_count=1500000,
            duration_seconds=120.5,
        )
        writer.write_record(record)

        sql = spark.sql.call_args[0][0]
        assert "INSERT INTO ml_feature.etl_audit_log" in sql
        assert "PARTITION (snap_date = '2024-01-31')" in sql
        assert "'run_001'" in sql
        assert "'feature_aum'" in sql
        assert "'success'" in sql
        assert "1500000" in sql

    def test_write_record_escapes_quotes(self):
        writer, spark = self._make_writer()
        spark.sql.reset_mock()

        record = AuditRecord(
            run_id="run_001",
            snap_date="2024-01-31",
            table_name="feature_aum",
            status="failed",
            error_message="can't parse column",
        )
        writer.write_record(record)

        sql = spark.sql.call_args[0][0]
        assert "can\\'t parse column" in sql

    def test_write_summary(self):
        writer, spark = self._make_writer()
        spark.sql.reset_mock()

        writer.write_summary("run_001", "2024-01-31", "success", 600.0)

        sql = spark.sql.call_args[0][0]
        assert "'__summary__'" in sql
        assert "'success'" in sql
        assert "600.0" in sql

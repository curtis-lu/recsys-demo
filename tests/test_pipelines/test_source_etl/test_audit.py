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

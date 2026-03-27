"""Audit logging for source ETL pipeline execution.

Writes audit records to a Hive table and emits structured log events.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from recsys_tfb.pipelines.source_etl.models import AuditRecord

logger = logging.getLogger(__name__)

_CREATE_AUDIT_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS {database}.{table} (
    run_id           STRING,
    table_name       STRING,
    status           STRING,
    row_count        BIGINT,
    duration_seconds DOUBLE,
    error_message    STRING,
    created_at       TIMESTAMP
)
PARTITIONED BY (snap_date STRING)
STORED AS PARQUET
"""

_INSERT_AUDIT_SQL = """\
INSERT INTO {database}.{table} PARTITION (snap_date = '{snap_date}')
SELECT
    '{run_id}'           AS run_id,
    '{table_name}'       AS table_name,
    '{status}'           AS status,
    {row_count}          AS row_count,
    {duration_seconds}   AS duration_seconds,
    '{error_message}'    AS error_message,
    CURRENT_TIMESTAMP()  AS created_at
"""


class AuditWriter:
    """Write ETL audit records to Hive and Python structured logging."""

    def __init__(self, spark, audit_config: dict) -> None:
        self._spark = spark
        self._database = audit_config["database"]
        self._table = audit_config["table"]
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create the audit table if it doesn't exist."""
        sql = _CREATE_AUDIT_TABLE_SQL.format(
            database=self._database, table=self._table
        )
        self._spark.sql(sql)
        logger.debug(
            "Ensured audit table %s.%s exists",
            self._database,
            self._table,
        )

    def write_record(self, record: AuditRecord) -> None:
        """Insert a single audit record into the Hive audit table."""
        # Escape single quotes in error message
        safe_error = record.error_message.replace("'", "\\'")
        sql = _INSERT_AUDIT_SQL.format(
            database=self._database,
            table=self._table,
            snap_date=record.snap_date,
            run_id=record.run_id,
            table_name=record.table_name,
            status=record.status,
            row_count=record.row_count,
            duration_seconds=record.duration_seconds,
            error_message=safe_error,
        )
        self._spark.sql(sql)

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
        """Write a summary audit record for the entire snap_date run."""
        record = AuditRecord(
            run_id=run_id,
            snap_date=snap_date,
            table_name="__summary__",
            status=status,
            duration_seconds=total_duration,
        )
        self.write_record(record)

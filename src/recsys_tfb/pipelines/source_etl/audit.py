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

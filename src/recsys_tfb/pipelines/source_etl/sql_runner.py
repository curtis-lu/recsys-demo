"""SQLRunner — core execution engine for the source ETL pipeline.

Reads YAML config, renders SQL templates, executes INSERT OVERWRITE
statements against Hive, and runs data quality checks.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from recsys_tfb.core.logging import generate_run_id
from recsys_tfb.pipelines.source_etl.audit import AuditWriter
from recsys_tfb.pipelines.source_etl.checks import (
    OutputChecker,
    SourceChecker,
)
from recsys_tfb.pipelines.source_etl.models import (
    AuditRecord,
    SourceCheckConfig,
    TableConfig,
)
from recsys_tfb.pipelines.source_etl.sql_renderer import SQLRenderer

logger = logging.getLogger(__name__)


class SourceETLError(Exception):
    """Raised when a source ETL check or execution fails."""


class SQLRunner:
    """Execute the source ETL pipeline: render SQL, run on Spark, validate."""

    def __init__(
        self,
        config: dict,
        sql_dir: Path,
        dry_run: bool = False,
    ) -> None:
        self._tables = [TableConfig.from_dict(t) for t in config["tables"]]
        self._source_checks = [
            SourceCheckConfig.from_dict(name, data)
            for name, data in config.get("source_checks", {}).items()
        ]
        self._variables = config.get("variables", {})
        self._audit_config = config.get("audit", {})
        self._sql_dir = sql_dir
        self._dry_run = dry_run
        self._renderer = SQLRenderer(sql_dir)
        self._target_db = self._variables.get("target_db", "default")

        # Validate depends_on consistency at init time
        self._validate_order()

    def _validate_order(self) -> None:
        """Verify that depends_on declarations are consistent with list order.

        For each table with depends_on, all dependencies must appear earlier
        in the tables list.
        """
        seen: set[str] = set()
        for table in self._tables:
            for dep in table.depends_on:
                if dep not in seen:
                    raise ValueError(
                        f"Table '{table.name}' depends on '{dep}', "
                        f"but '{dep}' does not appear before it in the tables list. "
                        f"Seen so far: {sorted(seen)}"
                    )
            seen.add(table.name)

    def run(
        self,
        snap_dates: list[str],
        restart_from: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Execute the ETL pipeline for the given snap dates.

        Args:
            snap_dates: List of snap_date strings to process.
            restart_from: If specified, skip tables before this one.
            run_id: External run ID to use. If None, generates a new one.
        """
        if run_id is None:
            run_id = generate_run_id()
        logger.info(
            "Source ETL run started: run_id=%s, snap_dates=%s, dry_run=%s",
            run_id,
            snap_dates,
            self._dry_run,
        )

        spark, audit = self._initialize_context()
        tables_to_run = self._get_tables_to_run(restart_from)

        for snap_date in snap_dates:
            logger.info("Processing snap_date=%s", snap_date)
            run_start = time.monotonic()

            # Source freshness checks (skip in dry-run)
            if not self._dry_run and self._source_checks:
                if not self._run_source_checks(spark, snap_date, run_id, audit):
                    continue  # skip this snap_date

            # Execute tables
            snap_status = "success"
            for table in tables_to_run:
                success = self._process_single_table(spark, table, snap_date, run_id, audit)
                if not success:
                    snap_status = "failed"
                    break

            # Summary
            total_duration = time.monotonic() - run_start
            if not self._dry_run and audit:
                audit.write_summary(run_id, snap_date, snap_status, total_duration)
            logger.info(
                "snap_date=%s finished: status=%s, duration=%.1fs",
                snap_date,
                snap_status,
                total_duration,
            )

    def _initialize_context(self) -> tuple:
        """Initialize and return the Spark context and AuditWriter."""
        if self._dry_run:
            return None, None
            
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        audit = None
        if self._audit_config:
            resolved_audit = {
                "database": self._audit_config["database"].replace(
                    "${target_db}", self._target_db
                ),
                "table": self._audit_config["table"],
            }
            audit = AuditWriter(spark, resolved_audit)
        return spark, audit

    def _get_tables_to_run(self, restart_from: str | None) -> list[TableConfig]:
        """Filter the tables list based on restart_from parameter."""
        if not restart_from:
            return self._tables
            
        table_names = [t.name for t in self._tables]
        if restart_from not in table_names:
            raise ValueError(
                f"restart_from='{restart_from}' not found in tables: {table_names}"
            )
            
        start_idx = next(i for i, t in enumerate(self._tables) if t.name == restart_from)
        for table in self._tables[:start_idx]:
            logger.info("Skipping %s (restart mode)", table.name)
            
        return self._tables[start_idx:]

    def _process_single_table(
        self,
        spark,
        table: TableConfig,
        snap_date: str,
        run_id: str,
        audit: AuditWriter | None
    ) -> bool:
        """Execute a single table rendering, Spark SQL processing, and output quality check."""
        variables = {**self._variables, "snap_date": snap_date}
        select_sql = self._renderer.render(table.sql_file, variables)
        full_sql = SQLRenderer.build_insert_overwrite(
            table, select_sql, self._target_db
        )

        if self._dry_run:
            logger.info(
                "DRY RUN [%s]:\n%s", table.name, full_sql
            )
            return True

        # Execute
        table_start = time.monotonic()
        try:
            logger.info("Executing %s ...", table.name)
            spark.sql(full_sql)
            duration = time.monotonic() - table_start
            logger.info(
                "Completed %s in %.1fs", table.name, duration
            )
        except Exception as exc:
            duration = time.monotonic() - table_start
            logger.error(
                "Failed %s after %.1fs: %s", table.name, duration, exc
            )
            if audit:
                audit.write_record(
                    AuditRecord(
                        run_id=run_id,
                        snap_date=snap_date,
                        table_name=table.name,
                        status="failed",
                        duration_seconds=duration,
                        error_message=str(exc),
                    )
                )
            return False

        # Output quality checks
        row_count = self._run_output_checks(
            spark, table, snap_date, run_id, audit, duration
        )
        if row_count < 0:
            return False
            
        return True

    def _run_source_checks(
        self,
        spark,
        snap_date: str,
        run_id: str,
        audit: AuditWriter | None,
    ) -> bool:
        """Run source freshness and schema checks. Returns True if all pass."""
        checker = SourceChecker(spark)
        results = checker.run_all(self._source_checks, snap_date)
        failed = [r for r in results if not r.passed]
        if failed:
            logger.error(
                "Source checks failed for snap_date=%s: %s",
                snap_date,
                [r.message for r in failed],
            )
            if audit:
                audit.write_record(
                    AuditRecord(
                        run_id=run_id,
                        snap_date=snap_date,
                        table_name="__source_check__",
                        status="failed",
                        error_message="; ".join(r.message for r in failed),
                    )
                )
            return False
        return True

    def _run_output_checks(
        self,
        spark,
        table: TableConfig,
        snap_date: str,
        run_id: str,
        audit: AuditWriter | None,
        duration: float,
    ) -> int:
        """Run output quality checks. Returns row_count or -1 on failure."""
        checker = OutputChecker(spark)
        results = checker.run_all(table, self._target_db, snap_date)
        failed = [r for r in results if not r.passed]

        # Get row count from results (first check is usually row_count)
        row_count = 0
        for r in results:
            if r.metric_value is not None and "row count" in r.message:
                row_count = int(r.metric_value)
                break

        if failed:
            logger.error(
                "Output checks failed for %s: %s",
                table.name,
                [r.message for r in failed],
            )
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
            return -1

        if audit:
            audit.write_record(
                AuditRecord(
                    run_id=run_id,
                    snap_date=snap_date,
                    table_name=table.name,
                    status="success",
                    row_count=row_count,
                    duration_seconds=duration,
                )
            )
        return row_count

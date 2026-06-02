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
        rendered_sql_dir: Path | None = None,
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
        self._rendered_sql_dir = rendered_sql_dir
        self._renderer = SQLRenderer(sql_dir)
        self._target_db = self._variables.get("target_db", "default")

        # Validate depends_on consistency at init time
        self._validate_order()

    def _write_rendered_sql(
        self, run_id: str, snap_date: str, table_name: str, sql: str
    ) -> None:
        out_dir = self._rendered_sql_dir / run_id / snap_date
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{table_name}.sql").write_text(sql, encoding="utf-8")
        logger.debug(
            "Rendered SQL written: %s/%s/%s.sql", run_id, snap_date, table_name
        )

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
        target_dates: list[str],
        restart_from: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Execute the ETL pipeline for the given target dates.

        Args:
            target_dates: List of target date strings (YYYY-MM-DD) to process.
                Each value is bound to the SQL variable ``${target_date}`` and to
                the Hive partition column ``snap_date`` for one iteration.
            restart_from: If specified, skip tables before this one.
            run_id: External run ID to use. If None, generates a new one.
        """
        if run_id is None:
            run_id = generate_run_id()
        logger.info(
            "Source ETL run started: run_id=%s, target_dates=%s, dry_run=%s",
            run_id,
            target_dates,
            self._dry_run,
        )

        spark, audit = self._initialize_context()
        tables_to_run = self._get_tables_to_run(restart_from)

        for snap_date in target_dates:
            logger.info("Processing snap_date=%s", snap_date)
            run_start = time.monotonic()
            snap_status = "success"
            try:
                # Source freshness checks (skip in dry-run)
                if not self._dry_run and self._source_checks:
                    if not self._run_source_checks(spark, snap_date, run_id, audit):
                        snap_status = "skipped_source_check"
                        continue  # skip this snap_date but keep processing the rest

                # Execute tables
                for table in tables_to_run:
                    success = self._process_single_table(
                        spark, table, snap_date, run_id, audit
                    )
                    if not success:
                        # Output-quality failure: record and stop this snap_date.
                        snap_status = "failed"
                        break
            except SourceETLError:
                # SQL/Spark execution error: abort the whole run after this iteration's
                # audit summary is written.
                snap_status = "failed"
                raise
            finally:
                total_duration = time.monotonic() - run_start
                if not self._dry_run and audit:
                    audit.write_summary(
                        run_id, snap_date, snap_status, total_duration
                    )
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
            
        from recsys_tfb.utils.spark import get_or_create_spark_session

        spark = get_or_create_spark_session()
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self._target_db}")
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
        audit: AuditWriter | None,
    ) -> bool:
        """Execute a single table rendering, Spark SQL processing, and output quality check."""
        variables = {**self._variables, "target_date": snap_date}
        select_sql = self._renderer.render(table.sql_file, variables)

        if self._dry_run:
            dry_sql = (
                "-- DRY RUN: table existence not checked; partition CAST skipped.\n"
                + SQLRenderer.build_insert_overwrite(
                    table,
                    SQLRenderer.strip_header_comments(select_sql),
                    self._target_db,
                )
            )
            if self._rendered_sql_dir:
                self._write_rendered_sql(run_id, snap_date, table.name, dry_sql)
            logger.info("DRY RUN [%s]:\n%s", table.name, dry_sql)
            return True

        # Real run: probe SELECT columns, then CTAS (new table) or schema-aware
        # INSERT OVERWRITE (existing table).
        table_start = time.monotonic()
        try:
            body = SQLRenderer.strip_header_comments(select_sql)
            probe = spark.sql(f"SELECT * FROM (\n{body}\n) _cols LIMIT 0")
            select_columns = probe.columns

            if not spark.catalog.tableExists(table.name, self._target_db):
                logger.info(
                    "Table %s.%s not found, creating via Hive CTAS",
                    self._target_db, table.name,
                )
                aligned_select = SQLRenderer.build_aligned_select(
                    select_sql, select_columns, table.partition_by
                )
                statements = [
                    SQLRenderer.build_hive_ctas(
                        table, aligned_select, self._target_db
                    )
                ]
            else:
                statements = self._build_existing_table_statements(
                    spark, table, select_sql, select_columns, probe
                )

            final_sql = "\n;\n".join(statements)
            if self._rendered_sql_dir:
                self._write_rendered_sql(run_id, snap_date, table.name, final_sql)

            logger.info("Executing %s ...", table.name)
            for stmt in statements:
                spark.sql(stmt)
            duration = time.monotonic() - table_start
            logger.info("Completed %s in %.1fs", table.name, duration)
        except Exception as exc:
            duration = time.monotonic() - table_start
            logger.error("Failed %s after %.1fs: %s", table.name, duration, exc)
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
            raise SourceETLError(
                f"{table.name} @ {snap_date}: {exc}"
            ) from exc

        row_count = self._run_output_checks(
            spark, table, snap_date, run_id, audit, duration
        )
        return row_count >= 0

    def _build_existing_table_statements(
        self,
        spark,
        table: TableConfig,
        select_sql: str,
        select_columns: list[str],
        probe,
    ) -> list[str]:
        """Reconcile the rendered SELECT against the existing table schema.

        Returns the ordered SQL statements to execute: an optional
        ``ALTER TABLE ADD COLUMNS`` (when the SELECT introduces new non-partition
        columns) followed by an ``INSERT OVERWRITE`` whose projection follows the
        TABLE's column order (existing columns first, new columns appended,
        partition columns cast last) so the positional insert lands correctly.

        Fail-loud (append-only policy): if the SELECT drops a column that the
        existing table has, raise — removing columns needs a versioned rebuild,
        not in-place overwrite.
        """
        fqn = f"{self._target_db}.{table.name}"
        part_lower = {k.lower() for k in table.partition_by}
        existing_nonpart = [
            f.name
            for f in spark.table(fqn).schema.fields
            if f.name.lower() not in part_lower
        ]
        existing_lower = {c.lower() for c in existing_nonpart}
        select_nonpart = [c for c in select_columns if c.lower() not in part_lower]
        select_lower = {c.lower() for c in select_nonpart}

        removed = [c for c in existing_nonpart if c.lower() not in select_lower]
        if removed:
            raise SourceETLError(
                f"Removing columns from existing table {fqn} is not supported in "
                f"source ETL: it breaks positional INSERT OVERWRITE and deployed "
                f"models. Removed columns: {removed}. Use a versioned rebuild "
                f"instead, or keep the column and exclude it downstream via "
                f"prepare_model_input.drop_columns."
            )

        new_cols = [c for c in select_nonpart if c.lower() not in existing_lower]
        statements: list[str] = []
        if new_cols:
            type_by_lower = {
                f.name.lower(): f.dataType.simpleString()
                for f in probe.schema.fields
            }
            new_with_types = [(c, type_by_lower[c.lower()]) for c in new_cols]
            logger.info(
                "Schema evolution on %s: ADD COLUMNS %s", fqn, new_with_types
            )
            statements.append(
                SQLRenderer.build_alter_add_columns(
                    table, new_with_types, self._target_db
                )
            )

        target_order = existing_nonpart + new_cols
        aligned_select = SQLRenderer.build_aligned_select_in_order(
            select_sql, select_columns, target_order, table.partition_by
        )
        statements.append(
            SQLRenderer.build_insert_overwrite(
                table, aligned_select, self._target_db
            )
        )
        return statements

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

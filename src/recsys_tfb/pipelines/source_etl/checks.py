"""Source freshness checks and output data quality checks."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from recsys_tfb.pipelines.source_etl.models import SourceCheckConfig, TableConfig

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Result of a single check."""

    passed: bool
    message: str
    metric_value: float | int | None = None
    # 報告用欄位（皆有預設，向後相容）
    table: str = ""
    check: str = ""          # source: partition_exists/row_count/schema_drift;
    #                          output: min_row_count/max_duplicate_key_ratio/max_null_ratio/schema_contract
    snap_date: str = ""
    expected: str = ""
    actual: str = ""


class SourceChecker:
    """Validate source table freshness and schema before ETL execution."""

    def __init__(self, spark) -> None:
        self._spark = spark

    def check_partition_exists(
        self, table: str, partition_key: str, snap_date: str
    ) -> CheckResult:
        """Check whether the expected partition exists in the source table."""
        try:
            partitions_df = self._spark.sql(f"SHOW PARTITIONS {table}")
            partition_values = [
                row[0] for row in partitions_df.collect()
            ]
            target = f"{partition_key}={snap_date}"
            expected = f"partition {target}"
            exists = any(target in p for p in partition_values)
            if exists:
                return CheckResult(
                    True, f"Partition {target} exists in {table}",
                    table=table, check="partition_exists",
                    expected=expected, actual="found",
                )
            return CheckResult(
                False, f"Partition {target} not found in {table}",
                table=table, check="partition_exists",
                expected=expected, actual="not found",
            )
        except Exception as exc:
            return CheckResult(
                False, f"Failed to check partitions for {table}: {exc}",
                table=table, check="partition_exists",
                expected=f"partition {partition_key}={snap_date}", actual=f"error: {exc}",
            )

    def check_row_count(
        self,
        table: str,
        partition_key: str,
        snap_date: str,
        min_count: int,
    ) -> CheckResult:
        """Check that the source partition has at least ``min_count`` rows."""
        sql = (
            f"SELECT COUNT(*) AS cnt FROM {table} "
            f"WHERE {partition_key} = '{snap_date}'"
        )
        row = self._spark.sql(sql).collect()[0]
        count = row["cnt"]
        passed = count >= min_count
        return CheckResult(
            passed,
            f"{table} row count: {count} (min: {min_count})",
            metric_value=count,
            table=table, check="row_count",
            expected=f">= {min_count}", actual=str(count),
        )

    def check_schema_drift(
        self,
        table: str,
        expected_columns: dict[str, str],
        allow_new_columns: bool = True,
    ) -> CheckResult:
        """Compare actual schema against expected columns.

        - Missing columns or type changes → fail
        - New columns → pass if ``allow_new_columns`` is True
        """
        if not expected_columns:
            return CheckResult(
                True, f"No schema expectations for {table}",
                table=table, check="schema_drift", expected="(none)", actual="ok",
            )

        desc_df = self._spark.sql(f"DESCRIBE {table}")
        actual = {
            row["col_name"]: row["data_type"]
            for row in desc_df.collect()
            if not row["col_name"].startswith("#")  # skip partition info header
        }

        errors: list[str] = []
        for col, expected_type in expected_columns.items():
            if col not in actual:
                errors.append(f"Missing column: {col}")
            elif actual[col] != expected_type:
                errors.append(
                    f"Type mismatch for {col}: expected {expected_type}, got {actual[col]}"
                )

        if not allow_new_columns:
            extra = set(actual.keys()) - set(expected_columns.keys())
            if extra:
                errors.append(f"Unexpected new columns: {sorted(extra)}")

        if errors:
            return CheckResult(
                False, f"Schema drift in {table}: {'; '.join(errors)}",
                table=table, check="schema_drift",
                expected="declared columns present & typed",
                actual="; ".join(errors),
            )
        return CheckResult(
            True, f"Schema OK for {table}",
            table=table, check="schema_drift",
            expected="declared columns present & typed", actual="ok",
        )

    def run_all(
        self, checks: list[SourceCheckConfig], snap_date: str
    ) -> list[CheckResult]:
        """Run all source checks and return results."""
        results: list[CheckResult] = []
        for cfg in checks:
            # Partition exists
            result = self.check_partition_exists(
                cfg.table_name, cfg.partition_key, snap_date
            )
            results.append(result)
            logger.info(result.message, extra={"event": "source_check", "passed": result.passed})
            if not result.passed:
                continue

            # Row count
            if cfg.min_row_count > 0:
                result = self.check_row_count(
                    cfg.table_name, cfg.partition_key, snap_date, cfg.min_row_count
                )
                results.append(result)
                logger.info(result.message, extra={"event": "source_check", "passed": result.passed})

            # Schema drift
            if cfg.expected_columns:
                result = self.check_schema_drift(
                    cfg.table_name, cfg.expected_columns, cfg.allow_new_columns
                )
                results.append(result)
                logger.info(result.message, extra={"event": "source_check", "passed": result.passed})

        for r in results:
            r.snap_date = snap_date
        return results


class OutputChecker:
    """Validate output table data quality after ETL execution."""

    def __init__(self, spark) -> None:
        self._spark = spark

    def check_row_count(
        self, db: str, table: str, snap_date: str, min_count: int
    ) -> CheckResult:
        """Check that the output table has at least ``min_count`` rows."""
        sql = (
            f"SELECT COUNT(*) AS cnt FROM {db}.{table} "
            f"WHERE snap_date = '{snap_date}'"
        )
        row = self._spark.sql(sql).collect()[0]
        count = row["cnt"]
        passed = count >= min_count
        return CheckResult(
            passed,
            f"{db}.{table} row count: {count} (min: {min_count})",
            metric_value=count,
            table=table, check="min_row_count",
            snap_date=snap_date, expected=f">= {min_count}", actual=str(count),
        )

    def check_duplicate_keys(
        self,
        db: str,
        table: str,
        snap_date: str,
        primary_key: list[str],
        max_ratio: float,
    ) -> CheckResult:
        """Check the ratio of duplicate keys."""
        pk_cols = ", ".join(primary_key)
        sql = (
            f"SELECT COUNT(*) AS total, COUNT(DISTINCT {pk_cols}) AS distinct_cnt "
            f"FROM {db}.{table} WHERE snap_date = '{snap_date}'"
        )
        row = self._spark.sql(sql).collect()[0]
        total = row["total"]
        distinct = row["distinct_cnt"]
        if total == 0:
            return CheckResult(
                True, f"{db}.{table} has 0 rows, skip dup check", metric_value=0.0,
                table=table, check="max_duplicate_key_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="0 rows",
            )
        ratio = (total - distinct) / total
        passed = ratio <= max_ratio
        return CheckResult(
            passed,
            f"{db}.{table} duplicate key ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
            table=table, check="max_duplicate_key_ratio",
            snap_date=snap_date, expected=f"<= {max_ratio}", actual=f"{ratio:.4f}",
        )

    def check_null_ratio(
        self, db: str, table: str, snap_date: str, max_ratio: float
    ) -> CheckResult:
        """Check overall null ratio across all columns."""
        # Get column names (exclude partition columns from null check)
        desc_df = self._spark.sql(f"DESCRIBE {db}.{table}")
        columns = [
            row["col_name"]
            for row in desc_df.collect()
            if not row["col_name"].startswith("#")
        ]
        if not columns:
            return CheckResult(
                True, f"{db}.{table} has no columns to check",
                table=table, check="max_null_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="no columns",
            )

        null_exprs = " + ".join(
            f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END)" for col in columns
        )
        sql = (
            f"SELECT ({null_exprs}) AS null_cnt, COUNT(*) * {len(columns)} AS total_cells "
            f"FROM {db}.{table} WHERE snap_date = '{snap_date}'"
        )
        row = self._spark.sql(sql).collect()[0]
        null_cnt = row["null_cnt"] or 0
        total_cells = row["total_cells"]
        if total_cells == 0:
            return CheckResult(
                True, f"{db}.{table} has 0 cells, skip null check", metric_value=0.0,
                table=table, check="max_null_ratio",
                snap_date=snap_date, expected=f"<= {max_ratio}", actual="0 cells",
            )
        ratio = null_cnt / total_cells
        passed = ratio <= max_ratio
        return CheckResult(
            passed,
            f"{db}.{table} null ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
            table=table, check="max_null_ratio",
            snap_date=snap_date, expected=f"<= {max_ratio}", actual=f"{ratio:.4f}",
        )

    def check_schema_contract(
        self,
        db: str,
        table: str,
        required_columns: list[str],
        snap_date: str = "",
    ) -> CheckResult:
        """Verify the output table contains all required schema columns.

        This enforces the schema contract at the source_etl output boundary:
        every column declared in ``TableConfig.primary_key`` (which must be a
        subset of ``schema.columns`` identity columns) must physically exist in
        the output table. Type checks are intentionally delegated to
        ``SourceChecker.check_schema_drift`` on the input side.
        """
        if not required_columns:
            return CheckResult(
                True, f"No required columns declared for {db}.{table}",
                table=table, check="schema_contract",
                snap_date=snap_date, expected="(none)", actual="ok",
            )

        desc_df = self._spark.sql(f"DESCRIBE {db}.{table}")
        actual = {
            row["col_name"]
            for row in desc_df.collect()
            if not row["col_name"].startswith("#")
        }

        missing = [col for col in required_columns if col not in actual]
        if missing:
            return CheckResult(
                False,
                f"Schema contract failed for {db}.{table}: missing columns {missing}",
                table=table, check="schema_contract",
                snap_date=snap_date, expected="required columns present",
                actual=f"missing {missing}",
            )
        return CheckResult(
            True, f"Schema contract OK for {db}.{table}",
            table=table, check="schema_contract",
            snap_date=snap_date, expected="required columns present", actual="ok",
        )

    def run_all(
        self, table_config: TableConfig, target_db: str, snap_date: str
    ) -> list[CheckResult]:
        """Run all configured quality checks for one output table."""
        results: list[CheckResult] = []
        qc = table_config.quality_checks

        # Schema contract check — unconditional when primary_key is declared.
        if table_config.primary_key:
            result = self.check_schema_contract(
                target_db, table_config.name, table_config.primary_key, snap_date
            )
            results.append(result)
            logger.info(result.message, extra={"event": "output_check", "passed": result.passed})

        if "min_row_count" in qc:
            result = self.check_row_count(
                target_db, table_config.name, snap_date, qc["min_row_count"]
            )
            results.append(result)
            logger.info(result.message, extra={"event": "output_check", "passed": result.passed})

        if "max_duplicate_key_ratio" in qc and table_config.primary_key:
            result = self.check_duplicate_keys(
                target_db,
                table_config.name,
                snap_date,
                table_config.primary_key,
                qc["max_duplicate_key_ratio"],
            )
            results.append(result)
            logger.info(result.message, extra={"event": "output_check", "passed": result.passed})

        if "max_null_ratio" in qc:
            result = self.check_null_ratio(
                target_db, table_config.name, snap_date, qc["max_null_ratio"]
            )
            results.append(result)
            logger.info(result.message, extra={"event": "output_check", "passed": result.passed})

        return results

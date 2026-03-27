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
            exists = any(target in p for p in partition_values)
            if exists:
                return CheckResult(True, f"Partition {target} exists in {table}")
            return CheckResult(
                False, f"Partition {target} not found in {table}"
            )
        except Exception as exc:
            return CheckResult(False, f"Failed to check partitions for {table}: {exc}")

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
            return CheckResult(True, f"No schema expectations for {table}")

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
            return CheckResult(False, f"Schema drift in {table}: {'; '.join(errors)}")
        return CheckResult(True, f"Schema OK for {table}")

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
            return CheckResult(True, f"{db}.{table} has 0 rows, skip dup check", metric_value=0.0)
        ratio = (total - distinct) / total
        passed = ratio <= max_ratio
        return CheckResult(
            passed,
            f"{db}.{table} duplicate key ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
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
            return CheckResult(True, f"{db}.{table} has no columns to check")

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
            return CheckResult(True, f"{db}.{table} has 0 cells, skip null check", metric_value=0.0)
        ratio = null_cnt / total_cells
        passed = ratio <= max_ratio
        return CheckResult(
            passed,
            f"{db}.{table} null ratio: {ratio:.4f} (max: {max_ratio})",
            metric_value=ratio,
        )

    def run_all(
        self, table_config: TableConfig, target_db: str, snap_date: str
    ) -> list[CheckResult]:
        """Run all configured quality checks for one output table."""
        results: list[CheckResult] = []
        qc = table_config.quality_checks

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

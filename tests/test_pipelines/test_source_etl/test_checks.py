"""Tests for source and output checks."""

from unittest.mock import MagicMock

import pytest

from recsys_tfb.pipelines.source_etl.checks import (
    OutputChecker,
    SourceChecker,
)
from recsys_tfb.pipelines.source_etl.models import (
    SourceCheckConfig,
    TableConfig,
)


def _mock_spark_sql(return_values: dict):
    """Create a mock SparkSession whose .sql() returns preset results.

    ``return_values`` maps SQL-prefix substrings to lists of Row-like dicts.
    """
    spark = MagicMock()

    def sql_side_effect(query):
        for key, rows in return_values.items():
            if key in query:
                df = MagicMock()
                mock_rows = [MagicMock(**{"__getitem__": lambda self, k, r=r: r[k]}) for r in rows]
                df.collect.return_value = mock_rows
                return df
        raise ValueError(f"Unexpected SQL: {query}")

    spark.sql.side_effect = sql_side_effect
    return spark


class TestSourceCheckerPartition:
    def test_partition_exists(self):
        # SHOW PARTITIONS returns Row with index-0 access
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value="snap_date=2024-01-31")
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        result = checker.check_partition_exists("db.t", "snap_date", "2024-01-31")
        assert result.passed is True

    def test_partition_missing(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: "snap_date=2024-02-29"
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        result = checker.check_partition_exists("db.t", "snap_date", "2024-01-31")
        assert result.passed is False

    def test_partition_check_exception(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("Hive error")

        checker = SourceChecker(spark)
        result = checker.check_partition_exists("db.t", "snap_date", "2024-01-31")
        assert result.passed is False
        assert "Failed" in result.message


class TestSourceCheckerRowCount:
    def test_row_count_pass(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 1500000
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        result = checker.check_row_count("db.t", "snap_date", "2024-01-31", 1000000)
        assert result.passed is True
        assert result.metric_value == 1500000

    def test_row_count_fail(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 500
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        result = checker.check_row_count("db.t", "snap_date", "2024-01-31", 1000000)
        assert result.passed is False


class TestSourceCheckerSchemaDrift:
    def _make_spark(self, actual_cols: dict[str, str]):
        rows = []
        for col_name, data_type in actual_cols.items():
            row = MagicMock()
            row.__getitem__ = lambda self, k, cn=col_name, dt=data_type: (
                cn if k == "col_name" else dt
            )
            rows.append(row)
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = rows
        return spark

    def test_schema_ok(self):
        spark = self._make_spark({"cust_id": "string", "amt": "double"})
        checker = SourceChecker(spark)
        result = checker.check_schema_drift(
            "db.t", {"cust_id": "string", "amt": "double"}
        )
        assert result.passed is True

    def test_missing_column(self):
        spark = self._make_spark({"cust_id": "string"})
        checker = SourceChecker(spark)
        result = checker.check_schema_drift(
            "db.t", {"cust_id": "string", "amt": "double"}
        )
        assert result.passed is False
        assert "Missing column: amt" in result.message

    def test_type_mismatch(self):
        spark = self._make_spark({"cust_id": "string", "amt": "int"})
        checker = SourceChecker(spark)
        result = checker.check_schema_drift(
            "db.t", {"cust_id": "string", "amt": "double"}
        )
        assert result.passed is False
        assert "Type mismatch" in result.message

    def test_new_columns_allowed(self):
        spark = self._make_spark({"cust_id": "string", "amt": "double", "new_col": "string"})
        checker = SourceChecker(spark)
        result = checker.check_schema_drift(
            "db.t", {"cust_id": "string", "amt": "double"}, allow_new_columns=True
        )
        assert result.passed is True

    def test_new_columns_not_allowed(self):
        spark = self._make_spark({"cust_id": "string", "amt": "double", "new_col": "string"})
        checker = SourceChecker(spark)
        result = checker.check_schema_drift(
            "db.t", {"cust_id": "string", "amt": "double"}, allow_new_columns=False
        )
        assert result.passed is False
        assert "Unexpected new columns" in result.message

    def test_empty_expected(self):
        spark = MagicMock()
        checker = SourceChecker(spark)
        result = checker.check_schema_drift("db.t", {})
        assert result.passed is True


class TestSourceCheckerRunAll:
    def test_run_all_skips_after_partition_fail(self):
        # Partition check fails -> row count and schema checks skipped
        row = MagicMock()
        row.__getitem__ = lambda self, k: "snap_date=2024-02-29"
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        cfg = SourceCheckConfig(
            table_name="db.t",
            partition_key="snap_date",
            min_row_count=100,
            expected_columns={"cust_id": "string"},
        )
        results = checker.run_all([cfg], "2024-01-31")
        assert len(results) == 1  # only partition check
        assert results[0].passed is False


class TestOutputCheckerRowCount:
    def test_pass(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 2000
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        result = checker.check_row_count("db", "t", "2024-01-31", 1000)
        assert result.passed is True

    def test_fail(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 500
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        result = checker.check_row_count("db", "t", "2024-01-31", 1000)
        assert result.passed is False


class TestOutputCheckerDuplicateKeys:
    def test_no_duplicates(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 100 if k == "total" else 100
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        result = checker.check_duplicate_keys(
            "db", "t", "2024-01-31", ["cust_id"], 0.0
        )
        assert result.passed is True
        assert result.metric_value == 0.0

    def test_duplicates_above_threshold(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: 100 if k == "total" else 90
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        result = checker.check_duplicate_keys(
            "db", "t", "2024-01-31", ["cust_id"], 0.05
        )
        assert result.passed is False
        assert result.metric_value == pytest.approx(0.1)


class TestOutputCheckerNullRatio:
    def test_below_threshold(self):
        # First call: DESCRIBE -> columns
        desc_rows = []
        for col in ["cust_id", "amt"]:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            desc_rows.append(r)

        # Second call: null ratio query
        null_row = MagicMock()
        null_row.__getitem__ = lambda self, k: 1 if k == "null_cnt" else 200

        desc_df = MagicMock()
        desc_df.collect.return_value = desc_rows
        null_df = MagicMock()
        null_df.collect.return_value = [null_row]

        spark = MagicMock()
        spark.sql.side_effect = [desc_df, null_df]

        checker = OutputChecker(spark)
        result = checker.check_null_ratio("db", "t", "2024-01-31", 0.05)
        assert result.passed is True
        assert result.metric_value == pytest.approx(0.005)


class TestOutputCheckerRunAll:
    def test_runs_configured_checks(self):
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by=["snap_date"],
            primary_key=["snap_date", "cust_id"],
            quality_checks={"min_row_count": 100, "max_duplicate_key_ratio": 0.0},
        )

        # Mock both queries
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, k: 200
        dup_row = MagicMock()
        dup_row.__getitem__ = lambda self, k: 200 if k == "total" else 200

        spark = MagicMock()
        count_df = MagicMock()
        count_df.collect.return_value = [count_row]
        dup_df = MagicMock()
        dup_df.collect.return_value = [dup_row]
        spark.sql.side_effect = [count_df, dup_df]

        checker = OutputChecker(spark)
        results = checker.run_all(cfg, "ml_feature", "2024-01-31")
        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_skips_checks_not_configured(self):
        cfg = TableConfig(
            name="feature_sav",
            sql_file="feature/feature_sav.sql",
            partition_by=["snap_date"],
        )
        spark = MagicMock()
        checker = OutputChecker(spark)
        results = checker.run_all(cfg, "ml_feature", "2024-01-31")
        assert len(results) == 0

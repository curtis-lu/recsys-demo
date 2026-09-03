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


_PK = ["snap_date", "cust_id", "prod_name"]


def _pk_aggregate(pk, total, distinct_cnt, keyed_total=None, nulls=None):
    """The one aggregate row ``check_primary_key`` reads, stated column by column.

    ``keyed_total`` defaults to ``total`` — the no-NULL table, where the
    duplicate ratio is the same number the check reported before #289.
    """
    nulls = nulls or {}
    values = {
        "total": total,
        "distinct_cnt": distinct_cnt,
        "keyed_total": total if keyed_total is None else keyed_total,
    }
    for col in pk:
        values[f"null_{col}"] = nulls.get(col, 0)
    return values


def _one_row_spark(values: dict):
    """A mock SparkSession whose ``.sql()`` returns a single dict-backed Row."""
    row = MagicMock()
    row.__getitem__ = lambda self, k: values[k]
    df = MagicMock()
    df.collect.return_value = [row]
    spark = MagicMock()
    spark.sql.return_value = df
    return spark


class TestOutputCheckerPrimaryKey:
    """One scan, two verdicts: NULL keys and duplicate keys (issue #289)."""

    @staticmethod
    def _by_check(results):
        by = {r.check: r for r in results}
        assert set(by) == {"primary_key_not_null", "max_duplicate_key_ratio"}, (
            "audit records must be able to tell the two verdicts apart"
        )
        return by

    def test_null_keys_are_reported_as_null_not_as_duplicates(self):
        # The measured case from issue #289: 5 rows, 2 with a NULL cust_id,
        # not one actually duplicated row. COUNT(DISTINCT ...) skips the NULL
        # rows, so the old single verdict called this a 0.4 duplicate ratio.
        spark = _one_row_spark(_pk_aggregate(
            _PK, total=5, distinct_cnt=3, keyed_total=3, nulls={"cust_id": 2}))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "sample_pool", "2024-01-31", _PK, 0.0))

        nulls = by["primary_key_not_null"]
        assert nulls.passed is False
        assert "cust_id" in nulls.message and "2" in nulls.message
        assert "duplicate" not in nulls.message.lower()
        # The columns that are fine must not be named — the operator is being
        # pointed at one column to go fix.
        assert "prod_name" not in nulls.actual

        dup = by["max_duplicate_key_ratio"]
        assert dup.passed is True
        assert dup.metric_value == 0.0

    def test_real_duplicates_report_exactly_as_before(self):
        spark = _one_row_spark(_pk_aggregate(_PK, total=100, distinct_cnt=90))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.05))

        dup = by["max_duplicate_key_ratio"]
        assert dup.passed is False
        assert dup.metric_value == pytest.approx(0.1)
        assert dup.message == "db.t duplicate key ratio: 0.1000 (max: 0.05)"
        assert by["primary_key_not_null"].passed is True

    def test_clean_table_passes_both(self):
        spark = _one_row_spark(_pk_aggregate(_PK, total=100, distinct_cnt=100))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0))

        assert by["primary_key_not_null"].passed is True
        assert by["max_duplicate_key_ratio"].passed is True
        assert by["max_duplicate_key_ratio"].metric_value == 0.0

    def test_nulls_and_duplicates_together_are_both_reported(self):
        # 10 rows: 2 carry a NULL cust_id, and of the 8 that carry a key only
        # 6 are distinct. Neither verdict may swallow the other.
        spark = _one_row_spark(_pk_aggregate(
            _PK, total=10, distinct_cnt=6, keyed_total=8, nulls={"cust_id": 2}))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0))

        assert by["primary_key_not_null"].passed is False
        assert by["max_duplicate_key_ratio"].passed is False
        # Ratio over the rows that actually carry a key, not over all 10.
        assert by["max_duplicate_key_ratio"].metric_value == pytest.approx(0.25)

    def test_every_null_column_is_named_with_its_count(self):
        spark = _one_row_spark(_pk_aggregate(
            _PK, total=6, distinct_cnt=2, keyed_total=2,
            nulls={"cust_id": 3, "prod_name": 1}))

        nulls = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0))["primary_key_not_null"]

        assert "cust_id=3" in nulls.actual
        assert "prod_name=1" in nulls.actual
        assert "snap_date" not in nulls.actual

    def test_it_stays_one_scan(self):
        # ADR-0006's cost invariant: splitting the diagnosis must not buy a
        # second aggregate over the table.
        spark = _one_row_spark(_pk_aggregate(_PK, total=4, distinct_cnt=4))
        OutputChecker(spark).check_primary_key("db", "t", "2024-01-31", _PK, 0.0)
        assert spark.sql.call_count == 1

    def test_sql_is_built_per_declared_key_column(self):
        # schema.entity is a list and the key columns come from config, so
        # nothing here may be spelled out in the query.
        pk = ["as_of", "member_ref", "channel"]
        spark = _one_row_spark(_pk_aggregate(pk, total=3, distinct_cnt=3))

        OutputChecker(spark).check_primary_key("db", "t", "2024-01-31", pk, 0.0)

        sql = spark.sql.call_args[0][0]
        for col in pk:
            # Its own SUM term, aliased per column — asserting only "IS NULL"
            # would be satisfied by the shared keyed_total expression, and the
            # per-column counts could quietly stop being computed.
            assert f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) " \
                   f"AS `null_{col}`" in sql, col
            # ...and the row the ratio is measured over excludes it too.
            assert f"`{col}` IS NULL" in sql.split("AS keyed_total")[0], col
        assert "cust_id" not in sql

    def test_zero_rows_skips_the_duplicate_check(self):
        spark = _one_row_spark(_pk_aggregate(_PK, total=0, distinct_cnt=0))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0))

        assert by["max_duplicate_key_ratio"].passed is True
        assert by["max_duplicate_key_ratio"].message == (
            "db.t has 0 rows, skip dup check")
        assert by["primary_key_not_null"].passed is True

    def test_every_row_missing_a_key_still_fails_on_null_only(self):
        # No keyed row survives, so there is nothing to call a duplicate; the
        # NULL verdict has to carry the whole report.
        spark = _one_row_spark(_pk_aggregate(
            _PK, total=4, distinct_cnt=0, keyed_total=0, nulls={"cust_id": 4}))

        by = self._by_check(OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0))

        assert by["primary_key_not_null"].passed is False
        assert by["max_duplicate_key_ratio"].passed is True
        assert "skip dup check" in by["max_duplicate_key_ratio"].message

    def test_null_verdict_is_not_mistaken_for_the_row_count_result(self):
        # sql_runner._run_output_checks picks the audit row_count out of the
        # results by looking for "row count" in the message. A new result
        # carrying a metric_value must not answer to that.
        spark = _one_row_spark(_pk_aggregate(
            _PK, total=5, distinct_cnt=3, keyed_total=3, nulls={"cust_id": 2}))

        for r in OutputChecker(spark).check_primary_key(
            "db", "t", "2024-01-31", _PK, 0.0
        ):
            assert "row count" not in r.message


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


class TestOutputCheckerSchemaContract:
    @staticmethod
    def _describe_df(columns: list[str]):
        rows = []
        for col in columns:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            rows.append(r)
        df = MagicMock()
        df.collect.return_value = rows
        return df

    def test_all_required_present_passes(self):
        spark = MagicMock()
        spark.sql.return_value = self._describe_df(["snap_date", "cust_id", "amt"])
        checker = OutputChecker(spark)
        result = checker.check_schema_contract(
            "ml_feature", "feature_aum", ["snap_date", "cust_id"]
        )
        assert result.passed is True

    def test_missing_column_fails(self):
        spark = MagicMock()
        spark.sql.return_value = self._describe_df(["snap_date", "amt"])
        checker = OutputChecker(spark)
        result = checker.check_schema_contract(
            "ml_feature", "feature_aum", ["snap_date", "cust_id"]
        )
        assert result.passed is False
        assert "cust_id" in result.message

    def test_skips_partition_header(self):
        # Rows beginning with '#' (partition info section) should be ignored.
        rows = []
        for col in ["snap_date", "cust_id", "# Partition Information", "# col_name"]:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            rows.append(r)
        df = MagicMock()
        df.collect.return_value = rows
        spark = MagicMock()
        spark.sql.return_value = df
        checker = OutputChecker(spark)
        result = checker.check_schema_contract(
            "ml_feature", "feature_aum", ["snap_date", "cust_id"]
        )
        assert result.passed is True

    def test_empty_required_passes(self):
        spark = MagicMock()
        checker = OutputChecker(spark)
        result = checker.check_schema_contract("ml_feature", "feature_aum", [])
        assert result.passed is True


class TestSourceCheckResultFields:
    def test_partition_check_populates_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value="snap_date=2024-02-29")
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        r = checker.check_partition_exists("db.t", "snap_date", "2024-01-31")
        assert r.passed is False
        assert r.table == "db.t"
        assert r.check == "partition_exists"
        assert r.expected == "partition snap_date=2024-01-31"
        assert r.actual == "not found"

    def test_row_count_populates_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 523 if k == "cnt" else None
        spark.sql.return_value.collect.return_value = [row]

        checker = SourceChecker(spark)
        r = checker.check_row_count("db.t", "snap_date", "2024-01-31", min_count=1000)
        assert r.passed is False
        assert r.check == "row_count"
        assert r.expected == ">= 1000"
        assert r.actual == "523"

    def test_run_all_stamps_snap_date(self):
        spark = MagicMock()
        prow = MagicMock()
        prow.__getitem__ = MagicMock(return_value="snap_date=2024-01-31")
        spark.sql.return_value.collect.return_value = [prow]

        checker = SourceChecker(spark)
        cfgs = [SourceCheckConfig(table_name="db.t", partition_key="snap_date")]
        results = checker.run_all(cfgs, "2024-01-31")
        assert all(r.snap_date == "2024-01-31" for r in results)


class TestOutputCheckerRunAll:
    def test_runs_configured_checks(self):
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by=["snap_date"],
            primary_key=["snap_date", "cust_id"],
            quality_checks={"min_row_count": 100, "max_duplicate_key_ratio": 0.0},
        )

        # Mock three queries: DESCRIBE (schema contract), COUNT (row count),
        # COUNT DISTINCT (dup keys)
        desc_rows = []
        for col in ["snap_date", "cust_id", "amt"]:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            desc_rows.append(r)
        desc_df = MagicMock()
        desc_df.collect.return_value = desc_rows

        count_row = MagicMock()
        count_row.__getitem__ = lambda self, k: 200
        pk_values = _pk_aggregate(cfg.primary_key, total=200, distinct_cnt=200)
        pk_row = MagicMock()
        pk_row.__getitem__ = lambda self, k: pk_values[k]

        spark = MagicMock()
        count_df = MagicMock()
        count_df.collect.return_value = [count_row]
        pk_df = MagicMock()
        pk_df.collect.return_value = [pk_row]
        spark.sql.side_effect = [desc_df, count_df, pk_df]

        checker = OutputChecker(spark)
        results = checker.run_all(cfg, "ml_feature", "2024-01-31")
        # schema_contract + min_row_count + the two primary-key verdicts, and
        # still three queries: the key check reads one aggregate (#289).
        assert [r.check for r in results] == [
            "schema_contract",
            "min_row_count",
            "primary_key_not_null",
            "max_duplicate_key_ratio",
        ]
        assert all(r.passed for r in results)
        assert spark.sql.call_count == 3

    def test_null_key_fails_the_table_through_run_all(self):
        # The wiring that matters: the NULL verdict has to reach the caller's
        # failed-results list, not just exist inside check_primary_key.
        cfg = TableConfig(
            name="sample_pool",
            sql_file="sample_pool/sample_pool.sql",
            partition_by=["snap_date"],
            primary_key=["snap_date", "cust_id"],
            quality_checks={"max_duplicate_key_ratio": 0.0},
        )
        desc_rows = []
        for col in ["snap_date", "cust_id"]:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            desc_rows.append(r)
        desc_df = MagicMock()
        desc_df.collect.return_value = desc_rows

        pk_values = _pk_aggregate(
            cfg.primary_key, total=5, distinct_cnt=3, keyed_total=3,
            nulls={"cust_id": 2})
        pk_row = MagicMock()
        pk_row.__getitem__ = lambda self, k: pk_values[k]
        pk_df = MagicMock()
        pk_df.collect.return_value = [pk_row]

        spark = MagicMock()
        spark.sql.side_effect = [desc_df, pk_df]

        results = OutputChecker(spark).run_all(cfg, "ml_recsys", "2024-01-31")
        failed = [r for r in results if not r.passed]
        assert [r.check for r in failed] == ["primary_key_not_null"]
        assert "cust_id" in failed[0].message

    def test_schema_contract_runs_even_without_quality_checks(self):
        # Only primary_key declared, no quality_checks → schema contract still runs.
        cfg = TableConfig(
            name="feature_aum",
            sql_file="feature/feature_aum.sql",
            partition_by=["snap_date"],
            primary_key=["snap_date", "cust_id"],
        )
        desc_rows = []
        for col in ["snap_date", "cust_id"]:
            r = MagicMock()
            r.__getitem__ = lambda self, k, c=col: c if k == "col_name" else "string"
            desc_rows.append(r)
        desc_df = MagicMock()
        desc_df.collect.return_value = desc_rows
        spark = MagicMock()
        spark.sql.return_value = desc_df

        checker = OutputChecker(spark)
        results = checker.run_all(cfg, "ml_feature", "2024-01-31")
        assert len(results) == 1
        assert results[0].passed is True

    def test_skips_checks_not_configured(self):
        # No primary_key and no quality_checks → nothing runs.
        cfg = TableConfig(
            name="feature_sav",
            sql_file="feature/feature_sav.sql",
            partition_by=["snap_date"],
        )
        spark = MagicMock()
        checker = OutputChecker(spark)
        results = checker.run_all(cfg, "ml_feature", "2024-01-31")
        assert len(results) == 0


class TestOutputCheckResultFields:
    def test_row_count_fields(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 0 if k == "cnt" else None
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        r = checker.check_row_count("db", "t", "2024-01-31", min_count=100)
        assert r.passed is False
        assert r.table == "t"
        assert r.check == "min_row_count"
        assert r.expected == ">= 100"
        assert r.actual == "0"
        assert r.snap_date == "2024-01-31"

    def test_run_all_sets_table_and_snap_date(self):
        spark = MagicMock()
        row = MagicMock()
        # DESCRIBE (schema_contract) reads row["col_name"]; COUNT reads row["cnt"].
        # Real Spark DESCRIBE never returns None col_name, so the mock returns a
        # concrete column name for any non-"cnt" key.
        row.__getitem__ = lambda self, k: 5 if k == "cnt" else "cust_id"
        spark.sql.return_value.collect.return_value = [row]

        checker = OutputChecker(spark)
        tc = TableConfig(
            name="feature_table", sql_file="x.sql",
            partition_by={"snap_date": "DATE"},
            primary_key=["snap_date", "cust_id"],
            quality_checks={"min_row_count": 1},
        )
        results = checker.run_all(tc, "ml_recsys", "2024-01-31")
        assert all(r.table == "feature_table" for r in results)
        assert all(r.snap_date == "2024-01-31" for r in results)

"""Tests for HiveTableDataset.

Most tests mock SparkSession because insertInto/catalog.tableExists require
a real Hive metastore. TestSchemaEvolutionIntegration uses the real local
`spark` fixture end-to-end.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from recsys_tfb.io.hive_table_dataset import HiveTableDataset


def _make_spark_mock() -> MagicMock:
    spark = MagicMock(name="SparkSession")
    return spark


def _patch_spark(spark: MagicMock):
    return patch(
        "recsys_tfb.utils.spark.get_or_create_spark_session",
        return_value=spark,
    )


def _configure_mock_table_exists(spark: MagicMock, database: str, table: str) -> None:
    """Configure spark mock so that _table_exists(spark) returns True.

    HiveTableDataset._table_exists uses SHOW TABLES IN <db> LIKE '<table>'
    because catalog.tableExists("db.table") returns False for qualified names
    in Spark 3.3.2 local-Hive mode (known PySpark quirk).
    """
    row = MagicMock()
    row.tableName = table
    show_result = MagicMock()
    show_result.collect.return_value = [row]

    _original_sql = spark.sql.side_effect  # preserve any existing side_effect

    def _sql_side_effect(query, *args, **kwargs):
        import re
        if re.search(
            rf"SHOW TABLES IN {re.escape(database)} LIKE '{re.escape(table)}'",
            query,
            re.IGNORECASE,
        ):
            return show_result
        if _original_sql is not None:
            return _original_sql(query, *args, **kwargs)
        return MagicMock()

    spark.sql.side_effect = _sql_side_effect


class TestValidation:
    def test_external_requires_location(self):
        with pytest.raises(ValueError, match="external=True requires 'location'"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                external=True,
                location=None,
            )

    def test_overlap_between_columns_and_partitions(self):
        with pytest.raises(ValueError, match="columns and partition_cols overlap"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[
                    {"name": "a", "type": "STRING"},
                    {"name": "snap_date", "type": "STRING"},
                ],
                partition_cols=[{"name": "snap_date", "type": "STRING"}],
                external=False,
            )

    def test_invalid_write_mode(self):
        with pytest.raises(ValueError, match="write_mode must be one of"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                external=False,
                write_mode="merge",
            )

    def test_writable_requires_columns(self):
        with pytest.raises(ValueError, match="columns is required"):
            HiveTableDataset(
                database="db",
                table="t",
                external=False,
            )

    def test_read_only_allows_no_columns(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            read_only=True,
        )
        assert ds._read_only is True

    def test_managed_with_location_warns(self, caplog):
        with caplog.at_level("WARNING"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                external=False,
                location="hdfs:///somewhere",
            )
        assert any("managed" in r.message.lower() for r in caplog.records)

    def test_partition_filter_overlaps_columns(self):
        with pytest.raises(ValueError, match="partition_filter.*overlap"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[
                    {"name": "a", "type": "STRING"},
                    {"name": "ver", "type": "STRING"},
                ],
                partition_filter={"ver": "abc12345"},
                external=False,
            )

    def test_partition_filter_overlaps_partition_cols(self):
        with pytest.raises(ValueError, match="partition_filter.*overlap"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_cols=[{"name": "ver", "type": "STRING"}],
                partition_filter={"ver": "abc12345"},
                external=False,
            )

    def test_partition_filter_value_must_be_non_empty_string(self):
        with pytest.raises(ValueError, match="partition_filter.*value"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_filter={"ver": ""},
                external=False,
            )
        with pytest.raises(ValueError, match="partition_filter.*value"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_filter={"ver": 123},
                external=False,
            )

    def test_partition_filter_allowed_on_read_only(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            partition_filter={"ver": "abc"},
            read_only=True,
        )
        assert ds._partition_filter == {"ver": "abc"}


class TestDDLExternalPartitioned:
    def _make_ds(self) -> HiveTableDataset:
        return HiveTableDataset(
            database="ml_recsys",
            table="score_table",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "score", "type": "DOUBLE"},
            ],
            partition_cols=[
                {"name": "snap_date", "type": "STRING"},
                {"name": "prod_name", "type": "STRING"},
                {"name": "model_version", "type": "STRING"},
            ],
            external=True,
            location="hdfs:///data/recsys/inference/score_table",
        )

    def test_ddl_has_external_and_location(self):
        ddl = self._make_ds()._build_create_ddl()
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS ml_recsys.score_table" in ddl
        assert "LOCATION 'hdfs:///data/recsys/inference/score_table'" in ddl

    def test_ddl_has_partitioned_by(self):
        ddl = self._make_ds()._build_create_ddl()
        assert (
            "PARTITIONED BY (snap_date STRING, prod_name STRING, model_version STRING)"
            in ddl
        )

    def test_ddl_stored_as_parquet(self):
        ddl = self._make_ds()._build_create_ddl()
        assert "STORED AS PARQUET" in ddl

    def test_ddl_column_defs(self):
        ddl = self._make_ds()._build_create_ddl()
        assert "cust_id STRING" in ddl
        assert "score DOUBLE" in ddl


class TestDDLManagedNonPartitioned:
    def test_ddl_no_external_no_partition_no_location(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="lookup",
            columns=[{"name": "k", "type": "STRING"}],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert "CREATE TABLE IF NOT EXISTS ml_recsys.lookup" in ddl
        assert "EXTERNAL" not in ddl
        assert "PARTITIONED BY" not in ddl
        assert "LOCATION" not in ddl


class TestDDLTableProperties:
    def test_ddl_includes_tblproperties(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            columns=[{"name": "a", "type": "STRING"}],
            external=False,
            table_properties={"parquet.compression": "SNAPPY"},
        )
        ddl = ds._build_create_ddl()
        assert "TBLPROPERTIES ('parquet.compression'='SNAPPY')" in ddl


class TestDDLColumnComment:
    def test_comment_in_ddl(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            columns=[
                {"name": "cust_id", "type": "STRING", "comment": "customer id"},
            ],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert "cust_id STRING COMMENT 'customer id'" in ddl

    def test_comment_escapes_single_quote(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            columns=[
                {"name": "col", "type": "STRING", "comment": "Alice's note"},
            ],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert "Alice\\'s note" in ddl


class TestDDLPartitionFilter:
    def test_filter_only_no_dynamic_partition(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_keys",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "snap_date", "type": "STRING"},
            ],
            partition_filter={"base_dataset_version": "abc12345"},
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert "PARTITIONED BY (base_dataset_version STRING)" in ddl

    def test_filter_outer_dynamic_inner(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_model_input",
            columns=[{"name": "cust_id", "type": "STRING"}],
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert (
            "PARTITIONED BY (base_dataset_version STRING, snap_date STRING)"
            in ddl
        )

    def test_filter_multiple_keys_preserve_order(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            columns=[{"name": "cust_id", "type": "STRING"}],
            partition_filter={
                "base_dataset_version": "abc12345",
                "train_variant_id": "def67890",
            },
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert (
            "PARTITIONED BY (base_dataset_version STRING, "
            "train_variant_id STRING, snap_date STRING)"
        ) in ddl


class TestSaveExternalPartitioned:
    def _make_ds(self) -> HiveTableDataset:
        return HiveTableDataset(
            database="ml_recsys",
            table="score_table",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "score", "type": "DOUBLE"},
            ],
            partition_cols=[
                {"name": "snap_date", "type": "STRING"},
                {"name": "prod_name", "type": "STRING"},
            ],
            external=True,
            location="hdfs:///tmp/score_table",
        )

    def test_save_runs_ddl_sets_dynamic_mode_and_insertInto(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        writer = MagicMock()
        df.write.mode.return_value = writer
        # df.select(...).select(...).collect() for partition logging
        df.select.return_value.distinct.return_value.collect.return_value = []

        with _patch_spark(spark):
            ds.save(df)

        # DDL executed
        ddl_sql = spark.sql.call_args_list[0][0][0]
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS ml_recsys.score_table" in ddl_sql

        # Dynamic partition mode set
        spark.conf.set.assert_any_call(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )

        # Column reorder: non-partition cols first, partition cols last
        df.select.assert_any_call("cust_id", "score", "snap_date", "prod_name")

        # insertInto with overwrite
        df.write.mode.assert_called_with("overwrite")
        writer.insertInto.assert_called_once_with("ml_recsys.score_table")


class TestSaveManagedNonPartitioned:
    def test_save_does_not_set_dynamic_mode(self):
        ds = HiveTableDataset(
            database="db",
            table="lookup",
            columns=[{"name": "k", "type": "STRING"}],
            external=False,
        )
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        # No dynamic mode call
        dyn_calls = [
            c for c in spark.conf.set.call_args_list
            if c[0][0] == "spark.sql.sources.partitionOverwriteMode"
        ]
        assert dyn_calls == []

        writer.insertInto.assert_called_once_with("db.lookup")


class TestSaveAppendMode:
    def test_append_uses_insert_into_semantics(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            columns=[{"name": "a", "type": "STRING"}],
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=True,
            location="hdfs:///tmp/t",
            write_mode="append",
        )
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        df.write.mode.assert_called_with("append")
        writer.insertInto.assert_called_once_with("db.t")


class TestSaveWithPartitionFilter:
    def _make_ds(self, **kw):
        defaults = dict(
            database="ml_recsys",
            table="val_model_input",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "score", "type": "DOUBLE"},
            ],
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        defaults.update(kw)
        return HiveTableDataset(**defaults)

    def test_save_adds_static_col_when_missing(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "snap_date"]
        df.withColumn.return_value = df
        df.select.return_value = df
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark), \
             patch("pyspark.sql.functions.lit") as mock_lit:
            mock_lit.return_value = "LIT_abc12345"
            ds.save(df)

        df.withColumn.assert_any_call("base_dataset_version", "LIT_abc12345")

        df.select.assert_any_call(
            "cust_id", "score", "base_dataset_version", "snap_date"
        )

        spark.conf.set.assert_any_call(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )

        writer.insertInto.assert_called_once_with("ml_recsys.val_model_input")

    def test_save_keeps_static_col_when_value_matches(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        distinct_row = MagicMock()
        distinct_row.__getitem__.return_value = "abc12345"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            distinct_row
        ]
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        for call in df.withColumn.call_args_list:
            assert call[0][0] != "base_dataset_version"

        writer.insertInto.assert_called_once_with("ml_recsys.val_model_input")

    def test_save_raises_on_static_col_value_mismatch(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        bad_row = MagicMock()
        bad_row.__getitem__.return_value = "XXBADXX"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            bad_row
        ]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="partition_filter.*mismatch"):
            ds.save(df)

    def test_save_raises_on_multiple_static_values(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        r1, r2 = MagicMock(), MagicMock()
        r1.__getitem__.return_value = "abc12345"
        r2.__getitem__.return_value = "OTHERVER"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            r1, r2
        ]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="partition_filter.*mismatch"):
            ds.save(df)


class TestReadOnly:
    def test_save_raises_on_read_only(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="feature_table",
            read_only=True,
        )
        with pytest.raises(RuntimeError, match="read-only"):
            ds.save(MagicMock())

    def test_load_works_on_read_only(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="feature_table",
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.table.assert_called_once_with("ml_recsys.feature_table")


class TestPandasAutoConvert:
    def test_pandas_input_is_converted_to_spark(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            columns=[{"name": "a", "type": "BIGINT"}],
            external=False,
        )
        spark = _make_spark_mock()
        created_df = MagicMock(name="SparkDataFrame")
        created_df.select.return_value = created_df
        writer = MagicMock()
        created_df.write.mode.return_value = writer
        spark.createDataFrame.return_value = created_df

        pdf = pd.DataFrame({"a": [1, 2, 3]})
        with _patch_spark(spark):
            ds.save(pdf)

        spark.createDataFrame.assert_called_once()
        args, _ = spark.createDataFrame.call_args
        assert args[0] is pdf
        writer.insertInto.assert_called_once_with("db.t")


class TestAutoInferColumns:
    def test_columns_auto_infers_from_dataframe(self):
        ds = HiveTableDataset(
            database="db",
            table="wide",
            columns="auto",
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=True,
            location="hdfs:///tmp/wide",
        )
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        schema_field = MagicMock()
        schema_field.name = "cust_id"
        schema_field.dataType.simpleString.return_value = "string"
        snap_field = MagicMock()
        snap_field.name = "snap_date"
        snap_field.dataType.simpleString.return_value = "string"
        score_field = MagicMock()
        score_field.name = "score"
        score_field.dataType.simpleString.return_value = "double"
        df.schema.fields = [schema_field, snap_field, score_field]

        with _patch_spark(spark):
            ds.save(df)

        # Extract the CREATE DDL call (skipping SHOW TABLES from _table_exists)
        all_sqls = [c[0][0] for c in spark.sql.call_args_list]
        create_sqls = [s for s in all_sqls if s.upper().startswith("CREATE")]
        assert create_sqls, "Expected a CREATE DDL call"
        ddl_sql = create_sqls[0]
        assert "cust_id STRING" in ddl_sql
        assert "score DOUBLE" in ddl_sql
        # snap_date is a partition col; must not be in main columns block
        main_block = ddl_sql.split("PARTITIONED BY")[0]
        assert "snap_date STRING" not in main_block


class TestExists:
    def test_exists_returns_true_when_table_present(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="foo",
            read_only=True,
        )
        spark = _make_spark_mock()
        _configure_mock_table_exists(spark, "ml_recsys", "foo")
        with _patch_spark(spark):
            assert ds.exists() is True

    def test_exists_returns_false_when_table_absent(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="foo",
            read_only=True,
        )
        spark = _make_spark_mock()
        # SHOW TABLES returns empty — table does not exist
        empty_result = MagicMock()
        empty_result.collect.return_value = []
        spark.sql.return_value = empty_result
        with _patch_spark(spark):
            assert ds.exists() is False

    def test_exists_returns_false_when_database_absent(self):
        """SHOW TABLES IN <db> raises AnalysisException when db does not exist;
        _table_exists should catch it and return False (mirrors catalog.tableExists)."""

        class _FakeAnalysisException(Exception):
            pass

        ds = HiveTableDataset(
            database="nonexistent_db",
            table="foo",
            read_only=True,
        )
        spark = _make_spark_mock()
        spark.sql.side_effect = _FakeAnalysisException("Database 'nonexistent_db' not found")
        with patch("pyspark.sql.utils.AnalysisException", _FakeAnalysisException), \
             _patch_spark(spark):
            assert ds.exists() is False


class TestLoadWithPartitionFilter:
    def test_load_without_filter_uses_spark_table(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="feature_table",
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.table.assert_called_once_with("ml_recsys.feature_table")
        spark.sql.assert_not_called()

    def test_load_single_filter_injects_where(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_model_input",
            partition_filter={"base_dataset_version": "abc12345"},
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.val_model_input "
            "WHERE base_dataset_version = 'abc12345'"
        )
        spark.table.assert_not_called()

    def test_load_multi_filter_joins_with_and(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            partition_filter={
                "base_dataset_version": "abc12345",
                "train_variant_id": "def67890",
            },
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.train_model_input "
            "WHERE base_dataset_version = 'abc12345' "
            "AND train_variant_id = 'def67890'"
        )

    def test_load_escapes_single_quote_in_value(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="t",
            partition_filter={"k": "ab'cd"},
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.t WHERE k = 'ab''cd'"
        )

    def test_load_drops_partition_filter_column(self, spark):
        """partition_filter columns are constant per load and must be dropped
        from the returned DataFrame so downstream joins don't hit
        ambiguous-column errors when two versioned tables are joined."""
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_model_input",
            partition_filter={"base_dataset_version": "abc12345"},
            read_only=True,
        )
        real_df = spark.createDataFrame(
            [("2024-01-01", 1, 0.5, "abc12345")],
            ["snap_date", "cust_id", "score", "base_dataset_version"],
        )
        mock_spark = _make_spark_mock()
        mock_spark.sql.return_value = real_df
        with _patch_spark(mock_spark):
            result = ds.load()
        assert set(result.columns) == {"snap_date", "cust_id", "score"}

    def test_load_drops_all_partition_filter_columns_keeps_partition_cols(self, spark):
        """All partition_filter keys are dropped; partition_cols (e.g.
        snap_date) are real data dimensions and must be kept."""
        ds = HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            partition_filter={
                "base_dataset_version": "abc12345",
                "train_variant_id": "def67890",
            },
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            read_only=True,
        )
        real_df = spark.createDataFrame(
            [("2024-01-01", 1, 0.5, "abc12345", "def67890")],
            ["snap_date", "cust_id", "score", "base_dataset_version",
             "train_variant_id"],
        )
        mock_spark = _make_spark_mock()
        mock_spark.sql.return_value = real_df
        with _patch_spark(mock_spark):
            result = ds.load()
        assert set(result.columns) == {"snap_date", "cust_id", "score"}


def _field(name: str, simple_type: str) -> MagicMock:
    f = MagicMock()
    f.name = name
    f.dataType.simpleString.return_value = simple_type
    return f


def _df_with_fields(*fields) -> MagicMock:
    df = MagicMock(name="DataFrame")
    df.schema.fields = list(fields)
    df.select.return_value = df
    df.withColumn.return_value = df
    df.select.return_value.distinct.return_value.collect.return_value = []
    writer = MagicMock()
    df.write.mode.return_value = writer
    return df


class TestSchemaEvolution:
    """columns: 'auto' 且表已存在時的 append-only 演化（spec D2）。"""

    def _make_ds(self) -> HiveTableDataset:
        return HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            columns="auto",
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )

    def _make_spark_with_table(self, *table_fields) -> MagicMock:
        spark = _make_spark_mock()
        _configure_mock_table_exists(spark, "ml_recsys", "train_model_input")
        # 表 schema 含分區欄（spark.table 回傳完整 schema），演化邏輯須自行排除
        part_filter = _field("base_dataset_version", "string")
        part_col = _field("snap_date", "string")
        spark.table.return_value.schema.fields = (
            list(table_fields) + [part_filter, part_col]
        )
        return spark

    def test_new_df_column_triggers_alter_and_table_order_projection(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("score", "double"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"),
            _field("new_feat", "double"),
            _field("score", "double"),
            _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "new_feat", "score", "snap_date"]

        with _patch_spark(spark):
            self._make_ds().save(df)

        # 只發 ALTER，不發 CREATE（SHOW TABLES 是 _table_exists 基礎設施呼叫，排除不計）
        ddl_sqls = [
            c[0][0] for c in spark.sql.call_args_list
            if not c[0][0].upper().startswith("SHOW TABLES")
        ]
        assert len(ddl_sqls) == 1
        assert (
            "ALTER TABLE ml_recsys.train_model_input ADD COLUMNS "
            "(new_feat DOUBLE)" in ddl_sqls[0]
        )
        # 投影按表序：既有欄在前、新欄附加、再接分區欄
        df.select.assert_any_call(
            "cust_id", "score", "new_feat", "base_dataset_version", "snap_date"
        )

    def test_df_missing_column_filled_with_typed_null(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("dropped_feat", "double"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"), _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "snap_date"]

        with _patch_spark(spark), \
             patch("pyspark.sql.functions.lit") as mock_lit:
            null_col = MagicMock(name="NullCol")
            mock_lit.return_value.cast.return_value = null_col
            self._make_ds().save(df)

        mock_lit.assert_any_call(None)
        mock_lit.return_value.cast.assert_any_call("double")
        df.withColumn.assert_any_call("dropped_feat", null_col)
        # 缺欄不是錯誤，不發 ALTER 也不發 CREATE
        #（排除 _table_exists 的 SHOW TABLES 基礎呼叫）
        ddl_calls = [
            c[0][0] for c in spark.sql.call_args_list
            if not c[0][0].upper().startswith("SHOW TABLES")
        ]
        assert ddl_calls == []

    def test_type_conflict_raises_value_error(self):
        spark = self._make_spark_with_table(
            _field("cust_id", "string"), _field("score", "int"),
        )
        df = _df_with_fields(
            _field("cust_id", "string"),
            _field("score", "double"),
            _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "score", "snap_date"]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="(?i)type conflict.*score"):
            self._make_ds().save(df)

    def test_column_name_case_difference_is_not_a_new_column(self):
        spark = self._make_spark_with_table(_field("CUST_ID", "string"))
        df = _df_with_fields(
            _field("cust_id", "string"), _field("snap_date", "string"),
        )
        df.columns = ["cust_id", "snap_date"]

        with _patch_spark(spark):
            self._make_ds().save(df)

        # 無 ALTER、無 CREATE（SHOW TABLES 是 _table_exists 基礎設施呼叫，排除不計）
        ddl_calls = [
            c[0][0] for c in spark.sql.call_args_list
            if not c[0][0].upper().startswith("SHOW TABLES")
        ]
        assert ddl_calls == []

    def test_explicit_columns_table_never_checks_existence(self):
        ds = HiveTableDataset(
            database="db",
            table="contract_table",
            columns=[{"name": "a", "type": "STRING"}],
            external=False,
        )
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.select.return_value = df
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        show_calls = [
            c for c in spark.sql.call_args_list
            if c[0][0].lstrip().upper().startswith("SHOW TABLES")
        ]
        assert show_calls == []
        # 既有契約路徑不變：CREATE IF NOT EXISTS 照發
        assert "CREATE TABLE IF NOT EXISTS" in spark.sql.call_args_list[0][0][0]


class TestSchemaEvolutionIntegration:
    """Real-Spark end-to-end：ALTER 演化 + 舊分區 NULL 讀回 + 缺欄 NULL 寫入。"""

    def _make_ds(self, version: str) -> HiveTableDataset:
        return HiveTableDataset(
            database="evo_test",
            table="model_input",
            columns="auto",
            partition_filter={"base_dataset_version": version},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )

    def test_add_then_drop_column_across_versions(self, spark):
        spark.sql("CREATE DATABASE IF NOT EXISTS evo_test")
        spark.sql("DROP TABLE IF EXISTS evo_test.model_input")
        try:
            # v1：窄 schema 首寫建表
            narrow = spark.createDataFrame(
                [("c1", 0.5, "2024-01-31")], ["cust_id", "score", "snap_date"]
            )
            self._make_ds("v1").save(narrow)

            # v2：多一欄 → 觸發 ALTER
            wide = spark.createDataFrame(
                [("c2", 0.7, 1.0, "2024-01-31")],
                ["cust_id", "score", "new_feat", "snap_date"],
            )
            self._make_ds("v2").save(wide)

            table_cols = [
                f.name
                for f in spark.table("evo_test.model_input").schema.fields
            ]
            assert "new_feat" in table_cols

            v1_rows = self._make_ds("v1").load().collect()
            assert len(v1_rows) == 1
            assert v1_rows[0]["new_feat"] is None  # 舊分區讀回 NULL

            v2_rows = self._make_ds("v2").load().collect()
            assert v2_rows[0]["new_feat"] == 1.0

            # v3：比表窄的 df → NULL 補欄寫入
            narrow2 = spark.createDataFrame(
                [("c3", 0.9, "2024-01-31")], ["cust_id", "score", "snap_date"]
            )
            self._make_ds("v3").save(narrow2)
            v3_rows = self._make_ds("v3").load().collect()
            assert v3_rows[0]["new_feat"] is None
        finally:
            spark.sql("DROP TABLE IF EXISTS evo_test.model_input")
            spark.sql("DROP DATABASE IF EXISTS evo_test CASCADE")

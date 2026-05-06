"""Tests for HiveTableDataset.

All tests mock SparkSession because insertInto/catalog.tableExists require
a real Hive metastore, which is not available in local dev.
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

        ddl_sql = spark.sql.call_args_list[0][0][0]
        assert "cust_id STRING" in ddl_sql
        assert "score DOUBLE" in ddl_sql
        # snap_date is a partition col; must not be in main columns block
        main_block = ddl_sql.split("PARTITIONED BY")[0]
        assert "snap_date STRING" not in main_block


class TestExists:
    def test_exists_delegates_to_catalog(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="foo",
            read_only=True,
        )
        spark = _make_spark_mock()
        spark.catalog.tableExists.return_value = True
        with _patch_spark(spark):
            assert ds.exists() is True
        spark.catalog.tableExists.assert_called_once_with("ml_recsys.foo")

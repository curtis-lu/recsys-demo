"""Tests for the zero-scan parquet footer statistics reader.

The reader is what lets the B8 precision gate answer "how large does this column
get" without an aggregation over the data (ADR-0006's cost invariant). Its two
halves are tested apart: partition-path parsing is pure string work and needs no
session, while the footer read is exercised against a real parquet file — a
hand-built fixture would only prove the parser matches the fixture.
"""

import pytest

from recsys_tfb.utils.parquet_stats import (
    group_by_partition,
    partition_value,
    read_max_abs_stats,
)


class TestPartitionValue:
    def test_reads_the_value_of_the_named_key(self):
        path = "hdfs://nn/wh/tbl/base_dataset_version=ab12cd34/snap_date=2026-01-31/p-0.parquet"
        assert partition_value(path, "snap_date") == "2026-01-31"
        assert partition_value(path, "base_dataset_version") == "ab12cd34"

    def test_absent_key_is_none(self):
        assert partition_value("file:///wh/tbl/p-0.parquet", "snap_date") is None

    def test_a_key_that_is_only_a_suffix_of_another_does_not_match(self):
        # `date=` must not be read out of `snap_date=`: a prefix match here
        # would silently group every month under one bogus value.
        path = "file:///wh/tbl/snap_date=2026-01-31/p-0.parquet"
        assert partition_value(path, "date") is None

    def test_a_value_containing_no_slash_stops_at_the_directory(self):
        path = "file:///wh/tbl/snap_date=2026-01-31/entity_bucket=3/p-0.parquet"
        assert partition_value(path, "snap_date") == "2026-01-31"


class TestGroupByPartition:
    def test_groups_paths_under_their_value(self):
        paths = [
            "/wh/t/snap_date=2026-01-31/a.parquet",
            "/wh/t/snap_date=2026-01-31/b.parquet",
            "/wh/t/snap_date=2026-02-28/c.parquet",
        ]
        grouped = group_by_partition(paths, "snap_date")
        assert sorted(grouped) == ["2026-01-31", "2026-02-28"]
        assert len(grouped["2026-01-31"]) == 2

    def test_paths_without_the_key_are_dropped_not_grouped_under_none(self):
        grouped = group_by_partition(["/wh/t/a.parquet"], "snap_date")
        assert grouped == {}


@pytest.mark.spark
class TestReadMaxAbsStats:
    @pytest.fixture
    def written(self, spark, tmp_path):
        from pyspark.sql import types as T

        schema = T.StructType([
            T.StructField("big_int", T.LongType()),
            T.StructField("small_int", T.IntegerType()),
            T.StructField("flag", T.BooleanType()),
            T.StructField("dbl", T.DoubleType()),
            T.StructField("all_null", T.LongType()),
            T.StructField("negative", T.LongType()),
        ])
        rows = [
            (999999957, 99, True, 0.99999, None, -(2 ** 25)),
            (686, 0, False, 3.39e-08, None, 5),
        ]
        out = str(tmp_path / "pq")
        spark.createDataFrame(rows, schema).coalesce(1).write.parquet(out)
        import glob
        return spark, sorted(glob.glob(out + "/*.parquet"))

    def test_reads_max_abs_for_integer_columns(self, written):
        spark, files = written
        stats = read_max_abs_stats(spark, files, ["big_int", "small_int"])
        assert stats == {"big_int": 999999957.0, "small_int": 99.0}

    def test_negative_extreme_wins_over_the_positive_max(self, written):
        # min=-2^25, max=5. Reading getMax() alone would report 5 and pass a
        # column that collides — the whole gate would be a no-op for any column
        # whose magnitude lives on the negative side.
        spark, files = written
        assert read_max_abs_stats(spark, files, ["negative"]) == {
            "negative": float(2 ** 25)}

    def test_boolean_reads_as_one(self, written):
        # Footer stats for BOOLEAN are the strings "true"/"false"; a float()
        # parse would raise and the column would look unmeasurable.
        spark, files = written
        assert read_max_abs_stats(spark, files, ["flag"]) == {"flag": 1.0}

    def test_all_null_column_reads_as_zero_not_none(self, written):
        # No min/max is recorded, but num_nulls == row count says why: there is
        # nothing in the column to lose. Reporting None would make the gate
        # refuse a column that is trivially safe.
        spark, files = written
        assert read_max_abs_stats(spark, files, ["all_null"]) == {"all_null": 0.0}

    def test_a_column_absent_from_the_file_reads_as_none(self, written):
        spark, files = written
        assert read_max_abs_stats(spark, files, ["not_a_column"]) == {
            "not_a_column": None}

    def test_no_files_reads_as_none_for_every_column(self, written):
        spark, _ = written
        assert read_max_abs_stats(spark, [], ["big_int"]) == {"big_int": None}

    def test_reads_across_several_files(self, written, spark, tmp_path):
        from pyspark.sql import types as T

        schema = T.StructType([T.StructField("big_int", T.LongType())])
        second = str(tmp_path / "pq2")
        spark.createDataFrame([(2 ** 40,)], schema).coalesce(1).write.parquet(second)
        import glob
        _, files = written
        both = files + sorted(glob.glob(second + "/*.parquet"))
        assert read_max_abs_stats(spark, both, ["big_int"]) == {
            "big_int": float(2 ** 40)}

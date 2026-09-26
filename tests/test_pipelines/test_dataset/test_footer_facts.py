"""Tests for the footer facts B8 and B10 stand on: which files this run landed,
and what their parquet footers record.

The reader is what lets the gates answer "how large does this column get" and
"how many rows landed" without an aggregation over the data (ADR-0006's cost
invariant). Its halves are tested apart: partition-path parsing and file
selection are pure string work and need no session, while the footer read is
exercised against real parquet files — a hand-built fixture would only prove
the parser matches the fixture.
"""

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.steps.footer_facts import (
    filter_by_partitions,
    footer_rows,
    footer_rows_by_month,
    group_by_partition,
    landed_partition_files,
    partition_value,
    read_max_abs_stats,
    read_row_count,
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


class TestFilterByPartitions:
    _PATHS = [
        "/wh/t/base_dataset_version=v1/train_variant_id=t1/snap_date=2026-01-31/a.parquet",
        "/wh/t/base_dataset_version=v1/train_variant_id=t2/snap_date=2026-01-31/b.parquet",
        "/wh/t/base_dataset_version=v2/train_variant_id=t1/snap_date=2026-01-31/c.parquet",
    ]

    def test_every_pair_must_match(self):
        kept = filter_by_partitions(
            self._PATHS,
            {"base_dataset_version": "v1", "train_variant_id": "t1"},
        )
        assert kept == [self._PATHS[0]]

    def test_a_path_missing_the_key_is_dropped(self):
        # partition_value returns None, which equals no declared value, so the
        # path is excluded rather than silently counted under the filter.
        assert filter_by_partitions(
            ["/wh/t/a.parquet"], {"base_dataset_version": "v1"}) == []

    def test_an_empty_filter_keeps_everything(self):
        assert filter_by_partitions(self._PATHS, {}) == sorted(self._PATHS)


@pytest.mark.spark
class TestReadRowCount:
    def test_sums_rows_across_files_without_reading_them(self, spark, tmp_path):
        import glob

        out = str(tmp_path / "rows")
        spark.range(0, 250).repartition(3).write.parquet(out)
        files = sorted(glob.glob(out + "/*.parquet"))
        assert len(files) == 3
        assert read_row_count(spark, files) == 250

    def test_a_subset_of_files_counts_only_those(self, spark, tmp_path):
        import glob

        out = str(tmp_path / "part")
        spark.range(0, 100).coalesce(1).write.parquet(out + "/p=1")
        spark.range(0, 7).coalesce(1).write.parquet(out + "/p=2")
        only_p2 = sorted(glob.glob(out + "/p=2/*.parquet"))
        assert read_row_count(spark, only_p2) == 7

    def test_no_files_is_zero_rows(self, spark):
        assert read_row_count(spark, []) == 0


# --- landed_partition_files: which files B8 and B10's test months read ---
# Pure path work, deliberately: it decides *which* footers a gate reads, and
# getting it wrong is silent in both directions — too few files understates a
# column's maximum and passes a lossy column, too many drags in months this run
# never touched. Neither shows up as an error, so both need a test rather than a
# Spark run to notice.

BASE = "hdfs://nn/wh/pft"
VERSION = "ab12cd34"


def _path(version: str, month: str, name: str = "part-0.parquet") -> str:
    return f"{BASE}/base_dataset_version={version}/snap_date={month}/{name}"


class TestLandedPartitionFiles:
    def test_keeps_only_the_months_this_run_processed(self):
        paths = [_path(VERSION, "2026-01-31"), _path(VERSION, "2026-02-28")]
        got = landed_partition_files(
            paths,
            base_version=VERSION,
            time_col="snap_date",
            months=[pd.Timestamp("2026-01-31")],
        )
        assert got == [_path(VERSION, "2026-01-31")]

    def test_other_base_dataset_versions_are_excluded(self):
        # Whether inputFiles() lists other versions of a catalog-loaded table is
        # unsettled (the module docstring). If it does, without this the gate
        # would read another version's parquet and report a column that this
        # run never wrote.
        paths = [_path(VERSION, "2026-01-31"), _path("ffffffff", "2026-01-31")]
        got = landed_partition_files(
            paths, base_version=VERSION, time_col="snap_date",
            months=[pd.Timestamp("2026-01-31")],
        )
        assert got == [_path(VERSION, "2026-01-31")]

    def test_a_month_spelled_differently_in_the_path_still_matches(self):
        # The partition value is whatever the source column held, not the
        # spelling `test_snap_dates` used (A26 exists because those two can
        # disagree). Comparing as calendar days is what keeps the gate from
        # silently reading zero files.
        paths = [_path(VERSION, "20260131")]
        got = landed_partition_files(
            paths, base_version=VERSION, time_col="snap_date",
            months=[pd.Timestamp("2026-01-31")],
        )
        assert got == paths

    def test_an_unparseable_partition_value_is_skipped_not_raised(self):
        paths = [_path(VERSION, "__HIVE_DEFAULT_PARTITION__"),
                 _path(VERSION, "2026-01-31")]
        got = landed_partition_files(
            paths, base_version=VERSION, time_col="snap_date",
            months=[pd.Timestamp("2026-01-31")],
        )
        assert got == [_path(VERSION, "2026-01-31")]

    def test_every_file_of_a_matching_month_is_kept(self):
        paths = [_path(VERSION, "2026-01-31", "part-0.parquet"),
                 _path(VERSION, "2026-01-31", "part-1.parquet")]
        got = landed_partition_files(
            paths, base_version=VERSION, time_col="snap_date",
            months=[pd.Timestamp("2026-01-31")],
        )
        assert sorted(got) == sorted(paths)

    def test_empty_month_list_selects_nothing(self):
        got = landed_partition_files(
            [_path(VERSION, "2026-01-31")], base_version=VERSION,
            time_col="snap_date", months=[],
        )
        assert got == []

    def test_result_is_deterministic(self):
        paths = [_path(VERSION, "2026-02-28"), _path(VERSION, "2026-01-31")]
        months = [pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28")]
        first = landed_partition_files(
            paths, base_version=VERSION, time_col="snap_date", months=months)
        second = landed_partition_files(
            list(reversed(paths)), base_version=VERSION,
            time_col="snap_date", months=list(reversed(months)))
        assert first == second


@pytest.mark.spark
class TestAFrameListingTwoVersions:
    """The version filter, pinned on a frame whose ``inputFiles()`` does list
    two versions side by side.

    Whether a frame the catalog loaded lists other versions is unsettled (the
    module docstring). A frame read from the table's root lists them for
    certain, so these hold whichever way that question comes out: each count
    is the version it was asked for, never the table's.
    """

    @pytest.fixture
    def two_versions(self, spark, tmp_path):
        from pyspark.sql import functions as F

        root = str(tmp_path / "t")
        for version, rows in (("v1", 7), ("v2", 100)):
            (spark.range(0, rows)
             .withColumn("base_dataset_version", F.lit(version))
             .withColumn("train_variant_id", F.lit("t1"))
             .withColumn("snap_date", F.lit("2026-01-31"))
             .write.mode("append")
             .partitionBy("base_dataset_version", "train_variant_id", "snap_date")
             .parquet(root))
        return spark.read.parquet(root)

    def test_the_frame_lists_both_versions(self, two_versions):
        # The premise. Without it the two below would pass for a gate that
        # filters nothing.
        assert {
            partition_value(p, "base_dataset_version")
            for p in two_versions.inputFiles()
        } == {"v1", "v2"}

    def test_a_table_compared_whole_counts_its_own_version(self, two_versions):
        rows, files, all_files = footer_rows(
            two_versions, {"base_dataset_version": "v1", "train_variant_id": "t1"})
        assert rows == 7
        assert files < all_files

    def test_a_table_compared_by_month_counts_its_own_version(self, two_versions):
        counted = footer_rows_by_month(
            two_versions, base_version="v1", time_col="snap_date",
            months=["2026-01-31"],
        )
        assert counted[pd.Timestamp("2026-01-31")][0] == 7

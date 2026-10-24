"""Tests for the facts B10 stands on: which files this run landed, and how
their rows are counted.

The gate answers "how many rows landed" with a ``count()`` that Spark plans to
read parquet footers only — the I/O half of ADR-0006's cost invariant, as
amended for #521.
Its halves are tested apart: partition-path parsing and file selection are
pure string work and need no session, while the counts and the plan leaves
they are planned on are exercised against real parquet files — a hand-built
fixture would only prove the parser matches the fixture.
"""

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.steps.footer_facts import (
    filter_by_partitions,
    group_by_partition,
    landed_partition_files,
    monthly_rows,
    partition_value,
    plan_monthly_count,
    plan_table_count,
    scan_leaves,
    table_rows,
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

    def test_the_listed_value_is_unescaped_to_what_was_written(self):
        # How inputFiles() lists a TIMESTAMP month: the URI escapes the space
        # and the `%` of the directory name's own `%3A`. Read as is, it is no
        # date, and B10 found no file for the month (#558).
        path = "file:///wh/tbl/snap_date=2026-01-31%2000%253A00%253A00/p-0.parquet"
        assert partition_value(path, "snap_date") == "2026-01-31 00:00:00"


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
class TestTableCount:
    def test_counts_every_row_of_the_frame(self, spark, tmp_path):
        out = str(tmp_path / "rows")
        spark.range(0, 250).repartition(3).write.parquet(out)
        planned = plan_table_count(spark.read.parquet(out), {})
        assert table_rows(planned) == 250
        assert (planned.files_in_scope, planned.files_in_table) == (3, 3)

    def test_the_scope_narrows_the_listing_and_not_the_rows(self, spark, tmp_path):
        # The rows are the frame's: a catalog-loaded frame's WHERE is already
        # the scope. The scope narrows only the file listing, which is what
        # B10's pre-check reads.
        out = str(tmp_path / "part")
        spark.range(0, 100).coalesce(1).write.parquet(out + "/p=1")
        spark.range(0, 7).coalesce(1).write.parquet(out + "/p=2")
        planned = plan_table_count(spark.read.parquet(out), {"p": "2"})
        assert table_rows(planned) == 107
        assert (planned.files_in_scope, planned.files_in_table) == (1, 2)


@pytest.mark.spark
class TestScanLeaves:
    """What the cost guard is handed: each leaf's node class and the data
    columns it reads. Judging them is ``model_input_grain_count_plan_errors``'
    (tests/test_core/test_consistency.py); these pin what is read off a plan."""

    @pytest.fixture
    def landed(self, spark, tmp_path):
        from pyspark.sql import functions as F

        out = str(tmp_path / "t")
        (spark.range(0, 10).withColumn("x", F.col("id") * 2)
         .withColumn("p", F.col("id") % 2)
         .write.partitionBy("p").parquet(out))
        return spark.read.parquet(out)

    def test_a_count_reads_no_column(self, landed):
        assert scan_leaves(landed.groupBy().count()) == [
            ("FileSourceScanExec", 0)]

    def test_a_count_grouped_by_a_partition_column_reads_no_column(self, landed):
        assert scan_leaves(landed.groupBy("p").count()) == [
            ("FileSourceScanExec", 0)]

    def test_a_count_filtered_on_a_data_column_reads_that_column(self, landed):
        from pyspark.sql import functions as F

        assert scan_leaves(landed.where(F.col("x") > 3).groupBy().count()) == [
            ("FileSourceScanExec", 1)]

    def test_a_frame_not_read_from_files_has_no_file_scan(self, spark):
        leaves = scan_leaves(
            spark.createDataFrame([(1,)], ["a"]).groupBy().count())
        assert len(leaves) == 1
        assert leaves[0].node != "FileSourceScanExec"
        assert leaves[0].columns_read is None


# --- landed_partition_files: which files B10's test months read ---
# Pure path work, deliberately: it decides *which* partitions the gate counts
# for a test month, and getting it wrong is silent in both directions — too
# few undercounts a month's rows, too many drags in months this run never
# touched. Neither shows up as an error, so both need a test rather than a
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
        # With spark.sql.hive.manageFilesourcePartitions false, inputFiles()
        # of a catalog-loaded table lists other versions too (the module
        # docstring). Without this the gate would then count another
        # version's month as this run's.
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
class TestMonthlyCount:
    """A table compared a month at a time, landed the way the catalog lands
    test: under the version, one partition per month."""

    @staticmethod
    def _landed(spark, tmp_path, months, *, version=VERSION):
        """``{partition value: rows}`` written under ``version``."""
        from pyspark.sql import functions as F

        root = str(tmp_path / "t")
        for value, rows in months.items():
            (spark.range(0, rows)
             .withColumn("base_dataset_version", F.lit(version))
             .withColumn("snap_date", F.lit(value))
             .write.mode("append")
             .partitionBy("base_dataset_version", "snap_date")
             .parquet(root))
        return spark.read.parquet(root).drop("base_dataset_version")

    def test_each_planned_month_is_counted_and_others_are_not(
        self, spark, tmp_path,
    ):
        df = self._landed(spark, tmp_path, {
            "2026-01-31": 5, "2026-02-28": 3, "2026-03-31": 11})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-01-31", "2026-02-28"])
        assert monthly_rows(planned) == {
            pd.Timestamp("2026-01-31"): (5, 1),
            pd.Timestamp("2026-02-28"): (3, 1),
        }

    def test_a_month_spelled_differently_on_disk_is_counted(
        self, spark, tmp_path,
    ):
        # Read back, ``20260131`` is an integer partition and ``2026-01-31``
        # the plan's text: the month is found by date and counted.
        df = self._landed(spark, tmp_path, {"20260131": 4})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-01-31"])
        assert monthly_rows(planned) == {pd.Timestamp("2026-01-31"): (4, 1)}

    def test_a_timestamp_month_on_disk_is_counted(self, spark, tmp_path):
        # A TIMESTAMP time column lands its month with a time of day, which
        # the directory name and the listing each escape (#558).
        df = self._landed(spark, tmp_path, {"2026-01-31 00:00:00": 4})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-01-31"])
        assert monthly_rows(planned) == {pd.Timestamp("2026-01-31"): (4, 1)}

    def test_a_planned_month_with_no_file_is_zero(self, spark, tmp_path):
        df = self._landed(spark, tmp_path, {"2026-01-31": 5})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-01-31", "2026-02-28"])
        assert monthly_rows(planned)[pd.Timestamp("2026-02-28")] == (0, 0)

    def test_no_planned_month_on_disk_plans_no_count(self, spark, tmp_path):
        # A count over an empty filter is planned as an empty local relation,
        # which the cost guard would refuse; there is nothing to count anyway.
        df = self._landed(spark, tmp_path, {"2026-01-31": 5})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-02-28"])
        assert planned.counting is None
        assert monthly_rows(planned) == {pd.Timestamp("2026-02-28"): (0, 0)}

    def test_the_count_reads_no_column(self, spark, tmp_path):
        df = self._landed(spark, tmp_path, {"2026-01-31": 5, "20260228": 2})
        planned = plan_monthly_count(
            df, base_version=VERSION, time_col="snap_date",
            months=["2026-01-31", "2026-02-28"])
        assert scan_leaves(planned.counting) == [("FileSourceScanExec", 0)]


@pytest.mark.spark
class TestAFrameListingTwoVersions:
    """The version filter on the file listing, pinned on a frame whose
    ``inputFiles()`` does list two versions side by side.

    A catalog-loaded frame lists other versions when
    ``spark.sql.hive.manageFilesourcePartitions`` is false (the module
    docstring), while its ``WHERE`` keeps the rows to its own version. A frame
    read from the table's root and filtered on the version does both under
    any setting, so it stands in for that case here.

    What is asserted is the listing side only — the file counts B10's scope
    pre-check and report read, and which months it finds on disk for test.
    The rows are the fixture's own ``WHERE`` and would only echo it back; that
    the catalog's entries carry that ``WHERE`` is pinned on the catalog
    itself (``test_pipeline.py``, ``TestGrainGateWiring``).
    """

    @pytest.fixture
    def two_versions(self, spark, tmp_path):
        from pyspark.sql import functions as F

        root = str(tmp_path / "t")
        # v2 also holds a month v1 does not: the case where a listing that
        # reaches past the version would find a month this run never wrote.
        for version, month, rows in (("v1", "2026-01-31", 7),
                                     ("v2", "2026-01-31", 100),
                                     ("v2", "2026-02-28", 5)):
            (spark.range(0, rows)
             .withColumn("base_dataset_version", F.lit(version))
             .withColumn("train_variant_id", F.lit("t1"))
             .withColumn("snap_date", F.lit(month))
             .write.mode("append")
             .partitionBy("base_dataset_version", "train_variant_id", "snap_date")
             .parquet(root))
        return (spark.read.parquet(root)
                .where(F.col("base_dataset_version") == "v1")
                .drop("base_dataset_version", "train_variant_id"))

    @staticmethod
    def _v1_files(frame):
        return [p for p in frame.inputFiles()
                if partition_value(p, "base_dataset_version") == "v1"]

    def test_the_frame_lists_both_versions(self, two_versions):
        # The premise. Without it the ones below would pass for a gate that
        # filters nothing.
        assert {
            partition_value(p, "base_dataset_version")
            for p in two_versions.inputFiles()
        } == {"v1", "v2"}

    def test_a_table_compared_whole_lists_only_its_own_version(
        self, two_versions,
    ):
        planned = plan_table_count(
            two_versions, {"base_dataset_version": "v1", "train_variant_id": "t1"})
        assert planned.files_in_scope == len(self._v1_files(two_versions))
        assert planned.files_in_scope < planned.files_in_table

    def test_a_table_compared_by_month_lists_only_its_own_version(
        self, two_versions,
    ):
        planned = plan_monthly_count(
            two_versions, base_version="v1", time_col="snap_date",
            months=["2026-01-31"],
        )
        assert planned.files == {
            pd.Timestamp("2026-01-31"): len(self._v1_files(two_versions))}

    def test_a_month_only_another_version_holds_is_not_found(self, two_versions):
        # Found through v2's files, the month would be counted on the v1
        # frame (0 rows) yet reported as holding files, and its partition
        # value would be put in the count's filter.
        planned = plan_monthly_count(
            two_versions, base_version="v1", time_col="snap_date",
            months=["2026-02-28"],
        )
        assert planned.counting is None
        assert monthly_rows(planned) == {pd.Timestamp("2026-02-28"): (0, 0)}

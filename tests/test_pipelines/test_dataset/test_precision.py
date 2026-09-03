"""Tests for the B8 gate's file-selection mechanism.

Pure path work, deliberately: it decides *which* footers the gate reads, and
getting it wrong is silent in both directions — too few files understates a
column's maximum and passes a lossy column, too many drags in months this run
never touched. Neither shows up as an error, so both need a test rather than a
Spark run to notice.
"""

import pandas as pd

from recsys_tfb.pipelines.dataset.steps.precision import landed_partition_files

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
        # inputFiles() answers for the whole table, not for the partition_filter
        # the catalog applies on load. Without this the gate would read another
        # version's parquet and report a column that this run never wrote.
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

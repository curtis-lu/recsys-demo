"""Tests for recsys_tfb.utils.hdfs."""

from collections import namedtuple
from unittest.mock import MagicMock

import pytest


# DESCRIBE FORMATTED rows expose .col_name and .data_type
FakeRow = namedtuple("FakeRow", ["col_name", "data_type"])


class TestGetHiveTableLocation:
    def test_parses_location_from_describe_formatted(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("col1", "STRING"),
            FakeRow("col2", "INT"),
            FakeRow("Location", "hdfs://nn:9000/warehouse/db.foo"),
            FakeRow("Table Type", "MANAGED_TABLE"),
        ]

        result = get_hive_table_location(spark, "db", "foo")

        assert result == "hdfs://nn:9000/warehouse/db.foo"
        spark.sql.assert_called_once_with("DESCRIBE FORMATTED db.foo")

    def test_strips_whitespace_in_col_name_and_data_type(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("  Location  ", "  hdfs://nn/path  "),
        ]

        result = get_hive_table_location(spark, "db", "foo")
        assert result == "hdfs://nn/path"

    def test_raises_when_location_row_missing(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("col1", "STRING"),
        ]

        with pytest.raises(RuntimeError, match="Location not found"):
            get_hive_table_location(spark, "db", "foo")

    def test_raises_when_location_data_type_is_none(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("Location", None),
        ]

        with pytest.raises(RuntimeError, match="Location not found"):
            get_hive_table_location(spark, "db", "foo")


def _make_fake_spark():
    """Build a MagicMock spark simulating the JVM bridge surface we use."""
    spark = MagicMock()
    spark._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoop_conf")

    fs = MagicMock(name="FileSystem")
    spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    def make_path(s):
        p = MagicMock(name=f"Path({s})")
        p.__str__ = lambda self: s
        # getName() returns the basename — needed for glob path computation
        p.getName.return_value = s.rstrip("/").split("/")[-1] or "/"
        return p

    spark._jvm.org.apache.hadoop.fs.Path.side_effect = make_path
    return spark, fs


class TestCopyHdfsToLocal:
    def test_non_glob_calls_copyToLocalFile_once(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        dst = str(tmp_path / "out")

        copy_hdfs_to_local(spark, "hdfs://nn/foo/bar", dst)

        assert fs.copyToLocalFile.call_count == 1
        assert (tmp_path / "out").exists()  # mkdir done

    def test_non_glob_uses_filesystem_from_src_path(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        copy_hdfs_to_local(spark, "hdfs://nn/x", str(tmp_path / "y"))

        # FileSystem.get is called with the hadoop config we built above
        spark._jvm.org.apache.hadoop.fs.FileSystem.get.assert_called_once()

    def test_glob_iterates_over_globStatus_results(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()

        # Build two FileStatus mocks with different basenames
        def make_status(basename):
            status = MagicMock(name=f"FileStatus({basename})")
            inner_path = MagicMock(name=f"Path({basename})")
            inner_path.getName.return_value = basename
            status.getPath.return_value = inner_path
            return status

        fs.globStatus.return_value = [
            make_status("snap_date=2025-10-31"),
            make_status("snap_date=2025-09-30"),
        ]

        dst = str(tmp_path / "cache")
        copy_hdfs_to_local(
            spark, "hdfs://nn/foo/snap_date=*", dst, glob=True
        )

        # globStatus called once with src pattern
        fs.globStatus.assert_called_once()
        # copyToLocalFile called twice, one per match
        assert fs.copyToLocalFile.call_count == 2

    def test_glob_raises_when_no_matches(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        fs.globStatus.return_value = None  # Hadoop returns null on no match

        with pytest.raises(FileNotFoundError, match="No HDFS paths matched"):
            copy_hdfs_to_local(
                spark, "hdfs://nn/empty/*", str(tmp_path), glob=True
            )

    def test_glob_raises_when_empty_match_array(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        fs.globStatus.return_value = []

        with pytest.raises(FileNotFoundError, match="No HDFS paths matched"):
            copy_hdfs_to_local(
                spark, "hdfs://nn/empty/*", str(tmp_path), glob=True
            )

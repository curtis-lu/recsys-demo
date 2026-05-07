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

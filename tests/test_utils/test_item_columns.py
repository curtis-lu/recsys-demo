"""Combining a multi-column ``item`` into one column on read (#394, ADR-0027).

Every entry that reads one of the user's own tables calls
``combine_item_columns``; after it the frame looks exactly like one whose SQL
had already combined the columns, which is what everything downstream expects.
"""

import pytest

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.item_columns import combine_item_columns


def _schema(item):
    return get_schema({"schema": {"columns": {
        "time": "snap_date", "entity": ["user_id"], "item": item,
    }}})


MULTI = _schema(["campaign_id", "creative_format"])


def _rows(df):
    return sorted(tuple(r) for r in df.collect())


class TestSingleColumnIsUntouched:
    def test_the_same_frame_comes_back(self, spark):
        df = spark.createDataFrame([("2025-01-06", "u1", "fund")],
                                   ["snap_date", "user_id", "prod_name"])
        assert combine_item_columns(df, _schema("prod_name"), "sample_pool") is df

    def test_a_single_column_table_may_have_a_column_named_item(self, spark):
        """Nothing is combined, so nothing can be overwritten."""
        df = spark.createDataFrame([("2025-01-06", "u1", "fund")],
                                   ["snap_date", "user_id", "item"])
        assert combine_item_columns(df, _schema("item"), "sample_pool") is df


class TestCombining:
    def test_values_are_joined_with_a_dash_into_item(self, spark):
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "c01", "banner", 1)],
            ["snap_date", "user_id", "campaign_id", "creative_format", "label"],
        )
        out = combine_item_columns(df, MULTI, "label_table")
        assert out.columns == ["snap_date", "user_id", "label", "item"]
        assert _rows(out) == [("2025-01-06", "u1", 1, "c01-banner")]

    def test_the_source_columns_are_dropped(self, spark):
        """Left in, a candidate-level feature table would turn them into two
        new model features — not what combining in SQL ever did."""
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "c01", "banner", 0.5)],
            ["snap_date", "user_id", "campaign_id", "creative_format", "ctr_30m"],
        )
        out = combine_item_columns(df, MULTI, "candidate_feature_table")
        assert "campaign_id" not in out.columns
        assert "creative_format" not in out.columns
        assert "ctr_30m" in out.columns

    def test_the_declared_order_is_the_value_order(self, spark):
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "c01", "banner")],
            ["snap_date", "user_id", "campaign_id", "creative_format"],
        )
        reversed_schema = _schema(["creative_format", "campaign_id"])
        out = combine_item_columns(df, reversed_schema, "sample_pool")
        assert [r["item"] for r in out.collect()] == ["banner-c01"]

    def test_a_value_holding_a_dash_is_kept_as_is(self, spark):
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "cmp-01", "banner")],
            ["snap_date", "user_id", "campaign_id", "creative_format"],
        )
        out = combine_item_columns(df, MULTI, "sample_pool")
        assert [r["item"] for r in out.collect()] == ["cmp-01-banner"]

    def test_a_non_string_column_is_cast(self, spark):
        df = spark.createDataFrame(
            [("2025-01-06", "u1", 7, "banner")],
            ["snap_date", "user_id", "campaign_id", "creative_format"],
        )
        out = combine_item_columns(df, MULTI, "sample_pool")
        assert [r["item"] for r in out.collect()] == ["7-banner"]

    def test_a_null_part_makes_the_item_null(self, spark):
        """``concat_ws`` would skip the null: ``("a-b", null)`` would become
        ``a-b`` — the same item as ``("a", "b")`` — and join that item's
        labels. A null part gives a null item, the same as a null single
        item column."""
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "a-b", None), ("2025-01-06", "u2", "a", "b")],
            "snap_date string, user_id string, campaign_id string, creative_format string",
        )
        out = combine_item_columns(df, MULTI, "sample_pool")
        assert sorted((r["user_id"], r["item"]) for r in out.collect()) == [
            ("u1", None), ("u2", "a-b"),
        ]


class TestRefusals:
    def test_a_missing_source_column_names_the_table_and_the_column(self, spark):
        df = spark.createDataFrame([("2025-01-06", "u1", "c01")],
                                   ["snap_date", "user_id", "campaign_id"])
        with pytest.raises(DataConsistencyError) as exc:
            combine_item_columns(df, MULTI, "label_table")
        assert "label_table" in str(exc.value)
        assert "creative_format" in str(exc.value)

    def test_a_table_that_already_has_an_item_column_raises(self, spark):
        """The combined column would silently overwrite it."""
        df = spark.createDataFrame(
            [("2025-01-06", "u1", "c01", "banner", "x")],
            ["snap_date", "user_id", "campaign_id", "creative_format", "item"],
        )
        with pytest.raises(DataConsistencyError) as exc:
            combine_item_columns(df, MULTI, "sample_pool")
        assert "sample_pool" in str(exc.value)
        assert "already has a column named 'item'" in str(exc.value)

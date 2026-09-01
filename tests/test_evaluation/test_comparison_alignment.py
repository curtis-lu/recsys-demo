"""Tests for comparison.alignment — common_universe pure function."""

import pytest
from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.alignment import common_universe


@pytest.fixture
def df_a(spark):
    return spark.createDataFrame(
        [
            ("c1", "p1"), ("c1", "p2"),
            ("c2", "p1"), ("c2", "p3"),
            ("c3", "p1"),
        ],
        ["cust_id", "prod_name"],
    )


@pytest.fixture
def df_b(spark):
    return spark.createDataFrame(
        [
            ("c2", "p1"), ("c2", "p2"),
            ("c3", "p2"), ("c3", "p3"),
            ("c4", "p1"),
        ],
        ["cust_id", "prod_name"],
    )


def test_intersection_entities_and_items(df_a, df_b):
    entities, items = common_universe(df_a, df_b, ["cust_id"], "prod_name")
    # entities are tuples — one element per schema.entity column, even at one
    # column, so callers always join on the whole entity.
    assert entities == {("c2",), ("c3",)}
    assert items == {"p1", "p2", "p3"}


def test_intersection_uses_every_entity_column(spark):
    """Two entity columns: an entity is the pair, not its first column.

    ``b1`` appears on both sides and ``c1`` appears on both sides, yet the
    pair ``(b1, c1)`` exists only in A. Intersecting first columns would keep
    it; intersecting entities drops it.
    """
    a = spark.createDataFrame(
        [("b1", "c1", "p1"), ("b1", "c2", "p1")],
        ["branch_id", "cust_id", "prod_name"],
    )
    b = spark.createDataFrame(
        [("b1", "c2", "p1"), ("b2", "c1", "p1")],
        ["branch_id", "cust_id", "prod_name"],
    )
    entities, items = common_universe(a, b, ["branch_id", "cust_id"], "prod_name")
    assert entities == {("b1", "c2")}
    assert items == {"p1"}


def test_empty_entity_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c9", "p1")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_entities"):
        common_universe(a, b, ["cust_id"], "prod_name")


def test_empty_item_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c1", "p9")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_items"):
        common_universe(a, b, ["cust_id"], "prod_name")

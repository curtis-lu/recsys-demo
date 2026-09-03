"""Tests for comparison.alignment — common_universe pure function."""

import pytest
from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.evaluation.comparison.alignment import common_universe


def _entity_tuples(df: SparkDataFrame) -> set[tuple]:
    """Collect an entity DataFrame in the test — fixtures here are tiny.

    Production never does this: the whole point of #275 is that the entity
    side stays in Spark. See ``test_entity_side_never_returns_to_driver``.
    """
    return {tuple(r) for r in df.collect()}


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
    # The entity DataFrame carries one row per entity, one column per
    # schema.entity column — so callers always join on the whole entity.
    assert entities.columns == ["cust_id"]
    assert _entity_tuples(entities) == {("c2",), ("c3",)}
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
    assert entities.columns == ["branch_id", "cust_id"]
    assert _entity_tuples(entities) == {("b1", "c2")}
    assert items == {"p1"}


def test_entity_side_never_returns_to_driver(df_a, df_b, monkeypatch):
    """The entity intersection stays in Spark; only items come back (#275).

    Production entity populations are millions of rows, and a ``.collect()``
    of them lands in the driver's *Python* heap — the one
    ``spark.driver.memory`` does not protect. Items are 22 products
    (ADR-0010), so collecting those is correct and stays.

    Asserting on the intersection's *values* would not catch a regression
    here: the values are the same either way. So spy on every
    ``DataFrame.collect`` and assert which columns each one pulled back.
    """
    collected_columns: list[list[str]] = []
    original_collect = SparkDataFrame.collect

    def spy(self):
        collected_columns.append(list(self.columns))
        return original_collect(self)

    monkeypatch.setattr(SparkDataFrame, "collect", spy)

    entities, items = common_universe(df_a, df_b, ["cust_id"], "prod_name")

    assert isinstance(entities, SparkDataFrame)
    assert collected_columns == [["prod_name"], ["prod_name"]]
    assert items == {"p1", "p2", "p3"}


def test_empty_check_does_not_count_on_the_happy_path(df_a, df_b, monkeypatch):
    """``count()`` is a full Spark action — it may only run when raising.

    Deciding "is the intersection empty" with ``count()`` would put two extra
    passes over a million-row table on every successful compare.
    """
    counted: list[list[str]] = []
    original_count = SparkDataFrame.count

    def spy(self):
        counted.append(list(self.columns))
        return original_count(self)

    monkeypatch.setattr(SparkDataFrame, "count", spy)

    common_universe(df_a, df_b, ["cust_id"], "prod_name")

    assert counted == []


def test_empty_entity_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c9", "p1")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_entities"):
        common_universe(a, b, ["cust_id"], "prod_name")


def test_empty_entity_message_still_reports_both_side_counts(spark):
    """The B3 message keeps its two population numbers (#275 must not drop them)."""
    a = spark.createDataFrame(
        [("c1", "p1"), ("c2", "p1")], ["cust_id", "prod_name"]
    )
    b = spark.createDataFrame([("c9", "p1")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError) as excinfo:
        common_universe(a, b, ["cust_id"], "prod_name")
    assert "A has 2 entities, B has 1 entities" in str(excinfo.value)


def test_empty_item_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c1", "p9")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_items"):
        common_universe(a, b, ["cust_id"], "prod_name")

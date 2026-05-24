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


def test_intersection_cust_and_prod(df_a, df_b):
    cust, prod = common_universe(df_a, df_b, "cust_id", "prod_name")
    assert cust == {"c2", "c3"}
    assert prod == {"p1", "p2", "p3"}


def test_empty_cust_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c9", "p1")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_cust"):
        common_universe(a, b, "cust_id", "prod_name")


def test_empty_prod_intersection_raises(spark):
    a = spark.createDataFrame([("c1", "p1")], ["cust_id", "prod_name"])
    b = spark.createDataFrame([("c1", "p9")], ["cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_prod"):
        common_universe(a, b, "cust_id", "prod_name")

"""Tests for preprocessing._spark private helpers."""

import pytest
from decimal import Decimal
from pyspark.sql import types as T

from recsys_tfb.preprocessing._spark import _cast_feature_decimals_to_float

pytestmark = pytest.mark.spark


@pytest.fixture
def mixed_df(spark):
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("label", T.IntegerType()),
        T.StructField("feature_a", T.DecimalType(38, 6)),
        T.StructField("feature_b", T.IntegerType()),
        T.StructField("feature_c", T.DecimalType(29, 0)),
        T.StructField("non_feature_decimal", T.DecimalType(15, 2)),
    ])
    rows = [
        ("C001", 1, Decimal("1.500000"), 10, Decimal("123"), Decimal("9.99")),
        ("C002", 0, Decimal("2.250000"), 20, Decimal("456"), Decimal("8.88")),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _dtype(df, col):
    return dict(df.dtypes)[col]


def test_cast_feature_decimals_casts_only_feature_decimals(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    out, _ = _cast_feature_decimals_to_float(mixed_df, feature_cols)

    assert _dtype(out, "feature_a") == "float"
    assert _dtype(out, "feature_c") == "float"
    # int feature untouched
    assert _dtype(out, "feature_b") == "int"
    # non-feature decimal untouched (not in feature_cols)
    assert _dtype(out, "non_feature_decimal").startswith("decimal")
    # identity / label untouched
    assert _dtype(out, "cust_id") == "string"
    assert _dtype(out, "label") == "int"


def test_cast_feature_decimals_returns_casted_list(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = _cast_feature_decimals_to_float(mixed_df, feature_cols)
    assert sorted(casted) == ["feature_a", "feature_c"]


def test_cast_feature_decimals_noop_when_no_decimals(spark):
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("feature_a", T.IntegerType()),
        T.StructField("feature_b", T.DoubleType()),
    ])
    df = spark.createDataFrame([("C001", 1, 2.5)], schema=schema)
    out, casted = _cast_feature_decimals_to_float(df, ["feature_a", "feature_b"])

    assert casted == []
    assert out.schema == df.schema


def test_cast_feature_decimals_preserves_values(mixed_df):
    feature_cols = ["feature_a"]
    out, _ = _cast_feature_decimals_to_float(mixed_df, feature_cols)
    rows = out.orderBy("cust_id").collect()
    assert rows[0].feature_a == pytest.approx(1.5)
    assert rows[1].feature_a == pytest.approx(2.25)


import pandas as pd
from recsys_tfb.preprocessing._spark import build_model_input


class TestFilterGroupsWithPositives:
    """Spark-side group-positive filter for val/test_model_input.

    A (time, *entity) group is dropped iff every label in that group is 0.
    Used at dataset-write time so val_model_input / test_model_input on Hive
    only contain customers with at least one positive label per snap_date.
    """

    def test_drops_all_zero_groups(self, spark):
        from recsys_tfb.preprocessing._spark import filter_groups_with_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
            "prod_name": ["a", "b"] * 3,
            "label": [1, 0, 0, 1, 0, 0],
            "feat": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }))
        out = filter_groups_with_positives(df, ["snap_date", "cust_id"], "label")
        rows = out.orderBy("cust_id", "prod_name").collect()
        # c3 dropped, c1 and c2 retained entirely (2 rows each)
        assert len(rows) == 4
        assert {r.cust_id for r in rows} == {"c1", "c2"}

    def test_keeps_all_rows_of_positive_groups(self, spark):
        """Group with even one positive row keeps every row in that group."""
        from recsys_tfb.preprocessing._spark import filter_groups_with_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "cust_id": ["c1"] * 4,
            "prod_name": ["a", "b", "c", "d"],
            "label": [0, 0, 1, 0],
        }))
        out = filter_groups_with_positives(df, ["snap_date", "cust_id"], "label")
        assert out.count() == 4

    def test_groups_split_across_snap_dates(self, spark):
        """(snap_date, cust_id) is the group key — same cust across two snaps
        is two separate groups."""
        from recsys_tfb.preprocessing._spark import filter_groups_with_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(
                ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"]
            ),
            "cust_id": ["c1", "c1", "c1", "c1"],
            "prod_name": ["a", "b", "a", "b"],
            "label": [1, 0, 0, 0],
        }))
        out = filter_groups_with_positives(df, ["snap_date", "cust_id"], "label")
        rows = out.orderBy("snap_date", "prod_name").collect()
        # 2025-01 has positive → keep both rows; 2025-02 all-zero → drop
        assert len(rows) == 2
        assert all(str(r.snap_date).startswith("2025-01") for r in rows)

    def test_preserves_column_schema(self, spark):
        from recsys_tfb.preprocessing._spark import filter_groups_with_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"]),
            "cust_id": ["c1"],
            "prod_name": ["a"],
            "label": [1],
            "feat": [3.14],
        }))
        out = filter_groups_with_positives(df, ["snap_date", "cust_id"], "label")
        assert out.columns == df.columns

    def test_empty_when_no_positives(self, spark):
        from recsys_tfb.preprocessing._spark import filter_groups_with_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": ["c1", "c2"],
            "prod_name": ["a", "b"],
            "label": [0, 0],
        }))
        out = filter_groups_with_positives(df, ["snap_date", "cust_id"], "label")
        assert out.count() == 0


class TestBuildModelInputCarry:
    def _prep(self):
        return {"feature_columns": ["prod_name", "f1"],
                "categorical_columns": ["prod_name"],
                "category_mappings": {"prod_name": ["a", "b"]},
                "drop_columns": []}

    def _params(self):
        return {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}

    def _frames(self, spark, with_carry):
        kcols = {"snap_date": pd.to_datetime(["2025-01-31"] * 2),
                 "cust_id": [1, 2], "prod_name": ["a", "b"]}
        if with_carry:
            kcols["cust_segment_typ"] = ["mass", "hnw"]
        keys = spark.createDataFrame(pd.DataFrame(kcols))
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0]}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        return keys, feats, labels

    def test_carry_in_output_when_present_in_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=True)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" in out.columns

    def test_no_carry_when_absent_from_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=False)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" not in out.columns

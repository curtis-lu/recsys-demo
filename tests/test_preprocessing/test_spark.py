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

"""Tests for the encoding / dtype mechanics shared across pipelines.

Scope matches the module's: only what has callers in more than one pipeline.
The dataset-only and inference-only behaviour that used to sit in
``preprocessing/`` is tested next to where it now lives — dataset node tests,
``test_model_input.py``, ``test_feature_columns.py``, and the models-layer
feature-selection tests.
"""

import pandas as pd
import pytest
from decimal import Decimal
from pyspark.sql import types as T

from recsys_tfb.preprocessing import (
    PreprocessorMetadata,
    _cast_feature_floats_to_float32,
    _encode_categoricals,
)

# Marked per item rather than for the module: the key contract below needs no
# SparkSession, and `pytest -m 'not spark'` is the documented fast dev loop — the
# loop in which a renamed artifact key most needs to turn red.


class TestPreprocessorMetadataContract:
    """The four keys of ``preprocessor.json``, pinned at their definition point.

    The writer is the dataset pipeline's fit node and the reader is the
    inference pipeline's apply node; before #168 they agreed only because they
    shared a file. This is the definition half of the guard — the other half is
    ``test_nodes_spark.py::TestFitPreprocessorMetadataKeyContract``, which
    asserts the real fit output against these same annotations, so a key renamed
    on the dataset side turns red there rather than in the company environment.
    """

    def test_the_contract_is_exactly_these_four_keys(self):
        # Spelled out rather than derived: this test's whole job is to be the
        # place a key rename has to argue with. Deriving it from the class would
        # make it pass for any set of keys at all.
        assert set(PreprocessorMetadata.__annotations__) == {
            "feature_columns",
            "categorical_columns",
            "category_mappings",
            "drop_columns",
        }

    # No reader-side test here on purpose. An earlier draft scanned
    # ``apply_preprocessor``'s AST for the keys it subscripts, which asserted on
    # syntax rather than behaviour and would have gone red on a legal rewrite to
    # ``.get()`` or destructuring. It was also redundant: renaming a key the
    # reader uses turns 8 tests in test_pipelines/test_inference/test_nodes_spark.py
    # red already (measured, not assumed). The reader side is covered; the writer
    # side is what had no guard, and that is TestFitPreprocessorMetadataKeyContract.


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


@pytest.mark.spark
def test_cast_feature_decimals_casts_only_feature_decimals(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    out, _ = _cast_feature_floats_to_float32(mixed_df, feature_cols)

    assert _dtype(out, "feature_a") == "float"
    assert _dtype(out, "feature_c") == "float"
    # int feature untouched
    assert _dtype(out, "feature_b") == "int"
    # non-feature decimal untouched (not in feature_cols)
    assert _dtype(out, "non_feature_decimal").startswith("decimal")
    # identity / label untouched
    assert _dtype(out, "cust_id") == "string"
    assert _dtype(out, "label") == "int"


@pytest.mark.spark
def test_cast_feature_decimals_returns_casted_list(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = _cast_feature_floats_to_float32(mixed_df, feature_cols)
    assert sorted(casted) == ["feature_a", "feature_c"]


@pytest.mark.spark
def test_cast_features_noop_when_nothing_castable(spark):
    """No-op when feature_cols contain no Decimal/Double — IntegerType and
    FloatType (already float32) pass through with the schema unchanged."""
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("feature_a", T.IntegerType()),
        T.StructField("feature_b", T.FloatType()),
    ])
    df = spark.createDataFrame([("C001", 1, 2.5)], schema=schema)
    out, casted = _cast_feature_floats_to_float32(df, ["feature_a", "feature_b"])

    assert casted == []
    assert out.schema == df.schema


@pytest.mark.spark
def test_cast_feature_decimals_preserves_values(mixed_df):
    feature_cols = ["feature_a"]
    out, _ = _cast_feature_floats_to_float32(mixed_df, feature_cols)
    rows = out.orderBy("cust_id").collect()
    assert rows[0].feature_a == pytest.approx(1.5)
    assert rows[1].feature_a == pytest.approx(2.25)


@pytest.mark.spark
def test_cast_feature_doubles_to_float32(spark):
    """DoubleType feature cols must also be cast to float (float32).

    LightGBM is histogram-based (max_bin=256); float32's 7-digit precision
    is well past binning resolution, so float64/DoubleType is wasted budget.
    """
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("feature_a", T.DoubleType()),
        T.StructField("feature_b", T.DoubleType()),
        T.StructField("non_feature_double", T.DoubleType()),
        T.StructField("feature_c", T.IntegerType()),
    ])
    df = spark.createDataFrame(
        [("C001", 1.5, 2.5, 9.99, 10)], schema=schema
    )
    out, casted = _cast_feature_floats_to_float32(
        df, ["feature_a", "feature_b", "feature_c"]
    )
    # DoubleType feature cols cast to float
    assert _dtype(out, "feature_a") == "float"
    assert _dtype(out, "feature_b") == "float"
    # int feature untouched
    assert _dtype(out, "feature_c") == "int"
    # non-feature DoubleType untouched (not in feature_cols)
    assert _dtype(out, "non_feature_double") == "double"
    # Returned list reports the casted DoubleType cols
    assert sorted(casted) == ["feature_a", "feature_b"]


@pytest.mark.spark
def test_cast_mixed_decimal_and_double(spark):
    """Mixed feature_cols (DecimalType + DoubleType + FloatType + IntegerType):
    Decimal and Double both → float; Float untouched (already float32);
    Integer untouched."""
    schema = T.StructType([
        T.StructField("dec_col", T.DecimalType(38, 6)),
        T.StructField("dbl_col", T.DoubleType()),
        T.StructField("flt_col", T.FloatType()),
        T.StructField("int_col", T.IntegerType()),
    ])
    df = spark.createDataFrame(
        [(Decimal("1.23"), 4.56, 7.89, 10)], schema=schema
    )
    feature_cols = ["dec_col", "dbl_col", "flt_col", "int_col"]
    out, casted = _cast_feature_floats_to_float32(df, feature_cols)

    assert _dtype(out, "dec_col") == "float"
    assert _dtype(out, "dbl_col") == "float"
    assert _dtype(out, "flt_col") == "float"
    assert _dtype(out, "int_col") == "int"
    assert sorted(casted) == ["dbl_col", "dec_col"]


@pytest.mark.spark
class TestEncodeCategoricalsEmptyMapping:
    """D15 — the whole-column-unknown branch of ``_encode_categoricals``.

    A category with no values fit (e.g. every row NULL in the train window)
    encodes the entire column to -1. The branch is not a shortcut for the
    general path: ``F.create_map()`` with no pairs is typed ``map<void,void>``
    and indexing it raises ``cannot resolve 'map()[col]' ... requires void
    type``, so deleting the branch fails loudly rather than producing the same
    answer more slowly. Verified by running the branchless form against both a
    string and a bigint column.
    """

    def test_empty_mapping_encodes_every_row_to_unknown(self, spark):
        df = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c2", "c3"],
            "risk_attr": ["low", "high", None],
        }))
        out = _encode_categoricals(df, ["risk_attr"], {"risk_attr": []})
        assert [r.risk_attr for r in out.orderBy("cust_id").collect()] == [-1, -1, -1]

    def test_empty_mapping_output_is_integer_typed(self, spark):
        """The encoded column must be an int like every other encoded column.

        Compared against the type object, not ``dtypes``' string form: a
        substring or repr comparison is satisfied by whatever ``simpleString``
        happens to emit and would survive a change of width.
        """
        from pyspark.sql import types as T

        df = spark.createDataFrame(pd.DataFrame({"risk_attr": ["low", "high"]}))
        out = _encode_categoricals(df, ["risk_attr"], {"risk_attr": []})
        assert out.schema["risk_attr"].dataType == T.IntegerType()

    def test_populated_mapping_still_encodes_by_position(self, spark):
        """The paired half: with values present the column is *not* all -1.

        Without this, "everything is -1" would also pass for an implementation
        that ignored ``category_mappings`` altogether.
        """
        df = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c2", "c3"],
            "risk_attr": ["low", "high", "unseen"],
        }))
        out = _encode_categoricals(df, ["risk_attr"], {"risk_attr": ["low", "high"]})
        # Index in the mapping list: low->0, high->1, anything else -> -1.
        assert [r.risk_attr for r in out.orderBy("cust_id").collect()] == [0, 1, -1]

"""Tests for the encoding / dtype mechanics shared across pipelines.

Scope matches the module's: only what has callers in more than one pipeline.
The dataset-only and inference-only behaviour that used to sit in
``preprocessing/`` is tested next to where it now lives — dataset node tests,
``test_model_input.py``, ``test_feature_columns.py``, and the models-layer
feature-selection tests.
"""

import numpy as np
import pandas as pd
import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql import types as T

from recsys_tfb.core.consistency import NUMERIC_STORAGE_TYPES
from recsys_tfb.preprocessing import (
    SPARK_CAST_TARGET,
    PreprocessorMetadata,
    cast_numeric_features_to_storage_type,
    encode_categoricals,
)

# Marked per item rather than for the module: the key contract below needs no
# SparkSession, and `pytest -m 'not spark'` is the documented fast dev loop — the
# loop in which a renamed artifact key most needs to turn red.


class TestPreprocessorMetadataContract:
    """The four keys of ``preprocessor.json``, pinned at their definition point.

    The writer is the dataset pipeline's fit node and the reader is the
    inference pipeline's apply node; before #168 they agreed only because they
    shared a file. This is the definition half of the guard — the other half is
    ``test_dataset/test_nodes.py::TestFitPreprocessorMetadataKeyContract``, which
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
    # reader uses turns 8 tests in test_pipelines/test_inference/test_nodes.py
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
def test_cast_covers_every_numeric_feature_column(mixed_df):
    """#283 — every numeric feature type converges, not just the float-like ones.

    ``feature_b`` is the column this test exists for: an IntegerType feature
    used to pass through untouched, and one such column is enough to make
    pandas pick float64 for the *whole* matrix (see
    ``test_a_frame_of_mixed_numeric_features_flattens_to_one_numeric_dtype``).
    """
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    out, _ = cast_numeric_features_to_storage_type(mixed_df, feature_cols, "float32")

    assert _dtype(out, "feature_a") == "float"     # decimal
    assert _dtype(out, "feature_c") == "float"     # decimal
    assert _dtype(out, "feature_b") == "float"     # integer — new in #283
    # non-feature decimal untouched (not in feature_cols)
    assert _dtype(out, "non_feature_decimal").startswith("decimal")
    # identity / label untouched — they are not in feature_cols by construction
    assert _dtype(out, "cust_id") == "string"
    assert _dtype(out, "label") == "int"


@pytest.mark.spark
def test_cast_returns_every_column_it_converted(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = cast_numeric_features_to_storage_type(mixed_df, feature_cols, "float32")
    assert sorted(casted) == ["feature_a", "feature_b", "feature_c"]


@pytest.mark.spark
def test_cast_leaves_a_non_numeric_feature_column_alone(spark):
    """The selector is a whitelist: a type it does not recognise is not cast.

    A string feature column is B6's business, not the cast's — feeding it to
    ``.cast("float")`` would turn every value into NULL and hand LightGBM a
    silently empty column where the gate would have named the problem.
    """
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("feature_a", T.StringType()),
        T.StructField("feature_b", T.TimestampType()),
    ])
    df = spark.createDataFrame(
        [("C001", "x", datetime(2026, 1, 31))], schema=schema
    )
    out, casted = cast_numeric_features_to_storage_type(
        df, ["feature_a", "feature_b"], "float32",
    )

    assert casted == []
    assert out.schema == df.schema


@pytest.mark.spark
def test_cast_feature_decimals_preserves_values(mixed_df):
    feature_cols = ["feature_a"]
    out, _ = cast_numeric_features_to_storage_type(mixed_df, feature_cols, "float32")
    rows = out.orderBy("cust_id").collect()
    assert rows[0].feature_a == pytest.approx(1.5)
    assert rows[1].feature_a == pytest.approx(2.25)


@pytest.mark.spark
def test_cast_covers_the_whole_numeric_whitelist(spark):
    """One frame holding every castable Spark type, all of them converged.

    Spelled out per type rather than derived from ``CASTABLE_NUMERIC_TYPES``:
    deriving it would make the test pass for whatever that tuple happens to
    say, including a tuple that had quietly lost boolean again.
    """
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("f_dec", T.DecimalType(38, 6)),
        T.StructField("f_dbl", T.DoubleType()),
        T.StructField("f_flt", T.FloatType()),
        T.StructField("f_byte", T.ByteType()),
        T.StructField("f_short", T.ShortType()),
        T.StructField("f_int", T.IntegerType()),
        T.StructField("f_long", T.LongType()),
        T.StructField("f_bool", T.BooleanType()),
        T.StructField("non_feature_dbl", T.DoubleType()),
    ])
    df = spark.createDataFrame(
        [("C001", Decimal("1.5"), 2.5, 3.5, 1, 2, 3, 4, True, 9.99)],
        schema=schema,
    )
    feature_cols = [c for c in df.columns
                    if c.startswith("f_")]
    out, casted = cast_numeric_features_to_storage_type(
        df, feature_cols, "float32",
    )

    assert sorted(casted) == sorted(feature_cols)
    assert [_dtype(out, c) for c in feature_cols] == ["float"] * len(feature_cols)
    # Outside feature_cols, so not the cast's business however numeric it is.
    assert _dtype(out, "non_feature_dbl") == "double"
    assert _dtype(out, "cust_id") == "string"


@pytest.mark.spark
def test_a_frame_of_mixed_numeric_features_flattens_to_one_numeric_dtype(spark):
    """The reason the cast covers integers and boolean, asserted end to end.

    ``pdf_to_X`` flattens the feature frame with ``DataFrame.values``, and
    pandas picks **one** dtype for the whole matrix. Measured on pandas 1.5.3
    without the cast: float32 + int64 -> float64 (the matrix doubles for one
    column's sake), float32 + bool -> ``object`` (the boxed-object OOM B6
    exists to prevent, arriving through a dtype B6 admits).

    Both halves are asserted: the un-cast frame really does degrade, and the
    cast really does prevent it. Without the first half this would pass on a
    day pandas stopped doing either, and a guard that cannot fail is worse than
    none (known-pitfalls.md section 19).

    The degraded half is built in pandas directly rather than by calling
    ``toPandas()`` on the un-cast Spark frame. Not for convenience: pyspark
    3.3.2's ``toPandas`` reaches for ``np.bool``, removed in this numpy, so a
    BooleanType column raises there before pandas gets to choose a dtype at
    all. The claim under test is pandas', so pandas is where it is asserted.
    """
    schema = T.StructType([
        T.StructField("f_flt", T.FloatType()),
        T.StructField("f_long", T.LongType()),
        T.StructField("f_bool", T.BooleanType()),
    ])
    df = spark.createDataFrame([(1.5, 7, True)], schema=schema)
    feature_cols = ["f_flt", "f_long", "f_bool"]

    # Guard the guard: this is the frame the cast is preventing.
    uncast = pd.DataFrame({
        "f_flt": np.array([1.5], dtype=np.float32),
        "f_long": np.array([7], dtype=np.int64),
        "f_bool": np.array([True]),
    })
    assert uncast.values.dtype == object
    # …and each degradation on its own, so neither is carried by the other.
    assert uncast[["f_flt", "f_long"]].values.dtype == np.float64
    assert uncast[["f_flt", "f_bool"]].values.dtype == object

    out, _ = cast_numeric_features_to_storage_type(df, feature_cols, "float32")
    assert out.toPandas()[feature_cols].values.dtype == np.float32


@pytest.mark.spark
def test_float64_is_declarable_and_reaches_the_frame(spark):
    """The declaration is real (#283): float64 stores double, not float32.

    Before #283 the helper wrote float32 unconditionally, so declaring float64
    rebuilt every artifact and changed no stored value. A test that only
    checked float32 would not have noticed.
    """
    schema = T.StructType([
        T.StructField("f_dec", T.DecimalType(38, 6)),
        T.StructField("f_flt", T.FloatType()),
        T.StructField("f_int", T.IntegerType()),
    ])
    df = spark.createDataFrame([(Decimal("1.5"), 2.5, 3)], schema=schema)
    feature_cols = ["f_dec", "f_flt", "f_int"]

    out, casted = cast_numeric_features_to_storage_type(
        df, feature_cols, "float64",
    )

    assert sorted(casted) == feature_cols
    assert [_dtype(out, c) for c in feature_cols] == ["double"] * 3
    assert out.toPandas()[feature_cols].values.dtype == np.float64


def test_every_declarable_storage_type_has_a_spark_cast_target():
    """``SPARK_CAST_TARGET`` and the declarable vocabulary cannot drift apart.

    A31 decides what may be written in the config; this dict decides what the
    cast does with it. A third storage type added to one and not the other
    would pass A31 at CLI entry and then ``KeyError`` inside a Spark job, five
    nodes into a run.
    """
    assert set(SPARK_CAST_TARGET) == set(NUMERIC_STORAGE_TYPES)


@pytest.mark.spark
def test_cast_preserves_column_order_and_untouched_columns(mixed_df):
    """The rebuild must hand back the same frame shape it was given.

    ``withColumn`` replaced a column in place, so order and the columns nobody
    asked about came for free; a single ``select`` has to list every column, in
    order, to match that. Expected values are spelled out from the fixture
    rather than read back off ``mixed_df`` — a projection that quietly moves
    the cast columns to the end, or drops the ones outside ``feature_cols``,
    has to argue with this test.
    """
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    out, _ = cast_numeric_features_to_storage_type(mixed_df, feature_cols, "float32")

    assert out.columns == [
        "cust_id",
        "label",
        "feature_a",
        "feature_b",
        "feature_c",
        "non_feature_decimal",
    ]

    # ``feature_b`` is no longer in this list: since #283 an integer feature
    # column is cast like any other numeric one. Identity, label and the
    # non-feature decimal are what the projection must still carry through
    # untouched.
    untouched = ["cust_id", "label", "non_feature_decimal"]
    rows = out.orderBy("cust_id").collect()
    assert [[row[c] for c in untouched] for row in rows] == [
        ["C001", 1, Decimal("9.99")],
        ["C002", 0, Decimal("8.88")],
    ]


@pytest.mark.spark
class TestCastBuildsOneProjectionNotOnePerColumn:
    """#282 — the cast is one ``select``, not a ``withColumn`` per column.

    Why that matters, with the measurements: see the comment in
    ``cast_numeric_features_to_storage_type`` and known-pitfalls.md section 20. The
    numbers are deliberately not repeated here — three copies of one table
    drift apart.

    The guard asserts structure, not elapsed time (#278 Out of Scope 9): a
    "must finish in N seconds" test flakes on CI load and ends up skipped,
    which is worse than no test because it still looks like a guard. What is
    pinned here is the thing the analyzer actually charges for — how many
    Project nodes the plan carries — and that it stays flat as columns grow.

    ``test_the_withColumn_loop_this_replaced_does_grow_the_plan`` is not
    decoration: without it, "the count is 1" would still pass on a day when
    ``_analyzed_project_count`` had quietly stopped being able to see the
    difference, and a silently-always-true guard is what section 19 of
    known-pitfalls warns this exact style of test decays into.
    """

    @staticmethod
    def _analyzed_project_count(df) -> int:
        # The analyzed plan, deliberately not the optimized one: the optimizer's
        # CollapseProject folds the stack back down, which is exactly the work
        # this helper must not have created in the first place.
        plan = df._jdf.queryExecution().analyzed().numberedTreeString()
        return sum(1 for line in plan.splitlines() if "Project" in line)

    @staticmethod
    def _decimal_feature_frame(spark, n_features: int):
        schema = T.StructType(
            [T.StructField("cust_id", T.StringType())]
            + [
                T.StructField(f"feature_{i}", T.DecimalType(38, 6))
                for i in range(n_features)
            ]
        )
        return spark.createDataFrame(
            [("C001", *([Decimal("1.5")] * n_features))], schema=schema
        )

    def test_plan_depth_is_flat_in_the_number_of_cast_columns(self, spark):
        narrow = self._decimal_feature_frame(spark, 2)
        wide = self._decimal_feature_frame(spark, 16)

        narrow_out, narrow_casted = cast_numeric_features_to_storage_type(
            narrow, [c for c in narrow.columns if c != "cust_id"],
            "float32",
        )
        wide_out, wide_casted = cast_numeric_features_to_storage_type(
            wide, [c for c in wide.columns if c != "cust_id"],
            "float32",
        )

        # Guard the guard: if nothing were castable both counts would be equal
        # for the wrong reason.
        assert len(narrow_casted) == 2
        assert len(wide_casted) == 16

        # One Project for the whole cast, whatever the column count.
        assert self._analyzed_project_count(narrow_out) == 1
        assert self._analyzed_project_count(wide_out) == 1

    def test_the_withColumn_loop_this_replaced_does_grow_the_plan(self, spark):
        """Proves the assertion above can fail — one Project per cast column."""
        for n_features in (2, 16):
            df = self._decimal_feature_frame(spark, n_features)
            looped = df
            for col in (c for c in df.columns if c != "cust_id"):
                looped = looped.withColumn(col, F.col(col).cast("float"))
            assert self._analyzed_project_count(looped) == n_features


@pytest.mark.spark
def test_cast_leaves_a_dotted_passthrough_column_alone(spark):
    """A column name with a dot must survive a cast it was never part of.

    The ``withColumn`` loop never referenced the columns outside
    ``feature_cols``; the single ``select`` has to name every one of them, and
    ``F.col("a.b")`` means "field b of struct a" unless the name is backticked.
    Without the backticks this raises ``AnalysisException: Column 'a.b' does
    not exist. Did you mean one of the following? [a.b, ...]`` — the frame is
    telling you it has the column it just refused to find.

    Dotted names are not expected from the Hive tables this reads, which is
    exactly why nothing else would catch the regression.
    """
    schema = T.StructType([
        T.StructField("a.b", T.StringType()),
        T.StructField("feature_a", T.DecimalType(38, 6)),
    ])
    df = spark.createDataFrame([("x", Decimal("1.5"))], schema=schema)

    out, casted = cast_numeric_features_to_storage_type(df, ["feature_a"], "float32")

    assert casted == ["feature_a"]
    assert out.columns == ["a.b", "feature_a"]
    assert _dtype(out, "a.b") == "string"
    assert _dtype(out, "feature_a") == "float"
    assert out.collect()[0]["a.b"] == "x"


@pytest.mark.spark
class TestEncodeCategoricalsEmptyMapping:
    """D15 — the whole-column-unknown branch of ``encode_categoricals``.

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
        out = encode_categoricals(df, ["risk_attr"], {"risk_attr": []})
        assert [r.risk_attr for r in out.orderBy("cust_id").collect()] == [-1, -1, -1]

    def test_empty_mapping_output_is_integer_typed(self, spark):
        """The encoded column must be an int like every other encoded column.

        Compared against the type object, not ``dtypes``' string form: a
        substring or repr comparison is satisfied by whatever ``simpleString``
        happens to emit and would survive a change of width.
        """
        from pyspark.sql import types as T

        df = spark.createDataFrame(pd.DataFrame({"risk_attr": ["low", "high"]}))
        out = encode_categoricals(df, ["risk_attr"], {"risk_attr": []})
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
        out = encode_categoricals(df, ["risk_attr"], {"risk_attr": ["low", "high"]})
        # Index in the mapping list: low->0, high->1, anything else -> -1.
        assert [r.risk_attr for r in out.orderBy("cust_id").collect()] == [0, 1, -1]


from recsys_tfb.preprocessing import castable_numeric_feature_columns


class TestCastableNumericFeatureColumns:
    """The selector both the cast and the B8 gate read, pinned once.

    Sharing it is what makes "the gate checks exactly what the cast converts"
    a structural property rather than a promise: widening the cast widens the
    gate in the same edit. Takes a ``StructType`` rather than a DataFrame so the
    rule can be tested without a SparkSession (cold start is minutes here).
    """

    def _schema(self) -> T.StructType:
        return T.StructType([
            T.StructField("cust_id", T.StringType()),
            T.StructField("label", T.IntegerType()),
            T.StructField("dec", T.DecimalType(38, 10)),
            T.StructField("dbl", T.DoubleType()),
            T.StructField("flt", T.FloatType()),
            T.StructField("i32", T.IntegerType()),
            T.StructField("i64", T.LongType()),
            T.StructField("flag", T.BooleanType()),
            T.StructField("name", T.StringType()),
            T.StructField("non_feature_dec", T.DecimalType(20, 2)),
        ])

    def _feature_cols(self) -> list[str]:
        return ["dec", "dbl", "flt", "i32", "i64", "flag", "name"]

    def test_selects_every_numeric_feature_column(self):
        # Frame order, and ``name`` (string) excluded: the selector is a
        # whitelist of numeric types, not "everything but the identity".
        assert castable_numeric_feature_columns(
            self._schema(), self._feature_cols()) == [
                "dec", "dbl", "flt", "i32", "i64", "flag"]

    def test_a_column_outside_feature_cols_is_never_selected(self):
        # Identity, label and anything else the frame carries are not the
        # cast's business, and must not become the gate's either.
        assert "non_feature_dec" not in castable_numeric_feature_columns(
            self._schema(), self._feature_cols())

    def test_float32_is_selected_even_though_it_may_be_a_no_op(self):
        # #283 — the selector answers "which columns converge", not "which
        # columns shrink". Skipping FloatType would leave it float32 on a
        # float64 run, which is the one case convergence exists to prevent.
        assert "flt" in castable_numeric_feature_columns(
            self._schema(), self._feature_cols())

    def test_boolean_is_selected(self):
        # The type B6 admits and pandas refuses to mix with float: it reaches
        # the model as a number only because this selector names it. See
        # ``core.consistency.spark_dtype_is_numeric``.
        assert "flag" in castable_numeric_feature_columns(
            self._schema(), self._feature_cols())

    def test_a_non_numeric_feature_column_is_not_selected(self):
        assert "name" not in castable_numeric_feature_columns(
            self._schema(), self._feature_cols())

    def test_a_declared_feature_absent_from_the_frame_is_skipped(self):
        assert castable_numeric_feature_columns(
            self._schema(), ["dec", "not_here"]) == ["dec"]

    def test_frame_order_not_feature_cols_order(self):
        # The cast rebuilds columns in frame order; a selector that returned
        # them in the caller's order would make the two disagree the day this
        # feeds a positional select.
        assert castable_numeric_feature_columns(
            self._schema(), ["dbl", "dec"]) == ["dec", "dbl"]


@pytest.mark.spark
def test_cast_helper_and_selector_agree(mixed_df):
    """The cast converts exactly what the selector names — the property B8's
    scope rests on. Asserted against the real helper rather than assumed."""
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = cast_numeric_features_to_storage_type(mixed_df, feature_cols, "float32")
    assert casted == castable_numeric_feature_columns(
        mixed_df.schema, feature_cols)

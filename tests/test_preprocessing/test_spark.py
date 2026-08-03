"""Tests for preprocessing._spark private helpers."""

import pytest
from decimal import Decimal
from pyspark.sql import types as T

from recsys_tfb.preprocessing._spark import _cast_feature_floats_to_float32

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


def test_cast_feature_decimals_returns_casted_list(mixed_df):
    feature_cols = ["feature_a", "feature_b", "feature_c"]
    _, casted = _cast_feature_floats_to_float32(mixed_df, feature_cols)
    assert sorted(casted) == ["feature_a", "feature_c"]


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


def test_cast_feature_decimals_preserves_values(mixed_df):
    feature_cols = ["feature_a"]
    out, _ = _cast_feature_floats_to_float32(mixed_df, feature_cols)
    rows = out.orderBy("cust_id").collect()
    assert rows[0].feature_a == pytest.approx(1.5)
    assert rows[1].feature_a == pytest.approx(2.25)


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


import pandas as pd
from recsys_tfb.core.schema import get_schema
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
        # apply_start_date is here to be *dropped*: it is not identity, not a
        # feature, not the label, and not in keys, so the output rule excludes
        # it. Without such a column the expected set degenerates into "the union
        # of every input column" — which is what `dataset` already is, so the
        # equality below could not tell a correct selection apart from no
        # selection at all. (label_table really does carry these; see the
        # label_table fixture in test_nodes_spark.py.)
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0],
            "apply_start_date": pd.to_datetime(["2025-02-01"] * 2)}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        return keys, feats, labels

    def _expected_columns(self, keys):
        """identity ∪ {label} ∪ feature_columns ∪ whatever the keys carried in.

        Asserted as an equality rather than ``in`` / ``not in`` so an extra
        column — a join column leaking through, a carry column that should have
        been filtered out — fails too. Only has that power because the label
        frame carries a column the rule must drop; see ``_frames``.

        Takes ``feature_columns`` from the preprocessor, so it checks that
        build_model_input honours the metadata it was handed — NOT that the
        metadata itself is right. Whether _compute_feature_columns picked the
        correct columns is a separate contract with its own tests.
        """
        schema = get_schema(self._params())
        return (
            set(schema["identity_columns"])
            | {schema["label"]}
            | set(self._prep()["feature_columns"])
            | set(keys.columns)
        )

    def test_carry_in_output_when_present_in_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=True)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" in out.columns
        assert set(out.columns) == self._expected_columns(keys)
        assert out.count() == keys.count()

    def test_no_carry_when_absent_from_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=False)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" not in out.columns
        assert set(out.columns) == self._expected_columns(keys)
        assert out.count() == keys.count()


class TestBuildModelInputJoinContract:
    """D14/D13/D10/D11 — what the two LEFT joins promise (ADR-0005 §3).

    Both joins are LEFT on purpose, and the reasons differ:

    - **label_table is sparse.** Only entities with a transaction have a row, so
      a key with no label row is the normal case and means "negative". The
      ``coalesce(label, 0)`` is that decision.
    - **feature_table has a different upstream population than sample_pool.**
      A key whose ``(time, entity)`` is absent produces an all-NULL *feature*
      row that stays in model_input; LightGBM handles the missing values. This
      is a legal output, not a bug — which is why the assertion is the contract
      itself rather than the fail-loud "no NULL features" the audit first
      proposed.

    Both are pinned by row count, so turning either join INNER turns this red —
    the mutation #140 names as currently passing 115 tests.

    The frame is built so the four miss/hit combinations are all present and
    separable. ``cust_id=9`` is absent from the *feature* frame but still has a
    label row on product "a" — that pairing is the point: it is what lets
    ``test_label_join_miss_becomes_a_negative`` show a feature miss does not
    cost a row its real label, which "fill 0 everywhere" would also satisfy if
    every feature-missing row happened to be label-missing too.
    """

    PREP = {
        "feature_columns": ["prod_name", "f1"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["a", "b"]},
        "drop_columns": [],
    }
    PARAMS = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"],
        "item": "prod_name", "label": "label"}}}

    @pytest.fixture
    def built(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            # cust 1 is in feature_table, cust 9 is not.
            "cust_id": [1, 1, 9, 9],
            "prod_name": ["a", "b", "a", "b"],
        }))
        # Labels exist only for the "a" rows: the "b" rows are label-join misses.
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 9], "prod_name": ["a", "a"], "label": [1, 1],
        }))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"]),
            "cust_id": [1], "f1": [0.5],
        }))
        out = build_model_input(keys, feats, labels, self.PREP, self.PARAMS)
        return keys, out.orderBy("cust_id", "prod_name").toPandas()

    def test_row_count_equals_keys_regardless_of_either_miss(self, built):
        """D14 — keys' grain is the output's grain even when both joins miss.

        This is the single assertion that turns red for INNER on either side:
        INNER on features drops the cust 9 rows, INNER on labels drops the "b"
        rows.
        """
        keys, pdf = built
        assert len(pdf) == keys.count() == 4

    def test_feature_join_miss_yields_null_features_not_a_dropped_row(self, built):
        """D14 — the all-NULL feature row is the contract, not a defect.

        Asserted only on the feature_table-sourced column: ``prod_name`` is also
        in ``feature_columns`` but arrives from *keys*, so "every feature column
        is NULL" would be false here and would misstate the contract.
        """
        _, pdf = built
        missing = pdf[pdf["cust_id"] == 9]
        assert len(missing) == 2
        assert missing["f1"].isna().all()
        # The identity-sourced feature is untouched by the miss.
        assert missing["prod_name"].tolist() == ["a", "b"]
        # ...and the entity that *is* present did get its feature, so this is
        # evidence about the join rather than about f1 being NULL everywhere.
        assert pdf[pdf["cust_id"] == 1]["f1"].notna().all()

    def test_label_join_miss_becomes_a_negative(self, built):
        """D13 — "sparse label_table, a miss is a negative" is a contract.

        Both halves matter: the misses become 0 *and* the hits keep their 1. An
        implementation that dropped the label join entirely and filled 0
        everywhere would satisfy the first half alone.
        """
        _, pdf = built
        by_key = {(r.cust_id, r.prod_name): r.label for r in pdf.itertuples()}
        assert by_key[(1, "b")] == 0
        assert by_key[(9, "b")] == 0
        assert by_key[(1, "a")] == 1
        # A feature-join miss must not cost the row its real label.
        assert by_key[(9, "a")] == 1

    def test_label_is_never_null(self, built):
        """D10 — training must not see a NULL label; coalesce is what stops it."""
        _, pdf = built
        assert pdf["label"].notna().all()

    def test_label_domain_is_zero_or_one(self, built):
        """D11 — a binary label column, asserted as a set so a stray 2 fails."""
        _, pdf = built
        assert set(pdf["label"].unique()) <= {0, 1}


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
        from recsys_tfb.preprocessing._spark import _encode_categoricals

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

        from recsys_tfb.preprocessing._spark import _encode_categoricals

        df = spark.createDataFrame(pd.DataFrame({"risk_attr": ["low", "high"]}))
        out = _encode_categoricals(df, ["risk_attr"], {"risk_attr": []})
        assert out.schema["risk_attr"].dataType == T.IntegerType()

    def test_populated_mapping_still_encodes_by_position(self, spark):
        """The paired half: with values present the column is *not* all -1.

        Without this, "everything is -1" would also pass for an implementation
        that ignored ``category_mappings`` altogether.
        """
        from recsys_tfb.preprocessing._spark import _encode_categoricals

        df = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["c1", "c2", "c3"],
            "risk_attr": ["low", "high", "unseen"],
        }))
        out = _encode_categoricals(df, ["risk_attr"], {"risk_attr": ["low", "high"]})
        # Index in the mapping list: low->0, high->1, anything else -> -1.
        assert [r.risk_attr for r in out.orderBy("cust_id").collect()] == [0, 1, -1]


class TestComputeFeatureColumnsDrops:
    """``drop_columns`` keeps a feature_table column out of ``feature_columns``.

    Needs no Spark: this is the pure derivation that decides the blacklist, and
    it is the step ``apply_preprocessor_to_features`` then uses to physically
    build ``keep_cols`` — so a column excluded here never reaches
    ``preprocessed_feature_table`` at all (ADR-0004).

    Every column named in ``drop`` below is present in ``FT_COLS``. The previous
    coverage for this behaviour asserted that three columns were absent from an
    output when the input frame never contained them either, which held whether
    or not drop_columns did anything.
    """

    FT_COLS = [
        "snap_date", "cust_id", "cust_segment_typ", "total_aum", "tenure_months", "label",
    ]
    IDENTITY = ["snap_date", "cust_id", "prod_name"]

    def _compute(self, drop):
        from recsys_tfb.preprocessing._spark import _compute_feature_columns

        return _compute_feature_columns(
            self.FT_COLS, self.IDENTITY, ["prod_name"], drop, "label"
        )

    def test_a_real_feature_table_column_is_dropped(self):
        assert "cust_segment_typ" in self.FT_COLS  # the column really is there
        # Equality, not `not in`: it also pins down that dropping one column
        # left the others alone.
        assert self._compute(["snap_date", "cust_id", "label", "cust_segment_typ"]) == [
            "prod_name", "total_aum", "tenure_months",
        ]

    def test_the_same_column_survives_when_not_dropped(self):
        # The paired half: without the drop entry the column is a feature. The
        # two together are what make the first one evidence about drop_columns
        # rather than about some other exclusion rule.
        assert self._compute(["snap_date", "cust_id", "label"]) == [
            "prod_name", "cust_segment_typ", "total_aum", "tenure_months",
        ]

    def test_dropping_several_columns_removes_all_of_them(self):
        assert self._compute(
            ["snap_date", "cust_id", "label", "cust_segment_typ", "tenure_months"]
        ) == ["prod_name", "total_aum"]

    def test_label_is_excluded_even_when_drop_columns_omits_it(self):
        # The label has its own exclusion rule, separate from the blacklist.
        # Every other case here lists "label" in drop, which would keep passing
        # if that rule were deleted — the blacklist alone already covers it.
        assert "label" in self.FT_COLS
        assert self._compute(["snap_date", "cust_id"]) == [
            "prod_name", "cust_segment_typ", "total_aum", "tenure_months",
        ]


class TestApplyPreprocessorToFeaturesRequiresSnapDates:
    """``snap_dates`` is required — the caller owns "which months is this run for".

    It used to default to ``None`` and fall back to
    ``collect_dataset_snap_dates(parameters)`` (the union of every configured
    month). Since the month plans moved into the catalog (#152) the node always
    passes ``month_plan.to_process``, so that fallback was unreachable. Pinning
    the argument as required is what keeps it unreachable: a future caller that
    forgets it now fails at the call, instead of silently re-encoding every
    configured month and quietly undoing the incremental behaviour of ADR-0002.
    """

    def test_omitting_snap_dates_raises_instead_of_defaulting(self):
        from recsys_tfb.preprocessing._spark import apply_preprocessor_to_features

        # Binding fails before the body runs, so the placeholder arguments are
        # never dereferenced. Were the default restored, this call would get
        # past binding and die on the placeholders instead (KeyError), so this
        # assertion is about the signature and not about the placeholders.
        with pytest.raises(TypeError, match="snap_dates"):
            apply_preprocessor_to_features(None, {}, {})

"""Tests for the Layer-2 data gate (B1 + B5 + B6 + B7).

Scope split: this module drives the gate end-to-end against real Spark frames —
does it fire on a violation, stay quiet on a lookalike, and name the right
column. The pure predicates it delegates to are tested in
tests/test_core/test_consistency.py, and the pure helper it uses to derive the
prospective feature set (``_compute_feature_columns``) in
tests/test_preprocessing/test_spark.py. Split by behaviour, so each one has
exactly one home.

Fixtures come from ``conftest.py``.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.pipelines.dataset.nodes_data_gate import validate_data_consistency
from tests.test_pipelines.test_dataset.fixture_shape import (
    _explicit_preprocessing_params,
)

pytestmark = pytest.mark.spark


class TestValidateDataConsistency:
    def test_clean_fixture_is_not_flagged_by_any_gate(
        self, sample_pool, label_table, feature_table, parameters
    ):
        """The gate must not misfire on a fixture that violates nothing.

        Covers B1/B5/B6 — it replaces three verbatim-identical ``is None``
        assertions that were filed under three different invariants and so
        promised a discrimination none of them had. Each invariant's raise side
        is covered by its own test below; this one only says "no false alarm".

        **Not** a B7 guard: the fixture configures no ``carry_columns``, so B7's
        input here is empty and unflagging it proves nothing. B7's false-positive
        side is covered by
        ``TestValidateDataConsistencyB7.test_carry_column_absent_from_feature_table_is_not_flagged``.

        Fixture properties it rests on: prod_name values ==
        schema.categorical_values.prod_name and every snap inside the configured
        windows (B1); feature_table's feature columns are all numeric and the
        lone declared categorical (prod_name) is an identity column absent from
        feature_table (B5/B6).
        """
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, parameters) is None

    def test_undeclared_value_raises(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # Shrink declared set so fund_stock (present in data) is undeclared.
        params = {
            **parameters,
            "schema": {
                **parameters["schema"],
                "categorical_values": {"prod_name": ["exchange_fx", "exchange_usd"]},
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "fund_stock" in msg
        assert "sample_pool" in msg

    def test_declared_value_absent_from_sample_pool_raises(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # 'ploan' is declared but never appears in sample_pool/label data ->
        # sp_missing direction (D3 second direction). declared-label is B3,
        # deferred, so the only error is the sample_pool "never produces" one.
        params = {
            **parameters,
            "schema": {
                **parameters["schema"],
                "categorical_values": {
                    "prod_name": [
                        "exchange_fx", "exchange_usd", "fund_stock", "ploan",
                    ]
                },
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "ploan" in msg
        assert "never produces" in msg

    def test_value_only_in_non_window_snap_is_ignored(
        self, spark, sample_pool, label_table, feature_table, parameters
    ):
        # 2024-12-31 is outside collect_dataset_snap_dates (train Jan-Mar,
        # val Apr, test May). An undeclared 'ploan' there must be filtered out.
        extra = spark.createDataFrame(
            pd.DataFrame([{
                "snap_date": pd.Timestamp("2024-12-31"),
                "cust_id": "C001",
                "cust_segment_typ": "mass",
                "prod_name": "ploan",
                "label": 0,
                "tenure_months": 12,
                "channel_preference": "digital",
            }])
        )
        sp = sample_pool.unionByName(extra)
        assert validate_data_consistency(sp, label_table, feature_table, parameters) is None

    def test_decimal_categorical_in_feature_table_raises(
        self, spark, sample_pool, label_table, parameters
    ):
        # B5: a column declared in categorical_columns is DecimalType in
        # feature_table -> fail fast at the gate (instead of the opaque
        # JSON-serialization crash 141s into fit_preprocessor_metadata).
        from decimal import Decimal

        from pyspark.sql import types as T

        ft_schema = T.StructType([
            T.StructField("snap_date", T.TimestampType()),
            T.StructField("cust_id", T.StringType()),
            T.StructField("industry_code", T.DecimalType(15, 0)),
        ])
        feature_table = spark.createDataFrame(
            [(pd.Timestamp("2024-01-31").to_pydatetime(), "C001", Decimal("1001"))],
            schema=ft_schema,
        )
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "prepare_model_input": {
                    "categorical_columns": ["prod_name", "industry_code"],
                },
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "industry_code" in msg
        assert "decimal" in msg


class TestValidateDataConsistencyB6:
    def test_unencoded_string_feature_raises(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 注入一個未宣告 categorical、也未 drop 的字串特徵欄
        rogue = feature_table.withColumn("rogue_str", F.lit("free_text"))
        with pytest.raises(DataConsistencyError, match="rogue_str"):
            validate_data_consistency(sample_pool, label_table, rogue, parameters)

    def test_boolean_feature_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 布林特徵欄是數值（bool→numeric），B6 不得誤報
        with_bool = feature_table.withColumn("flag_bool", F.lit(True))
        assert (
            validate_data_consistency(sample_pool, label_table, with_bool, parameters)
            is None
        )

    def test_declared_categorical_string_feature_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """B6 must be told which columns get encoded downstream.

        A string feature column that *is* declared categorical becomes an
        integer at encode time, so it is not the object-dtype footgun B6 guards
        — the gate has to hand the declared set to the predicate for that to
        hold. The rogue column is here so the assertion is "raised, and named
        only the rogue one" rather than "did not raise": a silently dead gate
        fails this test instead of passing it.
        """
        ft = (
            feature_table
            .withColumn("channel_preference", F.lit("digital"))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _explicit_preprocessing_params(parameters, categorical_extra=["channel_preference"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "rogue_str" in msg
        assert "channel_preference" not in msg

    def test_dropped_string_column_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """B6 looks at *prospective feature* columns, not raw feature_table ones.

        A dropped column never reaches the model, so flagging it would be a
        false alarm that no valid config could clear — the gate has to classify
        the ``_compute_feature_columns`` output. Same rogue-column anchor as
        above so a dead gate cannot pass.
        """
        ft = (
            feature_table
            .withColumn("legacy_note", F.lit("free_text"))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _explicit_preprocessing_params(parameters, drop_extra=["legacy_note"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "rogue_str" in msg
        assert "legacy_note" not in msg


class TestValidateDataConsistencyB7:
    """B7 — a carry column that also lives in feature_table must be dropped."""

    def test_undropped_carry_column_raises(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """Numeric on purpose: this is the case only B7 catches.

        B6 already rejects a *string* carry column that was left out of
        drop_columns (as an un-encoded object feature), so a string here would
        not prove B7 is wired — the raise could come from B6. A numeric one
        sails past B6 straight into ``Reference 'x' is ambiguous`` at
        build_model_input, which is the crash B7 exists to pre-empt.
        ``acct_age_months`` is the negative control: same collision, correctly
        dropped, must not be named.
        """
        ft = (
            feature_table
            .withColumn("tenure_months", F.lit(12))
            .withColumn("acct_age_months", F.lit(24))
        )
        params = _explicit_preprocessing_params(
            parameters,
            drop_extra=["acct_age_months"],
            carry=["tenure_months", "acct_age_months"],
        )
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "1 issue(s)" in msg
        assert "tenure_months" in msg
        # "carry_columns" appears in no other message this gate can emit, so it
        # pins the raise to B7 rather than to whichever rule fires first.
        assert "carry_columns" in msg
        assert "acct_age_months" not in msg

    def test_carry_column_absent_from_feature_table_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """The ordinary case: carry columns usually live only in sample_pool.

        ``channel_preference`` is a sample_pool column and not a feature_table
        one, so there is nothing to be ambiguous with. It is deliberately not
        ``cust_segment_typ`` — that one is in the default drop_columns, so an
        unflagged result there would be explained by the drop just as well as by
        the absence, and removing the feature_table check would not turn it red.
        """
        assert "channel_preference" not in feature_table.columns
        params = _explicit_preprocessing_params(parameters, carry=["channel_preference"])
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, params) is None

    def test_identity_column_in_carry_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """The gate has to tell B7 which columns are identity.

        ``cust_id`` is in feature_table and not in drop_columns, so the naive
        rule flags it — but ``select_keys`` never copies an identity column a
        second time, and running the real ``build_model_input`` this way
        completes normally. Flagging it would demand a drop_columns edit that
        changes no behaviour while busting base_dataset_version.
        """
        assert "cust_id" in feature_table.columns
        params = _explicit_preprocessing_params(parameters, carry=["cust_id"])
        params["dataset"]["prepare_model_input"]["drop_columns"] = [
            c for c in params["dataset"]["prepare_model_input"]["drop_columns"]
            if c != "cust_id"
        ]
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, params) is None


class TestValidateDataConsistencyCollectAll:
    def test_two_unrelated_violations_raise_once_naming_both(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """One raise listing every violation, so one fix pass clears them all.

        Two different invariants on purpose (B7 numeric collision + B6 rogue
        string): a gate that stopped at the first non-empty error list would
        still report one of them, and the count in the header is what separates
        that from reporting both.
        """
        ft = (
            feature_table
            .withColumn("tenure_months", F.lit(12))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _explicit_preprocessing_params(parameters, carry=["tenure_months"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "2 issue(s)" in msg
        assert "tenure_months" in msg
        assert "rogue_str" in msg

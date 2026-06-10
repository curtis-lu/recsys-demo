"""Tests for dataset building pipeline Spark nodes."""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.pipelines.dataset.nodes_spark import (
    apply_preprocessor_to_features,
    build_model_input,
    fit_preprocessor_metadata,
    select_test_keys,
    select_train_keys,
    select_val_keys,
    split_train_keys,
    validate_data_consistency,
)

pytestmark = pytest.mark.spark


@pytest.fixture
def feature_table(spark):
    pdf = pd.DataFrame(
        {
            "snap_date": pd.to_datetime(
                ["2024-01-31"] * 4
                + ["2024-02-29"] * 4
                + ["2024-03-31"] * 4
                + ["2024-04-30"] * 4
                + ["2024-05-31"] * 4
            ),
            "cust_id": ["C001", "C002", "C003", "C004"] * 5,
            "total_aum": [100.0, 200.0, 300.0, 400.0] * 5,
            "fund_aum": [10.0, 20.0, 30.0, 40.0] * 5,
            "in_amt_sum_l1m": [5.0] * 20,
            "out_amt_sum_l1m": [3.0] * 20,
            "in_amt_ratio_l1m": [0.05] * 20,
            "out_amt_ratio_l1m": [0.03] * 20,
        }
    )
    return spark.createDataFrame(pdf)


@pytest.fixture
def label_table(spark):
    products = ["exchange_fx", "exchange_usd", "fund_stock"]
    segments = {"C001": "mass", "C002": "affluent", "C003": "hnw", "C004": "mass"}
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004"]:
            for prod in products:
                rows.append(
                    {
                        "snap_date": snap_dt,
                        "cust_id": cid,
                        "cust_segment_typ": segments[cid],
                        "apply_start_date": snap_dt + pd.Timedelta(days=1),
                        "apply_end_date": snap_dt + pd.Timedelta(days=30),
                        "label": 1 if cid == "C001" and prod == "exchange_fx" else 0,
                        "prod_name": prod,
                    }
                )
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def sample_pool(spark):
    products = ["exchange_fx", "exchange_usd", "fund_stock"]
    segments = {"C001": "mass", "C002": "affluent", "C003": "hnw", "C004": "mass"}
    tenure = {"C001": 12, "C002": 36, "C003": 60, "C004": 24}
    channel = {"C001": "digital", "C002": "branch", "C003": "both", "C004": "digital"}
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004"]:
            for prod in products:
                rows.append({
                    "snap_date": snap_dt,
                    "cust_id": cid,
                    "cust_segment_typ": segments[cid],
                    "prod_name": prod,
                    "label": 1 if cid == "C001" and prod == "exchange_fx" else 0,
                    "tenure_months": tenure[cid],
                    "channel_preference": channel[cid],
                })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "schema": {
            "categorical_values": {
                "prod_name": ["exchange_fx", "exchange_usd", "fund_stock"],
            },
        },
        "dataset": {
            "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-03-31"],
            "sample_ratio": 0.5,
            "sample_group_keys": ["cust_segment_typ", "prod_name"],
            "sample_ratio_overrides": {},
            "train_dev_ratio": 0.2,
            "enable_calibration": False,
            "calibration_snap_dates": [],
            "calibration_sample_ratio": 1.0,
            "val_snap_dates": ["2024-04-30"],
            "val_sample_ratio": 1.0,
            "test_snap_dates": ["2024-05-31"],
        },
    }


class TestSelectTrainKeys:
    def test_returns_correct_columns(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        assert sorted(result.columns) == ["cust_id", "prod_name", "snap_date"]

    def test_filters_to_train_dates(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        pdf = result.toPandas()
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))
        excluded = val_dates | test_dates
        assert not pdf["snap_date"].isin(excluded).any()
        # All dates must be in train_snap_dates
        assert pdf["snap_date"].isin(train_dates).all()

    def test_full_ratio_returns_all(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        result = select_train_keys(sample_pool, params)
        # 4 customers x 3 train dates x 3 products = 36
        assert result.count() == 36

    def test_no_duplicates(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        assert result.count() == result.dropDuplicates(["snap_date", "cust_id", "prod_name"]).count()


class TestSplitTrainKeys:
    def test_no_cust_overlap(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        train_custs = set(train.select("cust_id").distinct().toPandas()["cust_id"])
        dev_custs = set(train_dev.select("cust_id").distinct().toPandas()["cust_id"])
        assert len(train_custs & dev_custs) == 0

    def test_all_keys_preserved(self, sample_pool, parameters):
        """train_keys ∪ train_dev_keys must be exactly sample_keys, even after
        repartition (which is the production scenario that broke F.rand-based splitting)."""
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params).repartition(8)
        train, train_dev = split_train_keys(sample_keys, params)

        identity = ["snap_date", "cust_id", "prod_name"]
        sample_set = {tuple(r) for r in sample_keys.select(*identity).toPandas().itertuples(index=False)}
        train_set = {tuple(r) for r in train.select(*identity).toPandas().itertuples(index=False)}
        dev_set = {tuple(r) for r in train_dev.select(*identity).toPandas().itertuples(index=False)}

        assert train_set | dev_set == sample_set
        assert train_set & dev_set == set()

    def test_deterministic_across_runs(self, sample_pool, parameters):
        """Two independent invocations with the same seed must yield identical splits."""
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)

        t1, d1 = split_train_keys(sample_keys, params)
        t2, d2 = split_train_keys(sample_keys, params)

        t1_custs = set(t1.select("cust_id").distinct().toPandas()["cust_id"])
        t2_custs = set(t2.select("cust_id").distinct().toPandas()["cust_id"])
        d1_custs = set(d1.select("cust_id").distinct().toPandas()["cust_id"])
        d2_custs = set(d2.select("cust_id").distinct().toPandas()["cust_id"])
        assert t1_custs == t2_custs
        assert d1_custs == d2_custs


class TestSelectValKeys:
    def test_full_population(self, label_table, parameters):
        result = select_val_keys(label_table, parameters)
        # identity_columns includes prod_name → 4 cust × 3 prod for val date
        assert result.count() == 12
        assert sorted(result.columns) == ["cust_id", "prod_name", "snap_date"]


class TestSelectTestKeys:
    def test_full_population(self, label_table, parameters):
        result = select_test_keys(label_table, parameters)
        # identity_columns includes prod_name → 4 cust × 3 prod for test date
        assert result.count() == 12
        assert sorted(result.columns) == ["cust_id", "prod_name", "snap_date"]


class TestBuildModelInput:
    def _train_keys(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        return select_train_keys(sample_pool, params)

    def _pft(self, feature_table, sample_pool, parameters):
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        return train_keys, preprocessor, pft

    def test_joins_with_product_keys(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """When keys include prod_name, join label_table on full identity key."""
        _, preprocessor, pft = self._pft(feature_table, sample_pool, parameters)

        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
                "prod_name": ["exchange_fx", "exchange_fx"],
            })
        )
        result = build_model_input(keys, pft, label_table, preprocessor, parameters)
        assert result.count() == 2
        assert "total_aum" in result.columns
        assert "label" in result.columns

    def test_joins_without_product_keys(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """When keys don't include prod_name, expand to all products."""
        _, preprocessor, pft = self._pft(feature_table, sample_pool, parameters)

        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
            })
        )
        result = build_model_input(keys, pft, label_table, preprocessor, parameters)
        assert result.count() == 6
        assert "total_aum" in result.columns
        assert "label" in result.columns
        assert "prod_name" in result.columns


class TestFitAndBuild:
    def _train_keys(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        return select_train_keys(sample_pool, params)

    def _label_only_keys(self, spark, label_table, snap):
        from pyspark.sql import functions as F

        return (
            label_table.filter(F.col("snap_date") == pd.Timestamp(snap))
            .select("snap_date", "cust_id")
            .dropDuplicates()
        )

    def test_output_format(self, spark, feature_table, label_table, sample_pool, parameters):
        from pyspark.sql import DataFrame

        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, cat_mappings = fit_preprocessor_metadata(
            feature_table, parameters
        )
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)

        train_mi = build_model_input(train_keys, pft, label_table, preprocessor, parameters)
        val_keys = self._label_only_keys(spark, label_table, "2024-04-30")
        test_keys = self._label_only_keys(spark, label_table, "2024-05-31")
        val_mi = build_model_input(val_keys, pft, label_table, preprocessor, parameters)
        test_mi = build_model_input(test_keys, pft, label_table, preprocessor, parameters)

        assert isinstance(train_mi, DataFrame)
        assert "label" in train_mi.columns
        assert train_mi.count() > 0
        assert val_mi.count() > 0
        assert test_mi.count() > 0

    def test_excludes_drop_columns(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        train_mi = build_model_input(train_keys, pft, label_table, preprocessor, parameters)

        forbidden = {"apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(train_mi.columns))

    def test_prod_name_preserved_as_identity(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """prod_name is an identity column — encoding is deferred to training."""
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        train_mi = build_model_input(train_keys, pft, label_table, preprocessor, parameters)
        train_pdf = train_mi.toPandas()

        assert train_pdf["prod_name"].dtype == object


class TestFitPreprocessorMissingDates:
    def test_missing_train_snap_date_raises(self, spark, feature_table, parameters):
        # parameters has train_snap_dates including 2024-02-29; feature_table has it.
        # Override to require a date that's not in feature_table.
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-12-31"],
            },
        }
        with pytest.raises(ValueError, match="missing required train_snap_dates"):
            fit_preprocessor_metadata(feature_table, params)

    def test_error_lists_missing_dates(self, spark, feature_table, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-12-31", "2024-11-30"],
            },
        }
        with pytest.raises(ValueError) as exc_info:
            fit_preprocessor_metadata(feature_table, params)
        msg = str(exc_info.value)
        assert "2024-11-30" in msg
        assert "2024-12-31" in msg


class TestFitPreprocessorItemMissingFromFeatures:
    """schema.item must appear in feature_columns; otherwise X loses the item dimension and HPO mAP collapses to a constant."""

    def test_categorical_columns_missing_item_raises(
        self, spark, feature_table, parameters
    ):
        # Simulate the real-world yaml mistake: categorical_columns lists other columns but omits prod_name.
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "prepare_model_input": {
                    "drop_columns": [
                        "snap_date", "cust_id", "label",
                        "apply_start_date", "apply_end_date", "cust_segment_typ",
                    ],
                    "categorical_columns": [],  # intentionally omits prod_name (and other cats, to bypass existing checks)
                },
            },
        }
        with pytest.raises(DataConsistencyError, match="schema.item='prod_name' is missing") as ei:
            fit_preprocessor_metadata(feature_table, params)
        assert isinstance(ei.value, ValueError)  # subclass: existing callers unaffected

    def test_default_categorical_columns_passes(
        self, spark, feature_table, parameters
    ):
        # When prepare_model_input is not supplied, _get_preprocessing_config
        # defaults categorical_columns=[schema.item], so prod_name lands in
        # feature_columns automatically.
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        assert "prod_name" in preprocessor["feature_columns"]


class TestApplyPreprocessorFilter:
    def test_filters_out_dates_outside_dataset_set(
        self, spark, feature_table, parameters
    ):
        """Test A: when feature_table contains mid-week rows, the filter must drop them."""
        # Add a "mid-week" row that isn't in any split's snap_dates
        midweek_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-15"] * 4),
            "cust_id": ["C001", "C002", "C003", "C004"],
            "total_aum": [999.0] * 4,
            "fund_aum": [99.0] * 4,
            "in_amt_sum_l1m": [9.0] * 4,
            "out_amt_sum_l1m": [9.0] * 4,
            "in_amt_ratio_l1m": [0.99] * 4,
            "out_amt_ratio_l1m": [0.99] * 4,
        })
        ft_with_midweek = feature_table.unionByName(spark.createDataFrame(midweek_pdf))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(ft_with_midweek, preprocessor, parameters)
        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        assert pd.Timestamp("2024-01-15") not in result_dates

    def test_missing_required_snap_date_raises(
        self, spark, feature_table, parameters
    ):
        """Test E2: feature_table missing any cal/val/test snap_date must raise."""
        # parameters val_snap_dates is 2024-04-30; remove 04-30 rows from feature_table
        ft_short = feature_table.filter(F.col("snap_date") != F.lit(pd.Timestamp("2024-04-30")))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        with pytest.raises(ValueError, match="missing required snap_dates"):
            apply_preprocessor_to_features(ft_short, preprocessor, parameters)


class TestFitApplyFilterScopes:
    def test_apply_includes_all_splits_fit_only_train(
        self, spark, feature_table, parameters
    ):
        """Test B: fit sees train; apply sees train ∪ cal ∪ val ∪ test."""
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(feature_table, preprocessor, parameters)

        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))

        # Apply must cover all splits
        assert train_dates.issubset(result_dates)
        assert val_dates.issubset(result_dates)
        assert test_dates.issubset(result_dates)


def test_build_model_input_casts_float_features_to_float32(
    spark, label_table, parameters
):
    """Decimal AND Double feature columns must both be cast to float (float32)
    inside build_model_input.

    Invariant: model_input's numeric feature columns are stored as float32.

    LightGBM is histogram-based (max_bin=256) — float32's ~7-digit precision
    is far beyond the 8-bit binning resolution, so decimal128 and float64 are
    pure waste. decimal128 is the disaster case (~10x peak-memory blow-up
    via Python decimal.Decimal objects); float64 is the silent 2x case.
    We bake the smaller representation into model_input at write time.
    """
    from decimal import Decimal

    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("snap_date", T.TimestampType()),
        T.StructField("cust_id", T.StringType()),
        T.StructField("total_aum", T.DecimalType(38, 6)),
        T.StructField("fund_aum", T.DoubleType()),
        T.StructField("in_amt_sum_l1m", T.DecimalType(29, 0)),
        T.StructField("out_amt_sum_l1m", T.DoubleType()),
        T.StructField("in_amt_ratio_l1m", T.DoubleType()),
        T.StructField("out_amt_ratio_l1m", T.DoubleType()),
    ])
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]:
        snap_ts = pd.Timestamp(snap).to_pydatetime()
        for cid, aum in [
            ("C001", "100.0"),
            ("C002", "200.0"),
            ("C003", "300.0"),
            ("C004", "400.0"),
        ]:
            rows.append((
                snap_ts, cid, Decimal(aum), 10.0, Decimal("5"),
                3.0, 0.05, 0.03,
            ))
    feature_table_decimal = spark.createDataFrame(rows, schema=schema)

    preprocessor, _ = fit_preprocessor_metadata(feature_table_decimal, parameters)
    pft = apply_preprocessor_to_features(feature_table_decimal, preprocessor, parameters)

    # Build train keys directly (cust × prod × train_snap_dates) — independent of
    # sample_pool fixture; the test is about dtype, not sampling.
    train_keys = spark.createDataFrame(
        pd.DataFrame({
            "snap_date": pd.to_datetime(
                ["2024-01-31"] * 4 + ["2024-02-29"] * 4 + ["2024-03-31"] * 4
            ),
            "cust_id": ["C001", "C002", "C003", "C004"] * 3,
            "prod_name": ["exchange_fx"] * 12,
        })
    )

    result = build_model_input(train_keys, pft, label_table, preprocessor, parameters)

    feature_cols = preprocessor["feature_columns"]
    out_dtypes = dict(result.dtypes)

    # No decimal nor double feature columns remain — every float-like feature
    # is float32.
    leftover = [
        c for c in feature_cols
        if "decimal" in out_dtypes[c] or out_dtypes[c] == "double"
    ]
    assert leftover == [], (
        f"feature_columns still contain decimal/double types: {leftover}"
    )

    # DecimalType inputs are now float.
    assert out_dtypes["total_aum"] == "float"
    assert out_dtypes["in_amt_sum_l1m"] == "float"
    # DoubleType inputs are also now float (this PR's addition).
    assert out_dtypes["fund_aum"] == "float"
    assert out_dtypes["out_amt_sum_l1m"] == "float"
    assert out_dtypes["in_amt_ratio_l1m"] == "float"
    assert out_dtypes["out_amt_ratio_l1m"] == "float"


class TestValidateDataConsistency:
    def test_consistent_fixtures_return_none(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # fixtures: prod_name in {exchange_fx,exchange_usd,fund_stock} ==
        # schema.categorical_values.prod_name; all snaps inside windows.
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

    def test_clean_feature_table_categoricals_pass(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # feature_table fixture has only numeric (non-categorical) columns and
        # the lone declared categorical (prod_name) is an identity column absent
        # from feature_table -> B5 finds nothing, B1 is clean -> None.
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, parameters) is None


class TestSplitTrainKeysCarry:
    def test_carry_column_survives_split(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6], "prod_name": ["a"] * 6,
            "cust_segment_typ": ["mass", "hnw", "mass", "aff", "mass", "hnw"]}))
        params = {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
            "dataset": {"train_dev_ratio": 0.3}, "random_seed": 42}
        tr, dv = split_train_keys(keys, params)
        assert "cust_segment_typ" in tr.columns
        assert "cust_segment_typ" in dv.columns


class TestApplyPreprocessorUnknownWarning:
    """apply_preprocessor_to_features 對每個含 unknown(-1) 的編碼欄各發一次 WARNING。
    鎖住此可觀察行為，確保 multi-count -> single-aggregation 的重構等價。"""

    def test_warns_per_column_with_unknown_categoricals(self, spark, caplog):
        import logging

        # feature_table 帶一個「非 identity」的類別特徵欄，資料含 fit 時沒見過的值
        # -> 編碼為 -1。identity_columns = [snap_date, cust_id, prod_name]，故
        # channel_preference 會進 encode_cols。
        ft = spark.createDataFrame(
            pd.DataFrame(
                {
                    "snap_date": pd.to_datetime(["2024-01-31"] * 3),
                    "cust_id": ["C001", "C002", "C003"],
                    "total_aum": [100.0, 200.0, 300.0],
                    "channel_preference": ["digital", "branch", "unseen_channel"],
                }
            )
        )
        preprocessor = {
            "feature_columns": ["channel_preference", "total_aum"],
            "categorical_columns": ["channel_preference"],
            # "unseen_channel" 刻意不在 mapping -> 編碼為 -1
            "category_mappings": {"channel_preference": ["digital", "branch"]},
            "drop_columns": [],
        }
        parameters = {
            "schema": {"categorical_values": {"prod_name": ["exchange_fx"]}},
            "dataset": {
                "train_snap_dates": ["2024-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": [],
                "test_snap_dates": [],
            },
        }

        with caplog.at_level(logging.WARNING):
            result = apply_preprocessor_to_features(ft, preprocessor, parameters)
            pdf = result.orderBy("cust_id").toPandas()

        # digital->0, branch->1, unseen_channel->-1
        assert pdf["channel_preference"].tolist() == [0, 1, -1]
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "1 unknowns in column 'channel_preference'" in m for m in warnings
        ), warnings

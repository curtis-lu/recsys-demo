"""Tests for dataset building pipeline Spark nodes."""

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_spark import (
    apply_preprocessor_to_features,
    build_model_input,
    fit_preprocessor_metadata,
    select_test_keys,
    select_train_keys,
    select_val_keys,
    split_train_keys,
)


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

    def test_cross_backend_consistency(self, sample_pool, parameters):
        """Spark and pandas backends must produce identical cust_id assignments."""
        from recsys_tfb.pipelines.dataset.nodes_pandas import (
            split_train_keys as split_pandas,
        )

        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys_spark = select_train_keys(sample_pool, params)
        sample_keys_pandas = sample_keys_spark.toPandas()

        t_spark, d_spark = split_train_keys(sample_keys_spark, params)
        t_pandas, d_pandas = split_pandas(sample_keys_pandas, params)

        spark_dev = set(d_spark.select("cust_id").distinct().toPandas()["cust_id"])
        pandas_dev = set(d_pandas["cust_id"])
        assert spark_dev == pandas_dev


class TestSelectValKeys:
    def test_full_population(self, label_table, parameters):
        result = select_val_keys(label_table, parameters)
        # identity_columns 含 prod_name → 4 cust × 3 prod for val date
        assert result.count() == 12
        assert sorted(result.columns) == ["cust_id", "prod_name", "snap_date"]


class TestSelectTestKeys:
    def test_full_population(self, label_table, parameters):
        result = select_test_keys(label_table, parameters)
        # identity_columns 含 prod_name → 4 cust × 3 prod for test date
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

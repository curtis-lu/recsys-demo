"""Tests for dataset building pipeline Spark nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_spark import (
    build_dataset,
    prepare_model_input,
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
                })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "dataset": {
            "train_snap_date_start": "2024-01-31",
            "train_snap_date_end": "2024-03-31",
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
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))
        excluded = val_dates | test_dates
        assert not pdf["snap_date"].isin(excluded).any()
        # All dates within train range
        start = pd.Timestamp(parameters["dataset"]["train_snap_date_start"])
        end = pd.Timestamp(parameters["dataset"]["train_snap_date_end"])
        assert all(pdf["snap_date"] >= start)
        assert all(pdf["snap_date"] <= end)

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
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)
        assert train.count() + train_dev.count() == sample_keys.count()


class TestSelectValKeys:
    def test_full_population(self, label_table, parameters):
        result = select_val_keys(label_table, parameters)
        assert result.count() == 4  # 4 unique cust_ids for val date


class TestSelectTestKeys:
    def test_full_population(self, label_table, parameters):
        result = select_test_keys(label_table, parameters)
        assert result.count() == 4  # 4 unique cust_ids for test date


class TestBuildDataset:
    def test_joins_with_product_keys(self, spark, feature_table, label_table, parameters):
        """When keys include prod_name, join label_table on full identity key."""
        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
                "prod_name": ["exchange_fx", "exchange_fx"],
            })
        )
        result = build_dataset(keys, feature_table, label_table, parameters)
        # 2 rows: one per key (specific product)
        assert result.count() == 2
        assert "total_aum" in result.columns
        assert "label" in result.columns

    def test_joins_without_product_keys(self, spark, feature_table, label_table, parameters):
        """When keys don't include prod_name, expand to all products."""
        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
            })
        )
        result = build_dataset(keys, feature_table, label_table, parameters)
        # 2 customers x 3 products = 6 rows
        assert result.count() == 6
        assert "total_aum" in result.columns
        assert "label" in result.columns
        assert "prod_name" in result.columns


class TestPrepareModelInput:
    def _build_four_sets(self, spark, feature_table, label_table, parameters):
        all_keys = label_table.select("snap_date", "cust_id").dropDuplicates()
        from pyspark.sql import functions as F

        train_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-01-31"))
        train_dev_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-02-29"))
        val_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-04-30"))
        test_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-05-31"))
        train_set = build_dataset(train_keys, feature_table, label_table, parameters)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table, parameters)
        val_set = build_dataset(val_keys, feature_table, label_table, parameters)
        test_set = build_dataset(test_keys, feature_table, label_table, parameters)
        return train_set, train_dev_set, val_set, test_set

    def test_output_format(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        assert len(result) == 10
        (
            X_train, y_train, X_train_dev, y_train_dev,
            X_val, y_val, X_test, y_test,
            preprocessor, cat_mappings,
        ) = result

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, pd.DataFrame)
        assert list(y_train.columns) == ["label"]
        assert len(y_train) == len(X_train)
        assert len(y_train_dev) == len(X_train_dev)
        assert len(y_val) == len(X_val)
        assert len(y_test) == len(X_test)

    def test_excludes_non_feature_columns(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        X_train = result[0]

        forbidden = {"snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(X_train.columns))

    def test_prod_name_encoded_as_int(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        X_train = result[0]
        assert X_train["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

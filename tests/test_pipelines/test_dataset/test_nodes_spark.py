"""Tests for dataset building pipeline Spark nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_spark import (
    build_dataset,
    prepare_model_input,
    select_sample_keys,
    split_keys,
)


@pytest.fixture
def feature_table(spark):
    pdf = pd.DataFrame(
        {
            "snap_date": pd.to_datetime(
                ["2024-01-31"] * 4 + ["2024-02-29"] * 4 + ["2024-03-31"] * 4
            ),
            "cust_id": ["C001", "C002", "C003", "C004"] * 3,
            "total_aum": [100.0, 200.0, 300.0, 400.0] * 3,
            "fund_aum": [10.0, 20.0, 30.0, 40.0] * 3,
            "in_amt_sum_l1m": [5.0] * 12,
            "out_amt_sum_l1m": [3.0] * 12,
            "in_amt_ratio_l1m": [0.05] * 12,
            "out_amt_ratio_l1m": [0.03] * 12,
        }
    )
    return spark.createDataFrame(pdf)


@pytest.fixture
def label_table(spark):
    products = ["exchange_fx", "exchange_usd", "fund_stock"]
    segments = {"C001": "mass", "C002": "affluent", "C003": "hnw", "C004": "mass"}
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31"]:
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
    segments = {"C001": "mass", "C002": "affluent", "C003": "hnw", "C004": "mass"}
    rows = []
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004"]:
            rows.append({
                "snap_date": snap_dt,
                "cust_id": cid,
                "cust_segment_typ": segments[cid],
            })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "dataset": {
            "sample_ratio": 0.5,
            "sample_group_keys": ["snap_date"],
            "train_dev_snap_dates": ["2024-02-29"],
            "val_snap_dates": ["2024-03-31"],
        },
    }


class TestSelectSampleKeys:
    def test_returns_correct_columns(self, sample_pool, parameters):
        result = select_sample_keys(sample_pool, parameters)
        assert sorted(result.columns) == ["cust_id", "snap_date"]

    def test_stratified_by_snap_date(self, sample_pool, parameters):
        result = select_sample_keys(sample_pool, parameters)
        pdf = result.toPandas()
        counts = pdf.groupby("snap_date").size()
        # 4 customers per snap_date, sample_ratio=0.5 → 2 each
        assert all(counts == 2)

    def test_full_ratio_returns_all(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        result = select_sample_keys(sample_pool, params)
        # 4 customers x 3 snap_dates = 12
        assert result.count() == 12

    def test_no_duplicates(self, sample_pool, parameters):
        result = select_sample_keys(sample_pool, parameters)
        assert result.count() == result.dropDuplicates(["snap_date", "cust_id"]).count()


class TestSplitKeys:
    def test_three_way_split(self, label_table, parameters):
        keys = label_table.select("snap_date", "cust_id").dropDuplicates()
        train, train_dev, val = split_keys(keys, label_table, parameters)

        train_pdf = train.toPandas()
        train_dev_pdf = train_dev.toPandas()
        val_pdf = val.toPandas()

        train_dates = set(pd.to_datetime(train_pdf["snap_date"].unique()))
        train_dev_dates = set(pd.to_datetime(train_dev_pdf["snap_date"].unique()))
        val_dates = set(pd.to_datetime(val_pdf["snap_date"].unique()))

        assert pd.Timestamp("2024-01-31") in train_dates
        assert pd.Timestamp("2024-02-29") in train_dev_dates
        assert pd.Timestamp("2024-03-31") in val_dates

    def test_no_date_overlap(self, label_table, parameters):
        keys = label_table.select("snap_date", "cust_id").dropDuplicates()
        train, train_dev, val = split_keys(keys, label_table, parameters)

        train_dates = set(train.select("snap_date").distinct().toPandas()["snap_date"])
        td_dates = set(train_dev.select("snap_date").distinct().toPandas()["snap_date"])
        val_dates = set(val.select("snap_date").distinct().toPandas()["snap_date"])

        assert len(train_dates & td_dates) == 0
        assert len(train_dates & val_dates) == 0
        assert len(td_dates & val_dates) == 0

    def test_val_is_full_population(self, sample_pool, label_table, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 0.5}}
        sample_keys = select_sample_keys(sample_pool, params)
        _, _, val = split_keys(sample_keys, label_table, params)
        # Val should have all 4 customers for 2024-03-31
        assert val.count() == 4


class TestBuildDataset:
    def test_joins_features_and_labels(self, spark, feature_table, label_table, parameters):
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
    def _build_three_sets(self, spark, feature_table, label_table, parameters):
        all_keys = label_table.select("snap_date", "cust_id").dropDuplicates()
        from pyspark.sql import functions as F

        train_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-01-31"))
        train_dev_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-02-29"))
        val_keys = all_keys.filter(F.col("snap_date") == pd.Timestamp("2024-03-31"))
        train_set = build_dataset(train_keys, feature_table, label_table, parameters)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table, parameters)
        val_set = build_dataset(val_keys, feature_table, label_table, parameters)
        return train_set, train_dev_set, val_set

    def test_output_format(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, cat_mappings = result

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, pd.DataFrame)
        assert list(y_train.columns) == ["label"]
        assert len(y_train) == len(X_train)
        assert len(y_train_dev) == len(X_train_dev)
        assert len(y_val) == len(X_val)

    def test_excludes_non_feature_columns(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_train = result[0]

        forbidden = {"snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(X_train.columns))

    def test_prod_name_encoded_as_int(self, spark, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            spark, feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_train = result[0]
        assert X_train["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

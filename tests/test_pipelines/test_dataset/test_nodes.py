"""Tests for dataset building pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_pandas import (
    build_dataset,
    prepare_model_input,
    select_sample_keys,
    split_keys,
)


@pytest.fixture
def feature_table():
    return pd.DataFrame(
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


@pytest.fixture
def label_table():
    products = ["fx", "usd", "stock"]
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
                        "label": 1 if cid == "C001" and prod == "fx" else 0,
                        "prod_name": prod,
                    }
                )
    return pd.DataFrame(rows)


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
    def test_returns_unique_keys(self, label_table, parameters):
        result = select_sample_keys(label_table, parameters)
        assert list(result.columns) == ["snap_date", "cust_id"]
        assert result.duplicated().sum() == 0

    def test_stratified_by_snap_date(self, label_table, parameters):
        result = select_sample_keys(label_table, parameters)
        counts = result.groupby("snap_date").size()
        # 4 customers per snap_date, sample_ratio=0.5 → 2 each
        assert all(counts == 2)

    def test_deterministic(self, label_table, parameters):
        r1 = select_sample_keys(label_table, parameters)
        r2 = select_sample_keys(label_table, parameters)
        pd.testing.assert_frame_equal(r1, r2)

    def test_stratified_by_multiple_keys(self, label_table, parameters):
        """Test sampling stratified by snap_date and cust_segment_typ."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["snap_date", "cust_segment_typ"],
                "sample_ratio": 0.5,
            },
        }
        result = select_sample_keys(label_table, params)
        assert list(result.columns) == ["snap_date", "cust_id"]
        assert result.duplicated().sum() == 0
        # Should still produce valid keys
        assert len(result) > 0

    def test_output_excludes_group_columns(self, label_table, parameters):
        """Even with extra group keys, output only has snap_date and cust_id."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["snap_date", "cust_segment_typ"],
            },
        }
        result = select_sample_keys(label_table, params)
        assert list(result.columns) == ["snap_date", "cust_id"]


class TestSplitKeys:
    def test_three_way_split(self, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train, train_dev, val = split_keys(keys, label_table, parameters)

        train_dates = set(pd.to_datetime(train["snap_date"].unique()))
        train_dev_dates = set(pd.to_datetime(train_dev["snap_date"].unique()))
        val_dates = set(pd.to_datetime(val["snap_date"].unique()))

        assert pd.Timestamp("2024-01-31") in train_dates
        assert pd.Timestamp("2024-02-29") in train_dev_dates
        assert pd.Timestamp("2024-03-31") in val_dates

    def test_no_date_overlap(self, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train, train_dev, val = split_keys(keys, label_table, parameters)

        train_dates = set(pd.to_datetime(train["snap_date"].unique()))
        train_dev_dates = set(pd.to_datetime(train_dev["snap_date"].unique()))
        val_dates = set(pd.to_datetime(val["snap_date"].unique()))

        assert len(train_dates & train_dev_dates) == 0
        assert len(train_dates & val_dates) == 0
        assert len(train_dev_dates & val_dates) == 0

    def test_val_is_full_population(self, label_table, parameters):
        """Val should contain all customers for val dates, regardless of sampling."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_ratio": 0.5,
            },
        }
        sample_keys = select_sample_keys(label_table, params)
        _, _, val = split_keys(sample_keys, label_table, params)

        # Val should have all 4 customers for 2024-03-31
        assert len(val) == 4

    def test_train_dev_is_sampled(self, label_table, parameters):
        """Train-dev should reflect the sampled keys, not full population."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_ratio": 0.5,
            },
        }
        sample_keys = select_sample_keys(label_table, params)
        _, train_dev, _ = split_keys(sample_keys, label_table, params)

        # Train-dev should have ~2 customers (50% of 4)
        assert len(train_dev) == 2


class TestBuildDataset:
    def test_joins_features_and_labels(self, feature_table, label_table):
        keys = pd.DataFrame(
            {
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
            }
        )
        result = build_dataset(keys, feature_table, label_table)
        # 2 customers x 3 products = 6 rows
        assert len(result) == 6
        assert "total_aum" in result.columns
        assert "label" in result.columns
        assert "prod_name" in result.columns

    def test_missing_features_filled_nan(self, feature_table, label_table):
        # Add a customer that exists in labels but not features
        extra_label = pd.DataFrame(
            {
                "snap_date": [pd.Timestamp("2024-01-31")],
                "cust_id": ["C999"],
                "cust_segment_typ": ["mass"],
                "apply_start_date": [pd.Timestamp("2024-02-01")],
                "apply_end_date": [pd.Timestamp("2024-03-01")],
                "label": [0],
                "prod_name": ["fx"],
            }
        )
        labels = pd.concat([label_table, extra_label], ignore_index=True)
        keys = pd.DataFrame(
            {
                "snap_date": [pd.Timestamp("2024-01-31")],
                "cust_id": ["C999"],
            }
        )
        result = build_dataset(keys, feature_table, labels)
        assert result["total_aum"].isna().any()


class TestPrepareModelInput:
    def _build_three_sets(self, feature_table, label_table, parameters):
        """Helper to build train, train_dev, val sets."""
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_keys = keys[keys["snap_date"] == pd.Timestamp("2024-01-31")]
        train_dev_keys = keys[keys["snap_date"] == pd.Timestamp("2024-02-29")]
        val_keys = keys[keys["snap_date"] == pd.Timestamp("2024-03-31")]
        train_set = build_dataset(train_keys, feature_table, label_table)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table)
        val_set = build_dataset(val_keys, feature_table, label_table)
        return train_set, train_dev_set, val_set

    def test_output_format(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)

        X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, cat_mappings = result

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, np.ndarray)
        assert isinstance(X_train_dev, pd.DataFrame)
        assert isinstance(y_train_dev, np.ndarray)
        assert len(y_train) == len(X_train)
        assert len(y_train_dev) == len(X_train_dev)
        assert len(y_val) == len(X_val)

    def test_excludes_non_feature_columns(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_train = result[0]

        forbidden = {"snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(X_train.columns))

    def test_preprocessor_contents(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        preprocessor = result[6]

        assert "feature_columns" in preprocessor
        assert "categorical_columns" in preprocessor
        assert "category_mappings" in preprocessor
        assert "drop_columns" in preprocessor
        assert "prod_name" in preprocessor["category_mappings"]

    def test_category_mappings_returned_separately(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        preprocessor = result[6]
        cat_mappings = result[7]

        assert cat_mappings == preprocessor["category_mappings"]
        assert "prod_name" in cat_mappings

    def test_prod_name_encoded_as_int(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_train = result[0]

        assert X_train["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

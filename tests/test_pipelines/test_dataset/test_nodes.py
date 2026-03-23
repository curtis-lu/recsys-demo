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
    return pd.DataFrame(rows)


@pytest.fixture
def sample_pool():
    """Independent sample pool table with customer-month rows and segment."""
    rows = []
    segments = {"C001": "mass", "C002": "affluent", "C003": "hnw", "C004": "mass"}
    for snap in ["2024-01-31", "2024-02-29", "2024-03-31"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004"]:
            rows.append({
                "snap_date": snap_dt,
                "cust_id": cid,
                "cust_segment_typ": segments[cid],
            })
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
    def test_returns_unique_keys(self, sample_pool, parameters):
        result = select_sample_keys(sample_pool, parameters)
        assert list(result.columns) == ["snap_date", "cust_id"]
        assert result.duplicated().sum() == 0

    def test_stratified_by_snap_date(self, sample_pool, parameters):
        result = select_sample_keys(sample_pool, parameters)
        counts = result.groupby("snap_date").size()
        # 4 customers per snap_date, sample_ratio=0.5 → 2 each
        assert all(counts == 2)

    def test_deterministic(self, sample_pool, parameters):
        r1 = select_sample_keys(sample_pool, parameters)
        r2 = select_sample_keys(sample_pool, parameters)
        pd.testing.assert_frame_equal(r1, r2)

    def test_stratified_by_multiple_keys(self, sample_pool, parameters):
        """Test sampling stratified by snap_date and cust_segment_typ."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["snap_date", "cust_segment_typ"],
                "sample_ratio": 0.5,
            },
        }
        result = select_sample_keys(sample_pool, params)
        assert list(result.columns) == ["snap_date", "cust_id"]
        assert result.duplicated().sum() == 0
        # Should still produce valid keys
        assert len(result) > 0

    def test_output_excludes_group_columns(self, sample_pool, parameters):
        """Even with extra group keys, output only has snap_date and cust_id."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["snap_date", "cust_segment_typ"],
            },
        }
        result = select_sample_keys(sample_pool, params)
        assert list(result.columns) == ["snap_date", "cust_id"]

    def test_sample_pool_minimal_columns(self, parameters):
        """select_sample_keys works with minimal sample_pool columns."""
        pool = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-31"] * 3),
            "cust_id": ["C001", "C002", "C003"],
            "cust_segment_typ": ["mass", "affluent", "hnw"],
        })
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        result = select_sample_keys(pool, params)
        assert len(result) == 3


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

    def test_val_is_full_population(self, sample_pool, label_table, parameters):
        """Val should contain all customers for val dates, regardless of sampling."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_ratio": 0.5,
            },
        }
        sample_keys = select_sample_keys(sample_pool, params)
        _, _, val = split_keys(sample_keys, label_table, params)

        # Val should have all 4 customers for 2024-03-31
        assert len(val) == 4

    def test_train_dev_is_sampled(self, sample_pool, label_table, parameters):
        """Train-dev should reflect the sampled keys, not full population."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_ratio": 0.5,
            },
        }
        sample_keys = select_sample_keys(sample_pool, params)
        _, train_dev, _ = split_keys(sample_keys, label_table, params)

        # Train-dev should have ~2 customers (50% of 4)
        assert len(train_dev) == 2


class TestBuildDataset:
    def test_joins_features_and_labels(self, feature_table, label_table, parameters):
        keys = pd.DataFrame(
            {
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
            }
        )
        result = build_dataset(keys, feature_table, label_table, parameters)
        # 2 customers x 3 products = 6 rows
        assert len(result) == 6
        assert "total_aum" in result.columns
        assert "label" in result.columns
        assert "prod_name" in result.columns

    def test_missing_features_filled_nan(self, feature_table, label_table, parameters):
        # Add a customer that exists in labels but not features
        extra_label = pd.DataFrame(
            {
                "snap_date": [pd.Timestamp("2024-01-31")],
                "cust_id": ["C999"],
                "cust_segment_typ": ["mass"],
                "apply_start_date": [pd.Timestamp("2024-02-01")],
                "apply_end_date": [pd.Timestamp("2024-03-01")],
                "label": [0],
                "prod_name": ["exchange_fx"],
            }
        )
        labels = pd.concat([label_table, extra_label], ignore_index=True)
        keys = pd.DataFrame(
            {
                "snap_date": [pd.Timestamp("2024-01-31")],
                "cust_id": ["C999"],
            }
        )
        result = build_dataset(keys, feature_table, labels, parameters)
        assert result["total_aum"].isna().any()


class TestPrepareModelInput:
    def _build_three_sets(self, feature_table, label_table, parameters):
        """Helper to build train, train_dev, val sets."""
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_keys = keys[keys["snap_date"] == pd.Timestamp("2024-01-31")]
        train_dev_keys = keys[keys["snap_date"] == pd.Timestamp("2024-02-29")]
        val_keys = keys[keys["snap_date"] == pd.Timestamp("2024-03-31")]
        train_set = build_dataset(train_keys, feature_table, label_table, parameters)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table, parameters)
        val_set = build_dataset(val_keys, feature_table, label_table, parameters)
        return train_set, train_dev_set, val_set

    def test_output_format(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)

        X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, cat_mappings = result

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, pd.DataFrame)
        assert list(y_train.columns) == ["label"]
        assert isinstance(X_train_dev, pd.DataFrame)
        assert isinstance(y_train_dev, pd.DataFrame)
        assert list(y_train_dev.columns) == ["label"]
        assert isinstance(y_val, pd.DataFrame)
        assert list(y_val.columns) == ["label"]
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

    def test_val_sample_ratio_default_keeps_all(self, feature_table, label_table, parameters):
        """With val_sample_ratio absent or 1.0, X_val has same row count as full val_set."""
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        full_val_rows = len(val_set)
        result = prepare_model_input(train_set, train_dev_set, val_set, parameters)
        X_val = result[4]
        assert len(X_val) == full_val_rows

    def test_val_sample_ratio_reduces_val(self, feature_table, label_table, parameters):
        """With val_sample_ratio=0.5, X_val should have approximately 50% of rows."""
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        full_val_rows = len(val_set)
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        result = prepare_model_input(train_set, train_dev_set, val_set, params)
        X_val = result[4]
        y_val = result[5]
        assert len(X_val) < full_val_rows
        assert len(X_val) == len(y_val)

    def test_val_sample_ratio_deterministic(self, feature_table, label_table, parameters):
        """Same seed produces same val sampling results."""
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        r1 = prepare_model_input(train_set, train_dev_set, val_set.copy(), params)
        r2 = prepare_model_input(train_set, train_dev_set, val_set.copy(), params)
        pd.testing.assert_frame_equal(r1[4], r2[4])

    def test_val_sample_ratio_group_keys_fallback(self, feature_table, label_table, parameters):
        """When group keys are missing from val_set, falls back to simple random sampling."""
        train_set, train_dev_set, val_set = self._build_three_sets(
            feature_table, label_table, parameters
        )
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "val_sample_ratio": 0.5,
                "sample_group_keys": ["nonexistent_column"],
            },
        }
        result = prepare_model_input(train_set, train_dev_set, val_set, params)
        X_val = result[4]
        assert len(X_val) < len(val_set)

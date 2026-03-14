"""Tests for dataset building pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes import (
    build_dataset,
    prepare_model_input,
    select_sample_keys,
    split_keys,
)


@pytest.fixture
def feature_table():
    return pd.DataFrame(
        {
            "snap_date": pd.to_datetime(["2024-01-31"] * 4 + ["2024-02-29"] * 4),
            "cust_id": ["C001", "C002", "C003", "C004"] * 2,
            "total_aum": [100.0, 200.0, 300.0, 400.0, 150.0, 250.0, 350.0, 450.0],
            "fund_aum": [10.0, 20.0, 30.0, 40.0, 15.0, 25.0, 35.0, 45.0],
            "in_amt_sum_l1m": [5.0] * 8,
            "out_amt_sum_l1m": [3.0] * 8,
            "in_amt_ratio_l1m": [0.05] * 8,
            "out_amt_ratio_l1m": [0.03] * 8,
        }
    )


@pytest.fixture
def label_table():
    products = ["fx", "usd", "stock"]
    rows = []
    for snap in ["2024-01-31", "2024-02-29"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004"]:
            for prod in products:
                rows.append(
                    {
                        "snap_date": snap_dt,
                        "cust_id": cid,
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
            "val_snap_dates": ["2024-02-29"],
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
        # 4 customers per snap_date, sample_ratio=0.5 → ~2 each
        assert all(counts == 2)

    def test_deterministic(self, label_table, parameters):
        r1 = select_sample_keys(label_table, parameters)
        r2 = select_sample_keys(label_table, parameters)
        pd.testing.assert_frame_equal(r1, r2)


class TestSplitKeys:
    def test_temporal_split(self, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train, val = split_keys(keys, parameters)

        train_dates = set(train["snap_date"].unique())
        val_dates = set(val["snap_date"].unique())
        assert pd.Timestamp("2024-02-29") in val_dates
        assert pd.Timestamp("2024-02-29") not in train_dates
        assert pd.Timestamp("2024-01-31") in train_dates

    def test_no_overlap(self, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train, val = split_keys(keys, parameters)

        train_set = set(zip(train["snap_date"], train["cust_id"]))
        val_set = set(zip(val["snap_date"], val["cust_id"]))
        assert len(train_set & val_set) == 0


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
    def test_output_format(self, feature_table, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_keys = keys[keys["snap_date"] == pd.Timestamp("2024-01-31")]
        val_keys = keys[keys["snap_date"] == pd.Timestamp("2024-02-29")]
        train_set = build_dataset(train_keys, feature_table, label_table)
        val_set = build_dataset(val_keys, feature_table, label_table)

        X_train, y_train, X_val, y_val, preprocessor = prepare_model_input(
            train_set, val_set, parameters
        )

        assert isinstance(X_train, pd.DataFrame)
        assert isinstance(y_train, np.ndarray)
        assert len(y_train) == len(X_train)
        assert len(y_val) == len(X_val)

    def test_excludes_non_feature_columns(self, feature_table, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_set = build_dataset(keys, feature_table, label_table)
        val_set = train_set.copy()

        X_train, _, _, _, _ = prepare_model_input(train_set, val_set, parameters)

        forbidden = {"snap_date", "cust_id", "label", "apply_start_date", "apply_end_date"}
        assert forbidden.isdisjoint(set(X_train.columns))

    def test_preprocessor_contents(self, feature_table, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_set = build_dataset(keys, feature_table, label_table)
        val_set = train_set.copy()

        _, _, _, _, preprocessor = prepare_model_input(train_set, val_set, parameters)

        assert "feature_columns" in preprocessor
        assert "categorical_columns" in preprocessor
        assert "category_mappings" in preprocessor
        assert "prod_name" in preprocessor["category_mappings"]

    def test_prod_name_encoded_as_int(self, feature_table, label_table, parameters):
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_set = build_dataset(keys, feature_table, label_table)
        val_set = train_set.copy()

        X_train, _, _, _, _ = prepare_model_input(train_set, val_set, parameters)

        assert X_train["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

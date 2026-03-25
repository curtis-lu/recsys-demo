"""Tests for dataset building pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_pandas import (
    _validate_date_splits,
    build_dataset,
    prepare_model_input,
    prepare_model_input_with_calibration,
    select_calibration_keys,
    select_keys,
    select_test_keys,
    select_train_keys,
    select_val_keys,
    split_train_keys,
)


@pytest.fixture
def feature_table():
    return pd.DataFrame(
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


@pytest.fixture
def label_table():
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
    return pd.DataFrame(rows)


@pytest.fixture
def sample_pool():
    """Sample pool at customer-month-product granularity (matches SQL schema)."""
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
    return pd.DataFrame(rows)


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


class TestDateValidation:
    def test_non_overlapping_dates_pass(self):
        params = {
            "dataset": {
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-09-30",
                "calibration_snap_dates": ["2024-10-31"],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        _validate_date_splits(params)  # should not raise

    def test_overlapping_val_test_raises(self):
        params = {
            "dataset": {
                "calibration_snap_dates": [],
                "val_snap_dates": ["2024-12-31"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        with pytest.raises(ValueError, match="val & test"):
            _validate_date_splits(params)

    def test_overlapping_cal_val_raises(self):
        params = {
            "dataset": {
                "calibration_snap_dates": ["2024-11-30"],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        with pytest.raises(ValueError, match="calibration & val"):
            _validate_date_splits(params)

    def test_overlapping_cal_test_raises(self):
        params = {
            "dataset": {
                "calibration_snap_dates": ["2024-12-31"],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        with pytest.raises(ValueError, match="calibration & test"):
            _validate_date_splits(params)

    def test_empty_calibration_dates_ok(self):
        params = {
            "dataset": {
                "calibration_snap_dates": [],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        _validate_date_splits(params)  # should not raise

    def test_train_start_after_end_raises(self):
        params = {
            "dataset": {
                "train_snap_date_start": "2024-06-30",
                "train_snap_date_end": "2024-01-31",
                "calibration_snap_dates": [],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        with pytest.raises(ValueError, match="train_snap_date_start"):
            _validate_date_splits(params)

    def test_train_overlaps_val_raises(self):
        params = {
            "dataset": {
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-11-30",
                "calibration_snap_dates": [],
                "val_snap_dates": ["2024-11-30"],
                "test_snap_dates": ["2024-12-31"],
            }
        }
        with pytest.raises(ValueError, match="train & val"):
            _validate_date_splits(params)


class TestSelectTrainKeys:
    def test_returns_identity_columns(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        assert list(result.columns) == ["snap_date", "cust_id", "prod_name"]
        assert result.duplicated().sum() == 0

    def test_filters_to_train_dates(self, sample_pool, parameters):
        """Should only include dates within train_snap_date_start ~ end."""
        result = select_train_keys(sample_pool, parameters)
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))
        excluded = val_dates | test_dates
        assert not result["snap_date"].isin(excluded).any()
        # All dates should be within start~end range
        start = pd.Timestamp(parameters["dataset"]["train_snap_date_start"])
        end = pd.Timestamp(parameters["dataset"]["train_snap_date_end"])
        assert all(result["snap_date"] >= start)
        assert all(result["snap_date"] <= end)

    def test_deterministic(self, sample_pool, parameters):
        r1 = select_train_keys(sample_pool, parameters)
        r2 = select_train_keys(sample_pool, parameters)
        pd.testing.assert_frame_equal(r1, r2)

    def test_full_ratio_no_overrides(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        result = select_train_keys(sample_pool, params)
        # 3 train dates x 4 customers x 3 products = 36
        assert len(result) == 36


class TestSelectKeys:
    def test_filters_to_specified_dates(self, sample_pool, parameters):
        snap_dates = [pd.Timestamp("2024-01-31")]
        result = select_keys(sample_pool, parameters, snap_dates, 1.0)
        assert all(result["snap_date"] == pd.Timestamp("2024-01-31"))

    def test_stratified_sampling(self, sample_pool, parameters):
        snap_dates = [pd.Timestamp("2024-01-31"), pd.Timestamp("2024-02-29")]
        result = select_keys(sample_pool, parameters, snap_dates, 0.5)
        assert len(result) > 0
        assert len(result) < 24  # 2 dates x 4 custs x 3 products

    def test_output_columns(self, sample_pool, parameters):
        snap_dates = [pd.Timestamp("2024-01-31")]
        result = select_keys(sample_pool, parameters, snap_dates, 1.0)
        assert list(result.columns) == ["snap_date", "cust_id", "prod_name"]


class TestSampleRatioOverrides:
    def test_single_column_override(self, sample_pool, parameters):
        """Override for a single group key value."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["cust_segment_typ"],
                "sample_ratio": 0.0,  # default: sample nothing
                "sample_ratio_overrides": {"mass": 1.0},  # except mass: keep all
            },
        }
        result = select_train_keys(sample_pool, params)
        # Only mass customers (C001, C004) should be kept
        assert len(result) > 0
        pool = sample_pool.set_index(["snap_date", "cust_id", "prod_name"])
        for _, row in result.iterrows():
            key = (row["snap_date"], row["cust_id"], row["prod_name"])
            assert pool.loc[key, "cust_segment_typ"] == "mass"

    def test_multi_column_override(self, sample_pool, parameters):
        """Override with multi-column group keys using '|' separator."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["cust_segment_typ", "prod_name"],
                "sample_ratio": 0.0,
                "sample_ratio_overrides": {
                    "mass|exchange_fx": 1.0,
                },
            },
        }
        result = select_train_keys(sample_pool, params)
        # Should only get mass customers with exchange_fx
        assert len(result) > 0
        for _, row in result.iterrows():
            assert row["prod_name"] == "exchange_fx"

    def test_fallback_to_default(self, sample_pool, parameters):
        """Groups not in overrides use the default sample_ratio."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_group_keys": ["cust_segment_typ"],
                "sample_ratio": 1.0,
                "sample_ratio_overrides": {"affluent": 0.0},
            },
        }
        result = select_train_keys(sample_pool, params)
        # affluent (C002) should be excluded, others kept
        assert "C002" not in result["cust_id"].values


class TestSplitTrainKeys:
    def test_cust_id_split(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        # No overlap in cust_ids
        train_custs = set(train["cust_id"].unique())
        dev_custs = set(train_dev["cust_id"].unique())
        assert len(train_custs & dev_custs) == 0

    def test_all_keys_preserved(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        total = len(train) + len(train_dev)
        assert total == len(sample_keys)

    def test_same_snap_dates(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        train_dates = set(train["snap_date"].unique())
        dev_dates = set(train_dev["snap_date"].unique())
        expected_dates = set(sample_keys["snap_date"].unique())
        assert train_dates | dev_dates == expected_dates

    def test_deterministic(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        t1, d1 = split_train_keys(sample_keys, params)
        t2, d2 = split_train_keys(sample_keys, params)
        pd.testing.assert_frame_equal(t1, t2)
        pd.testing.assert_frame_equal(d1, d2)

    def test_ratio_approximate(self, sample_pool, parameters):
        """train_dev_ratio=0.2 with 4 cust_ids → 1 in dev (at least 1)."""
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        _, train_dev = split_train_keys(sample_keys, params)
        dev_custs = train_dev["cust_id"].nunique()
        assert dev_custs >= 1


class TestSelectCalibrationKeys:
    def test_filter_to_calibration_dates(self, sample_pool, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        result = select_calibration_keys(sample_pool, params)
        assert all(result["snap_date"] == pd.Timestamp("2024-03-31"))

    def test_full_population(self, sample_pool, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "calibration_sample_ratio": 1.0,
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        result = select_calibration_keys(sample_pool, params)
        assert result["cust_id"].nunique() == 4  # all 4 customers

    def test_deterministic(self, sample_pool, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "calibration_sample_ratio": 0.5,
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        r1 = select_calibration_keys(sample_pool, params)
        r2 = select_calibration_keys(sample_pool, params)
        pd.testing.assert_frame_equal(r1, r2)

    def test_output_identity_columns(self, sample_pool, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        result = select_calibration_keys(sample_pool, params)
        assert list(result.columns) == ["snap_date", "cust_id", "prod_name"]

    def test_independent_overrides(self, sample_pool, parameters):
        """Calibration overrides should be independent from train overrides."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "calibration_sample_ratio": 0.0,
                "calibration_sample_ratio_overrides": {"mass|exchange_fx": 1.0},
                "sample_ratio": 0.0,
                "sample_ratio_overrides": {"affluent|exchange_usd": 1.0},
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        cal_result = select_calibration_keys(sample_pool, params)
        # Calibration should only have mass|exchange_fx rows
        assert len(cal_result) > 0
        pool = sample_pool.set_index(["snap_date", "cust_id", "prod_name"])
        for _, row in cal_result.iterrows():
            key = (row["snap_date"], row["cust_id"], row["prod_name"])
            assert pool.loc[key, "cust_segment_typ"] == "mass"
            assert row["prod_name"] == "exchange_fx"

        # Train should only have affluent|exchange_usd rows
        train_result = select_train_keys(sample_pool, params)
        assert len(train_result) > 0
        for _, row in train_result.iterrows():
            key = (row["snap_date"], row["cust_id"], row["prod_name"])
            assert pool.loc[key, "cust_segment_typ"] == "affluent"
            assert row["prod_name"] == "exchange_usd"

    def test_empty_calibration_overrides_uses_default_ratio(self, sample_pool, parameters):
        """When calibration_sample_ratio_overrides is empty, all groups use calibration_sample_ratio."""
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "enable_calibration": True,
                "calibration_snap_dates": ["2024-03-31"],
                "calibration_sample_ratio": 1.0,
                "calibration_sample_ratio_overrides": {},
                "sample_ratio_overrides": {"mass|exchange_fx": 0.0},
                "train_snap_date_start": "2024-01-31",
                "train_snap_date_end": "2024-02-29",
                "val_snap_dates": ["2024-04-30"],
                "test_snap_dates": ["2024-05-31"],
            },
        }
        result = select_calibration_keys(sample_pool, params)
        # Should have all 4 custs x 3 products = 12 rows (train overrides NOT applied)
        assert len(result) == 12


class TestSelectValKeys:
    def test_full_population(self, label_table, parameters):
        result = select_val_keys(label_table, parameters)
        assert result["cust_id"].nunique() == 4
        assert all(result["snap_date"] == pd.Timestamp("2024-04-30"))

    def test_random_sampling(self, label_table, parameters):
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        result = select_val_keys(label_table, params)
        assert result["cust_id"].nunique() < 4

    def test_sampling_by_cust_id(self, label_table, parameters):
        """All rows for a sampled cust_id should be included."""
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        result = select_val_keys(label_table, params)
        # Each sampled cust_id should have exactly 1 row (1 val date x 1 identity key)
        counts = result.groupby("cust_id").size()
        assert all(counts == 1)

    def test_output_identity_columns_only(self, label_table, parameters):
        result = select_val_keys(label_table, parameters)
        assert list(result.columns) == ["snap_date", "cust_id"]

    def test_deterministic(self, label_table, parameters):
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        r1 = select_val_keys(label_table, params)
        r2 = select_val_keys(label_table, params)
        pd.testing.assert_frame_equal(r1, r2)


class TestSelectTestKeys:
    def test_full_population(self, label_table, parameters):
        result = select_test_keys(label_table, parameters)
        assert result["cust_id"].nunique() == 4
        assert all(result["snap_date"] == pd.Timestamp("2024-05-31"))

    def test_no_sampling(self, label_table, parameters):
        """Test keys always returns full population."""
        result = select_test_keys(label_table, parameters)
        expected = label_table[
            label_table["snap_date"] == pd.Timestamp("2024-05-31")
        ][["snap_date", "cust_id"]].drop_duplicates()
        assert len(result) == len(expected)

    def test_output_identity_columns_only(self, label_table, parameters):
        result = select_test_keys(label_table, parameters)
        assert list(result.columns) == ["snap_date", "cust_id"]


class TestBuildDataset:
    def test_joins_with_product_keys(self, feature_table, label_table, parameters):
        """When keys include prod_name, join label_table on full identity key."""
        keys = pd.DataFrame(
            {
                "snap_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
                "cust_id": ["C001", "C002"],
                "prod_name": ["exchange_fx", "exchange_fx"],
            }
        )
        result = build_dataset(keys, feature_table, label_table, parameters)
        # 2 rows: one per key (specific product)
        assert len(result) == 2
        assert "total_aum" in result.columns
        assert "label" in result.columns
        assert all(result["prod_name"] == "exchange_fx")

    def test_joins_without_product_keys(self, feature_table, label_table, parameters):
        """When keys don't include prod_name, expand to all products via label_table."""
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
    def _build_four_sets(self, feature_table, label_table, parameters):
        """Helper to build train, train_dev, val, test sets."""
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_keys = keys[keys["snap_date"] == pd.Timestamp("2024-01-31")]
        train_dev_keys = keys[keys["snap_date"] == pd.Timestamp("2024-02-29")]
        val_keys = keys[keys["snap_date"] == pd.Timestamp("2024-04-30")]
        test_keys = keys[keys["snap_date"] == pd.Timestamp("2024-05-31")]
        train_set = build_dataset(train_keys, feature_table, label_table, parameters)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table, parameters)
        val_set = build_dataset(val_keys, feature_table, label_table, parameters)
        test_set = build_dataset(test_keys, feature_table, label_table, parameters)
        return train_set, train_dev_set, val_set, test_set

    def test_output_format(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
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

    def test_excludes_non_feature_columns(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        X_train = result[0]

        forbidden = {"snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(X_train.columns))

    def test_preprocessor_contents(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        preprocessor = result[8]

        assert "feature_columns" in preprocessor
        assert "categorical_columns" in preprocessor
        assert "category_mappings" in preprocessor
        assert "drop_columns" in preprocessor
        assert "prod_name" in preprocessor["category_mappings"]

    def test_category_mappings_returned_separately(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        preprocessor = result[8]
        cat_mappings = result[9]

        assert cat_mappings == preprocessor["category_mappings"]
        assert "prod_name" in cat_mappings

    def test_prod_name_encoded_as_int(self, feature_table, label_table, parameters):
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        X_train = result[0]

        assert X_train["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

    def test_no_val_sample_ratio_logic(self, feature_table, label_table, parameters):
        """prepare_model_input should NOT apply val_sample_ratio (moved to select_val_keys)."""
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        full_val_rows = len(val_set)
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": 0.5},
        }
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, params)
        X_val = result[4]
        # Should still have all rows — sampling is NOT done here
        assert len(X_val) == full_val_rows

    def test_test_set_included(self, feature_table, label_table, parameters):
        """Verify X_test and y_test are produced."""
        train_set, train_dev_set, val_set, test_set = self._build_four_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input(train_set, train_dev_set, val_set, test_set, parameters)
        X_test = result[6]
        y_test = result[7]
        assert len(X_test) == len(test_set)
        assert len(y_test) == len(test_set)


class TestPrepareModelInputWithCalibration:
    def _build_five_sets(self, feature_table, label_table, parameters):
        """Helper to build train, train_dev, calibration, val, test sets."""
        keys = label_table[["snap_date", "cust_id"]].drop_duplicates()
        train_keys = keys[keys["snap_date"] == pd.Timestamp("2024-01-31")]
        train_dev_keys = keys[keys["snap_date"] == pd.Timestamp("2024-02-29")]
        cal_keys = keys[keys["snap_date"] == pd.Timestamp("2024-03-31")]
        val_keys = keys[keys["snap_date"] == pd.Timestamp("2024-04-30")]
        test_keys = keys[keys["snap_date"] == pd.Timestamp("2024-05-31")]
        train_set = build_dataset(train_keys, feature_table, label_table, parameters)
        train_dev_set = build_dataset(train_dev_keys, feature_table, label_table, parameters)
        cal_set = build_dataset(cal_keys, feature_table, label_table, parameters)
        val_set = build_dataset(val_keys, feature_table, label_table, parameters)
        test_set = build_dataset(test_keys, feature_table, label_table, parameters)
        return train_set, train_dev_set, cal_set, val_set, test_set

    def test_output_format(self, feature_table, label_table, parameters):
        train_set, train_dev_set, cal_set, val_set, test_set = self._build_five_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input_with_calibration(
            train_set, train_dev_set, cal_set, val_set, test_set, parameters
        )

        assert len(result) == 12
        (
            X_train, y_train, X_train_dev, y_train_dev,
            X_cal, y_cal, X_val, y_val,
            X_test, y_test, preprocessor, cat_mappings,
        ) = result

        assert len(y_train) == len(X_train)
        assert len(y_train_dev) == len(X_train_dev)
        assert len(y_cal) == len(X_cal)
        assert len(y_val) == len(X_val)
        assert len(y_test) == len(X_test)
        assert "prod_name" in cat_mappings

    def test_category_from_train_only(self, feature_table, label_table, parameters):
        train_set, train_dev_set, cal_set, val_set, test_set = self._build_five_sets(
            feature_table, label_table, parameters
        )
        result = prepare_model_input_with_calibration(
            train_set, train_dev_set, cal_set, val_set, test_set, parameters
        )
        preprocessor = result[10]
        # category_mappings should match train_set products
        expected_prods = sorted(train_set["prod_name"].unique())
        assert preprocessor["category_mappings"]["prod_name"] == expected_prods

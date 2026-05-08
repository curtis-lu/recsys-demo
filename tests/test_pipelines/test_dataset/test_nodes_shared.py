"""Tests for backend-agnostic dataset pipeline helpers (nodes_shared)."""

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_shared import (
    collect_dataset_snap_dates,
    validate_date_splits,
)


class TestCollectDatasetSnapDates:
    def test_returns_sorted_union(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-03-31", "2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-06-30"],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert result == [
            pd.Timestamp("2025-01-31"),
            pd.Timestamp("2025-02-28"),
            pd.Timestamp("2025-03-31"),
            pd.Timestamp("2025-04-30"),
            pd.Timestamp("2025-05-31"),
            pd.Timestamp("2025-06-30"),
        ]

    def test_deduplicates_overlapping_entries(self):
        # 不同 split 不應重複；helper 不負責 overlap 檢查（那是 validate_date_splits）
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-02-28"],  # dup with train
                "val_snap_dates": [],
                "test_snap_dates": [],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert result == [pd.Timestamp("2025-01-31"), pd.Timestamp("2025-02-28")]

    def test_returns_pd_timestamp_objects(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": [],
                "test_snap_dates": [],
            }
        }
        result = collect_dataset_snap_dates(params)
        assert all(isinstance(d, pd.Timestamp) for d in result)

    def test_missing_train_snap_dates_raises(self):
        params = {
            "dataset": {
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-06-30"],
            }
        }
        with pytest.raises(KeyError, match="train_snap_dates"):
            collect_dataset_snap_dates(params)

    def test_optional_splits_default_to_empty(self):
        # cal/val/test 缺鍵時用 .get(..., [])，不應 raise
        params = {"dataset": {"train_snap_dates": ["2025-01-31"]}}
        result = collect_dataset_snap_dates(params)
        assert result == [pd.Timestamp("2025-01-31")]


class TestValidateDateSplits:
    def test_non_overlapping_passes(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-03-31"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        validate_date_splits(params)  # should not raise

    def test_train_cal_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-02-28"],
                "calibration_snap_dates": ["2025-02-28"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="train & calibration"):
            validate_date_splits(params)

    def test_train_val_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31", "2025-04-30"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="train & val"):
            validate_date_splits(params)

    def test_cal_val_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="calibration & val"):
            validate_date_splits(params)

    def test_val_test_overlap_raises(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-05-31"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError, match="val & test"):
            validate_date_splits(params)

    def test_three_way_overlap_reports_all_pairs(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-04-30"],
                "calibration_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        with pytest.raises(ValueError) as exc_info:
            validate_date_splits(params)
        msg = str(exc_info.value)
        assert "train & calibration" in msg
        assert "train & val" in msg
        assert "calibration & val" in msg

    def test_empty_calibration_passes(self):
        params = {
            "dataset": {
                "train_snap_dates": ["2025-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": ["2025-04-30"],
                "test_snap_dates": ["2025-05-31"],
            }
        }
        validate_date_splits(params)  # should not raise

    def test_missing_optional_keys_pass(self):
        # cal/val/test 完全沒提供時也應通過（用 .get(..., [])）
        params = {"dataset": {"train_snap_dates": ["2025-01-31"]}}
        validate_date_splits(params)  # should not raise

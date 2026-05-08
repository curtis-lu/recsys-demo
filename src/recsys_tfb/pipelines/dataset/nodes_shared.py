"""Shared functions for the dataset building pipeline."""

import pandas as pd


def validate_date_splits(parameters: dict) -> None:
    """Validate that train, calibration, val, and test snap_dates are mutually non-overlapping."""
    ds = parameters.get("dataset", {})

    # Build train date set from start/end range
    train_start = ds.get("train_snap_date_start")
    train_end = ds.get("train_snap_date_end")
    if train_start and train_end:
        train_start_ts = pd.Timestamp(train_start)
        train_end_ts = pd.Timestamp(train_end)
        if train_start_ts > train_end_ts:
            raise ValueError(
                f"train_snap_date_start ({train_start}) > train_snap_date_end ({train_end})"
            )

    calibration_dates = set(str(d) for d in ds.get("calibration_snap_dates", []))
    val_dates = set(str(d) for d in ds.get("val_snap_dates", []))
    test_dates = set(str(d) for d in ds.get("test_snap_dates", []))

    overlaps = []
    cal_val = calibration_dates & val_dates
    if cal_val:
        overlaps.append(f"calibration & val: {sorted(cal_val)}")
    cal_test = calibration_dates & test_dates
    if cal_test:
        overlaps.append(f"calibration & test: {sorted(cal_test)}")
    val_test = val_dates & test_dates
    if val_test:
        overlaps.append(f"val & test: {sorted(val_test)}")

    # Validate train range doesn't overlap with cal/val/test
    if train_start and train_end:
        train_start_ts = pd.Timestamp(train_start)
        train_end_ts = pd.Timestamp(train_end)
        for name, date_set in [("calibration", calibration_dates), ("val", val_dates), ("test", test_dates)]:
            for d in date_set:
                d_ts = pd.Timestamp(d)
                if train_start_ts <= d_ts <= train_end_ts:
                    overlaps.append(f"train & {name}: [{d}]")

    if overlaps:
        raise ValueError(f"Date splits overlap: {'; '.join(overlaps)}")


def collect_dataset_snap_dates(parameters: dict) -> list[pd.Timestamp]:
    """Return sorted union of train/cal/val/test snap_dates as pd.Timestamps.

    Single source of truth for "which snap_dates does the dataset pipeline use".
    Used by apply_preprocessor_to_features (all splits) — fit_preprocessor_metadata
    deliberately uses only train_snap_dates to prevent val/test leakage into the
    category-mapping fit.
    """
    ds = parameters["dataset"]
    dates: set[pd.Timestamp] = set()
    dates.update(pd.Timestamp(d) for d in ds["train_snap_dates"])
    dates.update(pd.Timestamp(d) for d in ds.get("calibration_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("val_snap_dates", []))
    dates.update(pd.Timestamp(d) for d in ds.get("test_snap_dates", []))
    return sorted(dates)

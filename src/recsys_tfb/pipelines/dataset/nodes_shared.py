"""Shared functions for the dataset building pipeline."""

import pandas as pd


def validate_date_splits(parameters: dict) -> None:
    """Validate that train/calibration/val/test snap_date sets are mutually disjoint."""
    ds = parameters.get("dataset", {})
    sets = {
        "train":       set(str(d) for d in ds.get("train_snap_dates", [])),
        "calibration": set(str(d) for d in ds.get("calibration_snap_dates", [])),
        "val":         set(str(d) for d in ds.get("val_snap_dates", [])),
        "test":        set(str(d) for d in ds.get("test_snap_dates", [])),
    }
    overlaps = []
    names = list(sets.keys())
    for i, a in enumerate(names):
        for b in names[i+1:]:
            common = sets[a] & sets[b]
            if common:
                overlaps.append(f"{a} & {b}: {sorted(common)}")
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

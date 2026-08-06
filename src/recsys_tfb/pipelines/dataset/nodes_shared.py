"""Shared functions for the dataset building pipeline."""

import logging
from typing import Iterable, NamedTuple

import pandas as pd

logger = logging.getLogger(__name__)


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


class SnapDatePlan(NamedTuple):
    """What an incremental dataset run will do, and what it will not do.

    ``to_process`` and ``skipped`` are always a partition of the configured
    snap_dates (deduplicated, sorted, disjoint, union == configured), so a
    caller can log or assert on both halves without recomputing anything.
    """

    to_process: list[pd.Timestamp]
    skipped: list[pd.Timestamp]


def plan_incremental_snap_dates(
    configured: Iterable,
    existing: Iterable,
    rebuild: Iterable = (),
) -> SnapDatePlan:
    """Split ``configured`` snap_dates into (to_process, skipped).

    A configured month is processed when it has not landed yet, or when it was
    explicitly named in ``rebuild``; otherwise it is skipped. This is the single
    place the "which months does this run actually touch" diff is defined; its
    one caller is :func:`~recsys_tfb.pipelines.dataset.month_plans.build_month_plans`,
    which applies it once per incremental artifact. See ADR-0002.

    Pure by design: the caller supplies ``existing`` (a zero-scan metastore
    partition listing), so the whole decision is testable without Spark.

    Both halves are returned, not just ``to_process``. A pipeline that decides
    to do less work has to be able to say what it decided not to do, and a
    returned list is the only form of that statement worth asserting on.

    Args:
        configured: snap_dates the config asks for. Accepts str / date /
            ``pd.Timestamp``; normalised to ``pd.Timestamp``.
        existing: snap_dates already landed. Values outside ``configured`` are
            ignored (a month dropped from config leaves its partition behind).
        rebuild: snap_dates to reprocess even though they already exist — the
            escape hatch for upstream backfill (ADR-0002). Values outside
            ``configured`` are ignored; that subset invariant is enforced
            up-front and fail-loud by consistency invariant A21, which keeps
            this function total.

    Returns:
        :class:`SnapDatePlan`.
    """
    configured_set = {pd.Timestamp(d) for d in configured}
    existing_set = {pd.Timestamp(d) for d in existing}
    rebuild_set = {pd.Timestamp(d) for d in rebuild}

    to_process = sorted(
        d for d in configured_set if d not in existing_set or d in rebuild_set
    )
    skipped = sorted(
        d for d in configured_set if d in existing_set and d not in rebuild_set
    )
    return SnapDatePlan(to_process=to_process, skipped=skipped)

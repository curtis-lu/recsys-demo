"""Shared functions for the dataset building pipeline."""

import logging
from typing import Iterable, NamedTuple

import pandas as pd

logger = logging.getLogger(__name__)

#: ``parameters`` keys injected by the dataset CLI entry point. The metastore
#: partition listing is done once, up front, by the caller (``__main__``) and
#: handed to the nodes — see :func:`resolve_snap_date_plan` for why.
EXISTING_SNAP_DATES_KEY = "_existing_snap_dates"
REBUILD_SNAP_DATES_KEY = "_rebuild_snap_dates"


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
    place the "which months does this run actually touch" diff is defined — the
    four test-branch nodes (encode / select test keys / build + filter test
    model input) all ask this function rather than each deriving a diff of its
    own. See ADR-0002.

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


def _fmt_dates(dates: Iterable[pd.Timestamp]) -> str:
    return ",".join(d.strftime("%Y-%m-%d") for d in dates) or "-"


def resolve_snap_date_plan(
    parameters: dict, dataset_name: str, configured: Iterable
) -> SnapDatePlan:
    """The single entry point every incremental dataset node goes through.

    Looks up the partition listing the CLI collected for ``dataset_name``,
    diffs it via :func:`plan_incremental_snap_dates`, and emits the structured
    "what I did / what I did not do" line.

    Why the listing is injected rather than queried here: the four test-branch
    nodes must agree with each other and with the manifest about which months
    this run covered. One metastore listing taken up front, before any node
    writes, gives all of them the same answer by construction — a per-node
    query would read a metastore that earlier nodes had already mutated. It
    also keeps every node testable without a metastore.

    Falls back to "nothing exists" when the keys are absent, so a pipeline
    driven outside the CLI (tests, notebooks) processes everything — the
    pre-ADR-0002 behaviour.
    """
    existing = (parameters.get(EXISTING_SNAP_DATES_KEY) or {}).get(dataset_name, [])
    rebuild = parameters.get(REBUILD_SNAP_DATES_KEY) or []
    plan = plan_incremental_snap_dates(configured, existing, rebuild)
    logger.info(
        "[months] dataset=%s processed=%s skipped=%s",
        dataset_name, _fmt_dates(plan.to_process), _fmt_dates(plan.skipped),
    )
    return plan

"""Tests for backend-agnostic dataset pipeline helpers (nodes_shared)."""

import datetime

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.nodes_shared import (
    collect_dataset_snap_dates,
    plan_incremental_snap_dates,
)


def _ts(*dates: str) -> list[pd.Timestamp]:
    return [pd.Timestamp(d) for d in dates]


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
        # Different splits must not duplicate; the helper does not check overlap (that's A24, core/consistency.py)
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
        # Missing cal/val/test keys fall back to .get(..., []); must not raise
        params = {"dataset": {"train_snap_dates": ["2025-01-31"]}}
        result = collect_dataset_snap_dates(params)
        assert result == [pd.Timestamp("2025-01-31")]


class TestPlanIncrementalSnapDates:
    """The (configured, existing, rebuild) -> (to_process, skipped) diff.

    Every assertion lands on the *returned lists*. Deliberately NOT asserted:
    log strings, and "the artifact was not written" — the latter is satisfied
    both by "correctly skipped" and by "never knew this month existed", so it
    has no discriminating power.
    """

    def test_returns_both_lists(self):
        # The skipped list is the observability contract: a pipeline that
        # decides to do less work must say what it decided not to do.
        plan = plan_incremental_snap_dates(
            configured=_ts("2026-01-31", "2026-02-28"),
            existing=_ts("2026-01-31"),
        )
        assert plan.to_process == _ts("2026-02-28")
        assert plan.skipped == _ts("2026-01-31")

    def test_plan_is_a_partition_of_configured(self):
        configured = _ts("2026-01-31", "2026-02-28", "2026-03-31")
        plan = plan_incremental_snap_dates(
            configured=configured,
            existing=_ts("2026-01-31"),
            rebuild=_ts("2026-02-28"),
        )
        assert set(plan.to_process) | set(plan.skipped) == set(configured)
        assert set(plan.to_process) & set(plan.skipped) == set()

    # --- degenerate inputs, one test each ---

    def test_configured_empty(self):
        plan = plan_incremental_snap_dates(
            configured=[], existing=_ts("2026-01-31"),
        )
        assert plan.to_process == []
        assert plan.skipped == []

    def test_existing_empty_processes_everything(self):
        configured = _ts("2026-01-31", "2026-02-28")
        plan = plan_incremental_snap_dates(configured=configured, existing=[])
        assert plan.to_process == configured
        assert plan.skipped == []

    def test_all_already_existing_processes_nothing(self):
        configured = _ts("2026-01-31", "2026-02-28")
        plan = plan_incremental_snap_dates(
            configured=configured, existing=configured,
        )
        assert plan.to_process == []
        assert plan.skipped == configured

    def test_rebuild_covering_all_configured_processes_everything(self):
        configured = _ts("2026-01-31", "2026-02-28")
        plan = plan_incremental_snap_dates(
            configured=configured, existing=configured, rebuild=configured,
        )
        assert plan.to_process == configured
        assert plan.skipped == []

    def test_rebuild_subset_of_existing_reprocesses_only_that_subset(self):
        configured = _ts("2026-01-31", "2026-02-28", "2026-03-31")
        plan = plan_incremental_snap_dates(
            configured=configured,
            existing=_ts("2026-01-31", "2026-02-28"),
            rebuild=_ts("2026-01-31"),
        )
        assert plan.to_process == _ts("2026-01-31", "2026-03-31")
        assert plan.skipped == _ts("2026-02-28")

    # --- normalisation / robustness ---

    def test_output_is_sorted_and_deduplicated(self):
        plan = plan_incremental_snap_dates(
            configured=["2026-03-31", "2026-01-31", "2026-01-31"],
            existing=[],
        )
        assert plan.to_process == _ts("2026-01-31", "2026-03-31")

    def test_mixed_input_types_compare_equal(self):
        # config yaml gives str, the metastore query gives str, callers may
        # already hold pd.Timestamp — all three must match each other.
        plan = plan_incremental_snap_dates(
            configured=["2026-01-31", pd.Timestamp("2026-02-28")],
            existing=[datetime.date(2026, 1, 31)],
        )
        assert plan.to_process == _ts("2026-02-28")
        assert plan.skipped == _ts("2026-01-31")

    def test_existing_outside_configured_is_ignored(self):
        # A month dropped from config leaves its partition behind; it must not
        # leak back into either list.
        plan = plan_incremental_snap_dates(
            configured=_ts("2026-02-28"),
            existing=_ts("2025-12-31", "2026-02-28"),
        )
        assert plan.to_process == []
        assert plan.skipped == _ts("2026-02-28")

    def test_rebuild_outside_configured_is_ignored(self):
        # The subset invariant is enforced up-front by consistency A21; the
        # pure helper stays total and simply cannot process an unconfigured
        # month.
        plan = plan_incremental_snap_dates(
            configured=_ts("2026-02-28"),
            existing=_ts("2026-02-28"),
            rebuild=_ts("2025-12-31"),
        )
        assert plan.to_process == []
        assert plan.skipped == _ts("2026-02-28")

    def test_rebuild_defaults_to_empty(self):
        plan = plan_incremental_snap_dates(
            configured=_ts("2026-01-31"), existing=_ts("2026-01-31"),
        )
        assert plan.to_process == []
        assert plan.skipped == _ts("2026-01-31")

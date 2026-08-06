"""Tests for the month-plan module (no Spark).

``build_month_plans`` is the single place that answers "which months is each
incremental artifact entitled to, and which of them have already landed". Two
layers are asserted here, in this order:

- the per-artifact wiring: the *configured* source differs per artifact (test
  tables read ``test_snap_dates``; ``preprocessed_feature_table`` reads the
  union of every split), and each artifact is gated on **its own** partition
  listing;
- the diff itself, ``plan_incremental_snap_dates``, plus the date-union helper
  ``collect_dataset_snap_dates`` — both moved here with the module they belong
  to (#169).

Every assertion lands on returned values. Deliberately not asserted: log lines,
and the catalog names (those belong to the pipeline-wiring tests).

This file needs no SparkSession, and that is load-bearing rather than
incidental: S2 pins ``month_plans.py`` to zero pyspark imports so these tests
never pay this repo's 2-4 minute Spark cold start.
"""

import datetime
import logging

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.month_plans import (
    INCREMENTAL_DATASETS,
    build_month_plans,
    collect_dataset_snap_dates,
    landed_months,
    month_plan_input,
    plan_incremental_snap_dates,
)


def _ts(*dates: str) -> list[pd.Timestamp]:
    return [pd.Timestamp(d) for d in dates]


#: train/cal/val/test are four *different* months each, so a plan that reads the
#: wrong config key cannot coincidentally produce the right list.
PARAMS = {
    "dataset": {
        "train_snap_dates": ["2026-01-31"],
        "calibration_snap_dates": ["2026-02-28"],
        "val_snap_dates": ["2026-03-31"],
        "test_snap_dates": ["2026-04-30", "2026-05-31"],
    }
}

ALL_MONTHS = _ts("2026-01-31", "2026-02-28", "2026-03-31", "2026-04-30", "2026-05-31")
TEST_MONTHS = _ts("2026-04-30", "2026-05-31")


class TestConfiguredSource:
    """Which configured months each artifact is entitled to."""

    def test_preprocessed_feature_table_takes_every_split(self):
        # It feeds train / train_dev / val / calibration / test alike, so its
        # months are the union — not the test months the other two use.
        plans = build_month_plans(PARAMS)
        assert plans["preprocessed_feature_table"].to_process == ALL_MONTHS

    def test_test_keys_takes_test_snap_dates_only(self):
        plans = build_month_plans(PARAMS)
        assert plans["test_keys"].to_process == TEST_MONTHS

    def test_test_model_input_takes_test_snap_dates_only(self):
        plans = build_month_plans(PARAMS)
        assert plans["test_model_input"].to_process == TEST_MONTHS

    def test_missing_train_snap_dates_raises(self):
        # collect_dataset_snap_dates is deliberately fail-loud on the one key
        # that is not optional; building the plans up front means the CLI hits
        # that error before it starts any Spark work rather than mid-pipeline.
        with pytest.raises(KeyError, match="train_snap_dates"):
            build_month_plans({"dataset": {"test_snap_dates": ["2026-04-30"]}})


class TestExistingListing:
    """Each artifact is gated on its own partitions, not on a shared answer."""

    def test_landed_months_are_skipped(self):
        plans = build_month_plans(
            PARAMS, existing={"test_keys": ["2026-04-30"]},
        )
        assert plans["test_keys"].to_process == _ts("2026-05-31")
        assert plans["test_keys"].skipped == _ts("2026-04-30")

    def test_one_artifacts_listing_does_not_silence_another(self):
        # test_keys and test_model_input are configured from the same key but
        # land independently: keys can be written and the model input not.
        plans = build_month_plans(
            PARAMS, existing={"test_keys": ["2026-04-30", "2026-05-31"]},
        )
        assert plans["test_keys"].to_process == []
        assert plans["test_model_input"].to_process == TEST_MONTHS
        assert plans["preprocessed_feature_table"].to_process == ALL_MONTHS

    def test_existing_defaults_to_nothing_has_landed(self):
        # The non-CLI entry point (tests, notebooks): one call, three plans,
        # every configured month processed.
        plans = build_month_plans(PARAMS)
        for name in INCREMENTAL_DATASETS:
            assert plans[name].skipped == []

    def test_absent_key_is_treated_as_nothing_landed(self):
        plans = build_month_plans(PARAMS, existing={"test_keys": ["2026-04-30"]})
        assert plans["test_model_input"].skipped == []


class TestRebuild:
    def test_rebuild_reprocesses_a_landed_month(self):
        plans = build_month_plans(
            PARAMS,
            existing={"test_model_input": ["2026-04-30", "2026-05-31"]},
            rebuild=["2026-04-30"],
        )
        assert plans["test_model_input"].to_process == _ts("2026-04-30")
        assert plans["test_model_input"].skipped == _ts("2026-05-31")

    def test_rebuild_applies_to_every_artifact_that_configures_the_month(self):
        # --rebuild-dates ⊆ test_snap_dates (A21), and those months are part of
        # preprocessed_feature_table's union too — re-encoding is the point.
        plans = build_month_plans(
            PARAMS,
            existing={
                "preprocessed_feature_table": [str(d.date()) for d in ALL_MONTHS],
                "test_keys": ["2026-04-30", "2026-05-31"],
            },
            rebuild=["2026-05-31"],
        )
        assert plans["preprocessed_feature_table"].to_process == _ts("2026-05-31")
        assert plans["test_keys"].to_process == _ts("2026-05-31")

    def test_rebuild_defaults_to_empty(self):
        plans = build_month_plans(
            PARAMS, existing={"test_keys": ["2026-04-30", "2026-05-31"]},
        )
        assert plans["test_keys"].to_process == []


class TestLandedMonths:
    """Reading a catalog dataset's partition specs into "which months landed".

    Not asserted here: that the specs are scoped to this run's
    ``base_dataset_version``, or that escaped values are decoded. Both are the
    catalog entry's ``partition_filter`` and ``HiveTableDataset``'s job, and
    duplicating them here is what this ticket deleted.
    """

    def test_reads_the_time_column_out_of_each_spec(self):
        assert landed_months(
            [{"snap_date": "2026-04-30"}, {"snap_date": "2026-05-31"}],
            time_col="snap_date", dataset_name="test_keys",
        ) == ["2026-04-30", "2026-05-31"]

    def test_multi_column_partitions_deduplicate_to_months(self):
        # test_model_input is partitioned by snap_date AND the item column, so
        # one month lists once per item; the question asked here is only which
        # months exist.
        assert landed_months(
            [
                {"snap_date": "2026-04-30", "prod_name": "fund_stock"},
                {"snap_date": "2026-04-30", "prod_name": "exchange_fx"},
            ],
            time_col="snap_date", dataset_name="test_model_input",
        ) == ["2026-04-30"]

    def test_hive_null_partition_sentinel_is_dropped_not_propagated(self, caplog):
        # Hive writes a NULL partition value as __HIVE_DEFAULT_PARTITION__.
        # Propagating it blows up later inside pd.Timestamp() with a message
        # that names neither the artifact nor the column. Dropping is the safe
        # direction: that month is then reprocessed rather than skipped.
        with caplog.at_level(logging.WARNING):
            assert landed_months(
                [
                    {"snap_date": "__HIVE_DEFAULT_PARTITION__"},
                    {"snap_date": "2026-04-30"},
                ],
                time_col="snap_date", dataset_name="test_keys",
            ) == ["2026-04-30"]
        # The warning has to name the value and the artifact: without them the
        # reader cannot tell which table needs fixing.
        assert "__HIVE_DEFAULT_PARTITION__" in caplog.text
        assert "test_keys" in caplog.text

    def test_a_dropped_month_is_replanned_end_to_end(self):
        # The guard is a decision, not a formatting detail: what it buys is that
        # the month comes back as to_process rather than skipped.
        existing = landed_months(
            [
                {"snap_date": "__HIVE_DEFAULT_PARTITION__"},
                {"snap_date": "2026-04-30"},
            ],
            time_col="snap_date", dataset_name="test_keys",
        )
        plans = build_month_plans(PARAMS, existing={"test_keys": existing})
        assert plans["test_keys"].to_process == _ts("2026-05-31")
        assert plans["test_keys"].skipped == _ts("2026-04-30")

    def test_honours_a_non_default_time_column(self):
        # schema.time is configurable; snap_date is this instantiation's name
        # for it, not the framework's.
        assert landed_months(
            [{"as_of_date": "2026-04-30"}],
            time_col="as_of_date", dataset_name="test_keys",
        ) == ["2026-04-30"]

    def test_specs_without_the_time_column_are_ignored(self):
        assert landed_months(
            [{"prod_name": "fund_stock"}], time_col="snap_date", dataset_name="t",
        ) == []

    def test_no_partitions_reads_as_nothing_landed(self):
        assert landed_months([], time_col="snap_date", dataset_name="test_keys") == []


class TestPlanSetShape:
    # Deliberately absent: `set(build_month_plans(...)) == set(INCREMENTAL_DATASETS)`.
    # INCREMENTAL_DATASETS is derived from the rule table and build_month_plans
    # loops over it, so that equality holds by construction and no edit can
    # break it — a green there would mean nothing. The property it used to
    # guard (a name registered without a rule) is now a KeyError at build time,
    # which test_every_artifact_has_a_usable_rule below exercises.

    def test_every_artifact_has_a_usable_rule(self):
        # Not vacuous: a rule entry that reads the wrong shape of `parameters`
        # raises here rather than silently yielding an empty plan.
        plans = build_month_plans(PARAMS)
        assert all(plans[name].to_process for name in INCREMENTAL_DATASETS)

    def test_the_authoritative_list_is_the_three_artifacts(self):
        assert set(INCREMENTAL_DATASETS) == {
            "preprocessed_feature_table", "test_keys", "test_model_input",
        }

    def test_catalog_name_is_the_artifact_name_plus_suffix(self):
        # The naming rule the pipeline definition and the CLI injection must
        # agree on; asserted once, here.
        assert month_plan_input("test_keys") == "test_keys_month_plan"
        assert month_plan_input("preprocessed_feature_table") == (
            "preprocessed_feature_table_month_plan"
        )


# =============================================================================
# Moved here with their module (#169): the (configured, existing, rebuild)
# diff and the date-union helper used to live in ``nodes_shared.py``, a
# module whose name promised nodes and held none.
# =============================================================================


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

"""Tests for the month-plan derivation rules (no Spark).

``build_month_plans`` is the single place that answers "which months is each
incremental artifact entitled to, and which of them have already landed". The
diff itself is ``plan_incremental_snap_dates`` and is tested next door in
``test_nodes_shared.py``; what is asserted here is the part that used to be
spread across four node bodies:

- the *configured* source differs per artifact (test tables read
  ``test_snap_dates``; ``preprocessed_feature_table`` reads the union of every
  split), and
- each artifact is gated on **its own** partition listing.

Every assertion lands on the returned plans. Deliberately not asserted: log
lines, and the catalog names (those belong to the pipeline-wiring tests).
"""

import logging

import pandas as pd
import pytest

from recsys_tfb.pipelines.dataset.month_plans import (
    INCREMENTAL_DATASETS,
    build_month_plans,
    landed_months,
    month_plan_input,
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
            "snap_date", "test_keys",
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
            "snap_date", "test_model_input",
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
                "snap_date", "test_keys",
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
            "snap_date", "test_keys",
        )
        plans = build_month_plans(PARAMS, existing={"test_keys": existing})
        assert plans["test_keys"].to_process == _ts("2026-05-31")
        assert plans["test_keys"].skipped == _ts("2026-04-30")

    def test_honours_a_non_default_time_column(self):
        # schema.time is configurable; snap_date is this instantiation's name
        # for it, not the framework's.
        assert landed_months(
            [{"as_of_date": "2026-04-30"}], "as_of_date", "test_keys",
        ) == ["2026-04-30"]

    def test_specs_without_the_time_column_are_ignored(self):
        assert landed_months([{"prod_name": "fund_stock"}], "snap_date", "t") == []

    def test_no_partitions_reads_as_nothing_landed(self):
        assert landed_months([], "snap_date", "test_keys") == []


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

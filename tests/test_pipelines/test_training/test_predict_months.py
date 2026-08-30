"""Tests for the test-month plan predict uses to decide what to re-write.

**No ``spark`` fixture and no ``pytest.mark.spark``**, which is the point: this
is the module that decides whether a month's predictions get written or skipped,
and a month skipped because it was never written looks exactly like a month
skipped because it was finished. That judgement needs tests that run in
milliseconds, so the module under test is kept free of pyspark — and of every
other project module, since that is how the dependency would leak back in — and
this file asserts both (:class:`TestModulePurity`). Same arrangement as
``tests/test_pipelines/test_inference/test_chunk_plans.py``.
"""

import ast
import logging
from pathlib import Path

from recsys_tfb.pipelines.training.steps import predict_months
from recsys_tfb.pipelines.training.steps.predict_months import (
    configured_months,
    month_dir,
    months_already_written,
    plan_predict_months,
    require_months_are_cached,
    warn_about_surplus_partitions,
    written_prediction_partitions,
)

import pytest

JAN, FEB = "2025-01-31", "2025-02-28"
JAN_KEY, FEB_KEY = "20250131", "20250228"
ITEMS = {"prod_A", "prod_B"}


class _FakeDataset:
    """Stands in for the catalog's HiveTableDataset, duck-typed like the real one."""

    def __init__(self, specs):
        self._specs = list(specs)

    def existing_partition_values(self):
        return self._specs


class _DatasetThatCannotList:
    """A catalog dataset type with no partition listing at all."""


class TestModulePurity:
    """Zero pyspark, including deferred imports inside function bodies.

    ``ast.walk`` rather than ``tree.body``: a deferred ``import pyspark`` inside
    a function is exactly the form that would otherwise read as "this module has
    no Spark dependency". No reachability hop is needed because the module
    imports nothing from the project at all — which this also pins, since a
    project import is how pyspark would arrive in one hop.

    This is the whole enforcement. The module purity register in the
    architecture audit is not extended to cover this file: that register is
    written to cover ``pipelines/dataset/`` and widening it to admit new code of
    our own is the move ``docs/agents/architecture-constraints.md`` warns
    against.
    """

    def _imported_roots(self):
        tree = ast.parse(Path(predict_months.__file__).read_text())
        roots = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                roots.add("." * node.level + module.split(".")[0])
        return roots

    def test_no_pyspark_import(self):
        assert "pyspark" not in {r.lstrip(".") for r in self._imported_roots()}

    def test_no_project_import(self):
        assert "recsys_tfb" not in {r.lstrip(".") for r in self._imported_roots()}


class TestMonthDir:
    def test_dashes_and_whitespace_are_stripped(self):
        assert month_dir("  2025-01-31 ") == JAN_KEY

    def test_two_spellings_of_one_day_stay_different_keys(self):
        """Normalises spelling, not calendar day.

        ``2025-1-31`` names the same day but not the same cache directory, so
        merging them here would point one config entry at a directory written
        for the other. Kept apart, it finds no cached rows and gets named by
        ``require_months_are_cached`` instead.
        """
        assert month_dir("2025-1-31") != month_dir("2025-01-31")

    def test_normalising_twice_changes_nothing(self):
        """resolve_cache_path applies it to values that may already be keys."""
        assert month_dir(month_dir(JAN)) == JAN_KEY


class TestConfiguredMonths:
    def test_repeats_of_one_month_collapse_to_one_entry(self):
        assert configured_months([JAN, JAN, FEB]) == {JAN_KEY: JAN, FEB_KEY: FEB}

    def test_the_configured_literal_is_carried_not_the_key(self):
        """Every message a person reads names the month the way they wrote it."""
        assert configured_months(["  2025-01-31 "])[JAN_KEY] == JAN

    def test_an_empty_config_plans_nothing(self):
        assert configured_months([]) == {}


class TestRequireMonthsAreCached:
    def test_a_configured_month_with_no_cached_rows_raises(self):
        """The alternative is silent: no cached items and no written items
        compare equal, so the month would read as complete, be skipped on every
        future run, and hand evaluation an empty report for it."""
        with pytest.raises(ValueError, match="no rows in the test cache"):
            require_months_are_cached({JAN_KEY: JAN}, {})

    def test_the_error_names_the_month_as_configured(self):
        with pytest.raises(ValueError, match=r"2025-01-31"):
            require_months_are_cached({JAN_KEY: JAN}, {FEB_KEY: ITEMS})

    def test_every_month_cached_is_silent(self):
        require_months_are_cached({JAN_KEY: JAN}, {JAN_KEY: ITEMS})


class TestMonthsAlreadyWritten:
    """Completeness is set equality — the decision the whole skip rests on."""

    def test_exactly_the_cached_items_is_complete(self):
        done = months_already_written(
            {JAN_KEY: JAN}, {JAN_KEY: ITEMS}, {JAN_KEY: set(ITEMS)}
        )
        assert done == {JAN_KEY}

    def test_a_half_written_month_is_not_complete(self):
        """The mutation target. Weaken the equality to "some partition exists"
        and this month reads as done: the run that died after prod_A would
        leave prod_B missing forever, with nothing to say so."""
        done = months_already_written(
            {JAN_KEY: JAN}, {JAN_KEY: ITEMS}, {JAN_KEY: {"prod_A"}}
        )
        assert done == set()

    def test_a_month_that_gained_an_item_is_not_complete(self):
        """The other half of the same equality: the written side was complete
        when it was written, and a new item in the cache has to reopen it."""
        done = months_already_written(
            {JAN_KEY: JAN},
            {JAN_KEY: ITEMS | {"prod_C"}},
            {JAN_KEY: set(ITEMS)},
        )
        assert done == set()

    def test_a_surplus_item_also_breaks_equality(self):
        done = months_already_written(
            {JAN_KEY: JAN}, {JAN_KEY: {"prod_A"}}, {JAN_KEY: ITEMS}
        )
        assert done == set()

    def test_a_month_never_written_is_not_complete(self):
        done = months_already_written({JAN_KEY: JAN}, {JAN_KEY: ITEMS}, {})
        assert done == set()

    def test_a_month_missing_from_the_cache_raises_rather_than_reads_complete(self):
        """``cache_items[key]``, not ``.get(key, set())``: ∅ == ∅ would be the
        one wrong answer that produces no error anywhere downstream."""
        with pytest.raises(KeyError):
            months_already_written({JAN_KEY: JAN}, {}, {})


class TestPlanPredictMonths:
    def test_a_complete_month_is_skipped_and_an_incomplete_one_is_processed(self):
        plan = plan_predict_months(
            {JAN_KEY: JAN, FEB_KEY: FEB}, done={JAN_KEY}, rebuild=set()
        )
        assert plan.to_process == [FEB]
        assert plan.skipped == [JAN]
        assert plan.rebuilt == []

    def test_rebuild_overrides_completeness(self):
        """--rebuild-dates is the only way to say "the inputs changed under an
        unchanged model_version", so it has to beat the completeness test rather
        than agree with it."""
        plan = plan_predict_months(
            {JAN_KEY: JAN}, done={JAN_KEY}, rebuild={JAN_KEY}
        )
        assert plan.to_process == [JAN]
        assert plan.rebuilt == [JAN]
        assert plan.skipped == []

    def test_a_rebuilt_month_is_listed_once_in_to_process(self):
        plan = plan_predict_months(
            {JAN_KEY: JAN}, done=set(), rebuild={JAN_KEY}
        )
        assert plan.to_process == [JAN]
        assert plan.rebuilt == [JAN]

    def test_processed_and_skipped_partition_the_configured_months(self):
        months = {JAN_KEY: JAN, FEB_KEY: FEB}
        plan = plan_predict_months(months, done={FEB_KEY}, rebuild=set())
        assert set(plan.to_process) & set(plan.skipped) == set()
        assert set(plan.to_process) | set(plan.skipped) == set(months.values())

    def test_months_come_out_in_chronological_order(self):
        """The manifest and the log read top to bottom; the YYYYMMDD key sorts
        chronologically, the configured order does not have to."""
        plan = plan_predict_months(
            {FEB_KEY: FEB, JAN_KEY: JAN}, done=set(), rebuild=set()
        )
        assert plan.to_process == [JAN, FEB]

    def test_a_rebuild_date_outside_the_config_plans_nothing(self):
        """--rebuild-dates is constrained to test_snap_dates at CLI entry (A21);
        if one ever got through, it must not invent a month."""
        plan = plan_predict_months(
            {JAN_KEY: JAN}, done={JAN_KEY}, rebuild={FEB_KEY}
        )
        assert plan.to_process == []
        assert plan.skipped == [JAN]


class TestSurplusWarning:
    def test_a_written_item_the_cache_no_longer_has_is_named(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_about_surplus_partitions(
                {JAN_KEY: JAN}, {JAN_KEY: {"prod_A"}}, {JAN_KEY: ITEMS}
            )
        assert "prod_B" in caplog.text
        assert JAN in caplog.text

    def test_a_renamed_item_is_caught_by_the_difference_not_a_superset_test(
        self, caplog
    ):
        """prod_B renamed to prod_C leaves a surplus *and* a missing partition,
        so neither set contains the other and a superset test sees nothing."""
        with caplog.at_level(logging.WARNING):
            warn_about_surplus_partitions(
                {JAN_KEY: JAN},
                {JAN_KEY: {"prod_A", "prod_C"}},
                {JAN_KEY: {"prod_A", "prod_B"}},
            )
        assert "prod_B" in caplog.text

    def test_a_complete_month_says_nothing(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_about_surplus_partitions(
                {JAN_KEY: JAN}, {JAN_KEY: ITEMS}, {JAN_KEY: set(ITEMS)}
            )
        assert caplog.text == ""

    def test_a_month_being_rebuilt_is_left_out(self, caplog):
        """The message's "will be re-predicted on every run" is not true of a
        month the operator asked to redo."""
        with caplog.at_level(logging.WARNING):
            warn_about_surplus_partitions(
                {JAN_KEY: JAN},
                {JAN_KEY: {"prod_A"}},
                {JAN_KEY: ITEMS},
                exclude={JAN_KEY},
            )
        assert caplog.text == ""


class TestWrittenPredictionPartitions:
    def test_partitions_are_grouped_by_month_key(self):
        written = written_prediction_partitions(
            _FakeDataset([
                {"snap_date": JAN, "prod_name": "prod_A"},
                {"snap_date": JAN, "prod_name": "prod_B"},
                {"snap_date": FEB, "prod_name": "prod_A"},
            ]),
            "snap_date", "prod_name",
        )
        assert written == {JAN_KEY: ITEMS, FEB_KEY: {"prod_A"}}

    def test_the_hive_partition_spelling_is_normalised_to_the_config_key(self):
        """Hive hands back ``20250131`` where the config says ``2025-01-31``;
        one of them has to be converted or no month ever matches."""
        written = written_prediction_partitions(
            _FakeDataset([{"snap_date": JAN_KEY, "prod_name": "prod_A"}]),
            "snap_date", "prod_name",
        )
        assert written == {JAN_KEY: {"prod_A"}}

    def test_a_dataset_that_cannot_list_reports_nothing_written(self, caplog):
        """Fails towards re-predicting (wasteful) rather than skipping
        (silently stale), and says so out loud."""
        with caplog.at_level(logging.WARNING):
            written = written_prediction_partitions(
                _DatasetThatCannotList(), "snap_date", "prod_name"
            )
        assert written == {}
        assert "cannot list partitions" in caplog.text

    def test_a_null_partition_is_dropped_and_announced(self, caplog):
        """Hive's NULL literal and the parquet side's reconstruction never
        match, so keeping it would leave the month permanently incomplete."""
        with caplog.at_level(logging.WARNING):
            written = written_prediction_partitions(
                _FakeDataset([
                    {"snap_date": "__HIVE_DEFAULT_PARTITION__",
                     "prod_name": "prod_A"},
                    {"snap_date": JAN, "prod_name": "prod_A"},
                ]),
                "snap_date", "prod_name",
            )
        assert written == {JAN_KEY: {"prod_A"}}
        assert "NULL" in caplog.text

    def test_a_partition_spec_missing_a_column_is_skipped(self):
        written = written_prediction_partitions(
            _FakeDataset([
                {"snap_date": JAN},
                {"prod_name": "prod_A"},
                {"snap_date": JAN, "prod_name": "prod_B"},
            ]),
            "snap_date", "prod_name",
        )
        assert written == {JAN_KEY: {"prod_B"}}

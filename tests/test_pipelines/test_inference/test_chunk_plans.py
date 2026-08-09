"""Tests for the scoring-chunk resume planner.

**No ``spark`` fixture and no ``pytest.mark.spark``**, which is the point: this
is the module that decides whether a chunk gets scored or skipped, and skipping
one that was never scored looks exactly like skipping a finished one. That
judgement needs tests that run in milliseconds, so the module under test is kept
free of pyspark and this file asserts that too
(:class:`TestModulePurity`). Same arrangement as
``tests/test_pipelines/test_dataset/test_month_plans.py``.
"""

import ast
from pathlib import Path

from recsys_tfb.pipelines.inference import chunk_plans
from recsys_tfb.pipelines.inference.chunk_plans import (
    HEALTHY_BUCKET_RANGE,
    ScoringChunk,
    plan_scoring_chunks,
)

import pytest

MONTH = "2025-12-31"
ITEMS = ["exchange_usd", "fund_stock"]


def _plan(**overrides):
    kwargs = {
        "snap_dates": [MONTH],
        "items": ITEMS,
        "n_buckets": 3,
        "written": (),
        "rebuild": (),
    }
    kwargs.update(overrides)
    return plan_scoring_chunks(**kwargs)


class TestFullGrid:
    def test_nothing_written_processes_every_chunk(self):
        plan = _plan()
        assert len(plan.to_process) == 3 * len(ITEMS)
        assert plan.skipped == ()
        assert plan.rebuilt == ()
        assert plan.surplus == ()

    def test_loop_order_is_bucket_outer_item_inner(self):
        """The one decision here whose mistake costs a factor of ``len(items)``.

        Iterating this tuple *is* the scoring loop, and the bucket has to be the
        outer key: one bucket's features cross into the driver once and the
        inner item loop reuses them. Item-outer is functionally identical and
        reads the whole population once per item (ADR-0010 section 4,
        decision 3), so the order is asserted rather than left to chance.
        """
        plan = _plan()
        assert list(plan.to_process) == [
            ScoringChunk(MONTH, 0, "exchange_usd"),
            ScoringChunk(MONTH, 0, "fund_stock"),
            ScoringChunk(MONTH, 1, "exchange_usd"),
            ScoringChunk(MONTH, 1, "fund_stock"),
            ScoringChunk(MONTH, 2, "exchange_usd"),
            ScoringChunk(MONTH, 2, "fund_stock"),
        ]

    def test_months_are_processed_in_configured_order(self):
        plan = _plan(snap_dates=["2025-11-30", MONTH], n_buckets=1)
        assert [chunk.snap_date for chunk in plan.to_process] == (
            ["2025-11-30"] * 2 + [MONTH] * 2
        )

    def test_duplicate_config_entries_do_not_duplicate_work(self):
        plan = _plan(snap_dates=[MONTH, MONTH], items=ITEMS + ITEMS, n_buckets=1)
        assert len(plan.to_process) == 2


class TestSkipping:
    def test_a_written_chunk_is_skipped_not_processed(self):
        written = [ScoringChunk(MONTH, 1, "fund_stock")]
        plan = _plan(written=written)
        assert plan.skipped == tuple(written)
        assert ScoringChunk(MONTH, 1, "fund_stock") not in plan.to_process
        assert len(plan.to_process) == 3 * len(ITEMS) - 1

    def test_plan_is_a_partition_of_the_grid(self):
        written = [ScoringChunk(MONTH, 0, "exchange_usd")]
        plan = _plan(written=written)
        assert set(plan.to_process) | set(plan.skipped) == {
            ScoringChunk(MONTH, bucket, item)
            for bucket in range(3)
            for item in ITEMS
        }
        assert not set(plan.to_process) & set(plan.skipped)

    def test_a_written_chunk_in_another_month_is_irrelevant(self):
        plan = _plan(written=[ScoringChunk("2025-11-30", 0, "fund_stock")])
        assert plan.skipped == ()
        assert len(plan.to_process) == 3 * len(ITEMS)

    def test_plain_tuples_are_accepted(self):
        """The caller builds these from ``existing_partition_values()`` dicts."""
        plan = _plan(written=[(MONTH, 2, "exchange_usd")])
        assert plan.skipped == (ScoringChunk(MONTH, 2, "exchange_usd"),)


class TestRebuild:
    def test_rebuild_reprocesses_a_written_month(self):
        written = [
            ScoringChunk(MONTH, bucket, item)
            for bucket in range(3)
            for item in ITEMS
        ]
        plan = _plan(written=written, rebuild=[MONTH])
        assert plan.skipped == ()
        assert len(plan.to_process) == 3 * len(ITEMS)

    def test_rebuilt_chunks_are_also_processed(self):
        """``rebuilt`` is a report, not a fourth disjoint bucket."""
        plan = _plan(rebuild=[MONTH], n_buckets=1)
        assert set(plan.rebuilt) == set(plan.to_process)

    def test_rebuild_does_not_reach_another_month(self):
        other = "2025-11-30"
        written = [ScoringChunk(other, 0, item) for item in ITEMS]
        plan = _plan(
            snap_dates=[other, MONTH], n_buckets=1,
            written=written, rebuild=[MONTH],
        )
        assert set(plan.skipped) == set(written)
        assert {chunk.snap_date for chunk in plan.rebuilt} == {MONTH}

    def test_rebuild_of_an_unwritten_month_is_still_reported(self):
        plan = _plan(rebuild=[MONTH], n_buckets=1)
        assert len(plan.rebuilt) == len(ITEMS)


class TestSurplus:
    """Stale partitions no future run will touch, and re-scoring cannot delete.

    A set difference against the grid, not a coverage count: lowering
    ``entity_buckets`` or renaming an item leaves partitions behind that keep
    contributing rows to this ``model_version``'s ranking. A "did we cover
    everything" count sees nothing wrong.
    """

    def test_a_bucket_beyond_the_configured_count_is_surplus(self):
        plan = _plan(n_buckets=2, written=[ScoringChunk(MONTH, 7, "fund_stock")])
        assert plan.surplus == (ScoringChunk(MONTH, 7, "fund_stock"),)
        assert plan.skipped == ()

    def test_an_unconfigured_item_is_surplus(self):
        plan = _plan(written=[ScoringChunk(MONTH, 0, "retired_product")])
        assert plan.surplus == (ScoringChunk(MONTH, 0, "retired_product"),)

    def test_another_months_partitions_are_not_surplus(self):
        """Other months legitimately have partitions; this run is not about them."""
        plan = _plan(written=[ScoringChunk("2025-11-30", 99, "retired_product")])
        assert plan.surplus == ()

    def test_surplus_is_warned_about(self, caplog):
        with caplog.at_level("WARNING"):
            _plan(n_buckets=2, written=[ScoringChunk(MONTH, 7, "fund_stock")])
        assert "Re-scoring cannot remove" in caplog.text


class TestEmptyGridRaises:
    """An empty plan would report a successful run that scored nobody."""

    def test_zero_buckets_raises(self):
        with pytest.raises(ValueError, match="entity_buckets must be at least 1"):
            _plan(n_buckets=0)

    def test_negative_buckets_raises(self):
        with pytest.raises(ValueError, match="entity_buckets must be at least 1"):
            _plan(n_buckets=-1)

    def test_no_snap_dates_raises(self):
        with pytest.raises(ValueError, match="snap_dates is empty"):
            _plan(snap_dates=[])

    def test_no_items_raises(self):
        with pytest.raises(ValueError, match="products is empty"):
            _plan(items=[])


class TestHealthyBucketWindow:
    def test_below_the_window_warns_but_proceeds(self, caplog):
        low, _ = HEALTHY_BUCKET_RANGE
        with caplog.at_level("WARNING"):
            plan = _plan(n_buckets=low - 1)
        assert "outside the healthy" in caplog.text
        assert len(plan.to_process) == (low - 1) * len(ITEMS)

    def test_above_the_window_warns_but_proceeds(self, caplog):
        _, high = HEALTHY_BUCKET_RANGE
        with caplog.at_level("WARNING"):
            plan = _plan(n_buckets=high + 1)
        assert "outside the healthy" in caplog.text
        assert len(plan.to_process) == (high + 1) * len(ITEMS)

    def test_inside_the_window_is_silent(self, caplog):
        low, high = HEALTHY_BUCKET_RANGE
        with caplog.at_level("WARNING"):
            _plan(n_buckets=(low + high) // 2)
        assert "outside the healthy" not in caplog.text


class TestModulePurity:
    """Zero pyspark, including deferred imports inside function bodies.

    ``ast.walk`` rather than ``tree.body``: a deferred ``import pyspark`` in a
    function body is the form that would otherwise read as "this module has no
    Spark dependency". No reachability hop is needed here (unlike S2's check on
    ``month_plans.py``) because this module imports nothing from the project at
    all — which this test also pins, since a project import is how the purity
    would leak in one hop.
    """

    def _imported_roots(self):
        tree = ast.parse(Path(chunk_plans.__file__).read_text())
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

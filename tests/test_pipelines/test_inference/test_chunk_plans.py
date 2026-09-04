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

from recsys_tfb.pipelines.inference.steps import chunk_plans
from recsys_tfb.pipelines.inference.steps.chunk_plans import (
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


class TestBuildChunkReport:
    """The on-disk record of what this run scored, skipped and redid.

    ``score_manifest`` holds those lists but is memory-only on purpose
    (``docs/pipelines/inference.md`` section 7.4), so after the process exits
    only the log's *counts* survive. This report is the copy that lands, and
    these tests pin the two things a count cannot answer: which chunks, and
    which month they were in.
    """

    MANIFEST = {
        "snap_dates": ["2025-11-30", "2025-12-31"],
        "items": ITEMS,
        "entity_buckets": 2,
        "model_version": "v1",
        "n_rows_written": 40,
        "chunks_processed": [["2025-12-31", 0, "fund_stock"]],
        "chunks_skipped": [
            ["2025-11-30", 0, "fund_stock"],
            ["2025-11-30", 1, "exchange_usd"],
        ],
        "chunks_rebuilt": [["2025-12-31", 0, "fund_stock"]],
        "chunks_empty": [["2025-12-31", 1, "exchange_usd"]],
        "expected_partitions": [],
        "written_partitions": [],
    }

    def _report(self, surplus=(), run_id="run-1"):
        return chunk_plans.build_chunk_report(self.MANIFEST, surplus, run_id)

    def test_the_report_names_the_run_that_wrote_it(self):
        """Which is what lets a reader tell it apart from the run before.

        The file sits in ``data/inference/<model_version>/<snap_date>/``, a
        directory every run of that model and month reuses. Without the stamp,
        a report left by an earlier run is indistinguishable from this one's.
        """
        assert self._report(run_id="run-7")["run_id"] == "run-7"

    def test_every_manifest_key_survives(self):
        report = self._report()
        for key, value in self.MANIFEST.items():
            assert report[key] == value

    def test_counts_are_the_list_lengths(self):
        assert self._report()["counts"] == {
            "processed": 1, "skipped": 2, "rebuilt": 1, "empty": 1, "surplus": 0,
        }

    def test_surplus_lands_instead_of_only_being_warned_about(self):
        """``plan.surplus`` reaches a ``logger.warning`` and nothing else.

        Partitions outside this run's grid keep contributing rows to the
        published ranking until someone drops them by hand, so "which ones"
        has to outlive the log.
        """
        stray = [ScoringChunk("2025-12-31", 9, "fund_stock")]
        report = self._report(surplus=stray)
        assert report["chunks_surplus"] == [["2025-12-31", 9, "fund_stock"]]
        assert report["counts"]["surplus"] == 1

    def test_by_snap_date_splits_the_chunks_by_month(self):
        assert self._report()["by_snap_date"]["2025-11-30"] == {
            "processed": 0, "skipped": 2, "rebuilt": 0, "empty": 0, "surplus": 0,
        }
        assert self._report()["by_snap_date"]["2025-12-31"] == {
            "processed": 1, "skipped": 0, "rebuilt": 1, "empty": 1, "surplus": 0,
        }

    def test_a_month_that_did_nothing_still_gets_a_row(self):
        """A configured month absent from every list is the interesting case.

        Dropping it would make "this month was entirely skipped" and "this
        month was never in the run" look identical in the report, which is the
        confusion the whole file exists to remove.
        """
        manifest = {
            **self.MANIFEST,
            "snap_dates": ["2025-10-31"] + self.MANIFEST["snap_dates"],
        }
        report = chunk_plans.build_chunk_report(manifest, (), "run-1")
        assert report["by_snap_date"]["2025-10-31"] == {
            "processed": 0, "skipped": 0, "rebuilt": 0, "empty": 0, "surplus": 0,
        }

    def test_adding_to_the_report_does_not_reach_the_manifest(self):
        """The two travel out of the same node and must not be one dict.

        Only the top level is copied — the chunk lists are shared, which is
        fine because neither side mutates them — so what this pins is that the
        report's own additions (``counts``, ``by_snap_date``, ``run_id``) stay
        out of the manifest ``validate_predictions`` reads.
        """
        report = self._report()
        report["counts"]["processed"] = 999
        assert "counts" not in self.MANIFEST
        assert "run_id" not in self.MANIFEST

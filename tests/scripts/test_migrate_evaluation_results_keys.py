"""Tests for the one-time evaluation_results.json key migration (#327).

What is worth asserting here is not "the three keys got renamed" — that much is
a dict comprehension — but the two ways this script can quietly ruin a file it
was pointed at: renaming a *time* key it must never touch, and picking a value
when both spellings are present.
"""
import json

import pytest

from recsys_tfb.evaluation.report_builder import _dataset_overview
from scripts.migrate_evaluation_results_keys import (
    RENAMES,
    apply_file,
    find_files,
    migrate_payload,
    plan_file,
)


def _legacy_overview() -> dict:
    """A pre-#327 ``dataset_overview``, every renameable key present."""
    return {
        "totals": {
            "n_rows": 100,
            "n_customers": 10,
            "n_products": 2,
            "n_snap_dates": 1,
            "n_positives": 20,
            "positive_rate": 0.2,
            "avg_positives_per_customer": 2.0,
        },
        "by_snap_date": {
            "2026-01-31": {
                "n_rows": 100, "n_positives": 20,
                "n_customers": 10, "positive_rate": 0.2,
            },
        },
        "by_item": {
            "A": {
                "n_rows": 50, "n_positives": 12,
                "n_customers": 10, "positive_rate": 0.24,
            },
        },
        "by_segment": {
            "mass": {
                "n_rows": 60, "n_positives": 14, "n_customers": 6,
                "positive_rate": 0.233, "n_queries": 6, "query_share": 0.6,
            },
        },
    }


def _legacy_payload() -> dict:
    return {
        "overall": {"map@1": 0.5},
        "dataset_overview": _legacy_overview(),
        "category": {
            "overall": {"map@1": 0.6},
            "dataset_overview": _legacy_overview(),
        },
    }


def test_every_renameable_key_moves_at_both_levels():
    out, renames, conflicts = migrate_payload(_legacy_payload())
    assert conflicts == []
    for overview in (out["dataset_overview"], out["category"]["dataset_overview"]):
        assert overview["totals"]["n_items"] == 2
        assert overview["totals"]["n_entities"] == 10
        assert overview["totals"]["avg_positives_per_entity"] == 2.0
        for group, expected in (
            ("by_snap_date", 10), ("by_item", 10), ("by_segment", 6),
        ):
            cell = next(iter(overview[group].values()))
            assert cell["n_entities"] == expected
            assert "n_customers" not in cell
    # 6 renames per level (3 in totals, 1 in each of the 3 cell groups), and
    # both levels are migrated -- a missed `category` bundle halves this.
    assert len(renames) == 12


def test_the_time_vocabulary_survives_untouched():
    """`snap_date` spellings are kept on purpose (ADR-0017).

    This is the one way a "tidying" edit to ``RENAMES`` corrupts every file the
    script has already touched, and nothing else in the suite would notice.
    """
    out, _, _ = migrate_payload(_legacy_payload())
    overview = out["dataset_overview"]
    assert "by_snap_date" in overview
    assert overview["totals"]["n_snap_dates"] == 1
    assert list(overview["by_snap_date"]) == ["2026-01-31"]
    assert not any("snap_date" in old for old in RENAMES)


def test_untouched_keys_keep_their_values_and_order():
    out, _, _ = migrate_payload(_legacy_payload())
    totals = out["dataset_overview"]["totals"]
    assert totals["n_rows"] == 100
    assert totals["positive_rate"] == 0.2
    assert list(totals) == [
        "n_rows", "n_entities", "n_items", "n_snap_dates",
        "n_positives", "positive_rate", "avg_positives_per_entity",
    ]


def test_running_twice_changes_nothing():
    once, _, _ = migrate_payload(_legacy_payload())
    twice, renames, conflicts = migrate_payload(once)
    assert renames == []
    assert conflicts == []
    assert twice == once


def test_a_file_carrying_both_spellings_is_reported_not_guessed():
    payload = _legacy_payload()
    payload["dataset_overview"]["totals"]["n_items"] = 999
    out, renames, conflicts = migrate_payload(payload)
    assert conflicts == ["dataset_overview.totals: both 'n_products' and 'n_items' present"]
    # The conflicted cell is left exactly as found — no half-migration.
    assert out["dataset_overview"]["totals"]["n_products"] == 2
    assert out["dataset_overview"]["totals"]["n_items"] == 999
    assert all(where != "dataset_overview.totals" for where, _, _ in renames)


def test_apply_rewrites_the_file_and_the_reader_then_accepts_it(tmp_path):
    """End to end: the refusal this script exists to clear actually clears.

    Asserting against ``_dataset_overview`` (the reader that raises) rather than
    against the JSON keeps the two sides from drifting into agreeing about
    different key names.
    """
    path = tmp_path / "evaluation_results.json"
    path.write_text(json.dumps(_legacy_payload()))

    before = json.loads(path.read_text())
    with pytest.raises(ValueError, match="predates the #327 key rename"):
        _dataset_overview(before)

    apply_file(path)

    after = json.loads(path.read_text())
    assert _dataset_overview(after)["totals"]["n_items"] == 2


def test_dry_run_leaves_the_file_alone(tmp_path):
    path = tmp_path / "evaluation_results.json"
    original = json.dumps(_legacy_payload())
    path.write_text(original)
    plan = plan_file(path)
    assert plan.renames
    assert path.read_text() == original


def test_find_files_takes_a_file_or_walks_a_tree(tmp_path):
    a = tmp_path / "v1" / "evaluation_results.json"
    b = tmp_path / "v2" / "evaluation_results.json"
    for p in (a, b):
        p.parent.mkdir(parents=True)
        p.write_text("{}")
    (tmp_path / "v1" / "predict_manifest.json").write_text("{}")

    assert find_files(a) == [a]
    assert find_files(tmp_path) == [a, b]

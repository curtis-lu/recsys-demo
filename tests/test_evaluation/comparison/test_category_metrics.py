"""大類指標 of the comparison report (``evaluation/comparison/
category_metrics.py``, #537): drawn like 各 item 指標 at the category grain,
only when categories are on and both sides have the grain. Inputs:
``fixtures.inputs`` (the real comparison and cut nodes)."""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.comparison import category_metrics
from tests.test_evaluation.comparison import fixtures as fx

TITLES = ["map_attr@K · 各大類", "per-item recall@K · 各大類"]


def _section(this, reference, p=None):
    p = p or fx.params(categories=True)
    return fx.read(category_metrics, fx.inputs(this, reference, p), p)


def test_absent_when_categories_are_off():
    p = fx.params(categories=False)
    assert _section(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                    fx.bundle(0.5, category=fx.category_bundle(0.4)), p) is None


def test_present_in_column_mode():
    """#379: categories read off a sample_pool column: the reader sees the
    same ``enabled`` switch and the same category grain."""
    p = fx.params(categories=True)
    p["evaluation"]["item_categories"] = {
        "enabled": True, "unmapped": "singleton", "column": "campaign_id"}
    sec = _section(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                   fx.bundle(0.5, category=fx.category_bundle(0.4)), p)
    assert [t.title for t in fx.tables(sec)] == TITLES


def test_absent_when_one_side_lacks_the_grain():
    """The comparison layer compares the category grain only when both
    sides have it."""
    assert _section(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                    fx.bundle(0.5)) is None


def test_a_comparison_landed_without_the_grain_is_refused():
    p = fx.params(categories=True)
    results = fx.inputs(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                        fx.bundle(0.5, category=fx.category_bundle(0.4)), p)
    del results["source_comparison"]["category"]
    with pytest.raises(ValueError, match="source_comparison has no category"):
        fx.read(category_metrics, results, p)


def test_tables_at_the_category_grain_s_own_k():
    """compare_k [1, 3, 5, "all"] with two categories: the category grain's
    "all" is @2, so the columns are @1 and @2 — not the item grain's."""
    p = fx.params(categories=True, compare_k=(1, 3, 5, "all"))
    this = fx.bundle(0.6, category=fx.category_bundle(0.5))
    reference = fx.bundle(0.5, category=fx.category_bundle(0.4))
    sec = _section(this, reference, p)
    assert [t.title for t in fx.tables(sec)] == TITLES
    for t in fx.tables(sec):
        assert t.row_header == "大類"
        assert [r.label for r in t.rows] == ["Macro 平均", *fx.CATEGORIES]
        assert [g for g, _ in fx.column_groups(t)] == ["@1"] * 3 + ["@2"] * 3
    a = this["category"]["per_item"]["cat_Y"]["map_attr@2"]
    b = reference["category"]["per_item"]["cat_Y"]["map_attr@2"]
    assert fx.row(fx.table(sec, TITLES[0]), "cat_Y").cells[3:] == \
        pytest.approx((a, b, a - b))


def test_the_category_grain_is_marked_by_its_own_annotations():
    """The category macro averages different categories while the item
    grain agrees: the category Macro row is ⚠ with its line, and the line
    names categories."""
    this = fx.bundle(0.6, category=fx.category_bundle(0.5))
    reference = fx.bundle(0.5, category=fx.category_bundle(
        0.4, macro_items=("cat_X",)))
    sec = _section(this, reference)
    for t in fx.tables(sec):
        assert fx.row(t, "Macro 平均").warn, t.title
        assert not any(c.warn for c in t.columns)
    assert ("Macro 平均：這次 2 個 item、對照 1 個（對照少 cat_Y），平均的 item "
            "不同。") in fx.notes(sec)


def test_the_section_declares_its_heading():
    sec = _section(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                   fx.bundle(0.5, category=fx.category_bundle(0.4)))
    assert (sec.title, sec.title_en, sec.key) == (
        "大類指標", "Per category", "category_metrics")
    assert sec.question.endswith("？")

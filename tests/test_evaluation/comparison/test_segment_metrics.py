"""分群指標 of the comparison report (``evaluation/comparison/
segment_metrics.py``, #537): empty list by default, so no section; with a
list, 「X@K · 整體與各分群」 per entry; not drawn when the two sides' segment
columns differ (decision 16 item 15). Inputs: ``fixtures.inputs`` (the real
comparison and cut nodes)."""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.comparison import report, segment_metrics
from recsys_tfb.evaluation.metric_catalog import QUERY_GROUP, CompareEntry
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT
from tests.test_evaluation.comparison import fixtures as fx

MAP = CompareEntry("map", QUERY_GROUP, "per_segment")


@pytest.fixture
def listed(monkeypatch):
    monkeypatch.setattr(segment_metrics, "COMPARED", (MAP,))


def _section(this, reference, p=None):
    p = p or fx.params()
    return fx.read(segment_metrics, fx.inputs(this, reference, p), p)


def test_by_default_the_section_is_not_on_the_page():
    assert segment_metrics.COMPARED == ()
    p = fx.params()
    results = fx.inputs(fx.bundle(0.6), fx.bundle(0.5), p)
    assert fx.read(segment_metrics, results, p) is None
    assert "s-segment_metrics" not in fx.section_ids(
        report.assemble_comparison_report(results, p))


def test_with_a_list_the_whole_and_each_segment(listed):
    this = fx.bundle(0.6, segments=(UNMATCHED_SEGMENT, *fx.SEGMENTS))
    reference = fx.bundle(0.5, segments=(UNMATCHED_SEGMENT, *fx.SEGMENTS))
    sec = _section(this, reference)
    (t,) = fx.tables(sec)
    assert t.title == "map@K · 整體與各分群"
    assert t.row_header == "分群"
    # 整體 pinned first; the unmatched rows are not a segment, so last.
    assert [r.label for r in t.rows] == ["整體", *fx.SEGMENTS, UNMATCHED_SEGMENT]
    assert [r.pinned for r in t.rows] == [True, False, False, False]
    assert [g for g, _ in fx.column_groups(t)] == ["@1"] * 3 + ["@3"] * 3
    a, b = this["overall"]["map@3"], reference["overall"]["map@3"]
    assert fx.row(t, "整體").cells[3:] == pytest.approx((a, b, a - b))
    a, b = this["per_segment"]["seg_2"]["map@1"], reference["per_segment"]["seg_2"]["map@1"]
    assert fx.row(t, "seg_2").cells[:3] == pytest.approx((a, b, a - b))


def test_not_drawn_when_the_segment_columns_differ(listed):
    """The compared side computed without segments: the table would have one
    side only, so neither it nor the section is drawn; the 「能不能比」
    table says so (test_comparability_marks)."""
    reference = fx.bundle(0.5, segment_columns=())
    assert _section(fx.bundle(0.6), reference) is None
    p = fx.params()
    html = report.assemble_comparison_report(
        fx.inputs(fx.bundle(0.6), reference, p), p)
    assert "s-segment_metrics" not in fx.section_ids(html)
    assert "整體與各分群" not in html


def test_a_list_reading_another_grain_is_refused(monkeypatch):
    monkeypatch.setattr(segment_metrics, "COMPARED", (
        CompareEntry("map", QUERY_GROUP, "overall"),))
    with pytest.raises(ValueError, match="draws grain 'per_segment'"):
        _section(fx.bundle(0.6), fx.bundle(0.5))


def test_the_section_declares_its_heading(listed):
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    assert (sec.title, sec.title_en, sec.key) == (
        "分群指標", "Per segment", "segment_metrics")
    assert sec.question.endswith("？")

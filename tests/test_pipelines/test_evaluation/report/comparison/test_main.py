"""The comparison report's page (``report/comparison/main.py``, #537):
its five sections in the order ADR-0031 decision 16 item 4 names, the page
title and the metadata. Inputs: ``fixtures.inputs`` (the real comparison and
cut nodes)."""
from __future__ import annotations

import pytest

from recsys_tfb.pipelines.evaluation.report.comparison import main, segment_metrics
from recsys_tfb.evaluation.metric_catalog import QUERY_GROUP, CompareEntry
from tests.test_pipelines.test_evaluation.report.comparison import fixtures as fx

#: Decision 16 item 4: the comparison report's sections, in page order.
SPEC_TITLES = ["概覽", "整體指標", "各 item 指標", "大類指標", "分群指標"]
SPEC_IDS = ["s-overview", "s-overall_metrics", "s-per_item_metrics",
            "s-category_metrics", "s-segment_metrics"]


@pytest.fixture
def every_section(monkeypatch):
    """Inputs and parameters under which every section is drawn: categories
    on, and a list for the segment section (empty by default)."""
    monkeypatch.setattr(segment_metrics, "COMPARED",
                        (CompareEntry("map", QUERY_GROUP, "per_segment"),))
    p = fx.params(categories=True)
    results = fx.inputs(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                        fx.bundle(0.5, category=fx.category_bundle(0.4)), p)
    return results, p


def test_the_five_sections_in_order(every_section):
    results, p = every_section
    html = main.assemble_comparison_report(results, p)
    assert [m.TITLE for m in main.SECTIONS] == SPEC_TITLES
    assert fx.section_ids(html) == SPEC_IDS
    for sid, title in zip(SPEC_IDS, SPEC_TITLES):
        assert f'<li><a href="#{sid}">{title}</a>' in html, sid


def test_no_glossary_section():
    p = fx.params()
    html = main.assemble_comparison_report(
        fx.inputs(fx.bundle(0.6), fx.bundle(0.5), p), p)
    assert "詞彙" not in html and "Glossary" not in html


def test_page_title_and_metadata_name_both_sides():
    """The page is titled in Chinese with the English beside it; the
    metadata names this run by its model_version and the compared source by
    its label, both escaped, then the evaluated dates and the time drawn."""
    p = fx.params()
    p["evaluation"]["compare"]["label"] = "<對照> & 'B'"
    html = main.assemble_comparison_report(
        fx.inputs(fx.bundle(0.6), fx.bundle(0.5), p), p)
    assert ('<h1>模型比較報表<span class="en">Model Comparison Report</span>'
            '</h1>') in html
    assert "<title>模型比較報表</title>" in html
    assert f"<tr><th>這次</th><td>{fx.THIS_MODEL_VERSION}</td></tr>" in html
    assert "<tr><th>對照</th><td>&lt;對照&gt; &amp; 'B'</td></tr>" in html
    assert "<tr><th>評估日期</th><td>2026-01-31</td></tr>" in html
    assert "<tr><th>產生時間</th><td>" in html


def test_several_evaluated_dates_read_as_a_range():
    """#374: several evaluated dates read as earliest ~ latest plus a count."""
    p = fx.params()
    p["evaluation"]["snap_date"] = ["2026-01-31", "2026-03-31", "2026-02-28"]
    html = main.assemble_comparison_report(
        fx.inputs(fx.bundle(0.6), fx.bundle(0.5), p), p)
    assert ("<tr><th>評估日期</th><td>2026-01-31 ~ 2026-03-31（3 個日期）"
            "</td></tr>") in html


def test_a_section_gets_only_what_it_declares(monkeypatch):
    """Each section is handed its own READS, so a read it did not declare
    fails rather than drifting from the declaration."""
    from recsys_tfb.pipelines.evaluation.report.comparison import overall_metrics

    monkeypatch.setattr(overall_metrics, "READS", ())
    p = fx.params()
    with pytest.raises(KeyError, match="source_comparison"):
        main.assemble_comparison_report(
            fx.inputs(fx.bundle(0.6), fx.bundle(0.5), p), p)

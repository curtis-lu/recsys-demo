"""The report shell's #537 look (``report/shell.py``): the table of
contents (title — question), the English beside each heading, escaping every
string (#366 item 1), and nothing collapsed in a section drawn from content
blocks, and a section without a key refused."""
from __future__ import annotations

import pytest

from recsys_tfb.pipelines.evaluation.report.shell import generate_html_report
from recsys_tfb.report.types import (
    Column,
    Paragraph,
    ReportSection,
    Row,
    Table,
)


def _blocks_section(**kw) -> ReportSection:
    base = dict(title="排序指標", description="", title_en="Ranking metrics",
                question="模型在各粒度排得怎樣？", key="ranking_metrics",
                blocks=[Paragraph("一段話"),
                        Table("map@K · 整體", "分群", (Column("@1", "ratio"),),
                              (Row("整體", (0.5,), pinned=True),))])
    base.update(kw)
    return ReportSection(**base)


def _toc(html: str) -> str:
    return html[html.index('<nav class="toc">'):html.index("</nav>")]


def test_toc_lists_each_section_title_and_question():
    html = generate_html_report([
        _blocks_section(),
        _blocks_section(title="次段", question="", key="second"),
    ])
    toc = _toc(html)
    assert ('<li><a href="#s-ranking_metrics">排序指標</a>'
            '<span class="q"> — 模型在各粒度排得怎樣？</span></li>') in toc
    # a section without a question has no dash
    assert '<li><a href="#s-second">次段</a></li>' in toc
    assert 'id="s-ranking_metrics"' in html and 'id="s-second"' in html


def test_h2_carries_the_english_title():
    html = generate_html_report([_blocks_section()], title="模型評估報表",
                                title_en="Model Evaluation Report")
    assert '<h2>排序指標<span class="en">Ranking metrics</span></h2>' in html
    assert ('<h1>模型評估報表<span class="en">Model Evaluation Report</span>'
            '</h1>') in html


def test_metadata_keys_and_values_are_escaped():
    html = generate_html_report([_blocks_section()],
                                metadata={"<k>": "a < b & <i>c</i>"})
    assert "<tr><th>&lt;k&gt;</th><td>a &lt; b &amp; &lt;i&gt;c&lt;/i&gt;</td></tr>" in html
    assert "<i>c</i>" not in html


def test_titles_questions_and_descriptions_are_escaped():
    html = generate_html_report(
        [_blocks_section(title="<b>段</b>", question="<i>問</i>？",
                         title_en="<u>en</u>"),
         _blocks_section(title="舊", key="old", description="k < R & <b>x</b>")],
        title="<script>報表</script>")
    for raw in ("<b>段</b>", "<i>問</i>", "<u>en</u>", "<script>報表",
                "<b>x</b>"):
        assert raw not in html, raw
    assert "<title>&lt;script&gt;報表&lt;/script&gt;</title>" in html
    assert '<p class="description">k &lt; R &amp; &lt;b&gt;x&lt;/b&gt;</p>' in html
    assert "<h2>&lt;b&gt;段&lt;/b&gt;" in html
    assert '<span class="q"> — &lt;i&gt;問&lt;/i&gt;？</span>' in html


def test_a_blocks_only_section_draws_no_details():
    html = generate_html_report([_blocks_section()])
    section = html[html.index('id="s-ranking_metrics"'):html.index("</main>")]
    assert "<details" not in section
    assert '<div class="section" id="s-ranking_metrics">' in html
    assert '<div class="tblk">' in section          # the blocks were drawn


def test_a_section_without_a_key_is_refused():
    with pytest.raises(ValueError, match="has no key"):
        generate_html_report([_blocks_section(key="")])


def test_two_sections_with_one_key_are_refused():
    with pytest.raises(ValueError, match=r"share a key: \['ranking_metrics'\]"):
        generate_html_report([_blocks_section(), _blocks_section()])


def test_the_page_loads_nothing_from_the_network():
    """No network in production: every script is inline."""
    import re

    html = generate_html_report([_blocks_section()])
    assert re.findall(r"<script[^>]*\bsrc=", html) == []
    assert re.findall(r"<link[^>]*\bhref=", html) == []

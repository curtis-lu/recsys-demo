"""Tests for evaluation.report module."""

import json
import re
import tempfile
from pathlib import Path

import plotly.graph_objects as go
import pytest

from recsys_tfb.evaluation.report import (
    ReportSection,
    generate_html_report,
    save_metrics_json,
    save_report,
)
from recsys_tfb.report.types import (
    Bullets,
    Column,
    Figure,
    Formula,
    Heading,
    Row,
    Table,
)


def _make_figure():
    fig = go.Figure(data=go.Bar(x=["a", "b"], y=[1, 2]))
    fig.update_layout(title="Test Chart")
    return fig


def _figure(title="圖的數"):
    """A figure block with its data table (every figure has one)."""
    return Figure(_make_figure(), Table(title, "item", (Column("值", "count"),),
                                        (Row("a", (1,)), Row("b", (2,)))))


def _sec(key="s", title="S", **kw):
    return ReportSection(title=title, description=kw.pop("description", "d"),
                         key=key, **kw)


class TestGenerateHtmlReport:
    def test_self_contained(self):
        sections = [_sec(title="Metrics", blocks=[_figure()])]
        html = generate_html_report(sections)
        assert "plotly" in html.lower()
        assert "<html>" in html
        assert "</html>" in html

    def test_all_sections_present(self):
        sections = [
            _sec("a", "Section A", description="Desc A"),
            _sec("b", "Section B", description="Desc B"),
        ]
        html = generate_html_report(sections)
        assert "Section A" in html
        assert "Section B" in html

    def test_formula_block_rendered(self):
        sections = [_sec(blocks=[Formula("Δ = mAP(F − offset) − mAP(F)")])]
        html = generate_html_report(sections)
        assert 'class="formula"' in html
        assert "Δ = mAP(F − offset) − mAP(F)" in html

    def test_bullets_block_rendered(self):
        sections = [_sec(blocks=[Bullets(("第一則", "第二則"))])]
        html = generate_html_report(sections)
        assert "<li>第一則</li>" in html
        assert "<li>第二則</li>" in html

    def test_bullets_are_escaped(self):
        sections = [_sec(blocks=[Bullets(("<script>alert(1)</script>",))])]
        html = generate_html_report(sections)
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html

    def test_blocks_are_drawn_in_order(self):
        """公式要排在圖前面：讀者先知道「這個數字怎麼算的」才看得懂圖。"""
        sections = [_sec(blocks=[
            Formula("F = x + y"), Bullets(("一則重點",)),
            _figure()])]
        html = generate_html_report(sections)
        body = html[html.index("</nav>"):]
        assert (body.index("F = x + y") < body.index("一則重點")
                < body.index("plotly-graph-div"))

    def test_a_formula_pulls_in_no_external_script(self):
        """生產限制是 no network——公式用 Unicode 純文字畫。驗的是頁面沒有任何
        外部載入的 ``<script src>``／``<link href>``（plotly.js 內嵌，自己就
        提到 MathJax，所以不能驗「沒有 mathjax 字樣」）。"""
        sections = [_sec(blocks=[Formula("Σ Δⱼ ≠ Δ")])]
        html = generate_html_report(sections)
        assert "Σ Δⱼ ≠ Δ" in html, "premise: the formula was drawn"
        assert re.findall(r"<script[^>]*\bsrc=[^>]*>", html) == []
        assert re.findall(r"<link[^>]*\bhref=[^>]*>", html) == []

    def test_a_section_without_blocks_draws_no_block_markup(self):
        html = generate_html_report([_sec()])
        body = html[html.index("</nav>"):]
        assert 'class="formula"' not in body
        assert 'class="section-bullets"' not in body
        assert 'class="tblk"' not in body

    def test_metadata_table(self):
        sections = [_sec(title="Test", description="Test")]
        metadata = {"Model Version": "abc12345", "Snap Date": "2024-03-31"}
        html = generate_html_report(sections, metadata=metadata)
        assert "abc12345" in html
        assert "2024-03-31" in html

    def test_figures_and_tables(self):
        table = Table("t", "product", (Column("map", "ratio"),),
                      (Row("exchange_fx", (0.5,)), Row("fund_bond", (0.3,))))
        sections = [_sec(title="Mixed", blocks=[
            _figure(), _figure(), table])]
        html = generate_html_report(sections)
        assert "exchange_fx" in html
        assert "fund_bond" in html

    def test_heading_blocks_rendered(self):
        sections = [_sec(title="Summary", blocks=[
            Heading("Overall"), Heading("Macro Average")])]
        html = generate_html_report(sections)
        assert "<h3>Overall</h3>" in html
        assert "<h3>Macro Average</h3>" in html

    def test_no_heading_without_heading_block(self):
        html = generate_html_report([_sec(blocks=[Formula("x")])])
        assert "<h3>" not in html

    def test_only_a_figure_s_data_table_collapses(self):
        """全篇只有圖的資料表收起（擁有者 2026-10-24，#537）：圖正下方一個
        ``<details class="fig-data">``，沒有 ``open``，裡面是同一種表
        （``div.tblk``，表格工具照樣認得）；一般表格不收合，也沒有開收合的 JS。"""
        shown = Table("一般表", "item", (Column("map", "ratio"),),
                      (Row("a", (0.5,)),))
        html = generate_html_report([_sec(blocks=[shown, _figure("圖的數")])])
        body = html[html.index("</nav>"):]
        assert re.findall(r"<details\b[^>]*>", body) == [
            '<details class="fig-data">']
        start = body.index('<details class="fig-data">')
        fig_data = body[start:body.index("</details>", start)]
        assert '<summary>數字表</summary><div class="tblk">' in fig_data
        assert "圖的數" in fig_data and "一般表" not in fig_data
        assert body.index("plotly-graph-div") < start   # under its figure
        # closed: an open <details> swallows every section after it
        assert html.count("<details") == html.count("</details>") == 1
        assert ".open = true" not in html and ".open=true" not in html

    def test_a_figure_without_a_data_table_is_refused(self):
        with pytest.raises(TypeError, match="Figure.data"):
            Figure(_make_figure(), None)

    def test_no_dead_metrics_table_class(self):
        sections = [_sec(blocks=[Table("t", "r", (Column("a", "count"),),
                                       (Row("x", (1,)),))])]
        html = generate_html_report(sections)
        assert 'class="metrics-table"' not in html
        assert 'class="dataframe metrics-table"' not in html

    def test_nav_renders_as_vertical_list(self):
        """TOC nav should be a <ul> of <li> anchors, not inline links —
        9+ sections need a scannable list."""
        sections = [_sec(f"k{i}", f"S{i}") for i in range(3)]
        html = generate_html_report(sections)
        assert '<nav class="toc">' in html
        assert html.count('<li><a href="#s-k') >= 3

    def test_back_to_top_button_present(self):
        """Floating Back-to-Top button + JS visibility handler + smooth scroll."""
        html = generate_html_report([_sec()])
        assert 'id="to-top"' in html
        assert "scrollY" in html
        assert "300" in html
        assert "scrollTo" in html
        assert "smooth" in html


class TestSaveReport:
    def test_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = save_report("<html>test</html>", tmpdir)
            assert path.exists()
            assert path.name == "report.html"
            assert path.read_text() == "<html>test</html>"

    def test_creates_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = Path(tmpdir) / "a" / "b" / "c"
            path = save_report("<html>test</html>", nested)
            assert path.exists()


class TestSaveMetricsJson:
    def test_json_roundtrip(self):
        metrics = {
            "overall": {"map": 0.5, "recall": 0.6},
            "per_item": {"exchange_fx": {"recall@5": 0.7}},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = save_metrics_json(metrics, tmpdir)
            assert path.exists()
            loaded = json.loads(path.read_text())
            assert loaded == metrics

    def test_creates_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = Path(tmpdir) / "x" / "y"
            path = save_metrics_json({"a": 1}, nested)
            assert path.exists()

"""Tests for evaluation.report module."""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.report import (
    ReportSection,
    _fmt_no_sci,
    _render_table,
    generate_html_report,
    save_metrics_json,
    save_report,
)


def _make_figure():
    fig = go.Figure(data=go.Bar(x=["a", "b"], y=[1, 2]))
    fig.update_layout(title="Test Chart")
    return fig


class TestGenerateHtmlReport:
    def test_self_contained(self):
        sections = [
            ReportSection(
                title="Metrics",
                description="Overall metrics",
                figures=[_make_figure()],
                tables=[pd.DataFrame({"metric": ["map"], "value": [0.5]})],
            )
        ]
        html = generate_html_report(sections)
        assert "plotly" in html.lower()
        assert "<html>" in html
        assert "</html>" in html

    def test_all_sections_present(self):
        sections = [
            ReportSection(title="Section A", description="Desc A"),
            ReportSection(title="Section B", description="Desc B"),
        ]
        html = generate_html_report(sections)
        assert "Section A" in html
        assert "Section B" in html

    def test_formula_rendered(self):
        """主報表也要渲染 ``formula``。

        只改診斷頁那個渲染器的話，``ReportSection`` 就多了一個「某個渲染器
        會默默丟掉」的欄位——之後有人在主報表的 section 加公式會靜默消失，
        而那種 bug 沒有任何錯誤訊息。
        """
        sections = [ReportSection(title="S", description="d",
                                  formula="Δ = mAP(F − offset) − mAP(F)")]
        html = generate_html_report(sections)
        assert 'class="formula"' in html
        assert "Δ = mAP(F − offset) − mAP(F)" in html

    def test_bullets_rendered(self):
        sections = [ReportSection(title="S", description="d",
                                  bullets=["第一則", "第二則"])]
        html = generate_html_report(sections)
        assert "<li>第一則</li>" in html
        assert "<li>第二則</li>" in html

    def test_bullets_are_escaped(self):
        sections = [ReportSection(title="S", description="d",
                                  bullets=["<script>alert(1)</script>"])]
        html = generate_html_report(sections)
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html

    def test_empty_formula_and_bullets_render_nothing(self):
        """既有 13 個 ``build_*_section`` 的輸出必須不受影響。

        ``<ul>`` 在主報表被目錄用掉了，所以只數 section 區塊之後的部分；
        比對渲染出的**標籤**而不是字串 "formula"，避免命中 CSS 裡的樣式定義。
        """
        sections = [ReportSection(title="S", description="d")]
        html = generate_html_report(sections)
        body = html[html.index("</nav>"):]
        assert "<ul>" not in body
        assert 'class="formula"' not in body
        assert 'class="section-bullets"' not in body

    def test_metadata_table(self):
        sections = [ReportSection(title="Test", description="Test")]
        metadata = {"Model Version": "abc12345", "Snap Date": "2024-03-31"}
        html = generate_html_report(sections, metadata=metadata)
        assert "abc12345" in html
        assert "2024-03-31" in html

    def test_figures_and_tables(self):
        fig = _make_figure()
        table = pd.DataFrame({"product": ["exchange_fx", "fund_bond"], "map": [0.5, 0.3]})
        sections = [
            ReportSection(
                title="Mixed",
                description="Both figures and tables",
                figures=[fig, fig],
                tables=[table],
            )
        ]
        html = generate_html_report(sections)
        assert "exchange_fx" in html
        assert "fund_bond" in html

    def test_table_titles_rendered(self):
        t1 = pd.DataFrame({"metric": ["map"], "value": [0.5]})
        t2 = pd.DataFrame({"metric": ["ndcg"], "value": [0.6]})
        t3 = pd.DataFrame({"metric": ["mrr"], "value": [0.7]})
        sections = [
            ReportSection(
                title="Summary",
                description="With subtitles",
                tables=[t1, t2, t3],
                table_titles=["Overall", "Macro Average", "Micro Average"],
            )
        ]
        html = generate_html_report(sections)
        assert "<h3>Overall</h3>" in html
        assert "<h3>Macro Average</h3>" in html
        assert "<h3>Micro Average</h3>" in html

    def test_table_titles_empty_by_default(self):
        t1 = pd.DataFrame({"metric": ["map"], "value": [0.5]})
        sections = [
            ReportSection(
                title="No Titles",
                description="No table_titles provided",
                tables=[t1],
            )
        ]
        html = generate_html_report(sections)
        assert "<h3>" not in html

    def test_collapsible_section_uses_details(self):
        sections = [ReportSection(title="Diag", description="d",
                                  collapsible=True)]
        html = generate_html_report(sections)
        assert "<details" in html
        assert "<summary>Diag</summary>" in html

    def test_non_collapsible_has_no_details(self):
        sections = [ReportSection(title="Main", description="d")]
        html = generate_html_report(sections)
        assert "<details" not in html

    def test_no_dead_metrics_table_class(self):
        sections = [ReportSection(title="T", description="d",
                                  tables=[pd.DataFrame({"a": [1]})])]
        html = generate_html_report(sections)
        assert 'class="metrics-table"' not in html
        assert 'class="dataframe metrics-table"' not in html

    def test_nav_renders_as_vertical_list(self):
        """TOC nav should be a <ul> of <li> anchors, not inline links —
        9+ sections need a scannable list."""
        sections = [
            ReportSection(title=f"S{i}", description="d") for i in range(3)
        ]
        html = generate_html_report(sections)
        # nav element exists with the TOC class
        assert '<nav class="toc">' in html
        # each section gets a <li> entry inside the nav
        assert html.count('<li><a href="#section-') >= 3

    def test_back_to_top_button_present(self):
        """Floating Back-to-Top button + JS visibility handler + smooth scroll."""
        sections = [ReportSection(title="S", description="d")]
        html = generate_html_report(sections)
        # Button element exists
        assert 'id="to-top"' in html
        # JS hides button until scrolled (>300px convention)
        assert "scrollY" in html
        assert "300" in html
        # JS triggers smooth scroll to top on click
        assert "scrollTo" in html
        assert "smooth" in html

    def test_collapsible_section_opens_when_nav_clicked(self):
        """Anchor-jumping into a closed <details> should auto-open it
        (otherwise nav link looks broken on collapsible sections)."""
        sections = [
            ReportSection(title="Diag", description="d", collapsible=True),
        ]
        html = generate_html_report(sections)
        # JS handler exists that sets .open = true on <details> target
        assert ".open = true" in html or ".open=true" in html


class TestFmtNoSci:
    """Number -> string with no scientific notation (report tables)."""

    def test_integer_valued_float_uses_thousands_separator(self):
        assert _fmt_no_sci(12345678.0) == "12,345,678"

    def test_small_value_is_fixed_point_not_scientific(self):
        assert _fmt_no_sci(3.4e-5) == "0.000034"

    def test_strips_trailing_zeros(self):
        assert _fmt_no_sci(0.5) == "0.5"
        assert _fmt_no_sci(0.123456789) == "0.123457"

    def test_tiny_value_collapses_to_zero_without_negative_sign(self):
        assert _fmt_no_sci(-1.5e-7) == "0"
        assert _fmt_no_sci(5e-8) == "0"

    def test_negative_value_kept(self):
        assert _fmt_no_sci(-0.0234) == "-0.0234"


class TestRenderTable:
    """_render_table formats every cell so no column flips to sci notation,
    big ints get thousands separators, and NaN/None render blank."""

    def _mixed_df(self):
        # A float column whose range (1e8 .. 3e-5) would flip the WHOLE
        # column to scientific notation under pandas' default to_html.
        return pd.DataFrame(
            {
                "rate": [0.000034, None, 0.5, 120000000.0],
                "count": [120000000, 22, 3, 7],
                "prod": ["exchange_fx", "fund_bond", "x", "y"],
            },
            index=["A", "B", "C", "D"],
        )

    def test_no_scientific_notation(self):
        html = _render_table(self._mixed_df())
        for bad in ("e-05", "e+08", "e-0", "e+0", "5.000000e-01"):
            assert bad not in html, f"found sci-notation token {bad!r}"

    def test_formatted_values_present(self):
        html = _render_table(self._mixed_df())
        assert "0.000034" in html
        assert "120,000,000" in html  # both the float and the int column

    def test_int_column_gets_thousands_separator(self):
        html = _render_table(self._mixed_df())
        # the raw, separator-less integer must not survive
        assert ">120000000<" not in html

    def test_nan_and_none_render_blank(self):
        html = _render_table(self._mixed_df())
        assert "NaN" not in html
        assert "<td></td>" in html

    def test_string_cells_pass_through(self):
        html = _render_table(self._mixed_df())
        assert "exchange_fx" in html
        assert "fund_bond" in html


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
            "overall": {"map": 0.5, "ndcg": 0.6},
            "per_item": {"exchange_fx": {"hit_rate@5": 0.7}},
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

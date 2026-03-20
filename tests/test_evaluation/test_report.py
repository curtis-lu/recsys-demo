"""Tests for evaluation.report module."""

import json
import tempfile
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.report import (
    ReportSection,
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
            "per_product": {"exchange_fx": {"map": 0.7}},
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

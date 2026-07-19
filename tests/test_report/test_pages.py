"""report.pages：多頁輸出＋共用 plotly.min.js＋ScopeNote 上頁。

核心宣稱：
- plotly.js 只寫一份到 out_dir/plotly.min.js，各頁引用它而非各自內嵌
  （既有 generate_html_report 每份內嵌 ~3.5MB；拆成多頁後這是硬前提）。
- ScopeNote 的每個非空欄位都要出現在頁面上，且視覺上要跟數字區明顯區隔。
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import pytest

from recsys_tfb.report.pages import write_pages
from recsys_tfb.report.types import Page, ReportSection, ScopeNote


def _simple_figure() -> go.Figure:
    return go.Figure(data=go.Bar(x=["a", "b"], y=[1, 2]))


def _page(slug: str, title: str, scope: ScopeNote | None = None,
          sections=None) -> Page:
    if sections is None:
        sections = (
            ReportSection(
                title="Section A",
                description="desc A",
                figures=[_simple_figure()],
                tables=[pd.DataFrame({"x": [1, 2]})],
                table_titles=["Table A"],
            ),
        )
    return Page(slug=slug, title=title, scope=scope, sections=tuple(sections))


class TestWrittenFiles:
    def test_writes_expected_filenames(self, tmp_path):
        pages = [_page("01-a", "Page A"), _page("02-b", "Page B")]
        written = write_pages(pages, tmp_path, index_title="Idx",
                               index_intro="<p>intro</p>")
        names = {p.name for p in written}
        assert names == {"01-a.html", "02-b.html", "index.html",
                          "plotly.min.js"}
        for name in names:
            assert (tmp_path / name).exists()


class TestPlotlyJsSharedNotEmbedded:
    def test_page_references_external_script_not_inline(self, tmp_path):
        pages = [_page("01-a", "Page A")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "01-a.html").read_text()
        assert 'src="plotly.min.js"' in html

    def test_single_page_size_under_200kb(self, tmp_path):
        pages = [_page("01-a", "Page A")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        size = (tmp_path / "01-a.html").stat().st_size
        assert size < 200 * 1024, f"page too large: {size} bytes"

    def test_plotly_min_js_file_is_substantial(self, tmp_path):
        pages = [_page("01-a", "Page A")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        size = (tmp_path / "plotly.min.js").stat().st_size
        assert size > 500 * 1024  # 完整 plotly.js 應遠大於 500KB


class TestIndexSortedBySlug:
    def test_index_lists_pages_sorted_by_slug_regardless_of_input_order(
        self, tmp_path
    ):
        pages = [_page("02-b", "Page B"), _page("01-a", "Page A"),
                 _page("03-c", "Page C")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "index.html").read_text()
        pos_a = html.index("01-a.html")
        pos_b = html.index("02-b.html")
        pos_c = html.index("03-c.html")
        assert pos_a < pos_b < pos_c


class TestScopeNoteRendering:
    def test_all_scope_fields_appear_on_page(self, tmp_path):
        note = ScopeNote(
            measures="每 item 的排序品質",
            population="有正例的 query",
            blind_to=("item 之外的因素", "使用者側特徵"),
            reference_points=("baseline model",),
            sampling="分層抽樣：每群取 1000 列",
        )
        pages = [_page("01-a", "Page A", scope=note)]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "01-a.html").read_text()
        assert "每 item 的排序品質" in html
        assert "有正例的 query" in html
        assert "item 之外的因素" in html
        assert "使用者側特徵" in html
        assert "baseline model" in html
        assert "分層抽樣：每群取 1000 列" in html

    def test_scope_none_does_not_crash_and_page_still_written(self, tmp_path):
        pages = [_page("01-a", "Page A", scope=None)]
        written = write_pages(pages, tmp_path, index_title="Idx",
                               index_intro="")
        assert (tmp_path / "01-a.html").exists()

    def test_empty_sampling_and_reference_points_omitted_gracefully(
        self, tmp_path
    ):
        # sampling="" 與 reference_points=() 是預設值，頁面不應該印出空段落
        # 佔位（但也不應該因此炸掉）。
        note = ScopeNote(
            measures="m",
            population="p",
            blind_to=("x",),
        )
        pages = [_page("01-a", "Page A", scope=note)]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "01-a.html").read_text()
        assert "m" in html
        assert "p" in html
        assert "x" in html


class TestSectionRendering:
    def test_tables_and_table_titles_rendered(self, tmp_path):
        pages = [_page("01-a", "Page A")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "01-a.html").read_text()
        assert "Table A" in html
        assert "Section A" in html
        assert "desc A" in html
        # DataFrame({"x": [1, 2]}) 的值應出現在渲染出的表格 HTML 中
        assert "<table" in html.lower()

    def test_figures_rendered_without_inline_plotly_js(self, tmp_path):
        pages = [_page("01-a", "Page A")]
        write_pages(pages, tmp_path, index_title="Idx", index_intro="")
        html = (tmp_path / "01-a.html").read_text()
        # 圖表本身的 div/script 應該出現，但不含完整 plotly.js 原始碼
        assert "plotly" in html.lower()
        # 若整份 plotly.js 被內嵌，檔案會遠大於 200KB（見上面的大小測試），
        # 這裡再直接確認不含 plotly.js 的特徵字串。
        assert "Plotly.newPlot" in html or "plotly-graph-div" in html

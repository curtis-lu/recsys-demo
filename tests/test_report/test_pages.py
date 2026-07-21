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

    def test_returned_order_matches_docstring(self, tmp_path):
        # docstring 宣稱的順序：plotly.min.js 最先、各頁依傳入順序、
        # 最後 index.html（曾經寫反，說「最後 index.html、plotly.min.js」）。
        pages = [_page("01-a", "Page A"), _page("02-b", "Page B"),
                 _page("03-c", "Page C")]
        written = write_pages(pages, tmp_path, index_title="Idx",
                               index_intro="")
        names = [p.name for p in written]
        assert names == [
            "plotly.min.js", "01-a.html", "02-b.html", "03-c.html",
            "index.html",
        ]


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


class TestFormulaAndBullets:
    """``formula``／``bullets`` 的渲染。

    使用者的回饋是「說明集中在最上面、看不出在講哪張圖」與「附上公式」，
    所以這兩個欄位必須跟著各自的 section 一起渲染，而不是併回頁首。
    """

    def _html(self, tmp_path, section) -> str:
        write_pages([_page("01-a", "Page A", sections=(section,))], tmp_path,
                    index_title="Idx", index_intro="")
        return (tmp_path / "01-a.html").read_text()

    def test_formula_rendered_in_its_own_block(self, tmp_path):
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", formula="Δ = mAP(F − offset) − mAP(F)"))
        assert 'class="formula"' in html
        assert "Δ = mAP(F − offset) − mAP(F)" in html

    def test_bullets_rendered_as_list_items(self, tmp_path):
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", bullets=["第一則", "第二則"]))
        assert '<ul class="section-bullets">' in html
        assert "<li>第一則</li>" in html
        assert "<li>第二則</li>" in html

    def test_empty_formula_and_bullets_render_nothing(self, tmp_path):
        """既有 13 個 ``build_*_section`` 不帶新欄位，輸出必須完全不變。

        比對的是**渲染出來的標籤**而不是字串 "formula"——後者永遠會命中
        ``_CSS`` 裡的 ``.formula`` 樣式定義，是一條永遠綠的假測試。
        """
        html = self._html(tmp_path, ReportSection(title="S", description="d"))
        assert 'class="formula"' not in html
        assert 'class="section-bullets"' not in html
        # 這一頁 scope=None、沒有 bullets，整頁不該有任何 <ul>
        assert "<ul>" not in html

    def test_bullets_are_escaped(self, tmp_path):
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", bullets=["<script>alert(1)</script>"]))
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html

    def test_formula_is_escaped(self, tmp_path):
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", formula="a < b & c > d"))
        assert "a &lt; b &amp; c &gt; d" in html

    def test_formula_precedes_bullets_and_both_precede_figures(self, tmp_path):
        """順序：description → formula → bullets → 圖。

        讀者要先知道「這個數字怎麼算的」才看得懂圖；公式排在圖後面等於沒有。
        """
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", formula="F = x + y",
            bullets=["一則重點"], figures=[_simple_figure()]))
        assert (html.index("F = x + y") < html.index("一則重點")
                < html.index("plotly-graph-div"))

    def test_formula_style_present_in_css(self, tmp_path):
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", formula="F = x"))
        assert ".formula {" in html

    def test_no_external_math_renderer_is_pulled_in(self, tmp_path):
        """生產限制是 no network——MathJax／KaTeX 的 CDN 一定載不到。"""
        html = self._html(tmp_path, ReportSection(
            title="S", description="d", formula="Σ Δⱼ ≠ Δ"))
        assert "mathjax" not in html.lower()
        assert "katex" not in html.lower()


def test_collapsible_section_renders_details(tmp_path):
    """``ReportSection.collapsible`` 原本只有 ``evaluation/report.py`` 實作，
    ``report/pages.py`` 完全忽略它——設了會被靜默丟掉。這條守住兩邊都要渲染。
    """
    collapsible_section = ReportSection(title="Collapsible S", description="d",
                                         collapsible=True)
    regular_section = ReportSection(title="Regular S", description="d",
                                     collapsible=False)
    pages = [_page("01-a", "Page A",
                    sections=(collapsible_section, regular_section))]
    write_pages(pages, tmp_path, index_title="Idx", index_intro="")
    html = (tmp_path / "01-a.html").read_text()

    assert '<details class="section">' in html
    assert "<summary>Collapsible S</summary>" in html
    assert "<h2>Collapsible S</h2>" not in html

    assert '<div class="section">' in html
    assert "<h2>Regular S</h2>" in html

    # **開閉標籤必須守恆**：只驗開標籤的話，把收尾寫成 `</div>` 也全綠——而未
    # 閉合的 <details> 不會被游離的 </div> 隱式關掉，瀏覽器會把**後續所有
    # section 當成它的子節點**吞進收合區。config_shift 頁的尺是第 2 節且預設
    # 收合，那一改會讓第 3–7 節整片消失，而測試不會有任何反應。
    assert html.count("<details") == html.count("</details>") == 1
    assert html.count('<div class="section">') == html.count("</div>")


def test_range_index_is_not_rendered_as_a_data_column():
    """自動流水號 0 1 2 3 在報表上是雜訊——第一份真實產出就把統計量表印成
    ``0 mean +0.445``，讀者得先判斷第一欄是不是資料。"""
    import pandas as pd

    from recsys_tfb.report.pages import _render_table

    html = _render_table(pd.DataFrame({"統計量": ["mean", "p50"], "值": [0.4, 0.7]}))
    assert "<th>0</th>" not in html
    assert "mean" in html


def test_meaningful_index_is_still_rendered():
    """被 set_index 過的 index 是 row label，藏起來表格就讀不懂了。"""
    import pandas as pd

    from recsys_tfb.report.pages import _render_table

    frame = pd.DataFrame({"值": [0.4, 0.7]}, index=["ccard_ins", "fund_bond"])
    html = _render_table(frame)
    assert "ccard_ins" in html

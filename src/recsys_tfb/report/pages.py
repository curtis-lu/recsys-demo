"""多頁 HTML 輸出：共用一份 plotly.min.js，每頁把 ScopeNote 放在數字前面。

既有的 ``generate_html_report``（``evaluation/report.py``）把整份 plotly.js
內嵌進單一 HTML（約 3.5MB）。診斷重構要拆成多份頁面（每份診斷一頁），若
各自內嵌就是 N×3.5MB——`src/recsys_tfb/diagnosis/hpo/render.py` 已經示範過
「dir 內共用一份」的做法（用 ``include_plotlyjs="directory"``），這裡採同
樣精神：把 ``plotly.offline.get_plotlyjs()`` 只寫一次到
``out_dir/plotly.min.js``，各頁用 ``<script src="plotly.min.js">`` 引用、
圖表本身用 ``fig.to_html(full_html=False, include_plotlyjs=False)``。

ScopeNote 區塊刻意跟數字區用不同的視覺樣式（左側色條＋淺色底），這是整個
重構的核心要求：每個數字自帶「量的是什麼／算在哪批列上／看不見什麼」，
不是可以略過的裝飾。
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.offline

from recsys_tfb.report.types import Page, ScopeNote

_PLOTLY_JS_FILENAME = "plotly.min.js"

_CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 40px; color: #222; }
h1 { color: #222; }
h2 { color: #444; border-bottom: 1px solid #ddd; padding-bottom: 8px; margin-top: 32px; }
h3 { color: #555; }
table { border-collapse: collapse; margin: 16px 0; }
th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
th { background: #f5f5f5; }
tr:nth-child(even) { background: #fafafa; }
.section { margin-bottom: 40px; }
.description { color: #666; margin-bottom: 16px; }
.index-list { line-height: 1.9; }
.index-intro { margin-bottom: 24px; }
.scope-note {
  background: #fff8e6;
  border-left: 4px solid #d97706;
  padding: 12px 20px;
  margin: 16px 0 28px;
  border-radius: 2px;
}
.scope-note h3 {
  margin-top: 0;
  color: #92400e;
  font-size: 0.95em;
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
.scope-note dl { margin: 8px 0; }
.scope-note dt { font-weight: 600; color: #78350f; margin-top: 8px; }
.scope-note dd { margin: 2px 0 2px 0; color: #444; }
.scope-note ul { margin: 4px 0; padding-left: 20px; }
"""


def _escape(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _fmt_cell(x: Any) -> Any:
    if x is None or x is pd.NA:
        return ""
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, (int, np.integer)):
        return f"{int(x):,}"
    if isinstance(x, (float, np.floating)):
        if math.isnan(x):
            return ""
        return f"{float(x):,.6f}".rstrip("0").rstrip(".")
    return x


def _render_table(table: pd.DataFrame) -> str:
    return table.applymap(_fmt_cell).to_html(index=True)


def _render_scope_note(scope: ScopeNote) -> str:
    parts = ['<div class="scope-note">']
    parts.append("<h3>範圍說明 / Scope</h3>")
    parts.append("<dl>")
    parts.append(f"<dt>量的是什麼</dt><dd>{_escape(scope.measures)}</dd>")
    parts.append(f"<dt>算在哪批列上</dt><dd>{_escape(scope.population)}</dd>")
    if scope.sampling:
        parts.append(f"<dt>抽樣設計</dt><dd>{_escape(scope.sampling)}</dd>")
    parts.append("<dt>看不見什麼</dt><dd><ul>")
    for item in scope.blind_to:
        parts.append(f"<li>{_escape(item)}</li>")
    parts.append("</ul></dd>")
    if scope.reference_points:
        parts.append("<dt>對照點</dt><dd><ul>")
        for item in scope.reference_points:
            parts.append(f"<li>{_escape(item)}</li>")
        parts.append("</ul></dd>")
    parts.append("</dl>")
    parts.append("</div>")
    return "\n".join(parts)


def _render_page_html(page: Page) -> str:
    parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        '<meta charset="utf-8">',
        f"<title>{_escape(page.title)}</title>",
        f"<style>{_CSS}</style>",
        f'<script src="{_PLOTLY_JS_FILENAME}"></script>',
        "</head><body>",
        f"<h1>{_escape(page.title)}</h1>",
    ]

    if page.scope is not None:
        parts.append(_render_scope_note(page.scope))

    for section in page.sections:
        parts.append('<div class="section">')
        parts.append(f"<h2>{_escape(section.title)}</h2>")
        parts.append(f'<p class="description">{_escape(section.description)}</p>')

        for fig in section.figures:
            parts.append(fig.to_html(full_html=False, include_plotlyjs=False))

        for i, table in enumerate(section.tables):
            if i < len(section.table_titles) and section.table_titles[i]:
                parts.append(f"<h3>{_escape(section.table_titles[i])}</h3>")
            parts.append(_render_table(table))

        parts.append("</div>")

    parts.append("</body></html>")
    return "\n".join(parts)


def _render_index_html(pages: list[Page], index_title: str,
                        index_intro: str) -> str:
    """``index_title`` 會被 escape；``index_intro`` 不會——這是刻意的不對稱。

    ``index_intro`` 的設計意圖就是放一段 HTML 片段（例如診斷首頁的邏輯架構
    說明表格），所以直接注入、不 escape。呼叫端若要放使用者輸入或任何不信任
    的字串進 ``index_intro``，**必須自己先 escape**——這裡不會替你做。
    """
    sorted_pages = sorted(pages, key=lambda p: p.slug)
    parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        '<meta charset="utf-8">',
        f"<title>{_escape(index_title)}</title>",
        f"<style>{_CSS}</style>",
        "</head><body>",
        f"<h1>{_escape(index_title)}</h1>",
        f'<div class="index-intro">{index_intro}</div>',
        '<ul class="index-list">',
    ]
    for page in sorted_pages:
        parts.append(
            f'<li><a href="{page.slug}.html">{_escape(page.title)}</a></li>'
        )
    parts.append("</ul>")
    parts.append("</body></html>")
    return "\n".join(parts)


def write_pages(
    pages: list[Page],
    out_dir: str | Path,
    index_title: str,
    index_intro: str,
) -> list[Path]:
    """把多個 ``Page`` 寫成 ``out_dir`` 下的多份 HTML＋一份共用 plotly.min.js。

    回傳實際寫出的檔案路徑（順序：``plotly.min.js`` 最先、各頁依傳入順序、
    最後 ``index.html``）。

    Args:
        index_intro: **raw HTML 片段**，直接注入 index 頁、不 escape（跟
            ``index_title`` 不對稱——``index_title`` 會 escape）。設計意圖
            就是放結構化說明（例如邏輯架構表格），呼叫端若放使用者輸入或
            其他不信任字串，必須自己先 escape。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    js_path = out_dir / _PLOTLY_JS_FILENAME
    js_path.write_text(plotly.offline.get_plotlyjs())
    written.append(js_path)

    for page in pages:
        page_path = out_dir / f"{page.slug}.html"
        page_path.write_text(_render_page_html(page))
        written.append(page_path)

    index_path = out_dir / "index.html"
    index_path.write_text(_render_index_html(pages, index_title, index_intro))
    written.append(index_path)

    return written

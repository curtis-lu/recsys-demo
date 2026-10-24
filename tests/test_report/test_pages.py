"""report.pages：內容塊的 HTML（表與數字格式見 ``test_table.py``）。"""
from __future__ import annotations

from recsys_tfb.report.pages import render_block
from recsys_tfb.report.types import Formula


def test_formula_is_escaped():
    html = render_block(Formula("a < b & c > d"))
    assert "a &lt; b &amp; c &gt; d" in html

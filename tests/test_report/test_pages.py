"""report.pages：主報表外殼（``evaluation/report.py``）用的表格與段落附加欄位。

這裡原本是多頁輸出（共用 plotly.min.js、ScopeNote 上頁）的測試；#532 拿掉
全部診斷頁，多頁外殼跟著拿掉。formula／bullets 經外殼畫出來的樣子在
``tests/test_evaluation/test_report.py``，這裡只留直接測這兩個 helper 才測得到的。
"""
from __future__ import annotations

from recsys_tfb.report.pages import render_section_extras
from recsys_tfb.report.types import ReportSection


def test_formula_is_escaped():
    parts = render_section_extras(ReportSection(
        title="S", description="d", formula="a < b & c > d"))
    assert "a &lt; b &amp; c &gt; d" in "".join(parts)


def test_range_index_is_not_rendered_as_a_data_column():
    """自動流水號 0 1 2 3 在報表上是雜訊——第一份真實產出就把統計量表印成
    ``0 mean +0.445``，讀者得先判斷第一欄是不是資料。"""
    import pandas as pd

    from recsys_tfb.report.pages import render_table

    html = render_table(pd.DataFrame({"統計量": ["mean", "p50"], "值": [0.4, 0.7]}))
    assert "<th>0</th>" not in html
    assert "mean" in html


def test_meaningful_index_is_still_rendered():
    """被 set_index 過的 index 是 row label，藏起來表格就讀不懂了。"""
    import pandas as pd

    from recsys_tfb.report.pages import render_table

    frame = pd.DataFrame({"值": [0.4, 0.7]}, index=["ccard_ins", "fund_bond"])
    html = render_table(frame)
    assert "ccard_ins" in html


def test_a_negative_that_rounds_to_zero_renders_as_zero():
    """A delta of ``-1e-9`` prints as ``0``, never ``-0``.

    Goes through ``render_table`` rather than the cell formatter: it is the
    one path every report table takes.
    """
    import pandas as pd

    from recsys_tfb.report.pages import render_table

    html = render_table(pd.DataFrame({"Δ": [-1e-9]}))
    assert "<td>0</td>" in html
    assert "-0" not in html

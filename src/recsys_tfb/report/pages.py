"""報表表格與段落附加欄位的 HTML：主報表外殼（``evaluation/report.py``）用它。

這裡原本也是多頁輸出（診斷頁共用一份 plotly.min.js、每頁把 ScopeNote 放在
數字前面）；#532 拿掉全部診斷頁之後，多頁外殼跟著拿掉，只留主報表還在用的
兩個 helper：``render_table`` 與 ``render_section_extras``。
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _escape(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _fmt_no_sci(x: float) -> str:
    """Format a float for report tables without scientific notation.

    Integer-valued -> thousands-separated integer (``12,345,678``); otherwise
    fixed 6-decimal with trailing zeros stripped (``0.000034``, ``0.5``).
    Magnitudes below display precision collapse to ``"0"`` and never render as
    ``"-0"``.
    """
    x = float(x)
    if x.is_integer():
        return f"{int(x):,}"
    s = f"{x:,.6f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "-", "") else s


def _fmt_cell(x: Any) -> Any:
    """Per-cell formatter for report tables. Numbers go through the
    no-scientific-notation rules (big ints also get thousands separators),
    NaN/None render blank, and non-numeric cells (strings, lists) pass through
    unchanged."""
    if x is None or x is pd.NA:
        return ""
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, (int, np.integer)):
        return f"{int(x):,}"
    if isinstance(x, (float, np.floating)):
        if math.isnan(x):
            return ""
        return _fmt_no_sci(float(x))
    return x


def _show_index(table: pd.DataFrame) -> bool:
    """預設 ``RangeIndex`` 不顯示，其餘顯示。

    ``0 1 2 3`` 這種自動流水號在報表上是純雜訊——讀者會花一秒判斷它是不是
    一欄資料（實測第一份真實產出就發生了：統計量表印成
    ``0 mean +0.445``）。反過來，被 ``set_index`` 成 item 名稱或客群名稱的
    index 是那張表的 row label，藏起來會讓表格讀不懂。

    用 index 的**型別**而不是 config 開關來判斷：呼叫端有沒有刻意設定 index，
    這件事它自己就表達完了，多一個參數只是讓每個呼叫端重講一次。
    """
    return not isinstance(table.index, pd.RangeIndex)


def render_table(table: pd.DataFrame) -> str:
    """Render a DataFrame to HTML with no scientific notation.

    pandas formats a whole float column uniformly, so one extreme value flips
    the entire column to exponential. Formatting every cell up front sidesteps
    that (and keeps object-dtype columns working too). ``applymap`` is used
    because ``DataFrame.map`` does not exist on pandas 1.5.x.

    The main report and the comparison report both render through
    ``evaluation/report.py``, which calls this.
    """
    return table.applymap(_fmt_cell).to_html(index=_show_index(table))


def render_section_extras(section) -> list[str]:
    """``formula`` 與 ``bullets`` 的 HTML；兩者皆空時回空 list。

    回空 list 而不是空字串是刻意的：呼叫端 ``extend`` 之後**一個標籤都不會多**，
    所以既有 13 個不帶新欄位的 ``build_*_section`` 輸出逐位元不變。
    """
    parts: list[str] = []
    if section.formula:
        parts.append(f'<div class="formula">{_escape(section.formula)}</div>')
    if section.bullets:
        parts.append('<ul class="section-bullets">')
        parts.extend(f"<li>{_escape(b)}</li>" for b in section.bullets)
        parts.append("</ul>")
    return parts

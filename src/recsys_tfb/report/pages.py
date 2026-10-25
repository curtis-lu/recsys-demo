"""報表表格與段落內容的 HTML：主報表與比較報表的外殼（``pipelines/evaluation/report/shell.py``）
用它。

這裡原本也是多頁輸出（診斷頁共用一份 plotly.min.js、每頁把 ScopeNote 放在
數字前面）；#532 拿掉全部診斷頁之後，多頁外殼跟著拿掉。

:func:`render_block` / :func:`render_report_table` —— 段落的內容塊
（``report.types`` 的 ``Heading`` … ``Note``）與 ``Table``。數字格式在
:func:`format_cell`。所有字串都跳脫（#366 第 1 項）。
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np

from recsys_tfb.report.types import (
    Bullets,
    Cell,
    Column,
    Definitions,
    Figure,
    Formula,
    Heading,
    Note,
    Paragraph,
    Row,
    Table,
)


def escape(text: Any) -> str:
    """Text content: ``&`` ``<`` ``>`` escaped (#366 item 1). Every string the
    report shell puts between tags goes through this."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def escape_attr(text: Any) -> str:
    """An attribute value inside double quotes: :func:`escape` plus ``"``."""
    return escape(text).replace('"', "&quot;")


# ---------------------------------------------------------------------------
# #537：內容塊與新表
# ---------------------------------------------------------------------------

#: 負數的負號（U+2212），畫面上用；``data-v`` 用半形 ``-``，貼進 Excel 才是數字。
MINUS = "\u2212"

#: 複製鈕的提示字。
_COPY_HINT = "複製看得到的列，貼進 Excel 一格一個數"

_WARN_MARK = '<sup class="warn">⚠</sup>'


def format_cell(value: Any, kind: str) -> tuple[str, str | None]:
    """一格的 ``(畫面上的字, data-v)``；文字欄的 ``data-v`` 是 ``None``（不寫）。

    * ``count`` —— 整數加千分位（``4,800``）；``data-v`` 是整數（``4800``）。
      值必須是整數（整數值的 float 也收，JSON 讀回來可能是 ``4800.0``）。
    * ``ratio`` —— 四位小數（``0.7953``）。
    * ``diff`` —— 帶正負號四位小數（``+0.0353``、``−0.0205``），負號是
      U+2212；四捨五入到四位是零的寫 ``0.0000``，不帶符號。
    * 數字欄的 ``data-v`` 是原始值到小數第 6 位（``0.795343``），負號半形；
      ``-0.000000`` 寫成 ``0.000000``。
    * ``None`` 與 NaN 是空格：畫面空白、``data-v=""``。
    * ``text`` —— ``str(value)``；``None`` 是空白。

    數字欄收到字串或 bool 時 raise ``TypeError``——那多半是欄的種類給錯了。
    """
    if kind == "text":
        return ("" if value is None else str(value)), None
    if value is None or (isinstance(value, (float, np.floating))
                         and math.isnan(value)):
        return "", ""
    if isinstance(value, (str, bool, np.bool_)):
        raise TypeError(
            f"{kind} 欄的格子要是數字或 None；收到 {value!r}")
    if kind == "count":
        as_float = float(value)
        if not as_float.is_integer():
            raise ValueError(f"count 欄的格子要是整數；收到 {value!r}")
        n = int(as_float)
        shown = f"{abs(n):,}"
        return (MINUS + shown if n < 0 else shown), str(n)
    v = float(value)
    raw = f"{v:.6f}"
    if raw == "-0.000000":
        raw = "0.000000"
    shown = f"{abs(v):.4f}"
    if shown == "0.0000":
        return "0.0000", raw
    if kind == "ratio":
        return (shown if v > 0 else MINUS + shown), raw
    if kind == "diff":
        return ("+" if v > 0 else MINUS) + shown, raw
    raise ValueError(f"unknown column kind {kind!r}")


def _leaf_th(full: str, label: str, warn: bool, rowspan: bool) -> str:
    span = ' rowspan="2"' if rowspan else ""
    mark = _WARN_MARK if warn else ""
    return (f'<th class="srt" data-full="{escape_attr(full)}"{span}>'
            f"{escape(label)}{mark}</th>")


def _runs(columns: tuple[Column, ...]) -> list[tuple[str | None, list[Column]]]:
    """相鄰同 ``group`` 的欄併成一段；沒有 group 的欄各自一段。"""
    runs: list[tuple[str | None, list[Column]]] = []
    for col in columns:
        if col.group is not None and runs and runs[-1][0] == col.group:
            runs[-1][1].append(col)
        else:
            runs.append((col.group, [col]))
    return runs


def _thead(table: Table) -> str:
    if not any(c.group is not None for c in table.columns):
        cells = [_leaf_th(table.row_header, table.row_header, False, False)]
        cells += [_leaf_th(c.label, c.label, c.warn, False)
                  for c in table.columns]
        return f"<thead><tr>{''.join(cells)}</tr></thead>"
    top = [_leaf_th(table.row_header, table.row_header, False, True)]
    bottom = []
    for group, cols in _runs(table.columns):
        if group is None:
            col = cols[0]
            top.append(_leaf_th(col.label, col.label, col.warn, True))
            continue
        whole = all(c.warn for c in cols)
        top.append(f'<th colspan="{len(cols)}" class="grp">{escape(group)}'
                   f"{_WARN_MARK if whole else ''}</th>")
        bottom += [_leaf_th(f"{group} {c.label}", c.label,
                            c.warn and not whole, False) for c in cols]
    return (f"<thead><tr>{''.join(top)}</tr>"
            f"<tr>{''.join(bottom)}</tr></thead>")


def _td(value: Any, column: Column) -> str:
    warn = False
    kind = column.kind
    if isinstance(value, Cell):
        warn = value.warn
        kind = value.kind or kind
        value = value.value
    shown, raw = format_cell(value, kind)
    if kind == "text":
        inner = (f'<span class="warn">{_WARN_MARK} {escape(shown)}</span>'
                 if warn else escape(shown))
        return f"<td>{inner}</td>"
    cls = "num dl" if kind == "diff" else "num"
    return (f'<td class="{cls}" data-v="{escape_attr(raw)}">{escape(shown)}'
            f"{_WARN_MARK if warn else ''}</td>")


def _tr(row: Row, columns: tuple[Column, ...]) -> str:
    cls = ' class="pin"' if row.pinned else ""
    head = (f'<th class="rl">{escape(row.label)}'
            f"{_WARN_MARK if row.warn else ''}</th>")
    cells = "".join(_td(v, c) for v, c in zip(row.cells, columns))
    return f"<tr{cls}>{head}{cells}</tr>"


def render_note(note: Note) -> str:
    """表下或段裡的一行小字：⚠ 說明（前面自動加「⚠ 」）或一般小字。"""
    if note.warn:
        return f'<p class="warn-note">⚠ {escape(note.text)}</p>'
    return f'<p class="note">{escape(note.text)}</p>'


def render_report_table(table: Table) -> str:
    """新長相的一張表：表名與複製鈕、篩選狀態列、表本身，再接表下的行。

    外殼的表格工具（``pipelines/evaluation/report/shell.py`` 的 JS）認 ``div.tblk``：每個欄頭
    ``th.srt`` 可以排序與篩選，複製鈕複製看得到的列——欄頭取 ``data-full``，
    數字格取 ``data-v``，其餘取看得到的字（不含 ⚠）。``tr.pin`` 永遠在最上面、
    不被篩掉。超過 15 列的表身限高、表頭固定（``.tw.tall``）。
    """
    tw = "tw tall" if table.tall else "tw"
    body = "".join(_tr(r, table.columns) for r in table.rows)
    html = (
        '<div class="tblk"><div class="thead-row">'
        f'<h4 class="tt">{escape(table.title)}</h4>'
        f'<button type="button" class="cpy" title="{_COPY_HINT}">複製</button>'
        '</div><div class="fstat" hidden></div>'
        f'<div class="{tw}"><table>{_thead(table)}<tbody>{body}</tbody>'
        "</table></div></div>"
    )
    return html + "".join(render_note(n) for n in table.notes)


def render_block(block: Any) -> str:
    """一個內容塊的 HTML（``report.types`` 的 ``Heading`` … ``Note``、
    ``Table``）。不認得的型別 raise ``TypeError``，不默默丟掉。"""
    if isinstance(block, Table):
        return render_report_table(block)
    if isinstance(block, Heading):
        return f"<h3>{escape(block.text)}</h3>"
    if isinstance(block, Paragraph):
        cls = ' class="description"' if block.muted else ""
        return f"<p{cls}>{escape(block.text)}</p>"
    if isinstance(block, Formula):
        return f'<div class="formula">{escape(block.text)}</div>'
    if isinstance(block, Bullets):
        if not block.items:
            return ""
        items = "".join(f"<li>{escape(b)}</li>" for b in block.items)
        return f'<ul class="section-bullets">{items}</ul>'
    if isinstance(block, Figure):
        return (block.fig.to_html(full_html=False, include_plotlyjs=False)
                + '<details class="fig-data"><summary>數字表</summary>'
                + render_report_table(block.data) + "</details>")
    if isinstance(block, Definitions):
        if not block.entries:
            return ""
        defs = "".join(f'<div class="def"><dt>{escape(term)}</dt>'
                       f"<dd>{escape(text)}</dd></div>"
                       for term, text in block.entries)
        return (f'<dl class="defs"><span class="defs-src">'
                f"{escape(block.source)}</span>{defs}</dl>")
    if isinstance(block, Note):
        return render_note(block)
    raise TypeError(f"not a content block: {block!r}")

"""主報表與比較報表的段落型別 ``ReportSection``，與段落裡依序畫的內容塊。

``ReportSection`` 從 ``evaluation.report`` 搬過來。這裡原本還有診斷頁用的
``ScopeNote``（一項診斷的範圍說明）與 ``Page``（多頁輸出的一頁），#532 拿掉
全部診斷頁時一起拿掉。

**內容塊（#537）**：一段的內容是一串依序畫的塊（``ReportSection.blocks``）——
:class:`Heading`、:class:`Paragraph`、:class:`Formula`、:class:`Bullets`、
:class:`Figure`、:class:`Table`、:class:`Definitions`、:class:`Note`。畫法在
``report.pages``（``render_block``）。塊裡的字串一律是純文字：畫的時候全部
跳脫（#366 第 1 項），所以不能夾 HTML 標籤。

:class:`Table` 是新長相的表（ADR-0031 決定 16）：欄有種類（文字、計數、
比例、差），種類決定數字格式；欄可以掛上層欄頭（比較表的 ``@K``）；列可以
固定在最上面（整體、Macro 平均）；欄頭、列名、格子可以標 ⚠。表的形狀在
建構時檢查，錯了就 raise，不等到畫的時候。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import plotly.graph_objects as go


@dataclass
class ReportSection:
    """A section in the evaluation report.

    * ``title`` / ``description`` —— 段標題與說明（新長相的段 ``description``
      給 ``""``，說明用 ``blocks``）。
    * ``title_en`` —— 段標題旁的英文小字（段檔的 ``TITLE_EN``）。
    * ``question`` —— 這段回答什麼問題，一句（段檔的 ``QUESTION``）；目錄印在
      段名後面。
    * ``key`` —— 段檔的模組短名（例 ``"ranking_metrics"``）；HTML id 是
      ``s-{key}``。**必填**：沒有 key 渲染時 raise。
    * ``blocks`` —— 依序畫的內容塊（本模組的 :class:`Heading` …
      :class:`Note`）。全篇不收合。
    """

    title: str
    description: str
    title_en: str = ""
    question: str = ""
    key: str = ""
    blocks: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# 內容塊
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Heading:
    """段裡的小標（``<h3>``），例：「item 粒度」「大類粒度」。"""

    text: str


@dataclass(frozen=True)
class Paragraph:
    """一段文字（``<p>``）。``muted`` 為真時用段落說明的淡色
    （``<p class="description">``）。"""

    text: str
    muted: bool = False


@dataclass(frozen=True)
class Formula:
    """公式（等寬字、淺底；``<div class="formula">``）。可以多行，換行照印。
    用 Unicode 純文字寫數學符號，不引入 MathJax／KaTeX（不連網）。"""

    text: str


@dataclass(frozen=True)
class Bullets:
    """列點（``<ul class="section-bullets">``），每則一句。"""

    items: tuple[str, ...]

    def __post_init__(self) -> None:
        if isinstance(self.items, str):
            raise TypeError(
                "Bullets.items 是一組字串（tuple），不是一個字串；"
                f"收到 {self.items!r}")
        object.__setattr__(self, "items", tuple(self.items))


@dataclass(frozen=True, eq=False)
class Figure:
    """一張 plotly 圖；畫成 ``fig.to_html(full_html=False,
    include_plotlyjs=False)``——plotly.js 由外殼內嵌一份。"""

    fig: go.Figure


@dataclass(frozen=True)
class Definitions:
    """定義區塊（``<dl class="defs">``）：一組 ``(名詞, 定義)``，放在那組表
    下面。``source`` 是區塊左上角的出處小字；從指標目錄產生的用預設值
    （``evaluation/report_sections/metric_tables.definitions``）。"""

    entries: tuple[tuple[str, str], ...]
    source: str = "定義（從指標目錄來）"

    def __post_init__(self) -> None:
        entries = tuple(tuple(e) for e in self.entries)
        for e in entries:
            if len(e) != 2:
                raise ValueError(
                    f"Definitions.entries 每項是 (名詞, 定義) 兩個字串；收到 {e!r}")
        object.__setattr__(self, "entries", entries)


@dataclass(frozen=True)
class Note:
    """一行小字。``warn`` 為真時是 ⚠ 說明（``<p class="warn-note">``，畫的
    時候自動在前面加「⚠ 」，``text`` 不要自己寫 ⚠），否則是一般小字
    （``<p class="note">``）。放在 :class:`Table` 的 ``notes`` 裡就畫在那張表
    正下方；單獨當一個塊也可以。"""

    text: str
    warn: bool = False


# ---------------------------------------------------------------------------
# 表
# ---------------------------------------------------------------------------

#: 欄的種類，決定格子怎麼印（``report.pages.format_cell``）：
#: ``text`` 原樣；``count`` 整數加千分位；``ratio`` 四位小數；``diff`` 帶正負號
#: 四位小數（負號 U+2212「−」，零寫 ``0.0000``）。
COLUMN_KINDS = ("text", "count", "ratio", "diff")


@dataclass(frozen=True)
class Column:
    """表的一欄（列名那一欄不算，它的欄頭是 :attr:`Table.row_header`）。

    * ``label`` —— 下層欄頭（「@1」「這次」「候選列」）。
    * ``kind`` —— :data:`COLUMN_KINDS` 之一。
    * ``group`` —— 上層欄頭（比較表的「@1」）。同 ``group`` 而且相鄰的欄合成
      一格（colspan）。複製時欄頭攤成一格（``data-full="@1 這次"``）。
    * ``warn`` —— 欄頭標 ⚠。一個上層欄頭底下的欄**全部** ``warn`` 時，⚠ 標在
      上層欄頭（比較表 ``@all`` 那組「@5⚠」），否則標在各自的下層欄頭。
    """

    label: str
    kind: str
    group: str | None = None
    warn: bool = False

    def __post_init__(self) -> None:
        if self.kind not in COLUMN_KINDS:
            raise ValueError(
                f"Column.kind 必須是 {COLUMN_KINDS} 之一；{self.label!r} 收到 "
                f"{self.kind!r}")


@dataclass(frozen=True)
class Cell:
    """要標 ⚠ 或換格式的格子：``Row.cells`` 裡放 ``Cell(值, warn=True)`` 取代
    原始值。⚠ 不進 ``data-v``、不跟著複製。例：「能不能比」表的結果欄
    ``Cell("不同", warn=True)`` 畫成「⚠ 不同」，複製出來是「不同」。

    ``kind`` 給了就蓋過欄的種類（:data:`COLUMN_KINDS` 之一），只管這一格：
    計數欄裡的一格要印字，例如總量表的 ``Cell("@6", kind="text")``。文字格
    不寫 ``data-v``，複製取看得到的字。"""

    value: Any
    warn: bool = False
    kind: str | None = None

    def __post_init__(self) -> None:
        if self.kind is not None and self.kind not in COLUMN_KINDS:
            raise ValueError(
                f"Cell.kind 必須是 {COLUMN_KINDS} 之一或 None；收到 {self.kind!r}")


@dataclass(frozen=True)
class Row:
    """表的一列。

    * ``label`` —— 列名（畫在列首的 ``<th class="rl">``）。
    * ``cells`` —— 原始值，跟 :attr:`Table.columns` 一一對應；``None`` 是空格。
      要標 ⚠ 的格子用 :class:`Cell` 包起來。
    * ``pinned`` —— 固定在最上面、不被篩掉也不被排序移走（整體、Macro 平均）。
      固定的列必須排在其他列前面。
    * ``warn`` —— 列名標 ⚠。
    """

    label: str
    cells: tuple
    pinned: bool = False
    warn: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "cells", tuple(self.cells))


#: 超過這麼多列的表，表身限高、表頭固定（``.tw.tall``）。
TALL_TABLE_ROWS = 15


@dataclass(frozen=True)
class Table:
    """新長相的一張表（ADR-0031 決定 16）。

    * ``title`` —— 表名＝內容；列是切片時加「 · 列是什麼」
      （``map@K · 整體與各分群``）。不放括號說明。
    * ``row_header`` —— 列名那一欄的欄頭：「分群」「item」「指標」「項目」「期」。
    * ``columns`` / ``rows`` —— 見 :class:`Column`、:class:`Row`。
    * ``notes`` —— 表下的行（:class:`Note`）：⚠ 說明、「參與 macro 的 item 數」。

    建構時檢查：每列的格數等於欄數；固定的列排在最前面；有上層欄頭時，沒有
    上層欄頭的欄必須排在有的前面（表頭兩列，沒有上層的欄跨兩列；排在中間的
    話，表格工具的排序、篩選與複製會對錯欄）。
    """

    title: str
    row_header: str
    columns: tuple[Column, ...]
    rows: tuple[Row, ...]
    notes: tuple[Note, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "rows", tuple(self.rows))
        object.__setattr__(self, "notes", tuple(self.notes))
        n = len(self.columns)
        for row in self.rows:
            if len(row.cells) != n:
                raise ValueError(
                    f"表 {self.title!r} 的列 {row.label!r} 有 {len(row.cells)} 格，"
                    f"欄有 {n} 個")
        seen_free = False
        for row in self.rows:
            if row.pinned and seen_free:
                raise ValueError(
                    f"表 {self.title!r} 的固定列 {row.label!r} 排在一般列後面；"
                    "固定的列（整體、Macro 平均）必須排在最前面")
            seen_free = seen_free or not row.pinned
        seen_group = False
        for col in self.columns:
            if col.group is None and seen_group:
                raise ValueError(
                    f"表 {self.title!r} 的欄 {col.label!r} 沒有上層欄頭，卻排在有"
                    "上層欄頭的欄後面；沒有上層的欄要排在最前面")
            seen_group = seen_group or col.group is not None
        for note in self.notes:
            if not isinstance(note, Note):
                raise TypeError(
                    f"表 {self.title!r} 的 notes 每項是 Note；收到 {note!r}")

    @property
    def tall(self) -> bool:
        """超過 :data:`TALL_TABLE_ROWS` 列：表身限高、表頭固定。"""
        return len(self.rows) > TALL_TABLE_ROWS

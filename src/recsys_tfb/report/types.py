"""主報表的段落型別 ``ReportSection``：一份報表裡的一個章節（標題、說明、圖、表）。

從 ``evaluation.report`` 搬過來（定義搬移，欄位逐字不變）。這裡原本還有
診斷頁用的 ``ScopeNote``（一項診斷的範圍說明）與 ``Page``（多頁輸出的一頁），
#532 拿掉全部診斷頁時一起拿掉。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
import plotly.graph_objects as go


@dataclass
class ReportSection:
    """A section in the evaluation report.

    ``formula`` 與 ``bullets`` 是後加的兩個**可選**欄位（新增於 diag-redesign，
    需求來自使用者對第一份真實產出的回饋）：

    * ``formula`` —— 這個區塊的數字是怎麼算出來的，一行。讀者不必翻手冊就能
      對上圖表裡的數字。用 Unicode 純文字寫數學符號（``Δ``／``Σ``／``≠``／
      ``ln``），**不要引入 MathJax／KaTeX**——生產限制是 no network、no
      additional packages，外部 CDN 一定載不到。
    * ``bullets`` —— 重點，每則一句。``description`` 在渲染器裡包進單一
      ``<p>``，塞不進列點；長段落正是被抱怨「沒耐心看完」的那個形狀。

    **兩者都必須維持可選且有預設值**：``evaluation/report_builder.py`` 大多數
    既有的 ``build_*_section`` 只傳 title/description，加必填欄位會讓它們全部
    ``TypeError``。渲染器是 ``evaluation/report.py``，它經
    ``report.pages.render_section_extras`` 畫這兩個欄位。
    """

    title: str
    description: str
    figures: list[go.Figure] = field(default_factory=list)
    tables: list[pd.DataFrame] = field(default_factory=list)
    table_titles: list[str] = field(default_factory=list)
    collapsible: bool = False
    formula: str = ""
    bullets: list[str] = field(default_factory=list)
    # ``collapsed_tables`` —— 與 ``tables`` 平行的旗標，``True`` 的那張明細表
    # 預設收合（``<details class="table-collapse">``，點標題才展開）。``collapsible``
    # 收的是**整段**；這個收的是**單張表**，兩者正交。維持可選有預設（同 formula/
    # bullets 的理由：大多數既有 build_* 不傳）；缺項／短於 tables 的位置視為不收合。
    collapsed_tables: list[bool] = field(default_factory=list)

"""中性呈現層的共用型別。

**現況**：``report`` 套件目前唯一的消費者是 ``evaluation/report.py``
re-export ``ReportSection``；``diagnosis/`` 與 ``evaluation/report_builder.py``
目前都沒有 import 它。**目標狀態**（後續 diag-redesign 計畫，見
``docs/superpowers/plans/diag-redesign/00-shared-context.md``）：各項診斷
模組會 import 本套件產出 ``ReportSection``，屆時才會是兩邊共同依賴。

本身不含任何診斷邏輯——只負責「怎麼呈現一個數字／一段敘述」，不負責「這個
數字代表什麼判斷」。三個型別各管一層：

* ``ReportSection`` —— 從 ``evaluation.report`` 搬過來（定義搬移，欄位逐字
  不變），沿用它原本的角色：一份報表裡的一個章節（標題、說明、圖、表）。
* ``ScopeNote`` —— 新增。一項診斷的範圍說明，跟著數字一起進報表：這個數字
  量的是什麼、算在哪批列上、報表放了哪些對照點、**不能**推論什麼。
* ``Page`` —— 新增。多個 ``ReportSection`` 加一個可選的 ``ScopeNote`` 組成
  一頁報表。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
import plotly.graph_objects as go


@dataclass
class ReportSection:
    """A section in the evaluation report."""

    title: str
    description: str
    figures: list[go.Figure] = field(default_factory=list)
    tables: list[pd.DataFrame] = field(default_factory=list)
    table_titles: list[str] = field(default_factory=list)
    collapsible: bool = False


@dataclass(frozen=True)
class ScopeNote:
    """一項診斷的範圍說明——跟數字一起進報表，不放在分離的手冊裡。

    ``blind_to`` 不得為空：一個數字如果說不出它看不見什麼，讀者就會過度
    解讀。這是契約，不是建議。

    ``blind_to``／``reference_points`` 都必須是 ``tuple``/``list`` of ``str``，
    **不能是單一字串**——字串本身是 ``Iterable[str]``，`pages.py` 的
    ``_render_scope_note`` 逐字元 iterate 會把它拆成一堆單字元 ``<li>``，
    不會噴任何錯誤，靜默壞掉。這是最容易誤用的地方，所以在建構時就明確擋。
    """

    measures: str                          # 這個數字量的是什麼
    population: str                        # 算在哪批列上
    blind_to: tuple[str, ...]              # 不能推論什麼（不得為空）
    reference_points: tuple[str, ...] = () # 報表放了哪些對照點、怎麼算的
    sampling: str = ""                     # 由 render() 從 meta["sampling_description"] 動態帶入

    def __post_init__(self) -> None:
        if isinstance(self.blind_to, str):
            raise TypeError(
                "ScopeNote.blind_to 不得是單一字串——需要 tuple/list of str"
                f"（例如 ('不能推論因果',)，不是 {self.blind_to!r}）；單一字串"
                "會被逐字元拆成多個 <li>"
            )
        if not self.blind_to:
            raise ValueError(
                "ScopeNote.blind_to 不得為空——每項診斷必須寫出它不能推論什麼"
            )
        if isinstance(self.reference_points, str):
            raise TypeError(
                "ScopeNote.reference_points 不得是單一字串——需要 tuple/list "
                f"of str（例如 ('baseline model',)，不是 "
                f"{self.reference_points!r}）；單一字串會被逐字元拆成多個 <li>"
            )


@dataclass(frozen=True)
class Page:
    """一頁報表：一個 slug/title 之下的多個 section，外加一份範圍說明。"""

    slug: str                        # 檔名主幹，例 "01-config-shift"
    title: str
    scope: ScopeNote | None
    sections: tuple[ReportSection, ...]

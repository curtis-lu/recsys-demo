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
    """A section in the evaluation report.

    ``formula`` 與 ``bullets`` 是後加的兩個**可選**欄位（新增於 diag-redesign，
    需求來自使用者對第一份真實產出的回饋）：

    * ``formula`` —— 這個區塊的數字是怎麼算出來的，一行。讀者不必翻手冊就能
      對上圖表裡的數字。用 Unicode 純文字寫數學符號（``Δ``／``Σ``／``≠``／
      ``ln``），**不要引入 MathJax／KaTeX**——生產限制是 no network、no
      additional packages，外部 CDN 一定載不到。
    * ``bullets`` —— 重點，每則一句。``description`` 在兩個渲染器都是包進單一
      ``<p>``，塞不進列點；長段落正是被抱怨「沒耐心看完」的那個形狀。

    **兩者都必須維持可選且有預設值**：``evaluation/report_builder.py`` 有 13 個
    既有的 ``build_*_section`` 只傳 title/description，加必填欄位會讓它們全部
    ``TypeError``。兩個渲染器（``report/pages.py`` 與 ``evaluation/report.py``）
    都必須渲染這兩個欄位——只改一邊的話，另一邊會**默默丟掉**它們，而那種 bug
    不會有任何錯誤訊息。
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
    # bullets 的理由：13 個既有 build_* 全不傳）；缺項／短於 tables 的位置視為不收合。
    collapsed_tables: list[bool] = field(default_factory=list)


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

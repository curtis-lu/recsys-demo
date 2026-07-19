"""中性呈現層：只管「怎麼呈現」，不含任何診斷判斷邏輯。

被 ``diagnosis/`` 與 ``evaluation/report_builder.py`` 共同依賴。三塊組成：

* ``report.types``  — ``ReportSection`` / ``ScopeNote`` / ``Page`` 型別。
* ``report.fmt``    — 按量的語意決定格式的數字格式器。
* ``report.scales`` — 色階（不含任何 good/bad 配色，見該模組 docstring）。
"""
from __future__ import annotations

from recsys_tfb.report.types import Page, ReportSection, ScopeNote

__all__ = ["Page", "ReportSection", "ScopeNote"]

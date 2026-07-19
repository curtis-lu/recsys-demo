"""中性呈現層：只管「怎麼呈現」，不含任何診斷判斷邏輯。

**現況**：目前唯一的消費者是 ``evaluation/report.py`` re-export
``ReportSection``（定義搬到這裡、欄位逐字不變，呼叫端沒改）。``diagnosis/``
與 ``evaluation/report_builder.py`` 目前都沒有 import 本套件。

**目標狀態**（後續 diag-redesign 計畫會做到，見
``docs/superpowers/plans/diag-redesign/00-shared-context.md``）：各項診斷
模組會 import 本套件產出 ``ReportSection``／``Page``，屆時才會是「被
``diagnosis/`` 與 ``report_builder.py`` 共同依賴」。

三塊組成：

* ``report.types``  — ``ReportSection`` / ``ScopeNote`` / ``Page`` 型別。
* ``report.fmt``    — 按量的語意決定格式的數字格式器。
* ``report.scales`` — 色階（不含任何 good/bad 配色，見該模組 docstring）。
"""
from __future__ import annotations

from recsys_tfb.report.types import Page, ReportSection, ScopeNote

__all__ = ["Page", "ReportSection", "ScopeNote"]

"""Neutral presentation layer: how things are shown, no diagnostic judgement.

Modules:

* ``report.types``   — ``ReportSection`` and the content blocks a section
  draws in order (``Heading`` … ``Table`` … ``Note``).
* ``report.pages``   — their HTML (``render_block``), the number formats
  (``format_cell``).
"""
from __future__ import annotations

from recsys_tfb.report.types import ReportSection

__all__ = ["ReportSection"]

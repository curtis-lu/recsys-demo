"""Neutral presentation layer: how things are shown, no diagnostic judgement.

Who imports it from outside the package (``grep -rn "recsys_tfb.report" src/
scripts/``, hits inside ``report/`` itself left out):

* ``evaluation/report.py`` — ``render_table`` and ``render_section_extras``
  from ``report.pages``; re-exports ``ReportSection``.

Nothing under ``scripts/`` imports it: ``scripts/profile_feature_table.py``
used to, and now writes its own single-page report (sortable, filterable
tables) because the multi-page shell that used to be here had no room for
that. That shell, with the figure builders, number formatters and colour
scales only the diagnosis pages used, went with those pages in #532.

Modules:

* ``report.types``   — ``ReportSection``.
* ``report.pages``   — the table renderer and the section extras (formula,
  bullets) the report shell draws.
"""
from __future__ import annotations

from recsys_tfb.report.types import ReportSection

__all__ = ["ReportSection"]

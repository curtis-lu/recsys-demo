"""Neutral presentation layer: how things are shown, no diagnostic judgement.

Who imports it from outside the package (``grep -rn "recsys_tfb.report" src/
scripts/``, hits inside ``report/`` itself left out):

* ``evaluation/report.py`` — ``render_block`` and the escapers from
  ``report.pages``;
  re-exports ``ReportSection``.
* ``evaluation/report_sections/metric_tables.py`` — the content-block types
  (``Table``, ``Column``, ``Row``, ``Cell``, ``Note``, ``Definitions``).

Nothing under ``scripts/`` imports it: ``scripts/profile_feature_table.py``
used to, and now writes its own single-page report (sortable, filterable
tables) because the multi-page shell that used to be here had no room for
that. That shell, with the figure builders, number formatters and colour
scales only the diagnosis pages used, went with those pages in #532.

Modules:

* ``report.types``   — ``ReportSection`` and the content blocks a section
  draws in order (``Heading`` … ``Table`` … ``Note``, #537).
* ``report.pages``   — their HTML (``render_block``), the number formats
  (``format_cell``).
"""
from __future__ import annotations

from recsys_tfb.report.types import ReportSection

__all__ = ["ReportSection"]

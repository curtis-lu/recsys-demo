"""Neutral presentation layer: how things are shown, no diagnostic judgement.

Who imports it from outside the package (``grep -rn "recsys_tfb.report" src/
scripts/``, hits inside ``report/`` itself left out):

* ``evaluation/report.py`` — ``render_table`` and ``render_section_extras``
  from ``report.pages``, so the main report and the diagnosis pages render
  tables and section extras the same way; re-exports ``ReportSection``.
* ``evaluation/report_builder.py`` — ``Page`` and ``report.pages.write_pages``,
  to write the diagnosis pages.
* the four registry diagnoses under ``diagnosis/metric/`` — ``ScopeNote`` in
  each ``__init__.py``; ``ReportSection``, ``report.figures`` and
  ``report.fmt`` in each ``_render.py``.

Nothing under ``scripts/`` imports it.

Modules:

* ``report.types``   — ``ReportSection`` / ``ScopeNote`` / ``Page``.
* ``report.pages``   — multi-page HTML output, and the table renderer both
  reports share.
* ``report.figures`` — plotly figure builders; draw only, never sort, filter
  or aggregate.
* ``report.fmt``     — number formatters chosen by what the quantity means.
* ``report.scales``  — colour scales (no good/bad colouring; see that module).
"""
from __future__ import annotations

from recsys_tfb.report.types import Page, ReportSection, ScopeNote

__all__ = ["Page", "ReportSection", "ScopeNote"]

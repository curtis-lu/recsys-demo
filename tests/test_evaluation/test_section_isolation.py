"""Sections do not mention each other (ADR-0031 decision 16): a section's text
may point back to 核心概念, the one section every other builds on, and to no
other; 核心概念 points to none. A reader who skips a section loses nothing
another section relies on, and moving or dropping a section breaks no
sentence.

The titles are the section files' ``TITLE``. The page is the main report as
``assemble_report`` draws it from the full fixture of
``test_report_builder`` (every section built), and a section's text is what
a reader sees in it: everything inside ``<div class="section" id="s-{key}">``
but scripts and styles. A plotly figure's own titles are not read: they sit
in its script, which plotly writes with every non-ASCII character escaped.

**The comparison report** gets the same check against its own sections'
titles (``comparison/report.py``'s ``SECTIONS``), on the page
``assemble_comparison_report`` draws with every section built. It has no
核心概念, so a section there may point to none.

**The ⚠ sentences.** Those pages print none: their two sides agree on every
annotation and no all-positive share warns. So each report is checked again
on a page that prints them (the end of this file): the two sides differing on
all four annotations, and in the main report an all-positive share above the
warning line at both grains.
"""
from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser

import pytest

from recsys_tfb.evaluation import report_builder as rb
from recsys_tfb.evaluation.report import generate_html_report
from recsys_tfb.report.types import Paragraph, ReportSection
from tests.test_evaluation import test_report_builder as fixtures

#: The section every other may point back to.
CORE = "core_concepts"

#: The main report's sections that carry a ``TITLE``: all of
#: ``report_builder.SECTIONS`` but the glossary, which #537 removes.
SECTION_MODULES = tuple(m for m in rb.SECTIONS
                        if m.__name__.rsplit(".", 1)[1] != "glossary")


def _key(module) -> str:
    return module.__name__.rsplit(".", 1)[1]


TITLES = {_key(m): m.TITLE for m in SECTION_MODULES}


class _SectionText(HTMLParser):
    """The visible text inside each ``div.section`` / ``details.section``, by
    its id; script and style content left out."""

    _SKIP = ("script", "style")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text: dict[str, str] = {}
        self._depth = 0          # open div/details elements
        self._open: list[tuple[str, int]] = []   # (section id, its depth)
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP:
            self._skip += 1
            return
        if tag in ("div", "details"):
            self._depth += 1
            a = dict(attrs)
            if "section" in (a.get("class") or "").split() and a.get("id"):
                self._open.append((a["id"], self._depth))
                self.text.setdefault(a["id"], "")

    def handle_endtag(self, tag):
        if tag in self._SKIP:
            self._skip -= 1
            return
        if tag in ("div", "details"):
            if self._open and self._open[-1][1] == self._depth:
                self._open.pop()
            self._depth -= 1

    def handle_data(self, data):
        if self._open and not self._skip:
            self.text[self._open[-1][0]] += data


def section_texts(html: str) -> dict[str, str]:
    """``{section id: visible text}`` of every section on the page."""
    parser = _SectionText()
    parser.feed(html)
    return parser.text


def mentions(key: str, text: str, titles: dict[str, str] = TITLES) -> list[str]:
    """The other sections' titles section ``key``'s text names — every title
    but its own and, unless it is 核心概念 itself, 核心概念's."""
    allowed = {key} | ({CORE} if key != CORE else set())
    return [titles[other] for other in titles
            if other not in allowed and titles[other] in text]


@pytest.fixture(scope="module")
def main_report_html() -> str:
    """The main report from the full fixture: every section built."""
    t = fixtures
    return t._assemble(
        t._results(evaluation_metrics=t._metrics(),
                   baseline_metrics=t._baseline_metrics_full(),
                   evaluation_report_aggregates=t._report_aggregates(),
                   prediction_quality_metrics=t._pq_payload()),
        t._params())


def test_every_section_declares_its_title():
    for module in SECTION_MODULES:
        for name in ("TITLE", "TITLE_EN", "QUESTION"):
            assert getattr(module, name, "").strip(), (module.__name__, name)
        assert module.QUESTION.endswith("？"), module.__name__
    assert len(set(TITLES.values())) == len(TITLES), TITLES


@pytest.mark.parametrize("key", list(TITLES))
def test_a_section_names_no_other_section(main_report_html, key):
    texts = section_texts(main_report_html)
    sid = f"s-{key}"
    assert sid in texts, (f"no section with id {sid!r} on the page; its "
                          f"ReportSection needs key={key!r}")
    assert mentions(key, texts[sid]) == []


def test_the_check_reads_section_text_by_id_and_allows_only_core():
    """The check itself, on a page of blocks-only sections: a mention in a
    paragraph is found, the table of contents (which names every section) is
    no section's text, a pointer back to 核心概念 is allowed everywhere but in
    核心概念."""
    pages = [
        ReportSection(title=TITLES["ranking_metrics"], description="",
                      key="ranking_metrics",
                      blocks=[Paragraph(f"見{TITLES['data_stats']}與"
                                        f"{TITLES[CORE]}的總量")]),
        ReportSection(title=TITLES["data_stats"], description="",
                      key="data_stats",
                      blocks=[Paragraph(f"總量見{TITLES[CORE]}")]),
        ReportSection(title=TITLES[CORE], description="", key=CORE,
                      blocks=[Paragraph(f"下面的{TITLES['completeness']}")]),
    ]
    html = generate_html_report(pages)
    texts = section_texts(html)
    assert set(texts) == {"s-ranking_metrics", "s-data_stats", f"s-{CORE}"}
    assert mentions("ranking_metrics", texts["s-ranking_metrics"]) == [
        TITLES["data_stats"]]
    # the table of contents names 排序指標 too, outside every section
    assert TITLES["ranking_metrics"] in html[:html.index("</nav>")]
    assert mentions("data_stats", texts["s-data_stats"]) == []
    assert mentions(CORE, texts[f"s-{CORE}"]) == [TITLES["completeness"]]


# ---------------------------------------------------------------------------
# The comparison report
# ---------------------------------------------------------------------------

from recsys_tfb.evaluation.comparison import report as comparison_report  # noqa: E402
from recsys_tfb.evaluation.comparison import segment_metrics  # noqa: E402
from recsys_tfb.evaluation.metric_catalog import (  # noqa: E402
    QUERY_GROUP,
    CompareEntry,
)
from tests.test_evaluation.comparison import fixtures as cmp_fixtures  # noqa: E402

#: The comparison report's section titles, by key.
COMPARISON_TITLES = {_key(m): m.TITLE for m in comparison_report.SECTIONS}


@pytest.fixture
def comparison_report_html(monkeypatch) -> str:
    """The comparison report with every section built: categories on and a
    list for the segment section (empty by default)."""
    fx = cmp_fixtures
    monkeypatch.setattr(segment_metrics, "COMPARED",
                        (CompareEntry("map", QUERY_GROUP, "per_segment"),))
    p = fx.params(categories=True)
    results = fx.inputs(fx.bundle(0.6, category=fx.category_bundle(0.5)),
                        fx.bundle(0.5, category=fx.category_bundle(0.4)), p)
    return comparison_report.assemble_comparison_report(results, p)


def test_comparison_titles_are_declared_and_distinct():
    for module in comparison_report.SECTIONS:
        for name in ("TITLE", "TITLE_EN", "QUESTION"):
            assert getattr(module, name, "").strip(), (module.__name__, name)
        assert module.QUESTION.endswith("？"), module.__name__
    assert len(set(COMPARISON_TITLES.values())) == len(COMPARISON_TITLES)


@pytest.mark.parametrize("key", list(COMPARISON_TITLES))
def test_a_comparison_section_names_no_other_section(comparison_report_html,
                                                     key):
    texts = section_texts(comparison_report_html)
    sid = f"s-{key}"
    assert sid in texts, f"no section with id {sid!r} on the comparison page"
    others = [COMPARISON_TITLES[o] for o in COMPARISON_TITLES
              if o != key and COMPARISON_TITLES[o] in texts[sid]]
    assert others == []


# ---------------------------------------------------------------------------
# The ⚠ sentences: pages that print them
# ---------------------------------------------------------------------------

from tests.test_evaluation.report_sections import (  # noqa: E402
    test_popularity_baseline as baseline_fixtures,
)

#: A ``Note(warn=True)`` as the page writes it (``report/pages.py``).
_WARN_NOTE = re.compile(r'<p class="warn-note">(.*?)</p>', re.S)

#: The ⚠ on a column header, a row label or a 能不能比 cell.
_WARN_MARK = '<sup class="warn">⚠</sup>'


def _warn_notes(html: str) -> list[str]:
    """The text of every ⚠ line on the page, as a reader sees it."""
    return [unescape(note) for note in _WARN_NOTE.findall(html)]


def _sections_warning(texts: dict[str, str], notes: list[str]) -> set[str]:
    """The ids of the sections whose text holds one of the ⚠ lines."""
    return {sid for sid, text in texts.items()
            if any(note in text for note in notes)}


def _differing(population: dict) -> list[str]:
    return [name for name, annotation in population.items()
            if not annotation["same"]]


@pytest.fixture(scope="module")
def main_report_texts_with_warnings() -> dict[str, str]:
    """The main report's section texts on a page with its ⚠ lines: the
    popularity baseline differs from the model on all four annotations —
    ``test_popularity_baseline``'s bundles (every catalog metric at every K
    of the grid), compared by ``compare_to_baseline`` itself — and the row
    facts' all-positive share is above the warning line at both grains
    (``test_report_builder._ap_facts``: 300 of 950 groups at the item grain,
    300 of 900 at the category grain; the accounts are
    ``all_positive_accounts``'). The premise is asserted here, so no case
    below can pass on a page without the ⚠ lines."""
    t, b = fixtures, baseline_fixtures
    params = t._params()
    params["evaluation"]["k_values"] = [1, 2, "all"]
    params["evaluation"]["report"]["display"]["compare_k"] = [1, "all"]
    facts = t._ap_facts(n_all_positive=300, category=t._ap_category())
    # The bundles' three items (A, B, C); the count is not in the accounts.
    facts["dataset_overview"]["totals"]["n_items"] = len(b._ITEMS)
    results = t._with_landed(t._results(
        evaluation_row_facts=facts,
        evaluation_metrics=b._bundle(0.5),
        baseline_metrics=b._baseline(params, all_k=2, macro_items=("A", "B"),
                                     scored=9, segment_columns=()),
        evaluation_report_aggregates=t._report_aggregates(),
        prediction_quality_metrics=t._pq_payload()), params)
    population = results["baseline_comparison"]["population"]
    assert _differing(population) == list(population), \
        "premise: the baseline differs on all four annotations"
    html = rb.assemble_report(results, params)
    texts = section_texts(html)
    notes = _warn_notes(html)
    # 核心概念: one line per grain; 熱門度基準: @all, Macro and the scored
    # groups (the segment columns are said in its 能不能比 table, marked ⚠).
    assert sum(note.startswith("⚠ 全正 query group") for note in notes) == 2
    assert len(notes) == 5, notes
    assert _sections_warning(texts, notes) == {
        f"s-{CORE}", "s-popularity_baseline"}
    assert _WARN_MARK in html
    return texts


@pytest.mark.parametrize("key", list(TITLES))
def test_a_section_names_no_other_section_in_its_warnings(
        main_report_texts_with_warnings, key):
    texts = main_report_texts_with_warnings
    sid = f"s-{key}"
    assert sid in texts, f"no section with id {sid!r} on the page"
    assert mentions(key, texts[sid]) == []


#: The comparison report's sections on a page whose two sides segment by
#: different columns: all but the segment section, which draws nothing then
#: (ADR-0031 decision 16 item 15; its 能不能比 row says why).
_DRAWN_WITH_OTHER_SEGMENTS = tuple(k for k in COMPARISON_TITLES
                                   if k != "segment_metrics")


@pytest.fixture
def comparison_texts_with_warnings(monkeypatch) -> dict[str, str]:
    """The comparison report's section texts on a page with its ⚠ lines:
    the compared side differs on all four annotations (its "all" is @4,
    ``event`` rows; two macro items; nine groups scored; no segment
    column), both bundles ``fixtures.bundle``'s, the comparison and its cut
    the real nodes' (``fixtures.inputs``). Categories on and a list for the
    segment section, so every section that can be drawn is. The premise is
    asserted here, as for the main report."""
    fx = cmp_fixtures
    monkeypatch.setattr(segment_metrics, "COMPARED",
                        (CompareEntry("map", QUERY_GROUP, "per_segment"),))
    p = fx.params(categories=True)
    results = fx.inputs(
        fx.bundle(0.6, category=fx.category_bundle(0.5)),
        fx.bundle(0.5, all_k=4, macro_items=("item_A", "item_B"), scored=9,
                  segment_columns=(), category=fx.category_bundle(0.4)),
        p)
    population = results["source_comparison"]["population"]
    assert _differing(population) == list(population), \
        "premise: the compared side differs on all four annotations"
    html = comparison_report.assemble_comparison_report(results, p)
    texts = section_texts(html)
    assert set(texts) == {f"s-{k}" for k in _DRAWN_WITH_OTHER_SEGMENTS}
    notes = _warn_notes(html)
    assert notes, "premise: the page prints ⚠ lines"
    assert _sections_warning(texts, notes) == {"s-overall_metrics",
                                               "s-per_item_metrics"}
    assert _WARN_MARK in html
    return texts


@pytest.mark.parametrize("key", _DRAWN_WITH_OTHER_SEGMENTS)
def test_a_comparison_section_names_no_other_section_in_its_warnings(
        comparison_texts_with_warnings, key):
    text = comparison_texts_with_warnings[f"s-{key}"]
    others = [COMPARISON_TITLES[o] for o in COMPARISON_TITLES
              if o != key and COMPARISON_TITLES[o] in text]
    assert others == []

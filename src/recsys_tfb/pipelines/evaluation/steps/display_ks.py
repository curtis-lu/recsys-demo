"""The K each table of the two reports shows (ADR-0031 decision 10: the K cut
is a computation, landed; #536). ``resolve_display_ks`` (the main report) and
``resolve_comparison_display_ks`` (the comparison report) decide which lists
are cut and at what; this module holds the lists and the cut.

Shape of what that node lands (the key constants are in
``evaluation.result_readers``)::

    {
      "evaluation_metrics": {             # the ranking-metrics section
          "all_k": int,
          "metrics_section": [k, ...],    # ints: k_values resolved ∪ all_k
          "category": {"all_k": int, "metrics_section": [k, ...]} | None,
      },
      "baseline_comparison": {            # the popularity-baseline section
          "all_k": int,
          "compare_k": [k, ...],          # ints: every table of the section
          "category": {"all_k": int, "compare_k": [k, ...]} | None,
      } | None,                           # baseline section off
      "config_fingerprint": {...},        # with the display keys it read
    }

Each block is keyed by the result its tables are drawn from. A list of ints
(:func:`k_columns`) is the K a table's columns are, in order: ``"all"``
resolved to the block's ``all_k`` and merged with an int that equals it, so
the header writes the real K (``@6``) once (ADR-0031 decision 16 item 7).

Shape of what ``resolve_comparison_display_ks`` lands, off the comparison of
this run's side with the compared source (``source_comparison``) — the K
columns of every per-K table of the comparison report (:func:`comparison_cut`;
#537: one list for every table of a grain, ADR-0031 decision 16 item 7)::

    {
      "all_k": int,                       # this run's side's "all"
      "compare_k": [k, ...],              # ints, ascending; "all" is all_k
      "category": {"all_k": int, "compare_k": [...]} | None,
      "config_fingerprint": {...},        # with the display keys it read
      "compare_source": {...},            # what it was compared against
    }

The column at ``all_k`` reads each side's own "all" (``name@all``,
``report_sections.metric_tables.metric_table``). No list of rows: since #537
no table of that report prints every computed key.

The cut itself is :func:`resolve_display_k` (bug 8, ADR-0020;
:func:`_k_exceeds_all_k`). It lived beside the report sections until the
comparison report stopped cutting K itself (#536).
"""

from __future__ import annotations

from typing import Optional

from recsys_tfb.core.consistency import EVALUATION_K_VALUES_DEFAULT
from recsys_tfb.evaluation.result_readers import (
    ALL_LABEL,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    DISPLAY_METRICS_SECTION,
)

#: The display keys the cut reads, as dotted paths: the fingerprint of what
#: ``resolve_display_ks`` lands carries their values (``extra_keys``), so
#: ``--only-node generate_report`` after one changed is refused and sent to
#: ``--from-node resolve_display_ks``. The cut reads ``evaluation.k_values``
#: too (the metrics section's K, and ``compare_k`` when it is not written);
#: that one is a computed key, already in every fingerprint
#: (``config_fingerprint.COMPUTED_KEYS``), so it is not listed here.
DISPLAY_KEYS_READ = ("evaluation.report.display.compare_k",)

#: The display keys the comparison report's cut reads, for the fingerprint of
#: what ``resolve_comparison_display_ks`` lands: a change re-runs that node.
#: The same key as the main report's since #537, which retired the
#: comparison's own recall K key (A63), so under ``--compare`` a change makes
#: both cuts stale (``nodes.COMPARE_MODE_DISPLAY_RERUN_NODE``).
COMPARISON_DISPLAY_KEYS_READ = ("evaluation.report.display.compare_k",)


def resolve_display_k(raw_k: list, all_k: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes.

    Returns the list filtered, ``'all'`` kept as the label; :func:`k_columns`
    then resolves it to ``all_k`` for the headers, which write the real K
    (ADR-0031 decision 16 item 7).

    ``all_k`` is the bundle's ``metrics.resolved_all_k``: the K its ``"all"``
    values are stored at, which is the item count unless the frame held one
    row per event (then the widest query group, #434).

    Filters out any int K > all_k (bug 8, ADR-0020): with e.g. 3 product
    categories, precision@K's denominator is K itself (not min(K, n_items)),
    so @4/@5 declined purely because the denominator grew, not because the
    model did anything different — those columns aren't wrong, they're
    meaningless. The bound is the longest list, and that is ``all_k``: with
    ``event`` rows a 30-row group gives @13 real rows to rank although there
    are 12 items. This is a filter over the given list, not a regenerated
    one: a caller list without "all" does not gain one, and "all" is always
    kept. When ``all_k <= 0`` (unknown) skip the filter entirely, since
    filtering by an unknown bound would collapse every table down to just
    "@all".
    """
    out = []
    for k in raw_k:
        if isinstance(k, str) and k.lower() == "all":
            out.append("all")
        else:
            out.append(int(k))
    return [k for k in out if not _k_exceeds_all_k(k, all_k)]


def _k_exceeds_all_k(k: int | str, all_k: int) -> bool:
    """True for an int K above ``all_k`` — the one bug 8 rule (ADR-0020),
    applied by :func:`resolve_display_k`. ``"all"`` never exceeds, and
    ``all_k <= 0`` (unknown) filters nothing.
    """
    if all_k <= 0 or k == "all":
        return False
    return int(k) > all_k


def k_columns(raw_k: list, all_k: int) -> list[int]:
    """The K a table's columns are: ``raw_k`` with ``"all"`` resolved to
    ``all_k``, every int K above ``all_k`` dropped (bug 8, the rule of
    :func:`resolve_display_k`), duplicates merged, ascending.

    Integers, not the ``"all"`` label, because the header writes the real K
    (ADR-0031 decision 16 item 7: ``@6``, not ``@all``) and a list holding
    both ``6`` and ``"all"`` would print the same column twice. A compared
    table still reads its ``all_k`` column at ``name@all``
    (``metric_tables.metric_table``), which pairs each side's own "all".
    """
    return sorted({k if k != ALL_LABEL else all_k
                   for k in resolve_display_k(raw_k, all_k)})


def _evaluation(parameters: dict) -> dict:
    return parameters.get("evaluation") or {}


def _display(parameters: dict) -> dict:
    return ((_evaluation(parameters).get("report") or {})
            .get("display") or {})


def configured_k_values(parameters: dict) -> list:
    """``evaluation.k_values``, or the default the metric layer computes when
    it is not written (``core.consistency.EVALUATION_K_VALUES_DEFAULT``, the
    one ``metrics_spark._resolve_k_grids`` reads)."""
    return list(_evaluation(parameters).get(
        "k_values", EVALUATION_K_VALUES_DEFAULT))


def configured_compare_k(parameters: dict) -> list:
    """``evaluation.report.display.compare_k``; not written, the K the run
    computes (:func:`configured_k_values`). Not a list of its own: the old
    default ``[1, 3, 5, "all"]`` asked for @1 and @3, which the default
    ``k_values`` (``[5, "all"]``) never computes — two columns printed empty
    in every table that shows ``compare_k``. A62 checks a written list
    against ``k_values``; an unwritten one is ``k_values`` itself."""
    display = _display(parameters)
    if "compare_k" in display:
        return display["compare_k"]
    return configured_k_values(parameters)


def evaluation_metrics_block(all_k: int, category_all_k: Optional[int],
                             k_values: list) -> dict:
    """The ``evaluation_metrics`` block: the metrics section's K —
    ``k_values`` resolved ∪ "all", cut at ``all_k`` (at ``category_all_k``
    for the category grain; ``None``: no category grain)."""
    section_k = [*k_values, ALL_LABEL]
    return {
        DISPLAY_ALL_K: all_k,
        DISPLAY_METRICS_SECTION: k_columns(section_k, all_k),
        "category": None if category_all_k is None else {
            DISPLAY_ALL_K: category_all_k,
            DISPLAY_METRICS_SECTION: k_columns(section_k, category_all_k),
        },
    }


def baseline_comparison_block(all_k: int, category_all_k: Optional[int],
                              compare_k: list) -> dict:
    """The ``baseline_comparison`` block: one K list for every table of the
    popularity-baseline section (ADR-0031 decision 16 item 7: a section's
    tables share one K), ``compare_k`` cut at ``all_k`` — at
    ``category_all_k`` for the category grain (``None``: no category
    grain)."""
    return {
        DISPLAY_ALL_K: all_k,
        DISPLAY_COMPARE_K: k_columns(compare_k, all_k),
        "category": None if category_all_k is None else {
            DISPLAY_ALL_K: category_all_k,
            DISPLAY_COMPARE_K: k_columns(compare_k, category_all_k),
        },
    }


def comparison_cut(all_k: int, compare_k: list) -> dict:
    """One grain of the comparison report's cut (module docstring): the K
    columns of every per-K table at a grain whose this run's side's ``"all"``
    is ``all_k`` — ``compare_k`` cut at ``all_k`` (:func:`resolve_display_k`),
    ``"all"`` written as ``all_k`` (a table header writes the real K,
    ADR-0031 decision 16 item 7), ascending, each K once (``[1, 3, 5,
    "all"]`` at 5 is ``[1, 3, 5]``).

    Cut at this side's ``"all"`` only: the column at ``all_k`` reads the cell
    pairing each side's own ``"all"`` (``name@all``), so only the other
    columns need a bound, and one bound keeps every row of a table on the
    same columns. The two sides' ``"all"`` differing is the comparison's
    ``population.all_k`` annotation, which the report marks.
    """
    return {DISPLAY_ALL_K: all_k, DISPLAY_COMPARE_K: k_columns(compare_k, all_k)}

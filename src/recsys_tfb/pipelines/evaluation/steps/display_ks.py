"""The K each table of the two reports shows (ADR-0031 decision 10: the K cut
is a computation, landed; #536). ``resolve_display_ks`` (the main report) and
``resolve_comparison_display_ks`` (the comparison report) decide which lists
are cut and at what; this module holds the lists and the cut.

Shape of what that node lands (the key constants are in
``evaluation.result_readers``)::

    {
      "evaluation_metrics": {             # the overview and the metrics section
          "all_k": int,
          "primary_map_k": [k, ..., "all"],
          "metrics_section": [k, ..., "all"],
          "headline_column_shown": bool,
          "category": {"all_k": int, "metrics_section": [...]} | None,
      },
      "baseline_comparison": {            # the baseline section
          "all_k": int, "primary_map_k": [...], "metrics_section": [...],
          "category": {...} | None,
      } | None,                           # baseline section off
      "config_fingerprint": {...},        # with the display keys it read
    }

Each block is keyed by the result its tables are drawn from.

Shape of what ``resolve_comparison_display_ks`` lands, off the comparison of
this run's side with the compared source (``source_comparison``)::

    {
      "per_item": {                       # the per-item M/B/Δ tables
          "guardrail_recall_k": [k, ...],
          "primary_map_k": [k, ..., "all"],
      },
      "overall": {                        # the every-key M/B/Δ table
          "rows": [key, ...],             # the table's rows, sorted
          "this": [key, ...],             # whose M cell is drawn
          "reference": [key, ...],        # whose B cell is drawn
          "diff": [key, ...],             # whose Δ is drawn: in both
      },
      "category": {"per_item": {...}, "overall": {...}} | None,
      "config_fingerprint": {...},        # with the display keys it read
      "compare_source": {...},            # what it was compared against
    }

The cut itself is :func:`resolve_display_k` for a display list and
:func:`drop_metric_keys_above_all_k` for a table that prints every key (bug
8, ADR-0020); both apply :func:`_k_exceeds_all_k`, so they cannot disagree.
They lived beside the report sections until the comparison report stopped
cutting K itself (#536).
"""

from __future__ import annotations

from typing import Optional

from recsys_tfb.evaluation.result_readers import (
    ALL_LABEL,
    DIFF,
    DISPLAY_ALL_K,
    DISPLAY_GUARDRAIL_RECALL_K,
    DISPLAY_HEADLINE_SHOWN,
    DISPLAY_METRICS_SECTION,
    DISPLAY_PRIMARY_MAP_K,
    DISPLAY_ROWS,
    REFERENCE,
    THIS,
)

#: The display keys the cut reads, as dotted paths: the fingerprint of what
#: ``resolve_display_ks`` lands carries their values (``extra_keys``), so
#: ``--only-node generate_report`` after one changed is refused and sent to
#: ``--from-node resolve_display_ks``. ``evaluation.metric.k``, which the
#: headline flag reads, is a computed key and in every fingerprint already.
DISPLAY_KEYS_READ = ("evaluation.report.display.primary_map_k",)

#: The display keys the comparison report's cut reads, for the fingerprint of
#: what ``resolve_comparison_display_ks`` lands: a change re-runs that node
#: only, and no other artifact goes stale (``guardrail_recall_k`` is read by
#: no table of the main report).
COMPARISON_DISPLAY_KEYS_READ = (
    "evaluation.report.display.guardrail_recall_k",
    "evaluation.report.display.primary_map_k",
)

#: ``evaluation.report.display.guardrail_recall_k`` when it is not set: what
#: the comparison report's per-item recall tables drew before the cut was
#: landed.
_GUARDRAIL_RECALL_K_DEFAULT = [1, 3, 5]

#: ``evaluation.report.display.primary_map_k`` when it is not set: what the
#: overview and the baseline section drew before the cut was landed.
_PRIMARY_MAP_K_DEFAULT = [1, 3, 5, "all"]

#: K columns of every metrics-section table, before the cut — the metrics
#: section's own tables and the baseline section's overall, per-segment and
#: category tables. A fixed list, not a setting.
_METRICS_SECTION_K = (1, 2, 3, 4, 5, "all")


def resolve_display_k(raw_k: list, all_k: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes.

    Returns labels as strings/ints that are used both as dict keys and for
    metric lookups. 'all' is looked up at ``all_k``
    (``report_sections.shared.k_to_lookup``) but is kept as the label 'all'
    for display.

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
    """True for an int K above ``all_k`` — the one bug 8 rule (ADR-0020).

    Shared by the display-list filter (``resolve_display_k``) and the
    metric-key filter (``drop_metric_keys_above_all_k``), so the two
    cannot disagree about which columns and rows a grain drops. ``"all"``
    never exceeds, and ``all_k <= 0`` (unknown) filters nothing.
    """
    if all_k <= 0 or k == "all":
        return False
    return int(k) > all_k


def drop_metric_keys_above_all_k(keys, all_k: int) -> list:
    """``keys`` minus those whose ``@K`` suffix is an int K above ``all_k`` (bug 8).

    For tables that print every computed metric key as a row (the comparison
    report's overall and category-overall tables), where there is no display
    K list to filter. Same rule and reason as ``resolve_display_k``: past
    the longest list precision@K keeps falling only because its denominator
    is K, so those rows are meaningless rather than wrong — nothing flags
    them. K == all_k stays (it is the ``"all"`` row), keys without an
    ``@<int>`` suffix stay, order is kept, and ``all_k <= 0`` keeps every key.
    """
    out = []
    for key in keys:
        _, sep, suffix = str(key).rpartition("@")
        if sep and suffix.isdigit() and _k_exceeds_all_k(int(suffix), all_k):
            continue
        out.append(key)
    return out


def _display(parameters: dict) -> dict:
    return (((parameters.get("evaluation") or {}).get("report") or {})
            .get("display") or {})


def configured_primary_map_k(parameters: dict) -> list:
    """``evaluation.report.display.primary_map_k``, or its default."""
    return _display(parameters).get("primary_map_k", _PRIMARY_MAP_K_DEFAULT)


def configured_guardrail_recall_k(parameters: dict) -> list:
    """``evaluation.report.display.guardrail_recall_k``, or its default."""
    return _display(parameters).get("guardrail_recall_k",
                                    _GUARDRAIL_RECALL_K_DEFAULT)


def metrics_section_ks(all_k: int) -> list:
    """The metrics section's K columns at a grain whose ``"all"`` is
    ``all_k``.

    One derivation for those tables and for the headline flag
    (:func:`headline_column_shown`): a note naming ``map_attr@{metric.k}`` is
    only true while that column survives the cut, and with a second copy of
    the list the note could keep naming a column the table dropped, and
    nothing would raise — the reader is pointed at a column that is not
    there.
    """
    return resolve_display_k(list(_METRICS_SECTION_K), all_k)


def headline_column_shown(metric_k: Optional[int], metrics_section: list) -> bool:
    """Whether the metrics section's ``map_attr`` table shows the
    ``@{metric_k}`` column the headline is read at. ``metric_k`` unset
    (``None``) is the ``@all`` column, which no flag is needed for: the note
    names it in words."""
    return metric_k is not None and metric_k in metrics_section


def cut_at(all_k: int, category_all_k: Optional[int],
           primary_map_k: list) -> dict:
    """One block: every list cut at ``all_k``, the category grain's at
    ``category_all_k`` (``None``: no category grain)."""
    return {
        DISPLAY_ALL_K: all_k,
        DISPLAY_PRIMARY_MAP_K: resolve_display_k(primary_map_k, all_k),
        DISPLAY_METRICS_SECTION: metrics_section_ks(all_k),
        "category": None if category_all_k is None else {
            DISPLAY_ALL_K: category_all_k,
            DISPLAY_METRICS_SECTION: metrics_section_ks(category_all_k),
        },
    }


def with_headline_flag(block: dict, metric_k: Optional[int]) -> dict:
    """``block`` plus :data:`DISPLAY_HEADLINE_SHOWN`, read off its metrics
    section list."""
    return {**block, DISPLAY_HEADLINE_SHOWN: headline_column_shown(
        metric_k, block[DISPLAY_METRICS_SECTION])}


def per_item_lists(all_k: int, guardrail_recall_k: list,
                   primary_map_k: list) -> dict:
    """The comparison report's two per-item lists, cut at ``all_k``."""
    return {
        DISPLAY_GUARDRAIL_RECALL_K: resolve_display_k(guardrail_recall_k, all_k),
        DISPLAY_PRIMARY_MAP_K: resolve_display_k(primary_map_k, all_k),
    }


def every_key_rows(cells: dict, all_k_this: int, all_k_reference: int) -> dict:
    """The rows of a table printing every key of a compared block, each
    side cut at its own ``"all"``: the shape's ``overall`` entry (module
    docstring).

    A side holds a key when its value in the cell is not ``None``: every
    value of an ``overall`` block is a float (``metrics_spark
    .aggregate_overall`` casts each), so ``None`` is the comparison layer's
    "this side has no such key". The ``name@all`` cells pair the two sides'
    own ``"all"`` for the per-item tables; the rows here are the keys as
    stored, ``map@{all_k}`` among them, so those cells are left out.
    """
    stored = [key for key in cells
              if str(key).rpartition("@")[2] != ALL_LABEL]

    def kept(side: str, all_k: int) -> list:
        return drop_metric_keys_above_all_k(
            [key for key in stored if cells[key][side] is not None], all_k)

    this, reference = kept(THIS, all_k_this), kept(REFERENCE, all_k_reference)
    rows = sorted(set(this) | set(reference))
    return {DISPLAY_ROWS: rows, THIS: this, REFERENCE: reference,
            DIFF: [key for key in rows if key in this and key in reference]}

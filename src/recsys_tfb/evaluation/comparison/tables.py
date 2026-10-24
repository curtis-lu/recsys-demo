"""What more than one section of the comparison report draws (#537) — not a
section. A helper only one section uses stays in that section's file.

* :func:`item_grain_blocks` — the compared per-K tables of an item grain, one
  per entry of a section's list, rows the items under a pinned Macro 平均,
  with the ⚠ marks and the lines under them. The item grain's section and the
  category grain's draw through it, so the two cannot mark their caveats
  apart.
* :func:`landed_cells` — a row's cells, refusing one that lacks a cell the
  section's list names (``shared.landed_value``: the refusal names the node
  to re-run from).
* :func:`metric_names`, :func:`require_grain` — a list's metrics in words,
  and the check that a list reads the grain its section draws.

Reads only landed results (``source_comparison``, ``comparison_display_ks``)
and the catalog (S9, ADR-0031 principle 1).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from recsys_tfb.evaluation.metric_catalog import CompareEntry
from recsys_tfb.evaluation.report_sections.metric_tables import (
    SliceRow,
    comparability,
    metric_table,
)
from recsys_tfb.evaluation.report_sections.shared import (
    COMPARISON_DISPLAY_KS,
    MACRO_LABEL,
    SOURCE_COMPARISON,
    both_present,
    landed_value,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    ALL_LABEL,
    CELLS,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    ENTRIES,
    MACRO_ITEMS_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
)
from recsys_tfb.report.types import Note

#: The catalog name of this run's side's row facts on the common universe
#: (``compute_compare_metrics``), as the comparison report reads them: the
#: item count a macro line says "all" of.
COMPARE_ROW_FACTS_A = "compare_row_facts_a"


def compared_key(entry: CompareEntry, k: int, all_k: int) -> str:
    """The cell a compared table's column at ``k`` reads — the rule of
    ``metric_tables.metric_table``: ``name@all`` at the grain's ``all_k``
    (the comparison's pairing of each side's own "all"), else ``name@{k}``;
    a metric without K, its one key."""
    return entry.metric.key(ALL_LABEL if k == all_k else k)


def landed_cells(comparison: dict, path: Sequence, keys: Iterable[str]) -> dict:
    """The cells at ``path`` of the landed comparison (``("per_item", CELLS,
    item)``, ``("category", "macro_by_item", CELLS)`` …), after checking that
    each of ``keys`` is among them.

    A row the comparison holds carries every key either side computed, at
    every K of the grid, so a key it lacks is a comparison landed off results
    computed for other K, or by an older node — not a blank to print, which
    would read as "no value". ``shared.landed_value`` raises naming the node
    to re-run from.
    """
    cells = landed_value(comparison, SOURCE_COMPARISON, *path)
    for key in keys:
        landed_value(comparison, SOURCE_COMPARISON, *path, key)
    return cells


def require_grain(entries: Iterable[CompareEntry], grain: str,
                  section: str) -> None:
    """Raise when an entry of a section's list is read at another grain than
    the one the section draws: its cells would be looked up in the wrong
    block and print blank."""
    wrong = [e for e in entries if e.grain != grain]
    if wrong:
        raise ValueError(
            f"{section} draws grain {grain!r}; its list holds {wrong}")


def metric_names(entries: Iterable[CompareEntry]) -> str:
    """A list's metrics in words, by their catalog display names:
    ``map_attr 與 per-item recall``, ``a、b 與 c``."""
    names = list(dict.fromkeys(e.metric.display_name for e in entries))
    if len(names) <= 1:
        return "".join(names)
    return f"{'、'.join(names[:-1])} 與 {names[-1]}"


def _has_entries(block: dict) -> bool:
    return any(block[ENTRIES][side] for side in (THIS, REFERENCE))


def item_grain_blocks(
    comparison: dict,
    cut: dict,
    entries: Sequence[CompareEntry],
    *,
    grain_path: tuple = (),
    row_header: str,
    slices: str,
    macro_count_label: str,
    n_items: int,
) -> list:
    """One compared table per entry of ``entries`` at an item grain — the
    item grain (``grain_path=()``) or the category grain (``("category",)``)
    of the landed comparison — then the lines under them.

    * Columns: the grain's K of the cut (``compare_k``, ``all_k``), three per
      K. Rows: Macro 平均 (pinned) when both sides had a macro, then the
      items in the comparison's order.
    * ⚠ (the comparison demo's case 1 and 2): the ``@{all_k}`` columns when
      the two sides' "all" differ and that column is shown; the Macro 平均
      row when the two sides averaged different items. Each says why in one
      line after the tables, once for all of them.
    * ``macro_count_label`` (``參與 macro 的 item 數``): the line naming how
      many of the ``n_items`` (this side's row facts; 0 = unknown) the macro
      averaged over, printed only when that is fewer than all and the two
      sides agree on them (ADR-0031 decision 16 item 5).
    """
    ks = landed_value(cut, COMPARISON_DISPLAY_KS, *grain_path,
                      DISPLAY_COMPARE_K)
    all_k = landed_value(cut, COMPARISON_DISPLAY_KS, *grain_path,
                         DISPLAY_ALL_K)
    population = landed_value(comparison, SOURCE_COMPARISON, *grain_path,
                              POPULATION_KEY)
    flags = comparability(population)
    per_item = landed_value(comparison, SOURCE_COMPARISON, *grain_path,
                            "per_item")
    macro = landed_value(comparison, SOURCE_COMPARISON, *grain_path,
                         "macro_by_item")
    macro_row = both_present(macro)
    warn_ks = [all_k] if flags.all_k_differs and all_k in ks else []

    blocks: list = []
    for entry in entries:
        keys = [compared_key(entry, k, all_k) for k in ks]
        rows = []
        if macro_row:
            # An empty macro (no item reached metric.min_positives) holds no
            # cell at all; its row is blank, not stale.
            rows.append(SliceRow(
                MACRO_LABEL,
                landed_cells(comparison, (*grain_path, "macro_by_item", CELLS),
                             keys if _has_entries(macro) else ()),
                pinned=True, warn=flags.macro_items_differs))
        rows += [
            SliceRow(item, landed_cells(
                comparison, (*grain_path, "per_item", CELLS, item), keys))
            for item in per_item[CELLS]
        ]
        blocks.append(metric_table(
            entry, ks, rows, row_header=row_header, slices=slices,
            compare=True, all_k=all_k, warn_ks=warn_ks))

    if warn_ks:
        blocks.append(flags.note(ALL_K_KEY))
    if macro_row and flags.macro_items_differs:
        blocks.append(flags.note(MACRO_ITEMS_KEY))
    elif macro_row:
        n_macro = len(population[MACRO_ITEMS_KEY][THIS])
        if 0 < n_items and n_macro < n_items:
            blocks.append(Note(
                f"{macro_count_label}：{n_macro:,}（全部 {n_items:,} 個）"))
    return blocks

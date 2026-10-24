"""排序指標 (ADR-0031 decision 16): the ranking metrics at each grain, one
table per metric — rows are slices, columns the K.

**The tables are the catalog's** (``metric_catalog``). Every per-K metric of
the query-group method gets a table of the overall and each segment
(``map@K · 整體與各分群``); every per-K metric of the item method a table of
the Macro average and each item (``map_attr@K · 各 item``). Both at the item
grain, and again at the category grain when the bundle has one. The lists are
read off the catalog when the section is drawn (:func:`metrics_at_k`), so a
metric added to the catalog gets its tables with no change here. A metric
without a K (``mean_pos``) is not drawn here: ``rank_distribution`` shows it.

**The K** are the cut's (``evaluation_display_ks``' ``metrics_section``, each
grain its own), resolved to integers (:func:`shown_ks`): the header writes
``@6``, not ``@all``. The cut lands them; this section cuts nothing.

The totals (query groups, rows, positives) are not repeated here: they are in
核心概念 (decision 16 item 12). The one count printed is the slice the Macro
row averages, under an item table, when it is fewer than all the items
(decision 16 item 5; ``population.macro_items``, landed by the metrics).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from recsys_tfb.evaluation.metric_catalog import (
    ITEM,
    MODEL_SELECTION_METRICS,
    QUERY_GROUP,
    Metric,
    metrics_of,
)
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.metric_tables import (
    SliceRow,
    definitions,
    macro_count_note,
    metric_table,
)
from recsys_tfb.evaluation.report_sections.shared import (
    DISPLAY_KS,
    MACRO_LABEL,
    count_items,
    k_to_lookup,
    landed_value,
    unmatched_last,
)
from recsys_tfb.evaluation.result_readers import (
    DISPLAY_ALL_K,
    DISPLAY_METRICS_SECTION,
    POPULATION_KEY,
    SEGMENT_COLUMNS_KEY,
    macro_item_count,
)
from recsys_tfb.report.types import Heading, Note, Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the item counts off the row facts, the values off the metrics, each
#: grain's K off the cut (#536).
READS = ("evaluation_row_facts", "evaluation_metrics", DISPLAY_KS)

#: The ``evaluation.report.sections`` switch that turns this section off
#: (ADR-0031 principle 3: off, the pipeline computes nothing only it reads).
SWITCH = "ranking_metrics"

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "排序指標"
TITLE_EN = "Ranking metrics"
QUESTION = "模型在各粒度排得怎樣？"

#: The label of the overall row of a query-group table.
OVERALL_LABEL = "整體"


@dataclass(frozen=True)
class _Grain:
    """What one grain's tables are called: its heading, what an item-method
    table's rows are (items, or categories) and whether they are categories
    (the line saying how many of them the Macro row averages,
    ``metric_tables.macro_count_note``)."""

    heading: str
    row_header: str
    slices: str
    category: bool


_ITEM_GRAIN = _Grain("item 粒度", "item", "各 item", category=False)
_CATEGORY_GRAIN = _Grain("大類粒度", "大類", "各大類", category=True)


def shown_ks(landed: Sequence, all_k: int) -> list[int]:
    """The K of a landed list as the table header writes them: ``"all"``
    resolved to ``all_k`` (``shared.k_to_lookup``), each K once — ``"all"``
    and an integer equal to it are one column — in the landed order."""
    out: list[int] = []
    for k in landed:
        resolved = int(k_to_lookup(k, all_k))
        if resolved not in out:
            out.append(resolved)
    return out


def metrics_at_k(method: str) -> list[Metric]:
    """The catalog's per-K metrics of ``method``, one table each, read when
    the section is drawn: the one a model is selected by first
    (``MODEL_SELECTION_METRICS``' sources — ``map``, ``map_attr``: the order
    of decision 16's demo and of the comparison report), the rest in catalog
    order."""
    selected = {(m.source.name, m.source.method)
                for m in MODEL_SELECTION_METRICS}
    return sorted((m for m in metrics_of(method) if m.per_k),
                  key=lambda m: (m.name, m.method) not in selected)


def _segment_rows(bundle: Mapping) -> list[SliceRow]:
    """One row per segment of ``bundle["per_segment"]``, the unmatched group
    last (``shared.unmatched_last``). ``per_segment`` slices by the first of
    the bundle's segment columns; with more than one, the label names the
    column (``segment seg_1``)."""
    per_segment = unmatched_last(bundle.get("per_segment") or {})
    columns = (bundle.get(POPULATION_KEY) or {}).get(SEGMENT_COLUMNS_KEY) or []
    prefix = f"{columns[0]} " if len(columns) > 1 else ""
    return [SliceRow(f"{prefix}{segment}", values)
            for segment, values in per_segment.items()]


def _macro_notes(bundle: Mapping, n_items: int, grain: _Grain) -> list[Note]:
    """The line under an item table when the Macro row averages fewer than
    all ``n_items`` (bug 5, ADR-0020; decision 16 item 5;
    ``metric_tables.macro_count_note``): the count is the bundle's
    ``population.macro_items`` (``result_readers.macro_item_count``), the
    list the metrics averaged — not ``min_positives`` applied again."""
    note = macro_count_note(macro_item_count(bundle), n_items,
                            category=grain.category)
    return [note] if note else []


def _grain_blocks(bundle: Mapping, ks: list[int], n_items: int, grain: _Grain,
                  *, with_definitions: bool) -> list:
    """One grain: its heading, a table per query-group metric (整體 pinned,
    then each segment), a table per item metric (Macro 平均 pinned, then
    each item in name order). The definitions follow each group of tables at
    the item grain only: the category grain's tables are the same metrics."""
    blocks: list = [Heading(grain.heading)]
    segment_rows = _segment_rows(bundle)
    group_rows = [SliceRow(OVERALL_LABEL, bundle.get("overall") or {},
                           pinned=True), *segment_rows]
    group_slices = "整體與各分群" if segment_rows else "整體"
    group_metrics = metrics_at_k(QUERY_GROUP)
    blocks += [metric_table(m, ks, group_rows, row_header="分群",
                            slices=group_slices) for m in group_metrics]
    if with_definitions:
        blocks.append(definitions(group_metrics))

    # The Macro row only when the bundle has a macro: an empty one (no item
    # reached metric.min_positives) would be a row of blanks, and the count
    # under the table is about that row.
    macro = (bundle.get("macro_avg") or {}).get("by_item") or {}
    item_rows = [SliceRow(MACRO_LABEL, macro, pinned=True)] if macro else []
    item_rows += [SliceRow(str(item), values) for item, values
                  in sorted((bundle.get("per_item") or {}).items())]
    notes = _macro_notes(bundle, n_items, grain) if macro else []
    item_metrics = metrics_at_k(ITEM)
    blocks += [metric_table(m, ks, item_rows, row_header=grain.row_header,
                            slices=grain.slices, notes=notes)
               for m in item_metrics]
    if with_definitions:
        blocks.append(definitions([*item_metrics, "macro"]))
    return blocks


def _description(ks: list[int], all_k: int) -> str:
    """The section's lead: how to read a table, the two methods with their
    metrics (from the catalog), and what the column at "all" means."""
    def names(method: str) -> str:
        return "、".join(m.display_name for m in metrics_at_k(method))

    text = (
        "模型在各粒度排得怎樣。每張表一個指標：列是切片，欄是 K。指標有兩種"
        f"算法：每個 query group 先算再平均（{names(QUERY_GROUP)}），以及每個 "
        f"item 一份、再對 item 平均成 Macro（{names(ITEM)}）。"
    )
    if all_k in ks:
        text += (
            f"@{all_k} 是全部候選：recall 恆為 1，precision 等於有正例那些組的"
            "正例比例，兩者都不再反映排序。"
        )
    return text


def build(
    results: Mapping[str, dict], parameters: dict,
) -> ReportSection | None:
    """排序指標: the description, then the item grain's tables and, with a
    category grain, the category grain's — the same skeleton (decision 16
    item 6)."""
    facts = results["evaluation_row_facts"]
    metrics = results["evaluation_metrics"]
    display_ks = results[DISPLAY_KS]

    def landed_ks(*grain) -> tuple[list[int], int]:
        # The list first: a cut landed without it is refused naming it.
        landed = landed_value(display_ks, DISPLAY_KS, "evaluation_metrics",
                              *grain, DISPLAY_METRICS_SECTION)
        all_k = landed_value(display_ks, DISPLAY_KS, "evaluation_metrics",
                             *grain, DISPLAY_ALL_K)
        return shown_ks(landed, all_k), all_k

    ks, all_k = landed_ks()
    blocks: list = [Paragraph(_description(ks, all_k), muted=True)]
    blocks += _grain_blocks(metrics, ks, count_items(facts), _ITEM_GRAIN,
                            with_definitions=True)
    category = metrics.get("category")
    if category:
        category_ks, _ = landed_ks("category")
        blocks += _grain_blocks(
            category, category_ks, count_items(facts.get("category") or {}),
            _CATEGORY_GRAIN, with_definitions=False)
    return ReportSection(
        title=TITLE, description="", title_en=TITLE_EN, question=QUESTION,
        key="ranking_metrics", blocks=blocks,
    )

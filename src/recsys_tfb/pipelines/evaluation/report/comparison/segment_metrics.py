"""分群指標 of the comparison report: the query-group metrics of
:data:`COMPARED` on the whole and per segment, both sides and their
difference. The list is empty by default (ADR-0031 decision 16 item 2), so
by default the section is not on the page.

When the two sides were computed with different segment columns, no table is
drawn: a table with one side's segments only cannot answer whether to
switch models, so the 「能不能比」 table alone says so (decision 16 item 15).
"""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.pipelines.evaluation.report.comparison.tables import (
    landed_cells,
    metric_names,
    require_grain,
)
from recsys_tfb.evaluation.metric_catalog import CompareEntry
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import (
    SliceRow,
    comparability,
    compared_key,
    definitions,
    metric_table,
)
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    COMPARISON_DISPLAY_KS,
    SOURCE_COMPARISON,
    has_entries,
    landed_value,
    unmatched_last,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    CELLS,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    POPULATION_KEY,
    REFERENCE,
    SCORED_QUERIES_KEY,
    THIS,
)
from recsys_tfb.report.types import Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = (SOURCE_COMPARISON, COMPARISON_DISPLAY_KS)

#: No switch turns this section off (ADR-0031 principle 3:
#: ``shared.sections_on``).
SWITCH = None

#: The metrics this section compares on the whole and per segment, in table
#: order: a program constant, not a setting, empty by default (decision 16
#: item 2). Each entry is read at ``per_segment``, e.g.
#: ``CompareEntry("map", QUERY_GROUP, "per_segment")``.
COMPARED: tuple[CompareEntry, ...] = ()

#: The heading, its English and the question the section answers.
TITLE = "分群指標"
TITLE_EN = "Per segment"
QUESTION = "整體與各分群，兩邊各多少？"

#: The pinned first row: the whole (``overall``).
_WHOLE = "整體"


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection | None:
    """分群指標：:data:`COMPARED` 每個指標一張「整體與各分群」比較表。

    ``None`` when the list is empty or the two sides' segment columns
    differ. ⚠ as the comparison demo marks it: the ``@{all_k}`` columns when
    the two sides' "all" differ, and a line under the table for that and for
    different query groups scored.
    """
    if not COMPARED:
        return None
    require_grain(COMPARED, "per_segment", __name__)
    comparison = results[SOURCE_COMPARISON]
    cut = results[COMPARISON_DISPLAY_KS]
    flags = comparability(landed_value(comparison, SOURCE_COMPARISON,
                                       POPULATION_KEY))
    if flags.segment_columns_differs:
        return None
    ks = landed_value(cut, COMPARISON_DISPLAY_KS, DISPLAY_COMPARE_K)
    all_k = landed_value(cut, COMPARISON_DISPLAY_KS, DISPLAY_ALL_K)
    overall = landed_value(comparison, SOURCE_COMPARISON, "overall")
    per_segment = landed_value(comparison, SOURCE_COMPARISON, "per_segment")
    whole_landed = has_entries(overall, THIS) or has_entries(overall, REFERENCE)
    warn_ks = [all_k] if flags.all_k_differs and all_k in ks else []
    notes = [n for n in (flags.note(ALL_K_KEY) if warn_ks else None,
                         flags.note(SCORED_QUERIES_KEY)) if n]

    blocks: list = [Paragraph(
        f"整體與各分群的 {metric_names(COMPARED)}，兩邊各多少。差＝這次 − 對照。",
        muted=True)]
    for entry in COMPARED:
        keys = [compared_key(entry, k, all_k) for k in ks]
        rows = [SliceRow(_WHOLE, landed_cells(
            comparison, ("overall", CELLS), keys if whole_landed else ()),
            pinned=True)]
        rows += [SliceRow(segment, landed_cells(
            comparison, ("per_segment", CELLS, segment), keys))
            for segment in unmatched_last(per_segment[CELLS])]
        blocks.append(metric_table(
            entry, ks, rows, row_header="分群", slices="整體與各分群",
            compare=True, all_k=all_k, warn_ks=warn_ks))
    # Once after the tables, for all of them (the demo's place).
    blocks += notes
    blocks.append(definitions(COMPARED))
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="segment_metrics",
                         blocks=blocks)

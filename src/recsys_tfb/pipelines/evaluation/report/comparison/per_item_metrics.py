"""各 item 指標 of the comparison report: the per-item metrics of
:data:`COMPARED`, both sides and their difference — one table each, rows the
items under a pinned Macro 平均, columns ``compare_k`` (ADR-0031 decision 16
items 2 and 7: the per-item guardrails, both at ``compare_k``)."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.pipelines.evaluation.report.comparison.tables import (
    COMPARE_ROW_FACTS_A,
    item_grain_blocks,
    metric_names,
    require_grain,
)
from recsys_tfb.evaluation.metric_catalog import ITEM, CompareEntry
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import definitions
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    COMPARISON_DISPLAY_KS,
    SOURCE_COMPARISON,
    count_items,
    has_entries,
    landed_value,
)
from recsys_tfb.evaluation.result_readers import REFERENCE
from recsys_tfb.report.types import Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1);
#: this side's row facts give the item count the macro line says "all" of.
READS = (SOURCE_COMPARISON, COMPARISON_DISPLAY_KS, COMPARE_ROW_FACTS_A)

#: No switch turns this section off (ADR-0031 principle 3:
#: ``shared.sections_on``).
SWITCH = None

#: The metrics this section compares, in table order (ADR-0031 decision 16
#: item 2): a program constant, not a setting. Each is a catalog entry read
#: at the item grain; a table is drawn for every one, so adding an entry the
#: computation lands adds a table and nothing else.
COMPARED: tuple[CompareEntry, ...] = (
    CompareEntry("map_attr", ITEM, "per_item"),
    CompareEntry("recall", ITEM, "per_item"),
)

#: The heading, its English and the question the section answers.
TITLE = "各 item 指標"
TITLE_EN = "Per item"
QUESTION = f"各 item 的 {metric_names(COMPARED)}，兩邊各多少？"


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection | None:
    """各 item 指標：:data:`COMPARED` 每個指標一張比較表，再加表下說明與定義。

    ``None`` when the list is empty or the compared side holds no per-item
    entry (nothing to compare against).
    """
    comparison = results[SOURCE_COMPARISON]
    per_item = landed_value(comparison, SOURCE_COMPARISON, "per_item")
    if not COMPARED or not has_entries(per_item, REFERENCE):
        return None
    require_grain(COMPARED, "per_item", __name__)
    blocks: list = [Paragraph(
        f"各 item 的 {metric_names(COMPARED)}，兩邊各多少。差＝這次 − 對照。",
        muted=True)]
    blocks += item_grain_blocks(
        comparison, results[COMPARISON_DISPLAY_KS], COMPARED,
        row_header="item", slices="各 item",
        n_items=count_items(results[COMPARE_ROW_FACTS_A]))
    blocks.append(definitions([*COMPARED, "macro"]))
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="per_item_metrics",
                         blocks=blocks)

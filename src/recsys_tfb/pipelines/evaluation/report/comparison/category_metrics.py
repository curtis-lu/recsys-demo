"""大類指標 of the comparison report: the category grain's metrics of
:data:`COMPARED`, both sides and their difference, drawn like 各 item 指標
(``tables.item_grain_blocks``) at the category grain's own K (ADR-0031
decision 16 item 2). Only when categories are on and both sides have the
grain (the comparison layer compares it only then)."""

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
    landed_value,
)
from recsys_tfb.report.types import Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = (SOURCE_COMPARISON, COMPARISON_DISPLAY_KS, COMPARE_ROW_FACTS_A)

#: No switch turns this section off (ADR-0031 principle 3:
#: ``shared.sections_on``).
SWITCH = None

#: The metrics this section compares at the category grain, in table order
#: (ADR-0031 decision 16 item 2): a program constant, not a setting.
COMPARED: tuple[CompareEntry, ...] = (
    CompareEntry("map_attr", ITEM, "category"),
    CompareEntry("recall", ITEM, "category"),
)

#: The heading, its English and the question the section answers.
TITLE = "大類指標"
TITLE_EN = "Per category"
QUESTION = "大類粒度，兩邊各多少？"


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection | None:
    """大類指標：:data:`COMPARED` 每個指標一張比較表（列是大類），再加表下說明
    與定義。``None`` when categories are off, when either side lacks the
    grain, or when the list is empty."""
    eval_params = parameters.get("evaluation", {}) or {}
    if not (eval_params.get("item_categories", {}) or {}).get("enabled"):
        return None
    category = landed_value(results[SOURCE_COMPARISON], SOURCE_COMPARISON,
                            "category")
    if category is None or not COMPARED:
        return None
    require_grain(COMPARED, "category", __name__)
    facts = results[COMPARE_ROW_FACTS_A].get("category") or {}
    blocks: list = [Paragraph(
        f"大類粒度的 {metric_names(COMPARED)}，只列兩邊共同的大類。"
        "差＝這次 − 對照。", muted=True)]
    blocks += item_grain_blocks(
        results[SOURCE_COMPARISON], results[COMPARISON_DISPLAY_KS], COMPARED,
        grain_path=("category",), row_header="大類", slices="各大類",
        n_items=count_items(facts))
    blocks.append(definitions([*COMPARED, "macro"]))
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="category_metrics",
                         blocks=blocks)

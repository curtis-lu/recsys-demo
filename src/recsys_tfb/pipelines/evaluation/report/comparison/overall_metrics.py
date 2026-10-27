"""整體指標 of the comparison report: the metrics training's HPO selects a
model by (``metric_catalog.MODEL_SELECTION_METRICS``), each at K = all, both
sides and their difference (ADR-0031 decision 16 items 1 and 2)."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.pipelines.evaluation.report.comparison.tables import landed_cells
from recsys_tfb.evaluation.metric_catalog import MODEL_SELECTION_METRICS
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import (
    REFERENCE_LABEL,
    THIS_LABEL,
    definitions,
    model_selection_table,
)
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    SOURCE_COMPARISON,
    has_entries,
    landed_value,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_LABEL,
    CELLS,
    ENTRIES,
    MACRO_ITEMS_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
)
from recsys_tfb.report.types import Note, Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = (SOURCE_COMPARISON,)

#: No switch turns this section off (ADR-0031 principle 3:
#: ``shared.sections_on``).
SWITCH = None

#: The heading, its English and the question the section answers. Neither
#: counts the metrics. The raw-score APs HPO can select by are not compared
#: (``MODEL_SELECTION_ON_PREDICTION_QUALITY``, #577).
TITLE = "整體指標"
TITLE_EN = "Overall"
QUESTION = "模型選擇指標，兩邊各多少？"

DESCRIPTION = "模型選擇指標，兩邊各多少。差＝這次 − 對照。"

#: Where a model-selection metric's cell sits in the comparison, by the
#: grain its source is read at (``metric_tables.model_selection_table``).
_BLOCK_OF_GRAIN = {"overall": "overall", "macro": "macro_by_item"}


def _check_landed(comparison: dict) -> None:
    """Each metric's ``name@all`` cell is in the comparison wherever its block
    holds any entry: a block computed without "all" was landed for other
    settings, and its row would print blank."""
    for metric in MODEL_SELECTION_METRICS:
        block = _BLOCK_OF_GRAIN[metric.source.grain]
        landed = landed_value(comparison, SOURCE_COMPARISON, block)
        if any(landed[ENTRIES][side] for side in (THIS, REFERENCE)):
            landed_cells(comparison, (block, CELLS),
                         [metric.source.metric.key(ALL_LABEL)])


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """整體指標：模型選擇指標表（每個指標一列，這次／對照／差）與定義。

    The ⚠ of the two sides averaging different macro items, and the line for
    different query groups scored, are the table's own
    (``model_selection_table``). Two blanks get a line saying why: a side
    with no query group to compare (every query zero positives; its 差 stays
    blank, never the other side's value — ADR-0020 bug 4), and a macro no
    item reached on either side.
    """
    comparison = results[SOURCE_COMPARISON]
    _check_landed(comparison)
    blocks: list = [Paragraph(DESCRIPTION, muted=True),
                    model_selection_table(comparison, compare=True)]
    overall = landed_value(comparison, SOURCE_COMPARISON, "overall")
    for side, label in ((THIS, THIS_LABEL), (REFERENCE, REFERENCE_LABEL)):
        if not has_entries(overall, side):
            blocks.append(Note(f"{label}沒有可比的 query group（全部零正例），"
                               f"{label}與差留空。"))
    macro_items = landed_value(comparison, SOURCE_COMPARISON, POPULATION_KEY,
                               MACRO_ITEMS_KEY)
    if not macro_items[THIS] and not macro_items[REFERENCE]:
        blocks += [Note(f"{m.display_name} 沒有值：進 Macro 平均的 item 兩邊都是 "
                        "0 個（只算正例數達到 metric.min_positives 的 item）。")
                   for m in MODEL_SELECTION_METRICS if m.source.grain == "macro"]
    blocks.append(definitions(MODEL_SELECTION_METRICS))
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="overall_metrics",
                         blocks=blocks)

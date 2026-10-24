"""概覽: the metrics training's HPO selects a model by, side by side, each
at K = all (ADR-0031 decision 16 item 1)."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.evaluation.metric_catalog import MODEL_SELECTION_METRICS
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.metric_tables import (
    definitions,
    model_selection_table,
)
from recsys_tfb.evaluation.result_readers import macro_item_count
from recsys_tfb.report.types import Note, Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_metrics",)

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "概覽"
TITLE_EN = "Overview"
QUESTION = "模型選擇指標各是多少？"

#: What the section says above its table. It does not count the metrics:
#: the catalog lists two today (``MODEL_SELECTION_METRICS``), and the two
#: binary-prediction ones join it later (``MODEL_SELECTION_PENDING``), when
#: this sentence gains theirs.
DESCRIPTION = (
    "training 調參（HPO）與挑模型時能選的指標，並列、不分主次；它們都只看"
    "同一個 query group 裡的先後。"
)


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """概覽：模型選擇指標表（每個指標一列，值是 K＝全部的那一格）與它們的定義。

    No headline, no ranking among them (decision 16 item 1): each row is the
    value the metrics computed — ``map@{all_k}`` of ``overall`` and
    ``map_attr@{all_k}`` of ``macro_avg.by_item`` — read by
    ``metric_tables.model_selection_table``, so the report holds one number
    per metric. The page's scale lives in 核心概念's totals; the table of
    contents replaces the old navigation table.
    """
    metrics = results["evaluation_metrics"]
    blocks = [Paragraph(DESCRIPTION, muted=True), model_selection_table(metrics)]
    # No item reached the macro (e.g. metric.min_positives above every
    # item's positives): its row is blank, and a bare blank reads as "computed,
    # came out empty". Say why, off the ledger's macro items (#536).
    if macro_item_count(metrics) == 0:
        blocks += [Note(f"{m.display_name} 沒有值：進 Macro 平均的 item 是 0 個"
                        "（只算正例數達到 metric.min_positives 的 item）。")
                   for m in MODEL_SELECTION_METRICS if m.source.grain == "macro"]
    blocks.append(definitions(MODEL_SELECTION_METRICS))
    return ReportSection(
        title=TITLE,
        description="",
        title_en=TITLE_EN,
        question=QUESTION,
        key="overview",
        blocks=blocks,
    )

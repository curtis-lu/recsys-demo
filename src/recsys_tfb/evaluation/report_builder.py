"""The main report: its sections in page order (``SECTIONS``) and the page
around them. Each section is a module of ``report_sections``; this module
only orders them and adds the metadata block."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from recsys_tfb.evaluation.report import generate_html_report
from recsys_tfb.evaluation.report_sections import (
    completeness,
    core_concepts,
    data_stats,
    overview,
    popularity_baseline,
    prediction_quality,
    rank_distribution,
    ranking_metrics,
)
from recsys_tfb.evaluation.report_sections.shared import (
    eval_dates_display,
    refuse_pre_landed_reads,
    refuse_pre_ledger,
    refuse_pre_rename_per_item,
    results_read_by,
    sections_on,
)

#: The main report's sections, in page order (ADR-0031 decision 16 item 4).
#: Moving a section is moving its line; a section reads only the results it
#: declares (``READS``).
SECTIONS = (
    overview,
    core_concepts,
    data_stats,
    ranking_metrics,
    prediction_quality,
    rank_distribution,
    popularity_baseline,
    completeness,
)

#: The results that are metrics bundles, the ones with a per-item block. The
#: others carry a ``per_item`` of their own shape (the prediction-quality
#: payload's is keyed by item), which the pre-rename check must not read.
_METRICS_BUNDLES = ("evaluation_metrics", "baseline_metrics")

#: The page's title, and its English beside it.
TITLE = "模型評估報表"
TITLE_EN = "Model Evaluation Report"


def assemble_report(results: Mapping[str, dict], parameters: dict) -> str:
    """Assemble every switched-on section (``shared.sections_on``), in
    ``SECTIONS`` order, into the final HTML string.

    ``results`` holds the landed results by catalog name (``generate_report``
    builds it): what the switched-on sections read, the evaluation
    pipeline's requirement list, and nothing else — a switched-off section's
    result was not computed. Each section gets the ones it declares in
    ``READS`` and no other, so a section reading a result it did not declare
    fails here rather than drifting from its declaration. A section returns
    ``None`` when it has nothing to show. Before any section, a metrics
    bundle written before the per-item ``recall@K`` rename
    (``refuse_pre_rename_per_item``) or before the population ledger
    (``refuse_pre_ledger``) is refused, and so is a result landed before it
    carried what the report reads instead of computing
    (``refuse_pre_landed_reads``, #536).

    段落順序（ADR-0031 決定 16 第 4 項）：概覽 → 核心概念 → 資料統計 →
    排序指標 → 預測品質（開了才有）→ 名次分布 → 熱門度基準 → 完整性檢查。

    The metadata block holds the model version, the evaluated dates and when
    the page was drawn — nothing counted: the totals and the all-positive
    warning are 核心概念's (decision 16 item 12).
    """
    for name in _METRICS_BUNDLES:
        refuse_pre_rename_per_item(name, results.get(name))
        refuse_pre_ledger(name, results.get(name))
    refuse_pre_landed_reads(results)
    sections = []
    for section in sections_on(SECTIONS, parameters):
        built = section.build(
            results_read_by(section.__name__, section.READS, results),
            parameters)
        if built is not None:
            sections.append(built)
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "模型版本": parameters.get("model_version", "unknown"),
        "評估日期": eval_dates_display(eval_params.get("snap_date", "unknown")),
        "產生時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(
        sections, title=TITLE, metadata=metadata, title_en=TITLE_EN
    )

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
    glossary,
    overview,
    popularity_baseline,
    prediction_quality,
    rank_distribution,
    ranking_metrics,
)
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    ALL_POSITIVE_WHY,
    eval_dates_display,
    format_share,
    grain_bundles,
    refuse_pre_landed_reads,
    refuse_pre_ledger,
    refuse_pre_rename_per_item,
    results_read_by,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARN_SHARE,
    ALL_POSITIVE_WARNS_KEY,
    N_WITH_POSITIVE_KEY,
    drop_all_positive_groups,
)

#: The main report's sections, in page order. Moving a section is moving
#: its line; a section reads only the results it declares (``READS``).
SECTIONS = (
    overview,
    core_concepts,
    data_stats,
    ranking_metrics,
    prediction_quality,
    rank_distribution,
    popularity_baseline,
    completeness,
    glossary,
)

#: The results that are metrics bundles, the ones with a per-item block. The
#: others carry a ``per_item`` of their own shape (the prediction-quality
#: payload's is keyed by item), which the pre-rename check must not read.
_METRICS_BUNDLES = ("evaluation_metrics", "baseline_metrics")


def _all_positive_warnings(row_facts: dict) -> dict[str, str]:
    """報表頭 metadata 的警告列：每個落地的 ``all_positive_warns`` 為真的粒度一列。

    警不警告是列的事實每個粒度落地的帳（``metrics.all_positive_accounts``，
    #536；log 讀同一份），這裡只讀、不重算。放在 metadata：每個讀者都會經過，
    而且算進了這些組的 mAP 就在它下面第一段。只陳述數字與設定鍵，不評這個比例
    高不高。
    """
    out = {}
    for grain, bundle in grain_bundles(row_facts):
        if not bundle[ALL_POSITIVE_WARNS_KEY]:
            continue
        # The flag is true only when the share is known, so the share and
        # both counts are there.
        out[f"⚠ 全正 query group（{grain}）"] = (
            "有正例的 query group 裡，全正的佔 "
            f"{format_share(bundle[ALL_POSITIVE_SHARE_KEY])}"
            f"（{bundle[ALL_POSITIVE_KEY]:,}／"
            f"{bundle[N_WITH_POSITIVE_KEY]:,} 組），超過 "
            f"{ALL_POSITIVE_WARN_SHARE:.0%}。這些組{ALL_POSITIVE_WHY}，照樣算進"
            "衡量指標，模型與熱門度 baseline 在這些組的組層級指標也相同。要排除"
            f"它們，設 {ALL_POSITIVE_SWITCH}: true；組數帳在「衡量指標」段。"
        )
    return out


def assemble_report(results: Mapping[str, dict], parameters: dict) -> str:
    """Assemble every enabled section, in ``SECTIONS`` order, into the final
    HTML string.

    ``results`` holds the landed results by catalog name (``generate_report``
    builds it). Each section gets the ones it declares in ``READS`` and no
    other, so a section reading a result it did not declare fails here rather
    than drifting from its declaration. A section returns ``None`` when its
    switch is off or it has nothing to show. Before any section, a metrics
    bundle written before the per-item ``recall@K`` rename
    (``refuse_pre_rename_per_item``) or before the population ledger
    (``refuse_pre_ledger``) is refused, and so is a result landed before it
    carried what the report reads instead of computing
    (``refuse_pre_landed_reads``, #536).

    spine（目的驅動、由粗到細、克制）：概覽 → 核心概念 → 基本統計 →
    衡量指標 → 預測品質（開了才有）→ per-item 細部拆解 → baseline →
    完整性檢查 → 詞彙表。
    """
    for name in _METRICS_BUNDLES:
        refuse_pre_rename_per_item(name, results.get(name))
        refuse_pre_ledger(name, results.get(name))
    refuse_pre_landed_reads(results)
    sections = []
    for section in SECTIONS:
        built = section.build(
            results_read_by(section.__name__, section.READS, results),
            parameters)
        if built is not None:
            sections.append(built)
    row_facts = results["evaluation_row_facts"]
    eval_params = parameters.get("evaluation", {}) or {}
    configured_dates = eval_params.get("snap_date", "unknown")
    metadata = {
        "Model Version": parameters.get("model_version", "unknown"),
        "Snap Date": eval_dates_display(configured_dates),
    }
    metadata.update({
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Queries": row_facts.get("n_queries"),
    })
    # n_excluded_queries counts only the groups without a positive. With
    # drop_all_positive_groups on, the all-positive groups are excluded too,
    # and a bare "Excluded Queries" would read as the whole count (#376).
    if drop_all_positive_groups(parameters):
        metadata["Excluded Queries (no positive)"] = row_facts.get(
            "n_excluded_queries")
        metadata["Excluded Queries (all positive)"] = row_facts.get(
            ALL_POSITIVE_KEY)
    else:
        metadata["Excluded Queries"] = row_facts.get("n_excluded_queries")
    # The all-positive share warning, right under the counts it is about.
    metadata.update(_all_positive_warnings(row_facts))
    return generate_html_report(
        sections, title="Model Evaluation Report", metadata=metadata
    )

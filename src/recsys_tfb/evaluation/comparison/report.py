"""Assemble report_comparison.html from A/B compare result + coverage info."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.metric_catalog import ITEM, entry
from recsys_tfb.evaluation.metrics import (
    drop_all_positive_groups,
    resolved_all_k,
)
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.report_sections import glossary
from recsys_tfb.evaluation.report_sections.shared import (
    count_items,
    drop_metric_keys_above_all_k,
    eval_dates_display,
    macro_coverage_suffix_mb,
    per_item_metric_compare_table,
    resolve_display_k,
    results_read_by,
)

#: What ``assemble_comparison_report`` reads, by name: this run's side's row
#: facts (its item counts), both sides' metrics bundles, their comparison and
#: the common-universe coverage (``compare_coverage_partial``).
READS = ("row_facts_a", "metrics_a", "metrics_b", "comparison",
         "compare_coverage_partial")

#: The per-item metrics both per-item sections compare, by catalog entry,
#: each with the ``report.display`` key that lists its K (ADR-0031 decision
#: 11: the report declares what it compares).
_PER_ITEM_COMPARED = (
    (("recall", ITEM), "guardrail_recall_k", [1, 3, 5]),
    (("map_attr", ITEM), "primary_map_k", [1, 3, 5, "all"]),
)


def assemble_comparison_report(
    results: Mapping[str, dict], parameters: dict,
) -> str:
    """Compose the comparison report's sections, then the main report's
    glossary, from ``results`` (:data:`READS`)."""
    results = results_read_by("assemble_comparison_report", READS, results)
    sections = [build(results, parameters) for build in (
        _build_coverage_section,
        _build_overall_section,
        _build_per_item_section,
        _build_category_section,
    )]
    sections.append(glossary.build({}, parameters))
    sections = [s for s in sections if s is not None]
    comparison = results["comparison"]
    label_a = comparison["label_a"]
    label_b = comparison["label_b"]
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "Comparison": f"{label_a} vs {label_b}",
        "Snap Date": eval_dates_display(eval_params.get("snap_date", "unknown")),
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(
        sections,
        title=f"Model Comparison: {label_a} vs {label_b}",
        metadata=metadata,
    )


def _build_coverage_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection:
    comparison, cov = results["comparison"], results["compare_coverage_partial"]
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    # The query group, said in words. With `occasion` declared the count is
    # of occasions, and a sentence that still said "time × entity" would
    # describe a smaller number than the one printed beside it (#428).
    query_unit = "一個時間 × 一個 entity" + (
        " × 一個場合（occasion）"
        if get_schema(parameters).get("occasion") else ""
    )
    # #376: with the switch on, both sides dropped their all-positive groups,
    # each deciding on its own rows (generate_comparison_report), so "every
    # per-query metric divides by n_query_group" is false, and a group whose
    # candidate rows differ between the sides can be dropped on one side only.
    # Off, the sentence every report carried before stays word for word.
    denominator = (
        "本報表所有 per-query 指標的分母都是它的一部分："
        "evaluation.query_filter.drop_all_positive_groups 開著，兩側都排除了"
        "全正的 query group，而且各自依自己的列判定；同一組在兩側的候選列"
        "不對稱時，可能一側排除、一側保留。"
        if drop_all_positive_groups(parameters) else
        "本報表所有 per-query 指標都以它為分母，所以母體大小與指標同一個尺度。"
    )
    meta = pd.DataFrame(
        {
            label_a: [
                cov.get("kind_a", ""), cov.get("model_version_a", "n/a"),
                cov.get("table_a", "n/a"), cov.get("n_query_group_A_full"),
                cov.get("n_item_A_full"),
            ],
            label_b: [
                cov.get("kind_b", ""), cov.get("model_version_b", "n/a"),
                cov.get("table_b", "n/a"), cov.get("n_query_group_B_full"),
                cov.get("n_item_B_full"),
            ],
        },
        index=[
            "kind", "model_version", "Hive table",
            "n_query_group (full)", "n_item (full)",
        ],
    )
    coverage = pd.DataFrame(
        {
            "A_full": [
                cov.get("n_query_group_A_full"), cov.get("n_item_A_full"),
            ],
            "B_full": [
                cov.get("n_query_group_B_full"), cov.get("n_item_B_full"),
            ],
            "common (used)": [
                cov.get("n_query_group_common"), cov.get("n_item_common"),
            ],
        },
        index=["n_query_group", "n_item"],
    )
    dropped = pd.DataFrame(
        {
            f"{label_a} dropped items": [
                len(cov.get("dropped_items_A", []) or []),
                ", ".join(cov.get("dropped_items_A", []) or []) or "(none)",
            ],
            f"{label_b} dropped items": [
                len(cov.get("dropped_items_B", []) or []),
                ", ".join(cov.get("dropped_items_B", []) or []) or "(none)",
            ],
        },
        index=["count", "list"],
    )
    return ReportSection(
        title="Compare 概頁",
        description=(
            "兩個模型的來源、coverage、被剔除的 item。"
            f"n_query_group ＝「{query_unit}」的相異組合數，也就是排名的單位；"
            f"{denominator}"
            "n_item ＝相異 item 數。後續章節皆在 common universe 上重排重算。"
            "common 欄的 n_query_group 是裁切後兩側都還在的 query group 數"
            "（entity 欄為 NULL 的列在裁切時就被丟掉，不計入）。兩側候選對稱時，它就是兩側指標用到的母體；"
            "某個 group 在一側只剩對方沒有的 item 時，它只進得了另一側的指標，不算 common。"
            "n_item 是兩側裁切前 item 集合的交集。"
        ),
        tables=[meta, coverage, dropped],
        table_titles=["雙方 metadata", "coverage", "被 drop 的 items"],
    )


def _each_side_up_to_its_k(
    overall_a: dict, overall_b: dict, overall_d: dict, all_k_a: int, all_k_b: int,
) -> tuple[list, dict, dict, dict]:
    """``(keys, a, b, Δ)`` of an every-key table, each side cut at its own K.

    bug 8 (ADR-0020): these tables print every computed key, so a key whose
    @K is past the longest list is dropped — precision@K keeps falling there
    only because its denominator is K. The bound is each side's own
    ``metrics.resolved_all_k``: the two sides keep their own rows inside the
    common universe, so with ``event`` their widest groups, and so their
    ``"all"`` keys, can differ (#434). A single bound — it used to be side
    A's item count — hides whichever side's ``"all"`` row lies past it.

    Rows are the union of what each side keeps; a side's cell is blank where
    it cut the key, and so is the Δ. With equal bounds this is exactly the old
    single filter.
    """
    kept_a = drop_metric_keys_above_all_k(overall_a, all_k_a)
    kept_b = drop_metric_keys_above_all_k(overall_b, all_k_b)
    both = set(kept_a) & set(kept_b)
    return (
        sorted(set(kept_a) | set(kept_b)),
        {k: overall_a[k] for k in kept_a},
        {k: overall_b[k] for k in kept_b},
        {k: v for k, v in overall_d.items() if k in both},
    )


def _build_overall_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection:
    metrics_a, comparison = results["metrics_a"], results["comparison"]
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    overall_a = comparison["result_a"].get("overall", {}) or {}
    overall_b = comparison["result_b"].get("overall", {}) or {}
    keys, cells_a, cells_b, cells_d = _each_side_up_to_its_k(
        overall_a, overall_b, comparison["overall_delta"],
        resolved_all_k(metrics_a), resolved_all_k(comparison["result_b"]),
    )
    tbl = pd.DataFrame(
        {
            label_a: [cells_a.get(k) for k in keys],
            label_b: [cells_b.get(k) for k in keys],
            "Δ": [cells_d.get(k) for k in keys],
        },
        index=keys,
    )
    return ReportSection(
        title="overall metrics (M/B/Δ)",
        description=(
            "per-query 指標在 common (entity × item) universe 上重算。Δ = A − B，"
            "只在兩側都有值時才算，否則留空。"
        ),
        tables=[tbl],
        table_titles=[
            f"overall{_empty_overall_note(overall_a, overall_b, label_a, label_b)}"
        ],
    )


def _empty_overall_note(
    overall_a: dict, overall_b: dict, label_a: str, label_b: str
) -> str:
    """Title suffix naming each side whose ``overall`` is empty (ADR-0020 bug 4).

    ``overall`` is empty when every query on that side had zero positives, so
    no row has a Δ and the whole Δ column is blank. Without this line the
    reader sees an empty column and nothing saying why.
    """
    empty = [
        label for label, overall in ((label_a, overall_a), (label_b, overall_b))
        if not overall
    ]
    if not empty:
        return ""
    sides = "、".join(f"{label} 側無可比的 query（全部零正例）" for label in empty)
    return f" — {sides}，Δ 欄留空"


def _per_item_compare_tables(
    per_item_a: dict, per_item_b: dict, per_item_delta: dict, disp: dict,
    all_k_a: int, all_k_b: int, macro_a: dict | None, macro_b: dict | None,
    title_prefix: str, item_cov: str,
) -> tuple[list[pd.DataFrame], list[str]]:
    """One M/B/Δ table per :data:`_PER_ITEM_COMPARED` entry, labelled by the
    catalog's display name; the item and category grains share it so the two
    cannot come to compare different metrics."""
    tables, titles = [], []
    for compared, ks_key, ks_default in _PER_ITEM_COMPARED:
        metric = entry(*compared)
        label = metric.display_name
        ks = resolve_display_k(disp.get(ks_key, ks_default), all_k_a)
        tables.append(per_item_metric_compare_table(
            per_item_a, per_item_b, per_item_delta,
            ks, all_k_a, metric.name, f"{label}@{{k}}",
            macro_a=macro_a, macro_b=macro_b, all_k_b=all_k_b,
        ))
        titles.append(f"{title_prefix}{label}@k (M/B/Δ){item_cov}")
    return tables, titles


def _build_per_item_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    metrics_a, metrics_b = results["metrics_a"], results["metrics_b"]
    comparison = results["comparison"]
    per_item_a = metrics_a.get("per_item", {}) or {}
    per_item_b = metrics_b.get("per_item", {}) or {}
    per_item_delta = comparison.get("per_item_delta", {}) or {}
    if not per_item_b:
        return None

    disp = (
        (parameters.get("evaluation", {}) or {}).get("report", {}) or {}
    ).get("display", {}) or {}
    n_items = count_items(results["row_facts_a"])
    # Each side's "all" at its own K (#434); the int columns follow A's.
    all_k_a, all_k_b = resolved_all_k(metrics_a), resolved_all_k(metrics_b)
    macro_a = (metrics_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (metrics_b.get("macro_avg", {}) or {}).get("by_item")
    # bug 5 (ADR-0020): disclose each side's macro item coverage.
    item_cov = macro_coverage_suffix_mb(
        per_item_a, per_item_b, n_items, parameters, macro_a, macro_b
    )

    tables, titles = _per_item_compare_tables(
        per_item_a, per_item_b, per_item_delta, disp, all_k_a, all_k_b,
        # No "per-item" prefix: the section says it, and the per-item
        # recall's display name says it again.
        macro_a, macro_b, "", item_cov,
    )
    return ReportSection(
        title="per-item M/B/Δ",
        description="細產品粒度的 recall / map_attr,頂列 Macro 平均。",
        tables=tables,
        table_titles=titles,
    )


def _build_category_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    metrics_a, metrics_b = results["metrics_a"], results["metrics_b"]
    eval_params = parameters.get("evaluation", {}) or {}
    if not (eval_params.get("item_categories", {}) or {}).get("enabled"):
        return None
    cat_a = metrics_a.get("category")
    cat_b = metrics_b.get("category")
    if not cat_a or not cat_b:
        return None
    comparison_cat = build_comparison_result(
        cat_a, cat_b,
        label_a="Model_cat",  # internal labels only — display uses metadata
        label_b="Compare_cat",
    )
    per_item_a = cat_a.get("per_item", {}) or {}
    per_item_b = cat_b.get("per_item", {}) or {}
    per_item_delta = comparison_cat.get("per_item_delta", {}) or {}
    disp = (eval_params.get("report", {}) or {}).get("display", {}) or {}
    n_cat = count_items(results["row_facts_a"].get("category") or {})
    cat_k_a, cat_k_b = resolved_all_k(cat_a), resolved_all_k(cat_b)
    macro_a = (cat_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (cat_b.get("macro_avg", {}) or {}).get("by_item")

    tables, titles = [], []
    # bug 8 (ADR-0020): same per-side key filter as _build_overall_section,
    # against each side's category K — 3 categories must not print
    # precision@4 / @5.
    overall_a = cat_a.get("overall", {}) or {}
    overall_b = cat_b.get("overall", {}) or {}
    keys, cells_a, cells_b, cells_d = _each_side_up_to_its_k(
        overall_a, overall_b, comparison_cat["overall_delta"], cat_k_a, cat_k_b,
    )
    side_a, side_b = "Model", "Compare"
    overall_tbl = pd.DataFrame(
        {side_a: [cells_a.get(k) for k in keys],
         side_b: [cells_b.get(k) for k in keys],
         "Δ": [cells_d.get(k) for k in keys]},
        index=keys,
    )
    tables.append(overall_tbl)
    titles.append(
        f"大類 overall{_empty_overall_note(overall_a, overall_b, side_a, side_b)}"
    )
    # bug 5 (ADR-0020): same disclosure as the fine-grained per-item section.
    item_cov = macro_coverage_suffix_mb(
        per_item_a, per_item_b, n_cat, parameters, macro_a, macro_b
    )
    more_tables, more_titles = _per_item_compare_tables(
        per_item_a, per_item_b, per_item_delta, disp, cat_k_a, cat_k_b,
        macro_a, macro_b, "大類 ", item_cov,
    )
    tables += more_tables
    titles += more_titles
    return ReportSection(
        title="大類 Category M/B/Δ",
        description="大類粒度 overall + per-category recall/map_attr。只列雙方共通的大類。",
        tables=tables,
        table_titles=titles,
    )

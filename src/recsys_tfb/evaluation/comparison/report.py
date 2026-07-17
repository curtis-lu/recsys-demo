"""Assemble report_comparison.html from A/B compare result + coverage info."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.report_builder import (
    _per_item_metric_compare_table,
    _resolve_display_k,
    _k_to_lookup,
    _n_products,
    _visible_metric_keys,
    build_glossary_section,
)


def assemble_comparison_report(
    metrics_a: dict,
    metrics_b: dict,
    comparison: dict,
    coverage_info: dict,
    parameters: dict,
) -> str:
    """Compose the 4-section + glossary HTML."""
    sections = [
        _build_coverage_section(comparison, coverage_info, parameters),
        _build_overall_section(comparison),
        _build_per_item_section(metrics_a, metrics_b, comparison, parameters),
        _build_category_section(metrics_a, metrics_b, parameters),
        build_glossary_section(parameters),
    ]
    sections = [s for s in sections if s is not None]
    label_a = comparison["label_a"]
    label_b = comparison["label_b"]
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "Comparison": f"{label_a} vs {label_b}",
        "Snap Date": eval_params.get("snap_date", "unknown"),
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(
        sections,
        title=f"Model Comparison: {label_a} vs {label_b}",
        metadata=metadata,
    )


def _build_coverage_section(
    comparison: dict, cov: dict, parameters: dict
) -> ReportSection:
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    meta = pd.DataFrame(
        {
            label_a: [
                cov.get("kind_a", ""), cov.get("model_version_a", "n/a"),
                cov.get("table_a", "n/a"), cov.get("n_cust_A_full"),
                cov.get("n_prod_A_full"),
            ],
            label_b: [
                cov.get("kind_b", ""), cov.get("model_version_b", "n/a"),
                cov.get("table_b", "n/a"), cov.get("n_cust_B_full"),
                cov.get("n_prod_B_full"),
            ],
        },
        index=["kind", "model_version", "Hive table", "n_cust (full)", "n_prod (full)"],
    )
    coverage = pd.DataFrame(
        {
            "A_full": [cov.get("n_cust_A_full"), cov.get("n_prod_A_full")],
            "B_full": [cov.get("n_cust_B_full"), cov.get("n_prod_B_full")],
            "common (used)": [cov.get("n_cust_common"), cov.get("n_prod_common")],
        },
        index=["n_cust", "n_prod"],
    )
    dropped = pd.DataFrame(
        {
            f"{label_a} dropped prods": [
                len(cov.get("dropped_prods_A", []) or []),
                ", ".join(cov.get("dropped_prods_A", []) or []) or "(none)",
            ],
            f"{label_b} dropped prods": [
                len(cov.get("dropped_prods_B", []) or []),
                ", ".join(cov.get("dropped_prods_B", []) or []) or "(none)",
            ],
        },
        index=["count", "list"],
    )
    return ReportSection(
        title="Compare 概頁",
        description="兩個模型的來源、coverage、被剔除的細產品。後續章節皆在 common universe 上重排重算。",
        tables=[meta, coverage, dropped],
        table_titles=["雙方 metadata", "coverage", "被 drop 的 prods"],
    )


def _build_overall_section(comparison: dict) -> ReportSection:
    label_a, label_b = comparison["label_a"], comparison["label_b"]
    overall_a = comparison["result_a"].get("overall", {}) or {}
    overall_b = comparison["result_b"].get("overall", {}) or {}
    overall_d = comparison["overall_delta"]
    keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_d))
    )
    tbl = pd.DataFrame(
        {
            label_a: [overall_a.get(k) for k in keys],
            label_b: [overall_b.get(k) for k in keys],
            "Δ": [overall_d.get(k) for k in keys],
        },
        index=keys,
    )
    return ReportSection(
        title="overall metrics (M/B/Δ)",
        description="per-query 指標在 common (cust × prod) universe 上重算。Δ = A − B。",
        tables=[tbl],
        table_titles=["overall"],
    )


def _build_per_item_section(
    metrics_a: dict, metrics_b: dict, comparison: dict, parameters: dict
) -> ReportSection | None:
    per_item_a = metrics_a.get("per_item", {}) or {}
    per_item_b = metrics_b.get("per_item", {}) or {}
    per_item_delta = comparison.get("per_item_delta", {}) or {}
    if not per_item_b:
        return None

    disp = (
        (parameters.get("evaluation", {}) or {}).get("report", {}) or {}
    ).get("display", {}) or {}
    n_prod = _n_products(metrics_a)
    rec_ks = _resolve_display_k(disp.get("guardrail_recall_k", [1, 3, 5]), n_prod)
    attr_ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod)

    macro_a = (metrics_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (metrics_b.get("macro_avg", {}) or {}).get("by_item")

    tables, titles = [], []
    for metric_key, col_fmt, ks, title in (
        ("hit_rate", "recall@{k}", rec_ks, "per-item recall@k (M/B/Δ)"),
        ("map_attr", "map_attr@{k}", attr_ks, "per-item map_attr@k (M/B/Δ)"),
    ):
        tbl = _per_item_metric_compare_table(
            per_item_a, per_item_b, per_item_delta,
            ks, n_prod, metric_key, col_fmt,
            macro_a=macro_a, macro_b=macro_b,
        )
        tables.append(tbl)
        titles.append(title)
    return ReportSection(
        title="per-item M/B/Δ",
        description="細產品粒度的 recall / map_attr,頂列 Macro 平均。",
        tables=tables,
        table_titles=titles,
    )


def _build_category_section(
    metrics_a: dict, metrics_b: dict, parameters: dict
) -> ReportSection | None:
    eval_params = parameters.get("evaluation", {}) or {}
    if not (eval_params.get("product_categories", {}) or {}).get("enabled"):
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
    n_cat = int(
        (cat_a.get("dataset_overview", {}) or {}).get("totals", {}).get("n_products", 0)
    )
    rec_ks = _resolve_display_k(disp.get("guardrail_recall_k", [1, 3, 5]), n_cat)
    attr_ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_cat)
    macro_a = (cat_a.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (cat_b.get("macro_avg", {}) or {}).get("by_item")

    tables, titles = [], []
    overall_a = cat_a.get("overall", {}) or {}
    overall_b = cat_b.get("overall", {}) or {}
    overall_d = comparison_cat["overall_delta"]
    keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_d))
    )
    overall_tbl = pd.DataFrame(
        {"Model": [overall_a.get(k) for k in keys],
         "Compare": [overall_b.get(k) for k in keys],
         "Δ": [overall_d.get(k) for k in keys]},
        index=keys,
    )
    tables.append(overall_tbl)
    titles.append("大類 overall")
    for metric_key, col_fmt, ks, title in (
        ("hit_rate", "recall@{k}", rec_ks, "大類 per-item recall@k (M/B/Δ)"),
        ("map_attr", "map_attr@{k}", attr_ks, "大類 per-item map_attr@k (M/B/Δ)"),
    ):
        tbl = _per_item_metric_compare_table(
            per_item_a, per_item_b, per_item_delta,
            ks, n_cat, metric_key, col_fmt,
            macro_a=macro_a, macro_b=macro_b,
        )
        tables.append(tbl)
        titles.append(title)
    return ReportSection(
        title="大類 Category M/B/Δ",
        description="大類粒度 overall + per-category recall/map_attr。只列雙方共通的大類。",
        tables=tables,
        table_titles=titles,
    )

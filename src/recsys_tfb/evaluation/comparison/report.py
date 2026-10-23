"""Assemble report_comparison.html from the landed comparison results: the
common-universe coverage, this run's side's row facts, the comparison of the
two sides (``source_comparison``) and the K cut (``comparison_display_ks``).

It only reads and draws (ADR-0031 principle 1): every difference is the
comparison layer's, every K list and row list the cut's, and a value missing
from them is refused with the node to re-run (``shared.landed_value``).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metric_catalog import ITEM, entry
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.report_sections import glossary
from recsys_tfb.evaluation.report_sections.shared import (
    COMPARISON_DISPLAY_KS,
    SOURCE_COMPARISON,
    compared_macro_coverage_suffix,
    compared_per_item_table,
    compared_value,
    count_items,
    eval_dates_display,
    has_entries,
    landed_value,
    results_read_by,
)
from recsys_tfb.evaluation.result_readers import (
    CELLS,
    DIFF,
    DISPLAY_GUARDRAIL_RECALL_K,
    DISPLAY_PRIMARY_MAP_K,
    DISPLAY_ROWS,
    MACRO_ITEMS_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
    drop_all_positive_groups,
)

#: The catalog names of the coverage and of this run's side's row facts.
COVERAGE = "compare_coverage"
ROW_FACTS_A = "compare_row_facts_a"

#: What ``assemble_comparison_report`` reads, by name: the coverage, this
#: run's side's row facts (its item counts), the comparison and its K cut.
READS = (COVERAGE, ROW_FACTS_A, SOURCE_COMPARISON, COMPARISON_DISPLAY_KS)

#: The per-item metrics both per-item sections compare, by catalog entry,
#: each with the K cut's list its columns are (ADR-0031 decision 11: the
#: report declares what it compares).
_PER_ITEM_COMPARED = (
    (("recall", ITEM), DISPLAY_GUARDRAIL_RECALL_K),
    (("map_attr", ITEM), DISPLAY_PRIMARY_MAP_K),
)

#: This run's side's name in the report; the compared source's is its
#: ``label``, or this when it has none.
_LABEL_A = "Model"
_LABEL_B_DEFAULT = "Compare"


def _labels(parameters: dict) -> tuple[str, str]:
    """Both sides' names, read off today's parameters: ``label`` only names
    the compared source, so it is not part of what the results were computed
    against (``compare_sources.compare_source_identity``) and a renamed
    source redraws with the new name."""
    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    return _LABEL_A, src.get("label", _LABEL_B_DEFAULT)


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
    label_a, label_b = _labels(parameters)
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
    cov = results[COVERAGE]
    label_a, label_b = _labels(parameters)
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


def _every_key_table(block: dict, rows: dict, column_a: str,
                     column_b: str) -> pd.DataFrame:
    """An M/B/Δ table of every key of a compared block, one row per key the
    K cut kept (``display_ks.every_key_rows``): a side's cell where the cut
    kept that side's key, the difference where it kept both, blank
    elsewhere."""
    cells = block[CELLS]
    keys = rows[DISPLAY_ROWS]

    def column(side: str) -> list:
        return [compared_value(cells, key, side) if key in rows[side] else None
                for key in keys]

    return pd.DataFrame(
        {column_a: column(THIS), column_b: column(REFERENCE),
         "Δ": column(DIFF)},
        index=keys,
    )


def _build_overall_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection:
    comparison = results[SOURCE_COMPARISON]
    label_a, label_b = _labels(parameters)
    block = landed_value(comparison, SOURCE_COMPARISON, "overall")
    tbl = _every_key_table(
        block, landed_value(results[COMPARISON_DISPLAY_KS],
                            COMPARISON_DISPLAY_KS, "overall"),
        label_a, label_b)
    return ReportSection(
        title="overall metrics (M/B/Δ)",
        description=(
            "per-query 指標在 common (entity × item) universe 上重算。Δ = A − B，"
            "只在兩側都有值時才算，否則留空。"
        ),
        tables=[tbl],
        table_titles=[
            f"overall{_empty_overall_note(block, label_a, label_b)}"
        ],
    )


def _empty_overall_note(block: dict, label_a: str, label_b: str) -> str:
    """Title suffix naming each side whose ``overall`` is empty (ADR-0020 bug 4).

    ``overall`` is empty when every query on that side had zero positives, so
    no row has a Δ and the whole Δ column is blank. Without this line the
    reader sees an empty column and nothing saying why. The comparison's
    ``entries`` say it: none, or no block at all.
    """
    empty = [label for label, side in ((label_a, THIS), (label_b, REFERENCE))
             if not has_entries(block, side)]
    if not empty:
        return ""
    sides = "、".join(f"{label} 側無可比的 query（全部零正例）" for label in empty)
    return f" — {sides}，Δ 欄留空"


def _per_item_compare_tables(
    per_item: dict, macro: dict, ks: dict, title_prefix: str, item_cov: str,
) -> tuple[list[pd.DataFrame], list[str]]:
    """One M/B/Δ table per :data:`_PER_ITEM_COMPARED` entry, labelled by the
    catalog's display name, its columns the K cut's list for it; the item
    and category grains share it so the two cannot come to compare
    different metrics."""
    tables, titles = [], []
    for compared, ks_key in _PER_ITEM_COMPARED:
        metric = entry(*compared)
        label = metric.display_name
        tables.append(compared_per_item_table(
            per_item, macro, ks[ks_key], metric.name, f"{label}@{{k}}"))
        titles.append(f"{title_prefix}{label}@k (M/B/Δ){item_cov}")
    return tables, titles


def _build_per_item_section(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    comparison = results[SOURCE_COMPARISON]
    per_item = landed_value(comparison, SOURCE_COMPARISON, "per_item")
    if not has_entries(per_item, REFERENCE):
        return None
    macro = landed_value(comparison, SOURCE_COMPARISON, "macro_by_item")
    # bug 5 (ADR-0020): disclose each side's macro item coverage.
    item_cov = compared_macro_coverage_suffix(
        macro,
        landed_value(comparison, SOURCE_COMPARISON, POPULATION_KEY,
                     MACRO_ITEMS_KEY),
        count_items(results[ROW_FACTS_A]),
    )
    tables, titles = _per_item_compare_tables(
        per_item, macro,
        landed_value(results[COMPARISON_DISPLAY_KS], COMPARISON_DISPLAY_KS,
                     "per_item"),
        # No "per-item" prefix: the section says it, and the per-item
        # recall's display name says it again.
        "", item_cov,
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
    eval_params = parameters.get("evaluation", {}) or {}
    if not (eval_params.get("item_categories", {}) or {}).get("enabled"):
        return None
    # Compared only when both sides have the grain (the comparison layer).
    category = landed_value(results[SOURCE_COMPARISON], SOURCE_COMPARISON,
                            "category")
    if category is None:
        return None
    cut = results[COMPARISON_DISPLAY_KS]
    n_cat = count_items(results[ROW_FACTS_A].get("category") or {})

    tables, titles = [], []
    # bug 8 (ADR-0020): each side cut at its own category K, like the overall
    # section — 3 categories must not print precision@4 / @5.
    side_a, side_b = "Model", "Compare"
    tables.append(_every_key_table(
        category["overall"],
        landed_value(cut, COMPARISON_DISPLAY_KS, "category", "overall"),
        side_a, side_b))
    titles.append(
        f"大類 overall"
        f"{_empty_overall_note(category['overall'], side_a, side_b)}"
    )
    # bug 5 (ADR-0020): same disclosure as the fine-grained per-item section.
    item_cov = compared_macro_coverage_suffix(
        category["macro_by_item"], category[POPULATION_KEY][MACRO_ITEMS_KEY],
        n_cat)
    more_tables, more_titles = _per_item_compare_tables(
        category["per_item"], category["macro_by_item"],
        landed_value(cut, COMPARISON_DISPLAY_KS, "category", "per_item"),
        "大類 ", item_cov,
    )
    tables += more_tables
    titles += more_titles
    return ReportSection(
        title="大類 Category M/B/Δ",
        description="大類粒度 overall + per-category recall/map_attr。只列雙方共通的大類。",
        tables=tables,
        table_titles=titles,
    )

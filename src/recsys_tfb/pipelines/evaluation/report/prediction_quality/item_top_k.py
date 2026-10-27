"""各 item 的 precision@K: per-item precision@K and F1@K over every query
group, the ones without a positive included (ADR-0031 decision 7), on
the ``--prediction-quality`` page — not on the main report's per-item
tables, whose rows are only the groups holding a positive."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.evaluation.metric_catalog import ITEM_EVERY_GROUP, metrics_of
from recsys_tfb.evaluation.result_readers import ITEM_TOP_K_KEY
from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    score_bins,
)
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import (
    definitions,
)
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.report.types import Column, Note, Paragraph, Row, Table

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("prediction_quality_metrics",)

#: No switch turns this section off (``shared.sections_on``).
SWITCH = None

#: The heading, its English and the question the section answers.
TITLE = "各 item 的 precision@K"
TITLE_EN = "Per-item precision@K"
QUESTION = "把一個 item 排進前 K 名推出去，幾成會成為正例？"

#: The counts table's title.
COUNTS_TABLE = "排進前 K 名的次數 · 各 item"

#: Which rows, said once at the top of the section.
POPULATION = (
    "分母是這個 item 在全部 query group 裡排進前 K 名的次數，沒有正例的組也算"
    "（它們也會把 item 排進前 K 名）。主報表排序指標段的 per-item 表只看有正例"
    "的組，母體不同，同名的數字不能互相比。名次是 query group 內的名次，跟分數"
    "跨 group 可不可比無關。"
)


def _metric_table(metric, top_k: Mapping) -> Table:
    """One metric: Macro first (pinned), then every item; one column per K."""
    ks = top_k["k_values"]
    keys = [metric.key(k) for k in ks]
    macro = top_k["macro"]
    rows = [Row("Macro 平均", tuple(macro[key]["value"] for key in keys),
                pinned=True)]
    rows += [Row(item, tuple(top_k["per_item"].get(item, {}).get(key)
                             for key in keys))
             for item in top_k["shared_numbers"]]
    counts = "、".join(f"@{k} {macro[key]['n_items']:,}"
                      for k, key in zip(ks, keys))
    note = Note(
        f"參與 Macro 的 item（全部 {macro['n_items']:,} 個）：{counts}。沒有值"
        "的 item 不進；正例數低於 metric.min_positives＝"
        f"{macro['min_positives']} 的也不進。")
    return Table(f"{metric.display_name}@K · 各 item", "item",
                 tuple(Column(f"@{k}", "ratio") for k in ks), tuple(rows),
                 (note,))


def _counts_table(top_k: Mapping, count_kind: str) -> Table:
    """The counts each cell divides: positives and, per K, times ranked
    within the top K, so every cell's denominator is on the page."""
    ks = top_k["k_values"]
    columns = (Column("正例數", count_kind),
               *(Column(f"@{k}", count_kind, group="排進前 K 名")
                 for k in ks))
    rows = tuple(
        Row(item, (cell["n_pos"], *(cell[f"n_top@{k}"] for k in ks)))
        for item, cell in top_k["shared_numbers"].items())
    return Table(COUNTS_TABLE, "item", columns, rows)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    """The section, or ``None`` when no item has a row. A payload landed
    before this block existed holds none: the section says so."""
    payload = results["prediction_quality_metrics"]
    if payload["overall"].get("summary") is None:
        return None

    def section(blocks: list) -> ReportSection:
        return ReportSection(
            title=TITLE, description="", title_en=TITLE_EN,
            question=QUESTION, key="item_top_k", blocks=blocks)

    top_k = payload.get(ITEM_TOP_K_KEY)
    if top_k is None:
        return section([Paragraph(
            "這份 prediction_quality.json 是加入 per-item precision@K 之前算的，"
            "沒有這一段的數字；重跑一次 --prediction-quality 就有。",
            muted=True)])
    entries = metrics_of(ITEM_EVERY_GROUP)
    weighted = payload["columns"].get("weight")
    return section([
        Paragraph(POPULATION + ("次數都是乘上權重之後的加權數（見上面的母體）。"
                                if weighted else ""), muted=True),
        *(_metric_table(m, top_k) for m in entries),
        _counts_table(top_k, score_bins.count_kind(weighted)),
        definitions([*entries, "macro"]),
    ])

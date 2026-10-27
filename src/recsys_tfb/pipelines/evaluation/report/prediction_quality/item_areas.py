"""AUC 與 AP: every item's PR-AUC and ROC-AUC and their macro, and the two
average precisions on the raw scores HPO can select by, on the
``--prediction-quality`` page. Which rows they are is the page's first
section (``population``)."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.metric_catalog import (
    MODEL_SELECTION_ON_PREDICTION_QUALITY,
    SCORE_BINS,
    metrics_of,
)
from recsys_tfb.evaluation.result_readers import (
    AVERAGE_PRECISION_KEY,
    frame_from_json,
)
from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    score_bins,
)
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import (
    definitions,
)
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.report.types import Bullets, Column, Note, Paragraph, Row, Table

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("prediction_quality_metrics",)

#: No switch turns this section off (``shared.sections_on``).
SWITCH = None

#: The heading, its English and the question the section answers.
TITLE = "AUC 與 AP"
TITLE_EN = "AUC and AP"
QUESTION = "每個 item 自己的分數，正例排得比負例前面嗎？"

#: The table titles.
AP_TABLE = "原始分數上的 AP"
AREA_TABLE = "PR-AUC 與 ROC-AUC · 各 item"

#: How to read the per-item areas, printed beside them: PR-AUC against the
#: positive rate, the scores' comparability across query groups, the blanks.
READING = (
    "PR-AUC 隨機排序時約等於正例佔比，各 item 的正例佔比不同，所以要對著旁邊"
    "那欄看；Macro 的 PR-AUC 混了不同的正例佔比。ROC-AUC 不受正例佔比影響，"
    "可以直接比。",
    "per-item 的 AUC 是拿同一個 item 在不同 entity 上的分數互比。模型用排序類"
    "目標（lambdarank、rank_xendcg）訓練時，分數只為同一個 query group 內的"
    "先後而學，跨 group 不保證可比；binary 目標沒有這個問題。"
    "下一段的 per-item precision@K 看的是組內名次，也沒有這個問題。",
    "空格是沒有定義，不是 0：沒有正例或沒有負例的 item 兩個都沒有值，也不進 "
    "Macro。",
)


def _ap_table(average_precision: Mapping) -> Table:
    """The two raw-score APs, read off the payload; rows in the catalog's
    order (overall, then macro)."""
    rows = tuple(
        Row(m.display_name, (average_precision.get(m.source.grain),))
        for m in MODEL_SELECTION_ON_PREDICTION_QUALITY)
    return Table(AP_TABLE, "指標", (Column("值", "ratio"),), rows)


def _macro_note(macro: Mapping, names: list[str], labels: list[str]) -> Note:
    """「參與 Macro 的 item：PR-AUC x／y、ROC-AUC x／y」: the count as x of y,
    always, so a reader sees how many items the macro stands for."""
    n_items = macro["n_items"]
    counts = "、".join(f"{label} {macro[name]['n_items']:,}／{n_items:,}"
                      for name, label in zip(names, labels))
    return Note(f"參與 Macro 的 item：{counts}（另外也不算正例數低於 "
                f"metric.min_positives＝{macro['min_positives']} 的 item）。")


def _area_table(per_item: Mapping, item_col: str, count_kind: str) -> Table:
    """Macro first (pinned), then every item, most rows first: its rows,
    positives and positive rate beside the two areas."""
    entries = metrics_of(SCORE_BINS)
    names = [m.name for m in entries]
    labels = [m.display_name for m in entries]
    totals = frame_from_json(per_item["totals"])
    macro = per_item["macro"]
    columns = (Column("候選列", count_kind), Column("正例數", count_kind),
               Column("正例佔比", "ratio"),
               *(Column(label, "ratio") for label in labels))
    rows = [Row("Macro 平均", (None, None, None,
                              *(macro[name]["value"] for name in names)),
                pinned=True)]
    for _, r in totals.iterrows():
        rows.append(Row(str(r[item_col]), (
            r["n"], r["n_pos"], r["positive_rate"],
            *(None if pd.isna(r[name]) else float(r[name])
              for name in names))))
    return Table(AREA_TABLE, "item", columns, tuple(rows),
                 (_macro_note(macro, names, labels),))


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    """The section, or ``None`` when the page has no row to show. A payload
    landed before these blocks existed holds neither: the section says so rather than
    leaving empty tables (rerunning ``--prediction-quality`` fills it)."""
    payload = results["prediction_quality_metrics"]
    if payload["overall"].get("summary") is None:
        return None
    per_item = payload["per_item"]

    def section(blocks: list) -> ReportSection:
        return ReportSection(
            title=TITLE, description="", title_en=TITLE_EN,
            question=QUESTION, key="item_areas", blocks=blocks)

    if "macro" not in per_item or AVERAGE_PRECISION_KEY not in payload:
        return section([Paragraph(
            "這份 prediction_quality.json 是加入各 item 的 AUC 與 AP 之前算的，"
            "沒有這一段的數字；重跑一次 --prediction-quality 就有。",
            muted=True)])
    count_kind = score_bins.count_kind(payload["columns"].get("weight"))
    return section([
        Paragraph(
            "AP 在原始分數上算，是 training 調參（HPO）能選的兩個指標，跟 "
            "training 用同一套計算。PR-AUC、ROC-AUC 在上一段的分箱上算，每個 "
            "item 都有，不只分箱表列出的那幾個。",
            muted=True),
        _ap_table(payload[AVERAGE_PRECISION_KEY]),
        definitions(MODEL_SELECTION_ON_PREDICTION_QUALITY),
        _area_table(per_item, payload["columns"]["item"], count_kind),
        Bullets(READING),
        definitions([*metrics_of(SCORE_BINS), "macro"]),
    ])

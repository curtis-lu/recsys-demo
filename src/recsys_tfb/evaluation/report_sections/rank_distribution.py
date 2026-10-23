"""per-item 細部拆解 (名次分布 in ADR-0031 decision 16): the score and
rank distributions of the same ranking, drawn from
``compute_report_aggregates``."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    section_on,
)
from recsys_tfb.evaluation.result_readers import (
    drop_all_positive_groups,
    frame_from_json,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_report_aggregates",)


#: 開關開著時，印在不套用它的段落旁的那一句。
ALL_POSITIVE_KEPT_NOTE = "這段沒有排除全正的 query group"


def _all_positive_kept_note(parameters: dict) -> str:
    """不套用開關的段落要加的那一句；開關關著回 ``""``（報表逐字不變）。"""
    if not drop_all_positive_groups(parameters):
        return ""
    return (
        f"⚠ {ALL_POSITIVE_KEPT_NOTE}：{ALL_POSITIVE_SWITCH} 開著，但它只套用在"
        "衡量指標與熱門度 baseline。"
    )


def _item_share_by_rank(counts_frame: pd.DataFrame) -> pd.DataFrame:
    """欄正規化：每個 rank 欄 ÷ 欄和 → 各 item 在該 rank 位置的佔比。

    每個 rank 欄加總=1（誰佔據該名次）。全 0 欄（0/0）得 NaN、render 端空白。
    依 §二，正規化後的矩陣用「按欄讀的數字表」呈現，不掛全域色階 heatmap。
    """
    col_sums = counts_frame.sum(axis=0)
    return counts_frame.divide(col_sums, axis=1)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    """per-item 細部拆解（原診斷區升為頂層）。

    同一批排名的分數／名次分布側面。沿用 score 分布圖與 rank 計數 heatmap；
    新增 item-share-by-rank（欄正規化，數字表，G#1）＋ positive rate by rank
    數字表。升為頂層（collapsible=False）。

    分數分箱（每格平均分數 vs 實際正例率）不在這一段：它是「預測品質」段的
    分箱表（#381，ADR-0024）。#381 之前的 ``report_aggregates.json`` 還帶一個
    ``calibration`` 鍵（在 ``[0, 1]`` 上等寬切），本段一直不畫它，讀到舊檔也
    一樣不畫。
    """
    if not section_on(parameters, "per_item_breakdown"):
        return None
    report_aggregates = results["evaluation_report_aggregates"]
    # 與 build_diagnostics_figures 同一個「有沒有 score_histogram 家族」判斷；
    # 沒有分布家族時（包括只剩舊檔的 calibration 鍵），本段不畫。
    if not report_aggregates or "score_histogram" not in report_aggregates:
        return None

    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )

    cols = report_aggregates["columns"]
    item_col, label_col = cols["item"], cols["label"]
    # 先群組所有圖（score 分布 2 張＋rank 矩陣 heatmap 3 張），再放數字表——
    # 段內視覺一致（section 先 render figures 再 render tables）。positive rate
    # 是有界 [0,1] 的率矩陣，全域色階有意義 → 用 heatmap。
    figs = [
        plot_score_histogram(
            frame_from_json(report_aggregates["score_histogram"]),
            item_col=item_col),
        plot_score_boxplot_by_label(
            frame_from_json(report_aggregates["score_box_by_label"]),
            item_col=item_col, label_col=label_col),
        plot_rank_heatmap(
            frame_from_json(report_aggregates["rank_counts"])),
        plot_positive_rank_heatmap(
            frame_from_json(report_aggregates["positive_rank_counts"])),
        plot_positive_rate_rank_heatmap(
            frame_from_json(report_aggregates["positive_rate"])),
    ]

    # item share by rank：逐欄正規化（每 rank 欄加總=1），刻意用數字表——掛全域
    # 色階 heatmap 會誘導跨欄比色誤讀（§二）。放在所有 heatmap 之後。
    rank_counts = frame_from_json(report_aggregates["rank_counts"])
    pos_rank_counts = frame_from_json(report_aggregates["positive_rank_counts"])
    tables = [
        _item_share_by_rank(rank_counts),
        _item_share_by_rank(pos_rank_counts),
    ]
    titles = [
        "item share by rank（query 數，欄正規化：每 rank 各 item 佔比，欄和=1）",
        "item share by rank（positive query 數，欄正規化）",
    ]
    return ReportSection(
        title="per-item 細部拆解",
        description=(
            # #376: compute_report_aggregates reads every evaluated row; the
            # switch does not reach it.
            _all_positive_kept_note(parameters)
            + "同一批排名的分數與名次分布側面。先看圖（群組在前）：score 分布、"
            "score by label、rank 計數 heatmap、positive rank 計數 heatmap、"
            "positive rate by rank heatmap；再看數字表：item share by rank（欄"
            "正規化，看誰佔據各名次）。item share 刻意用數字表而非 heatmap——它"
            "是逐欄正規化（每欄加總=1），掛全域色階會誘導跨欄比色誤讀，請在同一"
            "欄內比。rank 計數的欄和＝總 query 數。明細數字表點標題展開。分數分箱"
            "（每格平均分數 vs 實際正例率）在「預測品質」段，"
            "evaluation.report.sections.prediction_quality 打開時才有。"
        ),
        figures=figs,
        tables=tables,
        table_titles=titles,
        collapsed_tables=[True, True],
        collapsible=False,
    )

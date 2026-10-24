"""名次分布 (ADR-0031 decision 16): the score and rank distributions of the
same ranking — the figures drawn from ``compute_report_aggregates``, and
each item's mean positive rank (``mean_pos``) from the metrics bundle as a
bar chart. Every figure has its data table, collapsed under it."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.metric_catalog import ITEM, entry
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.metric_tables import definitions
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    MACRO_LABEL,
)
from recsys_tfb.evaluation.result_readers import (
    drop_all_positive_groups,
    frame_from_json,
)
from recsys_tfb.report.types import (
    Column,
    Figure,
    Note,
    Paragraph,
    Row,
    Table,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the distributions off the report aggregates, ``mean_pos`` off the metrics.
READS = ("evaluation_report_aggregates", "evaluation_metrics")

#: The ``evaluation.report.sections`` switch that turns this section off
#: (ADR-0031 principle 3: off, the pipeline computes nothing only it reads).
SWITCH = "rank_distribution"

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "名次分布"
TITLE_EN = "Rank distribution"
QUESTION = "每個 item 的分數與名次落在哪裡？"


#: 開關開著時，印在這段的那一句的開頭。
ALL_POSITIVE_KEPT_NOTE = "正例平均名次以外的圖沒有排除全正的 query group"

#: The titles of the figures drawn from the report aggregates, in the order
#: they are drawn (decision 16: in Chinese, named like the tables); each
#: figure's data table has its name. A share heatmap follows the count
#: heatmap it divides. 正例平均名次 comes last, from the metrics.
FIGURE_TITLES = (
    "分數分布 · 各 item",
    "分數分布 · 依 label",
    "名次計數 · 各 item",
    "名次佔比 · 各 item",
    "正例名次計數 · 各 item",
    "正例名次佔比 · 各 item",
    "各名次的正例率 · 各 item",
)


def _all_positive_kept_notes(parameters: dict) -> list[Note]:
    """開關開著時這段的 ⚠ 那一行；關著回空（#376）。

    ``compute_report_aggregates`` 讀全部評估列，開關碰不到它，所以正例平均名次
    以外的圖照樣算進全正的組；正例平均名次讀指標結果，跟著開關排除了。只說這段
    自己的數是哪一批，不提別段。
    """
    if not drop_all_positive_groups(parameters):
        return []
    return [Note(
        f"{ALL_POSITIVE_KEPT_NOTE}：{ALL_POSITIVE_SWITCH} 開著，但它們算在本次"
        "評估的全部候選列上；正例平均名次跟著開關，排除了全正的組。",
        warn=True)]


def _item_share_by_rank(counts_frame: pd.DataFrame) -> pd.DataFrame:
    """欄正規化：每個 rank 欄 ÷ 欄和 → 各 item 在該 rank 位置的佔比。

    每個 rank 欄加總=1（誰佔據該名次）。全 0 欄（0/0）得 NaN、畫成空格。
    每個 query group 的候選一樣多時（例如全網格）各欄的和相同，名次佔比圖跟
    名次計數圖長得一樣，只差一個倍數；段的說明講這件事。
    """
    col_sums = counts_frame.sum(axis=0)
    return counts_frame.divide(col_sums, axis=1)


def _matrix_table(title: str, matrix: pd.DataFrame, kind: str) -> Table:
    """An item × rank table: rows the items (the matrix's order), columns
    第 r 名 for each rank of the matrix, every cell of one ``kind``."""
    columns = tuple(Column(f"第 {rank} 名", kind) for rank in matrix.columns)
    rows = tuple(Row(str(item), tuple(matrix.loc[item]))
                 for item in matrix.index)
    return Table(title, "item", columns, rows)


def _histogram_table(title: str, hist: pd.DataFrame, item_col: str) -> Table:
    """The histogram's bars: one row per item and non-empty bin, in the
    order they are drawn (item, then bin from low scores up)."""
    hist = hist.sort_values([item_col, "bin_center"], kind="stable")
    columns = (Column("箱中心", "ratio"), Column("箱寬", "ratio"),
               Column("候選列", "count"))
    rows = tuple(Row(str(r[item_col]), (r["bin_center"], r["bin_width"],
                                        r["count"]))
                 for _, r in hist.iterrows())
    return Table(title, "item", columns, rows)


#: The boxplot table's five numbers: (the aggregate's key, the header).
_BOX_COLUMNS = (("lowerfence", "下緣"), ("q1", "Q1"), ("median", "中位數"),
                ("q3", "Q3"), ("upperfence", "上緣"))


def _box_table(title: str, box: pd.DataFrame, item_col: str,
               label_col: str) -> Table:
    """The boxes: one row per item and label, label 1 before 0, the labels
    the figure draws (``plot_score_boxplot_by_label``)."""
    drawn = box[box[label_col].isin((0, 1))].sort_values(
        [item_col, label_col], ascending=[True, False], kind="stable")
    columns = (Column(label_col, "text"),
               *(Column(header, "ratio") for _, header in _BOX_COLUMNS))
    rows = tuple(Row(str(r[item_col]), (str(int(r[label_col])),
                                        *(r[key] for key, _ in _BOX_COLUMNS)))
                 for _, r in drawn.iterrows())
    note = Note("下緣、上緣是 Q1 − 1.5 × 四分位距、Q3 + 1.5 × 四分位距，夾在"
                "實際的最小、最大分數之內。")
    return Table(title, "item", columns, rows, notes=(note,))


def _figures(report_aggregates: dict) -> list[Figure]:
    """The figures drawn from the report aggregates, titled in Chinese
    (:data:`FIGURE_TITLES`), each with the numbers it draws as its data
    table, named like it."""
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_rank_share_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )

    cols = report_aggregates["columns"]
    item_col, label_col = cols["item"], cols["label"]
    hist = frame_from_json(report_aggregates["score_histogram"])
    box = frame_from_json(report_aggregates["score_box_by_label"])
    ranks = frame_from_json(report_aggregates["rank_counts"])
    rank_share = _item_share_by_rank(ranks)
    positive_ranks = frame_from_json(report_aggregates["positive_rank_counts"])
    positive_share = _item_share_by_rank(positive_ranks)
    rate = frame_from_json(report_aggregates["positive_rate"])
    drawn = [
        (plot_score_histogram(hist, item_col=item_col),
         lambda t: _histogram_table(t, hist, item_col)),
        (plot_score_boxplot_by_label(box, item_col=item_col,
                                     label_col=label_col),
         lambda t: _box_table(t, box, item_col, label_col)),
        (plot_rank_heatmap(ranks),
         lambda t: _matrix_table(t, ranks, "count")),
        (plot_rank_share_heatmap(rank_share),
         lambda t: _matrix_table(t, rank_share, "ratio")),
        (plot_positive_rank_heatmap(positive_ranks),
         lambda t: _matrix_table(t, positive_ranks, "count")),
        (plot_rank_share_heatmap(positive_share),
         lambda t: _matrix_table(t, positive_share, "ratio")),
        (plot_positive_rate_rank_heatmap(rate),
         lambda t: _matrix_table(t, rate, "ratio")),
    ]
    figures = []
    for (fig, data), title in zip(drawn, FIGURE_TITLES):
        fig.update_layout(title=title)
        figures.append(Figure(fig, data(title)))
    return figures


def _mean_rank_blocks(metrics: Mapping | None) -> list:
    """「正例平均名次 · 各 item」— a bar per item in name order, Macro 平均
    a dashed line; its data table pins Macro 平均, then each item, the one
    value the catalog's ``mean_pos`` (item method) — and its definition.
    Nothing when the bundle has no item."""
    import plotly.graph_objects as go

    metric = entry("mean_pos", ITEM)
    per_item = sorted(((metrics or {}).get("per_item") or {}).items())
    if not per_item:
        return []
    macro = ((metrics or {}).get("macro_avg") or {}).get("by_item") or {}
    title = f"{metric.display_name} · 各 item"
    items = [str(item) for item, _ in per_item]
    values = [v.get(metric.key()) for _, v in per_item]
    fig = go.Figure(go.Bar(x=items, y=values, name=metric.display_name))
    if macro.get(metric.key()) is not None:
        fig.add_hline(y=macro[metric.key()], line_dash="dash",
                      annotation_text=MACRO_LABEL)
    fig.update_layout(title=title, xaxis_title="item",
                      yaxis_title=metric.display_name)
    fig.update_xaxes(type="category")
    rows = [Row(MACRO_LABEL, (macro.get(metric.key()),), pinned=True)] if macro else []
    rows += [Row(item, (value,)) for item, value in zip(items, values)]
    table = Table(title, "item", (Column(metric.display_name, "ratio"),),
                  tuple(rows))
    return [Figure(fig, table), definitions([metric])]


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    """名次分布: the description and the figures, each with its data
    table, the mean positive rank's last. ``None`` when the section is
    switched off or the aggregates hold no distribution family — a ``report_aggregates.json``
    from before #381 that holds only the ``calibration`` key included: the
    score bins are not drawn here.
    """
    report_aggregates = results["evaluation_report_aggregates"]
    if not report_aggregates or "score_histogram" not in report_aggregates:
        return None

    blocks: list = [Paragraph(
        "同一批名次的分數與名次分布。每張圖底下的數字表就是圖上的數，點開可以"
        "複製。名次佔比是名次計數每一欄除以那一欄的合計：每一欄加起來是 1，"
        "讀的是那個名次上各 item 各佔多少。"
        "名次計數每一欄的合計是那個名次上有候選的 query group 數；每個 query "
        "group 的候選一樣多時（例如全網格），每一欄都＝全部 query group 數"
        "（核心概念的總量），名次佔比圖就跟名次計數圖長得一樣。",
        muted=True)]
    blocks += _all_positive_kept_notes(parameters)
    blocks += _figures(report_aggregates)
    blocks += _mean_rank_blocks(results["evaluation_metrics"])
    return ReportSection(
        title=TITLE, description="", title_en=TITLE_EN, question=QUESTION,
        key="rank_distribution", blocks=blocks,
    )

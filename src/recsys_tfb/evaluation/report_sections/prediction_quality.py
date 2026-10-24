"""預測品質: every candidate row as one binary prediction, binned by score
(ADR-0024; drawn as ADR-0031 decision 16's demo)."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    kept_ratio_phrase,
    zero_positive_draw_on_test,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    DISPLAY_BINS_KEY,
    THRESHOLD_SWEEP_KEY,
    drop_all_positive_groups,
    frame_from_json,
)
from recsys_tfb.report.types import (
    Bullets,
    Column,
    Definitions,
    Figure,
    Formula,
    Note,
    Paragraph,
    Row,
    Table,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("prediction_quality_metrics", "evaluation_row_facts")

#: The ``evaluation.report.sections`` switch that turns this section off
#: (ADR-0031 principle 3: off, the pipeline computes nothing only it reads).
SWITCH = "prediction_quality"

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "預測品質"
TITLE_EN = "Prediction quality"
QUESTION = "把分數當成二元預測，切在哪裡、準不準？"

#: The upper headers of a bin table (decision 16 item 17): the bin's score
#: edges, what is in the bin, and what lies at or above its lower edge.
SCORE_GROUP, BIN_GROUP, ABOVE_GROUP = "分數", "這一格", "門檻以上"

#: A bin table's columns after 格: the display bin frame's column
#: (``prediction_quality.DISPLAY_BIN_COLUMNS``, landed), its header, its upper
#: header, its kind. ``None`` kind: a count — ``count``, or ``ratio`` for the
#: weighted counts of ``--post-training`` (:func:`_count_kind`).
_BIN_COLUMNS = (
    ("score_from", "下緣", SCORE_GROUP, "ratio"),
    ("score_to", "上緣", SCORE_GROUP, "ratio"),
    ("n", "候選列", BIN_GROUP, None),
    ("n_pos", "正例數", BIN_GROUP, None),
    ("mean_score", "平均分數", BIN_GROUP, "ratio"),
    ("positive_rate", "正例率", BIN_GROUP, "ratio"),
    ("precision", "precision", ABOVE_GROUP, "ratio"),
    ("recall", "recall", ABOVE_GROUP, "ratio"),
    ("f1", "F1", ABOVE_GROUP, "ratio"),
)

#: The 格 column: the display bin's number, from 1.
BIN_LABEL = "格"

#: The headline numbers of one summary (overall or one item's), as rows or
#: columns: the label and where the summary holds it — a key of the summary,
#: or of its ``best_f1``. No row count, positives or positive rate: those are
#: totals (核心概念) or an item's slice (decision 16 item 17).
_SUMMARY_FIELDS = (
    ("PR-AUC", None, "pr_auc"),
    ("ROC-AUC", None, "roc_auc"),
    ("F1 最佳門檻", "best_f1", "threshold"),
    ("該門檻的 precision", "best_f1", "precision"),
    ("該門檻的 recall", "best_f1", "recall"),
    ("該門檻的 F1", "best_f1", "f1"),
)

#: PR-AUC's definition, under the headline table. Not the catalog's: the
#: binned areas are not in ``metric_catalog`` yet (its docstring, "Not in it
#: yet"), so the block says only 定義.
PR_AUC_DEFINITION = (
    "PR-AUC",
    "分數分箱、箱內視為同分之後的 average precision。是近似值，不等於在原始"
    "分數上算的 pooled AP。鍵是 pr_auc。",
)

#: The figure's title (decision 16: named like the tables).
THRESHOLD_FIGURE_TITLE = "門檻對 precision、recall、F1 · 整體"


def _count_kind(weight_col: str | None) -> str:
    """The kind of the 候選列 and 正例數 columns: ``count`` — unless the rows
    are weighted (``--post-training`` with a kept share ``r`` of the
    zero-positive groups, each row ``1 / r``): the sums then are not whole
    numbers, and a count column refuses them, so they print as ratios."""
    return "ratio" if weight_col else "count"


def _summary_values(summary: Mapping) -> tuple:
    best = summary.get("best_f1") or {}
    return tuple((best if where else summary).get(key)
                 for _, where, key in _SUMMARY_FIELDS)


def _bin_columns(count_kind: str) -> tuple[Column, ...]:
    return tuple(Column(label, kind or count_kind, group=group)
                 for _, label, group, kind in _BIN_COLUMNS)


def _bin_cells(row: pd.Series) -> tuple:
    return tuple(row[key] for key, *_ in _BIN_COLUMNS)


def _overall_bin_table(display_bins: pd.DataFrame, count_kind: str) -> Table:
    """「分箱」: one row per display bin, labelled by its number."""
    rows = tuple(Row(str(i), _bin_cells(row))
                 for i, (_, row) in enumerate(display_bins.iterrows(), 1))
    return Table("分箱", BIN_LABEL, _bin_columns(count_kind), rows)


def _per_item_bin_table(display_bins: pd.DataFrame, item_col: str,
                        count_kind: str) -> Table:
    """「分箱 · 各 item」: every listed item's display bins in one table (the
    landed long frame, listed order), the item the row label and 格 the bin's
    number within its item — a reader ticks the items to see in the item
    column's filter (decision 16 item 11)."""
    number = display_bins.groupby(item_col, sort=False).cumcount() + 1
    columns = (Column(BIN_LABEL, "count"), *_bin_columns(count_kind))
    rows = tuple(Row(str(row[item_col]), (int(n), *_bin_cells(row)))
                 for (_, row), n in zip(display_bins.iterrows(), number))
    return Table("分箱 · 各 item", "item", columns, rows)


#: The curves of the threshold figure, and its data table's columns after
#: 門檻: (the sweep's key, the name).
_CURVES = (("precision", "precision"), ("recall", "recall"), ("f1", "F1"))


def _threshold_figure(sweep: pd.DataFrame) -> Figure:
    """precision / recall / F1 against the threshold, one point per non-empty
    fine bin edge: the shape the user reads to pick a cut, which a 1000-row
    table would bury — so the points are the figure's data table, collapsed
    under it, one row per point labelled by its fine bin's number from 1."""
    import plotly.graph_objects as go

    fig = go.Figure()
    for col, name in _CURVES:
        fig.add_trace(go.Scatter(
            x=sweep["threshold"], y=sweep[col], mode="lines", name=name))
    fig.update_layout(
        title=THRESHOLD_FIGURE_TITLE,
        xaxis_title="門檻（分數 ≥ 此值即預測為正）", yaxis_title="值",
        yaxis_range=[0, 1],
    )
    columns = (Column("門檻", "ratio"),
               *(Column(name, "ratio") for _, name in _CURVES))
    rows = tuple(Row(str(int(r["bin"]) + 1),
                     (r["threshold"], *(r[col] for col, _ in _CURVES)))
                 for _, r in sweep.iterrows())
    return Figure(fig, Table(THRESHOLD_FIGURE_TITLE, "細箱", columns, rows))


def _kept_zero_positive_groups(weight_col: str, parameters: dict) -> str:
    """The share of zero-positive test groups the dataset kept and the weight
    each of their rows carries — no count: the groups without a positive are
    a total of 核心概念."""
    ratio = zero_positive_draw_on_test(parameters)
    if not ratio:
        return ""
    return (
        f"{kept_ratio_phrase(ratio)}（dataset.test_zero_positive_group_ratio），"
        f"每列帶權重 1／r＝{1 / ratio:.4g}（{weight_col}）。1／r 是設計權重，"
        "不是無偏估計：留下的組少時，加權後的比值型指標不穩。"
    )


def _population(facts: dict, parameters: dict, weight_col: str | None) -> str:
    """Which rows this section is computed on, against the ranking metrics'
    (ADR-0024 decision 7) — one sentence per run mode, without naming another
    section and without the totals (decision 16 item 12; they are in
    核心概念). ``--post-training`` has two forms: with the zero-positive
    group weight in the payload the test table kept a share ``r`` of the
    groups without a positive, weighted back up by ``1 / r`` (ADR-0025
    decision 3); without it the dataset pipeline dropped them all, and with
    nothing else excluded the two populations are the same. A46 refuses the
    second form at the CLI entry, so a fresh run no longer produces it; it
    stays for a payload computed before the weight existed.
    """
    # #376: with drop_all_positive_groups on, the ranking metrics also drop
    # the all-positive groups; "only groups with a positive" and "the two
    # populations are the same" would both be false.
    drop = drop_all_positive_groups(parameters)
    if drop:
        ranking_rule = (
            "map@K、precision@K 這類只看組內先後的指標只算有正例、而且不是全正的"
            f"組（{ALL_POSITIVE_SWITCH} 開著）"
        )
    else:
        ranking_rule = "map@K、precision@K 這類只看組內先後的指標只算有正例的組"
    if parameters.get("post_training") and weight_col:
        return (
            "母體是本次評估的全部候選列（核心概念的總量），本身不排除任何 query "
            "group；但這是 --post-training，test 表在 dataset 階段只留下有正例的 "
            "query group 與一部分沒有正例的（filter_test_keys）。"
            + _kept_zero_positive_groups(weight_col, parameters)
            + f"這段的候選列與正例數都是乘上 {weight_col} 之後的加權數，代表"
            f"還原到全部曝光的估計。{ranking_rule}，所以兩邊的母體不同，"
            "precision 不能互相比。"
        )
    if parameters.get("post_training"):
        # With the switch on the ranking metrics also dropped the
        # all-positive groups; the populations are the same only if there
        # were none.
        same = (facts.get("n_excluded_queries") == 0
                and (not drop or facts.get(ALL_POSITIVE_KEY) == 0))
        return (
            "母體是本次評估的全部候選列（核心概念的總量），本身不排除任何 query "
            "group；但這是 --post-training，test 表在 dataset 階段已經丟掉沒有"
            "正例的 query group（filter_test_keys），所以這些列只來自「有正例的 "
            "query group」，不是全部曝光。正例佔比因此比全部曝光高，precision 與 "
            "PR-AUC 會比在全部曝光上算的大；要留下這類 query group，設 "
            f"dataset.test_zero_positive_group_ratio。{ranking_rule}，"
            + ("所以兩邊的母體相同，但 precision 的定義不同（見下面 per-item 那"
               "一條），仍不能互相比。" if same else
               "所以兩邊的母體不同，precision 不能互相比。")
        )
    return (
        "母體是全部候選列（核心概念的總量），包含沒有正例的 query group；"
        f"{ranking_rule}，所以兩邊的 precision 不能互相比。"
    )


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    """The prediction-quality section (ADR-0024): every candidate row as one
    binary prediction, binned by score.

    Every number here is read off the payload: the headline numbers, the
    display bin tables and the threshold sweep, all computed by
    ``compute_prediction_quality`` from the same fine bins
    (``evaluation/prediction_quality.py``, landed since #536), so the
    threshold sweep and the bin table cannot disagree. The row facts are read
    for one thing: whether the ranking metrics' query groups are the same as
    this section's rows (:func:`_population`).

    Decision 7's three statements are always printed, each where the number
    it qualifies is: the bin width (the best-F1 threshold's resolution, not an
    online setting), the two populations, and PR-AUC not being an average
    precision computed elsewhere.
    """
    prediction_quality = results["prediction_quality_metrics"]

    def section(blocks: list) -> ReportSection:
        return ReportSection(
            title=TITLE, description="", title_en=TITLE_EN, question=QUESTION,
            key="prediction_quality", blocks=blocks)

    overall = prediction_quality["overall"]
    summary = overall.get("summary")
    if summary is None:
        return section([Paragraph(
            "本次評估沒有任何候選列，這一段沒有東西可算。", muted=True)])
    facts = results["evaluation_row_facts"]
    bins_cfg = prediction_quality["bins"]
    lo, hi, width = bins_cfg["lo"], bins_cfg["hi"], bins_cfg["width"]
    n_bins, n_display = bins_cfg["n_bins"], bins_cfg["n_display_bins"]
    per_item = prediction_quality["per_item"]
    columns = prediction_quality["columns"]
    weight_col = columns.get("weight")
    count_kind = _count_kind(weight_col)

    blocks: list = [
        Paragraph(
            "把每一列候選當成一次「會不會是正例」的預測：分數 ≥ 門檻就預測為正。"
            "看切在哪裡 precision 與 recall 各多少，以及每段分數裡實際有多少"
            "正例。" + _population(facts, parameters, weight_col)
            + f"分箱表一格＝{n_bins // n_display} 個細箱；「{BIN_GROUP}」底下的"
            f"欄只算這一格，「{ABOVE_GROUP}」底下的欄是把門檻放在這格下緣、算"
            "門檻以上的全部候選列。",
            muted=True),
        Formula(
            "precision＝TP÷(TP+FP)　recall＝TP÷全部正例　F1＝2·P·R÷(P+R)"
            "　門檻只取細箱的下緣；箱內的列視為同分"),
        Bullets((
            f"門檻解析度：分數範圍取本次資料的最小～最大，切成 {n_bins} 個等寬"
            f"細箱，細箱寬 {width:g}。門檻只算得到細箱的邊界，所以 F1 最佳門檻的"
            "解析度就是這個寬度；它也是在這份資料上挑的，換一份資料（下個月、"
            "線上）分數分布會變，不能直接搬去當線上的設定值。",
            "PR-AUC＝把箱內的列視為同分之後的 average precision（Σ 每箱正例佔全部"
            "正例的比例 × 該箱下緣的 precision）；ROC-AUC＝箱內同分算一半的 ROC "
            "面積。兩者都是「分箱後分數」的精確值，但箱內的先後已經丟掉，不等於"
            "在原始分數上算的值——不能拿來跟外部工具（例如 sklearn）在原始分數上"
            "算的 average precision 或 ROC-AUC 直接比較。",
            "per-item 的 precision 分母是該 item 在門檻以上的候選列數；query "
            "group 的 precision@K 分母是 K，是不同的量。",
            "分箱表把每格的平均分數與實際正例率放在一起，只是並列兩個量：框架"
            "不做校準（#411），分數不保證是機率。上面每個指標都只看分數的大小"
            "順序，兩者相近或相差都不改變它們。",
            "一個門檻橫跨所有 query group，前提是不同 query group 的分數彼此可比。"
            "模型用排序類目標（lambdarank、rank_xendcg）訓練時，分數只為同一個 "
            "query group 內的先後而學，跨 group 用同一個門檻切是模型沒有優化過的"
            "用法；binary 目標沒有這個問題。",
            "分數範圍取最小～最大，少數極端的高分會把細箱撐寬，多數列擠進少數幾箱；"
            "看分箱表各格的候選列數就知道是不是這樣。",
        )),
    ]
    if summary.get("n_pos"):
        blocks.append(_threshold_figure(
            frame_from_json(overall[THRESHOLD_SWEEP_KEY])))

    # The headline numbers, then the bin width and the score range the
    # threshold is read off. The range is two rows, not one "lo ～ hi" cell:
    # a number column holds numbers, so each edge prints and copies as one.
    headline = [Row(label, (value,)) for (label, _, _), value
                in zip(_SUMMARY_FIELDS, _summary_values(summary))]
    headline += [Row("細箱寬", (width,)), Row("分數範圍下緣", (lo,)),
                 Row("分數範圍上緣", (hi,))]
    blocks += [
        Table("關鍵數", "項目", (Column("值", "ratio"),), tuple(headline)),
        Definitions((PR_AUC_DEFINITION,), source="定義"),
        _overall_bin_table(frame_from_json(overall[DISPLAY_BINS_KEY]),
                           count_kind),
    ]

    listed = per_item.get("listed") or []
    if listed:
        n_items = per_item.get("n_items")
        # The per-item budget (evaluation.prediction_quality.top_n): said
        # when it left items out, which the table cannot show by itself.
        notes = ((Note(
            f"只列出候選列最多的前 {len(listed)} 個 item"
            "（evaluation.prediction_quality.top_n），其餘的只算進整體。"),)
            if n_items is not None and len(listed) < n_items else ())
        blocks += [
            Table("關鍵數 · 各 item", "item",
                  tuple(Column(label, "ratio") for label, _, _
                        in _SUMMARY_FIELDS),
                  tuple(Row(str(item),
                            _summary_values(per_item["summary"][item]))
                        for item in listed),
                  notes),
            _per_item_bin_table(frame_from_json(per_item[DISPLAY_BINS_KEY]),
                                columns["item"], count_kind),
        ]
    return section(blocks)

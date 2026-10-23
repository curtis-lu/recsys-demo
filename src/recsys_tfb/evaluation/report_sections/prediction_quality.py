"""預測品質: every candidate row as one binary prediction, binned by score
(ADR-0024)."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    kept_zero_positive_groups_note,
    prediction_quality_shown,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    DISPLAY_BINS_KEY,
    THRESHOLD_SWEEP_KEY,
    drop_all_positive_groups,
    frame_from_json,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("prediction_quality_metrics", "evaluation_row_facts")


#: The display bin table's columns, as the report prints them. The first six
#: describe the bin; the last three read the threshold sweep at its lower edge.
_PQ_BIN_COLUMNS = {
    "score_from": "分數下緣",
    "score_to": "分數上緣",
    "n": "列數",
    "n_pos": "正例數",
    "mean_score": "平均分數",
    "positive_rate": "實際正例率",
    "precision": "以下緣為門檻：precision",
    "recall": "以下緣為門檻：recall",
    "f1": "以下緣為門檻：F1",
}


def _pq_summary_row(summary: dict) -> dict:
    """One item's (or the whole data's) headline numbers, as table cells."""
    best = summary.get("best_f1") or {}
    return {
        "列數": summary.get("n"),
        "正例數": summary.get("n_pos"),
        "正例率": summary.get("positive_rate"),
        "pr_auc": summary.get("pr_auc"),
        "roc_auc": summary.get("roc_auc"),
        "F1 最佳門檻（分數 ≥）": best.get("threshold"),
        "該門檻的 precision": best.get("precision"),
        "該門檻的 recall": best.get("recall"),
        "該門檻的 F1": best.get("f1"),
    }


def _pq_threshold_figure(sweep: pd.DataFrame):
    """precision / recall / F1 against the threshold, one point per non-empty
    fine bin edge: the shape the user reads to pick a cut, which a 1000-row
    table would bury."""
    import plotly.graph_objects as go

    fig = go.Figure()
    for col, name in (("precision", "precision"), ("recall", "recall"),
                      ("f1", "F1")):
        fig.add_trace(go.Scatter(
            x=sweep["threshold"], y=sweep[col], mode="lines", name=name))
    fig.update_layout(
        title="整體：門檻 → precision／recall／F1（每個非空細箱的下緣一點）",
        xaxis_title="門檻（分數 ≥ 此值即預測為正）", yaxis_title="值",
        yaxis_range=[0, 1],
    )
    return fig


#: The section's title, shared by its empty and full forms.
_PQ_TITLE = "預測品質 — 把每一列候選當二元預測"


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
    for one thing: the
    query groups the ranking section excludes, so the population note can put
    the two populations side by side with numbers.

    Decision 7's three statements are always printed, each where the number
    it qualifies is: the bin width (the best-F1 threshold's resolution, not an
    online setting), the two populations, and ``pr_auc`` not being an average
    precision computed elsewhere. The population note has one form per run
    mode, and ``--post-training`` has two: with the zero-positive group
    weight in the payload the test table kept a share ``r`` of the groups
    without a positive, and the counts are weighted back up by ``1 / r``
    (ADR-0025 decision 3); without it the dataset pipeline dropped them all,
    so "every candidate row" would be false and, with nothing else excluded,
    the two sections' populations are the same. A46 refuses the second form
    at the CLI entry, so a fresh run no longer produces it; it stays for a
    payload computed before the weight existed.
    """
    prediction_quality = results["prediction_quality_metrics"]
    if not prediction_quality_shown(prediction_quality, parameters):
        return None
    facts = results["evaluation_row_facts"]

    bins_cfg = prediction_quality["bins"]
    lo, width = bins_cfg["lo"], bins_cfg["width"]
    n_bins, n_display = bins_cfg["n_bins"], bins_cfg["n_display_bins"]
    overall = prediction_quality["overall"]
    summary = overall.get("summary")
    if summary is None:
        return ReportSection(
            title=_PQ_TITLE,
            description="本次評估沒有任何候選列，這一段沒有東西可算。",
        )
    per_item = prediction_quality["per_item"]
    item_col = prediction_quality["columns"]["item"]
    n_rows = summary["n"]

    # --- population: this section vs the ranking section (decision 7) -----
    # One sentence per run mode, not a correction appended to the other: under
    # --post-training the test table arrives already filtered, so with nothing
    # excluded here the two sections read the same rows.
    n_excl = facts.get("n_excluded_queries")
    # #376: with drop_all_positive_groups on, the ranking section also drops
    # the all-positive groups; "only groups with a positive" and "the two
    # populations are the same" would both be false.
    drop = drop_all_positive_groups(parameters)
    n_all = facts.get(ALL_POSITIVE_KEY)
    if drop:
        ranking_rule = (
            "主指標段（mAP／precision@K／recall@K）只算有正例、而且不是全正的 "
            f"query group（{ALL_POSITIVE_SWITCH} 開著）"
        )
        parts = []
        if n_excl is not None:
            parts.append(f"無正例的 {n_excl} 個（n_excluded_queries）")
        if n_all is not None:
            parts.append(f"全正的 {n_all} 個（{ALL_POSITIVE_KEY}）")
        excluded = f"，本次排除{'與'.join(parts)}" if parts else ""
    else:
        ranking_rule = "主指標段（mAP／precision@K／recall@K）只算有正例的 query group"
        excluded = (f"，本次排除 {n_excl} 個（n_excluded_queries）"
                    if n_excl is not None else "")
    weight_col = prediction_quality["columns"].get("weight")
    if parameters.get("post_training") and weight_col:
        population = (
            f"母體：本段算在本次評估的全部候選列上，本身不排除任何 query group；"
            "但這是 --post-training，test 表在 dataset 階段只留下有正例的 query "
            "group 與一部分沒有正例的（filter_test_keys）。"
            + kept_zero_positive_groups_note(facts, parameters)
            + f"本段的列數與正例數都是乘上 {weight_col} 之後的加權數（共 "
            f"{n_rows:,}），代表還原到全部曝光的估計。{ranking_rule}{excluded}。"
            "兩段的母體不同，precision 不可互相比較。"
        )
    elif parameters.get("post_training"):
        # With the switch on the ranking section also dropped the all-positive
        # groups; the populations are the same only if there were none.
        same = n_excl == 0 and (not drop or n_all == 0)
        population = (
            f"母體：本段算在本次評估的全部 {n_rows:,} 列候選上，本身不排除任何 "
            "query group；但這是 --post-training，test 表在 dataset 階段已經丟掉"
            "沒有正例的 query group（filter_test_keys），所以這些列只來自"
            "「有正例的 query group」，不是全部曝光。正例佔比因此比全部曝光高，"
            "precision 與 pr_auc 會比在全部曝光上算的大；要留下這類 query group，"
            f"設 dataset.test_zero_positive_group_ratio。{ranking_rule}{excluded}。"
            + ("兩段的母體相同，但 precision 的定義不同（見下方 per-item 那一條），"
               "仍不可互相比較。" if same else
               "兩段的母體不同，precision 不可互相比較。")
        )
    else:
        population = (
            f"母體：本段算在本次評估的全部 {n_rows:,} 列候選上，不排除任何 query "
            f"group。{ranking_rule}{excluded}。兩段的母體不同，precision 不可"
            "互相比較。"
        )

    card = pd.DataFrame([{
        **_pq_summary_row(summary),
        "細箱寬（門檻解析度）": width,
        "分數範圍（本次資料的最小～最大）": f"{lo:g} ～ {bins_cfg['hi']:g}",
    }]).T
    card.columns = ["value"]

    bin_table = frame_from_json(overall[DISPLAY_BINS_KEY]).rename(
        columns=_PQ_BIN_COLUMNS)

    tables = [card, bin_table]
    titles = [
        "整體：關鍵數",
        f"整體：分箱表（{n_display} 格等寬，每格＝{n_bins // n_display} 個細箱）",
    ]
    collapsed = [False, False]

    listed = per_item.get("listed") or []
    n_items = per_item.get("n_items")
    if listed:
        rows = {item: _pq_summary_row(per_item["summary"][item])
                for item in listed}
        tables.append(pd.DataFrame(rows).T)
        titles.append(
            f"per-item：關鍵數（列數最多的前 {len(listed)} 個 {item_col}，"
            f"共 {n_items} 個；其餘只算進整體）"
        )
        collapsed.append(False)
        # Every listed item's display bins, in listed order, the item
        # column first.
        tables.append(frame_from_json(per_item[DISPLAY_BINS_KEY]).rename(
            columns=_PQ_BIN_COLUMNS))
        titles.append(f"per-item：分箱表（每個 {item_col} {n_display} 格，"
                      f"讀法同整體分箱表）")
        collapsed.append(True)

    figures = []
    if summary.get("n_pos"):
        figures.append(_pq_threshold_figure(
            frame_from_json(overall[THRESHOLD_SWEEP_KEY])))

    return ReportSection(
        title=_PQ_TITLE,
        description=(
            "這一段不看同一個 query group 裡的名次，而是把每一列候選當成一次"
            "「會不會是正例」的預測：分數 ≥ 門檻就預測為正。回答的問題是「照分數"
            "切一刀，切在哪裡、precision 與 recall 各是多少」，以及每一段分數"
            "裡實際有多少正例。" + population
        ),
        formula=(
            "precision＝TP÷(TP+FP)　recall＝TP÷全部正例　F1＝2·P·R÷(P+R)"
            "　門檻只取細箱的下緣；箱內的列視為同分"
        ),
        bullets=[
            f"門檻解析度：分數範圍取本次資料的最小～最大，切成 {n_bins} 個等寬"
            f"細箱，細箱寬 {width:g}。門檻只算得到細箱的邊界，所以 F1 最佳門檻的"
            f"解析度就是這個寬度；它也是在這份資料上挑的，換一份資料（下個月、"
            f"線上）分數分布會變，不能直接搬去當線上的設定值。",
            "pr_auc＝把箱內的列視為同分之後的 average precision（Σ 每箱正例佔全部"
            "正例的比例 × 該箱下緣的 precision）；roc_auc＝箱內同分算一半的 ROC "
            "面積。兩者都是「分箱後分數」的精確值，但箱內的先後已經丟掉，不等於"
            "在原始分數上算的值——不能拿來跟外部工具（例如 sklearn）在原始分數上"
            "算的 average precision 或 ROC-AUC 直接比較。",
            "per-item 的 precision 分母是該 item 在門檻以上的列數；主指標段的 "
            "precision@K 分母是 K（每個 query group 的前 K 名），是不同的量。",
            "分箱表把每格的平均分數與實際正例率放在一起，只是並列兩個量：框架"
            "不做校準（#411），分數不保證是機率。上面每個指標都只看分數的大小"
            "順序，兩者相近或相差都不改變它們。",
            "一個門檻橫跨所有 query group，前提是不同 query group 的分數彼此可比。"
            "模型用排序類目標（lambdarank、rank_xendcg）訓練時，分數只為同一個 "
            "query group 內的先後而學，跨 group 用同一個門檻切是模型沒有優化過的"
            "用法；binary 目標沒有這個問題。",
            "分數範圍取最小～最大，少數極端的高分會把細箱撐寬，多數列擠進少數幾箱；"
            "看分箱表各格的列數就知道是不是這樣。",
        ],
        figures=figures,
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )

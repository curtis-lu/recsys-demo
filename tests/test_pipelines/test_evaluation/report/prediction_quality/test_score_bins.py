"""預測品質, the ``--prediction-quality`` page's body
(``report/prediction_quality/score_bins``, ADR-0032 decision 5; drawn as in
#537): the headline numbers without the totals, the bin tables with three
upper headers (分數, 這一格, 門檻以上) and every listed item's bins in one
table. Which rows they are is ``test_population``'s.

The payload is ``aggregate_score_bins`` + ``build_payload`` on the rows of
``landed`` (20 fine bins over [0.1, 0.9], 5 display bins, the 3 items with
the most rows listed unless ``top_n`` says otherwise).
"""
from __future__ import annotations

import re

import pytest

from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    score_bins,
)
from recsys_tfb.evaluation.result_readers import frame_from_json
from recsys_tfb.report.types import (
    Bullets,
    Definitions,
    Figure,
    Formula,
    Paragraph,
    Table,
)
from tests.test_pipelines.test_evaluation.report.sections import landed

#: The demo's rows of 關鍵數, less 分數範圍 (two rows here: see
#: ``prediction_quality``); its last four are the columns of
#: F1 最佳門檻 · 各 item.
HEADLINE = ["PR-AUC", "ROC-AUC", "F1 最佳門檻", "該門檻的 precision",
            "該門檻的 recall", "該門檻的 F1"]
#: The demo's bin table columns: (upper header, header).
BIN_COLUMNS = [
    ("分數", "下緣"), ("分數", "上緣"),
    ("這一格", "候選列"), ("這一格", "正例數"), ("這一格", "平均分數"),
    ("這一格", "正例率"),
    ("門檻以上", "precision"), ("門檻以上", "recall"), ("門檻以上", "F1"),
]


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return score_bins.build(results, parameters), results


def _table(section, title: str) -> Table:
    (table,) = [b for b in section.blocks
                if isinstance(b, Table) and b.title == title]
    return table


def _text(section) -> str:
    parts = []
    for b in section.blocks:
        if isinstance(b, (Paragraph, Formula)):
            parts.append(b.text)
        elif isinstance(b, Bullets):
            parts.extend(b.items)
        elif isinstance(b, Table):
            parts += [b.title, *(n.text for n in b.notes)]
    return " ".join(parts)


def test_the_blocks_come_in_the_demo_s_order(spark):
    s, _ = _draw(spark)
    assert [type(b) for b in s.blocks] == [
        Paragraph, Formula, Bullets, Figure, Table, Definitions, Table, Table,
        Table]
    assert [b.title for b in s.blocks if isinstance(b, Table)] == [
        "關鍵數", "分箱", "F1 最佳門檻 · 各 item", "分箱 · 各 item"]
    assert (s.key, s.title, s.title_en) == (
        "score_bins", "預測品質", "Prediction quality")
    figure = next(b for b in s.blocks if isinstance(b, Figure))
    assert figure.fig.layout.title.text == "門檻對 precision、recall、F1 · 整體"
    # Nothing collapses but the figure's data table (owner 2026-10-24).
    from recsys_tfb.pipelines.evaluation.report.shell import generate_html_report
    assert re.findall(r"<details\b[^>]*>", generate_html_report([s])) == [
        '<details class="fig-data">']


def test_the_threshold_figure_s_data_table_is_its_points(spark):
    """One row per point of the curves (the sweep's order, highest threshold
    first), labelled by its fine bin's number from 1: 門檻 and the three
    curves' values."""
    s, results = _draw(spark)
    figure = next(b for b in s.blocks if isinstance(b, Figure))
    table = figure.data
    assert table.title == figure.fig.layout.title.text
    assert table.row_header == "細箱"
    assert [(c.label, c.kind) for c in table.columns] == [
        ("門檻", "ratio"), ("precision", "ratio"), ("recall", "ratio"),
        ("F1", "ratio")]
    sweep = frame_from_json(
        results["prediction_quality_metrics"]["overall"]["threshold_sweep"])
    assert [r.label for r in table.rows] == [
        str(b + 1) for b in sweep["bin"]]
    curves = {t.name: list(t.y) for t in figure.fig.data}
    assert [r.cells for r in table.rows] == [
        tuple(v) for v in zip(figure.fig.data[0].x, curves["precision"],
                              curves["recall"], curves["F1"])]


def test_the_headline_table_has_no_row_count_positives_or_rate(spark):
    s, results = _draw(spark)
    payload = results["prediction_quality_metrics"]
    summary, bins = payload["overall"]["summary"], payload["bins"]
    table = _table(s, "關鍵數")
    assert (table.row_header, [(c.label, c.kind) for c in table.columns]) == (
        "項目", [("值", "ratio")])
    labels = [r.label for r in table.rows]
    assert labels == HEADLINE + ["細箱寬", "分數範圍下緣", "分數範圍上緣"]
    for gone in ("候選列", "列數", "正例數", "正例率"):
        assert not [label for label in labels if gone in label], gone
    best = summary["best_f1"]
    assert [r.cells[0] for r in table.rows] == [
        summary["pr_auc"], summary["roc_auc"], best["threshold"],
        best["precision"], best["recall"], best["f1"],
        bins["width"], bins["lo"], bins["hi"]]
    # Since #577 the two areas are catalog entries: their definitions are
    # the catalog's.
    (defs,) = [b for b in s.blocks if isinstance(b, Definitions)]
    assert [term for term, _ in defs.entries] == ["PR-AUC", "ROC-AUC"]
    assert defs.source == "定義（從指標目錄來）"


def test_the_bin_table_has_three_upper_headers_and_reads_the_display_bins(
        spark):
    s, results = _draw(spark)
    landed_bins = frame_from_json(
        results["prediction_quality_metrics"]["overall"]["display_bins"])
    table = _table(s, "分箱")
    assert table.row_header == "格"
    assert [(c.group, c.label) for c in table.columns] == BIN_COLUMNS
    assert [c.kind for c in table.columns] == [
        "ratio", "ratio", "count", "count"] + ["ratio"] * 5
    assert [r.label for r in table.rows] == ["1", "2", "3", "4", "5"]
    keys = ["score_from", "score_to", "n", "n_pos", "mean_score",
            "positive_rate", "precision", "recall", "f1"]
    for row, (_, expected) in zip(table.rows, landed_bins.iterrows()):
        assert row.cells == pytest.approx(tuple(expected[keys]), nan_ok=True)
    assert sum(r.cells[2] for r in table.rows) == 24   # every candidate row


def test_the_per_item_best_f1_table_lists_the_listed_items(spark):
    """Best F1 only: every item's PR-AUC and ROC-AUC are ``item_areas``'
    table (#577), not printed twice."""
    s, results = _draw(spark)
    per_item = results["prediction_quality_metrics"]["per_item"]
    table = _table(s, "F1 最佳門檻 · 各 item")
    assert table.row_header == "item"
    assert [c.label for c in table.columns] == HEADLINE[2:]
    assert [r.label for r in table.rows] == per_item["listed"] == [
        "A", "B", "C"]
    a = per_item["summary"]["A"]
    assert table.rows[0].cells == tuple(a["best_f1"][k] for k in (
        "threshold", "precision", "recall", "f1"))


def test_the_budget_line_appears_only_when_items_are_left_out(spark):
    s, _ = _draw(spark)            # top_n 3 of 4 items
    (note,) = _table(s, "F1 最佳門檻 · 各 item").notes
    assert note.text == (
        "只列出候選列最多的前 3 個 item 的 F1 最佳門檻與分箱表"
        "（evaluation.prediction_quality.top_n）；每個 item 的 PR-AUC、"
        "ROC-AUC 在下一段。")
    every, _ = _draw(spark, top_n=4)
    assert _table(every, "F1 最佳門檻 · 各 item").notes == ()


def test_every_listed_item_s_bins_are_one_table(spark):
    """One table, the item the row label and 格 the bin's number within it,
    the three upper headers after 格; past 15 rows it is a tall table."""
    s, results = _draw(spark)
    long = frame_from_json(
        results["prediction_quality_metrics"]["per_item"]["display_bins"])
    (table,) = [b for b in s.blocks
                if isinstance(b, Table) and b.title.startswith("分箱 · ")]
    assert table.title == "分箱 · 各 item"
    assert table.row_header == "item"
    assert [(c.group, c.label, c.kind) for c in table.columns][0] == (
        None, "格", "count")
    assert [(c.group, c.label) for c in table.columns][1:] == BIN_COLUMNS
    assert [r.label for r in table.rows] == ["A"] * 5 + ["B"] * 5 + ["C"] * 5
    assert [r.cells[0] for r in table.rows] == [1, 2, 3, 4, 5] * 3
    assert [r.cells[3] for r in table.rows] == list(long["n"])
    assert not table.tall              # 15 rows
    every, _ = _draw(spark, top_n=4)
    assert _table(every, "分箱 · 各 item").tall   # 20 rows


def test_weighted_counts_print_as_ratios(spark):
    """--post-training with a kept share 0.3 of the zero-positive groups:
    C3's rows weigh 1 / 0.3, the counts are not whole, and a count column
    refuses them."""
    s, results = _draw(spark, post_training=True, ratio=landed.KEPT_RATIO)
    n = [r.cells[2] for r in _table(s, "分箱").rows]
    assert any(v != int(v) for v in n)
    for title in ("分箱", "分箱 · 各 item"):
        kinds = {c.label: c.kind for c in _table(s, title).columns}
        assert (kinds["候選列"], kinds["正例數"]) == ("ratio", "ratio")
    assert _table(s, "分箱 · 各 item").columns[0].kind == "count"   # 格


def test_the_weighted_counts_are_said_to_be_weighted(spark):
    weighted, _ = _draw(spark, post_training=True, ratio=landed.KEPT_RATIO)
    assert "加權數" in weighted.blocks[0].text
    plain, _ = _draw(spark)
    assert "加權數" not in plain.blocks[0].text
    assert "分箱表一格＝4 個細箱" in plain.blocks[0].text


def test_decision_7_s_statements_are_printed(spark):
    """ADR-0024 decision 7: the bin width is the threshold's resolution and
    not an online setting; PR-AUC is not an external average precision; a
    per-item precision is not precision@K; one threshold across groups
    needs comparable scores."""
    text = _text(_draw(spark)[0])
    assert "細箱寬 0.04" in text and "解析度" in text and "線上" in text
    assert "average precision" in text and "不能拿來跟外部" in text
    assert "precision@K 分母是 K" in text
    assert "lambdarank" in text and "跨" in text


def test_no_candidate_row_draws_one_sentence(spark):
    results, parameters = landed.results(spark)
    results["prediction_quality_metrics"] = landed.empty_quality(spark)
    s = score_bins.build(results, parameters)
    assert [type(b) for b in s.blocks] == [Paragraph]
    assert "沒有任何候選列" in s.blocks[0].text
    assert s.key == "score_bins"



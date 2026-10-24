"""名次分布 (``report_sections/rank_distribution``, #537): the figures, the two
share-by-rank tables (columns 第 r 名) and each item's mean positive rank
(the catalog's ``mean_pos``, Macro 平均 pinned), not collapsed — only each
figure's data table, under it, is (owner 2026-10-24).

The aggregates are ``aggregate_report_diagnostics``' and the metrics
``compute_all_metrics``', computed on the same rows (``landed``).
"""
from __future__ import annotations

import math
import re

import pytest

from recsys_tfb.evaluation.metric_catalog import ITEM, entry
from recsys_tfb.evaluation.report_sections import rank_distribution
from recsys_tfb.report.types import (
    Definitions,
    Figure,
    Note,
    Paragraph,
    Table,
)
from tests.test_evaluation.report_sections import landed

#: The demo's figure titles (decision 16: in Chinese, named like the tables).
DEMO_FIGURE_TITLES = [
    "分數分布 · 各 item", "分數分布 · 依 label", "名次計數 · 各 item",
    "正例名次計數 · 各 item", "各名次的正例率 · 各 item",
]


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return rank_distribution.build(results, parameters), results


def _table(section, title: str) -> Table:
    (table,) = [b for b in section.blocks
                if isinstance(b, Table) and b.title == title]
    return table


def test_description_figures_then_three_tables_and_the_definition(spark):
    s, _ = _draw(spark)
    assert [type(b) for b in s.blocks] == (
        [Paragraph] + [Figure] * 5 + [Table] * 3 + [Definitions])
    assert [b.title for b in s.blocks if isinstance(b, Table)] == [
        "名次佔比 · 各 item", "正例名次佔比 · 各 item", "正例平均名次 · 各 item"]
    assert (s.key, s.title, s.title_en) == (
        "rank_distribution", "名次分布", "Rank distribution")
    # Not collapsed (decision 16 item 10) — but each figure's data table.
    from recsys_tfb.evaluation.report import generate_html_report
    assert re.findall(r"<details\b[^>]*>", generate_html_report([s])) == [
        '<details class="fig-data">'] * 5


def test_the_figures_are_titled_in_chinese(spark):
    s, _ = _draw(spark)
    titles = [b.fig.layout.title.text for b in s.blocks
              if isinstance(b, Figure)]
    assert titles == DEMO_FIGURE_TITLES


def _figure(section, title: str) -> Figure:
    (figure,) = [b for b in section.blocks if isinstance(b, Figure)
                 and b.fig.layout.title.text == title]
    return figure


def test_every_figure_s_data_table_is_named_like_it(spark):
    s, _ = _draw(spark)
    assert [b.data.title for b in s.blocks if isinstance(b, Figure)] == (
        DEMO_FIGURE_TITLES)


@pytest.mark.parametrize("title, kind", [
    ("名次計數 · 各 item", "count"),
    ("正例名次計數 · 各 item", "count"),
    ("各名次的正例率 · 各 item", "ratio"),
])
def test_a_heatmap_s_data_table_is_the_matrix_it_draws(spark, title, kind):
    """Rows the heatmap's items, columns 第 r 名, the cells its ``z``."""
    figure = _figure(_draw(spark)[0], title)
    (heatmap,) = figure.fig.data
    table = figure.data
    assert table.row_header == "item"
    assert [r.label for r in table.rows] == list(heatmap.y) == [
        "A", "B", "C", "D"]
    assert [(c.label, c.kind) for c in table.columns] == [
        (f"第 {r} 名", kind) for r in (1, 2, 3, 4)]
    assert [x.replace("Rank ", "第 ") + " 名" for x in heatmap.x] == [
        c.label for c in table.columns]
    assert [c for r in table.rows for c in r.cells] == pytest.approx(
        [v for z in heatmap.z for v in z], nan_ok=True)


def test_the_histogram_s_data_table_is_its_bars(spark):
    """One row per item and non-empty bin, in the bars' order: 箱中心, 箱寬,
    候選列 — each item's trace's x, width and y."""
    figure = _figure(_draw(spark)[0], "分數分布 · 各 item")
    table = figure.data
    assert [(c.label, c.kind) for c in table.columns] == [
        ("箱中心", "ratio"), ("箱寬", "ratio"), ("候選列", "count")]
    drawn = [(str(t.name), x, w, y) for t in figure.fig.data
             for x, w, y in zip(t.x, t.width, t.y)]
    assert drawn, "premise: bars were drawn"
    assert [(r.label, *r.cells) for r in table.rows] == drawn


def test_the_boxplot_s_data_table_is_its_five_numbers(spark):
    """One row per item and label (1, then 0), in the boxes' order: the
    label, 下緣, Q1, 中位數, Q3, 上緣 — each trace's fences and quartiles; the
    line under it says what the fences are."""
    s, results = _draw(spark)
    figure = _figure(s, "分數分布 · 依 label")
    table = figure.data
    label_col = results["evaluation_report_aggregates"]["columns"]["label"]
    assert [(c.label, c.kind) for c in table.columns] == [
        (label_col, "text"), ("下緣", "ratio"), ("Q1", "ratio"),
        ("中位數", "ratio"), ("Q3", "ratio"), ("上緣", "ratio")]
    by_label = {"Positive": "1", "Negative": "0"}
    drawn = sorted(
        (str(item), by_label[t.name], lo, q1, med, q3, hi)
        for t in figure.fig.data
        for item, lo, q1, med, q3, hi in zip(
            t.x, t.lowerfence, t.q1, t.median, t.q3, t.upperfence))
    rows = [(r.label, *r.cells) for r in table.rows]
    assert rows == sorted(rows, key=lambda r: (r[0], -int(r[1])))
    assert sorted(rows) == drawn and len(drawn) == 8
    (note,) = table.notes
    assert "1.5" in note.text and "四分位距" in note.text


@pytest.mark.parametrize("title, matrix", [
    ("名次佔比 · 各 item", "rank_counts"),
    ("正例名次佔比 · 各 item", "positive_rank_counts"),
])
def test_a_share_table_is_each_rank_column_over_its_sum(spark, title, matrix):
    """Columns 第 1 名 … one per rank of the landed matrix; each cell is the
    item's count at that rank over the column's sum, so a column sums to 1."""
    s, results = _draw(spark)
    landed_matrix = results["evaluation_report_aggregates"][matrix]
    table = _table(s, title)
    assert table.row_header == "item"
    assert [c.label for c in table.columns] == [
        f"第 {r} 名" for r in landed_matrix["columns"]] == [
        "第 1 名", "第 2 名", "第 3 名", "第 4 名"]
    assert [r.label for r in table.rows] == ["A", "B", "C", "D"]
    counts = landed_matrix["data"]
    for j in range(len(table.columns)):
        column_sum = sum(row[j] for row in counts)
        cells = [r.cells[j] for r in table.rows]
        if column_sum == 0:
            # No positive at that rank (here: 第 4 名 of the positives): 0/0
            # is a blank column, not a column of zeros.
            assert all(math.isnan(c) for c in cells)
            continue
        assert cells == pytest.approx([row[j] / column_sum for row in counts])
        assert sum(cells) == pytest.approx(1.0)
    assert not any(r.pinned for r in table.rows)


def test_the_mean_positive_rank_is_the_catalog_s_mean_pos(spark):
    """Macro 平均 pinned, then each item: the bundle's ``mean_pos`` (the
    catalog's item entry), one column named like the table."""
    s, results = _draw(spark, min_positives=2)
    m = results["evaluation_metrics"]
    table = _table(s, "正例平均名次 · 各 item")
    assert table.row_header == "item"
    assert [(c.label, c.kind) for c in table.columns] == [
        ("正例平均名次", "ratio")]
    assert [r.label for r in table.rows] == ["Macro 平均", "A", "B", "C", "D"]
    assert [r.pinned for r in table.rows] == [True] + [False] * 4
    assert [r.cells for r in table.rows] == [
        (m["macro_avg"]["by_item"]["mean_pos"],),
        *((m["per_item"][i]["mean_pos"],) for i in landed.ITEMS)]
    # The macro averages A, B, C only (min_positives 2): not D's rank.
    assert m["macro_avg"]["by_item"]["mean_pos"] == pytest.approx(
        sum(m["per_item"][i]["mean_pos"] for i in "ABC") / 3)
    (defs,) = [b for b in s.blocks if isinstance(b, Definitions)]
    assert defs.entries == (
        ("正例平均名次", entry("mean_pos", ITEM).definition),)


def test_the_switch_on_says_which_numbers_keep_the_all_positive_groups(spark):
    """The aggregates read every evaluated row (#376), the metrics drop the
    all-positive groups with the switch on: the ⚠ line says which is which,
    and only when the switch is on."""
    on, _ = _draw(spark, drop=True)
    (note,) = [b for b in on.blocks if isinstance(b, Note)]
    assert note.warn
    assert note.text.startswith(rank_distribution.ALL_POSITIVE_KEPT_NOTE)
    assert "evaluation.query_filter.drop_all_positive_groups 開著" in note.text
    assert "正例平均名次跟著開關" in note.text
    off, _ = _draw(spark, drop=False)
    assert not [b for b in off.blocks if isinstance(b, Note)]


def test_the_note_is_drawn_once_on_the_page_with_its_mark(spark):
    from recsys_tfb.evaluation.report import generate_html_report

    on, _ = _draw(spark, drop=True)
    html = generate_html_report([on])
    assert html.count(rank_distribution.ALL_POSITIVE_KEPT_NOTE) == 1
    assert (f'<p class="warn-note">⚠ '
            f"{rank_distribution.ALL_POSITIVE_KEPT_NOTE}：") in html


def test_the_description_points_only_to_core_concepts(spark):
    s, _ = _draw(spark)
    text = s.blocks[0].text
    assert "（核心概念的總量）" in text
    for other in ("預測品質", "排序指標", "衡量指標", "分數分箱"):
        assert other not in text, other


def test_not_drawn_when_off_or_without_the_distribution_family(spark):
    results, parameters = landed.results(spark)
    parameters["evaluation"]["report"]["sections"] = {
        "rank_distribution": False}
    assert rank_distribution.build(results, parameters) is None
    results, parameters = landed.results(spark)
    # A report_aggregates.json from before #381: only the calibration bins.
    results["evaluation_report_aggregates"] = {
        "columns": results["evaluation_report_aggregates"]["columns"],
        "calibration": {"kind": "long", "columns": [], "data": []}}
    assert rank_distribution.build(results, parameters) is None

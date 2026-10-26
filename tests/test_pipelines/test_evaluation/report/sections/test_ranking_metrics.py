"""排序指標 (``report/sections/ranking_metrics``, #537): one table per catalog
metric at K — rows are slices (整體 and the segments, or Macro 平均 and the
items), columns the K the cut landed — at the item grain and, with
categories, at the category grain.

The results are computed (``landed``): the metrics bundle and the row facts
by ``metrics_spark``, the K by the cut node ``resolve_display_ks``. The
expected titles, headers and labels are the demo's words (ADR-0031 decision
16), written out here, not derived from the section.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from recsys_tfb.evaluation import metric_catalog as mc
from recsys_tfb.pipelines.evaluation.report.sections import ranking_metrics
from recsys_tfb.report.types import (
    Bullets,
    Definitions,
    Heading,
    Note,
    Paragraph,
    Table,
)
from tests.test_pipelines.test_evaluation.report.sections import landed

#: The demo's table titles, in page order (decision 16).
ITEM_GRAIN_TITLES = [
    "map@K · 整體與各分群", "precision@K · 整體與各分群",
    "recall@K · 整體與各分群", "f1@K · 整體與各分群",
    "hit_rate@K · 整體與各分群", "map_attr@K · 各 item",
    "per-item recall@K · 各 item",
]
CATEGORY_GRAIN_TITLES = [
    "map@K · 整體與各分群", "precision@K · 整體與各分群",
    "recall@K · 整體與各分群", "f1@K · 整體與各分群",
    "hit_rate@K · 整體與各分群", "map_attr@K · 各大類",
    "per-item recall@K · 各大類",
]


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return ranking_metrics.build(results, parameters), results


def _tables(section) -> list[Table]:
    return [b for b in section.blocks if isinstance(b, Table)]


def _table(section, title: str, nth: int = 0) -> Table:
    return [t for t in _tables(section) if t.title == title][nth]


def _labels(table: Table) -> list[str]:
    return [row.label for row in table.rows]


def _grain_tables(section) -> dict[str, list[Table]]:
    """The tables under each grain heading."""
    out: dict[str, list[Table]] = {}
    current = None
    for block in section.blocks:
        if isinstance(block, Heading):
            current = out.setdefault(block.text, [])
        elif isinstance(block, Table):
            current.append(block)
    return out


def _per_k_count() -> int:
    """How many per-K metrics the catalog holds, both methods."""
    return sum(1 for m in mc.METRICS if m.per_k)


def test_one_table_per_catalog_metric_at_k_at_each_grain(spark):
    s, _ = _draw(spark)
    grains = _grain_tables(s)
    assert list(grains) == ["item 粒度", "大類粒度"]
    assert [t.title for t in grains["item 粒度"]] == ITEM_GRAIN_TITLES
    assert [t.title for t in grains["大類粒度"]] == CATEGORY_GRAIN_TITLES
    # The count the acceptance names: the catalog's per-K metrics × 2 grains.
    assert len(_tables(s)) == _per_k_count() * 2 == 14


def test_without_categories_only_the_item_grain(spark):
    s, _ = _draw(spark, category=False)
    assert list(_grain_tables(s)) == ["item 粒度"]
    assert [t.title for t in _tables(s)] == ITEM_GRAIN_TITLES
    assert len(_tables(s)) == _per_k_count()


def test_the_blocks_come_in_the_demo_s_order(spark):
    s, _ = _draw(spark)
    kinds = [type(b).__name__ for b in s.blocks]
    assert kinds == (["Paragraph", "Heading"] + ["Table"] * 5 + ["Definitions"]
                     + ["Table"] * 2 + ["Definitions", "Heading"]
                     + ["Table"] * 7)
    assert s.blocks[0].muted
    assert (s.key, s.title, s.title_en) == (
        "ranking_metrics", "排序指標", "Ranking metrics")
    assert s.question == ranking_metrics.QUESTION
    from recsys_tfb.pipelines.evaluation.report.shell import generate_html_report
    assert "<details" not in generate_html_report([s])  # nothing collapses


def test_overall_is_pinned_first_then_each_segment(spark):
    s, results = _draw(spark)
    m = results["evaluation_metrics"]
    for grain in ("item 粒度", "大類粒度"):
        for table in _grain_tables(s)[grain][:3]:
            assert table.row_header == "分群"
            assert _labels(table) == ["整體", "seg_1", "seg_2"]
            assert [r.pinned for r in table.rows] == [True, False, False]
    precision = _table(s, "precision@K · 整體與各分群")
    assert precision.rows[0].cells == tuple(
        m["overall"][f"precision@{k}"] for k in (1, 2, 3, 4))
    assert precision.rows[2].cells == tuple(
        m["per_segment"]["seg_2"][f"precision@{k}"] for k in (1, 2, 3, 4))


def test_without_segment_columns_the_table_is_the_overall_alone(spark):
    s, _ = _draw(spark, segments=())
    table = _table(s, "map@K · 整體")
    assert _labels(table) == ["整體"]
    assert not [t for t in _tables(s) if "各分群" in t.title]


def test_with_two_segment_columns_a_segment_row_names_its_column(spark):
    """``per_segment`` slices by the first column; with two, the label says
    which (decision 16 rule: 欄名 值)."""
    s, results = _draw(spark, segments=(landed.SEGMENT_COL, landed.REGION_COL))
    assert sorted(results["evaluation_metrics"]["per_segment"]) == [
        "seg_1", "seg_2"]
    assert _labels(_table(s, "map@K · 整體與各分群")) == [
        "整體", "segment seg_1", "segment seg_2"]


def test_the_unmatched_segment_is_listed_last(spark):
    """``aggregate_per_segment`` hands the segments back in Spark's
    ``groupBy`` order, which can put the unmatched group anywhere: put it
    first, and the table still lists it last."""
    results, parameters = landed.results(spark, unmatched=True)
    per_segment = results["evaluation_metrics"]["per_segment"]
    results["evaluation_metrics"]["per_segment"] = {
        "(unmatched)": per_segment.pop("(unmatched)"), **per_segment}
    s = ranking_metrics.build(results, parameters)
    labels = _labels(_table(s, "map@K · 整體與各分群"))
    assert sorted(labels[1:-1]) == ["seg_1", "seg_2"]
    assert labels[-1] == "(unmatched)"


def test_macro_is_pinned_first_then_each_item_by_name(spark):
    """Items by name whatever order the bundle holds them in (the per-item
    pass collects them in Spark's order): reversed here."""
    results, parameters = landed.results(spark)
    m = results["evaluation_metrics"]
    m["per_item"] = dict(reversed(list(m["per_item"].items())))
    m["category"]["per_item"] = dict(
        reversed(list(m["category"]["per_item"].items())))
    s = ranking_metrics.build(results, parameters)
    attr = _table(s, "map_attr@K · 各 item")
    assert attr.row_header == "item"
    assert _labels(attr) == ["Macro 平均", "A", "B", "C", "D"]
    assert [r.pinned for r in attr.rows] == [True] + [False] * 4
    assert attr.rows[0].cells == tuple(
        m["macro_avg"]["by_item"][f"map_attr@{k}"] for k in (1, 2, 3, 4))
    recall = _table(s, "per-item recall@K · 各 item")
    assert recall.rows[2].cells == tuple(
        m["per_item"]["B"][f"recall@{k}"] for k in (1, 2, 3, 4))
    category = _table(s, "map_attr@K · 各大類")
    assert category.row_header == "大類"
    assert _labels(category) == ["Macro 平均", "cat_X", "cat_Y"]
    assert category.rows[0].pinned


def test_the_k_columns_are_the_landed_lists_resolved(spark):
    """The cut lands ``[1, 2, 3, 4]`` at the item grain (all = 4) and
    ``[1, 2]`` at the category grain (all = 2), "all" already the K it
    stands for (``steps/display_ks.k_columns``): the headers are those K."""
    s, results = _draw(spark)
    cut = results["evaluation_display_ks"]["evaluation_metrics"]
    assert cut["metrics_section"] == [1, 2, 3, 4] and cut["all_k"] == 4
    assert cut["category"]["metrics_section"] == [1, 2]
    grains = _grain_tables(s)
    for table in grains["item 粒度"]:
        assert [c.label for c in table.columns] == ["@1", "@2", "@3", "@4"]
        assert {c.kind for c in table.columns} == {"ratio"}
    for table in grains["大類粒度"]:
        assert [c.label for c in table.columns] == ["@1", "@2"]


def test_the_section_reads_the_k_off_the_cut_not_a_list_of_its_own(spark):
    """A cut landing other K: the headers follow it, and "all" reads the
    value stored at the K it resolved to (``precision@4``)."""
    results, parameters = landed.results(spark)
    m = results["evaluation_metrics"]
    results["evaluation_display_ks"]["evaluation_metrics"][
        "metrics_section"] = [2, "all"]
    results["evaluation_display_ks"]["evaluation_metrics"]["category"][
        "metrics_section"] = ["all"]
    s = ranking_metrics.build(results, parameters)
    precision = _table(s, "precision@K · 整體與各分群")
    assert [c.label for c in precision.columns] == ["@2", "@4"]
    assert precision.rows[0].cells == (m["overall"]["precision@2"],
                                       m["overall"]["precision@4"])
    category = _grain_tables(s)["大類粒度"][0]
    assert [c.label for c in category.columns] == ["@2"]


def test_all_is_the_k_the_metrics_recorded_not_the_item_count(spark):
    """With ``event`` declared C0 holds five rows for four items: "all" is
    @5 (``population.all_k``, #434), the header runs to @5 and that column
    reads ``map@5`` — the item count, 4, holds another number."""
    s, results = _draw(spark, event=True)
    m = results["evaluation_metrics"]
    assert m["population"]["all_k"] == 5
    assert results["evaluation_row_facts"]["dataset_overview"]["totals"][
        "n_items"] == 4
    assert "precision@4" not in m["overall"]       # not a K computed
    cut = results["evaluation_display_ks"]["evaluation_metrics"]
    precision = _table(s, "precision@K · 整體與各分群")
    assert cut["metrics_section"][-1] == 5
    assert [c.label for c in precision.columns] == [
        f"@{k}" for k in cut["metrics_section"]]
    assert precision.columns[-1].label == "@5"
    assert precision.rows[0].cells[-1] == m["overall"]["precision@5"]
    attr = _table(s, "map_attr@K · 各 item")
    assert attr.rows[1].cells[-1] == m["per_item"]["A"]["map_attr@5"]
    assert "@5 是全部候選" in s.blocks[0].text


def _notes(section) -> dict[str, list[str]]:
    return {f"{grain}:{t.title}": [n.text for n in t.notes]
            for grain, tables in _grain_tables(section).items()
            for t in tables}


def test_every_item_reaches_the_macro_so_no_count_line(spark):
    s, results = _draw(spark)
    assert results["evaluation_metrics"]["population"]["macro_items"] == [
        "A", "B", "C", "D"]
    assert all(notes == [] for notes in _notes(s).values())


def test_the_count_line_appears_when_fewer_items_reach_the_macro(spark):
    """min_positives 2 leaves D (one positive) out of the item macro, the
    metrics land ``macro_items`` A, B, C; both categories still reach theirs.
    The line sits under each item table of the item grain only."""
    s, results = _draw(spark, min_positives=2)
    assert results["evaluation_metrics"]["population"]["macro_items"] == [
        "A", "B", "C"]
    notes = _notes(s)
    for title in ("map_attr@K · 各 item", "per-item recall@K · 各 item"):
        assert notes[f"item 粒度:{title}"] == ["參與 macro 的 item 數：3"]
    assert [k for k, v in notes.items() if v] == [
        "item 粒度:map_attr@K · 各 item", "item 粒度:per-item recall@K · 各 item"]
    assert not any(n.warn for t in _tables(s) for n in t.notes)


def test_the_category_grain_counts_its_own_macro(spark):
    """min_positives 3: only A at the item grain, only cat_X (4 positives;
    cat_Y has 2) at the category grain."""
    s, results = _draw(spark, min_positives=3)
    assert results["evaluation_metrics"]["category"]["population"][
        "macro_items"] == ["cat_X"]
    notes = _notes(s)
    assert notes["item 粒度:map_attr@K · 各 item"] == ["參與 macro 的 item 數：1"]
    assert notes["大類粒度:map_attr@K · 各大類"] == ["參與 macro 的大類數：1"]
    assert notes["大類粒度:per-item recall@K · 各大類"] == ["參與 macro 的大類數：1"]


def _toy_item_metric() -> mc.Metric:
    return mc.Metric(
        name="toy_i", method=mc.ITEM, formula=mc.Ratio("hits", "n_pos"),
        per_k=True, grains=("per_item", "macro"), population="toy",
        display_name="toy-item", definition="toy 的定義。")


def test_a_per_k_item_metric_added_to_the_catalog_gets_its_tables(
        spark, monkeypatch):
    """The tables are the catalog's: an item metric added to it is drawn —
    a table per grain, its values read off ``per_item`` and the macro, its
    definition in the block — with no change to the section."""
    results, parameters = landed.results(spark)
    before = len(_tables(ranking_metrics.build(results, parameters)))
    monkeypatch.setattr(mc, "METRICS", mc.METRICS + (_toy_item_metric(),))
    m = results["evaluation_metrics"]
    for k in (1, 2, 3, 4):
        m["macro_avg"]["by_item"][f"toy_i@{k}"] = 0.1 * k
        for i, item in enumerate(landed.ITEMS):
            m["per_item"][item][f"toy_i@{k}"] = 0.01 * k + i
    s = ranking_metrics.build(results, parameters)
    assert len(_tables(s)) == before + 2
    toy = _table(s, "toy-item@K · 各 item")
    assert _labels(toy) == ["Macro 平均", "A", "B", "C", "D"]
    assert toy.rows[0].cells == pytest.approx((0.1, 0.2, 0.3, 0.4))
    assert toy.rows[2].cells == pytest.approx((1.01, 1.02, 1.03, 1.04))
    assert [t.title for t in _grain_tables(s)["大類粒度"]][-1] == (
        "toy-item@K · 各大類")
    item_defs = [b for b in s.blocks if isinstance(b, Definitions)][1]
    assert ("toy-item@K", "toy 的定義。") in item_defs.entries
    assert "toy-item" in s.blocks[0].text


def test_a_per_k_query_group_metric_added_to_the_catalog_gets_its_tables(
        spark, monkeypatch):
    results, parameters = landed.results(spark)
    monkeypatch.setattr(mc, "METRICS", mc.METRICS + (mc.Metric(
        name="toy", method=mc.QUERY_GROUP, formula=mc.Ratio("hits", "K"),
        per_k=True, grains=("overall", "per_segment"), population="toy",
        display_name="toy", definition="toy 的定義。"),))
    m = results["evaluation_metrics"]
    for k in (1, 2, 3, 4):
        m["overall"][f"toy@{k}"] = 0.1 * k
        m["per_segment"]["seg_1"][f"toy@{k}"] = 0.2 * k
    s = ranking_metrics.build(results, parameters)
    item_grain = [t.title for t in _grain_tables(s)["item 粒度"]]
    assert item_grain[:6] == ITEM_GRAIN_TITLES[:5] + ["toy@K · 整體與各分群"]
    toy = _table(s, "toy@K · 整體與各分群")
    assert toy.rows[0].cells == pytest.approx((0.1, 0.2, 0.3, 0.4))
    assert toy.rows[1].cells == pytest.approx((0.2, 0.4, 0.6, 0.8))
    assert ("toy@K", "toy 的定義。") in [
        b for b in s.blocks if isinstance(b, Definitions)][0].entries


def test_the_tables_and_the_description_name_metrics_by_display_name(
        spark, monkeypatch):
    monkeypatch.setattr(mc, "METRICS", tuple(
        replace(m, display_name="MAP!") if (m.name, m.method) == (
            "map", mc.QUERY_GROUP) else m for m in mc.METRICS))
    s, _ = _draw(spark)
    assert _tables(s)[0].title == "MAP!@K · 整體與各分群"
    assert "（MAP!、precision、recall、f1、hit_rate）" in s.blocks[0].text


def test_the_definitions_are_the_catalog_s_under_each_group(spark):
    s, _ = _draw(spark)
    defs = [b for b in s.blocks if isinstance(b, Definitions)]
    assert len(defs) == 2
    assert defs[0].entries == tuple(
        (f"{m.display_name}@K", m.definition)
        for m in (mc.entry("map", mc.QUERY_GROUP),
                  mc.entry("precision", mc.QUERY_GROUP),
                  mc.entry("recall", mc.QUERY_GROUP),
                  mc.entry("f1", mc.QUERY_GROUP),
                  mc.entry("hit_rate", mc.QUERY_GROUP)))
    assert defs[1].entries == (
        ("map_attr@K", mc.entry("map_attr", mc.ITEM).definition),
        ("per-item recall@K", mc.entry("recall", mc.ITEM).definition),
        mc.GRAIN_DEFINITIONS["macro"],
    )
    assert defs[0].source == "定義（從指標目錄來）"


def test_the_description_names_the_methods_and_the_column_at_all(spark):
    s, _ = _draw(spark)
    text = s.blocks[0].text
    assert "（map、precision、recall、f1、hit_rate）" in text
    assert "（map_attr、per-item recall）" in text
    assert "@4 是全部候選" in text


def test_no_ledger_no_totals_no_verdict(spark):
    """The group counts moved to 核心概念 (decision 16 item 12): no bullets,
    no ledger line, no total; and no verdict words."""
    s, results = _draw(spark, drop=True)
    assert not [b for b in s.blocks if isinstance(b, Bullets)]
    text = " ".join(b.text for b in s.blocks
                    if isinstance(b, (Paragraph, Note, Heading)))
    text += " ".join(t.title for t in _tables(s))
    for gone in ("組數帳", "頭號", "query 數", "已排除", "收合",
                 "沒有排除全正"):
        assert gone not in text, gone
    for bad in ("護欄", "達標", "偏高", "偏低", "不足", "異常", "嚴重", "良好"):
        assert bad not in text, bad
    facts = results["evaluation_row_facts"]
    assert f"{facts['n_queries']} " not in text


def test_shown_ks_resolves_all_and_keeps_each_k_once():
    assert ranking_metrics.shown_ks([1, 2, 3, 4, "all"], 4) == [1, 2, 3, 4]
    assert ranking_metrics.shown_ks([1, 3, "all"], 6) == [1, 3, 6]
    assert ranking_metrics.shown_ks(["all"], 2) == [2]

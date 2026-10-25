"""Where the comparison report marks each of the four things two sides must
share to be compared as they are (ADR-0031 decision 15; the comparison
demo's case 1–4): one test per thing, each making only that one differ, and
checking every place it is marked — the ⚠ on a column header or a row label,
the line saying both values — and the places it must not be. Inputs:
``fixtures.inputs`` (the real comparison and cut nodes)."""
from __future__ import annotations

import pytest

from recsys_tfb.pipelines.evaluation.report.comparison import (
    overall_metrics,
    overview,
    per_item_metrics,
    segment_metrics,
)
from recsys_tfb.evaluation.metric_catalog import QUERY_GROUP, CompareEntry
from recsys_tfb.report.types import Cell
from tests.test_pipelines.test_evaluation.report.comparison import fixtures as fx

ITEM_TABLES = ["map_attr@K · 各 item", "per-item recall@K · 各 item"]


@pytest.fixture
def draw(monkeypatch):
    """The sections each case looks at, off this side against a reference
    that differs as the case says (the segment list is non-empty so its
    section can be drawn)."""
    monkeypatch.setattr(segment_metrics, "COMPARED",
                        (CompareEntry("map", QUERY_GROUP, "per_segment"),))

    def _draw(differs: str, **reference):
        p = fx.params()
        results = fx.inputs(fx.bundle(0.6), fx.bundle(0.5, **reference), p)
        population = results["source_comparison"]["population"]
        assert [n for n, a in population.items() if not a["same"]] == [differs], \
            "premise: the fixture differs on this one only"
        return {m: fx.read(m, results, p) for m in (
            overview, overall_metrics, per_item_metrics, segment_metrics)}

    return _draw


def _check_row(sections, label: str, reference_cell: str):
    t = fx.table(sections[overview], "能不能比")
    marked = [r.label for r in t.rows if r.cells[2] == Cell("不同", warn=True)]
    assert marked == [label]
    assert fx.row(t, label).cells[1] == reference_cell


def _warned_groups(t) -> set:
    return {c.group for c in t.columns if c.warn}


def test_all_differs_marks_the_all_column(draw):
    """Case 1: the reference's "all" is @2, this side's @3. The @3 columns
    of every per-K table carry ⚠ (and only they), and one line says both;
    the model-selection values are untruncated, so that table is not
    marked."""
    s = draw("all_k", all_k=2)
    _check_row(s, "全部＝@幾", "@2")
    line = "@3：「全部」這次是 @3、對照是 @2，這一欄兩邊各取自己的全部。"
    for t in fx.tables(s[per_item_metrics]):
        assert _warned_groups(t) == {"@3"}, t.title
        assert not any(r.warn for r in t.rows), t.title
    assert fx.notes(s[per_item_metrics]).count(line) == 1
    (seg,) = fx.tables(s[segment_metrics])
    assert _warned_groups(seg) == {"@3"}
    assert line in fx.notes(s[segment_metrics])
    model = fx.table(s[overall_metrics], "模型選擇指標")
    assert not any(r.warn for r in model.rows) and not model.notes


def test_macro_items_differ_marks_the_macro_rows(draw):
    """Case 2: the reference's macro left item_C out. The Macro 平均 row of
    both item tables and the macro model-selection row carry ⚠, with a line
    each place; no column is marked."""
    s = draw("macro_items", macro_items=("item_A", "item_B"))
    _check_row(s, "進 Macro 的 item", "2 個，少 item_C")
    for title in ITEM_TABLES:
        t = fx.table(s[per_item_metrics], title)
        assert [r.label for r in t.rows if r.warn] == ["Macro 平均"], title
        assert not any(c.warn for c in t.columns), title
    assert ("Macro 平均：這次 3 個 item、對照 2 個（對照少 item_C），平均的 item "
            "不同。") in fx.notes(s[per_item_metrics])
    model = fx.table(s[overall_metrics], "模型選擇指標")
    assert [r.label for r in model.rows if r.warn] == ["macro per-item mAP"]
    assert [n.text for n in model.notes] == [
        "macro per-item mAP：這次 3 個 item、對照 2 個（對照少 item_C），平均的 "
        "item 不同。"]


def test_scored_query_groups_differ_is_a_line_only(draw):
    """Case 3: no header or row is marked; a line under the model-selection
    table and the segment table says both counts. The item tables carry no
    line (the demo's)."""
    s = draw("scored_queries", scored=9)
    _check_row(s, "算進指標的 query group", "9 組")
    line = "算進指標的 query group：這次 10 組、對照 9 組，兩邊的分母不同。"
    for sec in (s[overall_metrics], s[segment_metrics], s[per_item_metrics]):
        for t in fx.tables(sec):
            assert not any(c.warn for c in t.columns), t.title
            assert not any(r.warn for r in t.rows), t.title
    assert [n.text for n in fx.table(s[overall_metrics], "模型選擇指標").notes] \
        == [line]
    assert line in fx.notes(s[segment_metrics])
    assert not any("算進指標" in n for n in fx.notes(s[per_item_metrics]))


def test_segment_columns_differ_drops_the_segment_tables(draw):
    """Case 4: the reference was computed without segment columns. The
    segment section is not drawn at all; the 「能不能比」 table alone says
    so; nothing else is marked."""
    s = draw("segment_columns", segment_columns=())
    _check_row(s, "分群欄", "沒有")
    assert s[segment_metrics] is None
    for sec in (s[overall_metrics], s[per_item_metrics]):
        for t in fx.tables(sec):
            assert not any(c.warn for c in t.columns), t.title
            assert not any(r.warn for r in t.rows), t.title
    assert not any("分群" in n for n in fx.notes(s[per_item_metrics]))

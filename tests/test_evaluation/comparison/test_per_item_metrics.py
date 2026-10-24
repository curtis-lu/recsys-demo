"""各 item 指標 of the comparison report (``evaluation/comparison/
per_item_metrics.py``, #537): one compared table per entry of the section's
list, rows the items under a pinned Macro 平均, columns ``compare_k`` cut at
this side's "all". Inputs: ``fixtures.inputs`` (the real comparison and cut
nodes)."""
from __future__ import annotations

from dataclasses import replace

import pytest

from recsys_tfb.evaluation import metric_catalog as mc
from recsys_tfb.evaluation.comparison import per_item_metrics
from recsys_tfb.evaluation.metric_catalog import ITEM, CompareEntry
from recsys_tfb.report.types import Definitions
from tests.test_evaluation.comparison import fixtures as fx

TITLES = ["map_attr@K · 各 item", "per-item recall@K · 各 item"]


def _section(this, reference, p=None, **kw):
    p = p or fx.params()
    return fx.read(per_item_metrics, fx.inputs(this, reference, p, **kw), p)


def _three_per_k(ks) -> list[tuple]:
    return [(f"@{k}", label) for k in ks for label in ("這次", "對照", "差")]


@pytest.mark.parametrize("compare_k, all_k, expected", [
    # "all" is written as the K it resolved to.
    ((1, 3, "all"), 5, [1, 3, 5]),
    # An int K above "all" is cut; "all" already listed as an int is once.
    ((1, 3, 5, "all"), 4, [1, 3, 4]),
    ((1, 3, 5, "all"), 5, [1, 3, 5]),
])
def test_both_tables_take_compare_k_three_columns_each(compare_k, all_k,
                                                       expected):
    p = fx.params(compare_k=compare_k)
    sec = _section(fx.bundle(0.6, all_k=all_k), fx.bundle(0.5, all_k=all_k), p)
    assert [t.title for t in fx.tables(sec)] == TITLES
    for t in fx.tables(sec):
        assert t.row_header == "item"
        assert fx.column_groups(t) == _three_per_k(expected), t.title
        assert [c.kind for c in t.columns] == ["ratio", "ratio", "diff"] * len(
            expected)


def test_rows_are_the_macro_then_the_items_and_cells_are_the_comparison_s():
    """The column at "all" reads each side's own "all" (``name@all``); the
    others ``name@K``; 差 is 這次 − 對照."""
    this, reference = fx.bundle(0.6), fx.bundle(0.5)
    sec = _section(this, reference)
    attr = fx.table(sec, TITLES[0])
    assert [r.label for r in attr.rows] == ["Macro 平均", *fx.ITEMS]
    assert [r.pinned for r in attr.rows] == [True, False, False, False]
    a, b = (this["per_item"]["item_B"], reference["per_item"]["item_B"])
    assert fx.row(attr, "item_B").cells == pytest.approx((
        a["map_attr@1"], b["map_attr@1"], a["map_attr@1"] - b["map_attr@1"],
        a["map_attr@3"], b["map_attr@3"], a["map_attr@3"] - b["map_attr@3"]))
    macro_a = this["macro_avg"]["by_item"]["recall@3"]
    macro_b = reference["macro_avg"]["by_item"]["recall@3"]
    assert fx.row(fx.table(sec, TITLES[1]), "Macro 平均").cells[3:] == \
        pytest.approx((macro_a, macro_b, macro_a - macro_b))


def test_the_all_column_reads_each_side_at_its_own_all():
    """#434: the two sides' "all" can resolve to different K (event rows).
    The @{this side's all} column pairs each side's own: here this side's
    @7 with the reference's @9 — not a blank for a map_attr@7 the reference
    never computed under that name."""
    p = fx.params(compare_k=(1, "all"))
    this, reference = fx.bundle(0.6, all_k=7), fx.bundle(0.5, all_k=9)
    attr = fx.table(_section(this, reference, p), TITLES[0])
    assert [g for g, _ in fx.column_groups(attr)] == ["@1"] * 3 + ["@7"] * 3
    for label, a, b in (
            ("item_A", this["per_item"]["item_A"]["map_attr@7"],
             reference["per_item"]["item_A"]["map_attr@9"]),
            ("Macro 平均", this["macro_avg"]["by_item"]["map_attr@7"],
             reference["macro_avg"]["by_item"]["map_attr@9"])):
        assert fx.row(attr, label).cells[3:] == pytest.approx((a, b, a - b))


def test_an_item_one_side_lacks_keeps_a_blank_difference():
    """ADR-0020 bug 4: an item only this side has a positive for keeps its
    這次 cells and gets no 差 — not 差 = this side's value."""
    reference = fx.bundle(0.5)
    del reference["per_item"]["item_C"]
    cells = fx.row(fx.table(_section(fx.bundle(0.6), reference), TITLES[1]),
                   "item_C").cells
    assert cells[0] is not None
    assert cells[1:3] == (None, None)


def test_a_cell_the_list_names_but_the_comparison_lacks_is_refused():
    """An item whose per-item block lacks a listed K (landed off results
    computed for other K): refused with the node to re-run, not a blank."""
    this, reference = fx.bundle(0.6), fx.bundle(0.5)
    for b in (this, reference):
        del b["per_item"]["item_A"]["recall@1"]
    with pytest.raises(ValueError) as exc:
        _section(this, reference)
    msg = str(exc.value)
    assert "source_comparison has no per_item.cells.item_A.recall@1" in msg
    assert "--from-node compare_to_source" in msg


def test_the_macro_count_line_only_when_fewer_than_all():
    """Decision 16 item 5: 「參與 macro 的 item 數」 only when the macro
    averaged fewer items than all, the two sides agreeing on them — the
    count alone: the total is 核心概念's (decision 16 item 12), the one
    wording of ``metric_tables.macro_count_note``."""
    fewer = dict(macro_items=("item_A", "item_B"))
    sec = _section(fx.bundle(0.6, **fewer), fx.bundle(0.5, **fewer))
    assert "參與 macro 的 item 數：2" in fx.notes(sec)
    assert not any("參與 macro" in n for n in fx.notes(
        _section(fx.bundle(0.6), fx.bundle(0.5))))


def test_the_definitions_are_the_list_s_and_the_macro_s():
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    (defs,) = [b for b in sec.blocks if isinstance(b, Definitions)]
    assert [term for term, _ in defs.entries] == [
        "map_attr@K", "per-item recall@K", "Macro 平均"]


def test_no_section_when_the_compared_side_has_no_item():
    reference = fx.bundle(0.5)
    reference["per_item"] = {}
    assert _section(fx.bundle(0.6), reference) is None


def test_a_metric_added_to_the_catalog_and_the_list_adds_a_table(monkeypatch):
    """The section draws what its list names, read off the catalog: a new
    per-K item metric in the catalog, added to the list, the comparison
    holding its keys — one more table, its title and definition from the
    catalog, its cells the comparison's, and no code changed."""
    added = replace(mc.entry("recall", ITEM), name="hit_share",
                    display_name="hit share",
                    definition="（測試用）這個 item 的正例排進前 K 名的份額。")
    monkeypatch.setattr(mc, "METRICS", (*mc.METRICS, added))
    monkeypatch.setattr(per_item_metrics, "COMPARED", (
        *per_item_metrics.COMPARED, CompareEntry("hit_share", ITEM, "per_item")))
    this = fx.bundle(0.6, extra_item_metrics=("hit_share",))
    reference = fx.bundle(0.5, extra_item_metrics=("hit_share",))
    sec = _section(this, reference)
    assert [t.title for t in fx.tables(sec)] == [*TITLES, "hit share@K · 各 item"]
    new = fx.table(sec, "hit share@K · 各 item")
    assert fx.column_groups(new) == _three_per_k([1, 3])
    a = this["per_item"]["item_A"]["hit_share@1"]
    b = reference["per_item"]["item_A"]["hit_share@1"]
    assert fx.row(new, "item_A").cells[:3] == pytest.approx((a, b, a - b))
    (defs,) = [b for b in sec.blocks if isinstance(b, Definitions)]
    assert ("hit share@K", added.definition) in defs.entries


def test_the_section_declares_its_heading():
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    assert (sec.title, sec.title_en, sec.key) == (
        "各 item 指標", "Per item", "per_item_metrics")
    assert sec.question.endswith("？")

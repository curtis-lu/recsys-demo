"""The tables both reports draw (``report_sections/metric_tables``, #537): a
metric's table on one side and compared, the 「能不能比」 table and its flags,
the model-selection table, the definitions block.

Compared results are what ``compare_metric_results`` (the comparison layer)
returns for two bundles — computed here, not written by hand, so a table
reads the shape the layer really lands. The bundles carry the keys
``compute_all_metrics`` writes (``overall``, ``per_item``, ``macro_avg``,
``per_segment``, the ledger); the numbers are made up.
"""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.metric_catalog import (
    ITEM,
    QUERY_GROUP,
    CompareEntry,
    entry,
)
from recsys_tfb.evaluation.report_sections import metric_tables as mt
from recsys_tfb.pipelines.evaluation.steps.comparison_layer import (
    compare_metric_results,
)
from recsys_tfb.report.types import Cell

MINUS = "−"


def _bundle(base: float, *, all_k: int = 3, macro_items=("A", "B"),
            scored: int = 10, segment_columns=("seg",)) -> dict:
    """A metrics bundle: map@K overall and per segment, map_attr@K per item
    and its macro, at K = 1 .. all_k. Values step by 0.01 per K so each key
    reads apart."""
    ks = range(1, all_k + 1)

    def at(offset: float, name: str) -> dict:
        return {f"{name}@{k}": round(base + offset + k / 100, 6) for k in ks}

    return {
        "overall": at(0.0, "map"),
        "per_segment": {"s1": at(0.1, "map"), "s2": at(0.2, "map")},
        "per_item": {item: at(0.3 + i / 10, "map_attr")
                     for i, item in enumerate(("A", "B"))},
        "macro_avg": {"by_item": at(0.05, "map_attr")},
        "population": {
            "all_k": all_k, "macro_items": list(macro_items),
            "n_scored_queries_with_positive": scored,
            "n_scored_queries_without_positive": 0,
            "segment_columns": list(segment_columns),
        },
    }


def _values(table) -> list[tuple]:
    return [row.cells for row in table.rows]


def test_one_side_has_a_column_per_k_read_off_the_bundle():
    bundle = _bundle(0.5)
    table = mt.metric_table(
        entry("map", QUERY_GROUP), [1, 2, 3],
        [mt.SliceRow("整體", bundle["overall"], pinned=True),
         mt.SliceRow("s1", bundle["per_segment"]["s1"])],
        row_header="分群", slices="整體與各分群")
    assert table.title == "map@K · 整體與各分群"
    assert table.row_header == "分群"
    assert [c.label for c in table.columns] == ["@1", "@2", "@3"]
    assert {c.kind for c in table.columns} == {"ratio"}
    assert _values(table) == [(0.51, 0.52, 0.53), (0.61, 0.62, 0.63)]
    assert [r.pinned for r in table.rows] == [True, False]


def test_compared_has_three_columns_per_k_and_reads_each_side_s_own_all():
    """The reference's "all" is @2: the @3 column (this side's all) reads the
    cell pairing both sides' all (``map@all``), so 對照 there is the
    reference's map@2 — not a blank for a ``map@3`` it never had."""
    this, reference = _bundle(0.5), _bundle(0.4, all_k=2)
    comparison = compare_metric_results(this, reference)
    table = mt.metric_table(
        entry("map", QUERY_GROUP), [1, 3],
        [mt.SliceRow("整體", comparison["overall"]["cells"], pinned=True)],
        row_header="分群", slices="整體", compare=True, all_k=3, warn_ks=[3])
    assert [(c.group, c.label, c.kind) for c in table.columns] == [
        ("@1", "這次", "ratio"), ("@1", "對照", "ratio"), ("@1", "差", "diff"),
        ("@3", "這次", "ratio"), ("@3", "對照", "ratio"), ("@3", "差", "diff")]
    (cells,) = _values(table)
    assert cells[:3] == pytest.approx((0.51, 0.41, 0.10))
    assert cells[3:] == pytest.approx((0.53, 0.42, 0.11))
    assert [c.warn for c in table.columns] == [False] * 3 + [True] * 3


def test_compared_without_all_k_is_refused():
    comparison = compare_metric_results(_bundle(0.5), _bundle(0.4))
    with pytest.raises(ValueError, match="needs all_k"):
        mt.metric_table(entry("map", QUERY_GROUP), [1],
                        [mt.SliceRow("整體", comparison["overall"]["cells"])],
                        row_header="分群", compare=True)


#: Each case: the one annotation that differs, the reference bundle that
#: makes it differ, the 對照 cell of its row, and the line under a table.
_ONE_DIFFERENCE = {
    "all_k": (dict(all_k=2), "@2",
              "@3：「全部」這次是 @3、對照是 @2，這一欄兩邊各取自己的全部。"),
    "macro_items": (dict(macro_items=("A",)), "1 個，少 B",
                    "Macro 平均：這次 2 個 item、對照 1 個（對照少 B），平均的 "
                    "item 不同。"),
    "scored_queries": (dict(scored=9), "9 組",
                       "算進指標的 query group：這次 10 組、對照 9 組，兩邊的分母"
                       "不同。"),
    "segment_columns": (dict(segment_columns=()), "沒有",
                        "對照沒有分群欄 seg，各分群的對照與差留空。"),
}
_ROW_OF = {"all_k": "全部＝@幾", "macro_items": "進 Macro 的 item",
           "scored_queries": "算進指標的 query group", "segment_columns": "分群欄"}


@pytest.mark.parametrize("name", list(_ONE_DIFFERENCE))
def test_comparability_marks_only_the_one_that_differs(name):
    changed, reference_cell, sentence = _ONE_DIFFERENCE[name]
    comparison = compare_metric_results(_bundle(0.5), _bundle(0.4, **changed))
    population = comparison["population"]
    assert [n for n, a in population.items() if not a["same"]] == [name], \
        "premise: the fixture differs on this one only"

    flags = mt.comparability(population)
    assert flags.differs == (name,)
    note = flags.note(name)
    assert note is not None and note.warn and note.text == sentence
    assert [flags.note(n) for n in _ROW_OF if n != name] == [None] * 3

    table = mt.comparability_table(population)
    assert [c.label for c in table.columns] == ["這次", "對照", "結果"]
    rows = {r.label: r.cells for r in table.rows}
    assert list(rows) == list(_ROW_OF.values())
    assert rows[_ROW_OF[name]][1] == reference_cell
    assert rows[_ROW_OF[name]][2] == Cell("不同", warn=True)
    assert [cells[2] for label, cells in rows.items()
            if label != _ROW_OF[name]] == ["相同"] * 3


def test_model_selection_one_side_reads_each_entry_at_the_bundle_s_all():
    bundle = _bundle(0.5)
    table = mt.model_selection_table(bundle)
    assert table.title == "模型選擇指標" and table.row_header == "指標"
    assert [c.label for c in table.columns] == ["值"]
    assert [(r.label, r.cells) for r in table.rows] == [
        ("overall mAP", (bundle["overall"]["map@3"],)),
        ("macro per-item mAP", (bundle["macro_avg"]["by_item"]["map_attr@3"],)),
    ]
    assert table.notes == ()


def test_model_selection_compared_marks_the_macro_row_when_macro_items_differ():
    this, reference = _bundle(0.5), _bundle(0.4, all_k=2, macro_items=("A",))
    comparison = compare_metric_results(this, reference)
    table = mt.model_selection_table(comparison, compare=True)
    assert [c.label for c in table.columns] == ["這次", "對照", "差"]
    overall, macro = table.rows
    assert overall.cells == pytest.approx((0.53, 0.42, 0.11))  # each side's all
    assert macro.cells == pytest.approx((0.58, 0.47, 0.11))
    assert (overall.warn, macro.warn) == (False, True)
    # untruncated values: a different all_k marks nothing here
    assert [n.text for n in table.notes] == [
        "macro per-item mAP：這次 2 個 item、對照 1 個（對照少 B），平均的 item "
        "不同。"]


def test_definitions_come_from_the_catalog_one_term_once():
    block = mt.definitions([
        entry("map", QUERY_GROUP), CompareEntry("map", QUERY_GROUP, "per_segment"),
        entry("mean_pos", ITEM), "macro", mt.MODEL_SELECTION_METRICS[0]])
    assert [term for term, _ in block.entries] == [
        "map@K", "正例平均名次", "Macro 平均", "overall mAP"]
    assert dict(block.entries)["map@K"] == entry("map", QUERY_GROUP).definition
    assert block.source == "定義（從指標目錄來）"

"""整體指標 of the comparison report (``evaluation/comparison/
overall_metrics.py``, #537): the model-selection metrics at K = all, both
sides and their difference, and their definitions. Inputs:
``fixtures.inputs`` (the real comparison and cut nodes)."""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.comparison import overall_metrics
from recsys_tfb.evaluation.metric_catalog import MODEL_SELECTION_METRICS
from recsys_tfb.report.types import Definitions, Paragraph
from tests.test_evaluation.comparison import fixtures as fx


def _section(this, reference, p=None):
    p = p or fx.params()
    return fx.read(overall_metrics, fx.inputs(this, reference, p), p)


def test_one_row_per_model_selection_metric_three_columns():
    """Each metric's row is its source at each side's own "all": overall
    map@{all_k} and the Macro map_attr@{all_k} — here this side's "all" is
    @3 and the other's @4, so the 對照 values are read at @4."""
    this, reference = fx.bundle(0.6), fx.bundle(0.5, all_k=4)
    t = fx.table(_section(this, reference), "模型選擇指標")
    assert t.row_header == "指標"
    assert [(c.label, c.kind) for c in t.columns] == [
        ("這次", "ratio"), ("對照", "ratio"), ("差", "diff")]
    assert [r.label for r in t.rows] == ["overall mAP", "macro per-item mAP"]
    expect = {
        "overall mAP": (this["overall"]["map@3"], reference["overall"]["map@4"]),
        "macro per-item mAP": (this["macro_avg"]["by_item"]["map_attr@3"],
                               reference["macro_avg"]["by_item"]["map_attr@4"]),
    }
    for label, (a, b) in expect.items():
        cells = fx.row(t, label).cells
        assert cells == pytest.approx((a, b, a - b)), label
    assert not any("沒有" in n for n in fx.notes(_section(this, reference)))


def test_the_description_does_not_count_the_metrics():
    """Two today, four once the binary-prediction ones are computed: the
    words say neither."""
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    text = sec.blocks[0].text
    assert isinstance(sec.blocks[0], Paragraph) and sec.blocks[0].muted
    assert "模型選擇指標" in text and "差＝這次 − 對照" in text
    for count in ("兩個", "四個", "2 個", "4 個"):
        assert count not in text and count not in sec.question


def test_the_definitions_are_the_catalog_s():
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    (defs,) = [b for b in sec.blocks if isinstance(b, Definitions)]
    assert defs.entries == tuple((m.display_name, m.definition)
                                 for m in MODEL_SELECTION_METRICS)


def test_a_comparison_without_the_all_cell_is_refused():
    """Both sides' overall computed without @all_k: the comparison has no
    ``map@all`` cell, and a blank row would read as "no value"."""
    this, reference = fx.bundle(0.6), fx.bundle(0.5)
    for b in (this, reference):
        b["overall"] = {k: v for k, v in b["overall"].items()
                        if not k.endswith("@3")}
    with pytest.raises(ValueError) as exc:
        _section(this, reference)
    msg = str(exc.value)
    assert "source_comparison has no overall.cells.map@all" in msg
    assert "--from-node compare_to_source" in msg


def test_an_empty_side_keeps_a_blank_difference_and_says_why():
    """ADR-0020 bug 4: every query of the compared side had zero positives
    (its overall is ``{}``): its cells and the 差 are blank — never this
    side's value — and a line says which side and why."""
    reference = fx.bundle(0.5)
    reference["overall"] = {}
    sec = _section(fx.bundle(0.6), reference)
    cells = fx.row(fx.table(sec, "模型選擇指標"), "overall mAP").cells
    assert cells[0] is not None and cells[1:] == (None, None)
    assert "對照沒有可比的 query group（全部零正例），對照與差留空。" in fx.notes(sec)
    assert not any(n.startswith("這次沒有") for n in fx.notes(sec))


def test_a_macro_no_item_reached_says_so():
    none = dict(macro_items=())
    sec = _section(fx.bundle(0.6, **none), fx.bundle(0.5, **none))
    assert any(n.startswith("macro per-item mAP 沒有值：進 Macro 平均的 item "
                            "兩邊都是 0 個") for n in fx.notes(sec)), fx.notes(sec)
    assert not any("沒有值" in n for n in fx.notes(
        _section(fx.bundle(0.6), fx.bundle(0.5))))


def test_the_section_declares_its_heading():
    sec = _section(fx.bundle(0.6), fx.bundle(0.5))
    assert (sec.title, sec.title_en, sec.key) == (
        "整體指標", "Overall", "overall_metrics")
    assert sec.question.endswith("？")

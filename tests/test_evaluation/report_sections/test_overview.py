"""概覽 (``report_sections/overview``, #537): the model-selection metrics
side by side, each read off the metrics bundle at the K it recorded as
"all", and their definitions from the catalog.

The bundles are ``test_report_builder``'s, which carry the keys
``compute_all_metrics`` writes; the end-to-end value on a real computation is
``test_nodes.TestAllPositiveQueryGroups.test_the_report_headline_follows_it``
and ``test_metrics_spark_overview``'s event case.
"""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.metric_catalog import MODEL_SELECTION_METRICS
from recsys_tfb.evaluation.report_sections import overview
from recsys_tfb.report.types import Definitions, Note, Paragraph, Table
from tests.test_evaluation import test_report_builder as rbt


def _table(section, title: str) -> Table:
    tables = [b for b in section.blocks
              if isinstance(b, Table) and b.title == title]
    assert len(tables) == 1, [getattr(b, "title", b) for b in section.blocks]
    return tables[0]


def _values(table: Table) -> dict:
    return {row.label: row.cells for row in table.rows}


def test_the_two_rows_are_the_bundle_s_values_at_its_recorded_all():
    """``all_k`` is 4 (event rows), the item count 2: ``map@2`` and
    ``map_attr@2`` are real keys holding other numbers, and so is
    ``map_attr@3``. The values are K = all (decision 16 item 1)."""
    metrics = rbt._metrics_with_recorded_all_k()
    assert metrics["population"]["all_k"] == 4
    assert metrics["overall"]["map@2"] != metrics["overall"]["map@4"]
    assert (metrics["macro_avg"]["by_item"]["map_attr@3"]
            != metrics["macro_avg"]["by_item"]["map_attr@4"])
    params = rbt._ap_params(event=True)
    s = rbt._draw(overview, rbt._results(evaluation_metrics=metrics), params)
    table = _table(s, "模型選擇指標")
    assert [(c.label, c.kind) for c in table.columns] == [("值", "ratio")]
    assert _values(table) == {"overall mAP": (0.9,),
                              "macro per-item mAP": (0.55,)}


def test_the_section_is_drawn_in_blocks_with_the_catalog_s_definitions():
    s = rbt._draw(overview, rbt._results(evaluation_metrics=rbt._metrics()),
                  rbt._params())
    assert (s.key, s.title, s.title_en) == ("overview", "概覽", "Overview")
    assert s.question == overview.QUESTION
    from recsys_tfb.evaluation.report import generate_html_report
    assert "<details" not in generate_html_report([s])  # nothing collapses
    assert [type(b) for b in s.blocks] == [Paragraph, Table, Definitions]
    assert s.blocks[0].muted
    defs = s.blocks[2]
    assert defs.entries == tuple((m.display_name, m.definition)
                                 for m in MODEL_SELECTION_METRICS)


def test_the_description_counts_no_metric_and_ranks_none():
    """Two model-selection metrics today, the two APs later: the sentence
    must stay true either way, and no metric is the headline."""
    s = rbt._draw(overview, rbt._results(evaluation_metrics=rbt._metrics()),
                  rbt._params())
    text = s.blocks[0].text
    for wrong in ("四個", "兩個", "前兩個", "後兩個", "二元預測", "頭號",
                  "headline"):
        assert wrong not in text, wrong
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "嚴重", "良好"):
        assert bad not in text, bad
    assert "不分主次" in text


def test_a_macro_without_items_is_blank_and_says_why():
    """No item reached the macro: the bundle's macro cell is ``None`` and its
    ledger lists no item. The row stays (blank), with a line under it saying
    why — a bare blank reads as "computed, came out empty"."""
    metrics = rbt._metrics_with_recorded_all_k()
    metrics["macro_avg"]["by_item"]["map_attr@4"] = None
    metrics["population"]["macro_items"] = []
    s = rbt._draw(overview, rbt._results(evaluation_metrics=metrics),
                  rbt._params())
    assert _values(_table(s, "模型選擇指標"))["macro per-item mAP"] == (None,)
    notes = [b for b in s.blocks if isinstance(b, Note)]
    assert [n.text for n in notes] == [
        "macro per-item mAP 沒有值：進 Macro 平均的 item 是 0 個（只算正例數"
        "達到 metric.min_positives 的 item）。"]
    # With items in the macro, no such line.
    full = rbt._draw(overview, rbt._results(
        evaluation_metrics=rbt._metrics_with_recorded_all_k()), rbt._params())
    assert not [b for b in full.blocks if isinstance(b, Note)]

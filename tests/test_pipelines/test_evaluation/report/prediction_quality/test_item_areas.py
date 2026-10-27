"""AUC 與 AP (``report/prediction_quality/item_areas``, #577): the raw-score
APs, and every item's PR-AUC / ROC-AUC with its rows, positives and positive
rate beside them, the Macro first.

The payload is ``landed``'s: 4 items, the 3 with the most rows listed
(``top_n`` 3), so the area table holding 4 items is the one #577 asks for.
"""
from __future__ import annotations

import pytest

from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    item_areas,
)
from recsys_tfb.evaluation.result_readers import frame_from_json
from recsys_tfb.report.types import Bullets, Paragraph, Table
from tests.test_pipelines.test_evaluation.report.sections import landed


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return item_areas.build(results, parameters), results


def _table(section, title: str) -> Table:
    (table,) = [b for b in section.blocks
                if isinstance(b, Table) and b.title == title]
    return table


def test_the_ap_table_reads_the_two_raw_score_aps(spark):
    s, results = _draw(spark)
    ap = results["prediction_quality_metrics"]["average_precision"]
    table = _table(s, item_areas.AP_TABLE)
    assert {r.label: r.cells for r in table.rows} == {
        "pooled AP": (ap["overall"],), "macro per-item AP": (ap["macro"],)}


def test_every_item_has_a_row_not_only_the_listed_ones(spark):
    s, results = _draw(spark)
    per_item = results["prediction_quality_metrics"]["per_item"]
    assert len(per_item["listed"]) == 3
    table = _table(s, item_areas.AREA_TABLE)
    assert [c.label for c in table.columns] == [
        "候選列", "正例數", "正例佔比", "PR-AUC", "ROC-AUC"]
    totals = frame_from_json(per_item["totals"])
    assert [r.label for r in table.rows] == ["Macro 平均"] + list(
        totals["prod_name"])
    assert len(table.rows) == 1 + 4


def test_the_macro_row_is_pinned_and_reads_the_landed_macro(spark):
    s, results = _draw(spark)
    macro = results["prediction_quality_metrics"]["per_item"]["macro"]
    first = _table(s, item_areas.AREA_TABLE).rows[0]
    assert first.pinned
    assert first.cells == (None, None, None, macro["pr_auc"]["value"],
                           macro["roc_auc"]["value"])


def test_an_item_row_prints_its_positive_rate_beside_its_areas(spark):
    s, results = _draw(spark)
    totals = frame_from_json(
        results["prediction_quality_metrics"]["per_item"]["totals"])
    row = _table(s, item_areas.AREA_TABLE).rows[1]
    first = totals.iloc[0]
    assert row.cells == pytest.approx((
        first["n"], first["n_pos"], first["n_pos"] / first["n"],
        first["pr_auc"], first["roc_auc"]))


def test_the_macro_counts_print_as_x_of_y(spark):
    s, results = _draw(spark)
    macro = results["prediction_quality_metrics"]["per_item"]["macro"]
    (note,) = _table(s, item_areas.AREA_TABLE).notes
    assert (f"PR-AUC {macro['pr_auc']['n_items']}／{macro['n_items']}"
            in note.text)
    assert (f"ROC-AUC {macro['roc_auc']['n_items']}／{macro['n_items']}"
            in note.text)


def test_says_how_to_read_the_areas(spark):
    """#541's first comment, 〈怎麼呈現〉 3-5: PR-AUC against the positive
    rate, the score comparability under a ranking objective, blanks."""
    s, _ = _draw(spark)
    (bullets,) = [b for b in s.blocks if isinstance(b, Bullets)]
    text = "".join(bullets.items)
    for phrase in ("正例佔比", "lambdarank", "空格是沒有定義"):
        assert phrase in text, phrase


def test_a_payload_from_before_577_says_so(spark):
    results, parameters = landed.results(spark)
    payload = results["prediction_quality_metrics"]
    del payload["per_item"]["macro"], payload["average_precision"]
    s = item_areas.build(results, parameters)
    assert [type(b) for b in s.blocks] == [Paragraph]
    assert "--prediction-quality" in s.blocks[0].text

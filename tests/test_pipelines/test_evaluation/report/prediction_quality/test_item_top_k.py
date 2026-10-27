"""各 item 的 precision@K (``report/prediction_quality/item_top_k``, #577):
per-item precision@K and F1@K over every query group, Macro first, the
counts they divide in a table of their own."""
from __future__ import annotations

from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    item_top_k,
)
from recsys_tfb.report.types import Paragraph, Table
from tests.test_pipelines.test_evaluation.report.sections import landed


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return item_top_k.build(results, parameters), results


def _tables(section) -> dict:
    return {b.title: b for b in section.blocks if isinstance(b, Table)}


def test_one_table_per_metric_and_the_counts(spark):
    s, _ = _draw(spark)
    assert list(_tables(s)) == ["per-item precision@K · 各 item",
                                "per-item F1@K · 各 item",
                                item_top_k.COUNTS_TABLE]


def test_the_precision_table_reads_the_landed_cells(spark):
    s, results = _draw(spark)
    top_k = results["prediction_quality_metrics"]["per_item_top_k"]
    ks = top_k["k_values"]
    table = _tables(s)["per-item precision@K · 各 item"]
    assert [c.label for c in table.columns] == [f"@{k}" for k in ks]
    assert table.rows[0].label == "Macro 平均" and table.rows[0].pinned
    assert table.rows[0].cells == tuple(
        top_k["macro"][f"precision@{k}"]["value"] for k in ks)
    item, cells = next(iter(top_k["per_item"].items()))
    row = {r.label: r for r in table.rows}[item]
    assert row.cells == tuple(cells.get(f"precision@{k}") for k in ks)


def test_the_counts_table_prints_positives_and_times_in_the_top_k(spark):
    s, results = _draw(spark)
    top_k = results["prediction_quality_metrics"]["per_item_top_k"]
    table = _tables(s)[item_top_k.COUNTS_TABLE]
    item, cell = next(iter(top_k["shared_numbers"].items()))
    row = {r.label: r for r in table.rows}[item]
    assert row.cells == (cell["n_pos"], *(cell[f"n_top@{k}"]
                                          for k in top_k["k_values"]))


def test_says_its_population_counts_the_groups_without_a_positive(spark):
    s, _ = _draw(spark)
    assert "沒有正例的組也算" in s.blocks[0].text


def test_a_payload_from_before_577_says_so(spark):
    results, parameters = landed.results(spark)
    del results["prediction_quality_metrics"]["per_item_top_k"]
    s = item_top_k.build(results, parameters)
    assert [type(b) for b in s.blocks] == [Paragraph]

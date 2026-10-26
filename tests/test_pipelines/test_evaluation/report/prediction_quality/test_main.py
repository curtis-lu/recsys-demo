"""The ``--prediction-quality`` page (``report/prediction_quality/main``,
ADR-0032 decision 5): the population first, then the numbers, in the shared
page shell."""
from __future__ import annotations

from recsys_tfb.pipelines.evaluation.report.prediction_quality import main
from tests.test_pipelines.test_evaluation.report.sections import landed


def test_the_page_opens_with_the_population_then_the_numbers(spark):
    results, parameters = landed.results(
        spark, post_training=True, ratio=landed.KEPT_RATIO)
    html = main.assemble_prediction_quality_report(
        {"prediction_quality_metrics": results["prediction_quality_metrics"]},
        parameters)
    assert main.TITLE in html
    assert html.index('id="s-population"') < html.index('id="s-score_bins"')
    assert "test 母體（--post-training）" in html     # the metadata block


def test_the_page_reads_the_landed_payload_alone():
    assert main.READS == ("prediction_quality_metrics",)


def test_the_metadata_names_the_scope(spark):
    results, parameters = landed.results(spark)
    payload = results["prediction_quality_metrics"]
    html = main.assemble_prediction_quality_report(
        {"prediction_quality_metrics": payload}, parameters)
    assert "全部候選列（沒有 --scope）" in html
    payload["population"]["scope"] = {
        "name": "campaign_a", "n_rows_in_scope": 3,
        "n_scope_rows_not_scored": 0, "evaluated_dates_without_rows": []}
    payload["scope_definition"] = {
        "table": "mkt.campaign_a", "columns": {"snap_date": "d", "cust_id": "c"}}
    html = main.assemble_prediction_quality_report(
        {"prediction_quality_metrics": payload}, parameters)
    assert "全部候選列（沒有 --scope）" not in html

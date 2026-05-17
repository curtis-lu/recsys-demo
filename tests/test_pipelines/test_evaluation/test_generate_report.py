"""generate_report: dict-driven HTML, toPandas only when diagnostics on."""

from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report


def _params(diagnostics=False):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": "20240331", "report": {
            "sections": {"diagnostics": diagnostics},
            "display": {"primary_map_k": [1], "guardrail_recall_k": [1]},
            "diagnostics": {"include_distributions": diagnostics,
                            "include_calibration": False,
                            "sample_rows": None}}},
    }


def _eval_pred(spark):
    return spark.createDataFrame(
        [("20240331", "c1", "A", 0.9, 1, 1),
         ("20240331", "c1", "B", 0.1, 0, 2),
         ("20240331", "c2", "A", 0.2, 0, 2),
         ("20240331", "c2", "B", 0.8, 1, 1)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def _metrics():
    return {
        "overall": {"map@1": 0.5},
        "per_item": {"A": {"hit_rate@1": 0.5, "mean_pos": 1.5}},
        "per_segment": {},
        "dataset_overview": {
            "totals": {"n_rows": 4, "n_customers": 2, "n_products": 2,
                       "n_snap_dates": 1, "n_positives": 2,
                       "positive_rate": 0.5,
                       "avg_positives_per_customer": 1.0},
            "by_snap_date": {}, "by_item": {}},
        "n_queries": 2, "n_excluded_queries": 0,
    }


def test_generate_report_html_no_diagnostics(spark):
    html = generate_report(_eval_pred(spark), _metrics(), _params(False), None)
    assert html.startswith("<!DOCTYPE html>")
    assert "摘要 Headline" in html
    assert "<details" not in html      # diagnostics off


def test_generate_report_with_diagnostics(spark):
    html = generate_report(_eval_pred(spark), _metrics(),
                           _params(True), None)
    assert "<details" in html          # collapsible diagnostics present

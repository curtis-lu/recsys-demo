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
                            "include_calibration": False}}},
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


def _params_diag_full():
    p = _params(True)
    diag = p["evaluation"]["report"]["diagnostics"]
    diag["include_distributions"] = True
    diag["include_calibration"] = True
    diag["n_calibration_bins"] = 5
    return p


def _eval_pred_n(spark, n_customers):
    rows = []
    for i in range(n_customers):
        s = (i % 100) / 100.0
        rows.append(("20240331", f"c{i}", "A", s, i % 2, 1 if s > 0.5 else 2))
        rows.append(("20240331", f"c{i}", "B", 1.0 - s, (i + 1) % 2, 2 if s > 0.5 else 1))
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def test_diagnostics_report_size_bounded_by_row_count(spark):
    """The whole point of Spark-side aggregation: diagnostics figures embed
    aggregated values (bins/quartiles/matrices), so report size must not grow
    with the number of evaluation rows. Raw-array embedding would make the
    large report ~100s of KB bigger."""
    small = generate_report(
        _eval_pred_n(spark, 100), _metrics(), _params_diag_full(), None
    )
    large = generate_report(
        _eval_pred_n(spark, 3000), _metrics(), _params_diag_full(), None
    )
    assert abs(len(large) - len(small)) < 20000

"""End-to-end tests for evaluation pipeline pandas backend."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.evaluation.nodes_pandas import (
    compute_metrics,
    generate_report,
    prepare_eval_data,
)

# --- Fixtures ---

PRODUCTS = ["prod_a", "prod_b", "prod_c"]
CUSTOMERS = [f"C{i:04d}" for i in range(1, 21)]
SNAP_DATE = "20240131"


def _make_parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "evaluation": {
            "snap_date": SNAP_DATE,
            "k_values": [3, "all"],
            "segment_columns": ["cust_segment_typ"],
            "segment_sources": {},
            "baseline": {"type": "global_popularity"},
            "report": {
                "include_baseline_comparison": False,
                "include_calibration": True,
                "include_distributions": True,
                "n_calibration_bins": 5,
            },
        },
    }


@pytest.fixture
def parameters():
    return _make_parameters()


@pytest.fixture
def synthetic_data():
    """Generate synthetic ranked_predictions and label_table."""
    rng = np.random.default_rng(42)
    rows = []
    for cust_id in CUSTOMERS:
        scores = rng.random(len(PRODUCTS))
        ranks = np.argsort(-scores) + 1
        for i, prod in enumerate(PRODUCTS):
            rows.append({
                "snap_date": SNAP_DATE,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": float(scores[i]),
                "rank": int(ranks[i]),
            })
    ranked_predictions = pd.DataFrame(rows)

    label_rows = []
    for cust_id in CUSTOMERS:
        segment = rng.choice(["mass", "affluent", "hnw"])
        for prod in PRODUCTS:
            label_rows.append({
                "snap_date": SNAP_DATE,
                "cust_id": cust_id,
                "prod_name": prod,
                "label": int(rng.random() < 0.3),
                "cust_segment_typ": segment,
            })
    label_table = pd.DataFrame(label_rows)

    return ranked_predictions, label_table


# --- Tests ---


class TestPrepareEvalData:
    def test_output_has_label_column(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        result = prepare_eval_data(ranked_predictions, label_table, parameters)
        assert "label" in result.columns

    def test_output_has_score_and_rank(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        result = prepare_eval_data(ranked_predictions, label_table, parameters)
        assert "score" in result.columns
        assert "rank" in result.columns

    def test_row_count_matches_inner_join(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        result = prepare_eval_data(ranked_predictions, label_table, parameters)
        # Should have same rows as predictions (all match in our synthetic data)
        assert len(result) == len(ranked_predictions)

    def test_segment_column_carried(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        result = prepare_eval_data(ranked_predictions, label_table, parameters)
        assert "cust_segment_typ" in result.columns


class TestComputeMetrics:
    def test_returns_required_keys(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)

        assert "overall" in metrics
        assert "per_product" in metrics
        assert "per_segment" in metrics
        assert "macro_avg" in metrics
        assert "micro_avg" in metrics
        assert "n_queries" in metrics
        assert "n_excluded_queries" in metrics

    def test_overall_has_metrics_for_each_k(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)

        overall = metrics["overall"]
        # k_values: [3, "all"] -> k=3 and k=N (N=3 products, deduped to just 3)
        assert "map@3" in overall
        assert "ndcg@3" in overall
        assert "mrr@3" in overall
        assert "precision@3" in overall
        assert "recall@3" in overall

    def test_per_product_keys(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)

        per_product = metrics["per_product"]
        for prod in PRODUCTS:
            assert prod in per_product

    def test_metric_values_in_range(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)

        for metric_name, value in metrics["overall"].items():
            assert 0.0 <= value <= 1.0, f"{metric_name}={value} out of range"


class TestGenerateReport:
    def test_returns_html_string(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)
        html = generate_report(eval_data, metrics, parameters)

        assert isinstance(html, str)
        assert "<html>" in html
        assert "Metrics Summary" in html

    def test_report_with_baseline(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)

        # Enable baseline comparison
        parameters["evaluation"]["report"]["include_baseline_comparison"] = True

        # Create fake baseline metrics with same structure
        baseline_metrics = {
            "overall": {k: v * 0.5 for k, v in metrics["overall"].items()},
            "per_product": {},
            "per_segment": {},
            "per_product_segment": {},
            "macro_avg": {},
            "micro_avg": {},
            "n_queries": metrics["n_queries"],
            "n_excluded_queries": 0,
        }

        html = generate_report(eval_data, metrics, parameters, baseline_metrics)
        assert "Baseline Comparison" in html

    def test_report_without_baseline(self, synthetic_data, parameters):
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)
        html = generate_report(eval_data, metrics, parameters, baseline_metrics=None)

        assert isinstance(html, str)
        assert "Baseline Comparison" not in html


class TestEndToEnd:
    def test_full_pipeline_flow(self, synthetic_data, parameters):
        """Run all three nodes sequentially."""
        ranked_predictions, label_table = synthetic_data
        eval_data = prepare_eval_data(ranked_predictions, label_table, parameters)
        metrics = compute_metrics(eval_data, parameters)
        html = generate_report(eval_data, metrics, parameters)

        assert len(eval_data) > 0
        assert metrics["n_queries"] > 0
        assert len(html) > 100

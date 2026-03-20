"""Tests for evaluation.compare module."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.compare import (
    build_comparison_result,
    plot_comparison_metrics,
    plot_comparison_score_distributions,
)
from recsys_tfb.evaluation.metrics import compute_all_metrics


def _make_test_data(n_customers=10, products=None, seed=42):
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["exchange_fx", "fund_bond", "fund_stock"]

    pred_rows = []
    label_rows = []
    snap_date = "20240331"

    for i in range(n_customers):
        cust_id = f"C{i:04d}"
        scores = rng.rand(len(products))
        for j, prod in enumerate(products):
            pred_rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": scores[j],
                "rank": 0,
            })
            label_rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "label": int(rng.rand() > 0.6),
                "cust_segment_typ": ["mass", "affluent", "hnw"][i % 3],
            })

    preds = pd.DataFrame(pred_rows)
    preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)
    labels = pd.DataFrame(label_rows)
    return preds, labels


class TestBuildComparisonResult:
    def test_positive_delta_means_a_better(self):
        preds_a, labels = _make_test_data(seed=42)
        preds_b, _ = _make_test_data(seed=99)
        result_a = compute_all_metrics(preds_a, labels, k_values=[3])
        result_b = compute_all_metrics(preds_b, labels, k_values=[3])

        comparison = build_comparison_result(result_a, result_b)
        # Delta should equal A - B
        for metric in comparison["overall_delta"]:
            expected = result_a["overall"][metric] - result_b["overall"][metric]
            assert comparison["overall_delta"][metric] == expected

    def test_identical_models_zero_delta(self):
        preds, labels = _make_test_data()
        result = compute_all_metrics(preds, labels, k_values=[3])

        comparison = build_comparison_result(result, result)
        for metric, delta in comparison["overall_delta"].items():
            assert delta == 0.0

    def test_delta_at_all_levels(self):
        preds_a, labels = _make_test_data(seed=42)
        preds_b, _ = _make_test_data(seed=99)
        result_a = compute_all_metrics(preds_a, labels, k_values=[3])
        result_b = compute_all_metrics(preds_b, labels, k_values=[3])

        comparison = build_comparison_result(result_a, result_b)
        assert "overall_delta" in comparison
        assert "per_product_delta" in comparison
        assert "per_segment_delta" in comparison
        assert "macro_avg_delta" in comparison
        assert "micro_avg_delta" in comparison


class TestPlotComparisonMetrics:
    def test_returns_figures(self):
        preds_a, labels = _make_test_data(seed=42)
        preds_b, _ = _make_test_data(seed=99)
        result_a = compute_all_metrics(preds_a, labels, k_values=[3])
        result_b = compute_all_metrics(preds_b, labels, k_values=[3])

        comparison = build_comparison_result(result_a, result_b, "A", "B")
        figs = plot_comparison_metrics(comparison)
        assert len(figs) > 0
        assert all(isinstance(f, go.Figure) for f in figs)

    def test_side_by_side_bars(self):
        preds_a, labels = _make_test_data(seed=42)
        preds_b, _ = _make_test_data(seed=99)
        result_a = compute_all_metrics(preds_a, labels, k_values=[3])
        result_b = compute_all_metrics(preds_b, labels, k_values=[3])

        comparison = build_comparison_result(result_a, result_b, "A", "B")
        figs = plot_comparison_metrics(comparison)
        # Each figure should have 2 bar traces (A and B)
        for fig in figs:
            assert len(fig.data) == 2


class TestPlotComparisonScoreDistributions:
    def test_returns_figures(self):
        preds_a, _ = _make_test_data(seed=42)
        preds_b, _ = _make_test_data(seed=99)
        figs = plot_comparison_score_distributions(preds_a, preds_b)
        assert len(figs) > 0
        assert all(isinstance(f, go.Figure) for f in figs)

    def test_per_product_histograms(self):
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        preds_a, _ = _make_test_data(seed=42, products=products)
        preds_b, _ = _make_test_data(seed=99, products=products)
        figs = plot_comparison_score_distributions(preds_a, preds_b)
        # 2 figures per product (histogram + boxplot)
        assert len(figs) == len(products) * 2

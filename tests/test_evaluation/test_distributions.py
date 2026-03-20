"""Tests for evaluation.distributions module."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_positive_rate_rank_heatmap,
    plot_rank_heatmap,
    plot_score_distributions,
    plot_score_distributions_by_label,
)


def _make_predictions(n_customers=20, products=None, seed=42):
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["exchange_fx", "fund_bond", "fund_stock"]

    rows = []
    for i in range(n_customers):
        scores = rng.rand(len(products))
        for j, prod in enumerate(products):
            rows.append({
                "snap_date": "20240331",
                "cust_id": f"C{i:04d}",
                "prod_name": prod,
                "score": scores[j],
                "rank": 0,
            })
    df = pd.DataFrame(rows)
    df["rank"] = df.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)
    return df


class TestPlotScoreDistributions:
    def test_returns_two_figures(self):
        preds = _make_predictions()
        figs = plot_score_distributions(preds)
        assert len(figs) == 2
        assert all(isinstance(f, go.Figure) for f in figs)

    def test_all_products_shown(self):
        products = ["exchange_fx", "fund_bond", "fund_stock", "ccard_ins", "ccard_bill"]
        preds = _make_predictions(products=products)
        figs = plot_score_distributions(preds)
        # Histogram should have 5 traces
        assert len(figs[0].data) == 5
        # Boxplot should have 5 traces
        assert len(figs[1].data) == 5

    def test_title_prefix(self):
        preds = _make_predictions()
        figs = plot_score_distributions(preds, title_prefix="Model A: ")
        assert figs[0].layout.title.text.startswith("Model A: ")


class TestPlotRankHeatmap:
    def test_returns_figure(self):
        preds = _make_predictions()
        fig = plot_rank_heatmap(preds)
        assert isinstance(fig, go.Figure)

    def test_dimensions(self):
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        preds = _make_predictions(products=products)
        fig = plot_rank_heatmap(preds)
        heatmap = fig.data[0]
        assert heatmap.z.shape == (3, 3)  # 3 products × 3 rank positions

    def test_row_sums_equal_total_queries(self):
        n_customers = 20
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        preds = _make_predictions(n_customers=n_customers, products=products)
        fig = plot_rank_heatmap(preds)
        heatmap = fig.data[0]
        for row in heatmap.z:
            assert sum(row) == n_customers


def _make_labels(predictions, seed=42):
    rng = np.random.RandomState(seed)
    labels = predictions[["snap_date", "cust_id", "prod_name"]].copy()
    labels["label"] = (rng.rand(len(labels)) > 0.6).astype(int)
    return labels


class TestPlotPositiveRankHeatmap:
    def test_returns_figure(self):
        preds = _make_predictions()
        labels = _make_labels(preds)
        fig = plot_positive_rank_heatmap(preds, labels)
        assert isinstance(fig, go.Figure)

    def test_dimensions(self):
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        preds = _make_predictions(products=products)
        labels = _make_labels(preds)
        fig = plot_positive_rank_heatmap(preds, labels)
        heatmap = fig.data[0]
        assert heatmap.z.shape == (3, 3)

    def test_only_positive_counts(self):
        products = ["exchange_fx", "fund_bond"]
        preds = _make_predictions(n_customers=10, products=products)
        labels = _make_labels(preds, seed=99)
        fig = plot_positive_rank_heatmap(preds, labels)
        heatmap = fig.data[0]
        total_in_heatmap = sum(sum(row) for row in heatmap.z)
        assert total_in_heatmap == labels["label"].sum()


class TestPlotPositiveRateRankHeatmap:
    def test_returns_figure(self):
        preds = _make_predictions()
        labels = _make_labels(preds)
        fig = plot_positive_rate_rank_heatmap(preds, labels)
        assert isinstance(fig, go.Figure)

    def test_values_between_0_and_1(self):
        preds = _make_predictions()
        labels = _make_labels(preds)
        fig = plot_positive_rate_rank_heatmap(preds, labels)
        heatmap = fig.data[0]
        for row in heatmap.z:
            for val in row:
                assert 0.0 <= val <= 1.0


class TestPlotScoreDistributionsByLabel:
    def test_returns_list(self):
        preds = _make_predictions()
        labels = _make_labels(preds)
        figs = plot_score_distributions_by_label(preds, labels)
        assert isinstance(figs, list)
        assert len(figs) == 1
        assert isinstance(figs[0], go.Figure)

    def test_has_positive_and_negative_traces(self):
        preds = _make_predictions()
        labels = _make_labels(preds)
        figs = plot_score_distributions_by_label(preds, labels)
        trace_names = {t.name for t in figs[0].data}
        assert "Positive" in trace_names
        assert "Negative" in trace_names

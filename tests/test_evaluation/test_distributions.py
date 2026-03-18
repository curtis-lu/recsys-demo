"""Tests for evaluation.distributions module."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.distributions import (
    plot_rank_heatmap,
    plot_score_distributions,
)


def _make_predictions(n_customers=20, products=None, seed=42):
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["fx", "bond", "stock"]

    rows = []
    for i in range(n_customers):
        scores = rng.rand(len(products))
        for j, prod in enumerate(products):
            rows.append({
                "snap_date": "20240331",
                "cust_id": f"C{i:04d}",
                "prod_code": prod,
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
        products = ["fx", "bond", "stock", "fund", "insurance"]
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
        products = ["fx", "bond", "stock"]
        preds = _make_predictions(products=products)
        fig = plot_rank_heatmap(preds)
        heatmap = fig.data[0]
        assert heatmap.z.shape == (3, 3)  # 3 products × 3 rank positions

    def test_row_sums_equal_total_queries(self):
        n_customers = 20
        products = ["fx", "bond", "stock"]
        preds = _make_predictions(n_customers=n_customers, products=products)
        fig = plot_rank_heatmap(preds)
        heatmap = fig.data[0]
        for row in heatmap.z:
            assert sum(row) == n_customers

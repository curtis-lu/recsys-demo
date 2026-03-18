"""Score and rank distribution visualizations."""

import pandas as pd
import plotly.graph_objects as go


def plot_score_distributions(
    predictions: pd.DataFrame, title_prefix: str = ""
) -> list[go.Figure]:
    """Plot score distributions per product.

    Returns:
        List of two Figures: [histogram, boxplot].
    """
    products = sorted(predictions["prod_code"].unique())

    # Histogram
    fig_hist = go.Figure()
    for prod in products:
        scores = predictions[predictions["prod_code"] == prod]["score"]
        fig_hist.add_trace(
            go.Histogram(x=scores, name=prod, opacity=0.7, nbinsx=50)
        )
    fig_hist.update_layout(
        title=f"{title_prefix}Score Distribution (Histogram)",
        xaxis_title="Score",
        yaxis_title="Count",
        barmode="overlay",
        legend_title="Product",
    )

    # Boxplot
    fig_box = go.Figure()
    for prod in products:
        scores = predictions[predictions["prod_code"] == prod]["score"]
        fig_box.add_trace(go.Box(y=scores, name=prod))
    fig_box.update_layout(
        title=f"{title_prefix}Score Distribution (Boxplot)",
        yaxis_title="Score",
        legend_title="Product",
    )

    return [fig_hist, fig_box]


def plot_rank_heatmap(
    predictions: pd.DataFrame, title_prefix: str = ""
) -> go.Figure:
    """Plot rank distribution heatmap.

    Rows = products, Columns = rank positions.
    Cell values = count of how many times each product appears at each rank.
    """
    products = sorted(predictions["prod_code"].unique())
    n_products = len(products)
    ranks = list(range(1, n_products + 1))

    # Build count matrix
    rank_counts = predictions.groupby(["prod_code", "rank"]).size().unstack(fill_value=0)

    # Ensure all rank columns are present
    for r in ranks:
        if r not in rank_counts.columns:
            rank_counts[r] = 0
    rank_counts = rank_counts[ranks]

    # Reindex to sorted products
    rank_counts = rank_counts.reindex(products, fill_value=0)

    fig = go.Figure(
        data=go.Heatmap(
            z=rank_counts.values,
            x=[f"Rank {r}" for r in ranks],
            y=products,
            colorscale="Blues",
            text=rank_counts.values,
            texttemplate="%{text}",
            hovertemplate="Product: %{y}<br>%{x}<br>Count: %{z}<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"{title_prefix}Rank Distribution Heatmap",
        xaxis_title="Rank Position",
        yaxis_title="Product",
    )

    return fig

"""Score and rank distribution visualizations."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go


def plot_score_distributions(
    predictions: pd.DataFrame, title_prefix: str = "",
    item_col: str = "prod_name", score_col: str = "score",
) -> list[go.Figure]:
    """Plot score distributions per product.

    Returns:
        List of two Figures: [histogram, boxplot].
    """
    products = sorted(predictions[item_col].unique())

    # Histogram
    fig_hist = go.Figure()
    for prod in products:
        scores = predictions[predictions[item_col] == prod][score_col]
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
        scores = predictions[predictions[item_col] == prod][score_col]
        fig_box.add_trace(go.Box(y=scores, name=prod))
    fig_box.update_layout(
        title=f"{title_prefix}Score Distribution (Boxplot)",
        yaxis_title="Score",
        legend_title="Product",
    )

    return [fig_hist, fig_box]


def plot_rank_heatmap(
    predictions: pd.DataFrame, title_prefix: str = "",
    item_col: str = "prod_name", rank_col: str = "rank",
) -> go.Figure:
    """Plot rank distribution heatmap.

    Rows = products, Columns = rank positions.
    Cell values = count of how many times each product appears at each rank.
    """
    products = sorted(predictions[item_col].unique())
    n_products = len(products)
    ranks = list(range(1, n_products + 1))

    # Build count matrix
    rank_counts = predictions.groupby([item_col, rank_col]).size().unstack(fill_value=0)

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


def plot_positive_rank_heatmap(
    predictions: pd.DataFrame, labels: pd.DataFrame, title_prefix: str = "",
    id_cols: tuple = ("snap_date", "cust_id", "prod_name"),
    item_col: str = "prod_name", rank_col: str = "rank",
    label_col: str = "label",
) -> go.Figure:
    """Positive-label rank count heatmap.

    Merges predictions with labels, filters to label=1, then builds a heatmap
    of how many positive samples appear at each (product, rank) position.
    """
    merged = predictions.merge(
        labels[list(id_cols) + [label_col]],
        on=list(id_cols),
        how="left",
    )
    merged[label_col] = merged[label_col].fillna(0)
    pos = merged[merged[label_col] == 1]

    products = sorted(predictions[item_col].unique())
    n_products = len(products)
    ranks = list(range(1, n_products + 1))

    if len(pos) > 0:
        rank_counts = pos.groupby([item_col, rank_col]).size().unstack(fill_value=0)
    else:
        rank_counts = pd.DataFrame(0, index=products, columns=ranks)

    for r in ranks:
        if r not in rank_counts.columns:
            rank_counts[r] = 0
    rank_counts = rank_counts[ranks].reindex(products, fill_value=0)

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
        title=f"{title_prefix}Positive Label Rank Distribution",
        xaxis_title="Rank Position",
        yaxis_title="Product",
    )
    return fig


def plot_positive_rate_rank_heatmap(
    predictions: pd.DataFrame, labels: pd.DataFrame, title_prefix: str = "",
    id_cols: tuple = ("snap_date", "cust_id", "prod_name"),
    item_col: str = "prod_name", rank_col: str = "rank",
    label_col: str = "label",
) -> go.Figure:
    """Positive rate at each (product, rank) position heatmap.

    Cell value = count(label=1) / count(total) at each (product, rank).
    """
    merged = predictions.merge(
        labels[list(id_cols) + [label_col]],
        on=list(id_cols),
        how="left",
    )
    merged[label_col] = merged[label_col].fillna(0)

    products = sorted(predictions[item_col].unique())
    n_products = len(products)
    ranks = list(range(1, n_products + 1))

    total_counts = merged.groupby([item_col, rank_col]).size().unstack(fill_value=0)
    pos_counts = (
        merged[merged[label_col] == 1]
        .groupby([item_col, rank_col])
        .size()
        .unstack(fill_value=0)
    )

    for r in ranks:
        if r not in total_counts.columns:
            total_counts[r] = 0
        if r not in pos_counts.columns:
            pos_counts[r] = 0
    total_counts = total_counts[ranks].reindex(products, fill_value=0)
    pos_counts = pos_counts[ranks].reindex(products, fill_value=0)

    with np.errstate(divide="ignore", invalid="ignore"):
        rate = np.where(total_counts.values > 0, pos_counts.values / total_counts.values, 0.0)

    fig = go.Figure(
        data=go.Heatmap(
            z=rate,
            x=[f"Rank {r}" for r in ranks],
            y=products,
            colorscale="YlGnBu",
            text=rate,
            texttemplate="%{text:.1%}",
            hovertemplate="Product: %{y}<br>%{x}<br>Rate: %{z:.1%}<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"{title_prefix}Positive Rate by Rank Position",
        xaxis_title="Rank Position",
        yaxis_title="Product",
    )
    return fig


def plot_score_distributions_by_label(
    predictions: pd.DataFrame, labels: pd.DataFrame, title_prefix: str = "",
    id_cols: tuple = ("snap_date", "cust_id", "prod_name"),
    item_col: str = "prod_name", score_col: str = "score",
    label_col: str = "label",
) -> list[go.Figure]:
    """Score distributions split by positive/negative label.

    Returns a list with one grouped boxplot Figure.
    """
    merged = predictions.merge(
        labels[list(id_cols) + [label_col]],
        on=list(id_cols),
        how="left",
    )
    merged[label_col] = merged[label_col].fillna(0)
    merged["label_str"] = merged[label_col].map({1: "Positive", 0: "Negative"})

    products = sorted(merged[item_col].unique())

    fig = go.Figure()

    # Positive trace
    pos = merged[merged[label_col] == 1]
    fig.add_trace(
        go.Box(
            x=pos[item_col],
            y=pos[score_col],
            name="Positive",
            marker_color="green",
        )
    )

    # Negative trace
    neg = merged[merged[label_col] == 0]
    fig.add_trace(
        go.Box(
            x=neg[item_col],
            y=neg[score_col],
            name="Negative",
            marker_color="gray",
        )
    )

    fig.update_layout(
        title=f"{title_prefix}Score Distribution by Label",
        xaxis_title="Product",
        yaxis_title="Score",
        boxmode="group",
        legend_title="Label",
    )

    return [fig]

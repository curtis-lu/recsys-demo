"""Calibration curve visualizations."""

import pandas as pd
import plotly.graph_objects as go
from sklearn.calibration import calibration_curve


def plot_calibration_curves(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    n_bins: int = 10,
    title_prefix: str = "",
    id_cols: tuple = ("snap_date", "cust_id", "prod_name"),
    item_col: str = "prod_name",
    score_col: str = "score",
    label_col: str = "label",
) -> go.Figure:
    """Plot calibration curves per product.

    Args:
        predictions: DataFrame with id columns + score column.
        labels: DataFrame with id columns + label column.
        n_bins: Number of bins for calibration curve.
        title_prefix: Optional prefix for the chart title.
        id_cols: Column names that identify a unique (date, entity, item) tuple.
        item_col: Column name for the product/item.
        score_col: Column name for predicted scores.
        label_col: Column name for ground-truth labels.

    Returns:
        A single Figure with one trace per product plus a diagonal reference line.
    """
    merged = predictions.merge(
        labels[list(id_cols) + [label_col]],
        on=list(id_cols),
        how="inner",
    )

    products = sorted(merged[item_col].unique())

    fig = go.Figure()

    # Diagonal reference line
    fig.add_trace(
        go.Scatter(
            x=[0, 1],
            y=[0, 1],
            mode="lines",
            name="Perfect Calibration",
            line=dict(dash="dash", color="gray"),
            showlegend=True,
        )
    )

    for prod in products:
        subset = merged[merged[item_col] == prod]
        y_true = subset[label_col].values
        y_prob = subset[score_col].values

        if len(y_true) < n_bins or y_true.sum() == 0:
            continue

        prob_true, prob_pred = calibration_curve(
            y_true, y_prob, n_bins=n_bins, strategy="uniform"
        )

        fig.add_trace(
            go.Scatter(
                x=prob_pred,
                y=prob_true,
                mode="lines+markers",
                name=prod,
            )
        )

    fig.update_layout(
        title=f"{title_prefix}Calibration Curves",
        xaxis_title="Mean Predicted Probability",
        yaxis_title="Fraction of Positives",
        legend_title="Product",
    )

    return fig

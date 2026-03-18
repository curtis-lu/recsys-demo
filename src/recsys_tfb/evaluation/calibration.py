"""Calibration curve visualizations."""

import pandas as pd
import plotly.graph_objects as go
from sklearn.calibration import calibration_curve


def plot_calibration_curves(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    n_bins: int = 10,
    title_prefix: str = "",
) -> go.Figure:
    """Plot calibration curves per product.

    Args:
        predictions: DataFrame with [snap_date, cust_id, prod_code, score].
        labels: DataFrame with [snap_date, cust_id, prod_name, label].
        n_bins: Number of bins for calibration curve.
        title_prefix: Optional prefix for the chart title.

    Returns:
        A single Figure with one trace per product plus a diagonal reference line.
    """
    labels_renamed = labels.rename(columns={"prod_name": "prod_code"})
    merged = predictions.merge(
        labels_renamed[["snap_date", "cust_id", "prod_code", "label"]],
        on=["snap_date", "cust_id", "prod_code"],
        how="inner",
    )

    products = sorted(merged["prod_code"].unique())

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
        subset = merged[merged["prod_code"] == prod]
        y_true = subset["label"].values
        y_prob = subset["score"].values

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

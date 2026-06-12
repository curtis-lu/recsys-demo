"""Calibration curve visualizations.

Consumes the pre-aggregated per-item calibration points produced by
``evaluation.diagnostics_spark.calibration_bins`` (the skip rule for sparse /
all-negative items is applied there), so this module only draws the points.
"""

import pandas as pd
import plotly.graph_objects as go


def plot_calibration_curves(
    calib_bins: pd.DataFrame,
    title_prefix: str = "",
    item_col: str = "prod_name",
) -> go.Figure:
    """Plot per-item calibration curves plus a diagonal reference line.

    ``calib_bins`` columns: ``[item_col, "bin", "prob_pred", "prob_true"]``,
    already filtered to the items worth plotting and one point per non-empty
    bin.
    """
    fig = go.Figure()
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

    if not calib_bins.empty:
        for item in sorted(calib_bins[item_col].unique()):
            sub = calib_bins[calib_bins[item_col] == item].sort_values("bin")
            fig.add_trace(
                go.Scatter(
                    x=sub["prob_pred"].tolist(),
                    y=sub["prob_true"].tolist(),
                    mode="lines+markers",
                    name=str(item),
                )
            )

    fig.update_layout(
        title=f"{title_prefix}Calibration Curves",
        xaxis_title="Mean Predicted Probability",
        yaxis_title="Fraction of Positives",
        legend_title="Product",
    )
    return fig

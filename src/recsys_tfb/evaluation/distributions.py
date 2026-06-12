"""Score and rank distribution visualizations.

All plotting functions consume the small pre-aggregated frames produced by
``evaluation.diagnostics_spark`` (bin counts, boxplot stats, rank matrices), so
the rendered figures embed only aggregated values — never raw per-row arrays.
"""

import pandas as pd
import plotly.graph_objects as go

# d3 tickformat for count axes: thousands separators, no scientific notation.
_COUNT_TICKFORMAT = ","


def plot_score_histogram(
    hist_counts: pd.DataFrame,
    title_prefix: str = "",
    item_col: str = "prod_name",
) -> go.Figure:
    """Overlay histogram (one ``go.Bar`` trace per item) from shared-bin counts.

    ``hist_counts`` columns: ``[item_col, "bin_center", "count", "bin_width"]``.
    """
    fig = go.Figure()
    for item in sorted(hist_counts[item_col].unique()):
        sub = hist_counts[hist_counts[item_col] == item].sort_values("bin_center")
        fig.add_trace(
            go.Bar(
                x=sub["bin_center"].tolist(),
                y=sub["count"].tolist(),
                width=sub["bin_width"].tolist(),
                name=str(item),
                opacity=0.7,
            )
        )
    fig.update_layout(
        title=f"{title_prefix}Score Distribution (Histogram)",
        xaxis_title="Score",
        yaxis_title="Count",
        yaxis_tickformat=_COUNT_TICKFORMAT,
        barmode="overlay",
        legend_title="Product",
    )
    return fig


def _add_box(fig: go.Figure, name: str, stats: pd.DataFrame, **kwargs) -> None:
    """Add one pre-computed ``go.Box`` (no raw points) from a frame of stat
    rows. ``stats`` rows supply q1/median/q3/lowerfence/upperfence."""
    fig.add_trace(
        go.Box(
            q1=stats["q1"].tolist(),
            median=stats["median"].tolist(),
            q3=stats["q3"].tolist(),
            lowerfence=stats["lowerfence"].tolist(),
            upperfence=stats["upperfence"].tolist(),
            name=name,
            **kwargs,
        )
    )


def plot_score_boxplot(
    box_stats: pd.DataFrame,
    title_prefix: str = "",
    item_col: str = "prod_name",
) -> go.Figure:
    """Boxplot (one box per item) from pre-computed quartiles/fences.

    ``box_stats`` columns: ``[item_col, "q1", "median", "q3", "lowerfence",
    "upperfence"]`` (one row per item).
    """
    fig = go.Figure()
    for item in sorted(box_stats[item_col].unique()):
        sub = box_stats[box_stats[item_col] == item]
        _add_box(fig, str(item), sub)
    fig.update_layout(
        title=f"{title_prefix}Score Distribution (Boxplot)",
        yaxis_title="Score",
        legend_title="Product",
    )
    return fig


def plot_score_boxplot_by_label(
    box_stats_by_label: pd.DataFrame,
    title_prefix: str = "",
    item_col: str = "prod_name",
    label_col: str = "label",
) -> go.Figure:
    """Grouped boxplot split by positive/negative label, from pre-computed
    stats. ``box_stats_by_label`` columns: ``[item_col, label_col, "q1",
    "median", "q3", "lowerfence", "upperfence"]``."""
    items = sorted(box_stats_by_label[item_col].unique())
    fig = go.Figure()
    for label_val, name, color in ((1, "Positive", "green"), (0, "Negative", "gray")):
        sub = box_stats_by_label[box_stats_by_label[label_col] == label_val]
        sub = sub.set_index(item_col).reindex(items).dropna(how="all").reset_index()
        if sub.empty:
            continue
        _add_box(fig, name, sub, x=sub[item_col].tolist(), marker_color=color)
    fig.update_layout(
        title=f"{title_prefix}Score Distribution by Label",
        xaxis_title="Product",
        yaxis_title="Score",
        boxmode="group",
        legend_title="Label",
    )
    return fig


def _heatmap_from_matrix(
    matrix: pd.DataFrame,
    title: str,
    colorscale: str,
    texttemplate: str,
    rate: bool = False,
) -> go.Figure:
    """Build a rank heatmap from an item x rank count/rate matrix."""
    ranks = list(matrix.columns)
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.values,
            x=[f"Rank {r}" for r in ranks],
            y=list(matrix.index),
            colorscale=colorscale,
            text=matrix.values,
            texttemplate=texttemplate,
            hovertemplate=(
                "Product: %{y}<br>%{x}<br>"
                + ("Rate: %{z:.1%}" if rate else "Count: %{z}")
                + "<extra></extra>"
            ),
        )
    )
    fig.update_layout(title=title, xaxis_title="Rank Position", yaxis_title="Product")
    return fig


def plot_rank_heatmap(
    rank_matrix: pd.DataFrame, title_prefix: str = ""
) -> go.Figure:
    """Rank distribution heatmap from an item x rank count matrix."""
    return _heatmap_from_matrix(
        rank_matrix, f"{title_prefix}Rank Distribution Heatmap",
        "Blues", "%{text}",
    )


def plot_positive_rank_heatmap(
    pos_rank_matrix: pd.DataFrame, title_prefix: str = ""
) -> go.Figure:
    """Positive-label rank count heatmap from an item x rank matrix."""
    return _heatmap_from_matrix(
        pos_rank_matrix, f"{title_prefix}Positive Label Rank Distribution",
        "Blues", "%{text}",
    )


def plot_positive_rate_rank_heatmap(
    rate_matrix: pd.DataFrame, title_prefix: str = ""
) -> go.Figure:
    """Positive-rate-by-rank heatmap from an item x rank rate matrix."""
    return _heatmap_from_matrix(
        rate_matrix, f"{title_prefix}Positive Rate by Rank Position",
        "YlGnBu", "%{text:.1%}", rate=True,
    )

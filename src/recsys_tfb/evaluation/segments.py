"""Segment-level metrics and visualizations."""

import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.metrics import compute_all_metrics

logger = logging.getLogger(__name__)


def load_and_join_segment_sources(
    labels: pd.DataFrame,
    segment_sources: dict,
) -> pd.DataFrame:
    """Load external segment Parquet files and join them to labels.

    Args:
        labels: Labels DataFrame to enrich with external segment columns.
        segment_sources: Dict from parameters_evaluation.yaml, keyed by segment name.
            Each value has: filepath, key_columns, segment_column.

    Returns:
        Labels DataFrame with external segment columns joined (left join).
    """
    for seg_name, source_config in segment_sources.items():
        filepath = Path(source_config["filepath"])
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        if not filepath.exists():
            logger.warning(
                "Segment source '%s' file not found: %s — skipping",
                seg_name,
                filepath,
            )
            continue

        seg_df = pd.read_parquet(filepath)
        labels = labels.merge(
            seg_df[key_columns + [segment_column]],
            on=key_columns,
            how="left",
        )
        logger.info(
            "Joined segment source '%s' (%s) — %d/%d matched",
            seg_name,
            segment_column,
            labels[segment_column].notna().sum(),
            len(labels),
        )

    return labels


def compute_segment_metrics(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    segment_column: str = "cust_segment_typ",
    k_values: list[int | str] | None = None,
) -> dict:
    """Compute ranking metrics grouped by segment.

    Returns:
        Dict keyed by segment value, each containing the full metrics result
        from compute_all_metrics.
    """
    if k_values is None:
        k_values = [5, "all"]

    seg_map = labels[["cust_id", segment_column]].drop_duplicates("cust_id")

    # Join segment info to predictions
    pred_with_seg = predictions.merge(seg_map, on="cust_id", how="left")

    segments = sorted(pred_with_seg[segment_column].dropna().unique())
    result = {}

    for seg in segments:
        seg_preds = pred_with_seg[pred_with_seg[segment_column] == seg].drop(
            columns=[segment_column]
        )
        seg_labels = labels[labels[segment_column] == seg]

        if len(seg_preds) == 0 or len(seg_labels) == 0:
            continue

        result[seg] = compute_all_metrics(seg_preds, seg_labels, k_values=k_values)

    return result


def build_segment_metrics_table(segment_metrics: dict) -> pd.DataFrame:
    """Convert compute_segment_metrics result to a DataFrame.

    Args:
        segment_metrics: Dict keyed by segment value, each containing
            the full metrics result from compute_all_metrics.

    Returns:
        DataFrame with rows=segments, columns=metric names (from overall).
    """
    rows = {
        seg: metrics["overall"]
        for seg, metrics in segment_metrics.items()
        if "overall" in metrics
    }
    return pd.DataFrame.from_dict(rows, orient="index")


def plot_segment_charts(
    segment_metrics: dict, title_prefix: str = ""
) -> list[go.Figure]:
    """Plot grouped bar charts for segment-level metrics.

    Args:
        segment_metrics: Dict from compute_segment_metrics — keyed by segment,
            each value has an "overall" dict with metric values.

    Returns:
        List of Figures, one per metric.
    """
    return _plot_dimension_charts(segment_metrics, "Segment", title_prefix)



def _plot_dimension_charts(
    dim_metrics: dict, dim_label: str, title_prefix: str
) -> list[go.Figure]:
    """Create grouped bar charts from dimension metrics."""
    if not dim_metrics:
        return []

    # Get metric keys from first entry
    first_key = next(iter(dim_metrics))
    metric_keys = list(dim_metrics[first_key].get("overall", {}).keys())
    dim_values = sorted(dim_metrics.keys())

    figures = []
    for metric in metric_keys:
        fig = go.Figure()
        values = [
            dim_metrics[dv].get("overall", {}).get(metric, 0.0) for dv in dim_values
        ]
        fig.add_trace(
            go.Bar(x=dim_values, y=values, name=metric)
        )
        fig.update_layout(
            title=f"{title_prefix}{metric.upper()} by {dim_label}",
            xaxis_title=dim_label,
            yaxis_title=metric,
        )
        figures.append(fig)

    return figures

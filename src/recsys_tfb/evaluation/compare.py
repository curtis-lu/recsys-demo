"""Comparison logic for evaluating two models or model vs baseline."""

import pandas as pd
import plotly.graph_objects as go


def build_comparison_result(
    result_a: dict,
    result_b: dict,
    label_a: str = "Model A",
    label_b: str = "Model B",
) -> dict:
    """Compute deltas (A - B) for all metrics at all levels.

    Args:
        result_a: Output of compute_all_metrics for model A.
        result_b: Output of compute_all_metrics for model B.
        label_a: Display name for model A.
        label_b: Display name for model B.

    Returns:
        Dict with original results, labels, and all deltas.
    """
    comparison = {
        "label_a": label_a,
        "label_b": label_b,
        "result_a": result_a,
        "result_b": result_b,
    }

    # Overall delta
    comparison["overall_delta"] = _compute_delta(
        result_a.get("overall", {}), result_b.get("overall", {})
    )

    # Per-product delta
    comparison["per_product_delta"] = _compute_nested_delta(
        result_a.get("per_product", {}), result_b.get("per_product", {})
    )

    # Per-segment delta
    comparison["per_segment_delta"] = _compute_nested_delta(
        result_a.get("per_segment", {}), result_b.get("per_segment", {})
    )

    # Macro/micro avg delta
    comparison["macro_avg_delta"] = {}
    comparison["micro_avg_delta"] = {}
    for dim_key in ("by_product", "by_segment", "by_product_segment"):
        macro_a = result_a.get("macro_avg", {}).get(dim_key, {})
        macro_b = result_b.get("macro_avg", {}).get(dim_key, {})
        if macro_a or macro_b:
            comparison["macro_avg_delta"][dim_key] = _compute_delta(macro_a, macro_b)

        micro_a = result_a.get("micro_avg", {}).get(dim_key, {})
        micro_b = result_b.get("micro_avg", {}).get(dim_key, {})
        if micro_a or micro_b:
            comparison["micro_avg_delta"][dim_key] = _compute_delta(micro_a, micro_b)

    return comparison


def _compute_delta(metrics_a: dict, metrics_b: dict) -> dict:
    """Compute metric-level delta (A - B)."""
    all_keys = set(list(metrics_a.keys()) + list(metrics_b.keys()))
    return {
        k: metrics_a.get(k, 0.0) - metrics_b.get(k, 0.0)
        for k in sorted(all_keys)
    }


def _compute_nested_delta(nested_a: dict, nested_b: dict) -> dict:
    """Compute delta for each sub-key in a nested metrics dict."""
    all_keys = set(list(nested_a.keys()) + list(nested_b.keys()))
    return {
        k: _compute_delta(nested_a.get(k, {}), nested_b.get(k, {}))
        for k in sorted(all_keys)
    }


def plot_comparison_metrics(comparison: dict) -> list[go.Figure]:
    """Create side-by-side bar charts comparing two models per product.

    Returns a list of Figures, one per metric.
    """
    result_a = comparison["result_a"]
    result_b = comparison["result_b"]
    label_a = comparison["label_a"]
    label_b = comparison["label_b"]

    per_product_a = result_a.get("per_product", {})
    per_product_b = result_b.get("per_product", {})

    products = sorted(set(list(per_product_a.keys()) + list(per_product_b.keys())))

    if not products:
        return []

    # Get metric keys
    sample = per_product_a.get(products[0]) or per_product_b.get(products[0], {})
    metric_keys = list(sample.keys())

    figures = []
    for metric in metric_keys:
        values_a = [per_product_a.get(p, {}).get(metric, 0.0) for p in products]
        values_b = [per_product_b.get(p, {}).get(metric, 0.0) for p in products]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=products, y=values_a, name=label_a, marker_color="steelblue"))
        fig.add_trace(go.Bar(x=products, y=values_b, name=label_b, marker_color="darkorange"))
        fig.update_layout(
            title=f"{metric.upper()} Comparison",
            xaxis_title="Product",
            yaxis_title=metric,
            barmode="group",
        )
        figures.append(fig)

    return figures


def plot_comparison_score_distributions(
    predictions_a: pd.DataFrame,
    predictions_b: pd.DataFrame,
    label_a: str = "Model A",
    label_b: str = "Model B",
) -> list[go.Figure]:
    """Create overlay histograms and side-by-side boxplots comparing two models.

    Returns [histogram, boxplot] per product.
    """
    products = sorted(
        set(
            predictions_a["prod_code"].unique().tolist()
            + predictions_b["prod_code"].unique().tolist()
        )
    )

    figures = []

    for prod in products:
        scores_a = predictions_a[predictions_a["prod_code"] == prod]["score"]
        scores_b = predictions_b[predictions_b["prod_code"] == prod]["score"]

        # Overlay histogram
        fig_hist = go.Figure()
        fig_hist.add_trace(
            go.Histogram(x=scores_a, name=label_a, opacity=0.6, nbinsx=50)
        )
        fig_hist.add_trace(
            go.Histogram(x=scores_b, name=label_b, opacity=0.6, nbinsx=50)
        )
        fig_hist.update_layout(
            title=f"{prod}: Score Distribution Comparison",
            xaxis_title="Score",
            yaxis_title="Count",
            barmode="overlay",
        )
        figures.append(fig_hist)

        # Side-by-side boxplot
        fig_box = go.Figure()
        fig_box.add_trace(go.Box(y=scores_a, name=label_a))
        fig_box.add_trace(go.Box(y=scores_b, name=label_b))
        fig_box.update_layout(
            title=f"{prod}: Score Distribution Comparison (Boxplot)",
            yaxis_title="Score",
        )
        figures.append(fig_box)

    return figures

"""Comparison logic for evaluating two models or model vs baseline."""


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

    comparison["per_item_delta"] = _compute_nested_delta(
        result_a.get("per_item", {}), result_b.get("per_item", {})
    )
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



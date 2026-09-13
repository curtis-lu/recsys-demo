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
    """Compute metric-level delta (A - B), only for keys both sides have.

    A key missing on one side gets no delta entry, so the report leaves that
    cell blank (ADR-0020 bug 4). Reading the missing side as 0.0 printed A's
    absolute value as a Δ, and a 0 there reads as "the control scored 0" —
    no footnote can undo that reading.
    """
    return {
        k: metrics_a[k] - metrics_b[k]
        for k in sorted(set(metrics_a) & set(metrics_b))
    }


def _compute_nested_delta(nested_a: dict, nested_b: dict) -> dict:
    """Compute delta for each sub-key present on both sides (see _compute_delta)."""
    return {
        k: _compute_delta(nested_a[k], nested_b[k])
        for k in sorted(set(nested_a) & set(nested_b))
    }



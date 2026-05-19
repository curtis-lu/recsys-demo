"""Pure data-driven suggestion logic for the sampling overrides editor.

No Spark, no Typer, no I/O — unit-testable in isolation. Implements the D8
formulas from the design spec.
"""

from __future__ import annotations


def suggest_ratio(n_pos: int, n_neg: int, target_neg_pos: float) -> float:
    """Downsample ratio for negatives: keep all positives, target neg:pos = R.

    neg_ratio = clamp(R * n_pos / n_neg, 0, 1). n_neg == 0 -> 1.0 (nothing to
    downsample).
    """
    if n_neg <= 0:
        return 1.0
    return min(1.0, max(0.0, target_neg_pos * n_pos / n_neg))


def suggest_weight(
    n_pos: int, median_pos: float, alpha: float, w_max: float
) -> float:
    """Cold-product boost weight: clamp((median_pos/n_pos)**alpha, 1.0, w_max).

    n_pos <= 0 -> treated as maximally cold (returns w_max).
    """
    if n_pos <= 0:
        return w_max
    raw = (median_pos / n_pos) ** alpha
    return min(w_max, max(1.0, raw))

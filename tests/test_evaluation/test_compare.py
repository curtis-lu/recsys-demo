"""Tests for evaluation.compare module.

These tests exercise comparison/plot logic against synthetic result dicts —
they do not depend on a real metric computation. The shape of the dicts
matches what ``metrics_spark.compute_all_metrics`` produces.
"""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.compare import (
    build_comparison_result,
)


def _make_result_dict(seed: int) -> dict:
    """Construct a synthetic metrics dict in the new (post-redesign) shape."""
    rng = np.random.RandomState(seed)
    items = ["exchange_fx", "fund_bond", "fund_stock"]
    segments = ["mass", "affluent", "hnw"]

    overall = {
        "map@3": float(rng.rand()),
        "ndcg@3": float(rng.rand()),
        "precision@3": float(rng.rand()),
        "recall@3": float(rng.rand()),
    }
    per_item = {
        it: {
            "hit_rate@3": float(rng.rand()),
            "map_attr@3": float(rng.rand()),
            "ndcg_attr@3": float(rng.rand()),
            "mean_pos": float(rng.rand() * 3 + 1),
        }
        for it in items
    }
    per_segment = {
        s: {
            "map@3": float(rng.rand()),
            "ndcg@3": float(rng.rand()),
            "precision@3": float(rng.rand()),
            "recall@3": float(rng.rand()),
        }
        for s in segments
    }
    per_item_segment = {
        f"{it}_{s}": {
            "hit_rate@3": float(rng.rand()),
            "map_attr@3": float(rng.rand()),
            "ndcg_attr@3": float(rng.rand()),
            "mean_pos": float(rng.rand() * 3 + 1),
        }
        for it in items
        for s in segments
    }
    macro_avg = {
        "by_item": {
            "hit_rate@3": float(np.mean([v["hit_rate@3"] for v in per_item.values()])),
            "map_attr@3": float(np.mean([v["map_attr@3"] for v in per_item.values()])),
        },
        "by_segment": {
            "map@3": float(np.mean([v["map@3"] for v in per_segment.values()])),
        },
        "by_item_segment": {
            "hit_rate@3": float(
                np.mean([v["hit_rate@3"] for v in per_item_segment.values()])
            ),
        },
    }
    return {
        "overall": overall,
        "per_item": per_item,
        "per_segment": per_segment,
        "per_item_segment": per_item_segment,
        "macro_avg": macro_avg,
        "n_queries": 30,
        "n_excluded_queries": 0,
    }


class TestBuildComparisonResult:
    def test_positive_delta_equals_a_minus_b(self):
        result_a = _make_result_dict(seed=42)
        result_b = _make_result_dict(seed=99)

        comparison = build_comparison_result(result_a, result_b)
        for metric, delta in comparison["overall_delta"].items():
            expected = result_a["overall"][metric] - result_b["overall"][metric]
            assert delta == expected

    def test_identical_results_zero_delta(self):
        result = _make_result_dict(seed=42)
        comparison = build_comparison_result(result, result)
        for delta in comparison["overall_delta"].values():
            assert delta == 0.0

    def test_delta_keys_at_all_levels(self):
        result_a = _make_result_dict(seed=42)
        result_b = _make_result_dict(seed=99)
        comparison = build_comparison_result(result_a, result_b)
        assert "overall_delta" in comparison
        assert "per_item_delta" in comparison



def test_build_comparison_keeps_overall_and_per_item_only():
    from recsys_tfb.evaluation.compare import build_comparison_result
    a = {"overall": {"map@5": 0.5}, "per_item": {"A": {"hit_rate@5": 0.4}}}
    b = {"overall": {"map@5": 0.3}, "per_item": {"A": {"hit_rate@5": 0.1}}}
    c = build_comparison_result(a, b, "M", "B")
    assert c["overall_delta"]["map@5"] == pytest.approx(0.2)
    assert c["per_item_delta"]["A"]["hit_rate@5"] == pytest.approx(0.3)
    assert "per_segment_delta" not in c
    assert "macro_avg_delta" not in c


def test_plot_comparison_score_distributions_removed():
    import recsys_tfb.evaluation.compare as cmp
    assert not hasattr(cmp, "plot_comparison_score_distributions")


def test_plot_comparison_metrics_removed():
    import recsys_tfb.evaluation.compare as cmp
    assert not hasattr(cmp, "plot_comparison_metrics")

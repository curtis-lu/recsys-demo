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
        "precision@3": float(rng.rand()),
        "recall@3": float(rng.rand()),
    }
    per_item = {
        it: {
            "hit_rate@3": float(rng.rand()),
            "map_attr@3": float(rng.rand()),
            "mean_pos": float(rng.rand() * 3 + 1),
        }
        for it in items
    }
    per_segment = {
        s: {
            "map@3": float(rng.rand()),
            "precision@3": float(rng.rand()),
            "recall@3": float(rng.rand()),
        }
        for s in segments
    }
    per_item_segment = {
        it: {
            s: {
                "hit_rate@3": float(rng.rand()),
                "map_attr@3": float(rng.rand()),
                "mean_pos": float(rng.rand() * 3 + 1),
            }
            for s in segments
        }
        for it in items
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
                np.mean([
                    cell["hit_rate@3"]
                    for by_seg in per_item_segment.values()
                    for cell in by_seg.values()
                ])
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


def test_delta_only_where_both_sides_have_the_key():
    """bug 4 (ADR-0020): a key one side lacks gets no Δ at all.

    It used to read the missing side as 0.0, so an item with a positive on A
    only printed Δ = A's absolute value — a number no reader can tell from a
    real difference against a control group of 0.
    """
    a = {
        "overall": {"map@5": 0.5},
        "per_item": {
            "p1": {"hit_rate@5": 0.4, "map_attr@5": 0.3},
            "p2": {"hit_rate@5": 0.42},
        },
    }
    b = {
        "overall": {"map@5": 0.3},
        "per_item": {"p1": {"hit_rate@5": 0.1}},
    }
    c = build_comparison_result(a, b)
    assert "p2" not in c["per_item_delta"]           # item on A only
    assert "map_attr@5" not in c["per_item_delta"]["p1"]  # key on A only
    assert c["per_item_delta"]["p1"]["hit_rate@5"] == pytest.approx(0.3)
    assert c["overall_delta"]["map@5"] == pytest.approx(0.2)


def test_empty_overall_side_yields_no_overall_delta():
    """bug 4 (ADR-0020): every query on B had zero positives → ``overall`` is
    ``{}`` → no Δ, instead of Δ equal to A's absolute values."""
    a = {"overall": {"map@5": 0.5, "recall@5": 0.8}, "per_item": {}}
    b = {"overall": {}, "per_item": {}}
    assert build_comparison_result(a, b)["overall_delta"] == {}
    assert build_comparison_result(b, a)["overall_delta"] == {}


def test_plot_comparison_score_distributions_removed():
    import recsys_tfb.evaluation.compare as cmp
    assert not hasattr(cmp, "plot_comparison_score_distributions")


def test_plot_comparison_metrics_removed():
    import recsys_tfb.evaluation.compare as cmp
    assert not hasattr(cmp, "plot_comparison_metrics")

"""Tests for evaluation.compare module.

These tests exercise comparison/plot logic against synthetic result dicts —
they do not depend on a real metric computation. The shape of the dicts
matches what ``metrics_spark.compute_all_metrics`` produces.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.compare import (
    build_comparison_result,
    plot_comparison_metrics,
    plot_comparison_score_distributions,
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


def _make_predictions_df(seed: int, n_customers: int = 10) -> pd.DataFrame:
    """For score-distribution plots; only needs cust_id / prod_name / score."""
    rng = np.random.RandomState(seed)
    items = ["exchange_fx", "fund_bond", "fund_stock"]
    rows = []
    for i in range(n_customers):
        scores = rng.rand(len(items))
        for j, prod in enumerate(items):
            rows.append({"cust_id": f"C{i:04d}", "prod_name": prod, "score": float(scores[j])})
    return pd.DataFrame(rows)


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
        assert "per_segment_delta" in comparison
        assert "macro_avg_delta" in comparison
        # Macro avg delta carries by_item / by_segment / by_item_segment when populated.
        assert "by_item" in comparison["macro_avg_delta"]
        assert "by_segment" in comparison["macro_avg_delta"]
        assert "by_item_segment" in comparison["macro_avg_delta"]


class TestPlotComparisonMetrics:
    def test_returns_one_figure_per_metric_key(self):
        result_a = _make_result_dict(seed=42)
        result_b = _make_result_dict(seed=99)
        comparison = build_comparison_result(result_a, result_b, "A", "B")
        figs = plot_comparison_metrics(comparison)
        # 4 metric keys per item: hit_rate@3, map_attr@3, ndcg_attr@3, mean_pos
        expected_metric_count = len(next(iter(result_a["per_item"].values())))
        assert len(figs) == expected_metric_count
        assert all(isinstance(f, go.Figure) for f in figs)

    def test_each_figure_has_two_traces(self):
        result_a = _make_result_dict(seed=42)
        result_b = _make_result_dict(seed=99)
        comparison = build_comparison_result(result_a, result_b, "A", "B")
        figs = plot_comparison_metrics(comparison)
        for fig in figs:
            assert len(fig.data) == 2


class TestPlotComparisonScoreDistributions:
    def test_returns_figures(self):
        preds_a = _make_predictions_df(seed=42)
        preds_b = _make_predictions_df(seed=99)
        figs = plot_comparison_score_distributions(preds_a, preds_b)
        assert len(figs) > 0
        assert all(isinstance(f, go.Figure) for f in figs)

    def test_two_figures_per_item(self):
        preds_a = _make_predictions_df(seed=42)
        preds_b = _make_predictions_df(seed=99)
        figs = plot_comparison_score_distributions(preds_a, preds_b)
        items = sorted(
            set(preds_a["prod_name"].unique().tolist() + preds_b["prod_name"].unique().tolist())
        )
        # 2 figures per item (histogram + boxplot)
        assert len(figs) == len(items) * 2

"""Tests for evaluation.distributions — plotting from pre-aggregated frames.

The figures must embed only the aggregated values (bin counts / box stats /
matrices), never raw per-row arrays, so figure size does not scale with the
number of evaluation rows.
"""

import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_positive_rate_rank_heatmap,
    plot_rank_heatmap,
    plot_score_boxplot_by_label,
    plot_score_histogram,
)


class TestPlotScoreHistogram:
    def _hist(self):
        return pd.DataFrame(
            {
                "prod_name": ["A", "A", "B"],
                "bin_center": [0.25, 0.75, 0.25],
                "count": [2, 3, 5],
                "bin_width": [0.5, 0.5, 0.5],
            }
        )

    def test_one_bar_trace_per_item(self):
        fig = plot_score_histogram(self._hist())
        assert isinstance(fig, go.Figure)
        assert all(isinstance(t, go.Bar) for t in fig.data)
        assert {t.name for t in fig.data} == {"A", "B"}

    def test_bar_data_is_bounded_to_bins(self):
        fig = plot_score_histogram(self._hist())
        a = next(t for t in fig.data if t.name == "A")
        # A has two bins -> two bars (not row count)
        assert list(a.x) == [0.25, 0.75]
        assert list(a.y) == [2, 3]

    def test_count_axis_avoids_scientific_notation(self):
        fig = plot_score_histogram(self._hist())
        assert fig.layout.yaxis.tickformat is not None


class TestPlotScoreBoxplotByLabel:
    def _stats(self):
        return pd.DataFrame(
            {
                "prod_name": ["A", "A", "B", "B"],
                "label": [1, 0, 1, 0],
                "q1": [1.0, 1.0, 2.0, 2.0],
                "median": [2.0, 2.0, 3.0, 3.0],
                "q3": [3.0, 3.0, 4.0, 4.0],
                "lowerfence": [0.0, 0.0, 1.0, 1.0],
                "upperfence": [4.0, 4.0, 5.0, 5.0],
            }
        )

    def test_positive_and_negative_traces(self):
        fig = plot_score_boxplot_by_label(self._stats())
        assert {t.name for t in fig.data} == {"Positive", "Negative"}

    def test_no_raw_points(self):
        fig = plot_score_boxplot_by_label(self._stats())
        for t in fig.data:
            assert t.y is None
            assert len(t.q1) == 2  # one box per item, bounded


class TestRankHeatmaps:
    def _matrix(self):
        return pd.DataFrame([[1, 1], [1, 1]], index=["A", "B"], columns=[1, 2])

    def test_rank_heatmap_from_matrix(self):
        fig = plot_rank_heatmap(self._matrix())
        hm = fig.data[0]
        assert isinstance(hm, go.Heatmap)
        assert list(hm.y) == ["A", "B"]
        assert hm.z.shape == (2, 2)

    def test_positive_rank_heatmap_from_matrix(self):
        fig = plot_positive_rank_heatmap(self._matrix())
        assert isinstance(fig.data[0], go.Heatmap)

    def test_positive_rate_heatmap_from_matrix(self):
        rate = pd.DataFrame(
            [[0.5, 0.0], [1.0, 0.25]], index=["A", "B"], columns=[1, 2]
        )
        fig = plot_positive_rate_rank_heatmap(rate)
        hm = fig.data[0]
        assert isinstance(hm, go.Heatmap)
        assert ((hm.z >= 0.0) & (hm.z <= 1.0)).all()

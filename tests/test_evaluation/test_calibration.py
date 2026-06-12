"""Tests for evaluation.calibration — plotting from pre-aggregated bins.

The skip rule (too few rows / no positives) now lives in
diagnostics_spark.calibration_bins; this module just draws the points it is
given, so the input frame already contains only the items to plot.
"""

import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.calibration import plot_calibration_curves


def _calib_bins():
    return pd.DataFrame(
        {
            "prod_name": ["A", "A", "B", "B"],
            "bin": [0, 1, 0, 1],
            "prob_pred": [0.1, 0.6, 0.2, 0.7],
            "prob_true": [0.0, 1.0, 0.3, 0.8],
        }
    )


class TestPlotCalibrationCurves:
    def test_returns_figure(self):
        fig = plot_calibration_curves(_calib_bins())
        assert isinstance(fig, go.Figure)

    def test_first_trace_is_reference_diagonal(self):
        fig = plot_calibration_curves(_calib_bins())
        assert fig.data[0].name == "Perfect Calibration"

    def test_one_curve_per_item_plus_diagonal(self):
        fig = plot_calibration_curves(_calib_bins())
        names = [t.name for t in fig.data]
        assert "A" in names and "B" in names
        assert len(fig.data) == 3  # diagonal + A + B

    def test_curve_uses_bin_means_only(self):
        fig = plot_calibration_curves(_calib_bins())
        a = next(t for t in fig.data if t.name == "A")
        assert list(a.x) == [0.1, 0.6]
        assert list(a.y) == [0.0, 1.0]

    def test_empty_frame_yields_only_diagonal(self):
        empty = pd.DataFrame(
            columns=["prod_name", "bin", "prob_pred", "prob_true"]
        )
        fig = plot_calibration_curves(empty)
        assert len(fig.data) == 1
        assert fig.data[0].name == "Perfect Calibration"

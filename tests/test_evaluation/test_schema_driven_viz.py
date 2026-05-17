"""Regression: viz/stats helpers accept non-default column names."""

import pandas as pd

from recsys_tfb.evaluation.statistics import compute_product_statistics
from recsys_tfb.evaluation.distributions import plot_score_distributions
from recsys_tfb.evaluation.calibration import plot_calibration_curves


def test_product_statistics_custom_cols():
    df = pd.DataFrame({
        "item": ["A", "A", "B"], "uid": ["c1", "c2", "c1"],
        "y": [1, 0, 1]})
    stats = compute_product_statistics(
        df, item_col="item", entity_col="uid", label_col="y")
    assert "positive_rate" in stats.columns
    assert set(stats.index) == {"A", "B"}


def test_score_distributions_custom_cols():
    df = pd.DataFrame({"item": ["A", "B"], "sc": [0.1, 0.9]})
    figs = plot_score_distributions(df, item_col="item", score_col="sc")
    assert len(figs) == 2


def test_calibration_custom_cols():
    preds = pd.DataFrame({"t": ["d"] * 4, "u": ["c1", "c2", "c3", "c4"],
                          "item": ["A"] * 4, "sc": [0.2, 0.4, 0.6, 0.8]})
    labs = pd.DataFrame({"t": ["d"] * 4, "u": ["c1", "c2", "c3", "c4"],
                         "item": ["A"] * 4, "y": [0, 0, 1, 1]})
    fig = plot_calibration_curves(
        preds, labs, n_bins=2,
        id_cols=("t", "u", "item"), item_col="item",
        score_col="sc", label_col="y")
    assert len(fig.data) > 0

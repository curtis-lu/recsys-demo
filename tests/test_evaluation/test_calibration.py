"""Tests for evaluation.calibration module."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.calibration import plot_calibration_curves


def _make_data(n_customers=100, products=None, seed=42):
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["exchange_fx", "fund_bond", "fund_stock"]

    pred_rows = []
    label_rows = []

    for i in range(n_customers):
        for prod in products:
            score = rng.rand()
            label = int(rng.rand() < score)  # roughly calibrated
            pred_rows.append({
                "snap_date": "20240331",
                "cust_id": f"C{i:04d}",
                "prod_name": prod,
                "score": score,
                "rank": 0,
            })
            label_rows.append({
                "snap_date": "20240331",
                "cust_id": f"C{i:04d}",
                "prod_name": prod,
                "label": label,
            })

    return pd.DataFrame(pred_rows), pd.DataFrame(label_rows)


class TestPlotCalibrationCurves:
    def test_returns_figure(self):
        preds, labels = _make_data()
        fig = plot_calibration_curves(preds, labels)
        assert isinstance(fig, go.Figure)

    def test_has_reference_line(self):
        preds, labels = _make_data()
        fig = plot_calibration_curves(preds, labels)
        # First trace should be the diagonal reference
        assert fig.data[0].name == "Perfect Calibration"

    def test_all_products_on_one_figure(self):
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        preds, labels = _make_data(products=products)
        fig = plot_calibration_curves(preds, labels)
        # 1 reference line + 3 product traces
        assert len(fig.data) == 4

    def test_insufficient_data_handled(self):
        """Products with too few samples should be skipped."""
        preds, labels = _make_data(n_customers=2)
        fig = plot_calibration_curves(preds, labels, n_bins=10)
        # Should not raise, some products may be skipped
        assert isinstance(fig, go.Figure)

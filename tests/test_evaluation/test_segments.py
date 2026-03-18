"""Tests for evaluation.segments module."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.segments import (
    compute_segment_metrics,
    load_and_join_segment_sources,
    plot_segment_charts,
)


def _make_data(n_customers=30, products=None, segments=None, seed=42):
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["fx", "bond", "stock"]
    if segments is None:
        segments = ["mass", "affluent", "hnw"]

    pred_rows = []
    label_rows = []
    snap_date = "20240331"

    for i in range(n_customers):
        cust_id = f"C{i:04d}"
        seg = segments[i % len(segments)]
        scores = rng.rand(len(products))

        for j, prod in enumerate(products):
            pred_rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_code": prod,
                "score": scores[j],
                "rank": 0,
            })
            label_rows.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "label": int(rng.rand() > 0.6),
                "cust_segment_typ": seg,
            })

    preds = pd.DataFrame(pred_rows)
    preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)

    labels = pd.DataFrame(label_rows)
    return preds, labels


class TestLoadAndJoinSegmentSources:
    def test_join_single_source(self, tmp_path):
        _, labels = _make_data()
        # Create external segment parquet
        seg_df = labels[["cust_id", "snap_date"]].drop_duplicates().copy()
        seg_df["holding_combo"] = [f"combo_{i % 3}" for i in range(len(seg_df))]
        seg_path = tmp_path / "holding_combo.parquet"
        seg_df.to_parquet(seg_path)

        segment_sources = {
            "holding_combo": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "holding_combo" in result.columns
        assert result["holding_combo"].notna().all()

    def test_missing_file_skipped(self, tmp_path):
        _, labels = _make_data()
        segment_sources = {
            "missing_seg": {
                "filepath": str(tmp_path / "nonexistent.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "missing_col",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        # Should return labels unchanged (no new column)
        assert "missing_col" not in result.columns

    def test_partial_join_coverage(self, tmp_path):
        _, labels = _make_data(n_customers=30)
        # Only provide segment for half the customers
        unique_custs = labels[["cust_id", "snap_date"]].drop_duplicates()
        half = unique_custs.iloc[:15].copy()
        half["risk_level"] = "high"
        seg_path = tmp_path / "risk.parquet"
        half.to_parquet(seg_path)

        segment_sources = {
            "risk": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "risk_level",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "risk_level" in result.columns
        assert result["risk_level"].notna().sum() > 0
        assert result["risk_level"].isna().sum() > 0

    def test_multiple_sources(self, tmp_path):
        _, labels = _make_data()
        unique_custs = labels[["cust_id", "snap_date"]].drop_duplicates()

        # Source 1
        s1 = unique_custs.copy()
        s1["holding_combo"] = "combo_a"
        s1.to_parquet(tmp_path / "combo.parquet")

        # Source 2
        s2 = unique_custs.copy()
        s2["risk_level"] = "medium"
        s2.to_parquet(tmp_path / "risk.parquet")

        segment_sources = {
            "holding_combo": {
                "filepath": str(tmp_path / "combo.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            },
            "risk_level": {
                "filepath": str(tmp_path / "risk.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "risk_level",
            },
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "holding_combo" in result.columns
        assert "risk_level" in result.columns


class TestComputeSegmentMetrics:
    def test_returns_all_segments(self):
        preds, labels = _make_data()
        result = compute_segment_metrics(preds, labels, k_values=[3])
        assert set(result.keys()) == {"mass", "affluent", "hnw"}

    def test_each_segment_has_metrics(self):
        preds, labels = _make_data()
        result = compute_segment_metrics(preds, labels, k_values=[3])
        for seg, metrics in result.items():
            assert "overall" in metrics
            assert "map" in metrics["overall"]


class TestHoldingComboAsSegment:
    def test_holding_combo_via_compute_segment_metrics(self, tmp_path):
        """Holding combo loaded as external segment works with compute_segment_metrics."""
        preds, labels = _make_data()
        # Create external holding_combo segment
        unique_custs = labels[["cust_id", "snap_date"]].drop_duplicates()
        unique_custs["holding_combo"] = [
            f"combo_{i % 3}" for i in range(len(unique_custs))
        ]
        seg_path = tmp_path / "holding_combo.parquet"
        unique_custs.to_parquet(seg_path)

        segment_sources = {
            "holding_combo": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            }
        }
        labels_enriched = load_and_join_segment_sources(labels, segment_sources)
        result = compute_segment_metrics(
            preds, labels_enriched, segment_column="holding_combo", k_values=[3]
        )
        assert isinstance(result, dict)
        assert len(result) == 3  # combo_0, combo_1, combo_2
        for seg_metrics in result.values():
            assert "overall" in seg_metrics
            assert "map" in seg_metrics["overall"]

    def test_holding_combo_plots(self, tmp_path):
        """plot_segment_charts works with holding combo segment metrics."""
        preds, labels = _make_data()
        unique_custs = labels[["cust_id", "snap_date"]].drop_duplicates()
        unique_custs["holding_combo"] = [
            f"combo_{i % 2}" for i in range(len(unique_custs))
        ]
        seg_path = tmp_path / "holding_combo.parquet"
        unique_custs.to_parquet(seg_path)

        segment_sources = {
            "holding_combo": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            }
        }
        labels_enriched = load_and_join_segment_sources(labels, segment_sources)
        seg_metrics = compute_segment_metrics(
            preds, labels_enriched, segment_column="holding_combo", k_values=[3]
        )
        figs = plot_segment_charts(seg_metrics)
        assert len(figs) > 0


class TestPlotSegmentCharts:
    def test_returns_figures(self):
        preds, labels = _make_data()
        seg_metrics = compute_segment_metrics(preds, labels, k_values=[3])
        figs = plot_segment_charts(seg_metrics)
        assert len(figs) > 0
        assert all(isinstance(f, go.Figure) for f in figs)



"""End-to-end tests for baselines pipeline pandas backend."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.baselines.nodes_pandas import (
    compute_baseline_metrics,
    compute_baselines,
)

PRODUCTS = ["prod_a", "prod_b", "prod_c"]
CUSTOMERS = [f"C{i:04d}" for i in range(1, 21)]
SNAP_DATE = "20240131"
HIST_DATES = ["20231031", "20231130", "20231231"]


def _make_parameters(baseline_type="global_popularity"):
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "evaluation": {
            "snap_date": SNAP_DATE,
            "k_values": [3, "all"],
            "segment_columns": ["cust_segment_typ"],
            "segment_sources": {},
            "baseline": {
                "type": baseline_type,
                "segment_column": "cust_segment_typ",
            },
        },
    }


@pytest.fixture
def label_table():
    rng = np.random.default_rng(42)
    rows = []
    all_dates = HIST_DATES + [SNAP_DATE]
    for snap_date in all_dates:
        for cust_id in CUSTOMERS:
            segment = rng.choice(["mass", "affluent", "hnw"])
            for prod in PRODUCTS:
                rows.append({
                    "snap_date": snap_date,
                    "cust_id": cust_id,
                    "prod_name": prod,
                    "label": int(rng.random() < 0.2),
                    "cust_segment_typ": segment,
                })
    return pd.DataFrame(rows)


class TestComputeBaselines:
    def test_global_popularity_output_shape(self, label_table):
        params = _make_parameters("global_popularity")
        result = compute_baselines(label_table, params)

        # Should have one row per (customer, product) at snap_date
        assert len(result) == len(CUSTOMERS) * len(PRODUCTS)

    def test_global_popularity_columns(self, label_table):
        params = _make_parameters("global_popularity")
        result = compute_baselines(label_table, params)

        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns
        assert "score" in result.columns
        assert "rank" in result.columns

    def test_global_popularity_ranks_valid(self, label_table):
        params = _make_parameters("global_popularity")
        result = compute_baselines(label_table, params)

        # Each customer should have ranks 1..N
        for cust_id in CUSTOMERS:
            cust_data = result[result["cust_id"] == cust_id]
            ranks = sorted(cust_data["rank"].tolist())
            assert ranks == list(range(1, len(PRODUCTS) + 1))

    def test_segment_popularity_output_shape(self, label_table):
        params = _make_parameters("segment_popularity")
        result = compute_baselines(label_table, params)
        assert len(result) == len(CUSTOMERS) * len(PRODUCTS)

    def test_scores_are_non_negative(self, label_table):
        params = _make_parameters("global_popularity")
        result = compute_baselines(label_table, params)
        assert (result["score"] >= 0).all()


class TestComputeBaselineMetrics:
    def test_returns_metrics_dict(self, label_table):
        params = _make_parameters("global_popularity")
        baseline = compute_baselines(label_table, params)
        metrics = compute_baseline_metrics(baseline, label_table, params)

        assert "overall" in metrics
        assert "per_product" in metrics
        assert "n_queries" in metrics

    def test_metric_values_in_range(self, label_table):
        params = _make_parameters("global_popularity")
        baseline = compute_baselines(label_table, params)
        metrics = compute_baseline_metrics(baseline, label_table, params)

        for name, value in metrics["overall"].items():
            assert 0.0 <= value <= 1.0, f"{name}={value} out of range"


class TestEndToEnd:
    def test_full_baselines_flow(self, label_table):
        params = _make_parameters("global_popularity")
        baseline = compute_baselines(label_table, params)
        metrics = compute_baseline_metrics(baseline, label_table, params)

        assert len(baseline) > 0
        assert metrics["n_queries"] > 0
        assert len(metrics["overall"]) > 0

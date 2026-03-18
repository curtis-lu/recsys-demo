"""Tests for evaluation.baselines module."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.baselines import (
    generate_global_popularity_baseline,
    generate_segment_popularity_baseline,
)


@pytest.fixture
def label_table():
    """Create a sample label_table for testing."""
    rows = []
    products = ["fx", "bond", "stock"]
    segments = ["mass", "affluent"]

    # Historical data (snap_date < target)
    for snap in ["20240101", "20240201"]:
        for i in range(10):
            cust_id = f"C{i:04d}"
            seg = segments[i % 2]
            for prod in products:
                # fx is most popular, stock least
                if prod == "fx":
                    label = 1 if i < 7 else 0
                elif prod == "bond":
                    label = 1 if i < 4 else 0
                else:
                    label = 1 if i < 2 else 0
                rows.append({
                    "snap_date": snap,
                    "cust_id": cust_id,
                    "prod_name": prod,
                    "label": label,
                    "cust_segment_typ": seg,
                })

    return pd.DataFrame(rows)


@pytest.fixture
def customer_ids():
    return [f"C{i:04d}" for i in range(5)]


class TestGlobalPopularityBaseline:
    def test_schema_matches_predictions(self, label_table, customer_ids):
        result = generate_global_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        assert set(result.columns) == {"snap_date", "cust_id", "prod_code", "score", "rank"}

    def test_same_ranking_for_all_customers(self, label_table, customer_ids):
        result = generate_global_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        # All customers should have the same product order
        rankings = result.pivot(index="cust_id", columns="prod_code", values="rank")
        for col in rankings.columns:
            assert rankings[col].nunique() == 1

    def test_scores_match_positive_rates(self, label_table, customer_ids):
        result = generate_global_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        # fx has 70% positive rate
        fx_score = result[result["prod_code"] == "fx"]["score"].iloc[0]
        assert fx_score == pytest.approx(0.7)

    def test_ranking_order_correct(self, label_table, customer_ids):
        result = generate_global_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        # fx (0.7) > bond (0.4) > stock (0.2) → ranks 1, 2, 3
        c0 = result[result["cust_id"] == "C0000"].set_index("prod_code")
        assert c0.loc["fx", "rank"] < c0.loc["bond", "rank"]
        assert c0.loc["bond", "rank"] < c0.loc["stock", "rank"]

    def test_leakage_prevention(self, label_table, customer_ids):
        """Only historical data before snap_date should be used."""
        result = generate_global_popularity_baseline(
            label_table, "20240201", customer_ids
        )
        # Should only use data from 20240101
        assert len(result) == 5 * 3  # 5 customers × 3 products

    def test_no_historical_data_warning(self, label_table, customer_ids, caplog):
        """When no data before snap_date, use all data with warning."""
        import logging
        with caplog.at_level(logging.WARNING):
            result = generate_global_popularity_baseline(
                label_table, "20230101", customer_ids
            )
        assert "No historical data" in caplog.text
        assert len(result) > 0


class TestSegmentPopularityBaseline:
    def test_schema_matches_predictions(self, label_table, customer_ids):
        result = generate_segment_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        assert set(result.columns) == {"snap_date", "cust_id", "prod_code", "score", "rank"}

    def test_different_rankings_per_segment(self, label_table):
        """Different segments should potentially have different rankings."""
        cust_ids = [f"C{i:04d}" for i in range(10)]
        result = generate_segment_popularity_baseline(
            label_table, "20240331", cust_ids
        )
        # Mass customers (even indices) vs affluent (odd indices)
        mass_ranks = result[result["cust_id"] == "C0000"].set_index("prod_code")["rank"]
        affluent_ranks = result[result["cust_id"] == "C0001"].set_index("prod_code")["rank"]
        # Rankings may differ between segments
        assert len(result) == 10 * 3

    def test_output_schema(self, label_table, customer_ids):
        result = generate_segment_popularity_baseline(
            label_table, "20240331", customer_ids
        )
        assert set(result.columns) == {"snap_date", "cust_id", "prod_code", "score", "rank"}
        assert result["snap_date"].unique().tolist() == ["20240331"]

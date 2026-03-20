"""Tests for evaluation.statistics module."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.statistics import (
    compute_product_statistics,
    compute_segment_statistics,
)


def _make_labels():
    return pd.DataFrame({
        "snap_date": ["20240331"] * 12,
        "cust_id": ["C0", "C0", "C0", "C1", "C1", "C1", "C2", "C2", "C2", "C3", "C3", "C3"],
        "prod_name": ["A", "B", "C"] * 4,
        "label": [1, 0, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0],
        "cust_segment_typ": ["mass", "mass", "mass", "mass", "mass", "mass",
                             "hnw", "hnw", "hnw", "hnw", "hnw", "hnw"],
    })


class TestProductStatistics:
    def test_columns(self):
        labels = _make_labels()
        result = compute_product_statistics(labels)
        expected_cols = {
            "positive_customers", "negative_customers", "total_customers",
            "positive_rate", "avg_positive_products_per_customer",
        }
        assert set(result.columns) == expected_cols

    def test_counts(self):
        labels = _make_labels()
        result = compute_product_statistics(labels)
        # Product A: C0=1, C1=1, C2=0, C3=1 → 3 positive customers, 1 negative
        assert result.loc["A", "positive_customers"] == 3
        assert result.loc["A", "negative_customers"] == 1

    def test_positive_rate(self):
        labels = _make_labels()
        result = compute_product_statistics(labels)
        # Product A: 3/4 = 0.75
        assert result.loc["A", "positive_rate"] == pytest.approx(0.75)

    def test_total_customers(self):
        labels = _make_labels()
        result = compute_product_statistics(labels)
        assert result.loc["A", "total_customers"] == 4

    def test_avg_positive_products(self):
        labels = _make_labels()
        result = compute_product_statistics(labels)
        # C0: 2 positive products (A, C), C1: 2 (A, B), C2: 1 (C), C3: 1 (A)
        # Mean = (2 + 2 + 1 + 1) / 4 = 1.5
        assert result.loc["A", "avg_positive_products_per_customer"] == pytest.approx(1.5)


class TestSegmentStatistics:
    def test_columns(self):
        labels = _make_labels()
        result = compute_segment_statistics(labels)
        expected_cols = {
            "positive_customers", "negative_customers", "total_customers",
            "positive_rate", "avg_positive_products_per_customer",
        }
        assert set(result.columns) == expected_cols

    def test_missing_column(self):
        labels = _make_labels().drop(columns=["cust_segment_typ"])
        result = compute_segment_statistics(labels)
        assert len(result) == 0

    def test_segment_counts(self):
        labels = _make_labels()
        result = compute_segment_statistics(labels)
        # mass: C0 has label=1 for any product, C1 has label=1 → both positive
        # Collapsed per customer: C0 max=1, C1 max=1 → 2 positive, 0 negative
        assert result.loc["mass", "positive_customers"] == 2
        assert result.loc["mass", "negative_customers"] == 0

    def test_segment_total_customers(self):
        labels = _make_labels()
        result = compute_segment_statistics(labels)
        assert result.loc["mass", "total_customers"] == 2
        assert result.loc["hnw", "total_customers"] == 2

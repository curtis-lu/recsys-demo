"""Tests for evaluation.metrics module."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.metrics import (
    _resolve_k_values,
    compute_all_metrics,
    compute_ap,
    compute_ap_at_k,
    compute_mrr,
    compute_mrr_at_k,
    compute_ndcg,
    compute_precision_at_k,
    compute_recall_at_k,
)


# ---------------------------------------------------------------------------
# Single-query metric functions
# ---------------------------------------------------------------------------


class TestComputeAP:
    def test_known_values(self):
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        ap = compute_ap(y_true, y_score)
        # Precision at pos 1: 1/1=1.0, pos 3: 2/3
        # AP = (1.0 + 2/3) / 2 = 5/6
        assert ap == pytest.approx(5 / 6)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) is None

    def test_all_positives(self):
        y_true = np.array([1, 1, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) == pytest.approx(1.0)

    def test_worst_case(self):
        y_true = np.array([0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        # Only positive at rank 3: precision@3 = 1/3
        assert compute_ap(y_true, y_score) == pytest.approx(1 / 3)


class TestComputeNDCG:
    def test_perfect_ranking(self):
        y_true = np.array([1, 1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.3, 0.1])
        assert compute_ndcg(y_true, y_score) == pytest.approx(1.0)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ndcg(y_true, y_score) == 0.0

    def test_at_k(self):
        y_true = np.array([1, 0, 1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        ndcg_full = compute_ndcg(y_true, y_score)
        ndcg_at3 = compute_ndcg(y_true, y_score, k=3)
        # Both should be valid floats
        assert 0 < ndcg_at3 <= 1.0
        assert 0 < ndcg_full <= 1.0

    def test_inverse_ranking(self):
        y_true = np.array([0, 0, 1, 1])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        # Positives at bottom, should be less than 1.0
        assert compute_ndcg(y_true, y_score) < 1.0


class TestComputePrecisionAtK:
    def test_known_values(self):
        y_true = np.array([1, 0, 1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        # Top 3: [1, 0, 1] → precision = 2/3
        assert compute_precision_at_k(y_true, y_score, k=3) == pytest.approx(2 / 3)

    def test_k_equals_1(self):
        y_true = np.array([1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_precision_at_k(y_true, y_score, k=1) == pytest.approx(1.0)

    def test_no_positives_in_top_k(self):
        y_true = np.array([0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_precision_at_k(y_true, y_score, k=2) == pytest.approx(0.0)


class TestComputeRecallAtK:
    def test_all_positives_in_top_k(self):
        y_true = np.array([1, 0, 1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        # Top 3: [1, 0, 1], total positives = 2 → recall = 2/2
        assert compute_recall_at_k(y_true, y_score, k=3) == pytest.approx(1.0)

    def test_partial_recall(self):
        y_true = np.array([1, 0, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        # Top 3: [1, 0, 0], total positives = 2 → recall = 1/2
        assert compute_recall_at_k(y_true, y_score, k=3) == pytest.approx(0.5)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_recall_at_k(y_true, y_score, k=2) == 0.0


class TestComputeMRR:
    def test_first_positive_at_rank_3(self):
        y_true = np.array([0, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mrr(y_true, y_score) == pytest.approx(1 / 3)

    def test_first_positive_at_rank_1(self):
        y_true = np.array([1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_mrr(y_true, y_score) == pytest.approx(1.0)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_mrr(y_true, y_score) == 0.0


class TestComputeAPAtK:
    def test_known_values(self):
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        # Top 2: [1, 0], AP@2 = (1/1 * 1) / 2 = 0.5
        assert compute_ap_at_k(y_true, y_score, k=2) == pytest.approx(0.5)

    def test_full_k_matches_ap(self):
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_ap_at_k(y_true, y_score, k=4) == pytest.approx(
            compute_ap(y_true, y_score)
        )

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap_at_k(y_true, y_score, k=2) is None

    def test_no_positives_in_top_k(self):
        y_true = np.array([0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap_at_k(y_true, y_score, k=2) == pytest.approx(0.0)


class TestComputeMRRAtK:
    def test_first_positive_within_k(self):
        y_true = np.array([0, 1, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mrr_at_k(y_true, y_score, k=3) == pytest.approx(0.5)

    def test_first_positive_beyond_k(self):
        y_true = np.array([0, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mrr_at_k(y_true, y_score, k=2) == pytest.approx(0.0)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_mrr_at_k(y_true, y_score, k=2) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# compute_all_metrics
# ---------------------------------------------------------------------------


def _make_test_data(n_customers=10, products=None, segments=None, seed=42):
    """Create synthetic predictions and labels for testing."""
    rng = np.random.RandomState(seed)
    if products is None:
        products = ["exchange_fx", "fund_bond", "fund_stock"]
    if segments is None:
        segments = ["mass", "affluent", "hnw"]

    snap_date = "20240331"
    rows_pred = []
    rows_label = []

    for i in range(n_customers):
        cust_id = f"C{i:04d}"
        seg = segments[i % len(segments)]
        scores = rng.rand(len(products))

        for j, prod in enumerate(products):
            rows_pred.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "score": scores[j],
                "rank": 0,  # will be set below
            })
            rows_label.append({
                "snap_date": snap_date,
                "cust_id": cust_id,
                "prod_name": prod,
                "label": int(rng.rand() > 0.7),
                "cust_segment_typ": seg,
            })

    predictions = pd.DataFrame(rows_pred)
    # Compute ranks per customer
    predictions["rank"] = predictions.groupby(["snap_date", "cust_id"])["score"].rank(
        method="first", ascending=False
    ).astype(int)

    labels = pd.DataFrame(rows_label)
    return predictions, labels


class TestComputeAllMetrics:
    def test_overall_metrics_present(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        overall = result["overall"]
        assert "map@3" in overall
        assert "ndcg@3" in overall
        assert "precision@3" in overall
        assert "recall@3" in overall
        assert "mrr@3" in overall
        # No non-@K keys
        assert "map" not in overall
        assert "ndcg" not in overall
        assert "mrr" not in overall

    def test_per_product_metrics(self):
        predictions, labels = _make_test_data(products=["exchange_fx", "fund_bond", "fund_stock"])
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert set(result["per_product"].keys()) == {"exchange_fx", "fund_bond", "fund_stock"}
        for prod_metrics in result["per_product"].values():
            assert "map@3" in prod_metrics

    def test_per_product_metrics_not_all_one(self):
        """Per-product metrics should NOT all be 1.0 (the old bug)."""
        predictions, labels = _make_test_data(n_customers=20, seed=123)
        result = compute_all_metrics(predictions, labels, k_values=[3])
        per_product = result["per_product"]
        all_map_values = [m["map@3"] for m in per_product.values()]
        # With random data, it's extremely unlikely all products get perfect mAP
        assert not all(v == pytest.approx(1.0) for v in all_map_values)

    def test_per_product_map_known_values(self):
        """Verify per-product mAP with hand-computed values.

        Setup: 2 customers, 3 products.
        Customer C0: scores [0.9, 0.5, 0.1] for [A, B, C] → ranking: A(1), B(2), C(3)
                     labels [1,   0,   1]
          - Product A: label=1, rank=1 → precision=1/1=1.0
          - Product C: label=1, rank=3 → precision=2/3
        Customer C1: scores [0.3, 0.8, 0.6] for [A, B, C] → ranking: B(1), C(2), A(3)
                     labels [0,   1,   0]
          - Product B: label=1, rank=1 → precision=1/1=1.0

        Per-product mAP:
          A: mean of precisions for A where label=1 → only C0: 1.0 → mAP(A) = 1.0
          B: mean of precisions for B where label=1 → only C1: 1.0 → mAP(B) = 1.0
          C: mean of precisions for C where label=1 → only C0: 2/3 → mAP(C) = 2/3
        """
        predictions = pd.DataFrame({
            "snap_date": ["20240331"] * 6,
            "cust_id": ["C0", "C0", "C0", "C1", "C1", "C1"],
            "prod_name": ["A", "B", "C", "A", "B", "C"],
            "score": [0.9, 0.5, 0.1, 0.3, 0.8, 0.6],
            "rank": [1, 2, 3, 3, 1, 2],
        })
        labels = pd.DataFrame({
            "snap_date": ["20240331"] * 6,
            "cust_id": ["C0", "C0", "C0", "C1", "C1", "C1"],
            "prod_name": ["A", "B", "C", "A", "B", "C"],
            "label": [1, 0, 1, 0, 1, 0],
        })
        result = compute_all_metrics(predictions, labels, k_values=[3])
        per_product = result["per_product"]
        assert per_product["A"]["map@3"] == pytest.approx(1.0)
        assert per_product["B"]["map@3"] == pytest.approx(1.0)
        assert per_product["C"]["map@3"] == pytest.approx(2 / 3)

    def test_per_segment_metrics(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert "per_segment" in result
        # Should have segments since labels have cust_segment_typ
        assert len(result["per_segment"]) > 0

    def test_per_segment_equal_customer_weight(self):
        """Per-segment metrics should use equal customer weighting (mean of per-customer)."""
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        # Per-segment values should exist and be reasonable
        for seg, seg_metrics in result["per_segment"].items():
            assert 0 <= seg_metrics["map@3"] <= 1.0
            assert 0 <= seg_metrics["ndcg@3"] <= 1.0

    def test_per_product_segment_metrics(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert "per_product_segment" in result
        # Check keys are in format "product_segment"
        for key in result["per_product_segment"]:
            assert "_" in key

    def test_macro_micro_avg_by_product(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert "by_product" in result["macro_avg"]
        assert "by_product" in result["micro_avg"]
        assert "map@3" in result["macro_avg"]["by_product"]

    def test_macro_micro_avg_by_segment(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert "by_segment" in result["macro_avg"]
        assert "by_segment" in result["micro_avg"]

    def test_n_queries_tracked(self):
        predictions, labels = _make_test_data(n_customers=10)
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert result["n_queries"] == 10
        assert isinstance(result["n_excluded_queries"], int)

    def test_no_segment_column(self):
        predictions, labels = _make_test_data()
        labels_no_seg = labels.drop(columns=["cust_segment_typ"])
        result = compute_all_metrics(predictions, labels_no_seg, k_values=[3])
        assert result["per_segment"] == {}
        assert "by_segment" not in result["macro_avg"]

    def test_default_k_values(self):
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels)
        overall = result["overall"]
        # Default k_values is [5, "all"]; with 3 products, "all" resolves to 3
        assert "precision@3" in overall
        assert "precision@5" in overall
        assert "recall@3" in overall
        assert "recall@5" in overall

    def test_excluded_queries(self):
        """Queries with no positives should be excluded from AP/nDCG."""
        predictions, labels = _make_test_data(n_customers=5, seed=0)
        # Force all labels to 0 for first customer
        labels.loc[labels["cust_id"] == "C0000", "label"] = 0
        result = compute_all_metrics(predictions, labels, k_values=[3])
        assert result["n_excluded_queries"] >= 1

    def test_return_dict_structure(self):
        """Verify the return dict has all expected keys."""
        predictions, labels = _make_test_data()
        result = compute_all_metrics(predictions, labels, k_values=[3])
        expected_keys = {
            "overall", "per_product", "per_segment", "per_product_segment",
            "macro_avg", "micro_avg", "n_queries", "n_excluded_queries",
        }
        assert set(result.keys()) == expected_keys


# ---------------------------------------------------------------------------
# _resolve_k_values
# ---------------------------------------------------------------------------


class TestResolveKValues:
    def test_all_resolves_to_n_products(self):
        result = _resolve_k_values([5, "all"], n_products=22)
        assert result == [5, 22]

    def test_all_only(self):
        result = _resolve_k_values(["all"], n_products=10)
        assert result == [10]

    def test_integers_only(self):
        result = _resolve_k_values([3, 10], n_products=22)
        assert result == [3, 10]

    def test_dedup_and_sort(self):
        # If "all" equals an existing value, deduplicate
        result = _resolve_k_values([5, "all"], n_products=5)
        assert result == [5]

    def test_mixed_types(self):
        result = _resolve_k_values([3, "all", 5], n_products=22)
        assert result == [3, 5, 22]


class TestComputeAllMetricsKValuesAll:
    def test_all_resolves_to_product_count(self):
        products = ["exchange_fx", "fund_bond", "fund_stock", "ccard_ins", "ccard_bill"]
        predictions, labels = _make_test_data(products=products)
        result = compute_all_metrics(predictions, labels, k_values=[5, "all"])
        overall = result["overall"]
        # "all" should resolve to 5 (number of products)
        # Since 5 == "all", only @5 keys should exist (deduped)
        assert "precision@5" in overall
        assert "recall@5" in overall
        assert "ndcg@5" in overall

    def test_all_with_different_n(self):
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        predictions, labels = _make_test_data(products=products)
        result = compute_all_metrics(predictions, labels, k_values=[5, "all"])
        overall = result["overall"]
        # "all" resolves to 3, so we get both @3 and @5
        assert "precision@3" in overall
        assert "precision@5" in overall
        assert "ndcg@3" in overall
        assert "ndcg@5" in overall

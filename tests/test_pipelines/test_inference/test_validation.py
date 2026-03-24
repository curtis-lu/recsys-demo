"""Tests for inference pipeline validation (sanity checks)."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.inference.nodes_pandas import validate_predictions
from recsys_tfb.pipelines.inference.validation import ValidationError


@pytest.fixture
def parameters():
    return {
        "inference": {
            "snap_dates": ["2024-03-31"],
            "products": ["exchange_fx", "fund_stock", "fund_bond"],
        },
    }


def _make_valid_data(n_customers=3):
    """Build a valid ranked_predictions and matching scoring_dataset."""
    products = ["exchange_fx", "fund_stock", "fund_bond"]
    rows = []
    for i in range(n_customers):
        cid = f"C{i+1:03d}"
        scores = sorted(np.random.rand(3), reverse=True)
        for rank, (prod, score) in enumerate(zip(products, scores), 1):
            rows.append({
                "snap_date": pd.Timestamp("2024-03-31"),
                "cust_id": cid,
                "prod_name": prod,
                "score": score,
                "rank": rank,
            })
    ranked = pd.DataFrame(rows)
    # scoring_dataset just needs matching row count
    scoring = ranked[["snap_date", "cust_id", "prod_name"]].copy()
    scoring["total_aum"] = 100.0
    return ranked, scoring


class TestValidatePredicationsPass:
    def test_valid_data_passes(self, parameters):
        ranked, scoring = _make_valid_data()
        result = validate_predictions(ranked, scoring, parameters)
        assert result is ranked

    def test_valid_data_multiple_customers(self, parameters):
        ranked, scoring = _make_valid_data(n_customers=10)
        result = validate_predictions(ranked, scoring, parameters)
        assert len(result) == 30


class TestRowCountMatch:
    def test_mismatch_raises(self, parameters):
        ranked, scoring = _make_valid_data()
        # Remove one row from scoring
        scoring_short = scoring.iloc[:-1]
        with pytest.raises(ValidationError, match="row_count_match"):
            validate_predictions(ranked, scoring_short, parameters)


class TestScoreRange:
    def test_score_below_zero(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "score"] = -0.1
        # Fix rank consistency after score change
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, scoring, parameters)

    def test_score_above_one(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "score"] = 1.5
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, scoring, parameters)


class TestNoMissing:
    def test_nan_in_score(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "score"] = np.nan
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, scoring, parameters)

    def test_nan_in_identity(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "cust_id"] = None
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, scoring, parameters)


class TestCompleteness:
    def test_missing_product(self, parameters):
        ranked, scoring = _make_valid_data()
        # Remove one product from first customer
        ranked = ranked.drop(index=0).reset_index(drop=True)
        scoring = scoring.drop(index=0).reset_index(drop=True)
        with pytest.raises(ValidationError, match="completeness"):
            validate_predictions(ranked, scoring, parameters)


class TestRankConsistency:
    def test_non_sequential_ranks(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "rank"] = 5  # Invalid rank
        with pytest.raises(ValidationError, match="rank_consistency"):
            validate_predictions(ranked, scoring, parameters)

    def test_score_order_mismatch(self, parameters):
        ranked, scoring = _make_valid_data()
        # Swap scores but keep ranks — rank 1 should have highest score
        group = ranked[ranked["cust_id"] == "C001"]
        idx_rank1 = group[group["rank"] == 1].index[0]
        idx_rank3 = group[group["rank"] == 3].index[0]
        ranked.loc[idx_rank1, "score"], ranked.loc[idx_rank3, "score"] = (
            ranked.loc[idx_rank3, "score"],
            ranked.loc[idx_rank1, "score"],
        )
        with pytest.raises(ValidationError, match="rank_consistency"):
            validate_predictions(ranked, scoring, parameters)


class TestNoDuplicates:
    def test_duplicate_rows(self, parameters):
        ranked, scoring = _make_valid_data()
        # Duplicate a row
        dup_row = ranked.iloc[[0]]
        ranked = pd.concat([ranked, dup_row], ignore_index=True)
        scoring = pd.concat([scoring, scoring.iloc[[0]]], ignore_index=True)
        with pytest.raises(ValidationError, match="no_duplicates"):
            validate_predictions(ranked, scoring, parameters)


class TestMultipleFailures:
    def test_multiple_checks_fail(self, parameters):
        ranked, scoring = _make_valid_data()
        ranked.loc[0, "score"] = -0.5  # score_range fail
        ranked.loc[1, "score"] = np.nan  # no_missing fail
        with pytest.raises(ValidationError) as exc_info:
            validate_predictions(ranked, scoring, parameters)
        assert len(exc_info.value.failures) >= 2


def _rerank(df: pd.DataFrame) -> pd.DataFrame:
    """Re-rank by score descending within each group."""
    df = df.copy()
    df["rank"] = (
        df.groupby(["snap_date", "cust_id"])["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return df

"""Tests for inference pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.inference.nodes_pandas import (
    apply_preprocessor,
    build_scoring_dataset,
    predict_scores,
    rank_predictions,
)


@pytest.fixture
def feature_table():
    return pd.DataFrame(
        {
            "snap_date": pd.to_datetime(
                ["2024-01-31"] * 3 + ["2024-03-31"] * 3
            ),
            "cust_id": ["C001", "C002", "C003"] * 2,
            "total_aum": [100.0, 200.0, 300.0] * 2,
            "fund_aum": [10.0, 20.0, 30.0] * 2,
            "in_amt_sum_l1m": [5.0] * 6,
            "out_amt_sum_l1m": [3.0] * 6,
            "in_amt_ratio_l1m": [0.05] * 6,
            "out_amt_ratio_l1m": [0.03] * 6,
        }
    )


@pytest.fixture
def parameters():
    return {
        "inference": {
            "snap_dates": ["2024-03-31"],
            "products": ["exchange_fx", "fund_stock", "fund_bond"],
        },
    }


@pytest.fixture
def preprocessor():
    return {
        "drop_columns": [
            "snap_date", "cust_id", "label",
            "apply_start_date", "apply_end_date", "cust_segment_typ",
        ],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund_bond", "exchange_fx", "fund_stock"]},
        "feature_columns": [
            "prod_name", "total_aum", "fund_aum",
            "in_amt_sum_l1m", "out_amt_sum_l1m",
            "in_amt_ratio_l1m", "out_amt_ratio_l1m",
        ],
    }


class TestBuildScoringDataset:
    def test_cross_join_shape(self, feature_table, parameters):
        result = build_scoring_dataset(feature_table, parameters)
        # 3 customers x 3 products = 9 rows (only snap_date 2024-03-31)
        assert len(result) == 9

    def test_columns_present(self, feature_table, parameters):
        result = build_scoring_dataset(feature_table, parameters)
        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns
        assert "total_aum" in result.columns

    def test_all_products_per_customer(self, feature_table, parameters):
        result = build_scoring_dataset(feature_table, parameters)
        products = parameters["inference"]["products"]
        for cid in ["C001", "C002", "C003"]:
            cust_prods = result.loc[result["cust_id"] == cid, "prod_name"].tolist()
            assert sorted(cust_prods) == sorted(products)

    def test_multiple_snap_dates(self, feature_table, parameters):
        params = {
            "inference": {
                "snap_dates": ["2024-01-31", "2024-03-31"],
                "products": ["exchange_fx", "fund_stock"],
            },
        }
        result = build_scoring_dataset(feature_table, params)
        # 3 customers x 2 products x 2 snap_dates = 12
        assert len(result) == 12


class TestApplyPreprocessor:
    def test_output_columns_match_training(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor)
        assert list(result.columns) == preprocessor["feature_columns"]

    def test_prod_name_encoded(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor)
        assert result["prod_name"].dtype in [np.int8, np.int16, np.int32, np.int64]

    def test_drops_non_feature_columns(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor)
        forbidden = {"snap_date", "cust_id", "label"}
        assert forbidden.isdisjoint(set(result.columns))

    def test_missing_feature_raises(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        preprocessor["feature_columns"] = preprocessor["feature_columns"] + ["nonexistent_col"]
        with pytest.raises(ValueError, match="Missing feature columns"):
            apply_preprocessor(scoring, preprocessor)

    def test_label_column_ignored(self, feature_table, parameters, preprocessor):
        """Inference data has no label column; drop_columns should use errors='ignore'."""
        scoring = build_scoring_dataset(feature_table, parameters)
        assert "label" not in scoring.columns
        # Should not raise
        result = apply_preprocessor(scoring, preprocessor)
        assert len(result) == len(scoring)


class TestPredictScores:
    def test_output_columns(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor)

        # Create a mock booster-like object
        class MockModel:
            def predict(self, X):
                return np.random.rand(len(X))

        result = predict_scores(MockModel(), X_score, scoring)
        assert set(result.columns) == {"snap_date", "cust_id", "prod_name", "score"}

    def test_row_count_matches(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring)
        assert len(result) == len(scoring)

    def test_prod_name_from_prod_name(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring)
        assert set(result["prod_name"].unique()) == set(parameters["inference"]["products"])


class TestRankPredictions:
    def test_rank_column_added(self, parameters):
        score_table = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        result = rank_predictions(score_table, parameters)
        assert "rank" in result.columns

    def test_rank_order(self, parameters):
        score_table = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        result = rank_predictions(score_table, parameters)
        # exchange_fx has highest score -> rank 1
        fx_rank = result.loc[result["prod_name"] == "exchange_fx", "rank"].iloc[0]
        bond_rank = result.loc[result["prod_name"] == "fund_bond", "rank"].iloc[0]
        stock_rank = result.loc[result["prod_name"] == "fund_stock", "rank"].iloc[0]
        assert fx_rank == 1
        assert bond_rank == 2
        assert stock_rank == 3

    def test_rank_per_group(self, parameters):
        score_table = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3 + ["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"] * 2,
            "score": [0.9, 0.3, 0.6, 0.1, 0.8, 0.5],
        })
        result = rank_predictions(score_table, parameters)
        # Each customer should have ranks 1, 2, 3
        for cid in ["C001", "C002"]:
            cust_ranks = sorted(result.loc[result["cust_id"] == cid, "rank"].tolist())
            assert cust_ranks == [1, 2, 3]

    def test_output_columns(self, parameters):
        score_table = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 2),
            "cust_id": ["C001"] * 2,
            "prod_name": ["exchange_fx", "fund_stock"],
            "score": [0.9, 0.3],
        })
        result = rank_predictions(score_table, parameters)
        assert set(result.columns) == {"snap_date", "cust_id", "prod_name", "score", "rank"}

"""Tests for inference pipeline Spark nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.inference.nodes_spark import (
    apply_preprocessor,
    build_scoring_dataset,
    predict_scores,
    rank_predictions,
)


@pytest.fixture
def feature_table(spark):
    pdf = pd.DataFrame(
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
    return spark.createDataFrame(pdf)


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
        assert result.count() == 9

    def test_columns_present(self, feature_table, parameters):
        result = build_scoring_dataset(feature_table, parameters)
        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns
        assert "total_aum" in result.columns

    def test_all_products_per_customer(self, feature_table, parameters):
        result = build_scoring_dataset(feature_table, parameters)
        pdf = result.toPandas()
        products = parameters["inference"]["products"]
        for cid in ["C001", "C002", "C003"]:
            cust_prods = pdf.loc[pdf["cust_id"] == cid, "prod_name"].tolist()
            assert sorted(cust_prods) == sorted(products)


class TestApplyPreprocessor:
    def test_output_has_identity_and_features(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor, parameters)
        # Should have identity cols + feature cols
        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns  # encoded as int but column name preserved
        for fc in preprocessor["feature_columns"]:
            assert fc in result.columns

    def test_feature_column_order(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor, parameters)
        identity_cols = ["snap_date", "cust_id", "prod_name"]
        expected = identity_cols + preprocessor["feature_columns"]
        assert result.columns == expected

    def test_missing_feature_raises(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        preprocessor["feature_columns"] = preprocessor["feature_columns"] + ["nonexistent_col"]
        with pytest.raises(ValueError, match="Missing feature columns"):
            apply_preprocessor(scoring, preprocessor, parameters)


class TestPredictScores:
    def test_output_is_spark_df(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.random.rand(len(X))

        result = predict_scores(MockModel(), X_score, scoring, parameters)
        from pyspark.sql import DataFrame
        assert isinstance(result, DataFrame)

    def test_output_columns(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring, parameters)
        assert set(result.columns) == {"snap_date", "cust_id", "prod_name", "score"}

    def test_row_count_matches(self, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring, parameters)
        assert result.count() == 9  # 3 customers x 3 products


class TestRankPredictions:
    def test_rank_column_added(self, spark, parameters):
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        score_table = spark.createDataFrame(score_pdf)
        result = rank_predictions(score_table, parameters)
        assert "rank" in result.columns

    def test_rank_order(self, spark, parameters):
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        score_table = spark.createDataFrame(score_pdf)
        result = rank_predictions(score_table, parameters)
        pdf = result.toPandas()
        fx_rank = pdf.loc[pdf["prod_name"] == "exchange_fx", "rank"].iloc[0]
        bond_rank = pdf.loc[pdf["prod_name"] == "fund_bond", "rank"].iloc[0]
        stock_rank = pdf.loc[pdf["prod_name"] == "fund_stock", "rank"].iloc[0]
        assert fx_rank == 1
        assert bond_rank == 2
        assert stock_rank == 3

    def test_rank_per_group(self, spark, parameters):
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3 + ["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"] * 2,
            "score": [0.9, 0.3, 0.6, 0.1, 0.8, 0.5],
        })
        score_table = spark.createDataFrame(score_pdf)
        result = rank_predictions(score_table, parameters)
        pdf = result.toPandas()
        for cid in ["C001", "C002"]:
            cust_ranks = sorted(pdf.loc[pdf["cust_id"] == cid, "rank"].tolist())
            assert cust_ranks == [1, 2, 3]

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

pytestmark = pytest.mark.spark


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

    def test_missing_snap_date_raises(self, feature_table, parameters):
        parameters["inference"]["snap_dates"] = [
            "2024-03-31",
            "2024-04-30",
        ]

        with pytest.raises(
            ValueError, match="feature_table missing inference.snap_dates"
        ):
            build_scoring_dataset(feature_table, parameters)


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

    def test_casts_float_features_to_float32(self, spark, parameters, preprocessor):
        """Decimal AND Double feature columns in scoring data must be cast to
        float (float32). Mirror of build_model_input behaviour — the inference
        scoring parquet is read back via pandas downstream and would otherwise
        face the same memory issues (Decimal → 70 B/value Python objects;
        Double → 8 B vs 4 B float32 for no model benefit since LightGBM
        histogram resolves at log2(max_bin)=8 bits).
        """
        from decimal import Decimal

        from pyspark.sql import types as T

        # Pick a non-categorical numeric feature column to force into DecimalType.
        # feature_columns[0] is "prod_name" (categorical, string in scoring data),
        # so use index 1 ("total_aum") which is a numeric feature.
        feature_cols = preprocessor["feature_columns"]
        categorical_cols = set(preprocessor["categorical_columns"])
        decimal_col = next(c for c in feature_cols if c not in categorical_cols)

        schema = T.StructType([
            T.StructField("snap_date", T.TimestampType()),
            T.StructField("cust_id", T.StringType()),
            T.StructField("prod_name", T.StringType()),
            *[
                T.StructField(
                    c,
                    T.DecimalType(38, 6) if c == decimal_col else T.DoubleType(),
                )
                for c in feature_cols
                if c not in categorical_cols
            ],
        ])
        snap_ts = pd.Timestamp("2024-03-31").to_pydatetime()
        row_values: list = [snap_ts, "C001", "exchange_fx"]
        for c in feature_cols:
            if c in categorical_cols:
                continue
            row_values.append(Decimal("1.5") if c == decimal_col else 0.5)
        scoring = spark.createDataFrame([tuple(row_values)], schema=schema)

        result = apply_preprocessor(scoring, preprocessor, parameters)

        out_dtypes = dict(result.dtypes)
        # No feature column should remain decimal nor double after
        # apply_preprocessor (all float-like → float32).
        assert out_dtypes[decimal_col] == "float", (
            f"{decimal_col} still {out_dtypes[decimal_col]}, expected float"
        )
        leftover = [
            c for c in feature_cols
            if "decimal" in out_dtypes[c] or out_dtypes[c] == "double"
        ]
        assert leftover == [], (
            f"feature_columns still contain decimal/double types: {leftover}"
        )


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

    def test_uses_model_feature_names_for_training_feature_selection(
        self, feature_table, parameters, preprocessor
    ):
        scoring = build_scoring_dataset(feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            seen_columns = []

            def feature_names(self):
                return ["prod_name", "total_aum"]

            def predict(self, X):
                self.seen_columns.append(list(X.columns))
                return np.full(len(X), 0.5)

        model = MockModel()
        result = predict_scores(
            model, X_score, scoring, parameters
        )

        assert result.count() == 9
        assert model.seen_columns
        assert all(
            columns == ["prod_name", "total_aum"]
            for columns in model.seen_columns
        )


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

    def test_filters_persisted_history_to_current_model_and_dates(
        self, spark, parameters
    ):
        parameters["model_version"] = "current"
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(
                ["2024-03-31"] * 3
                + ["2024-01-31"] * 3
                + ["2024-03-31"] * 3
            ),
            "cust_id": ["C001"] * 9,
            "prod_name": [
                "exchange_fx", "fund_stock", "fund_bond",
            ] * 3,
            "score": [0.9, 0.3, 0.6] * 3,
            "model_version": (
                ["current"] * 3 + ["current"] * 3 + ["previous"] * 3
            ),
        })

        result = rank_predictions(spark.createDataFrame(score_pdf), parameters)
        rows = result.select("snap_date", "model_version", "rank").collect()

        assert len(rows) == 3
        assert {row["model_version"] for row in rows} == {"current"}
        assert {
            row["snap_date"].strftime("%Y-%m-%d") for row in rows
        } == {"2024-03-31"}
        assert sorted(row["rank"] for row in rows) == [1, 2, 3]

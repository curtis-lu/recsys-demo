"""Tests for inference pipeline validation (Spark backend)."""

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.pipelines.inference.nodes_spark import validate_predictions
from recsys_tfb.pipelines.inference.validation import ValidationError

pytestmark = pytest.mark.spark


@pytest.fixture
def parameters():
    return {
        "inference": {
            "snap_dates": ["2024-03-31"],
            "products": ["exchange_fx", "fund_stock", "fund_bond"],
        },
    }


def _rerank(df):
    """Re-rank by score descending within each (snap_date, cust_id) group."""
    w = Window.partitionBy("snap_date", "cust_id").orderBy(F.col("score").desc())
    return df.withColumn("rank", F.row_number().over(w))


def _make_valid_data(spark, n_customers=3):
    """Build a valid ranked_predictions and matching scoring_dataset (Spark)."""
    products = ["exchange_fx", "fund_stock", "fund_bond"]
    snap = date(2024, 3, 31)
    scores = [0.9, 0.6, 0.3]
    ranked_rows = []
    scoring_rows = []
    for i in range(n_customers):
        cid = f"C{i+1:03d}"
        for rank, (prod, score) in enumerate(zip(products, scores), 1):
            ranked_rows.append((snap, cid, prod, float(score), rank))
            scoring_rows.append((snap, cid, prod, 100.0))
    ranked = spark.createDataFrame(
        ranked_rows, ["snap_date", "cust_id", "prod_name", "score", "rank"]
    )
    scoring = spark.createDataFrame(
        scoring_rows, ["snap_date", "cust_id", "prod_name", "total_aum"]
    )
    return ranked, scoring


class TestValidatePredictionsPass:
    def test_valid_data_passes(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        result = validate_predictions(ranked, scoring, parameters)
        assert result is ranked

    def test_valid_data_multiple_customers(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark, n_customers=10)
        result = validate_predictions(ranked, scoring, parameters)
        assert result.count() == 30


class TestRowCountMatch:
    def test_mismatch_raises(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        scoring_short = scoring.limit(scoring.count() - 1)
        with pytest.raises(ValidationError, match="row_count_match"):
            validate_predictions(ranked, scoring_short, parameters)


class TestScoreRange:
    def test_score_below_zero(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(-0.1),
            ).otherwise(F.col("score")),
        )
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, scoring, parameters)

    def test_score_above_one(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(1.5),
            ).otherwise(F.col("score")),
        )
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, scoring, parameters)


class TestNoMissing:
    def test_nan_in_score(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(None).cast("double"),
            ).otherwise(F.col("score")),
        )
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, scoring, parameters)

    def test_nan_in_identity(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "cust_id",
            F.when(F.col("cust_id") == "C001", F.lit(None).cast("string"))
            .otherwise(F.col("cust_id")),
        )
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, scoring, parameters)


class TestCompleteness:
    def test_missing_product(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        drop_filter = ~(
            (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx")
        )
        ranked = ranked.filter(drop_filter)
        scoring = scoring.filter(drop_filter)
        with pytest.raises(ValidationError, match="completeness"):
            validate_predictions(ranked, scoring, parameters)


class TestRankConsistency:
    def test_non_sequential_ranks(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "rank",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(5),
            ).otherwise(F.col("rank")),
        )
        with pytest.raises(ValidationError, match="rank_consistency"):
            validate_predictions(ranked, scoring, parameters)

    def test_score_order_mismatch(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("rank") == 1),
                F.lit(0.3),
            ).when(
                (F.col("cust_id") == "C001") & (F.col("rank") == 3),
                F.lit(0.9),
            ).otherwise(F.col("score")),
        )
        with pytest.raises(ValidationError, match="rank_consistency"):
            validate_predictions(ranked, scoring, parameters)


class TestNoDuplicates:
    def test_duplicate_rows(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        dup_ranked = ranked.filter(
            (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx")
        )
        dup_scoring = scoring.filter(
            (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx")
        )
        ranked = ranked.unionByName(dup_ranked)
        scoring = scoring.unionByName(dup_scoring)
        with pytest.raises(ValidationError, match="no_duplicates"):
            validate_predictions(ranked, scoring, parameters)


class TestMultipleFailures:
    def test_multiple_checks_fail(self, spark, parameters):
        ranked, scoring = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(-0.5),
            ).when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "fund_stock"),
                F.lit(None).cast("double"),
            ).otherwise(F.col("score")),
        )
        with pytest.raises(ValidationError) as exc_info:
            validate_predictions(ranked, scoring, parameters)
        assert len(exc_info.value.failures) >= 2

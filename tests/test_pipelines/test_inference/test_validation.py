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


PRODUCTS = ["exchange_fx", "fund_stock", "fund_bond"]


def _manifest(expected=None, written=None, n_buckets=1):
    """A score manifest whose partition bookkeeping agrees with itself.

    The scoring node reports what should exist (what it wrote plus what it
    skipped) and what the metastore says does. Every test below corrupts the
    *ranked frame*, so unless it is about ``partition_completeness`` the two
    lists agree.
    """
    grid = [
        ["2024-03-31", bucket, product]
        for bucket in range(n_buckets)
        for product in PRODUCTS
    ]
    return {
        "expected_partitions": grid if expected is None else expected,
        "written_partitions": grid if written is None else written,
    }


def _make_valid_data(spark, n_customers=3):
    """Build a valid ranked_predictions frame and a matching score manifest."""
    snap = date(2024, 3, 31)
    scores = [0.9, 0.6, 0.3]
    ranked_rows = []
    for i in range(n_customers):
        cid = f"C{i+1:03d}"
        for rank, (prod, score) in enumerate(zip(PRODUCTS, scores), 1):
            ranked_rows.append((snap, cid, prod, float(score), rank))
    ranked = spark.createDataFrame(
        ranked_rows, ["snap_date", "cust_id", "prod_name", "score", "rank"]
    )
    return ranked, _manifest()


class TestValidatePredictionsPass:
    def test_valid_data_passes(self, spark, parameters):
        """回傳的是裁過月份的 frame，不是原物件。

        publish 寫的就是這個回傳值，所以它必須只含本次的月份；`is` 比對在
        `_filter_current_inference_scope` 時代之所以成立，只是因為測試沒設
        `model_version`、整個 helper 直接 early-return。
        """
        ranked, manifest = _make_valid_data(spark)
        result = validate_predictions(ranked, manifest, parameters)
        assert sorted(result.columns) == sorted(ranked.columns)
        assert result.collect() == ranked.collect()

    def test_valid_data_multiple_customers(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark, n_customers=10)
        result = validate_predictions(ranked, manifest, parameters)
        assert result.count() == 30

    def test_ignores_other_snap_date_partitions(self, spark, parameters):
        """`ranked_staging` 跨月份累積，驗證只該看本次的月份。

        模型版本那一半不在這裡了：catalog 的 `partition_filter: model_version`
        讓 load 發 `WHERE model_version = '…'` 並把該欄 drop 掉，所以節點看到的
        frame 本來就只剩本次模型（ADR-0010 §5）。月份沒有任何東西擋，不自己裁
        的話每一條逐組檢查都會把舊月份算進來。
        """
        ranked, manifest = _make_valid_data(spark)
        other_date = (
            ranked.withColumn("snap_date", F.lit("2024-01-31").cast("date"))
            .withColumn("score", F.lit(2.0))
        )
        persisted_staging = ranked.unionByName(other_date)

        result = validate_predictions(persisted_staging, manifest, parameters)

        assert result.count() == ranked.count()
        assert {
            row["snap_date"].strftime("%Y-%m-%d")
            for row in result.select("snap_date").distinct().collect()
        } == {"2024-03-31"}


class TestPartitionCompleteness:
    """Replaces ``row_count_match``, which is unbuildable in the new shape.

    The old check compared the ranked row count against ``scoring_dataset``'s.
    The un-exploded ``inference_population_features`` is ``len(items)`` times
    shorter, so that comparison would fail on every correct run — and comparing
    the manifest's row count instead fails on every *resume*, which writes only
    a fraction (ADR-0011 §3). Partition sets are the quantity that survives
    both, and they need no scan.
    """

    def test_a_chunk_with_no_partition_raises(self, spark, parameters):
        """A successive save that replaced another chunk's partition."""
        ranked, _ = _make_valid_data(spark)
        manifest = _manifest(
            written=[["2024-03-31", 0, "exchange_fx"]],
        )
        with pytest.raises(ValidationError, match="partition_completeness"):
            validate_predictions(ranked, manifest, parameters)

    def test_a_partition_no_chunk_claims_raises(self, spark, parameters):
        """A stale bucket left behind by an ``entity_buckets`` change.

        Re-scoring can never delete it and it keeps contributing rows to this
        ``model_version``'s ranking, so it is a failure rather than a warning.
        """
        ranked, _ = _make_valid_data(spark)
        manifest = _manifest()
        manifest["written_partitions"] = manifest["written_partitions"] + [
            ["2024-03-31", 7, "fund_stock"]
        ]
        with pytest.raises(ValidationError, match="partition_completeness"):
            validate_predictions(ranked, manifest, parameters)

    def test_a_manifest_without_the_bookkeeping_raises_rather_than_passes(
        self, spark, parameters
    ):
        """Two absent keys would compare equal and the check would be decorative.

        The whole point of this check is catching a silently short table, so its
        own silent-pass mode has to be closed.
        """
        ranked, _ = _make_valid_data(spark)
        with pytest.raises(KeyError):
            validate_predictions(ranked, {}, parameters)

    def test_a_resumed_run_with_every_chunk_present_passes(self, spark, parameters):
        """Nothing written this run, everything skipped — still complete."""
        ranked, manifest = _make_valid_data(spark)
        result = validate_predictions(ranked, manifest, parameters)
        assert result.count() == ranked.count()


class TestScoreRange:
    def test_score_below_zero(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(-0.1),
            ).otherwise(F.col("score")),
        )
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, manifest, parameters)

    def test_score_above_one(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(1.5),
            ).otherwise(F.col("score")),
        )
        ranked = _rerank(ranked)
        with pytest.raises(ValidationError, match="score_range"):
            validate_predictions(ranked, manifest, parameters)


class TestNoMissing:
    def test_nan_in_score(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(None).cast("double"),
            ).otherwise(F.col("score")),
        )
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, manifest, parameters)

    def test_nan_in_identity(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "cust_id",
            F.when(F.col("cust_id") == "C001", F.lit(None).cast("string"))
            .otherwise(F.col("cust_id")),
        )
        with pytest.raises(ValidationError, match="no_missing"):
            validate_predictions(ranked, manifest, parameters)


class TestCompleteness:
    def test_missing_product(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        drop_filter = ~(
            (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx")
        )
        ranked = ranked.filter(drop_filter)
        with pytest.raises(ValidationError, match="completeness"):
            validate_predictions(ranked, manifest, parameters)


class TestRankConsistency:
    def test_non_sequential_ranks(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "rank",
            F.when(
                (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx"),
                F.lit(5),
            ).otherwise(F.col("rank")),
        )
        with pytest.raises(ValidationError, match="rank_consistency"):
            validate_predictions(ranked, manifest, parameters)

    def test_score_order_mismatch(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
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
            validate_predictions(ranked, manifest, parameters)


class TestNoDuplicates:
    def test_duplicate_rows(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        dup_ranked = ranked.filter(
            (F.col("cust_id") == "C001") & (F.col("prod_name") == "exchange_fx")
        )
        ranked = ranked.unionByName(dup_ranked)
        with pytest.raises(ValidationError, match="no_duplicates"):
            validate_predictions(ranked, manifest, parameters)


class TestMultipleFailures:
    def test_multiple_checks_fail(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
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
            validate_predictions(ranked, manifest, parameters)
        assert len(exc_info.value.failures) >= 2

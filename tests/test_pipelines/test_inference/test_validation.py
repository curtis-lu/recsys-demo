"""The batch layer of inference validation (Spark backend).

The other layer's tests are in ``test_chunk_validation.py`` and need no Spark.
``TestWhichLayerEachCheckLivesIn`` below is the half of the layering pin that
has to run here.
"""

import logging
from datetime import date

import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import GroupedData
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from recsys_tfb.pipelines.inference.nodes import validate_predictions
from recsys_tfb.pipelines.inference.steps.validation import (
    BATCH_CHECKS,
    CHUNK_CHECKS,
    ValidationError,
)

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


def _failed_checks(ranked, manifest, parameters) -> set[str]:
    """The names of the checks that fired — asserting on the set, not just one.

    ``pytest.raises(match=…)`` says a check fired; it does not say the *other*
    checks stayed quiet. Half of ADR-0011's audit is "red on the right one".
    """
    with pytest.raises(ValidationError) as exc_info:
        validate_predictions(ranked, manifest, parameters)
    return {failure["check"] for failure in exc_info.value.failures}


class ActionCountingFrame:
    """Counts the Spark actions ``validate_predictions`` asks the frame for.

    The check budget is the whole point of splitting validation in two, and
    nothing about the *result* of validation can see it: a re-added
    whole-table check passes its own test, changes no output, and quietly
    costs another full scan of a Hive table. Same reasoning — and same shape —
    as ``ReadCountingFrame`` in ``test_nodes.py``, which pins the loop
    order for ADR-0010.

    Every DataFrame-returning call is re-wrapped so the counter follows the
    chain, and ``groupBy`` gets its own wrapper because ``GroupedData.agg`` is
    lazy: counting it as an action would credit the node for work it has not
    asked for yet.
    """

    #: Broader than what the node uses today, deliberately. The point is to
    #: notice an action the node *starts* using — a list narrowed to `count`
    #: and `collect` would let a new `toPandas()` through silently, which is
    #: the regression this class exists to catch.
    _ACTIONS = frozenset({
        "count", "collect", "toPandas", "take", "first", "head", "show",
        "foreach", "toLocalIterator",
    })

    def __init__(self, df, counter=None):
        self._df = df
        self.counter = {"actions": 0} if counter is None else counter

    @property
    def columns(self):
        return self._df.columns

    def _wrap(self, result):
        if isinstance(result, SparkDataFrame):
            return ActionCountingFrame(result, self.counter)
        if isinstance(result, GroupedData):
            return _CountingGroupedData(result, self.counter)
        return result

    def __getattr__(self, name):
        attr = getattr(self._df, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            unwrapped = [
                arg._df if isinstance(arg, ActionCountingFrame) else arg
                for arg in args
            ]
            result = attr(*unwrapped, **kwargs)
            if name in self._ACTIONS:
                self.counter["actions"] += 1
            return self._wrap(result)

        return call


class _CountingGroupedData:
    def __init__(self, grouped, counter):
        self._grouped = grouped
        self.counter = counter

    def __getattr__(self, name):
        attr = getattr(self._grouped, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            result = attr(*args, **kwargs)
            if isinstance(result, SparkDataFrame):
                return ActionCountingFrame(result, self.counter)
            return result

        return call


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


class TestScoreRangeIsGone:
    """``score_range`` was deleted, not relocated, and this keeps it deleted.

    On every path that applies a calibrator, ``[0, 1]`` holds by construction
    and the assertion could not go red; on an uncalibrated ranking objective —
    a configuration A7 explicitly permits and
    ``inference.use_calibration: false`` explicitly supports — the raw booster
    output is unbounded and the assertion is a false alarm. Decorative or
    wrong, no third case (ADR-0011 §2).
    """

    def test_a_score_outside_zero_to_one_is_not_a_failure(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        ranked = _rerank(
            ranked.withColumn("score", F.col("score") * F.lit(100.0) - F.lit(50.0))
        )
        result = validate_predictions(ranked, manifest, parameters)
        assert result.count() == 9


class TestScoreVariesWithinGroup:
    """The data-layer half of the ``require_item_is_a_feature`` backstop.

    That backstop lives at the config layer: it stops a run whose ``schema``
    forgot the item. It cannot stop a run whose config is right and whose
    pipeline fed the model a degenerate item value — and the by-chunk scoring
    shape makes that a one-line mistake in the driver (ADR-0011 §1, example
    two).
    """

    def test_every_group_scoring_every_product_identically(self, spark, parameters):
        """Every other check stays green on this frame, which is the point.

        The groups are complete, the ranks are 1..3, and the lag check compares
        with ``>`` so a tie does not violate it. Only this check sees it.
        """
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn("score", F.lit(0.5))
        with pytest.raises(ValidationError) as exc_info:
            validate_predictions(ranked, manifest, parameters)
        assert [f["check"] for f in exc_info.value.failures] == [
            "score_varies_within_group"
        ]

    def test_a_few_tied_groups_are_logged_and_published(
        self, spark, parameters, caplog
    ):
        """A correct isotonic run looks like this, so it must not block publication.

        ``IsotonicRegression`` fits a monotone function with plateaus: a group
        whose raw scores all land on one plateau comes out exactly tied with
        nothing wrong upstream. Measured on a synthetic fit — 61 of 200,000
        groups (0.03%) tied, against zero uncalibrated. A ``> 0`` rule would
        fail every correct run at production scale, which is the false alarm
        the product-form partition count was rejected for.

        Still logged: a tied group's internal ranking really is arbitrary, and
        silence would make it unfindable.
        """
        ranked, manifest = _make_valid_data(spark, n_customers=10)
        ranked = ranked.withColumn(
            "score",
            F.when(F.col("cust_id") == "C001", F.lit(0.5))
            .otherwise(F.col("score")),
        )
        with caplog.at_level(logging.WARNING):
            result = validate_predictions(ranked, manifest, parameters)

        assert result.count() == 30
        assert "1 of 10 query group(s)" in caplog.text

    def test_a_single_item_configuration_is_not_a_failure(self, spark):
        """A group of one cannot vary, so flagging it would be a false alarm.

        Group size is ``completeness``'s question. Without this guard every
        run of a one-item configuration would fail validation while being
        entirely correct — the same shape of false positive the product-form
        partition count had on small populations.
        """
        parameters = {
            "inference": {
                "snap_dates": ["2024-03-31"],
                "products": ["exchange_fx"],
            },
        }
        ranked = spark.createDataFrame(
            [(date(2024, 3, 31), "C001", "exchange_fx", 0.9, 1)],
            ["snap_date", "cust_id", "prod_name", "score", "rank"],
        )
        manifest = {
            "expected_partitions": [["2024-03-31", 0, "exchange_fx"]],
            "written_partitions": [["2024-03-31", 0, "exchange_fx"]],
        }
        result = validate_predictions(ranked, manifest, parameters)
        assert result.count() == 1


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


class TestMultipleFailures:
    def test_multiple_checks_fail(self, spark, parameters):
        """Collect-all, not fail-fast: one run should name every broken thing."""
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.filter(
            ~(
                (F.col("cust_id") == "C001")
                & (F.col("prod_name") == "exchange_fx")
            )
        ).withColumn("score", F.lit(0.5))
        with pytest.raises(ValidationError) as exc_info:
            validate_predictions(ranked, manifest, parameters)
        assert {f["check"] for f in exc_info.value.failures} == {
            "completeness", "score_varies_within_group",
        }


class TestWhichLayerEachCheckLivesIn:
    """ADR-0011 §3's table, from the batch side. Chunk side: ``test_chunk_validation``.

    Without a pin here the two layers drift back into one: someone adds a
    whole-table null check "to be safe", it passes its own test, and the early
    failure the split exists for is gone along with the action budget.
    """

    def test_every_registered_batch_check_is_reachable(self, spark, parameters):
        """Each name in the register can be produced, and nothing else can.

        Also the guard against a check being deleted while its name stays in
        the register — the layering assertions would keep passing.
        """
        triggered = set()

        ranked, _ = _make_valid_data(spark)
        triggered |= _failed_checks(
            ranked, _manifest(written=[["2024-03-31", 0, "exchange_fx"]]),
            parameters,
        )

        ranked, manifest = _make_valid_data(spark)
        triggered |= _failed_checks(
            ranked.filter(
                ~(
                    (F.col("cust_id") == "C001")
                    & (F.col("prod_name") == "exchange_fx")
                )
            ),
            manifest, parameters,
        )

        ranked, manifest = _make_valid_data(spark)
        triggered |= _failed_checks(
            ranked.withColumn(
                "rank",
                F.when(F.col("rank") == 3, F.lit(9)).otherwise(F.col("rank")),
            ),
            manifest, parameters,
        )

        ranked, manifest = _make_valid_data(spark)
        triggered |= _failed_checks(
            ranked.withColumn("score", F.lit(0.5)), manifest, parameters,
        )

        assert triggered == set(BATCH_CHECKS)
        # Equality, not a subset: it is also the "reports nothing from the
        # chunk register" half. `no_missing` and `no_duplicates` reappearing
        # here would mean the two layers had drifted back into one.
        assert triggered.isdisjoint(CHUNK_CHECKS)

    def test_a_null_score_is_the_chunk_layer_s_problem(self, spark, parameters):
        """Nulls are checked where they are cheap and early, not here.

        And they genuinely are not visible here: ``min``/``max`` skip nulls and
        the lag comparison is false against one, so a whole-table null check
        would be a third scan buying an answer the chunk layer already gave
        hours earlier.
        """
        ranked, manifest = _make_valid_data(spark)
        ranked = ranked.withColumn(
            "score",
            F.when(F.col("rank") == 3, F.lit(None).cast("double"))
            .otherwise(F.col("score")),
        )
        result = validate_predictions(ranked, manifest, parameters)
        assert result.count() == 9

    def test_a_duplicate_row_is_reported_as_completeness(self, spark, parameters):
        """The batch layer sees a duplicate only as a group of the wrong size.

        Which is enough — and is why ``no_duplicates`` moved rather than being
        kept in both places. A ``dropDuplicates`` here is a shuffle over the
        whole table to re-derive what the group sizes already said.
        """
        ranked, manifest = _make_valid_data(spark)
        duplicated = ranked.unionByName(
            ranked.filter(
                (F.col("cust_id") == "C001")
                & (F.col("prod_name") == "exchange_fx")
            )
        )
        assert _failed_checks(duplicated, manifest, parameters) == {"completeness"}


class TestBatchLayerActionBudget:
    """AC: the whole-table layer costs 2-3 Spark actions. It landed at 2.

    Nothing in the validation *result* can see this number, so it needs its own
    assertion — see :class:`ActionCountingFrame`.
    """

    def test_the_success_path_costs_two_actions(self, spark, parameters):
        ranked, manifest = _make_valid_data(spark)
        counting = ActionCountingFrame(ranked)

        validate_predictions(counting, manifest, parameters)

        assert counting.counter["actions"] == 2, (
            "one grouped aggregation (completeness + score variance + rank "
            "range) and one windowed pass (score vs rank order); "
            "partition_completeness scans nothing"
        )

    def test_partition_completeness_adds_no_action(self, spark, parameters):
        """It answers from two lists the manifest already carries.

        The check it replaced compared row counts, which cost a full re-run of
        the population-to-feature join every time (ADR-0011 §3). Failing it
        must cost the same as a clean run, not more.
        """
        ranked, _ = _make_valid_data(spark)
        counting = ActionCountingFrame(ranked)

        with pytest.raises(ValidationError, match="partition_completeness"):
            validate_predictions(
                counting,
                _manifest(written=[["2024-03-31", 0, "exchange_fx"]]),
                parameters,
            )

        assert counting.counter["actions"] == 2

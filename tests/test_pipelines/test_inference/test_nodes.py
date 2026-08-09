"""Tests for the inference pipeline's nodes."""

import logging

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.inference.steps.chunk_plans import ScoringChunk
from recsys_tfb.pipelines.inference.nodes import (
    build_inference_population_features,
    predict_and_write_scores,
    rank_predictions,
    validate_predictions,
)
from recsys_tfb.pipelines.inference.steps.partitions import ENTITY_BUCKET_COL
from recsys_tfb.pipelines.inference.steps.validation import ValidationError

pytestmark = pytest.mark.spark

PARTITION_COLS = ["snap_date", "prod_name", ENTITY_BUCKET_COL]


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
def inference_population(feature_table):
    return feature_table.select("snap_date", "cust_id").distinct()


@pytest.fixture
def parameters():
    return {
        "inference": {
            "snap_dates": ["2024-03-31"],
            "products": ["exchange_fx", "fund_stock", "fund_bond"],
            # One bucket keeps the fixture's three customers in a single chunk,
            # so tests that are not about bucketing get a deterministic layout.
            # TestEntityBuckets uses a larger population and several buckets.
            "entity_buckets": 1,
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


@pytest.fixture
def population_features(inference_population, feature_table, parameters, preprocessor):
    return build_inference_population_features(
        inference_population, feature_table, preprocessor, parameters
    )


class FakeScoreTable:
    """Stands in for the ``HiveTableDataset`` handed over by ``Node(writes=…)``.

    Records every frame passed to ``save()`` and answers
    ``existing_partition_values()`` the way the metastore does — from what has
    actually been written, including whatever the constructor says was left by a
    previous run. That pairing is what makes the resume tests mean something:
    a chunk is "already done" here for exactly the reason it is in production.

    Note the whole double needs no Spark: the node hands ``save()`` a pandas
    frame it just built in the driver, which is also why
    ``require_single_partition`` can check the partition spread for free.
    """

    def __init__(self, existing=()):
        self.existing = [ScoringChunk(*chunk) for chunk in existing]
        self.saved: list[pd.DataFrame] = []

    def save(self, pdf: pd.DataFrame) -> None:
        self.saved.append(pdf.copy())
        for _, row in pdf[PARTITION_COLS].drop_duplicates().iterrows():
            chunk = ScoringChunk(
                str(row["snap_date"]),
                int(row[ENTITY_BUCKET_COL]),
                str(row["prod_name"]),
            )
            if chunk not in self.existing:
                self.existing.append(chunk)

    def existing_partition_values(self) -> list[dict[str, str]]:
        return [
            {
                "snap_date": chunk.snap_date,
                "prod_name": chunk.item,
                ENTITY_BUCKET_COL: str(chunk.entity_bucket),
            }
            for chunk in self.existing
        ]

    # --- assertions the tests share ---

    def partitions_per_save(self) -> list[int]:
        return [len(pdf[PARTITION_COLS].drop_duplicates()) for pdf in self.saved]

    def saved_chunks(self) -> set[ScoringChunk]:
        return {
            ScoringChunk(
                str(row["snap_date"]),
                int(row[ENTITY_BUCKET_COL]),
                str(row["prod_name"]),
            )
            for pdf in self.saved
            for _, row in pdf[PARTITION_COLS].drop_duplicates().iterrows()
        }


class ReadCountingFrame:
    """Counts materialisations of the landed feature table.

    Only the methods the node uses are forwarded, on purpose: if the node starts
    reaching for another one this raises ``AttributeError`` instead of silently
    letting an uncounted read through.

    This is audit 2 of ADR-0010 in test form. Reversing the loop order is
    functionally invisible — same scores, same partitions — and costs
    ``len(items)`` times the reads, so the count is the only thing that sees it.

    ``collect`` is counted separately from ``toPandas``: the node makes one
    ``distinct().collect()`` per *month* to learn which buckets hold entities,
    and one ``toPandas()`` per *bucket* to score it. Lumping them would let a
    per-bucket bucket-listing hide inside the per-bucket read budget.
    """

    def __init__(self, df, counter=None):
        self._df = df
        self.counter = (
            {"toPandas": 0, "collect": 0} if counter is None else counter
        )

    @property
    def columns(self):
        return self._df.columns

    def filter(self, *args, **kwargs):
        return ReadCountingFrame(self._df.filter(*args, **kwargs), self.counter)

    def select(self, *args, **kwargs):
        return ReadCountingFrame(self._df.select(*args, **kwargs), self.counter)

    def distinct(self, *args, **kwargs):
        return ReadCountingFrame(self._df.distinct(*args, **kwargs), self.counter)

    def collect(self):
        self.counter["collect"] += 1
        return self._df.collect()

    def toPandas(self):
        self.counter["toPandas"] += 1
        return self._df.toPandas()


class ConstantModel:
    def predict(self, X):
        return np.full(len(X), 0.5)


class ItemSensitiveModel:
    """Scores by the item code, so an entity's items get different scores.

    ``ConstantModel`` cannot be used where a *valid* run is the premise: every
    query group would score identically, which is a real failure
    (``score_varies_within_group``) rather than fixture noise.
    """

    def feature_names(self):
        return ["prod_name", "total_aum"]

    def predict(self, X):
        return X[:, 0].astype(float)


class TestBuildInferencePopulationFeatures:
    def test_grain_is_time_by_entity_with_no_item_explosion(
        self, population_features, parameters
    ):
        """Three customers, three products, three rows — not nine.

        The whole cost argument rests on this: the ``(entity, item)`` feature
        vector is the entity's features plus one categorical scalar, so an
        explosion stores ``len(items) - 1`` copies that carry no information
        (ADR-0010 section 4).
        """
        assert population_features.count() == 3
        assert len(parameters["inference"]["products"]) == 3

    def test_item_column_is_absent(self, population_features):
        assert "prod_name" not in population_features.columns

    def test_columns_are_identity_then_features_then_bucket(
        self, population_features, preprocessor
    ):
        expected = (
            ["snap_date", "cust_id"]
            + [c for c in preprocessor["feature_columns"] if c != "prod_name"]
            + [ENTITY_BUCKET_COL]
        )
        assert population_features.columns == expected

    def test_stores_the_full_feature_set_not_a_subset(
        self, population_features, preprocessor
    ):
        """Every feature the artifact declares, minus the item.

        Trimming this to one model's columns would bind the table to a
        ``model_version`` and silently kill its reuse across models — with
        nothing going red, which is why this is asserted rather than trusted
        (ADR-0010 section 5).
        """
        stored = set(population_features.columns)
        for column in preprocessor["feature_columns"]:
            if column == "prod_name":
                continue
            assert column in stored, column

    def test_membership_comes_from_the_population_not_the_feature_table(self, spark):
        params = _population_params()
        out = build_inference_population_features(
            _population(spark), _features(spark), _population_preprocessor(), params
        )
        custs = {r["cust_id"] for r in out.select("cust_id").distinct().collect()}
        assert custs == {"c1", "c2", "c3"}

    def test_member_with_no_features_is_kept(self, spark):
        """Dropping it would silently shrink the ranked output for that entity."""
        out = build_inference_population_features(
            _population(spark), _features(spark), _population_preprocessor(),
            _population_params(),
        )
        rows = {r["cust_id"]: r["total_aum"] for r in out.collect()}
        assert set(rows) == {"c1", "c2", "c3"}
        assert rows["c3"] is None

    def test_feature_coverage_is_logged_per_month(self, spark, caplog):
        """The durable record of how many members had no features."""
        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.inference.steps.population"
        ):
            build_inference_population_features(
                _population(spark), _features(spark), _population_preprocessor(),
                _population_params(),
            )
        assert any(
            "members=3 missing_features=1" in r.getMessage() for r in caplog.records
        ), caplog.text

    def test_missing_snap_date_raises(self, spark):
        params = _population_params()
        params["inference"]["snap_dates"] = ["2024-03-31", "2024-04-30"]
        with pytest.raises(
            ValueError, match="inference_population missing inference.snap_dates"
        ):
            build_inference_population_features(
                _population(spark), _features(spark),
                _population_preprocessor(), params,
            )

    def test_missing_feature_raises(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        preprocessor["feature_columns"] = (
            preprocessor["feature_columns"] + ["nonexistent_col"]
        )
        with pytest.raises(ValueError, match="Missing feature columns"):
            build_inference_population_features(
                inference_population, feature_table, preprocessor, parameters
            )

    def test_time_column_is_a_partition_ready_string(self, population_features):
        """The partition directory name must not depend on Spark's date coercion.

        The resume planner compares metastore directory names against
        ``YYYY-MM-DD`` strings; anything else and every chunk looks unwritten.
        """
        assert dict(population_features.dtypes)["snap_date"] == "string"
        assert {
            row["snap_date"] for row in population_features.collect()
        } == {"2024-03-31"}

    def test_non_identity_categorical_is_still_encoded(self, spark, parameters):
        """Deferring identity categoricals must not defer the ordinary ones.

        ``channel`` is not identity, so it is the Spark side's job — the same
        rule the dataset pipeline's ``encodable_categoricals`` applies.
        """
        population = spark.createDataFrame(
            [("2024-03-31", "C001"), ("2024-03-31", "C002")],
            ["snap_date", "cust_id"],
        )
        features = spark.createDataFrame(
            [
                ("2024-03-31", "C001", "web", 1.0),
                ("2024-03-31", "C002", "branch", 2.0),
            ],
            ["snap_date", "cust_id", "channel", "total_aum"],
        )
        preprocessor = {
            "drop_columns": [],
            "categorical_columns": ["prod_name", "channel"],
            "category_mappings": {
                "prod_name": ["fund_bond", "exchange_fx", "fund_stock"],
                "channel": ["branch", "web"],
            },
            "feature_columns": ["prod_name", "channel", "total_aum"],
        }

        result = build_inference_population_features(
            population, features, preprocessor, parameters
        )

        rows = result.select("cust_id", "channel").collect()
        assert {row["cust_id"]: row["channel"] for row in rows} == {
            "C001": 1, "C002": 0,
        }

    def test_warns_when_the_population_holds_a_category_the_fit_never_saw(
        self, spark, parameters, caplog
    ):
        """The scoring population is not the training population.

        A month whose vocabulary has drifted is a real event here and nothing
        else reports it. The aggregation now runs on the un-exploded frame,
        which is the same set of values it was already reporting.
        """
        population = spark.createDataFrame(
            [("2024-03-31", "C001"), ("2024-03-31", "C002")],
            ["snap_date", "cust_id"],
        )
        features = spark.createDataFrame(
            [
                ("2024-03-31", "C001", "web", 1.0),
                ("2024-03-31", "C002", "kiosk", 2.0),
            ],
            ["snap_date", "cust_id", "channel", "total_aum"],
        )
        preprocessor = {
            "drop_columns": [],
            "categorical_columns": ["prod_name", "channel"],
            "category_mappings": {
                "prod_name": ["fund_bond", "exchange_fx", "fund_stock"],
                "channel": ["branch", "web"],
            },
            "feature_columns": ["prod_name", "channel", "total_aum"],
        }

        with caplog.at_level(logging.WARNING, logger="recsys_tfb.preprocessing"):
            build_inference_population_features(
                population, features, preprocessor, parameters
            )

        assert [
            r.getMessage()
            for r in caplog.records
            if r.name == "recsys_tfb.preprocessing" and r.levelno >= logging.WARNING
        ] == [
            "build_inference_population_features: 1 unknowns in column 'channel'"
        ]

    def test_casts_float_features_to_float32(self, spark, parameters, preprocessor):
        """Decimal AND Double feature columns must be cast to float (float32).

        Mirror of build_model_input behaviour: this table is read back into
        pandas per chunk and would otherwise face the same memory cost
        (Decimal → 70 B/value Python objects; Double → 8 B vs 4 B for no model
        benefit, LightGBM's histogram resolving at log2(max_bin)=8 bits).
        """
        from decimal import Decimal

        from pyspark.sql import types as T

        feature_cols = [
            c for c in preprocessor["feature_columns"]
            if c not in set(preprocessor["categorical_columns"])
        ]
        decimal_col = feature_cols[0]

        schema = T.StructType([
            T.StructField("snap_date", T.TimestampType()),
            T.StructField("cust_id", T.StringType()),
            *[
                T.StructField(
                    c,
                    T.DecimalType(38, 6) if c == decimal_col else T.DoubleType(),
                )
                for c in feature_cols
            ],
        ])
        snap_ts = pd.Timestamp("2024-03-31").to_pydatetime()
        row_values: list = [snap_ts, "C001"]
        for c in feature_cols:
            row_values.append(Decimal("1.5") if c == decimal_col else 0.5)
        features = spark.createDataFrame([tuple(row_values)], schema=schema)
        population = features.select("snap_date", "cust_id")

        result = build_inference_population_features(
            population, features, preprocessor, parameters
        )

        out_dtypes = dict(result.dtypes)
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


def _population_params(entity_buckets: int = 1) -> dict:
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "score": "score", "rank": "rank",
        }},
        "inference": {
            "snap_dates": ["2024-03-31"],
            "products": ["fund_stock", "fund_bond"],
            "entity_buckets": entity_buckets,
        },
    }


def _population_preprocessor() -> dict:
    return {
        "drop_columns": ["snap_date", "cust_id"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund_bond", "fund_stock"]},
        "feature_columns": ["prod_name", "total_aum"],
    }


def _population(spark):
    return spark.createDataFrame(
        [("2024-03-31", "c1"), ("2024-03-31", "c2"), ("2024-03-31", "c3")],
        ["snap_date", "cust_id"],
    )


def _features(spark):
    return spark.createDataFrame(
        [("2024-03-31", "c1", 1.0), ("2024-03-31", "c2", 2.0),
         ("2024-03-31", "c9", 9.0)],
        ["snap_date", "cust_id", "total_aum"],
    )


class TestEntityBuckets:
    """The bucket is a function of the entity, and nothing else.

    That is the load-bearing property: the ranking group is ``(time, entity)``,
    so the bucket has to be a function of the group, or one entity's items would
    be split across chunks that are scored and validated independently
    (ADR-0010 section 5).
    """

    def _wide_population(self, spark):
        rows = [("2024-03-31", f"c{i:03d}") for i in range(60)]
        rows += [("2024-01-31", f"c{i:03d}") for i in range(60)]
        return spark.createDataFrame(rows, ["snap_date", "cust_id"])

    def _wide_features(self, spark):
        rows = [
            (month, f"c{i:03d}", float(i))
            for month in ("2024-03-31", "2024-01-31")
            for i in range(60)
        ]
        return spark.createDataFrame(rows, ["snap_date", "cust_id", "total_aum"])

    def _build(self, spark, n_buckets, months=("2024-03-31",)):
        params = _population_params(entity_buckets=n_buckets)
        params["inference"]["snap_dates"] = list(months)
        return build_inference_population_features(
            self._wide_population(spark), self._wide_features(spark),
            _population_preprocessor(), params,
        )

    def test_bucket_values_stay_inside_the_configured_count(self, spark):
        out = self._build(spark, 4)
        buckets = {
            row[ENTITY_BUCKET_COL]
            for row in out.select(ENTITY_BUCKET_COL).distinct().collect()
        }
        assert buckets <= {"0", "1", "2", "3"}
        assert len(buckets) > 1, "60 entities over 4 buckets should not collapse"

    def test_an_entity_gets_the_same_bucket_in_every_month(self, spark):
        """The time column is deliberately not part of the hash."""
        out = self._build(spark, 4, months=("2024-03-31", "2024-01-31"))
        by_cust: dict[str, set[str]] = {}
        for row in out.select("cust_id", ENTITY_BUCKET_COL).collect():
            by_cust.setdefault(row["cust_id"], set()).add(row[ENTITY_BUCKET_COL])
        assert all(len(buckets) == 1 for buckets in by_cust.values())

    def test_bucket_assignment_is_deterministic_across_runs(self, spark):
        """A reshuffle between runs would orphan every partition already written."""
        first = {
            (row["cust_id"], row[ENTITY_BUCKET_COL])
            for row in self._build(spark, 4).collect()
        }
        second = {
            (row["cust_id"], row[ENTITY_BUCKET_COL])
            for row in self._build(spark, 4).collect()
        }
        assert first == second

    def test_changing_the_bucket_count_changes_the_assignment(self, spark):
        """Why entity_buckets is not a free knob: it re-partitions the table."""
        four = {
            (row["cust_id"], row[ENTITY_BUCKET_COL])
            for row in self._build(spark, 4).collect()
        }
        five = {
            (row["cust_id"], row[ENTITY_BUCKET_COL])
            for row in self._build(spark, 5).collect()
        }
        assert four != five


class TestPredictAndWriteScores:
    def test_returns_a_manifest_not_a_frame(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable()
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert isinstance(manifest, dict)
        assert manifest["items"] == sorted(parameters["inference"]["products"])
        assert manifest["entity_buckets"] == 1

    def test_one_save_per_chunk(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        # 1 bucket x 3 items
        assert len(table.saved) == 3

    def test_each_save_covers_exactly_one_partition(
        self, population_features, preprocessor, parameters
    ):
        """Constraint C, at the seam where it can be violated.

        ``save()`` is ``insertInto`` under ``partitionOverwriteMode=dynamic``:
        it touches only the partitions in the frame, but *replaces* those
        wholesale. A frame spanning two chunks means the second write deletes
        the first chunk's rows, with no error and no missing-column symptom.

        Deliberately not a before/after diff of the partition listing:
        re-publishing an existing partition makes that diff empty by
        construction, so it is blind to the very overwrite this pins.
        """
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert table.partitions_per_save() == [1, 1, 1]

    def test_a_frame_spanning_two_partitions_is_refused(self):
        from recsys_tfb.pipelines.inference.steps.partitions import (
            require_single_partition,
        )

        pdf = pd.DataFrame({
            "cust_id": ["c1", "c2"],
            "score": [0.1, 0.2],
            "snap_date": ["2024-03-31"] * 2,
            "prod_name": ["fund_bond", "fund_stock"],
            ENTITY_BUCKET_COL: ["0", "0"],
        })
        with pytest.raises(ValueError, match="exactly one partition"):
            require_single_partition(pdf, PARTITION_COLS)

    def test_saved_columns_are_entity_score_and_partition_cols(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert list(table.saved[0].columns) == [
            "cust_id", "score", "snap_date", "prod_name", ENTITY_BUCKET_COL,
        ]

    def test_model_version_column_is_left_to_the_catalog(
        self, population_features, preprocessor, parameters
    ):
        """The node does not inject ``model_version`` — ``partition_filter`` owns it.

        ``_apply_partition_filter_cols`` adds the column when missing and, when
        present, pays a ``distinct()`` action to check the value it just wrote.
        Injecting here buys that action for nothing, and from the same
        ``parameters["model_version"]``.
        """
        parameters["model_version"] = "mv-abc123"
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert "model_version" not in table.saved[0].columns

    def test_row_count_per_save_is_the_bucket_population(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable()
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert [len(pdf) for pdf in table.saved] == [3, 3, 3]
        assert manifest["n_rows_written"] == 9

    def test_item_values_are_names_not_codes(
        self, population_features, preprocessor, parameters
    ):
        """This value becomes the ``prod_name`` partition directory name.

        Asserting the column *name* stays green while the values are ``0``..``7``
        — the bug ADR-0010 section 6 reproduced on a real run.
        """
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        written = {
            value
            for pdf in table.saved
            for value in pdf["prod_name"].unique()
        }
        assert written == set(parameters["inference"]["products"])

    def test_model_receives_item_codes_from_the_category_mapping(
        self, population_features, preprocessor, parameters
    ):
        """The feature position holds ``category_mappings`` indexes.

        Not ``inference.products`` indexes: A4 makes the two lists hold the same
        values, nothing makes them hold the same *order*. The fixture's two
        orders differ, so reading the wrong list mis-codes every product while
        every existing sanity check stays green (ADR-0010 section 4).
        """
        mapping = preprocessor["category_mappings"]["prod_name"]
        products = parameters["inference"]["products"]
        assert [mapping.index(p) for p in products] != list(range(len(products))), (
            "fixture must give the two lists different orders, or the wrong "
            "source encodes to the same codes and this test cannot fail"
        )

        class CodeReportingModel:
            """Reports the item code it was fed as the score.

            Which puts the code next to the identity value it came from. The set
            of codes alone would not do: both lists hold the same products, so
            both orderings produce ``{0, 1, 2}`` and only the pairing separates
            them.
            """

            def feature_names(self):
                return ["prod_name", "total_aum"]

            def predict(self, X):
                return X[:, 0].astype(float)

        table = FakeScoreTable()
        predict_and_write_scores(
            CodeReportingModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )

        pairs = {
            (row["prod_name"], int(row["score"]))
            for pdf in table.saved
            for _, row in pdf.iterrows()
        }
        assert pairs == {(p, mapping.index(p)) for p in products}

    def test_reads_the_feature_table_once_per_bucket_not_once_per_chunk(
        self, spark, preprocessor, parameters
    ):
        """Audit 2 of ADR-0010, and the only thing that sees the loop order.

        Item-outer/bucket-inner produces identical scores and identical
        partitions at ``len(items)`` times the reads. Nothing about the output
        distinguishes the two.
        """
        params = _population_params(entity_buckets=3)
        population = spark.createDataFrame(
            [("2024-03-31", f"c{i:03d}") for i in range(60)],
            ["snap_date", "cust_id"],
        )
        features = spark.createDataFrame(
            [("2024-03-31", f"c{i:03d}", float(i)) for i in range(60)],
            ["snap_date", "cust_id", "total_aum"],
        )
        landed = build_inference_population_features(
            population, features, _population_preprocessor(), params
        )
        populated_buckets = len({
            row[ENTITY_BUCKET_COL]
            for row in landed.select(ENTITY_BUCKET_COL).distinct().collect()
        })
        assert populated_buckets == 3, "fixture must fill every bucket"

        counting = ReadCountingFrame(landed)
        table = FakeScoreTable()
        predict_and_write_scores(
            ConstantModel(), counting, _population_preprocessor(), params,
            unranked_predictions=table,
        )

        n_items = len(params["inference"]["products"])
        assert len(table.saved) == populated_buckets * n_items
        assert counting.counter["toPandas"] == populated_buckets, (
            "one read per bucket; "
            f"{populated_buckets * n_items} would be item-outer"
        )
        assert counting.counter["collect"] == 1, (
            "the populated-bucket listing is one action per month, not per "
            "bucket and not per chunk"
        )

    def test_an_empty_bucket_writes_nothing_and_is_reported(
        self, spark, parameters
    ):
        """A population smaller than the bucket count leaves buckets empty.

        ``insertInto`` creates no partition for an empty frame, so "expected"
        has to exclude them or every small-population run fails validation.
        """
        params = _population_params(entity_buckets=8)
        population = spark.createDataFrame(
            [("2024-03-31", "c1"), ("2024-03-31", "c2")],
            ["snap_date", "cust_id"],
        )
        features = spark.createDataFrame(
            [("2024-03-31", "c1", 1.0), ("2024-03-31", "c2", 2.0)],
            ["snap_date", "cust_id", "total_aum"],
        )
        landed = build_inference_population_features(
            population, features, _population_preprocessor(), params
        )
        table = FakeScoreTable()

        manifest = predict_and_write_scores(
            ConstantModel(), landed, _population_preprocessor(), params,
            unranked_predictions=table,
        )

        n_items = len(params["inference"]["products"])
        populated = len({
            row[ENTITY_BUCKET_COL]
            for row in landed.select(ENTITY_BUCKET_COL).distinct().collect()
        })
        assert populated < 8, "fixture must leave buckets empty"
        assert len(manifest["chunks_empty"]) == (8 - populated) * n_items
        assert len(manifest["expected_partitions"]) == populated * n_items
        assert manifest["expected_partitions"] == manifest["written_partitions"]

    def test_a_bucket_that_has_a_partition_but_reads_empty_raises(
        self, spark, parameters
    ):
        """The hole that excluding empty buckets from "expected" would open.

        A bucket with no partition is legitimately empty and must not count as
        missing. But a bucket that HAS a partition and still reads back empty
        means those entities are about to vanish from the published ranking —
        and nothing downstream can see it: no partition is expected for the
        chunk, and ``completeness`` only looks at the groups that *are* there,
        so an absent tenth of the population forms no groups to be incomplete.

        This is the same "data silently short, zero error messages" failure the
        whole ticket exists to prevent, so it raises rather than warns.
        """
        params = _population_params(entity_buckets=4)
        population = spark.createDataFrame(
            [("2024-03-31", f"c{i:03d}") for i in range(40)],
            ["snap_date", "cust_id"],
        )
        features = spark.createDataFrame(
            [("2024-03-31", f"c{i:03d}", float(i)) for i in range(40)],
            ["snap_date", "cust_id", "total_aum"],
        )
        landed = build_inference_population_features(
            population, features, _population_preprocessor(), params
        )
        doomed = sorted(
            row[ENTITY_BUCKET_COL]
            for row in landed.select(ENTITY_BUCKET_COL).distinct().collect()
        )[0]

        doomed_custs = {
            row["cust_id"]
            for row in landed.filter(
                landed[ENTITY_BUCKET_COL] == doomed
            ).select("cust_id").collect()
        }
        assert doomed_custs, "fixture must pick a populated bucket"

        class PartitionSaysYesDataSaysNo(ReadCountingFrame):
            """The listing sees the partition; the read of it comes back empty.

            Which is the shape of a chunk filter that does not match the table's
            partition spelling, or of a partition whose files went away
            underneath a run.
            """

            def filter(self, *args, **kwargs):
                return PartitionSaysYesDataSaysNo(
                    self._df.filter(*args, **kwargs), self.counter
                )

            def select(self, *args, **kwargs):
                return PartitionSaysYesDataSaysNo(
                    self._df.select(*args, **kwargs), self.counter
                )

            def toPandas(self):
                pdf = super().toPandas()
                if not pdf.empty and pdf["cust_id"].map(
                    lambda c: c in doomed_custs
                ).all():
                    return pdf.iloc[0:0]
                return pdf

        with pytest.raises(ValueError, match="has a partition"):
            predict_and_write_scores(
                ConstantModel(), PartitionSaysYesDataSaysNo(landed),
                _population_preprocessor(), params,
                unranked_predictions=FakeScoreTable(),
            )

    def test_model_declaring_a_subset_is_accepted_and_is_what_gets_sliced(
        self, population_features, preprocessor, parameters
    ):
        """A model trained under ``training.feature_selection.exclude``.

        The acquitting half of the subsequence assertion. ``preprocessor.json``
        holds the full feature set by design (A14 only guarantees the item
        survives selection), so a model declaring fewer columns is a *legal*
        configuration and must not raise — and the columns it declared, not the
        artifact's full list, are what X is sliced to.
        """
        assert len(preprocessor["feature_columns"]) > 2, (
            "fixture must be a strict superset"
        )

        class SubsetModel:
            def __init__(self):
                self.widths = []

            def feature_names(self):
                return ["prod_name", "total_aum"]

            def predict(self, X):
                self.widths.append(X.shape[1])
                return np.full(len(X), 0.5)

        model = SubsetModel()
        table = FakeScoreTable()
        predict_and_write_scores(
            model, population_features, preprocessor, parameters,
            unranked_predictions=table,
        )

        assert len(table.saved) == 3
        assert model.widths and set(model.widths) == {2}

    def test_model_declaring_a_permuted_order_raises(
        self, population_features, preprocessor, parameters
    ):
        """Same columns, different order — a model that does not match the artifact.

        The refusing half. Order is the numpy column layout, so realigning here
        would be guessing; the node fails instead (ADR-0011 section 5).
        """

        class PermutedModel:
            def feature_names(self):
                return ["total_aum", "prod_name"]

            def predict(self, X):  # pragma: no cover - must not be reached
                raise AssertionError("predict must not run on a mismatched model")

        with pytest.raises(ValueError, match="order-preserving subsequence"):
            predict_and_write_scores(
                PermutedModel(), population_features, preprocessor, parameters,
                unranked_predictions=FakeScoreTable(),
            )

    def test_model_declaring_a_column_the_artifact_lacks_raises(
        self, population_features, preprocessor, parameters
    ):
        """A stale ``preprocessor.json``: the model knows a feature it does not."""

        class StaleArtifactModel:
            def feature_names(self):
                return ["prod_name", "total_aum", "since_removed_feature"]

            def predict(self, X):  # pragma: no cover - must not be reached
                raise AssertionError("predict must not run on a stale artifact")

        with pytest.raises(ValueError, match="order-preserving subsequence"):
            predict_and_write_scores(
                StaleArtifactModel(), population_features, preprocessor,
                parameters, unranked_predictions=FakeScoreTable(),
            )

    def test_a_model_column_absent_from_the_landed_table_raises(
        self, population_features, preprocessor, parameters
    ):
        # The artifact knows the column and the model declares it, so the
        # subsequence assertion passes; the landed table was built before it
        # existed, which is the case this check is for.
        declared = preprocessor["feature_columns"] + ["never_landed"]
        preprocessor["feature_columns"] = declared

        class ModelWantingMore:
            def feature_names(self):
                return list(declared)

            def predict(self, X):  # pragma: no cover - must not be reached
                raise AssertionError("predict must not run")

        with pytest.raises(ValueError, match="missing columns required by the model"):
            predict_and_write_scores(
                ModelWantingMore(), population_features, preprocessor, parameters,
                unranked_predictions=FakeScoreTable(),
            )


class TestScoredChunksThroughToValidation:
    """ADR-0011's audit needs the chain, not the node.

    Both failures in that audit table are one-line mistakes inside the scoring
    loop, and neither is visible in the loop's own output. The item-domain one
    stops at the chunk layer; the score-variance one cannot be seen until an
    entity's items sit next to each other, which is only true after every chunk
    has landed and been ranked. Testing ``predict_and_write_scores`` alone would
    show one and miss the other.
    """

    def _score_rank_validate(
        self, spark, model, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable()
        manifest = predict_and_write_scores(
            model, population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        unranked = spark.createDataFrame(
            pd.concat(table.saved, ignore_index=True)
        )
        ranked = rank_predictions(unranked, manifest, parameters)
        return validate_predictions(ranked, manifest, parameters)

    def test_a_correctly_scored_run_passes_both_layers(
        self, spark, population_features, preprocessor, parameters
    ):
        """The acquitting half: a run that is right must survive both layers.

        Without it the two checks below could be passing because validation is
        red on everything.
        """
        result = self._score_rank_validate(
            spark, ItemSensitiveModel(), population_features, preprocessor,
            parameters,
        )
        assert result.count() == 9

    def test_scores_that_do_not_move_with_the_item_are_caught_at_the_batch_layer(
        self, spark, population_features, preprocessor, parameters
    ):
        """Row two of the audit table, and the reason the check is at the batch layer.

        A scorer whose output does not depend on the item is what a degenerate
        item feature *looks like* downstream — the two are indistinguishable
        once the scores exist, which is exactly why the check has to live where
        a whole query group is visible. A chunk holds one item and cannot ask
        the question at all.

        The failure must be this check and no other: every shape check is green
        on this frame (complete groups, ranks 1..3, no rank/score inversion
        because the lag comparison is ``>`` and every pair is a tie).
        """
        with pytest.raises(ValidationError) as exc_info:
            self._score_rank_validate(
                spark, ConstantModel(), population_features, preprocessor,
                parameters,
            )
        assert [f["check"] for f in exc_info.value.failures] == [
            "score_varies_within_group"
        ]

    def test_a_nan_score_stops_the_chunk_before_it_is_written(
        self, spark, population_features, preprocessor, parameters
    ):
        """The chunk layer is wired into the loop, and it fires before ``save()``.

        Deleting the call would leave every other test here green: the frames
        the node writes are unchanged, and the batch layer cannot see a null
        score (``min``/``max`` skip it, the lag comparison is false against it).
        Nothing reaching the table is the other half — a chunk that failed must
        not be published for a later run's resume to then skip.

        Row one of the audit table (``item_values_are_known``) has no test in
        this class on purpose: the identity item is the loop variable, so no
        configuration can put an unknown value there. It is a guard on that
        assignment line, and the mutation audit is what demonstrates it.
        """

        class NaNScoringModel:
            def predict(self, X):
                return np.full(len(X), np.nan)

        table = FakeScoreTable()
        with pytest.raises(ValidationError) as exc_info:
            predict_and_write_scores(
                NaNScoringModel(), population_features, preprocessor,
                parameters, unranked_predictions=table,
            )
        assert [f["check"] for f in exc_info.value.failures] == ["no_missing"]
        assert not table.saved, "a failed chunk must not reach the table"

    def test_a_repeated_entity_in_the_landed_table_stops_the_chunk(
        self, spark, population_features, preprocessor, parameters
    ):
        """``no_duplicates`` at its new home, on a frame that can really produce it.

        The landed table's grain is meant to be ``(time, entity)``; nothing at
        read time enforces it. One row per entity per chunk is what makes the
        whole-table ``dropDuplicates`` shuffle unnecessary, so the assumption
        has to be checked where it is free.
        """
        table = FakeScoreTable()
        with pytest.raises(ValidationError) as exc_info:
            predict_and_write_scores(
                ItemSensitiveModel(),
                population_features.unionByName(population_features),
                preprocessor, parameters, unranked_predictions=table,
            )
        assert [f["check"] for f in exc_info.value.failures] == ["no_duplicates"]


class TestPredictResume:
    def test_an_existing_partition_is_skipped(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable(
            existing=[ScoringChunk("2024-03-31", 0, "fund_stock")]
        )
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert len(table.saved) == 2
        assert manifest["chunks_skipped"] == [["2024-03-31", 0, "fund_stock"]]
        assert ScoringChunk("2024-03-31", 0, "fund_stock") not in table.saved_chunks()

    def test_skipped_chunks_still_count_as_expected_partitions(
        self, population_features, preprocessor, parameters
    ):
        """A skipped chunk's partition is the evidence it could be skipped.

        Leaving it out of "expected" would make the batch completeness check
        report the table as over-full on every resumed run.
        """
        table = FakeScoreTable(
            existing=[ScoringChunk("2024-03-31", 0, "fund_stock")]
        )
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert len(manifest["expected_partitions"]) == 3
        assert manifest["expected_partitions"] == manifest["written_partitions"]

    def test_everything_already_written_writes_nothing_and_does_not_raise(
        self, population_features, preprocessor, parameters
    ):
        table = FakeScoreTable(existing=[
            ScoringChunk("2024-03-31", 0, item)
            for item in parameters["inference"]["products"]
        ])
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert table.saved == []
        assert len(manifest["chunks_skipped"]) == 3
        assert manifest["n_rows_written"] == 0

    def test_rebuild_dates_force_a_rewrite(
        self, population_features, preprocessor, parameters
    ):
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY

        existing = [
            ScoringChunk("2024-03-31", 0, item)
            for item in parameters["inference"]["products"]
        ]
        parameters[REBUILD_SNAP_DATES_KEY] = ["2024-03-31"]
        table = FakeScoreTable(existing=existing)

        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )

        assert len(table.saved) == 3
        assert manifest["chunks_skipped"] == []
        assert len(manifest["chunks_rebuilt"]) == 3

    def test_a_dataset_that_cannot_list_partitions_rescores_everything(
        self, population_features, preprocessor, parameters, caplog
    ):
        """Failing towards wasteful, never towards silently stale."""

        class NoListing:
            def __init__(self):
                self.saved = []

            def save(self, pdf):
                self.saved.append(pdf)

        table = NoListing()
        with caplog.at_level(logging.WARNING):
            predict_and_write_scores(
                ConstantModel(), population_features, preprocessor, parameters,
                unranked_predictions=table,
            )
        assert len(table.saved) == 3
        assert "cannot list partitions" in caplog.text

    def test_a_stale_bucket_partition_is_reported_as_surplus(
        self, population_features, preprocessor, parameters
    ):
        """Lowering ``entity_buckets`` leaves partitions no run will touch.

        They keep contributing rows to this ``model_version``'s ranking and
        re-scoring cannot delete them, so they have to surface — here in the
        manifest, where ``validate_predictions`` turns them into a failure.
        """
        table = FakeScoreTable(
            existing=[ScoringChunk("2024-03-31", 7, "fund_stock")]
        )
        manifest = predict_and_write_scores(
            ConstantModel(), population_features, preprocessor, parameters,
            unranked_predictions=table,
        )
        assert ["2024-03-31", 7, "fund_stock"] in manifest["written_partitions"]
        assert ["2024-03-31", 7, "fund_stock"] not in manifest["expected_partitions"]

    def test_a_hive_null_partition_value_is_ignored(
        self, population_features, preprocessor, parameters, caplog
    ):
        class NullPartitionTable(FakeScoreTable):
            def existing_partition_values(self):
                return super().existing_partition_values() + [{
                    "snap_date": "2024-03-31",
                    "prod_name": "__HIVE_DEFAULT_PARTITION__",
                    ENTITY_BUCKET_COL: "0",
                }]

        table = NullPartitionTable()
        with caplog.at_level(logging.WARNING):
            predict_and_write_scores(
                ConstantModel(), population_features, preprocessor, parameters,
                unranked_predictions=table,
            )
        assert len(table.saved) == 3
        assert "NULL value" in caplog.text

    def test_a_non_numeric_bucket_value_is_ignored(
        self, population_features, preprocessor, parameters, caplog
    ):
        class BadBucketTable(FakeScoreTable):
            def existing_partition_values(self):
                return super().existing_partition_values() + [{
                    "snap_date": "2024-03-31",
                    "prod_name": "fund_stock",
                    ENTITY_BUCKET_COL: "not-a-number",
                }]

        table = BadBucketTable()
        with caplog.at_level(logging.WARNING):
            predict_and_write_scores(
                ConstantModel(), population_features, preprocessor, parameters,
                unranked_predictions=table,
            )
        assert len(table.saved) == 3
        assert "non-numeric" in caplog.text


class TestRankPredictions:
    def _unranked(self, spark, buckets=("0", "0", "0")):
        return spark.createDataFrame(pd.DataFrame({
            "cust_id": ["C001"] * 3,
            "score": [0.9, 0.3, 0.6],
            "snap_date": ["2024-03-31"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            ENTITY_BUCKET_COL: list(buckets),
        }))

    def test_rank_column_added(self, spark, parameters):
        result = rank_predictions(self._unranked(spark), {}, parameters)
        assert "rank" in result.columns

    def test_entity_bucket_is_dropped(self, spark, parameters):
        """The bucket stops here: it is a mechanism column, not a contract one.

        Carrying it into ``ranked_staging`` would leak the driver-memory knob
        into the shape downstream consumers query, and it is the one column
        ADR-0010 section 5 says must not reach the published tables.
        """
        result = rank_predictions(self._unranked(spark), {}, parameters)
        assert ENTITY_BUCKET_COL not in result.columns

    def test_rank_order(self, spark, parameters):
        result = rank_predictions(self._unranked(spark), {}, parameters)
        pdf = result.toPandas()
        ranks = dict(zip(pdf["prod_name"], pdf["rank"]))
        assert ranks == {"exchange_fx": 1, "fund_bond": 2, "fund_stock": 3}

    def test_rank_per_group(self, spark, parameters):
        unranked = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
            "score": [0.9, 0.3, 0.6, 0.1, 0.8, 0.5],
            "snap_date": ["2024-03-31"] * 6,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"] * 2,
            ENTITY_BUCKET_COL: ["0"] * 6,
        }))
        pdf = rank_predictions(unranked, {}, parameters).toPandas()
        for cid in ["C001", "C002"]:
            assert sorted(pdf.loc[pdf["cust_id"] == cid, "rank"].tolist()) == [1, 2, 3]

    def test_restricts_persisted_history_to_this_run_snap_dates(
        self, spark, parameters
    ):
        """表跨月份累積，而 `inference.snap_dates` 每次只有一個月。

        不限縮的話第二個月起會讀回全部歷史月份、重新排名、把舊月份無聲重新
        發布——`model_version` 提為 `partition_filter` 之後 catalog 只擋掉
        模型那一半，月份這一半沒有任何東西擋（ADR-0010 §5）。
        """
        unranked = spark.createDataFrame(pd.DataFrame({
            "cust_id": ["C001"] * 6,
            "score": [0.9, 0.3, 0.6] * 2,
            "snap_date": ["2024-03-31"] * 3 + ["2024-01-31"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"] * 2,
            ENTITY_BUCKET_COL: ["0"] * 6,
        }))

        rows = rank_predictions(unranked, {}, parameters).select(
            "snap_date", "rank"
        ).collect()

        assert len(rows) == 3
        assert {row["snap_date"] for row in rows} == {"2024-03-31"}
        assert sorted(row["rank"] for row in rows) == [1, 2, 3]

    def test_accepts_a_frame_with_no_model_version_column(self, spark, parameters):
        """catalog 的 load 在有 `partition_filter` 時會把該欄 drop 掉。

        所以節點看到的就是沒有 `model_version` 的 frame；舊的比對式過濾在
        這種輸入上是死碼，這條釘住「拿掉它之後仍然跑得動」。
        """
        parameters["model_version"] = "current"
        result = rank_predictions(self._unranked(spark), {}, parameters)
        assert "model_version" not in result.columns
        assert result.count() == 3


class TestRestrictToSnapDates:
    """單一職責：只裁月份。模型版本那一半由 catalog 的 partition_filter 負責。"""

    def _frame(self, spark):
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31", "2024-01-31"]),
            "cust_id": ["C001", "C001"],
            "score": [0.9, 0.1],
        }))

    def test_keeps_only_configured_snap_dates(self, spark, parameters):
        from recsys_tfb.pipelines.inference.steps.scoping import (
            restrict_to_snap_dates,
        )
        out = restrict_to_snap_dates(self._frame(spark), parameters)
        assert [
            r["snap_date"].strftime("%Y-%m-%d") for r in out.collect()
        ] == ["2024-03-31"]

    def test_does_not_touch_model_version(self, spark, parameters):
        """它不認得 model_version——兩種過濾合在一個 helper 正是被拆掉的東西。"""
        from recsys_tfb.pipelines.inference.steps.scoping import (
            restrict_to_snap_dates,
        )
        parameters["model_version"] = "current"
        pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 2),
            "cust_id": ["C001", "C002"],
            "model_version": ["current", "previous"],
        })
        out = restrict_to_snap_dates(spark.createDataFrame(pdf), parameters)
        assert {r["model_version"] for r in out.collect()} == {
            "current", "previous"
        }

    def test_missing_snap_dates_raises(self, spark, parameters):
        """空的 scope 不得靜默退化成「全部留下」——那正是它要防的失效。"""
        from recsys_tfb.pipelines.inference.steps.scoping import (
            restrict_to_snap_dates,
        )
        parameters["inference"]["snap_dates"] = []
        with pytest.raises(ValueError, match="inference.snap_dates"):
            restrict_to_snap_dates(self._frame(spark), parameters)

    def test_missing_time_column_raises(self, spark, parameters):
        from recsys_tfb.pipelines.inference.steps.scoping import (
            restrict_to_snap_dates,
        )
        pdf = pd.DataFrame({"cust_id": ["C001"], "score": [0.5]})
        with pytest.raises(ValueError, match="snap_date"):
            restrict_to_snap_dates(spark.createDataFrame(pdf), parameters)

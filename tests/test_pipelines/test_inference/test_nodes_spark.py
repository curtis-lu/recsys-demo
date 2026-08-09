"""Tests for inference pipeline Spark nodes."""

import logging

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
def inference_population(feature_table):
    return feature_table.select("snap_date", "cust_id").distinct()


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
    def test_cross_join_shape(self, inference_population, feature_table, parameters):
        result = build_scoring_dataset(inference_population, feature_table, parameters)
        # 3 customers x 3 products = 9 rows (only snap_date 2024-03-31)
        assert result.count() == 9

    def test_columns_present(self, inference_population, feature_table, parameters):
        result = build_scoring_dataset(inference_population, feature_table, parameters)
        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns
        assert "total_aum" in result.columns

    def test_all_products_per_customer(self, inference_population, feature_table, parameters):
        result = build_scoring_dataset(inference_population, feature_table, parameters)
        products = parameters["inference"]["products"]
        # toPandas() 在 BooleanType 欄（feature_present）上會觸發 PySpark 3.3.2 + NumPy 1.25 的
        # np.bool 廢棄問題，改用 Spark 端 collect() 避免
        for cid in ["C001", "C002", "C003"]:
            cust_prods = [
                row["prod_name"]
                for row in result.filter(result["cust_id"] == cid)
                .select("prod_name")
                .collect()
            ]
            assert sorted(cust_prods) == sorted(products)

    def test_missing_snap_date_raises(self, inference_population, feature_table, parameters):
        parameters["inference"]["snap_dates"] = [
            "2024-03-31",
            "2024-04-30",
        ]

        with pytest.raises(
            ValueError, match="inference_population missing inference.snap_dates"
        ):
            build_scoring_dataset(inference_population, feature_table, parameters)


class TestApplyPreprocessor:
    def test_output_has_identity_and_features(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor, parameters)
        # Should have identity cols + feature cols
        assert "snap_date" in result.columns
        assert "cust_id" in result.columns
        assert "prod_name" in result.columns  # identity *and* feature: appears twice
        for fc in preprocessor["feature_columns"]:
            assert fc in result.columns

    def test_feature_column_order(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor, parameters)
        identity_cols = ["snap_date", "cust_id", "prod_name"]
        expected = identity_cols + preprocessor["feature_columns"]
        assert result.columns == expected

    def test_identity_categorical_keeps_raw_value(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """`prod_name` is identity *and* feature; the Spark side must not encode it.

        The identity position is what reaches the partition column of the three
        inference tables, so an integer here is the ADR-0010 §6 bug: partition
        directories named ``prod_name=0``. The integer code belongs to the
        feature position only, and is applied in the driver (ADR-0010 §4).
        """
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        result = apply_preprocessor(scoring, preprocessor, parameters)

        values = {row["prod_name"] for row in result.select("prod_name").collect()}
        assert values == set(parameters["inference"]["products"])

    def test_non_identity_categorical_is_still_encoded(self, spark, parameters):
        """Deferring identity categoricals must not defer the ordinary ones.

        ``channel`` lives in the scoring frame and is not identity, so it is the
        Spark side's job — same rule as the dataset pipeline's
        ``encodable_categoricals``.
        """
        scoring = spark.createDataFrame(
            [
                ("2024-03-31", "C001", "fund_bond", "web", 1.0),
                ("2024-03-31", "C002", "fund_stock", "branch", 2.0),
            ],
            ["snap_date", "cust_id", "prod_name", "channel", "total_aum"],
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

        result = apply_preprocessor(scoring, preprocessor, parameters)

        rows = result.select("cust_id", "channel", "prod_name").collect()
        assert {row["cust_id"]: row["channel"] for row in rows} == {"C001": 1, "C002": 0}
        assert {row["prod_name"] for row in rows} == {"fund_bond", "fund_stock"}

    def test_warns_when_the_population_holds_a_category_the_fit_never_saw(
        self, spark, parameters, caplog
    ):
        """The scoring population is not the training population.

        The dataset pipeline has warned about sentinel encodings since it had
        one; inference silently encoded to ``-1``. A month whose vocabulary has
        drifted is a real event here and nothing else reports it.
        """
        scoring = spark.createDataFrame(
            [
                ("2024-03-31", "C001", "fund_bond", "web", 1.0),
                ("2024-03-31", "C002", "fund_stock", "kiosk", 2.0),
            ],
            ["snap_date", "cust_id", "prod_name", "channel", "total_aum"],
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
            apply_preprocessor(scoring, preprocessor, parameters)

        assert [
            r.getMessage()
            for r in caplog.records
            if r.name == "recsys_tfb.preprocessing" and r.levelno >= logging.WARNING
        ] == ["apply_preprocessor: 1 unknowns in column 'channel'"]

    def test_missing_feature_raises(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
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
    def test_output_is_spark_df(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.random.rand(len(X))

        result = predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)
        from pyspark.sql import DataFrame
        assert isinstance(result, DataFrame)

    def test_output_columns(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)
        assert set(result.columns) == {"snap_date", "cust_id", "prod_name", "score"}

    def test_model_version_column_is_left_to_the_catalog(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """節點不再自己塞 `model_version`——那是 `partition_filter` 的工作。

        `HiveTableDataset.save()` 的 `_apply_partition_filter_cols` 缺欄就補、
        有欄就先 `distinct()` 核對值；節點先塞好等於白付一次 action，而值來源
        還是同一個 `parameters["model_version"]`。訓練端的 `training_eval_predictions`
        本來就是這個形狀。
        """
        parameters["model_version"] = "mv-abc123"
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)
        assert "model_version" not in result.columns

    def test_row_count_matches(self, inference_population, feature_table, parameters, preprocessor):
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)
        assert result.count() == 9  # 3 customers x 3 products

    def test_output_item_values_are_the_configured_products(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """The item column carries product *names*, not their integer codes.

        This value reaches the ``prod_name`` partition column of all three
        inference tables. Asserting the column *name* — which the tests above do
        — stays green while the values are ``0``..``7`` (ADR-0010 §6).
        """
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def predict(self, X):
                return np.full(len(X), 0.5)

        result = predict_scores(
            MockModel(), X_score, scoring, preprocessor, parameters
        )

        values = {row["prod_name"] for row in result.select("prod_name").collect()}
        assert values == set(parameters["inference"]["products"])

    def test_model_receives_item_codes_from_the_category_mapping(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """The feature position holds ``category_mappings`` indexes.

        Not ``inference.products`` indexes: A4 makes the two lists hold the same
        values, nothing makes them hold the same *order*. The fixture's two
        orders differ, so reading the wrong list mis-codes every product while
        every existing sanity check stays green (ADR-0010 §4).
        """
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)
        mapping = preprocessor["category_mappings"]["prod_name"]
        products = parameters["inference"]["products"]
        assert [mapping.index(p) for p in products] != list(range(len(products))), (
            "fixture must give the two lists different orders, or the wrong "
            "source encodes to the same codes and this test cannot fail"
        )

        class MockModel:
            """Reports the item code it was fed as the score.

            Which puts the code next to the identity value it was derived from
            in the output frame. Collecting the codes alone would not do: both
            lists hold the same three products, so both orderings produce the
            same *set* {0, 1, 2} and only the pairing tells them apart.
            """

            def feature_names(self):
                return ["prod_name", "total_aum"]

            def predict(self, X):
                return X[:, 0].astype(float)

        result = predict_scores(
            MockModel(), X_score, scoring, preprocessor, parameters
        )

        pairs = {
            (row["prod_name"], int(row["score"]))
            for row in result.select("prod_name", "score").collect()
        }
        assert pairs == {(p, mapping.index(p)) for p in products}

    def test_model_declaring_a_subset_is_accepted_and_is_what_gets_sliced(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """A model trained under ``training.feature_selection.exclude``.

        The acquitting half of the subsequence assertion. ``preprocessor.json``
        holds the full feature set by design (A14 only guarantees the item
        survives selection), so a model declaring fewer columns is a *legal*
        configuration and must not raise — and the columns it declared, not the
        artifact's full list, are what X is sliced to.
        """
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)
        assert len(preprocessor["feature_columns"]) > 2, "fixture must be a strict superset"

        class MockModel:
            def __init__(self):
                self.widths = []

            def feature_names(self):
                return ["prod_name", "total_aum"]

            def predict(self, X):
                self.widths.append(X.shape[1])
                return np.full(len(X), 0.5)

        model = MockModel()
        result = predict_scores(
            model, X_score, scoring, preprocessor, parameters
        )

        assert result.count() == 9
        assert model.widths and set(model.widths) == {2}

    def test_model_declaring_a_permuted_order_raises(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """Same columns, different order — a model that does not match the artifact.

        The refusing half. Order is the numpy column layout, so realigning here
        would be guessing; the node fails instead (ADR-0011 §5).
        """
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def feature_names(self):
                return ["total_aum", "prod_name"]

            def predict(self, X):  # pragma: no cover - must not be reached
                raise AssertionError("predict must not run on a mismatched model")

        with pytest.raises(ValueError, match="order-preserving subsequence"):
            predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)

    def test_model_declaring_a_column_the_artifact_lacks_raises(
        self, inference_population, feature_table, parameters, preprocessor
    ):
        """A stale ``preprocessor.json``: the model knows a feature it does not."""
        scoring = build_scoring_dataset(inference_population, feature_table, parameters)
        X_score = apply_preprocessor(scoring, preprocessor, parameters)

        class MockModel:
            def feature_names(self):
                return ["prod_name", "total_aum", "since_removed_feature"]

            def predict(self, X):  # pragma: no cover - must not be reached
                raise AssertionError("predict must not run on a stale artifact")

        with pytest.raises(ValueError, match="order-preserving subsequence"):
            predict_scores(MockModel(), X_score, scoring, preprocessor, parameters)


class TestBuildScoringDatasetPopulation:
    def _params(self):
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "score": "score", "rank": "rank",
            }},
            "inference": {"snap_dates": ["2024-03-31"],
                          "products": ["fund_stock", "fund_bond"]},
        }

    def _pop(self, spark):
        return spark.createDataFrame(
            [("2024-03-31", "c1"), ("2024-03-31", "c2"), ("2024-03-31", "c3")],
            ["snap_date", "cust_id"],
        )

    def _features(self, spark):
        return spark.createDataFrame(
            [("2024-03-31", "c1", 1.0), ("2024-03-31", "c2", 2.0),
             ("2024-03-31", "c9", 9.0)],
            ["snap_date", "cust_id", "total_aum"],
        )

    def test_membership_from_population_not_feature_table(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        custs = {r["cust_id"] for r in out.select("cust_id").distinct().collect()}
        assert custs == {"c1", "c2", "c3"}

    def test_missing_feature_member_kept_and_flagged(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        flags = {r["cust_id"]: r["feature_present"]
                 for r in out.select("cust_id", "feature_present").distinct().collect()}
        assert flags == {"c1": True, "c2": True, "c3": False}

    def test_row_count_is_members_times_products(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        out = build_scoring_dataset(self._pop(spark), self._features(spark), self._params())
        assert out.count() == 3 * 2

    def test_missing_snap_date_raises(self, spark):
        from recsys_tfb.pipelines.inference.nodes_spark import build_scoring_dataset
        params = self._params()
        params["inference"]["snap_dates"] = ["2024-03-31", "2024-04-30"]
        with pytest.raises(ValueError, match="inference_population missing inference.snap_dates"):
            build_scoring_dataset(self._pop(spark), self._features(spark), params)


class TestRankPredictions:
    def test_rank_column_added(self, spark, parameters):
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        unranked = spark.createDataFrame(score_pdf)
        result = rank_predictions(unranked, parameters)
        assert "rank" in result.columns

    def test_rank_order(self, spark, parameters):
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })
        unranked = spark.createDataFrame(score_pdf)
        result = rank_predictions(unranked, parameters)
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
        unranked = spark.createDataFrame(score_pdf)
        result = rank_predictions(unranked, parameters)
        pdf = result.toPandas()
        for cid in ["C001", "C002"]:
            cust_ranks = sorted(pdf.loc[pdf["cust_id"] == cid, "rank"].tolist())
            assert cust_ranks == [1, 2, 3]

    def test_restricts_persisted_history_to_this_run_snap_dates(
        self, spark, parameters
    ):
        """表跨月份累積，而 `inference.snap_dates` 每次只有一個月。

        不限縮的話第二個月起會讀回全部歷史月份、重新排名、把舊月份無聲重新
        發布——`model_version` 提為 `partition_filter` 之後 catalog 只擋掉
        模型那一半，月份這一半沒有任何東西擋（ADR-0010 §5）。
        """
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(
                ["2024-03-31"] * 3 + ["2024-01-31"] * 3
            ),
            "cust_id": ["C001"] * 6,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"] * 2,
            "score": [0.9, 0.3, 0.6] * 2,
        })

        result = rank_predictions(spark.createDataFrame(score_pdf), parameters)
        rows = result.select("snap_date", "rank").collect()

        assert len(rows) == 3
        assert {
            row["snap_date"].strftime("%Y-%m-%d") for row in rows
        } == {"2024-03-31"}
        assert sorted(row["rank"] for row in rows) == [1, 2, 3]

    def test_accepts_a_frame_with_no_model_version_column(
        self, spark, parameters
    ):
        """catalog 的 load 在有 `partition_filter` 時會把該欄 drop 掉。

        所以節點看到的就是沒有 `model_version` 的 frame；舊的比對式過濾在
        這種輸入上是死碼，這條釘住「拿掉它之後仍然跑得動」。
        """
        parameters["model_version"] = "current"
        score_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * 3),
            "cust_id": ["C001"] * 3,
            "prod_name": ["exchange_fx", "fund_stock", "fund_bond"],
            "score": [0.9, 0.3, 0.6],
        })

        result = rank_predictions(spark.createDataFrame(score_pdf), parameters)

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
        from recsys_tfb.pipelines.inference.nodes_spark import (
            restrict_to_snap_dates,
        )
        out = restrict_to_snap_dates(self._frame(spark), parameters)
        assert [
            r["snap_date"].strftime("%Y-%m-%d") for r in out.collect()
        ] == ["2024-03-31"]

    def test_does_not_touch_model_version(self, spark, parameters):
        """它不認得 model_version——兩種過濾合在一個 helper 正是被拆掉的東西。"""
        from recsys_tfb.pipelines.inference.nodes_spark import (
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
        from recsys_tfb.pipelines.inference.nodes_spark import (
            restrict_to_snap_dates,
        )
        parameters["inference"]["snap_dates"] = []
        with pytest.raises(ValueError, match="inference.snap_dates"):
            restrict_to_snap_dates(self._frame(spark), parameters)

    def test_missing_time_column_raises(self, spark, parameters):
        from recsys_tfb.pipelines.inference.nodes_spark import (
            restrict_to_snap_dates,
        )
        pdf = pd.DataFrame({"cust_id": ["C001"], "score": [0.5]})
        with pytest.raises(ValueError, match="snap_date"):
            restrict_to_snap_dates(spark.createDataFrame(pdf), parameters)

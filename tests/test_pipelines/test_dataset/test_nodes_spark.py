"""Tests for dataset building pipeline Spark nodes."""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.nodes_spark import (
    apply_preprocessor_to_features,
    build_model_input,
    fit_preprocessor_metadata,
    select_test_keys,
    select_train_keys,
    select_val_keys,
    split_train_keys,
    validate_data_consistency,
)

pytestmark = pytest.mark.spark


# --- fixture shape -----------------------------------------------------------
# Every fixture in this module is generated from these three lists, and every
# count assertion is derived from them (or from ``parameters``) instead of being
# written as a literal. Resizing the fixture is then a one-line edit here rather
# than a hunt for a dozen hand-computed constants — and a constant that silently
# stops matching the fixture can no longer make an assertion vacuous.
#
# Entity count (24) is chosen so train / train-dev actually split: at the old
# size of 4, train_dev_ratio=0.2 rounded the dev side to empty and every
# assertion in TestSplitTrainKeys held for a `split` that did nothing (replacing
# it with "return sample_keys, sample_keys.limit(0)" kept them green). Item
# count stays at 3 and is deliberately NOT aligned with the 8 in conf/ — that
# gap is orthogonal to this one (see #140).
_ENTITIES = [f"C{i:03d}" for i in range(1, 25)]
_PRODUCTS = ["exchange_fx", "exchange_usd", "fund_stock"]
_SNAP_DATES = ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]

# Per-entity attribute values, cycled so any fixture size stays covered. These
# are inert with respect to every assertion below (they are neither identity nor
# feature columns); they exist so the frames look like the real tables.
_SEGMENTS = ["mass", "affluent", "hnw"]
_CHANNELS = ["digital", "branch", "both"]


def _entity_index(cid: str) -> int:
    return _ENTITIES.index(cid)


def _segment(cid: str) -> str:
    return _SEGMENTS[_entity_index(cid) % len(_SEGMENTS)]


def _channel(cid: str) -> str:
    return _CHANNELS[_entity_index(cid) % len(_CHANNELS)]


def _is_positive(cid: str, prod: str) -> bool:
    """One positive cell: the first entity on the first product."""
    return cid == _ENTITIES[0] and prod == _PRODUCTS[0]


# --- derivations used by assertions ------------------------------------------


def _n_items(parameters) -> int:
    """Declared item count. The schema is the source of truth, not the fixture."""
    schema = get_schema(parameters)
    return len(parameters["schema"]["categorical_values"][schema["item"]])


def _expected_key_count(parameters, split: str) -> int:
    """Row count of a full-population key selection for ``split``.

    sample_pool is a literal cross join, so a full-population selection is
    entities x items x that split's months.
    """
    n_dates = len(parameters["dataset"][f"{split}_snap_dates"])
    return len(_ENTITIES) * _n_items(parameters) * n_dates


def _identity_columns(parameters) -> list[str]:
    return sorted(get_schema(parameters)["identity_columns"])


def _expected_model_input_columns(keys, preprocessor, parameters) -> set[str]:
    """model_input's column set, derived from the rule build_model_input follows.

    identity ∪ {label} ∪ feature_columns ∪ (whatever else the keys carried in).
    Used as an equality so an *extra* column — a leaked join column, a carry
    column that should have been dropped — fails the assertion too; ``in`` /
    ``isdisjoint`` only ever catch missing ones.
    """
    schema = get_schema(parameters)
    return (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set(preprocessor["feature_columns"])
        | set(keys.columns)
    )


@pytest.fixture
def feature_table(spark):
    rows = []
    for snap in _SNAP_DATES:
        for cid in _ENTITIES:
            aum = 100.0 * (_entity_index(cid) + 1)
            rows.append({
                "snap_date": pd.Timestamp(snap),
                "cust_id": cid,
                "total_aum": aum,
                "fund_aum": aum / 10.0,
                "in_amt_sum_l1m": 5.0,
                "out_amt_sum_l1m": 3.0,
                "in_amt_ratio_l1m": 0.05,
                "out_amt_ratio_l1m": 0.03,
            })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def label_table(spark):
    rows = []
    for snap in _SNAP_DATES:
        snap_dt = pd.Timestamp(snap)
        for cid in _ENTITIES:
            for prod in _PRODUCTS:
                rows.append(
                    {
                        "snap_date": snap_dt,
                        "cust_id": cid,
                        "cust_segment_typ": _segment(cid),
                        "apply_start_date": snap_dt + pd.Timedelta(days=1),
                        "apply_end_date": snap_dt + pd.Timedelta(days=30),
                        "label": 1 if _is_positive(cid, prod) else 0,
                        "prod_name": prod,
                    }
                )
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def sample_pool(spark):
    rows = []
    for snap in _SNAP_DATES:
        snap_dt = pd.Timestamp(snap)
        for cid in _ENTITIES:
            for prod in _PRODUCTS:
                rows.append({
                    "snap_date": snap_dt,
                    "cust_id": cid,
                    "cust_segment_typ": _segment(cid),
                    "prod_name": prod,
                    "label": 1 if _is_positive(cid, prod) else 0,
                    "tenure_months": 12 * (_entity_index(cid) % 5 + 1),
                    "channel_preference": _channel(cid),
                })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "schema": {
            "categorical_values": {
                "prod_name": list(_PRODUCTS),
            },
        },
        "dataset": {
            "train_snap_dates": _SNAP_DATES[:3],
            "sample_ratio": 0.5,
            "sample_group_keys": ["cust_segment_typ", "prod_name"],
            "sample_ratio_overrides": {},
            "train_dev_ratio": 0.2,
            "enable_calibration": False,
            "calibration_snap_dates": [],
            "calibration_sample_ratio": 1.0,
            "val_snap_dates": _SNAP_DATES[3:4],
            "val_sample_ratio": 1.0,
            "test_snap_dates": _SNAP_DATES[4:5],
        },
    }


class TestSelectTrainKeys:
    def test_returns_correct_columns(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        assert sorted(result.columns) == _identity_columns(parameters)

    def test_filters_to_train_dates(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        pdf = result.toPandas()
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))
        excluded = val_dates | test_dates
        assert not pdf["snap_date"].isin(excluded).any()
        # All dates must be in train_snap_dates
        assert pdf["snap_date"].isin(train_dates).all()

    def test_full_ratio_returns_all(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        result = select_train_keys(sample_pool, params)
        assert result.count() == _expected_key_count(params, "train")

    def test_no_duplicates(self, sample_pool, parameters):
        result = select_train_keys(sample_pool, parameters)
        identity = get_schema(parameters)["identity_columns"]
        assert result.count() == result.dropDuplicates(identity).count()


class TestSplitTrainKeys:
    def test_both_sides_are_non_empty(self, sample_pool, parameters):
        """D21: the fixture is big enough that the split actually splits.

        Without this the other three tests in this class are vacuous — at the
        old 4-entity size the dev side was always empty, and replacing the whole
        function with ``return sample_keys, sample_keys.limit(0)`` kept them
        green. Asserted on distinct entities (not rows) because that is the unit
        the split is defined on.
        """
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        entity_col = get_schema(params)["entity"][0]
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        n_total = sample_keys.select(entity_col).distinct().count()
        n_dev = train_dev.select(entity_col).distinct().count()
        n_train = train.select(entity_col).distinct().count()

        assert n_total == len(_ENTITIES)
        assert 0 < n_dev < n_total
        assert n_train + n_dev == n_total

    def test_no_cust_overlap(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        train_custs = set(train.select("cust_id").distinct().toPandas()["cust_id"])
        dev_custs = set(train_dev.select("cust_id").distinct().toPandas()["cust_id"])
        assert len(train_custs & dev_custs) == 0

    def test_all_keys_preserved(self, sample_pool, parameters):
        """train_keys ∪ train_dev_keys must be exactly sample_keys, even after
        repartition (which is the production scenario that broke F.rand-based splitting)."""
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params).repartition(8)
        train, train_dev = split_train_keys(sample_keys, params)

        identity = ["snap_date", "cust_id", "prod_name"]
        sample_set = {tuple(r) for r in sample_keys.select(*identity).toPandas().itertuples(index=False)}
        train_set = {tuple(r) for r in train.select(*identity).toPandas().itertuples(index=False)}
        dev_set = {tuple(r) for r in train_dev.select(*identity).toPandas().itertuples(index=False)}

        assert train_set | dev_set == sample_set
        assert train_set & dev_set == set()

    def test_deterministic_across_runs(self, sample_pool, parameters):
        """Two independent invocations with the same seed must yield identical splits."""
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        sample_keys = select_train_keys(sample_pool, params)

        t1, d1 = split_train_keys(sample_keys, params)
        t2, d2 = split_train_keys(sample_keys, params)

        t1_custs = set(t1.select("cust_id").distinct().toPandas()["cust_id"])
        t2_custs = set(t2.select("cust_id").distinct().toPandas()["cust_id"])
        d1_custs = set(d1.select("cust_id").distinct().toPandas()["cust_id"])
        d2_custs = set(d2.select("cust_id").distinct().toPandas()["cust_id"])
        assert t1_custs == t2_custs
        assert d1_custs == d2_custs


class TestSplitTrainKeysEmptyTrainDev:
    """``train_dev_ratio > 0`` that yields an empty dev split must raise.

    train_dev is the early-stopping validation set handed to every HPO trial.
    Empty means early stopping never fires: each trial burns its full round
    budget and the search reports nothing unusual. See ADR-0005.
    """

    def _keys(self, spark):
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime([_SNAP_DATES[0]] * 2),
            "cust_id": _ENTITIES[:2],
            "prod_name": _PRODUCTS[:2],
        }))

    def test_empty_train_dev_raises(self, spark, parameters):
        # ratio_to_threshold(1e-7) rounds to a bucket threshold of 0, so the dev
        # side is empty by construction — the test does not depend on where two
        # particular cust_ids happen to hash.
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "train_dev_ratio": 1e-7},
        }
        with pytest.raises(ValueError, match="empty train-dev split") as ei:
            split_train_keys(self._keys(spark), params)
        msg = str(ei.value)
        # The message has to name both halves of the cause: a ratio alone does
        # not tell you whether to change the ratio or the sample size.
        assert "train_dev_ratio=1e-07" in msg
        assert "2 distinct cust_id" in msg

    def test_zero_ratio_permits_empty_train_dev(self, spark, parameters):
        """train_dev_ratio == 0 asks for no dev split; that is not the bug."""
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "train_dev_ratio": 0.0},
        }
        keys = self._keys(spark)
        train, train_dev = split_train_keys(keys, params)
        assert train_dev.count() == 0
        assert train.count() == keys.count()


class TestSelectValKeys:
    def test_full_population(self, sample_pool, parameters):
        result = select_val_keys(sample_pool, parameters)
        assert result.count() == _expected_key_count(parameters, "val")
        assert sorted(result.columns) == _identity_columns(parameters)


class TestSelectTestKeys:
    def test_full_population(self, sample_pool, parameters):
        result = select_test_keys(sample_pool, parameters)
        assert result.count() == _expected_key_count(parameters, "test")
        assert sorted(result.columns) == _identity_columns(parameters)


class TestBuildModelInput:
    def _train_keys(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        return select_train_keys(sample_pool, params)

    def _pft(self, feature_table, sample_pool, parameters):
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        return train_keys, preprocessor, pft

    def test_joins_with_product_keys(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """When keys include prod_name, join label_table on full identity key."""
        _, preprocessor, pft = self._pft(feature_table, sample_pool, parameters)

        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime([_SNAP_DATES[0]] * 2),
                "cust_id": _ENTITIES[:2],
                "prod_name": [_PRODUCTS[0]] * 2,
            })
        )
        result = build_model_input(keys, pft, label_table, preprocessor, parameters)
        # keys' grain is model_input's grain: no fan-out, no drop.
        assert result.count() == keys.count()
        assert set(result.columns) == _expected_model_input_columns(
            keys, preprocessor, parameters
        )

    def test_keys_missing_item_raises(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """keys without the item column is an error, not a mode (ADR-0005).

        It used to fall back to a base-key label join, which fanned each
        (snap_date, cust_id) out to one row per item in label_table and took
        prod_name's values from there — a model_input silently N times too
        large, with no error anywhere.
        """
        _, preprocessor, pft = self._pft(feature_table, sample_pool, parameters)
        item_col = get_schema(parameters)["item"]

        keys = spark.createDataFrame(
            pd.DataFrame({
                "snap_date": pd.to_datetime([_SNAP_DATES[0]] * 2),
                "cust_id": _ENTITIES[:2],
            })
        )
        # "build_model_input keys" (not bare "build_model_input") — the other
        # _validate_columns call in this function reports the latter, and this
        # pattern must not be satisfiable by it.
        with pytest.raises(
            ValueError, match=rf"build_model_input keys: \['{item_col}'\]"
        ):
            build_model_input(keys, pft, label_table, preprocessor, parameters)


class TestFitAndBuild:
    def _train_keys(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        return select_train_keys(sample_pool, params)

    def test_output_format(self, spark, feature_table, label_table, sample_pool, parameters):
        """Every split's model_input, built from the real key-selection nodes.

        val/test keys used to be hand-built as (snap_date, cust_id) — a shape no
        production node produces, and the only shape that exercised the
        base-key-join branch that ADR-0005 removed.
        """
        from pyspark.sql import DataFrame

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)

        splits = {
            "train": self._train_keys(sample_pool, parameters),
            "val": select_val_keys(sample_pool, parameters),
            "test": select_test_keys(sample_pool, parameters),
        }
        for split, keys in splits.items():
            mi = build_model_input(keys, pft, label_table, preprocessor, parameters)
            n_keys = keys.count()
            assert isinstance(mi, DataFrame)
            assert n_keys == _expected_key_count(parameters, split), split
            assert mi.count() == n_keys, split
            assert set(mi.columns) == _expected_model_input_columns(
                keys, preprocessor, parameters
            ), split

    def test_excludes_drop_columns(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        train_mi = build_model_input(train_keys, pft, label_table, preprocessor, parameters)

        forbidden = {"apply_start_date", "apply_end_date", "cust_segment_typ"}
        assert forbidden.isdisjoint(set(train_mi.columns))
        # Equality, so a *new* leaked column fails here too — `isdisjoint` only
        # ever catches the three names spelled out above.
        assert set(train_mi.columns) == _expected_model_input_columns(
            train_keys, preprocessor, parameters
        )
        assert train_mi.count() == train_keys.count()

    def test_prod_name_preserved_as_identity(
        self, spark, feature_table, label_table, sample_pool, parameters
    ):
        """prod_name is an identity column — encoding is deferred to training."""
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(feature_table, preprocessor, parameters)
        train_mi = build_model_input(train_keys, pft, label_table, preprocessor, parameters)
        train_pdf = train_mi.toPandas()

        assert train_mi.count() == train_keys.count()
        assert train_pdf["prod_name"].dtype == object


class TestFitPreprocessorMissingDates:
    def test_missing_train_snap_date_raises(self, spark, feature_table, parameters):
        # parameters has train_snap_dates including 2024-02-29; feature_table has it.
        # Override to require a date that's not in feature_table.
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-12-31"],
            },
        }
        with pytest.raises(ValueError, match="missing required train_snap_dates"):
            fit_preprocessor_metadata(feature_table, params)

    def test_error_lists_missing_dates(self, spark, feature_table, parameters):
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "train_snap_dates": ["2024-01-31", "2024-12-31", "2024-11-30"],
            },
        }
        with pytest.raises(ValueError) as exc_info:
            fit_preprocessor_metadata(feature_table, params)
        msg = str(exc_info.value)
        assert "2024-11-30" in msg
        assert "2024-12-31" in msg


class TestFitPreprocessorItemMissingFromFeatures:
    """schema.item must appear in feature_columns; otherwise X loses the item dimension and HPO mAP collapses to a constant."""

    def test_categorical_columns_missing_item_raises(
        self, spark, feature_table, parameters
    ):
        # Simulate the real-world yaml mistake: categorical_columns lists other columns but omits prod_name.
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "prepare_model_input": {
                    "drop_columns": [
                        "snap_date", "cust_id", "label",
                        "apply_start_date", "apply_end_date", "cust_segment_typ",
                    ],
                    "categorical_columns": [],  # intentionally omits prod_name (and other cats, to bypass existing checks)
                },
            },
        }
        with pytest.raises(DataConsistencyError, match="schema.item='prod_name' is missing") as ei:
            fit_preprocessor_metadata(feature_table, params)
        assert isinstance(ei.value, ValueError)  # subclass: existing callers unaffected

    def test_default_categorical_columns_passes(
        self, spark, feature_table, parameters
    ):
        # When prepare_model_input is not supplied, _get_preprocessing_config
        # defaults categorical_columns=[schema.item], so prod_name lands in
        # feature_columns automatically.
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        assert "prod_name" in preprocessor["feature_columns"]


class TestApplyPreprocessorFilter:
    def test_filters_out_dates_outside_dataset_set(
        self, spark, feature_table, parameters
    ):
        """Test A: when feature_table contains mid-week rows, the filter must drop them."""
        # Add a "mid-week" row that isn't in any split's snap_dates
        n = len(_ENTITIES)
        midweek_pdf = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-15"] * n),
            "cust_id": list(_ENTITIES),
            "total_aum": [999.0] * n,
            "fund_aum": [99.0] * n,
            "in_amt_sum_l1m": [9.0] * n,
            "out_amt_sum_l1m": [9.0] * n,
            "in_amt_ratio_l1m": [0.99] * n,
            "out_amt_ratio_l1m": [0.99] * n,
        })
        ft_with_midweek = feature_table.unionByName(spark.createDataFrame(midweek_pdf))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(ft_with_midweek, preprocessor, parameters)
        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        assert pd.Timestamp("2024-01-15") not in result_dates

    def test_missing_required_snap_date_raises(
        self, spark, feature_table, parameters
    ):
        """Test E2: feature_table missing any cal/val/test snap_date must raise."""
        # parameters val_snap_dates is 2024-04-30; remove 04-30 rows from feature_table
        ft_short = feature_table.filter(F.col("snap_date") != F.lit(pd.Timestamp("2024-04-30")))

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        with pytest.raises(ValueError, match="missing required snap_dates"):
            apply_preprocessor_to_features(ft_short, preprocessor, parameters)


class TestFitApplyFilterScopes:
    def test_apply_includes_all_splits_fit_only_train(
        self, spark, feature_table, parameters
    ):
        """Test B: fit sees train; apply sees train ∪ cal ∪ val ∪ test."""
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(feature_table, preprocessor, parameters)

        result_dates = {
            row.snap_date for row in result.select("snap_date").distinct().collect()
        }
        train_dates = set(pd.to_datetime(parameters["dataset"]["train_snap_dates"]))
        val_dates = set(pd.to_datetime(parameters["dataset"]["val_snap_dates"]))
        test_dates = set(pd.to_datetime(parameters["dataset"]["test_snap_dates"]))

        # Apply must cover all splits
        assert train_dates.issubset(result_dates)
        assert val_dates.issubset(result_dates)
        assert test_dates.issubset(result_dates)


def test_build_model_input_casts_float_features_to_float32(
    spark, label_table, parameters
):
    """Decimal AND Double feature columns must both be cast to float (float32)
    inside build_model_input.

    Invariant: model_input's numeric feature columns are stored as float32.

    LightGBM is histogram-based (max_bin=256) — float32's ~7-digit precision
    is far beyond the 8-bit binning resolution, so decimal128 and float64 are
    pure waste. decimal128 is the disaster case (~10x peak-memory blow-up
    via Python decimal.Decimal objects); float64 is the silent 2x case.
    We bake the smaller representation into model_input at write time.
    """
    from decimal import Decimal

    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("snap_date", T.TimestampType()),
        T.StructField("cust_id", T.StringType()),
        T.StructField("total_aum", T.DecimalType(38, 6)),
        T.StructField("fund_aum", T.DoubleType()),
        T.StructField("in_amt_sum_l1m", T.DecimalType(29, 0)),
        T.StructField("out_amt_sum_l1m", T.DoubleType()),
        T.StructField("in_amt_ratio_l1m", T.DoubleType()),
        T.StructField("out_amt_ratio_l1m", T.DoubleType()),
    ])
    rows = []
    for snap in _SNAP_DATES:
        snap_ts = pd.Timestamp(snap).to_pydatetime()
        for cid in _ENTITIES:
            rows.append((
                snap_ts, cid, Decimal(f"{100.0 * (_entity_index(cid) + 1)}"),
                10.0, Decimal("5"), 3.0, 0.05, 0.03,
            ))
    feature_table_decimal = spark.createDataFrame(rows, schema=schema)

    preprocessor, _ = fit_preprocessor_metadata(feature_table_decimal, parameters)
    pft = apply_preprocessor_to_features(feature_table_decimal, preprocessor, parameters)

    # Build train keys directly (cust × prod × train_snap_dates) — independent of
    # sample_pool fixture; the test is about dtype, not sampling.
    train_dates = parameters["dataset"]["train_snap_dates"]
    train_keys = spark.createDataFrame(
        pd.DataFrame({
            "snap_date": pd.to_datetime(
                [d for d in train_dates for _ in _ENTITIES]
            ),
            "cust_id": _ENTITIES * len(train_dates),
            "prod_name": [_PRODUCTS[0]] * (len(_ENTITIES) * len(train_dates)),
        })
    )

    result = build_model_input(train_keys, pft, label_table, preprocessor, parameters)
    assert result.count() == train_keys.count()

    feature_cols = preprocessor["feature_columns"]
    out_dtypes = dict(result.dtypes)

    # No decimal nor double feature columns remain — every float-like feature
    # is float32.
    leftover = [
        c for c in feature_cols
        if "decimal" in out_dtypes[c] or out_dtypes[c] == "double"
    ]
    assert leftover == [], (
        f"feature_columns still contain decimal/double types: {leftover}"
    )

    # DecimalType inputs are now float.
    assert out_dtypes["total_aum"] == "float"
    assert out_dtypes["in_amt_sum_l1m"] == "float"
    # DoubleType inputs are also now float (this PR's addition).
    assert out_dtypes["fund_aum"] == "float"
    assert out_dtypes["out_amt_sum_l1m"] == "float"
    assert out_dtypes["in_amt_ratio_l1m"] == "float"
    assert out_dtypes["out_amt_ratio_l1m"] == "float"


class TestValidateDataConsistency:
    def test_consistent_fixtures_return_none(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # fixtures: prod_name in {exchange_fx,exchange_usd,fund_stock} ==
        # schema.categorical_values.prod_name; all snaps inside windows.
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, parameters) is None

    def test_undeclared_value_raises(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # Shrink declared set so fund_stock (present in data) is undeclared.
        params = {
            **parameters,
            "schema": {
                **parameters["schema"],
                "categorical_values": {"prod_name": ["exchange_fx", "exchange_usd"]},
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "fund_stock" in msg
        assert "sample_pool" in msg

    def test_declared_value_absent_from_sample_pool_raises(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # 'ploan' is declared but never appears in sample_pool/label data ->
        # sp_missing direction (D3 second direction). declared-label is B3,
        # deferred, so the only error is the sample_pool "never produces" one.
        params = {
            **parameters,
            "schema": {
                **parameters["schema"],
                "categorical_values": {
                    "prod_name": [
                        "exchange_fx", "exchange_usd", "fund_stock", "ploan",
                    ]
                },
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "ploan" in msg
        assert "never produces" in msg

    def test_value_only_in_non_window_snap_is_ignored(
        self, spark, sample_pool, label_table, feature_table, parameters
    ):
        # 2024-12-31 is outside collect_dataset_snap_dates (train Jan-Mar,
        # val Apr, test May). An undeclared 'ploan' there must be filtered out.
        extra = spark.createDataFrame(
            pd.DataFrame([{
                "snap_date": pd.Timestamp("2024-12-31"),
                "cust_id": "C001",
                "cust_segment_typ": "mass",
                "prod_name": "ploan",
                "label": 0,
                "tenure_months": 12,
                "channel_preference": "digital",
            }])
        )
        sp = sample_pool.unionByName(extra)
        assert validate_data_consistency(sp, label_table, feature_table, parameters) is None

    def test_decimal_categorical_in_feature_table_raises(
        self, spark, sample_pool, label_table, parameters
    ):
        # B5: a column declared in categorical_columns is DecimalType in
        # feature_table -> fail fast at the gate (instead of the opaque
        # JSON-serialization crash 141s into fit_preprocessor_metadata).
        from decimal import Decimal

        from pyspark.sql import types as T

        ft_schema = T.StructType([
            T.StructField("snap_date", T.TimestampType()),
            T.StructField("cust_id", T.StringType()),
            T.StructField("industry_code", T.DecimalType(15, 0)),
        ])
        feature_table = spark.createDataFrame(
            [(pd.Timestamp("2024-01-31").to_pydatetime(), "C001", Decimal("1001"))],
            schema=ft_schema,
        )
        params = {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "prepare_model_input": {
                    "categorical_columns": ["prod_name", "industry_code"],
                },
            },
        }
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, feature_table, params)
        msg = str(ei.value)
        assert "industry_code" in msg
        assert "decimal" in msg

    def test_clean_feature_table_categoricals_pass(
        self, sample_pool, label_table, feature_table, parameters
    ):
        # feature_table fixture has only numeric (non-categorical) columns and
        # the lone declared categorical (prod_name) is an identity column absent
        # from feature_table -> B5 finds nothing, B1 is clean -> None.
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, parameters) is None


class TestSplitTrainKeysCarry:
    def test_carry_column_survives_split(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6], "prod_name": ["a"] * 6,
            "cust_segment_typ": ["mass", "hnw", "mass", "aff", "mass", "hnw"]}))
        params = {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
            "dataset": {"train_dev_ratio": 0.3}, "random_seed": 42}
        tr, dv = split_train_keys(keys, params)
        assert "cust_segment_typ" in tr.columns
        assert "cust_segment_typ" in dv.columns


class TestApplyPreprocessorUnknownWarning:
    """apply_preprocessor_to_features 對每個含 unknown(-1) 的編碼欄各發一次 WARNING。
    鎖住此可觀察行為，確保 multi-count -> single-aggregation 的重構等價。"""

    def test_warns_per_column_with_unknown_categoricals(self, spark, caplog):
        import logging

        # feature_table 帶一個「非 identity」的類別特徵欄，資料含 fit 時沒見過的值
        # -> 編碼為 -1。identity_columns = [snap_date, cust_id, prod_name]，故
        # channel_preference 會進 encode_cols。
        ft = spark.createDataFrame(
            pd.DataFrame(
                {
                    "snap_date": pd.to_datetime(["2024-01-31"] * 3),
                    "cust_id": ["C001", "C002", "C003"],
                    "total_aum": [100.0, 200.0, 300.0],
                    "channel_preference": ["digital", "branch", "unseen_channel"],
                }
            )
        )
        preprocessor = {
            "feature_columns": ["channel_preference", "total_aum"],
            "categorical_columns": ["channel_preference"],
            # "unseen_channel" 刻意不在 mapping -> 編碼為 -1
            "category_mappings": {"channel_preference": ["digital", "branch"]},
            "drop_columns": [],
        }
        parameters = {
            "schema": {"categorical_values": {"prod_name": ["exchange_fx"]}},
            "dataset": {
                "train_snap_dates": ["2024-01-31"],
                "calibration_snap_dates": [],
                "val_snap_dates": [],
                "test_snap_dates": [],
            },
        }

        with caplog.at_level(logging.WARNING):
            result = apply_preprocessor_to_features(ft, preprocessor, parameters)
            pdf = result.orderBy("cust_id").toPandas()

        # digital->0, branch->1, unseen_channel->-1
        assert pdf["channel_preference"].tolist() == [0, 1, -1]
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "1 unknowns in column 'channel_preference'" in m for m in warnings
        ), warnings


class TestValidateDataConsistencyB6:
    def test_unencoded_string_feature_raises(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 注入一個未宣告 categorical、也未 drop 的字串特徵欄
        rogue = feature_table.withColumn("rogue_str", F.lit("free_text"))
        with pytest.raises(DataConsistencyError, match="rogue_str"):
            validate_data_consistency(sample_pool, label_table, rogue, parameters)

    def test_clean_feature_table_passes(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 乾淨 feature_table（既有 fixture 特徵欄全數值）→ 不 raise
        assert (
            validate_data_consistency(sample_pool, label_table, feature_table, parameters)
            is None
        )

    def test_boolean_feature_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 布林特徵欄是數值（bool→numeric），B6 不得誤報
        with_bool = feature_table.withColumn("flag_bool", F.lit(True))
        assert (
            validate_data_consistency(sample_pool, label_table, with_bool, parameters)
            is None
        )


# --- ADR-0002: the test branch only processes months that have not landed ---

from recsys_tfb.pipelines.dataset.nodes_shared import (
    EXISTING_SNAP_DATES_KEY,
    REBUILD_SNAP_DATES_KEY,
)
from recsys_tfb.pipelines.dataset.nodes_spark import (
    build_test_model_input,
    filter_test_model_input,
)

_TEST_MONTHS = _SNAP_DATES[3:5]


def _incremental_params(parameters, existing=None, rebuild=None):
    """parameters with two test months and an injected partition listing."""
    params = {
        **parameters,
        "dataset": {
            **parameters["dataset"],
            "val_snap_dates": [],          # keep splits disjoint
            "test_snap_dates": list(_TEST_MONTHS),
        },
    }
    if existing is not None:
        params[EXISTING_SNAP_DATES_KEY] = existing
    if rebuild is not None:
        params[REBUILD_SNAP_DATES_KEY] = rebuild
    return params


def _months(df):
    # snap_date is a timestamp when built from pandas, a string when it came
    # back through a Hive partition column — normalise both.
    return sorted(
        str(pd.Timestamp(d).date())
        for d in df.select("snap_date").distinct().toPandas()["snap_date"]
    )


class TestSelectTestKeysIncremental:
    def test_processes_only_the_month_that_has_not_landed(self, label_table, parameters):
        params = _incremental_params(
            parameters, existing={"test_keys": ["2024-04-30"]},
        )
        assert _months(select_test_keys(label_table, params)) == ["2024-05-31"]

    def test_all_months_landed_yields_nothing(self, label_table, parameters):
        params = _incremental_params(
            parameters, existing={"test_keys": list(_TEST_MONTHS)},
        )
        assert select_test_keys(label_table, params).count() == 0

    def test_rebuild_reprocesses_a_landed_month(self, label_table, parameters):
        params = _incremental_params(
            parameters,
            existing={"test_keys": list(_TEST_MONTHS)},
            rebuild=["2024-04-30"],
        )
        assert _months(select_test_keys(label_table, params)) == ["2024-04-30"]

    def test_no_injected_listing_processes_everything(self, label_table, parameters):
        # Backwards-compatible default: driven outside the CLI, nothing is known
        # to exist, so every configured month is processed.
        params = _incremental_params(parameters)
        assert _months(select_test_keys(label_table, params)) == _TEST_MONTHS

    def test_reads_its_own_dataset_key_not_another(self, label_table, parameters):
        # A listing for a *different* dataset must not silence this node — each
        # node is gated on the partitions of the artifact it produces.
        params = _incremental_params(
            parameters, existing={"test_model_input": list(_TEST_MONTHS)},
        )
        assert _months(select_test_keys(label_table, params)) == _TEST_MONTHS


class TestBuildTestModelInputIncremental:
    def test_filters_keys_that_already_landed(
        self, feature_table, label_table, parameters
    ):
        # test_keys is a persistent Hive table: reading it back yields every
        # month, including ones select_test_keys did not rewrite. This node must
        # re-apply the diff itself rather than trust its input.
        params = _incremental_params(
            parameters, existing={"test_model_input": ["2024-04-30"]},
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(feature_table, preprocessor, params)
        all_month_keys = label_table.select(
            "snap_date", "cust_id", "prod_name"
        ).dropDuplicates()

        result = build_test_model_input(
            all_month_keys, encoded, label_table, preprocessor, params,
        )
        assert _months(result) == ["2024-05-31"]

    def test_rebuild_reprocesses_a_landed_month(
        self, feature_table, label_table, parameters
    ):
        params = _incremental_params(
            parameters,
            existing={"test_model_input": list(_TEST_MONTHS)},
            rebuild=["2024-05-31"],
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(feature_table, preprocessor, params)
        all_month_keys = label_table.select(
            "snap_date", "cust_id", "prod_name"
        ).dropDuplicates()

        result = build_test_model_input(
            all_month_keys, encoded, label_table, preprocessor, params,
        )
        assert _months(result) == ["2024-05-31"]


class TestFilterTestModelInputIncremental:
    def test_filters_months_that_already_landed(
        self, feature_table, label_table, parameters
    ):
        params = _incremental_params(
            parameters, existing={"test_model_input": ["2024-04-30"]},
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(feature_table, preprocessor, params)
        all_month_keys = label_table.select(
            "snap_date", "cust_id", "prod_name"
        ).dropDuplicates()
        # Feed it an unfiltered frame on purpose (the sliced-resume scenario).
        unfiltered = build_test_model_input(
            all_month_keys, encoded, label_table, preprocessor,
            _incremental_params(parameters),
        )
        assert _months(unfiltered) == _TEST_MONTHS

        result = filter_test_model_input(unfiltered, params)
        assert _months(result) == ["2024-05-31"]


class TestApplyPreprocessorIncremental:
    def test_skips_months_that_already_landed(self, feature_table, parameters):
        params = _incremental_params(
            parameters,
            existing={"preprocessed_feature_table": ["2024-01-31", "2024-04-30"]},
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        result = apply_preprocessor_to_features(feature_table, preprocessor, params)
        # configured union = train(01,02,03) + test(04,05); 01 and 04 landed.
        assert _months(result) == ["2024-02-29", "2024-03-31", "2024-05-31"]

    def test_all_months_landed_yields_an_empty_but_typed_frame(
        self, feature_table, parameters
    ):
        params = _incremental_params(parameters)
        full = apply_preprocessor_to_features(
            feature_table, fit_preprocessor_metadata(feature_table, params)[0], params,
        )
        landed = {"preprocessed_feature_table": list(_SNAP_DATES)}
        params_all_landed = _incremental_params(parameters, existing=landed)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params_all_landed)
        result = apply_preprocessor_to_features(
            feature_table, preprocessor, params_all_landed,
        )
        assert result.count() == 0
        # Schema must survive the empty case: an empty write with a drifted
        # schema is how a dynamic-partition overwrite corrupts a live table.
        assert result.dtypes == full.dtypes

    def test_missing_feature_table_month_is_only_checked_when_processed(
        self, feature_table, parameters
    ):
        # A month absent from feature_table but already landed must NOT raise —
        # this run does not read it.
        params = _incremental_params(parameters)
        params["dataset"] = {
            **params["dataset"],
            "test_snap_dates": ["2024-04-30", "2024-05-31", "2024-12-31"],
        }
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)

        with pytest.raises(ValueError, match="2024-12-31"):
            apply_preprocessor_to_features(feature_table, preprocessor, params)

        params_landed = {
            **params,
            EXISTING_SNAP_DATES_KEY: {"preprocessed_feature_table": ["2024-12-31"]},
        }
        apply_preprocessor_to_features(feature_table, preprocessor, params_landed)


class TestIncrementalFilterHandlesHiveStringDates:
    """snap_date comes back from Hive as a STRING partition column.

    The runner reloads every node input through the catalog, so
    build/filter_test_model_input see ``snap_date: string``, not the timestamp
    dtype a pandas-built fixture produces. Comparing that string column against
    ``pd.Timestamp`` literals matches zero rows and raises nothing — the frame
    silently empties and the Hive write touches no partitions. Caught by a
    real run, not by the pandas-typed fixtures above.
    """

    @pytest.fixture
    def string_dated_keys(self, spark, label_table):
        # Mirror HiveTableDataset.load: snap_date arrives as 'YYYY-MM-DD'.
        return (
            label_table.select("snap_date", "cust_id", "prod_name")
            .dropDuplicates()
            .withColumn("snap_date", F.date_format("snap_date", "yyyy-MM-dd"))
        )

    def test_string_snap_date_still_matches(
        self, spark, feature_table, label_table, string_dated_keys, parameters
    ):
        assert dict(string_dated_keys.dtypes)["snap_date"] == "string"
        params = _incremental_params(
            parameters, existing={"test_model_input": ["2024-04-30"]},
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(feature_table, preprocessor, params)

        result = build_test_model_input(
            string_dated_keys, encoded, label_table, preprocessor, params,
        )
        assert result.count() > 0, "string-typed snap_date silently matched nothing"
        assert _months(result) == ["2024-05-31"]

    def test_filter_node_also_handles_string_snap_date(
        self, spark, feature_table, label_table, string_dated_keys, parameters
    ):
        params = _incremental_params(
            parameters, existing={"test_model_input": ["2024-04-30"]},
        )
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(feature_table, preprocessor, params)
        unfiltered = build_test_model_input(
            string_dated_keys, encoded, label_table, preprocessor,
            _incremental_params(parameters),
        ).withColumn("snap_date", F.date_format("snap_date", "yyyy-MM-dd"))

        result = filter_test_model_input(unfiltered, params)
        assert result.count() > 0, "string-typed snap_date silently matched nothing"
        assert _months(result) == ["2024-05-31"]

    def test_select_test_keys_still_works_on_date_typed_source(
        self, label_table, parameters
    ):
        # The other direction: sample_pool/label_table keep a real date dtype.
        # Normalising to DATE must not break that path.
        assert dict(label_table.dtypes)["snap_date"].startswith("timestamp")
        params = _incremental_params(
            parameters, existing={"test_keys": ["2024-04-30"]},
        )
        assert _months(select_test_keys(label_table, params)) == ["2024-05-31"]

"""Tests for the dataset pipeline's node functions, Layer-2 gate included."""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_entity_grouping, get_schema
from recsys_tfb.pipelines.dataset.month_plans import build_month_plans
from recsys_tfb.pipelines.dataset.nodes import (
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
# size of 4, train_dev_ratio=0.2 rounded the dev side to empty, and every
# assertion in TestSplitTrainKeys held for a `split` that did nothing. See
# TestSplitTrainKeys.test_both_sides_are_non_empty for how that was verified.
# Item count stays at 3 and is deliberately NOT aligned with the 8 in conf/ —
# that gap is orthogonal to this one (see #140).
_ENTITIES = [f"C{i:03d}" for i in range(1, 25)]
_PRODUCTS = ["exchange_fx", "exchange_usd", "fund_stock"]
_SNAP_DATES = ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]

# Per-entity attribute values, cycled so any fixture size stays covered. These
# are inert with respect to every assertion below (they are neither identity nor
# feature columns); they exist so the frames look like the real tables.
_SEGMENTS = ["mass", "affluent", "hnw"]
_CHANNELS = ["digital", "branch", "both"]


# The month plan an incremental node would receive from the CLI. Omitting
# ``existing`` means "nothing has landed", i.e. process every configured month —
# what every test outside the incremental block below is asserting against.
def _encode_plan(parameters, existing=None):
    return build_month_plans(parameters, existing=existing)[
        "preprocessed_feature_table"
    ]


def _test_keys_plan(parameters, existing=None):
    return build_month_plans(parameters, existing=existing)["test_keys"]


def _entity_index(cid: str) -> int:
    return _ENTITIES.index(cid)


def _segment(cid: str) -> str:
    return _SEGMENTS[_entity_index(cid) % len(_SEGMENTS)]


def _channel(cid: str) -> str:
    return _CHANNELS[_entity_index(cid) % len(_CHANNELS)]


# The one entity with no positive on any product. Keeps a zero-positive query
# group in the fixture (so the group filter still has something to drop) while
# every other entity carries one.
_ALL_NEGATIVE_ENTITY = _ENTITIES[-1]


def _is_positive(cid: str, prod: str) -> bool:
    """Positive on the first product for every entity but one.

    Not concentrated on a single entity, because the train / train-dev split
    partitions **by entity**: one positive entity lands wholly on one side and
    leaves the other with an all-zero label column — the state
    ``TestModelInputSchemaPerSplit.test_positive_rate_is_strictly_inside_its_bounds``
    exists to reject, and train_dev is the worst place to tolerate it since it
    is every HPO trial's early-stopping set (ADR-0005 §2). Verified by narrowing
    this back to ``_ENTITIES[0]``: train_dev then holds 0 positives in 18 rows.
    Spreading it over every *third* entity was not enough either — train_dev's
    three entities happened to miss all of them, and "which entities land in
    dev" is a hash detail no assertion should depend on.

    Two properties this shape guarantees for any split, whatever the hash does:

    - at most one positive per query group, so no group is all-positive;
    - at least one positive in any split holding two or more entities.
    """
    return cid != _ALL_NEGATIVE_ENTITY and prod == _PRODUCTS[0]


# --- derivations used by assertions ------------------------------------------


def _n_items(parameters) -> int:
    """Declared item count, read from the schema the node under test also reads.

    The fixture builds its items from the same list `parameters` declares, so
    this is one list expressed once rather than a second constant to keep in
    sync — not an independent oracle.
    """
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
    Used as an equality so an *extra* column — a join column leaking through
    from label_table — fails the assertion too; ``in`` / ``isdisjoint`` only
    ever catch missing ones.

    Three limits worth knowing before trusting a green:

    - Every keys frame in this module is exactly identity_columns, so the
      ``keys.columns`` term adds nothing here. The carry branch of the rule is
      covered in tests/test_pipelines/test_dataset/test_model_input.py instead.
    - That term mirrors what the code does (carry = anything in keys that is
      not identity/feature/label), not what ADR-0004 states the rule to be
      (``carry_columns ∩ keys``). So it cannot catch a key column that gets
      carried but should not have been — the implementation never consults
      ``carry_columns``. Reconciling the two is #140 D4, not this ticket.
    - ``feature_columns`` comes from the preprocessor, so this checks that
      build_model_input honours the metadata handed to it — not that the
      metadata is correct. Whether compute_feature_columns selected the right
      columns is a separate contract (#140 D3) with its own tests; drop a
      feature there and this equality follows it down without complaining.
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

        Without this the other three tests in this class are vacuous. Verified
        by reconstructing the pre-#141 state — 4 entities, no empty-dev guard,
        and the body replaced by ``return sample_keys, sample_keys.limit(0)``:
        the other three stayed green for a split that did nothing. (Under the
        guard added in this same change that mutation now raises instead, so the
        demonstration only reproduces with the guard removed too.)

        Asserted on distinct entities, not rows, because that is the unit the
        split is defined on -- and an entity is *every* column of
        ``schema.entity``, not its first one. Counting the first column alone
        agrees with this only while the schema declares a single column; under
        a two-column entity it would count something coarser than the thing
        being split, and the assertions below would stop meaning what they say
        without any of them going red (S4).
        """
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        entity_cols = get_schema(params)["entity"]
        sample_keys = select_train_keys(sample_pool, params)
        train, train_dev = split_train_keys(sample_keys, params)

        n_total = sample_keys.select(*entity_cols).distinct().count()
        n_dev = train_dev.select(*entity_cols).distinct().count()
        n_train = train.select(*entity_cols).distinct().count()

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
        keys = self._keys(spark)
        # The unit the node splits on: what the user declared, else the whole
        # entity (``get_entity_grouping``) -- the same call the node makes, so
        # this stays true if the split unit is ever declared coarser. Reading
        # the first entity column instead would silently diverge from the node
        # under a multi-column entity (S4).
        split_cols = get_entity_grouping(params, "train_split_keys")
        n_entities = keys.select(*split_cols).distinct().count()

        with pytest.raises(ValueError, match="empty train-dev split") as ei:
            split_train_keys(keys, params)
        msg = str(ei.value)
        # The message has to name both halves of the cause: a ratio alone does
        # not tell you whether to change the ratio or the sample size.
        assert "train_dev_ratio=1e-07" in msg
        assert f"{n_entities} distinct {', '.join(split_cols)}" in msg

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

    def test_negative_ratio_raises(self, spark, parameters):
        """A negative ratio reaches the same silent state by a different route.

        ratio_to_threshold(-0.1) is negative, so `_bucket < threshold` is empty
        and `_bucket >= threshold` takes everything — dev is empty exactly as in
        the too-small-ratio case. A `> 0` guard would wave it through, so one
        stray minus sign would resurrect the whole failure mode.
        """
        params = {
            **parameters,
            "dataset": {**parameters["dataset"], "train_dev_ratio": -0.1},
        }
        with pytest.raises(ValueError, match="empty train-dev split") as ei:
            split_train_keys(self._keys(spark), params)
        assert "train_dev_ratio=-0.1" in str(ei.value)

    def test_empty_sample_keys_blames_upstream(self, spark, parameters):
        """Empty *input* is a different fault, and must not be misreported.

        With no keys at all both sides are empty, so "puts every entity on the
        train side" would be false and "raise train_dev_ratio" would be useless
        advice — no ratio produces a non-empty split from nothing.
        """
        empty = self._keys(spark).limit(0)
        with pytest.raises(ValueError, match="no sampled keys at all") as ei:
            split_train_keys(empty, parameters)
        msg = str(ei.value)
        assert "sample_ratio" in msg
        assert "puts every entity on the train side" not in msg


class TestSelectValKeys:
    def test_full_population(self, sample_pool, parameters):
        result = select_val_keys(sample_pool, parameters)
        assert result.count() == _expected_key_count(parameters, "val")
        assert sorted(result.columns) == _identity_columns(parameters)


class TestSelectTestKeys:
    def test_full_population(self, sample_pool, parameters):
        result = select_test_keys(sample_pool, _test_keys_plan(parameters), parameters)
        assert result.count() == _expected_key_count(parameters, "test")
        assert sorted(result.columns) == _identity_columns(parameters)


class TestBuildModelInput:
    def _train_keys(self, sample_pool, parameters):
        params = {**parameters, "dataset": {**parameters["dataset"], "sample_ratio": 1.0}}
        return select_train_keys(sample_pool, params)

    def _pft(self, feature_table, sample_pool, parameters):
        train_keys = self._train_keys(sample_pool, parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(parameters), parameters,
        )
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
        # keys' grain is model_input's grain. Note this catches fan-out only:
        # label_table covers every key in these fixtures, so it cannot tell a
        # LEFT join from an INNER one. Pinning the join side needs a key with no
        # label row (#140 D10/D14), which is not this ticket.
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
        # require_columns_present call in this function reports the latter, and this
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
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(parameters), parameters,
        )

        splits = {
            "train": self._train_keys(sample_pool, parameters),
            "val": select_val_keys(sample_pool, parameters),
            "test": select_test_keys(sample_pool, _test_keys_plan(parameters), parameters),
        }
        for split, keys in splits.items():
            mi = build_model_input(keys, pft, label_table, preprocessor, parameters)
            n_keys = keys.count()
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
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(parameters), parameters,
        )
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
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(parameters), parameters,
        )
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
        # When prepare_model_input is not supplied, prepare_model_input_config
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
        result = apply_preprocessor_to_features(
            ft_with_midweek, preprocessor, _encode_plan(parameters), parameters,
        )
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
            apply_preprocessor_to_features(
                ft_short, preprocessor, _encode_plan(parameters), parameters,
            )


class TestFitApplyFilterScopes:
    def test_apply_includes_all_splits_fit_only_train(
        self, spark, feature_table, parameters
    ):
        """Test B: fit sees train; apply sees train ∪ cal ∪ val ∪ test."""
        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        result = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(parameters), parameters,
        )

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
    pft = apply_preprocessor_to_features(
        feature_table_decimal, preprocessor, _encode_plan(parameters), parameters,
    )

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


# --- where Layer-2 gate tests live -------------------------------------------
# `validate_data_consistency` is a node like any other, so it lives in
# `pipelines/dataset/nodes.py` (#169). Its integration tests stay here,
# next to the existing B1/B5/B6/B7 ones and the fixtures they all need. The pure
# helpers it calls (`compute_feature_columns`) are tested in
# test_feature_columns.py. Split by "gate wiring vs pure helper", not by which
# file the symbol happens to live in, so a behaviour still has exactly one home.


def _gate_params(parameters, *, drop_extra=(), categorical_extra=(), carry=None) -> dict:
    """``parameters`` with the keys the Layer-2 gate reads made explicit.

    The fixture leaves ``prepare_model_input`` unset, so the gate falls back to
    the defaults in ``pipelines/dataset/steps/feature_columns.py``. Those are read from
    that module rather than restated here: a hand-written copy would be equal to
    the real list only as long as feature_table happens to lack the columns
    where the two differ (``apply_start_date`` / ``apply_end_date`` /
    ``cust_segment_typ``), and would then silently stop dropping them the day a
    fixture grows one — turning every count assertion below into a different
    question without turning anything red.

    Callers add to the defaults rather than replacing them, so a test says which
    columns *it* cares about and inherits the rest.
    """
    from recsys_tfb.pipelines.dataset.steps.feature_columns import prepare_model_input_config

    default_drop, default_categorical = prepare_model_input_config(parameters)
    dataset = {
        **parameters["dataset"],
        "prepare_model_input": {
            "drop_columns": [*default_drop, *drop_extra],
            "categorical_columns": [*default_categorical, *categorical_extra],
        },
    }
    if carry is not None:
        dataset["carry_columns"] = list(carry)
    return {**parameters, "dataset": dataset}


class TestValidateDataConsistency:
    def test_clean_fixture_is_not_flagged_by_any_gate(
        self, sample_pool, label_table, feature_table, parameters
    ):
        """The gate must not misfire on a fixture that violates nothing.

        Covers B1/B5/B6 — it replaces three verbatim-identical ``is None``
        assertions that were filed under three different invariants and so
        promised a discrimination none of them had. Each invariant's raise side
        is covered by its own test below; this one only says "no false alarm".

        **Not** a B7 guard: the fixture configures no ``carry_columns``, so B7's
        input here is empty and unflagging it proves nothing. B7's false-positive
        side is covered by
        ``TestValidateDataConsistencyB7.test_carry_column_absent_from_feature_table_is_not_flagged``.

        Fixture properties it rests on: prod_name values ==
        schema.categorical_values.prod_name and every snap inside the configured
        windows (B1); feature_table's feature columns are all numeric and the
        lone declared categorical (prod_name) is an identity column absent from
        feature_table (B5/B6).
        """
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
        # This frame has to keep both sides non-empty or the empty-dev guard
        # fires and the test dies for a reason unrelated to carry columns —
        # a dependency on where six particular ids hash. Asserted rather than
        # left in a comment, so resizing the frame reports the problem instead
        # of relying on someone reading a note first.
        assert dv.count() > 0
        assert tr.count() > 0
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
            result = apply_preprocessor_to_features(
                ft, preprocessor, _encode_plan(parameters), parameters,
            )
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

    def test_boolean_feature_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        # 布林特徵欄是數值（bool→numeric），B6 不得誤報
        with_bool = feature_table.withColumn("flag_bool", F.lit(True))
        assert (
            validate_data_consistency(sample_pool, label_table, with_bool, parameters)
            is None
        )

    def test_declared_categorical_string_feature_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """B6 must be told which columns get encoded downstream.

        A string feature column that *is* declared categorical becomes an
        integer at encode time, so it is not the object-dtype footgun B6 guards
        — the gate has to hand the declared set to the predicate for that to
        hold. The rogue column is here so the assertion is "raised, and named
        only the rogue one" rather than "did not raise": a silently dead gate
        fails this test instead of passing it.
        """
        ft = (
            feature_table
            .withColumn("channel_preference", F.lit("digital"))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _gate_params(parameters, categorical_extra=["channel_preference"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "rogue_str" in msg
        assert "channel_preference" not in msg

    def test_dropped_string_column_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """B6 looks at *prospective feature* columns, not raw feature_table ones.

        A dropped column never reaches the model, so flagging it would be a
        false alarm that no valid config could clear — the gate has to classify
        the ``compute_feature_columns`` output. Same rogue-column anchor as
        above so a dead gate cannot pass.
        """
        ft = (
            feature_table
            .withColumn("legacy_note", F.lit("free_text"))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _gate_params(parameters, drop_extra=["legacy_note"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "rogue_str" in msg
        assert "legacy_note" not in msg


class TestValidateDataConsistencyB7:
    """B7 — a carry column that also lives in feature_table must be dropped."""

    def test_undropped_carry_column_raises(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """Numeric on purpose: this is the case only B7 catches.

        B6 already rejects a *string* carry column that was left out of
        drop_columns (as an un-encoded object feature), so a string here would
        not prove B7 is wired — the raise could come from B6. A numeric one
        sails past B6 straight into ``Reference 'x' is ambiguous`` at
        build_model_input, which is the crash B7 exists to pre-empt.
        ``acct_age_months`` is the negative control: same collision, correctly
        dropped, must not be named.
        """
        ft = (
            feature_table
            .withColumn("tenure_months", F.lit(12))
            .withColumn("acct_age_months", F.lit(24))
        )
        params = _gate_params(
            parameters,
            drop_extra=["acct_age_months"],
            carry=["tenure_months", "acct_age_months"],
        )
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "1 issue(s)" in msg
        assert "tenure_months" in msg
        # "carry_columns" appears in no other message this gate can emit, so it
        # pins the raise to B7 rather than to whichever rule fires first.
        assert "carry_columns" in msg
        assert "acct_age_months" not in msg

    def test_carry_column_absent_from_feature_table_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """The ordinary case: carry columns usually live only in sample_pool.

        ``channel_preference`` is a sample_pool column and not a feature_table
        one, so there is nothing to be ambiguous with. It is deliberately not
        ``cust_segment_typ`` — that one is in the default drop_columns, so an
        unflagged result there would be explained by the drop just as well as by
        the absence, and removing the feature_table check would not turn it red.
        """
        assert "channel_preference" not in feature_table.columns
        params = _gate_params(parameters, carry=["channel_preference"])
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, params) is None

    def test_identity_column_in_carry_is_not_flagged(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """The gate has to tell B7 which columns are identity.

        ``cust_id`` is in feature_table and not in drop_columns, so the naive
        rule flags it — but the key-selecting nodes never copy an identity column a
        second time, and running the real ``build_model_input`` this way
        completes normally. Flagging it would demand a drop_columns edit that
        changes no behaviour while busting base_dataset_version.
        """
        assert "cust_id" in feature_table.columns
        params = _gate_params(parameters, carry=["cust_id"])
        params["dataset"]["prepare_model_input"]["drop_columns"] = [
            c for c in params["dataset"]["prepare_model_input"]["drop_columns"]
            if c != "cust_id"
        ]
        assert validate_data_consistency(
            sample_pool, label_table, feature_table, params) is None


class TestValidateDataConsistencyCollectAll:
    def test_two_unrelated_violations_raise_once_naming_both(
        self, spark, feature_table, sample_pool, label_table, parameters
    ):
        """One raise listing every violation, so one fix pass clears them all.

        Two different invariants on purpose (B7 numeric collision + B6 rogue
        string): a gate that stopped at the first non-empty error list would
        still report one of them, and the count in the header is what separates
        that from reporting both.
        """
        ft = (
            feature_table
            .withColumn("tenure_months", F.lit(12))
            .withColumn("rogue_str", F.lit("free_text"))
        )
        params = _gate_params(parameters, carry=["tenure_months"])
        with pytest.raises(DataConsistencyError) as ei:
            validate_data_consistency(sample_pool, label_table, ft, params)
        msg = str(ei.value)
        assert "2 issue(s)" in msg
        assert "tenure_months" in msg
        assert "rogue_str" in msg


# --- ADR-0002 / ADR-0007: the test branch processes only the months it is
# handed. The plan arrives as a node argument, so a test states the months
# directly instead of assembling the CLI's partition listing; which listing
# produces which plan is asserted without Spark in test_month_plans.py. ---

from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan
from recsys_tfb.pipelines.dataset.nodes import build_test_model_input

_TEST_MONTHS = _SNAP_DATES[3:5]


def _incremental_params(parameters):
    """parameters whose test split is the last two fixture months."""
    return {
        **parameters,
        "dataset": {
            **parameters["dataset"],
            "val_snap_dates": [],          # keep splits disjoint
            "test_snap_dates": list(_TEST_MONTHS),
        },
    }


def _plan(*to_process) -> SnapDatePlan:
    """A month plan for ``to_process``; everything else in the fixture skipped.

    ``skipped`` is filled in for realism only — no node reads it, and no
    assertion below depends on it. What every assertion does depend on is that
    the fixtures hold months this plan leaves out (see
    ``test_fixtures_hold_months_outside_a_partial_plan``): without that, "the
    node filtered to the plan" and "the fixture had nothing else" are the same
    observation and the whole block is vacuously green.
    """
    keep = [pd.Timestamp(d) for d in to_process]
    return SnapDatePlan(
        to_process=keep,
        skipped=[pd.Timestamp(d) for d in _SNAP_DATES if pd.Timestamp(d) not in keep],
    )


def _count_key_rows_in(keys, months):
    """Rows of ``keys`` inside ``months``.

    The row count build_test_model_input owes for those months — its output
    grain is its (filtered) keys' grain just like build_model_input's, so this
    is the D9 identity for the incremental branch. date_format normalises the
    timestamp and Hive-string forms of snap_date alike.
    """
    return keys.filter(
        F.date_format(F.col("snap_date"), "yyyy-MM-dd").isin(list(months))
    ).count()


def _months(df):
    # snap_date is a timestamp when built from pandas, a string when it came
    # back through a Hive partition column — normalise both.
    return sorted(
        str(pd.Timestamp(d).date())
        for d in df.select("snap_date").distinct().toPandas()["snap_date"]
    )


def test_fixtures_hold_months_outside_a_partial_plan(label_table, feature_table):
    """The precondition every scoping assertion in this block rests on."""
    partial = {str(d.date()) for d in _plan("2024-05-31").to_process}
    assert set(_months(label_table)) - partial
    assert set(_months(feature_table)) - partial


class TestSelectTestKeysScoping:
    def test_processes_only_the_months_in_the_plan(self, label_table, parameters):
        params = _incremental_params(parameters)
        result = select_test_keys(label_table, _plan("2024-05-31"), params)
        assert _months(result) == ["2024-05-31"]

    def test_empty_plan_yields_nothing(self, label_table, parameters):
        # Every configured month already landed: a normal state, not an error.
        params = _incremental_params(parameters)
        assert select_test_keys(label_table, _plan(), params).count() == 0

    def test_full_plan_yields_every_test_month(self, label_table, parameters):
        params = _incremental_params(parameters)
        result = select_test_keys(label_table, _plan(*_TEST_MONTHS), params)
        assert _months(result) == _TEST_MONTHS


class TestBuildTestModelInputScoping:
    def test_filters_keys_outside_the_plan(
        self, feature_table, label_table, parameters
    ):
        # test_keys is a persistent Hive table: reading it back yields every
        # month, including ones select_test_keys did not rewrite. This node must
        # apply the plan itself rather than trust its input.
        params = _incremental_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(params), params,
        )
        all_month_keys = label_table.select(
            "snap_date", "cust_id", "prod_name"
        ).dropDuplicates()

        result = build_test_model_input(
            all_month_keys, encoded, label_table, preprocessor,
            _plan("2024-05-31"), params,
        )
        assert _months(result) == ["2024-05-31"]
        assert result.count() == _count_key_rows_in(all_month_keys, ["2024-05-31"])


class TestApplyPreprocessorScoping:
    def test_encodes_only_the_months_in_the_plan(self, feature_table, parameters):
        params = _incremental_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        result = apply_preprocessor_to_features(
            feature_table, preprocessor,
            _plan("2024-02-29", "2024-03-31", "2024-05-31"), params,
        )
        assert _months(result) == ["2024-02-29", "2024-03-31", "2024-05-31"]

    def test_empty_plan_yields_an_empty_but_typed_frame(
        self, feature_table, parameters
    ):
        params = _incremental_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        full = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(params), params,
        )
        result = apply_preprocessor_to_features(
            feature_table, preprocessor, _plan(), params,
        )
        assert result.count() == 0
        # Schema must survive the empty case: an empty write with a drifted
        # schema is how a dynamic-partition overwrite corrupts a live table.
        assert result.dtypes == full.dtypes

    def test_a_month_missing_from_feature_table_raises_only_when_planned(
        self, feature_table, parameters
    ):
        # Why the node hands preprocessing the month *list* and not a
        # pre-filtered frame: preprocessing owns the "you asked for a month
        # feature_table does not have" check, and it can only fire on months
        # this run actually intends to read.
        params = _incremental_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)

        with pytest.raises(ValueError, match="2024-12-31"):
            apply_preprocessor_to_features(
                feature_table, preprocessor,
                _plan(*_SNAP_DATES, "2024-12-31"), params,
            )

        # Same absent month, this time already landed -> not read, not checked.
        apply_preprocessor_to_features(
            feature_table, preprocessor, _plan(*_SNAP_DATES), params,
        )


class TestScopedNodesHandleHiveStringDates:
    """snap_date comes back from Hive as a STRING partition column.

    The runner reloads every node input through the catalog, so
    build_test_model_input sees ``snap_date: string``, not the timestamp dtype a
    pandas-built fixture produces. Comparing that string column against
    ``pd.Timestamp`` literals matches zero rows and raises nothing — the frame
    silently empties and the Hive write touches no partitions. Caught by a real
    run, not by the pandas-typed fixtures above.
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
        params = _incremental_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        encoded = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(params), params,
        )

        result = build_test_model_input(
            string_dated_keys, encoded, label_table, preprocessor,
            _plan("2024-05-31"), params,
        )
        assert result.count() > 0, "string-typed snap_date silently matched nothing"
        assert _months(result) == ["2024-05-31"]
        assert result.count() == _count_key_rows_in(string_dated_keys, ["2024-05-31"])

    def test_select_test_keys_still_works_on_date_typed_source(
        self, label_table, parameters
    ):
        # The other direction: sample_pool/label_table keep a real date dtype.
        # Normalising to DATE must not break that path.
        assert dict(label_table.dtypes)["snap_date"].startswith("timestamp")
        params = _incremental_params(parameters)
        result = select_test_keys(label_table, _plan("2024-05-31"), params)
        assert _months(result) == ["2024-05-31"]


# =============================================================================
# T3 (#144) — ML-semantics contracts the pre-#140 suite never asserted.
#
# Everything below builds on the module fixtures above. Where a test needs a
# different split layout it derives one from ``_SNAP_DATES`` rather than adding
# a second fixture, so the "resize in one place" property of the fixture block
# survives.
# =============================================================================

from recsys_tfb.pipelines.dataset.nodes import (
    filter_groups_with_positives,
    select_calibration_keys,
)


def _four_split_params(parameters, **overrides):
    """``parameters`` reshaped so all four key-selecting splits are populated.

    The module fixture spends all five months on train/val/test, leaving none
    for calibration — and a month may belong to exactly one split (invariant
    A24, enforced on the ``dataset`` command), so they cannot silently share
    one. Train gives up its third month to calibration; every month still comes
    from ``_SNAP_DATES``, so resizing the fixture still resizes this.
    """
    dataset = {
        **parameters["dataset"],
        "sample_ratio": 1.0,
        "train_snap_dates": _SNAP_DATES[:2],
        "calibration_snap_dates": _SNAP_DATES[2:3],
        "enable_calibration": True,
        "val_snap_dates": _SNAP_DATES[3:4],
        "test_snap_dates": _SNAP_DATES[4:5],
    }
    dataset.update(overrides)
    return {**parameters, "dataset": dataset}


def _columns_by_derivation_rule(keys, preprocessor, params) -> set[str]:
    """ADR-0004's rule for a split's model_input columns.

    ``identity ∪ {label} ∪ feature_columns ∪ (carry_columns ∩ that split's keys)``

    Deliberately phrased in terms of the **config** key ``carry_columns``, not
    in terms of ``keys.columns``. The module-level ``_expected_model_input_columns``
    uses the latter, which mirrors what ``build_model_input`` computes (carry =
    anything in keys that is not identity/feature/label) — self-consistent, and
    therefore blind to a key frame that carries a column nobody asked to carry.
    Stating the rule against the config is what closes that gap (#140 D4).
    """
    schema = get_schema(params)
    carry = set(params["dataset"].get("carry_columns") or [])
    return (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set(preprocessor["feature_columns"])
        | (carry & set(keys.columns))
    )


class TestModelInputSchemaPerSplit:
    """D4 — every split's schema follows one derivation rule (ADR-0004).

    train / train_dev / calibration are sampled with carry columns and so carry
    ``carry_columns``; val / test select identity only. ADR-0004 records that
    asymmetry as a *derived* result — sample weights only apply to train-side
    splits, and per-segment evaluation reads segments from ``sample_pool`` later
    — so the assertion is the rule, not "train has it, val does not". Changing
    ``carry_columns`` then needs no edit here.
    """

    @pytest.fixture
    def built(self, feature_table, label_table, sample_pool, parameters):
        params = _four_split_params(parameters, carry_columns=["channel_preference"])
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(params), params,
        )

        sample_keys = select_train_keys(sample_pool, params)
        train_keys, train_dev_keys = split_train_keys(sample_keys, params)
        keys_by_split = {
            "train": train_keys,
            "train_dev": train_dev_keys,
            "calibration": select_calibration_keys(sample_pool, params),
            "val": select_val_keys(sample_pool, params),
            "test": select_test_keys(sample_pool, _test_keys_plan(params), params),
        }
        built = {
            split: build_model_input(keys, pft, label_table, preprocessor, params)
            for split, keys in keys_by_split.items()
        }
        return params, preprocessor, keys_by_split, built

    def test_every_split_matches_the_derivation_rule(self, built):
        params, preprocessor, keys_by_split, built_by_split = built
        for split, mi in built_by_split.items():
            assert set(mi.columns) == _columns_by_derivation_rule(
                keys_by_split[split], preprocessor, params
            ), split

    def test_the_fixture_exercises_both_sides_of_the_carry_term(self, built):
        """Without this the rule above is satisfiable by a vacuous carry term.

        ``carry_columns ∩ keys`` is empty for every split if the key selection
        stops carrying at all — and the rule stays true, because both sides lose
        the column together. This asserts the fixture actually distinguishes the
        two cases, without naming which split is which.
        """
        params, _, keys_by_split, _ = built
        carry = set(params["dataset"]["carry_columns"])
        carried = {
            split for split, keys in keys_by_split.items()
            if carry & set(keys.columns)
        }
        assert carried, "no split carried anything — the rule's carry term is vacuous"
        assert carried != set(keys_by_split), (
            "every split carried — the asymmetry the rule describes is untested"
        )

    def test_positive_rate_is_strictly_inside_its_bounds(self, built):
        """D12 — an all-zero (or all-one) label column must not pass silently.

        A dataset with no positives trains a model that cannot rank, and every
        mAP downstream is 0 — but nothing raises. Bounds are strict on both
        sides and expressed in rows, so a label column that stopped joining
        (all 0) and one that got overwritten with a constant 1 both fail.
        """
        _, _, _, built_by_split = built
        for split, mi in built_by_split.items():
            n_rows = mi.count()
            n_pos = mi.filter(F.col("label") == 1).count()
            assert n_rows > 0, split
            assert 0 < n_pos < n_rows, f"{split}: {n_pos} positives in {n_rows} rows"


class TestQueryGroupCompleteness:
    """D18 — every query group carries the full candidate set.

    mAP's denominator is the number of relevant items within a query group, so
    a group that lost candidates reports a metric computed over a different
    population than the one that will be served. ADR-0006 declines to spend a
    full scan on a data gate for this because in this repo it is a structural
    guarantee — ``sample_pool`` is a literal cross join, val/test take the whole
    population, and the group filter only ever drops whole groups. A test is
    what holds that structure in place.
    """

    def _group_sizes(self, mi, params):
        schema = get_schema(params)
        group_cols = [schema["time"]] + schema["entity"]
        return mi.groupBy(*group_cols).count().toPandas()["count"].tolist()

    def test_val_and_test_groups_hold_every_declared_item(
        self, feature_table, label_table, sample_pool, parameters
    ):
        params = _four_split_params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(feature_table, params)
        pft = apply_preprocessor_to_features(
            feature_table, preprocessor, _encode_plan(params), params,
        )
        schema = get_schema(params)
        declared = set(params["schema"]["categorical_values"][schema["item"]])

        for split, keys in {
            "val": select_val_keys(sample_pool, params),
            "test": select_test_keys(sample_pool, _test_keys_plan(params), params),
        }.items():
            mi = build_model_input(keys, pft, label_table, preprocessor, params)
            sizes = self._group_sizes(mi, params)
            assert sizes, split
            # Every group is the same size *and* that size is the declared item
            # count — "all equal" alone would hold for a set uniformly missing
            # the same item.
            assert set(sizes) == {_n_items(params)}, split
            items = {r[schema["item"]] for r in mi.select(schema["item"]).distinct().collect()}
            assert items == declared, split

    def test_group_filter_drops_whole_groups_only(self, spark, parameters):
        """D19 — the group key is (time, *entity); adding item guts the filter.

        With item in the key each row is its own group, so "drop groups whose
        label sum is 0" degenerates into "keep only positive rows" — every
        negative candidate disappears and the surviving groups have exactly one
        row each. #140 records that mutation passing all 16 existing tests,
        because they call the private helper with an explicit ``group_cols``
        and so never exercise the node wrapper's derivation.

        Both directions are asserted: the mixed group keeps all its rows, and
        the all-zero group is dropped entirely. Without the second half a filter
        that does nothing at all would also pass.
        """
        mi = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime([_SNAP_DATES[0]] * 6),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
            "prod_name": _PRODUCTS * 2,
            # C001 has one positive; C002 has none.
            "label": [1, 0, 0, 0, 0, 0],
        }))
        out = filter_groups_with_positives(mi, parameters).toPandas()

        assert sorted(out["cust_id"].unique()) == ["C001"]
        # All three of C001's candidates survive — negatives included.
        assert len(out) == _n_items(parameters)
        assert set(out["prod_name"]) == set(_PRODUCTS)


class TestFitUsesTrainMonthsOnly:
    """D20 — category mappings are fit on train months, asserted at the fit.

    The apply side already had coverage, but "no leakage" is a property of the
    *fit*: if ``fit_preprocessor_metadata`` looked at every month, the mapping
    would contain a category the model can only have learned from val/test, and
    the apply side would encode it perfectly happily — no unknown, no warning,
    nothing red.
    """

    @pytest.fixture
    def ft_with_val_only_category(self, spark, feature_table):
        """feature_table plus a categorical whose 'digital_v2' value is val-only."""
        val_month = pd.Timestamp(_SNAP_DATES[3])
        return feature_table.withColumn(
            "risk_attr",
            F.when(F.col("snap_date") == F.lit(val_month), F.lit("digital_v2"))
             .otherwise(F.lit("standard")),
        )

    def _params(self, parameters):
        return _gate_params(parameters, categorical_extra=["risk_attr"])

    def test_val_only_category_never_enters_the_mapping(
        self, ft_with_val_only_category, parameters
    ):
        params = self._params(parameters)
        preprocessor, mappings = fit_preprocessor_metadata(
            ft_with_val_only_category, params
        )
        assert mappings["risk_attr"] == ["standard"]
        assert "digital_v2" not in mappings["risk_attr"]

    def test_the_val_only_value_really_is_in_the_source_table(
        self, ft_with_val_only_category
    ):
        """The paired half: the value exists, it was excluded on purpose.

        Without it, an empty-or-missing 'digital_v2' would be explained just as
        well by a fixture that never had the value.
        """
        values = {
            r.risk_attr for r in
            ft_with_val_only_category.select("risk_attr").distinct().collect()
        }
        assert values == {"standard", "digital_v2"}

    def test_apply_encodes_the_unseen_value_as_unknown(
        self, ft_with_val_only_category, parameters
    ):
        """The downstream consequence, so the fit-side rule is grounded.

        A value absent from the mapping becomes -1 at encode time — that is what
        "the model never saw it" looks like in the data.
        """
        params = self._params(parameters)
        preprocessor, _ = fit_preprocessor_metadata(ft_with_val_only_category, params)
        out = apply_preprocessor_to_features(
            ft_with_val_only_category, preprocessor, _encode_plan(params), params,
        )
        val_month = pd.Timestamp(_SNAP_DATES[3])
        encoded = out.filter(F.col("snap_date") == F.lit(val_month))
        assert encoded.count() > 0
        assert {r.risk_attr for r in encoded.select("risk_attr").distinct().collect()} == {-1}


class TestIdentityCategoricalNotEncoded:
    """D16 — identity categoricals are deliberately left un-encoded here.

    ``prod_name`` is declared categorical *and* is an identity column. Encoding
    is deferred to training (extract_Xy), so ``apply_preprocessor_to_features``
    excludes identity columns from its encode list. It is easy to "tidy up" that
    exclusion into uniform encoding, which would turn identity values into
    integers in ``preprocessed_feature_table`` and break every downstream join
    that matches on them.

    The fixture's feature_table has no ``prod_name`` column, so the exclusion is
    unreachable there — this one adds it, which is what makes the test capable
    of failing.
    """

    @pytest.fixture
    def ft_with_item_column(self, feature_table):
        return feature_table.withColumn("prod_name", F.lit(_PRODUCTS[0]))

    def test_identity_categorical_stays_its_original_type(
        self, ft_with_item_column, parameters
    ):
        """Asserted against the type object, not ``dtypes``' string form.

        ``dtypes`` yields ``simpleString()`` output; comparing substrings of a
        repr this test does not own is how a type assertion silently stops
        testing anything.
        """
        from pyspark.sql import types as T

        preprocessor, _ = fit_preprocessor_metadata(ft_with_item_column, parameters)
        out = apply_preprocessor_to_features(
            ft_with_item_column, preprocessor, _encode_plan(parameters), parameters,
        )

        assert "prod_name" in out.columns
        assert out.schema["prod_name"].dataType == T.StringType()
        assert {r.prod_name for r in out.select("prod_name").distinct().collect()} == {
            _PRODUCTS[0]
        }

    def test_it_is_declared_categorical_all_the_same(
        self, ft_with_item_column, parameters
    ):
        """The exclusion is about being *identity*, not about being undeclared.

        If prod_name were simply missing from categorical_columns the type would
        also survive, and this test would be evidence for the wrong rule.
        """
        preprocessor, _ = fit_preprocessor_metadata(ft_with_item_column, parameters)
        assert "prod_name" in preprocessor["categorical_columns"]


class TestUnknownEncodingRateIsAssertable:
    """D17 — the unknown(-1) share is readable off the returned frame.

    #140 asks for the unknown rate to be assertable rather than log-only. It is
    asserted here as a property of ``apply_preprocessor_to_features``'s output,
    which needs no signature change: the encoded column *is* the measurement.
    (The node's return type is unchanged on purpose — this ticket adds tests;
    the parent's four product changes are all elsewhere.)
    """

    @pytest.fixture
    def encoded(self, spark):
        ft = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime([_SNAP_DATES[0]] * 4),
            "cust_id": ["C001", "C002", "C003", "C004"],
            "total_aum": [100.0, 200.0, 300.0, 400.0],
            "channel_preference": ["digital", "branch", "unseen_a", "unseen_b"],
        }))
        preprocessor = {
            "feature_columns": ["channel_preference", "total_aum"],
            "categorical_columns": ["channel_preference"],
            "category_mappings": {"channel_preference": ["digital", "branch"]},
            "drop_columns": [],
        }
        params = {
            "schema": {"categorical_values": {"prod_name": [_PRODUCTS[0]]}},
            "dataset": {
                "train_snap_dates": [_SNAP_DATES[0]],
                "calibration_snap_dates": [], "val_snap_dates": [], "test_snap_dates": [],
            },
        }
        return apply_preprocessor_to_features(ft, preprocessor, _encode_plan(params), params)

    def test_unknown_share_is_the_expected_fraction(self, encoded):
        total = encoded.count()
        n_unknown = encoded.filter(F.col("channel_preference") == -1).count()
        assert total == 4
        # Two of four values were absent from the mapping. Asserted as a number,
        # not as "> 0": a rate assertion that only checks non-emptiness cannot
        # tell 2 unknowns from 4.
        assert n_unknown == 2
        assert n_unknown / total == 0.5

    def test_known_values_keep_their_mapped_positions(self, encoded):
        """So the rate above is a measurement, not the whole column collapsing."""
        by_cust = {
            r.cust_id: r.channel_preference
            for r in encoded.select("cust_id", "channel_preference").collect()
        }
        assert by_cust["C001"] == 0
        assert by_cust["C002"] == 1


class TestSelectKeysDeduplication:
    """D7 — val/test key selection deduplicates its input.

    The sampled splits lean on sample_pool's primary key being
    enforced upstream and does not dedup; val/test call ``dropDuplicates``
    explicitly. That difference is invisible until sample_pool actually contains
    a duplicate, which the clean fixture never does.
    """

    @pytest.fixture
    def duplicated_pool(self, sample_pool):
        # Every row twice: a duplicate key ratio of 1.0, not a single stray row,
        # so a partially-working dedup fails too.
        return sample_pool.unionByName(sample_pool)

    def test_val_keys_are_unique_despite_duplicate_input(
        self, duplicated_pool, sample_pool, parameters
    ):
        assert duplicated_pool.count() == 2 * sample_pool.count()
        result = select_val_keys(duplicated_pool, parameters)
        assert result.count() == _expected_key_count(parameters, "val")
        identity = get_schema(parameters)["identity_columns"]
        assert result.count() == result.dropDuplicates(identity).count()

    def test_test_keys_are_unique_despite_duplicate_input(
        self, duplicated_pool, parameters
    ):
        result = select_test_keys(duplicated_pool, _test_keys_plan(parameters), parameters)
        assert result.count() == _expected_key_count(parameters, "test")
        identity = get_schema(parameters)["identity_columns"]
        assert result.count() == result.dropDuplicates(identity).count()


class TestSelectKeysOverridePath:
    """D8 — the override lookup must not change the row count.

    ``with_effective_sample_ratio`` maps group key -> ratio with a broadcast
    LEFT join. The
    comment in ``sampling.py`` claims it "never fans out rows" (the lookup
    has unique keys) and that unmatched groups fall back to ``sample_ratio``
    via coalesce. Both halves are row-count claims, and neither was tested: a
    lookup with a duplicate key silently multiplies rows, and an INNER join
    silently drops every group without an override.
    """

    def _params(self, parameters, overrides):
        return {
            **parameters,
            "dataset": {
                **parameters["dataset"],
                "sample_ratio": 1.0,
                "sample_group_keys": ["cust_segment_typ"],
                "sample_ratio_overrides": overrides,
            },
        }

    def test_partial_overrides_keep_every_row(self, sample_pool, parameters):
        """Ratios all 1.0, and only some groups listed.

        Keeping every row is then the correct answer by two separate routes —
        matched groups at ratio 1.0 and unmatched groups falling back to
        sample_ratio 1.0 — so a count below the full population means either the
        join dropped unmatched groups or the coalesce stopped defaulting.
        """
        params = self._params(parameters, {"mass": 1.0})
        result = select_train_keys(sample_pool, params)
        assert result.count() == _expected_key_count(params, "train")

    def test_full_overrides_keep_every_row(self, sample_pool, parameters):
        """Every group listed, so nothing relies on the fallback.

        Paired with the test above: this one still fails if the lookup fans out,
        but passes if only the fallback broke — which is what separates the two
        failure modes.
        """
        params = self._params(
            parameters, {seg: 1.0 for seg in _SEGMENTS},
        )
        result = select_train_keys(sample_pool, params)
        assert result.count() == _expected_key_count(params, "train")

    def test_override_actually_reduces_the_targeted_group(
        self, sample_pool, parameters
    ):
        """The negative control: the override path is doing something.

        Without this, 'row count unchanged' would also hold for an override
        mechanism that was never wired up at all.
        """
        params = self._params(parameters, {"mass": 0.0})
        result = select_train_keys(sample_pool, params)
        segments = {
            r.cust_segment_typ for r in
            sample_pool.select("cust_id", "cust_segment_typ").distinct().collect()
        }
        assert "mass" in segments  # the group being zeroed really exists
        assert 0 < result.count() < _expected_key_count(params, "train")


class TestSelectCalibrationKeys:
    """D23 — the calibration node, and its independence from the train draw.

    ``select_calibration_keys`` registers as a pipeline node whenever
    ``enable_calibration`` is set, and had no unit test of its own.
    """

    def test_selects_the_calibration_months_at_full_population(
        self, sample_pool, parameters
    ):
        params = _four_split_params(parameters)
        result = select_calibration_keys(sample_pool, params)
        assert result.count() == _expected_key_count(params, "calibration")
        assert sorted(result.columns) == _identity_columns(params)
        months = {
            str(pd.Timestamp(d).date())
            for d in result.select("snap_date").distinct().toPandas()["snap_date"]
        }
        assert months == set(params["dataset"]["calibration_snap_dates"])

    def test_carry_columns_reach_calibration_keys(self, sample_pool, parameters):
        """Calibration makes the same carry decision train does."""
        params = _four_split_params(parameters, carry_columns=["channel_preference"])
        result = select_calibration_keys(sample_pool, params)
        assert "channel_preference" in result.columns

    def test_the_two_nodes_pass_different_sampling_sites(
        self, sample_pool, parameters, monkeypatch
    ):
        """The wiring, asserted structurally rather than through the draw.

        Whether two hash sites happen to produce different key sets depends on
        where 24 particular entities land; whether the two nodes *ask* for
        different sites does not. A spy answers the actual question — #140's
        concern is that the two share a seed and would otherwise draw the same
        rows.
        """
        import recsys_tfb.pipelines.dataset.nodes as nodes
        from recsys_tfb.pipelines.dataset.steps.sampling import keep_rows_drawn_under_ratio

        seen = {}

        def _spy(keys, identity_key, seed, *, site):
            seen[site] = seen.get(site, 0) + 1
            return keep_rows_drawn_under_ratio(keys, identity_key, seed, site=site)

        # The draw is the step that takes ``site``; spying on it is spying on
        # the wiring. Both splits must sample for it to be reached at all, so
        # the ratios below are what put each node on its drawing path.
        monkeypatch.setattr(nodes, "keep_rows_drawn_under_ratio", _spy)
        params = _four_split_params(
            parameters, sample_ratio=0.5, calibration_sample_ratio=0.5,
        )
        select_train_keys(sample_pool, params)
        select_calibration_keys(sample_pool, params)

        assert len(seen) == 2, f"both nodes used the same sampling site: {seen}"
        assert set(seen.values()) == {1}

    def test_different_sites_draw_different_rows(self, sample_pool, parameters):
        """And the site argument is what makes the draws differ.

        Same pool, same months, same ratio, same seed — only ``site`` differs.
        Paired with the spy above: that one pins the wiring, this one pins that
        the wiring matters.
        """
        # The two splits are given the *same* months here so ``site`` is the
        # only thing left that can differ. A24 forbids that overlap on the
        # ``dataset`` command, not in the node — which is what makes it usable
        # as an isolating fixture and useless as a config.
        months = list(_SNAP_DATES[:2])
        params = _four_split_params(
            parameters,
            sample_ratio=0.5,
            calibration_sample_ratio=0.5,
            train_snap_dates=months,
            calibration_snap_dates=months,
        )

        def _draw(node):
            keys = node(sample_pool, params)
            return {tuple(r) for r in keys.toPandas().itertuples(index=False)}

        train_draw = _draw(select_train_keys)
        cal_draw = _draw(select_calibration_keys)
        assert train_draw and cal_draw
        assert train_draw != cal_draw


class TestSelectValKeysSampling:
    """D24 — the ``val_sample_ratio < 1.0`` branch, which had no coverage.

    Val sampling is per-*entity* on purpose: a query group must keep all of its
    candidates or the mAP computed over it answers a different question (see
    TestQueryGroupCompleteness). Sampling rows instead would be invisible in a
    row count alone.
    """

    def _params(self, parameters, ratio):
        return {
            **parameters,
            "dataset": {**parameters["dataset"], "val_sample_ratio": ratio},
        }

    def test_sampling_reduces_the_population(self, sample_pool, parameters):
        params = self._params(parameters, 0.5)
        full = _expected_key_count(parameters, "val")
        result = select_val_keys(sample_pool, params)
        assert 0 < result.count() < full

    def test_sampling_keeps_whole_entities(self, sample_pool, parameters):
        """Every surviving entity keeps its full candidate set."""
        params = self._params(parameters, 0.5)
        pdf = select_val_keys(sample_pool, params).toPandas()
        sizes = pdf.groupby(["snap_date", "cust_id"]).size().unique().tolist()
        assert sizes == [_n_items(parameters)]

    def test_sampling_is_deterministic(self, sample_pool, parameters):
        params = self._params(parameters, 0.5)
        first = {tuple(r) for r in select_val_keys(sample_pool, params)
                 .toPandas().itertuples(index=False)}
        second = {tuple(r) for r in select_val_keys(sample_pool, params)
                  .toPandas().itertuples(index=False)}
        assert first == second

    def test_full_ratio_skips_sampling_entirely(self, sample_pool, parameters):
        """The other side of the branch, so 'reduced' is attributable to ratio."""
        params = self._params(parameters, 1.0)
        assert select_val_keys(sample_pool, params).count() == _expected_key_count(
            parameters, "val"
        )


# =============================================================================
# Moved here by #168, when preprocessing/ was dissolved and these two
# implementations came to live in this module. Assertions are unchanged from
# the old tests/test_preprocessing/test_spark.py; only the import paths moved.
# =============================================================================


class TestFilterGroupsWithPositives:
    """Spark-side group-positive filter for val/test_model_input.

    A (time, *entity) group is dropped iff every label in that group is 0.
    Used at dataset-write time so val_model_input / test_model_input on Hive
    only contain customers with at least one positive label per snap_date.
    """

    def test_drops_all_zero_groups(self, spark):
        from recsys_tfb.pipelines.dataset.steps.model_input import drop_groups_without_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
            "prod_name": ["a", "b"] * 3,
            "label": [1, 0, 0, 1, 0, 0],
            "feat": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }))
        out = drop_groups_without_positives(df, ["snap_date", "cust_id"], "label")
        rows = out.orderBy("cust_id", "prod_name").collect()
        # c3 dropped, c1 and c2 retained entirely (2 rows each)
        assert len(rows) == 4
        assert {r.cust_id for r in rows} == {"c1", "c2"}

    def test_keeps_all_rows_of_positive_groups(self, spark):
        """Group with even one positive row keeps every row in that group."""
        from recsys_tfb.pipelines.dataset.steps.model_input import drop_groups_without_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "cust_id": ["c1"] * 4,
            "prod_name": ["a", "b", "c", "d"],
            "label": [0, 0, 1, 0],
        }))
        out = drop_groups_without_positives(df, ["snap_date", "cust_id"], "label")
        assert out.count() == 4

    def test_groups_split_across_snap_dates(self, spark):
        """(snap_date, cust_id) is the group key — same cust across two snaps
        is two separate groups."""
        from recsys_tfb.pipelines.dataset.steps.model_input import drop_groups_without_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(
                ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"]
            ),
            "cust_id": ["c1", "c1", "c1", "c1"],
            "prod_name": ["a", "b", "a", "b"],
            "label": [1, 0, 0, 0],
        }))
        out = drop_groups_without_positives(df, ["snap_date", "cust_id"], "label")
        rows = out.orderBy("snap_date", "prod_name").collect()
        # 2025-01 has positive → keep both rows; 2025-02 all-zero → drop
        assert len(rows) == 2
        assert all(str(r.snap_date).startswith("2025-01") for r in rows)

    def test_preserves_column_schema(self, spark):
        from recsys_tfb.pipelines.dataset.steps.model_input import drop_groups_without_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"]),
            "cust_id": ["c1"],
            "prod_name": ["a"],
            "label": [1],
            "feat": [3.14],
        }))
        out = drop_groups_without_positives(df, ["snap_date", "cust_id"], "label")
        assert out.columns == df.columns

    def test_empty_when_no_positives(self, spark):
        from recsys_tfb.pipelines.dataset.steps.model_input import drop_groups_without_positives

        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": ["c1", "c2"],
            "prod_name": ["a", "b"],
            "label": [0, 0],
        }))
        out = drop_groups_without_positives(df, ["snap_date", "cust_id"], "label")
        assert out.count() == 0


class TestApplyPreprocessorToFeaturesRequiresSnapDates:
    """The month scope is required — the caller owns "which months is this run for".

    It used to default to ``None`` and fall back to
    ``collect_dataset_snap_dates(parameters)`` (the union of every configured
    month). Since the month plans moved into the catalog (#152) the caller
    always supplies one, so that fallback was unreachable. Pinning the argument
    as required is what keeps it unreachable: a future caller that forgets it
    now fails at the call, instead of silently re-encoding every configured
    month and quietly undoing the incremental behaviour of ADR-0002.

    #170 dissolved the private twin this used to guard, so the assertion moved
    to the node that absorbed it: ``month_plan`` is the argument that now
    carries the scope, and it is equally required.
    """

    def test_omitting_the_month_plan_raises_instead_of_defaulting(self):
        from recsys_tfb.pipelines.dataset.nodes import (
            apply_preprocessor_to_features,
        )

        # Binding fails before the body runs, so the placeholder arguments are
        # never dereferenced. Were a default restored, this call would get past
        # binding and die on the placeholders instead (KeyError), so this
        # assertion is about the signature and not about the placeholders.
        with pytest.raises(TypeError, match="month_plan"):
            apply_preprocessor_to_features(None, {}, parameters={})


class TestFitPreprocessorMetadataKeyContract:
    """The writer half of the ``preprocessor.json`` key contract (#168).

    ``PreprocessorMetadata`` is the definition point; this is the assertion that
    the dataset side actually writes those keys and no others. It is the guard
    the acceptance criteria asks for: after the dissolution the reader lives in
    the inference pipeline, which this repo cannot exercise end-to-end locally
    (#63), so a key renamed here would otherwise surface only in the company
    environment — or worse, be read as a silently-missing key.

    Equality, not ``issuperset``: an *added* key is exactly as much of a contract
    change as a renamed one, and the reader side has no way to learn about it.
    """

    def test_fit_output_keys_are_the_declared_contract(
        self, feature_table, parameters
    ):
        from recsys_tfb.preprocessing import PreprocessorMetadata

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        assert set(preprocessor) == set(PreprocessorMetadata.__annotations__)


# =============================================================================
# Moved up from ``test_helpers_spark.py`` (#169), then rewritten by #170 to go
# through ``select_train_keys``: the four decisions these tests pin are now
# steps in the node, and ``select_keys`` — the helper that used to hold all four
# — no longer exists. Every assertion is the one that was written against the
# helper; only the call moved. Local fixtures are prefixed ``_sampling_`` to
# stay clear of the module fixture block above.
# =============================================================================


def _sampling_params(carry=None, group_keys=None, ratio=1.0, overrides=None):
    p = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "dataset": {
            "sample_group_keys": group_keys or ["cust_segment_typ", "prod_name", "label"],
            "train_snap_dates": ["2025-01-31"],
            "sample_ratio_overrides": overrides if overrides is not None else {},
            "sample_ratio": ratio},
        "random_seed": 42}
    if carry is not None:
        p["dataset"]["carry_columns"] = carry
    return p


def _sampling_pool(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "cust_id": [1, 2, 3, 4],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
        "label": [1, 0, 1, 0]}))


class TestSelectKeysCarry:
    def test_carry_present_no_sampling_path(self, spark):
        df = select_train_keys(
            _sampling_pool(spark), _sampling_params(carry=["cust_segment_typ"]))
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name",
                                   "cust_segment_typ"}

    def test_carry_present_overrides_path(self, spark):
        df = select_train_keys(
            _sampling_pool(spark),
            _sampling_params(carry=["cust_segment_typ"], overrides={"mass|a|1": 1.0}))
        assert "cust_segment_typ" in df.columns

    def test_no_carry_returns_identity_only(self, spark):
        df = select_train_keys(_sampling_pool(spark), _sampling_params())
        assert set(df.columns) == {"snap_date", "cust_id", "prod_name"}


class TestSelectKeysOverrideLookup:
    """Broadcast-join override lookup. Using ratios in {0.0, 1.0} makes the
    crc32 bucket irrelevant (threshold 0 drops all, threshold HASH_BUCKETS keeps
    all), so these assert the group_key -> ratio mapping itself — the part the
    broadcast join replaced. In _pool: custs 1 & 3 are (mass, a, label=1),
    custs 2 & 4 are (hnw, b, 0) and (aff, b, 0).
    """

    @staticmethod
    def _survivors(df):
        return {r["cust_id"] for r in df.collect()}

    def test_override_zero_drops_only_that_group(self, spark):
        # default 1.0 keeps all; the single override drops just its group.
        df = select_train_keys(
            _sampling_pool(spark), _sampling_params(overrides={"mass|a|1": 0.0}))
        assert self._survivors(df) == {2, 4}

    def test_unmatched_falls_back_to_default(self, spark):
        # default 0.0 drops all; only the overridden group survives (coalesce
        # fallback path for the unmatched rows).
        df = select_train_keys(
            _sampling_pool(spark),
            _sampling_params(ratio=0.0, overrides={"mass|a|1": 1.0}))
        assert self._survivors(df) == {1, 3}

    def test_multiple_overrides(self, spark):
        # two groups overridden in one lookup table; default 1.0 keeps the rest.
        df = select_train_keys(
            _sampling_pool(spark),
            _sampling_params(overrides={"mass|a|1": 0.0, "aff|b|0": 0.0}))
        assert self._survivors(df) == {2}

    def test_single_group_key_no_concat(self, spark):
        # group_keys == [label] exercises the len==1 (no concat_ws) path; the
        # override key is the string-cast label value.
        df = select_train_keys(
            _sampling_pool(spark),
            _sampling_params(group_keys=["label"], overrides={"0": 0.0}))
        assert self._survivors(df) == {1, 3}


class TestDatasetStepTiming:
    """``log_step`` marks the blocks that really cost time, not every block.

    Spark is lazy: a block that only chains transformations builds a plan in
    microseconds and the whole computation runs later, at the write. Timing
    such a block reports a guaranteed ~0.00s, which reads exactly like "this
    step was fast" — so only blocks that fire a Spark action carry a
    ``log_step``.

    The month-presence pre-check fires one (``distinct().collect()`` over the
    time column, ``steps/scoping.py``), which is why it is timed.
    """

    @staticmethod
    def _timed_steps(caplog) -> set:
        return {
            getattr(r, "step", None)
            for r in caplog.records
            if getattr(r, "event", None) == "step_completed"
        }

    def test_fit_times_its_month_presence_check(
        self, spark, feature_table, parameters, caplog
    ):
        import logging

        with caplog.at_level(logging.INFO):
            fit_preprocessor_metadata(feature_table, parameters)

        assert "require_months_present(train_snap_dates)" in self._timed_steps(caplog)

    def test_apply_times_its_month_presence_check(
        self, spark, feature_table, parameters, caplog
    ):
        import logging

        preprocessor, _ = fit_preprocessor_metadata(feature_table, parameters)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            apply_preprocessor_to_features(
                feature_table, preprocessor, _encode_plan(parameters), parameters,
            )

        assert "require_months_present(snap_dates)" in self._timed_steps(caplog)


# --- two-column entity -------------------------------------------------------
# Every test below runs on ``two_column_entity_params`` (tests/conftest.py), the
# shared fixture whose nesting is verified by
# tests/test_core/test_schema.py::TestTwoColumnEntityFixture. Hand-rolling a
# second schema dict here is the one thing that would make these tests vacuous:
# mis-nest it and get_schema silently returns the one-column default, so
# single-entity data would run against single-entity code and pass regardless.

_BRANCHES = ["B1", "B2"]
_TWO_COL_CUSTS = [f"C{i:03d}" for i in range(1, 13)]


def _two_column_params(two_column_entity_params, **dataset_over):
    """The shared two-column schema plus the dataset block these nodes read."""
    dataset = {
        "train_snap_dates": _SNAP_DATES[:1],
        "sample_ratio": 1.0,
        "sample_group_keys": ["prod_name"],
        "sample_ratio_overrides": {},
        "train_dev_ratio": 0.5,
        "val_snap_dates": _SNAP_DATES[3:4],
        "val_sample_ratio": 0.5,
        "test_snap_dates": _SNAP_DATES[4:5],
    }
    dataset.update(dataset_over)
    return {**two_column_entity_params, "random_seed": 42, "dataset": dataset}


def _two_column_pool(spark, snap_dates):
    """Every branch holds every customer, so the two columns disagree on purpose.

    A branch is coarser than an entity here: grouping on ``branch_id`` alone
    yields 2 groups where the entity yields 24. That gap is what every
    assertion below reads.
    """
    rows = [
        {
            "snap_date": pd.Timestamp(snap),
            "branch_id": branch,
            "cust_id": cid,
            "prod_name": _PRODUCTS[0],
            "label": 0,
        }
        for snap in snap_dates
        for branch in _BRANCHES
        for cid in _TWO_COL_CUSTS
    ]
    return spark.createDataFrame(pd.DataFrame(rows))


def _entity_pairs(df) -> set:
    pdf = df.select("branch_id", "cust_id").distinct().toPandas()
    return set(map(tuple, pdf.itertuples(index=False)))


def _branches_split_across(left: set, right: set) -> set:
    """Branches with at least one entity on each side."""
    return {b for b, _ in left} & {b for b, _ in right}


class TestSplitTrainKeysTwoColumnEntity:
    def test_split_groups_on_the_whole_entity_not_its_first_column(
        self, spark, two_column_entity_params
    ):
        """A branch must be able to straddle the train / train-dev boundary.

        The split's unit is the entity, and here an entity is (branch, cust).
        Grouping on ``branch_id`` alone would make the straddle structurally
        impossible — every customer of a branch would be forced onto one side,
        and with two branches the "split" would be a coin flip between two
        blocks of twelve. Nothing errors in that state; it just isn't a split
        on the unit the config declares.
        """
        params = _two_column_params(two_column_entity_params)
        keys = _two_column_pool(spark, _SNAP_DATES[:1])

        train, train_dev = split_train_keys(keys, params)
        train_pairs, dev_pairs = _entity_pairs(train), _entity_pairs(train_dev)

        assert _branches_split_across(train_pairs, dev_pairs) == set(_BRANCHES)

    def test_split_is_still_a_clean_partition_of_the_entities(
        self, spark, two_column_entity_params
    ):
        """Guards the straddle assertion above from a degenerate reading.

        "Some branch appears on both sides" is also satisfied by a broken split
        that duplicates or drops entities. Complete and disjoint is what makes
        the straddle mean what it says.
        """
        params = _two_column_params(two_column_entity_params)
        keys = _two_column_pool(spark, _SNAP_DATES[:1])

        train, train_dev = split_train_keys(keys, params)
        train_pairs, dev_pairs = _entity_pairs(train), _entity_pairs(train_dev)

        assert train_pairs | dev_pairs == _entity_pairs(keys)
        assert train_pairs & dev_pairs == set()
        assert len(_entity_pairs(keys)) == len(_BRANCHES) * len(_TWO_COL_CUSTS)

    def test_declared_train_split_keys_coarsen_the_unit(
        self, spark, two_column_entity_params
    ):
        """``train_split_keys: [branch_id]`` must actually reach the node.

        This is the user story the key exists for — a leakage unit coarser
        than the query group — and it is the exact behaviour the default must
        NOT have. Asserting it also pins the direction of the previous test:
        the straddle there is a property of the default, not of the data.
        """
        params = _two_column_params(
            two_column_entity_params, train_split_keys=["branch_id"])
        keys = _two_column_pool(spark, _SNAP_DATES[:1])

        train, train_dev = split_train_keys(keys, params)
        train_pairs, dev_pairs = _entity_pairs(train), _entity_pairs(train_dev)

        assert _branches_split_across(train_pairs, dev_pairs) == set()
        assert train_pairs and dev_pairs


class TestSelectValKeysTwoColumnEntity:
    def test_draw_groups_on_the_whole_entity_not_its_first_column(
        self, spark, two_column_entity_params
    ):
        """A branch must be able to be partially sampled.

        The draw keeps an entity whole; with the entity being (branch, cust),
        a branch is a bag of 12 entities and the draw runs on each. Drawing on
        ``branch_id`` alone would keep or drop all twelve together, so
        ``val_sample_ratio: 0.5`` would return 0, 12 or 24 entities instead of
        roughly half of them — still a valid sample of *something*, which is
        why nothing downstream would complain.
        """
        params = _two_column_params(two_column_entity_params)
        pool = _two_column_pool(spark, _SNAP_DATES[3:4])

        kept = _entity_pairs(select_val_keys(pool, params))
        dropped = _entity_pairs(pool) - kept

        assert _branches_split_across(kept, dropped) == set(_BRANCHES)

    def test_the_draw_keeps_a_strict_non_empty_subset(
        self, spark, two_column_entity_params
    ):
        """Guards the assertion above: it is vacuous if nothing was dropped."""
        params = _two_column_params(two_column_entity_params)
        pool = _two_column_pool(spark, _SNAP_DATES[3:4])

        all_pairs = _entity_pairs(pool)
        kept = _entity_pairs(select_val_keys(pool, params))

        assert kept < all_pairs
        assert kept

    def test_declared_val_sample_keys_coarsen_the_unit(
        self, spark, two_column_entity_params
    ):
        """``val_sample_keys: [branch_id]`` must actually reach the node."""
        params = _two_column_params(
            two_column_entity_params, val_sample_keys=["branch_id"])
        pool = _two_column_pool(spark, _SNAP_DATES[3:4])

        kept = _entity_pairs(select_val_keys(pool, params))
        dropped = _entity_pairs(pool) - kept

        assert _branches_split_across(kept, dropped) == set()
        assert kept and dropped


# --- B8 numeric precision gate -----------------------------------------------
# The gate reads footer statistics from the partitions ``apply_preprocessor_to_
# features`` just wrote, so these tests write a real partitioned parquet table
# and hand the node the frame read back from it. A hand-built stats mapping
# would test ``numeric_precision_errors`` again (it already has its own tests in
# test_core/test_consistency.py) and would prove nothing about the wiring
# between a month plan, a set of files, and a footer. ---

from unittest.mock import patch

from pyspark.sql import types as T

from recsys_tfb.pipelines.dataset import nodes as dataset_nodes
from recsys_tfb.pipelines.dataset.nodes import validate_numeric_precision

_BASE_VERSION = "ab12cd34"


def _landed_features(spark, tmp_path, per_month: dict) -> "DataFrame":
    """A preprocessed_feature_table as it lands: partitioned, read back.

    ``per_month`` maps a snap_date to that month's ``big_int`` value, which is
    the column every assertion below is about.
    """
    schema = T.StructType([
        T.StructField("cust_id", T.StringType()),
        T.StructField("big_int", T.LongType()),
        T.StructField("dbl", T.DoubleType()),
        T.StructField("base_dataset_version", T.StringType()),
        T.StructField("snap_date", T.StringType()),
    ])
    rows = [
        ("C001", int(value), 0.5, _BASE_VERSION, month)
        for month, value in per_month.items()
    ]
    root = str(tmp_path / "pft")
    spark.createDataFrame(rows, schema).write.partitionBy(
        "base_dataset_version", "snap_date",
    ).parquet(root)
    return spark.read.parquet(root)


def _precision_params(parameters, **dataset) -> dict:
    return {
        **parameters,
        "base_dataset_version": _BASE_VERSION,
        "dataset": {**parameters["dataset"], **dataset},
    }


_METADATA = {"feature_columns": ["big_int", "dbl"]}


class TestValidateNumericPrecisionScope:
    """What the gate checks today, with the real cast selector."""

    def test_an_integer_column_over_the_bound_is_not_flagged_today(
        self, spark, tmp_path, parameters,
    ):
        # The promise this ticket makes: landing the gate cannot make an
        # existing config fail. The cast converts Decimal and Double only, so
        # no integer column is in scope, however large it is.
        landed = _landed_features(
            spark, tmp_path, {"2024-01-31": 2 ** 40})
        validate_numeric_precision(
            landed, _METADATA, _plan("2024-01-31"),
            _precision_params(parameters),
        )

    def test_scope_is_read_from_the_cast_not_restated(self):
        # The structural claim behind the test above: the node asks the cast's
        # own selector. If this import ever stops pointing at it, the gate and
        # the cast can disagree without any test noticing.
        from recsys_tfb.preprocessing import castable_numeric_feature_columns

        assert (dataset_nodes.castable_numeric_feature_columns
                is castable_numeric_feature_columns)


class TestValidateNumericPrecisionGate:
    """The firing path, with the cast standing in as it will be after #283.

    ``castable_numeric_feature_columns`` is patched to name the integer column,
    which is exactly what widening the cast to integers will make it return.
    Without the patch every assertion here would be vacuously green — the gate
    would report "nothing to check" and the raise below could never fire.
    """

    def _run(self, landed, params, plan_months=("2024-01-31",)):
        with patch.object(
            dataset_nodes, "castable_numeric_feature_columns",
            return_value=["big_int"],
        ):
            validate_numeric_precision(
                landed, _METADATA, _plan(*plan_months), params)

    def test_max_exactly_at_the_bound_passes(self, spark, tmp_path, parameters):
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 24})
        self._run(landed, _precision_params(parameters))

    def test_one_above_the_bound_raises_and_names_the_column(
        self, spark, tmp_path, parameters,
    ):
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 24 + 1})
        with pytest.raises(DataConsistencyError) as exc:
            self._run(landed, _precision_params(parameters))
        assert "big_int" in str(exc.value)
        assert "B8" in str(exc.value)

    def test_truncate_policy_logs_instead_of_raising(
        self, spark, tmp_path, parameters, caplog,
    ):
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 24 + 1})
        with caplog.at_level("WARNING"):
            self._run(landed, _precision_params(
                parameters, numeric_precision_policy="truncate"))
        assert "big_int" in caplog.text

    def test_float64_declared_widens_the_bound(self, spark, tmp_path, parameters):
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 24 + 1})
        self._run(landed, _precision_params(
            parameters, numeric_feature_storage_type="float64"))

    def test_only_the_months_in_the_plan_are_read(
        self, spark, tmp_path, parameters,
    ):
        # The incremental property: a month that already landed is not re-read,
        # so an offending value outside the plan does not stop this run. Without
        # this the gate would re-fail every run over history it never wrote.
        landed = _landed_features(spark, tmp_path, {
            "2024-01-31": 2 ** 24,
            "2024-02-29": 2 ** 40,
        })
        self._run(landed, _precision_params(parameters),
                  plan_months=("2024-01-31",))

    def test_a_month_added_to_the_plan_is_checked(
        self, spark, tmp_path, parameters,
    ):
        # The other half of the pair: the same fixture, with the offending month
        # now in the plan, must fail. Alone, the test above passes for a gate
        # that reads nothing at all.
        landed = _landed_features(spark, tmp_path, {
            "2024-01-31": 2 ** 24,
            "2024-02-29": 2 ** 40,
        })
        with pytest.raises(DataConsistencyError):
            self._run(landed, _precision_params(parameters),
                      plan_months=("2024-01-31", "2024-02-29"))

    def test_an_empty_plan_checks_nothing(self, spark, tmp_path, parameters):
        # Every configured month already landed — a normal state, not an error.
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 40})
        self._run(landed, _precision_params(parameters), plan_months=())

    def test_another_dataset_version_is_not_read(
        self, spark, tmp_path, parameters,
    ):
        # inputFiles() answers for the whole relation, so the run's own version
        # is selected back out of the paths. Asked for a version the table does
        # not hold, the gate finds no files — and says so once, rather than
        # reporting every column as unmeasurable.
        landed = _landed_features(spark, tmp_path, {"2024-01-31": 2 ** 40})
        with pytest.raises(DataConsistencyError) as exc:
            self._run(landed, {**_precision_params(parameters),
                               "base_dataset_version": "ffffffff"})
        message = str(exc.value)
        assert "found no parquet files" in message
        assert message.count("B8:") == 1


class TestValidateNumericPrecisionOnDecimal:
    """The gate firing on today's cast, with nothing patched.

    Decimal is what ``cast_feature_floats_to_float32`` converts right now, and
    Spark's DecimalType is exact fixed-point — its values sit on a grid of
    ``10**-scale``. So this is the one class here that exercises the whole path
    (selector -> dtype step -> footer read -> bound) against the production
    wiring. If it ever needs a patch to fire, the gate has stopped covering the
    cast.
    """

    @staticmethod
    def _landed_decimal(spark, tmp_path, value, scale=2):
        from decimal import Decimal

        schema = T.StructType([
            T.StructField("cust_id", T.StringType()),
            T.StructField("bal", T.DecimalType(18, scale)),
            T.StructField("base_dataset_version", T.StringType()),
            T.StructField("snap_date", T.StringType()),
        ])
        rows = [("C001", Decimal(str(value)), _BASE_VERSION, "2024-01-31")]
        root = str(tmp_path / "pft_dec")
        spark.createDataFrame(rows, schema).write.partitionBy(
            "base_dataset_version", "snap_date",
        ).parquet(root)
        return spark.read.parquet(root)

    _META = {"feature_columns": ["bal"]}

    def _run(self, landed, params):
        validate_numeric_precision(
            landed, self._META, _plan("2024-01-31"), params)

    def test_a_decimal_column_within_its_bound_passes(
        self, spark, tmp_path, parameters,
    ):
        # decimal(18,2) values are 0.01 apart, so float32 holds them intact up
        # to 131,072 -- two orders of magnitude below the integer bound.
        landed = self._landed_decimal(spark, tmp_path, "131072.00")
        self._run(landed, _precision_params(parameters))

    def test_a_decimal_column_past_its_bound_is_caught_unpatched(
        self, spark, tmp_path, parameters,
    ):
        landed = self._landed_decimal(spark, tmp_path, "200000.00")
        with pytest.raises(DataConsistencyError) as exc:
            self._run(landed, _precision_params(parameters))
        message = str(exc.value)
        assert "bal" in message
        # The bound quoted must be the decimal one, not the integer one: a gate
        # that ignored the column's scale would report 16,777,216 and pass.
        assert "131,072" in message
        assert "200,000" in message

    def test_the_same_magnitude_passes_at_scale_zero(
        self, spark, tmp_path, parameters,
    ):
        # Paired with the test above so neither is vacuous: 200,000 is fine on a
        # grid of 1 and lossy on a grid of 0.01. Same value, same code path,
        # opposite verdict -- which is the scale actually being read.
        landed = self._landed_decimal(spark, tmp_path, "200000", scale=0)
        self._run(landed, _precision_params(parameters))

    def test_truncate_policy_lets_the_same_column_through(
        self, spark, tmp_path, parameters, caplog,
    ):
        # The operator's switch, on the path that actually fires today.
        landed = self._landed_decimal(spark, tmp_path, "200000.00")
        with caplog.at_level("WARNING"):
            self._run(landed, _precision_params(
                parameters, numeric_precision_policy="truncate"))
        assert "bal" in caplog.text

    def test_a_passing_run_returns_a_report_row_for_the_column(
        self, spark, tmp_path, parameters,
    ):
        # The artifact side: a gate that only says "fine" cannot tell an
        # operator this column is at 99% of its limit. The report can.
        landed = self._landed_decimal(spark, tmp_path, "65536.00")
        report = validate_numeric_precision(
            landed, self._META, _plan("2024-01-31"),
            _precision_params(parameters),
        )
        assert report["storage_type"] == "float32"
        assert report["policy"] == "block"
        assert report["months"] == ["2024-01-31"]
        assert report["breaches"] == 0
        row, = report["columns"]
        assert row["column"] == "bal"
        assert row["dtype"] == "decimal(18,2)"
        assert row["limit"] == 131072.0
        # 131,072 over 65,536 -- one doubling of the column away from breaching.
        assert row["headroom"] == pytest.approx(2.0)
        assert row["verdict"] == "ok"

    def test_a_truncated_run_still_returns_the_report(
        self, spark, tmp_path, parameters,
    ):
        # The survey workflow: run once under truncate, read the artifact. Under
        # block the run aborts and the catalog never writes it, which is why the
        # table is logged too.
        landed = self._landed_decimal(spark, tmp_path, "200000.00")
        report = validate_numeric_precision(
            landed, self._META, _plan("2024-01-31"),
            _precision_params(parameters, numeric_precision_policy="truncate"),
        )
        assert report["breaches"] == 1
        assert report["columns"][0]["verdict"] == "breach"

    def test_the_logged_table_carries_the_numbers_when_block_aborts(
        self, spark, tmp_path, parameters, caplog,
    ):
        # The one case the persisted report cannot cover. If this stops holding,
        # a blocked run leaves the operator with no numbers at all.
        landed = self._landed_decimal(spark, tmp_path, "200000.00")
        with caplog.at_level("INFO"):
            with pytest.raises(DataConsistencyError):
                self._run(landed, _precision_params(parameters))
        assert "headroom" in caplog.text
        assert "200,000" in caplog.text and "131,072" in caplog.text

    def test_nothing_to_check_still_returns_a_well_formed_report(
        self, spark, tmp_path, parameters,
    ):
        # The catalog writes whatever this returns, so the early-exit path
        # cannot hand it None.
        landed = self._landed_decimal(spark, tmp_path, "1.00")
        report = validate_numeric_precision(
            landed, self._META, _plan(), _precision_params(parameters))
        assert report["columns"] == []
        assert report["checked_columns"] == 0
        assert report["months"] == []

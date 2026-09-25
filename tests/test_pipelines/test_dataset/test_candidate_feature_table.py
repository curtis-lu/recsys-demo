"""The candidate-level feature table across the dataset nodes (ADR-0026).

A deployment has exactly one entity-level feature table (``feature_table``,
joined on the base key) and at most one candidate-level feature table (catalog
entry ``candidate_feature_table``, joined on identity). The second one is never
landed: ``build_model_input`` reads the months this run reads, encodes it and
joins it after sampling, because it is as large as ``sample_pool``.

The fixture is one entity in one month, shown in two requests. That is the
smallest shape in which the three candidate join keys the framework has — base
key, query group, identity — all give different answers, so a join on the
wrong one cannot produce the expected rows by coincidence.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan
from recsys_tfb.pipelines.dataset.nodes import (
    build_model_input,
    fit_preprocessor_metadata,
    validate_data_consistency,
    validate_numeric_precision,
)

pytestmark = pytest.mark.spark

_MONTH = "2024-01-31"
_OTHER_MONTH = "2024-02-29"
#: ``p2`` is declared but never shown, so a vocabulary read from the candidate
#: table's rows is visibly smaller than the declared one.
_ITEMS = ["p0", "p1", "p2"]
_PAGES = ["home", "search"]


def _params() -> dict:
    return {
        "random_seed": 42,
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "occasion": "req_id",
                "item": "prod_name",
            },
            "categorical_values": {"prod_name": list(_ITEMS)},
        },
        "dataset": {
            "train_snap_dates": [_MONTH],
            "val_snap_dates": [_OTHER_MONTH],
            "test_snap_dates": [],
            "prepare_model_input": {
                "categorical_columns": ["prod_name", "page_type"],
                "drop_columns": ["snap_date", "cust_id", "label"],
            },
        },
    }


def _preprocessor() -> dict:
    """What the fit would produce: the entity-level feature, then the
    candidate-level ones, and a vocabulary for the candidate categorical."""
    return {
        "feature_columns": ["prod_name", "total_aum", "browse_30m", "page_type"],
        "categorical_columns": ["prod_name", "page_type"],
        "category_mappings": {"prod_name": list(_ITEMS), "page_type": list(_PAGES)},
        "drop_columns": ["snap_date", "cust_id", "label"],
    }


def _plan(*months) -> SnapDatePlan:
    """The entity-level table's month plan (B8's own input)."""
    return SnapDatePlan(to_process=[pd.Timestamp(m) for m in months], skipped=[])


def _months(*months) -> list:
    """The months a build reads."""
    return [pd.Timestamp(m) for m in months]


def _keys(spark):
    """Request r1 showed p0 and p1; request r2 showed p0 and p1 again."""
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH] * 4),
        "cust_id": ["C1"] * 4,
        "req_id": ["r1", "r1", "r2", "r2"],
        "prod_name": ["p0", "p1", "p0", "p1"],
    }))


def _labels(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH]),
        "cust_id": ["C1"],
        "req_id": ["r1"],
        "prod_name": ["p0"],
        "label": [1],
    }))


def _preprocessed_feature_table(spark):
    """The entity-level table, already encoded: one row per (time, entity)."""
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH]),
        "cust_id": ["C1"],
        "total_aum": [100.0],
    }))


def _candidate_feature_table(spark):
    """One row per candidate — except (r2, p1), which has none.

    Every row's ``browse_30m`` is distinct, so a candidate that picked up
    another candidate's row is visible by value. ``impression_id`` is a
    non-feature column the table carries, as a real one would. The row in the
    other month shares every other key with (r1, p0); only the months filter
    keeps it from being read (see the last test of the class).
    """
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH, _MONTH, _MONTH, _OTHER_MONTH]),
        "cust_id": ["C1"] * 4,
        "req_id": ["r1", "r1", "r2", "r1"],
        "prod_name": ["p0", "p1", "p0", "p0"],
        "impression_id": ["i1", "i2", "i3", "i9"],
        "browse_30m": [1, 2, 3, 99],
        "page_type": ["home", "search", "unseen", "home"],
    }))


def _by_candidate(result, column):
    return {(r["req_id"], r["prod_name"]): r[column] for r in result.collect()}


class TestBuildModelInputJoinsTheCandidateTable:
    def _build(self, spark, months=(_MONTH,)):
        return build_model_input(
            _keys(spark), _preprocessed_feature_table(spark), _labels(spark),
            _preprocessor(), _params(),
            _candidate_feature_table(spark), months=_months(*months),
        )

    def test_each_candidate_gets_its_own_row_and_a_miss_is_null(self, spark):
        """Joined on identity: one candidate, one row of the table.

        A key missing a role — the hand-spelled ``[time, *entity, item]`` that
        forgets ``occasion`` — matches the same item's row in the other request
        too: the row count grows and the values mix, which is what the two
        assertions below see (4 rows become 6). A key missing ``item`` fails
        louder still, as an ambiguous ``prod_name`` inside the build.
        """
        result = self._build(spark)

        assert result.count() == 4
        assert _by_candidate(result, "browse_30m") == {
            ("r1", "p0"): 1.0,
            ("r1", "p1"): 2.0,
            ("r2", "p0"): 3.0,
            # No row in the candidate table: the candidate stays, its
            # candidate-level features are NULL (same contract as ADR-0005's
            # entity-level miss).
            ("r2", "p1"): None,
        }

    def test_the_entity_level_features_still_reach_every_candidate(self, spark):
        result = self._build(spark)

        assert set(_by_candidate(result, "total_aum").values()) == {100.0}

    def test_its_categoricals_are_encoded_with_the_fitted_vocabulary(self, spark):
        """Same encoding as the entity-level table: the index in the vocabulary,
        and the unknown sentinel for a value the train months never showed."""
        result = self._build(spark)

        assert _by_candidate(result, "page_type") == {
            ("r1", "p0"): 0.0,
            ("r1", "p1"): 1.0,
            ("r2", "p0"): -1.0,
            ("r2", "p1"): None,
        }

    def test_only_the_features_and_identity_come_through(self, spark):
        """``impression_id`` is in the table but is not a feature, so it must not
        reach model_input — it would be one more column for training to read."""
        result = self._build(spark)

        assert set(result.columns) == {
            "snap_date", "cust_id", "req_id", "prod_name", "label",
            "total_aum", "browse_30m", "page_type",
        }

    def test_only_the_months_this_run_reads_are_joined(self, spark):
        """The filter is about cost, not correctness — identity includes time, so
        another month's rows could never match — which is exactly why no other
        assertion here would notice it missing. A plan that names only the
        other month leaves nothing for this month's candidates to match; without
        the filter they would still find their rows."""
        assert _by_candidate(self._build(spark), "browse_30m")[("r1", "p0")] == 1.0
        assert set(
            _by_candidate(self._build(spark, months=(_OTHER_MONTH,)), "browse_30m")
            .values()
        ) == {None}


class TestWithoutACandidateTable:
    def test_the_output_is_what_it_was(self, spark):
        """No candidate table declared: the candidate-level columns are simply
        absent, and nothing else about the output moves."""
        preprocessor = {
            **_preprocessor(),
            "feature_columns": ["prod_name", "total_aum"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": list(_ITEMS)},
        }
        result = build_model_input(
            _keys(spark), _preprocessed_feature_table(spark), _labels(spark),
            preprocessor, _params(), months=_months(_MONTH),
        )

        assert set(result.columns) == {
            "snap_date", "cust_id", "req_id", "prod_name", "label", "total_aum",
        }
        assert _by_candidate(result, "label") == {
            ("r1", "p0"): 1, ("r1", "p1"): 0, ("r2", "p0"): 0, ("r2", "p1"): 0,
        }


# --- fit ---------------------------------------------------------------------


def _fit_params() -> dict:
    """``impression_id`` is dropped: it lives in the candidate table and is not a
    feature. Left out of drop_columns it would be derived as one."""
    params = _params()
    params["dataset"]["prepare_model_input"]["drop_columns"] = [
        "snap_date", "cust_id", "label", "impression_id",
    ]
    return params


def _raw_feature_table(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH, _OTHER_MONTH]),
        "cust_id": ["C1", "C1"],
        "total_aum": [100.0, 110.0],
    }))


def _raw_candidate_table(spark, months=(_MONTH, _OTHER_MONTH)):
    """``promo`` appears only in the val month: a vocabulary fit on anything
    wider than the train months would contain it."""
    rows = {
        _MONTH: [("r1", "p0", "i1", 1, "home"), ("r1", "p1", "i2", 2, "search")],
        _OTHER_MONTH: [("r3", "p0", "i3", 3, "promo")],
    }
    records = [
        {"snap_date": pd.Timestamp(m), "cust_id": "C1", "req_id": r,
         "prod_name": p, "impression_id": i, "browse_30m": b, "page_type": pt}
        for m in months for (r, p, i, b, pt) in rows[m]
    ]
    return spark.createDataFrame(pd.DataFrame(records))


class TestFitPreprocessorMetadataReadsTheCandidateTable:
    def test_its_features_follow_the_entity_level_ones(self, spark):
        """The order is LightGBM's feature order, so it is a contract: identity
        categoricals, the entity-level table's features, then the candidate
        table's. Its identity columns (the join key) and dropped columns are
        not features."""
        preprocessor, _ = fit_preprocessor_metadata(
            _raw_feature_table(spark), _fit_params(), _raw_candidate_table(spark),
        )

        assert preprocessor["feature_columns"] == [
            "prod_name", "total_aum", "browse_30m", "page_type",
        ]

    def test_its_vocabulary_comes_from_the_train_months_only(self, spark):
        _, mappings = fit_preprocessor_metadata(
            _raw_feature_table(spark), _fit_params(), _raw_candidate_table(spark),
        )

        assert mappings["page_type"] == ["home", "search"]

    def test_the_item_vocabulary_still_comes_from_the_schema(self, spark):
        """The candidate table carries ``prod_name`` as part of its key. That
        must not make it a column whose vocabulary is read from the data — the
        declaration is the full domain; this table only holds what was shown."""
        _, mappings = fit_preprocessor_metadata(
            _raw_feature_table(spark), _fit_params(), _raw_candidate_table(spark),
        )

        assert mappings["prod_name"] == list(_ITEMS)

    def test_a_train_month_missing_from_it_is_an_error(self, spark):
        """Same rule as the entity-level table: a month that went missing
        upstream would otherwise fit a smaller vocabulary and train on NULL
        features without a word."""
        with pytest.raises(ValueError, match="candidate_feature_table missing required train_snap_dates"):
            fit_preprocessor_metadata(
                _raw_feature_table(spark), _fit_params(),
                _raw_candidate_table(spark, months=(_OTHER_MONTH,)),
            )

    def test_without_one_the_fit_is_what_it_was(self, spark):
        params = _fit_params()
        params["dataset"]["prepare_model_input"]["categorical_columns"] = ["prod_name"]
        preprocessor, mappings = fit_preprocessor_metadata(
            _raw_feature_table(spark), params,
        )

        assert preprocessor["feature_columns"] == ["prod_name", "total_aum"]
        assert set(mappings) == {"prod_name"}


class TestUnusedDropColumnsAreReportedAcrossBothTables:
    """``drop_columns`` applies to both feature tables, so a name is unused only
    when neither has it. The warning used to live in
    ``apply_preprocessor_to_features``, which sees the entity-level table alone
    and would call ``impression_id`` unused."""

    def _warned(self, caplog):
        return [
            r.getMessage() for r in caplog.records
            if "drop_columns not found" in r.getMessage()
        ]

    def test_a_column_only_the_candidate_table_has_is_not_unused(self, spark, caplog):
        with caplog.at_level("WARNING"):
            fit_preprocessor_metadata(
                _raw_feature_table(spark), _fit_params(), _raw_candidate_table(spark),
            )

        assert not any("impression_id" in m for m in self._warned(caplog))

    def test_a_column_neither_table_has_is(self, spark, caplog):
        params = _fit_params()
        params["dataset"]["prepare_model_input"]["drop_columns"].append("gone_col")
        with caplog.at_level("WARNING"):
            fit_preprocessor_metadata(
                _raw_feature_table(spark), params, _raw_candidate_table(spark),
            )

        assert any("gone_col" in m for m in self._warned(caplog))


# --- the Layer-2 data gate -----------------------------------------------------


def _sample_pool(spark):
    """Shows every declared item (B1 requires it), p2 included — the candidate
    table has no row for that one, which is a miss, not an error."""
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_MONTH, _MONTH, _MONTH, _OTHER_MONTH]),
        "cust_id": ["C1"] * 4,
        "req_id": ["r1", "r1", "r2", "r3"],
        "prod_name": ["p0", "p1", "p2", "p0"],
        "label": [1, 0, 0, 0],
    }))


def _gate(spark, candidate, params=None):
    validate_data_consistency(
        _sample_pool(spark), _labels(spark), _raw_feature_table(spark),
        params or _fit_params(), candidate,
    )


class TestTheDataGateChecksTheCandidateTable:
    """Every check the gate makes of ``feature_table``'s columns, it makes of the
    candidate table's too — and names that table when it fails. Two more are
    about the pair: the candidate table's join key (B13) and a feature both
    tables hold (B14)."""

    def test_a_well_formed_one_passes(self, spark):
        _gate(spark, _raw_candidate_table(spark))

    def test_a_missing_identity_column_is_B13(self, spark):
        with pytest.raises(DataConsistencyError, match=r"B13: candidate_feature_table is missing identity column\(s\) \['req_id'\]"):
            _gate(spark, _raw_candidate_table(spark).drop("req_id"))

    def test_a_feature_in_both_tables_is_B14(self, spark):
        with pytest.raises(DataConsistencyError, match=r"B14: column\(s\) \['total_aum'\]"):
            _gate(spark, _raw_candidate_table(spark).withColumn("total_aum", F.lit(1.0)))

    def test_an_undropped_string_column_in_it_is_B6(self, spark):
        """``impression_id`` left out of drop_columns: a string the model cannot
        read, and would OOM on as an object column (B6's reason)."""
        params = _fit_params()
        params["dataset"]["prepare_model_input"]["drop_columns"].remove("impression_id")
        with pytest.raises(DataConsistencyError, match="'impression_id'"):
            _gate(spark, _raw_candidate_table(spark), params)

    def test_a_categorical_of_the_wrong_type_is_B5_and_names_the_table(self, spark):
        candidate = _raw_candidate_table(spark).withColumn(
            "page_type", F.lit(1.5),
        )
        with pytest.raises(DataConsistencyError, match=r"'page_type' is .* in candidate_feature_table"):
            _gate(spark, candidate)

    def test_a_carried_column_in_it_is_B7_and_names_the_table(self, spark):
        params = _fit_params()
        params["dataset"]["carry_columns"] = ["device"]
        pool = _sample_pool(spark).withColumn("device", F.lit("ios"))
        candidate = _raw_candidate_table(spark).withColumn("device", F.lit(1))
        with pytest.raises(DataConsistencyError, match="also a column of candidate_feature_table"):
            validate_data_consistency(
                pool, _labels(spark), _raw_feature_table(spark), params, candidate,
            )


# --- B8 and month coverage -----------------------------------------------------

#: The first integer float32 cannot hold exactly: 2**24 + 1 lands on 2**24.
_BEYOND_FLOAT32 = 2**24 + 1
#: A test month this run builds, and one an earlier run already landed.
_TEST_MONTH = "2024-03-31"
_LANDED_TEST_MONTH = "2023-12-31"


def _precision_params(policy="block", train=(_MONTH,), val=()) -> dict:
    params = _fit_params()
    params["dataset"]["numeric_precision_policy"] = policy
    params["dataset"]["train_snap_dates"] = list(train)
    params["dataset"]["val_snap_dates"] = list(val)
    return params


def _entity_side(spark):
    """An entity-level table whose only feature is a double: no value grid, so
    B8 has nothing to check there and every finding below is the candidate
    table's."""
    return _preprocessed_feature_table(spark)


def _candidate_with(spark, browse_by_month: dict):
    """One candidate row per month, ``browse_30m`` set per month."""
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(list(browse_by_month)),
        "cust_id": ["C1"] * len(browse_by_month),
        "req_id": ["r1"] * len(browse_by_month),
        "prod_name": ["p0"] * len(browse_by_month),
        "impression_id": [f"i{n}" for n in range(len(browse_by_month))],
        "browse_30m": list(browse_by_month.values()),
        "page_type": ["home"] * len(browse_by_month),
    }))


def _check(
    spark, candidate, train=(_MONTH,), val=(), test=(), skipped=(),
    only_test_months=False, policy="block",
):
    """The gate with each build's months where the builds read them from: the
    train and val months in the config, the test months in the test build's
    plan (``skipped`` are test months an earlier run landed)."""
    test_plan = SnapDatePlan(to_process=_months(*test), skipped=_months(*skipped))
    return validate_numeric_precision(
        _entity_side(spark), _preprocessor(), _plan(),
        _precision_params(policy, train=train, val=val),
        candidate, test_plan, only_test_months,
    )


class TestPrecisionGateCoversTheCandidateTable:
    """The candidate table is cast to the storage type inside build_model_input
    like every other feature, so B8 has to see it. It is never landed, so there
    are no footers to read: one aggregation over the months this run reads
    stands in for them (ADR-0026's recorded exception to ADR-0006)."""

    def test_a_value_float32_cannot_hold_is_a_breach(self, spark):
        with pytest.raises(DataConsistencyError, match="candidate_feature_table.*browse_30m"):
            _check(spark, _candidate_with(spark, {_MONTH: _BEYOND_FLOAT32}))

    def test_a_value_it_can_hold_passes_and_is_reported(self, spark):
        report = _check(spark, _candidate_with(spark, {_MONTH: 3}))

        section = report["candidate_feature_table"]
        assert [(c["column"], c["max_abs"], c["verdict"]) for c in section["columns"]] == [
            ("browse_30m", 3.0, "ok"),
        ]
        assert section["months"] == [_MONTH]

    def test_only_the_months_this_run_reads_are_scanned(self, spark):
        """A value in a month nothing reads this run is not this run's to judge."""
        report = _check(
            spark,
            _candidate_with(spark, {_MONTH: 3, _OTHER_MONTH: _BEYOND_FLOAT32}),
        )

        assert report["candidate_feature_table"]["columns"][0]["max_abs"] == 3.0

    def test_the_months_are_what_each_build_reads(self, spark):
        """Each build's own rule, the union checked: the train months, the val
        months, and the test plan's ``to_process`` — not the test month an
        earlier run landed, which no build reads this run (ADR-0029
        decision 2)."""
        report = _check(
            spark,
            _candidate_with(spark, {
                _MONTH: 3, _OTHER_MONTH: 3, _TEST_MONTH: 3,
                _LANDED_TEST_MONTH: _BEYOND_FLOAT32,
            }),
            train=(_MONTH,), val=(_OTHER_MONTH,), test=(_TEST_MONTH,),
            skipped=(_LANDED_TEST_MONTH,),
        )

        assert report["candidate_feature_table"]["months"] == [
            _MONTH, _OTHER_MONTH, _TEST_MONTH,
        ]

    def test_only_test_months_checks_the_test_build_alone(self, spark):
        """Under ``--only-test-months`` the test build is the only one in the
        run, so the train and val months — configured, and holding a breach —
        are not this run's to scan. The mode exists to make adding an eval
        month cheap; scanning the train months of the biggest table in the
        deployment would undo that."""
        report = _check(
            spark,
            _candidate_with(spark, {
                _MONTH: _BEYOND_FLOAT32, _OTHER_MONTH: _BEYOND_FLOAT32,
                _TEST_MONTH: 3,
            }),
            train=(_MONTH,), val=(_OTHER_MONTH,), test=(_TEST_MONTH,),
            only_test_months=True,
        )

        assert report["candidate_feature_table"]["months"] == [_TEST_MONTH]
        assert report["candidate_feature_table"]["breaches"] == 0

    def test_truncate_lets_a_breach_through(self, spark):
        report = _check(
            spark, _candidate_with(spark, {_MONTH: _BEYOND_FLOAT32}), policy="truncate",
        )

        assert report["candidate_feature_table"]["breaches"] == 1

    def test_a_month_it_does_not_have_is_an_error_whatever_the_policy(self, spark):
        """A month missing from a table that is read in full every run would
        become a month of NULL features, silently. Not a precision question, so
        ``truncate`` does not let it through."""
        with pytest.raises(ValueError, match=r"candidate_feature_table missing required snap_dates: \['2024-02-29'\]"):
            _check(
                spark, _candidate_with(spark, {_MONTH: 3}),
                val=(_OTHER_MONTH,), policy="truncate",
            )

    def test_without_one_the_report_has_no_such_section(self, spark):
        report = validate_numeric_precision(
            _entity_side(spark), _preprocessor(), _plan(), _precision_params(),
        )

        assert "candidate_feature_table" not in report

    def test_an_integer_categorical_is_judged_by_its_index_not_its_code(self, spark):
        """``campaign_code`` is declared categorical: build_model_input encodes
        it to a vocabulary index before the cast, so its raw values — however
        large — are never what gets narrowed."""
        preprocessor = {
            **_preprocessor(),
            "feature_columns": [*_preprocessor()["feature_columns"], "campaign_code"],
            "categorical_columns": [*_preprocessor()["categorical_columns"], "campaign_code"],
        }
        candidate = _candidate_with(spark, {_MONTH: 3}).withColumn(
            "campaign_code", F.lit(_BEYOND_FLOAT32).cast("bigint"),
        )

        report = validate_numeric_precision(
            _entity_side(spark), preprocessor, _plan(), _precision_params(),
            candidate,
        )

        assert [c["column"] for c in report["candidate_feature_table"]["columns"]] == [
            "browse_30m",
        ]

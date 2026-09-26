"""A multi-column ``item`` through the dataset nodes (#394, ADR-0027).

The user's tables carry ``campaign_id`` and ``creative_format``; every node that
reads one combines them into ``item`` first. The reference each test compares
against is the same data with the columns already combined in "SQL" — a
single-column ``item`` named ``item`` — which is exactly what a deployment had
to hand the framework before #394. Same rows in, same rows out, or the
combining is not what the SQL did.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.pipelines.dataset.nodes import (
    build_model_input,
    filter_train_keys,
    fit_preprocessor_metadata,
    select_test_keys,
    select_sample_keys,
    select_val_keys,
    validate_data_consistency,
)
from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan

pytestmark = pytest.mark.spark

_TRAIN = "2024-01-31"
_VAL = "2024-02-29"
_TEST = "2024-03-31"
_COMBOS = [("c01", "banner"), ("c01", "video"), ("cmp-2", "banner")]
_ITEMS = [f"{c}-{f}" for c, f in _COMBOS]


def _params(item=("campaign_id", "creative_format"), **dataset) -> dict:
    return {
        "random_seed": 42,
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["user_id"], "item": list(item),
            },
            "categorical_values": {"item": list(_ITEMS)},
        },
        "dataset": {
            "train_snap_dates": [_TRAIN],
            "val_snap_dates": [_VAL],
            "test_snap_dates": [_TEST],
            "sample_ratio": 0.5,
            "sample_group_keys": ["item"],
            "sample_ratio_overrides": {},
            "val_sample_ratio": 1.0,
            "prepare_model_input": {
                "categorical_columns": ["item", "page_type"],
                "drop_columns": ["snap_date", "user_id", "label"],
            },
            **dataset,
        },
    }


#: The pre-#394 shape: the same data, combined upstream into one column.
_COMBINED_PARAMS = _params(item=("item",))


def _pool_records(n_users=60):
    return [
        {"snap_date": pd.Timestamp(month), "user_id": f"u{u:03d}",
         "campaign_id": c, "creative_format": f,
         "label": int((u + i) % 4 == 0)}
        for month in (_TRAIN, _VAL, _TEST)
        for u in range(n_users)
        for i, (c, f) in enumerate(_COMBOS)
    ]


def _split(spark, records):
    """The same rows twice: source columns, and combined as the SQL would."""
    pdf = pd.DataFrame(records)
    combined = pdf.assign(item=pdf["campaign_id"] + "-" + pdf["creative_format"])
    combined = combined.drop(columns=["campaign_id", "creative_format"])
    return spark.createDataFrame(pdf), spark.createDataFrame(combined)


def _rows(df):
    return sorted(tuple(r) for r in df.select(*sorted(df.columns)).collect())


class TestKeysAreTheSqlCombinedKeys:
    def test_train_keys_draw_the_same_rows(self, spark):
        """The draw hashes identity; a combined value equal to the SQL's is
        the same hash, so the same rows survive."""
        raw, combined = _split(spark, _pool_records())
        got = select_sample_keys(raw, _params())
        want = select_sample_keys(combined, _COMBINED_PARAMS)
        assert "campaign_id" not in got.columns
        assert _rows(got) == _rows(want)
        # The draw really dropped rows, so equality is not "both kept all".
        assert got.count() < len(_COMBOS) * 60

    def test_val_keys(self, spark):
        raw, combined = _split(spark, _pool_records())
        assert _rows(select_val_keys(raw, _params())) == _rows(
            select_val_keys(combined, _COMBINED_PARAMS)
        )

    def test_test_keys(self, spark):
        raw, combined = _split(spark, _pool_records())
        plan = SnapDatePlan(to_process=[pd.Timestamp(_TEST)], skipped=[])
        got = select_test_keys(raw, plan, _params())
        assert {r["item"] for r in got.collect()} == set(_ITEMS)
        assert _rows(got) == _rows(select_test_keys(combined, plan, _COMBINED_PARAMS))


def _keys(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_TRAIN] * 3),
        "user_id": ["u1"] * 3,
        "item": list(_ITEMS),
    }))


def _labels(spark):
    """Only ``cmp-2-banner`` is a positive — the value holding its own dash."""
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_TRAIN]),
        "user_id": ["u1"],
        "campaign_id": ["cmp-2"],
        "creative_format": ["banner"],
        "label": [1],
    }))


def _candidate_table(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_TRAIN] * 3),
        "user_id": ["u1"] * 3,
        "campaign_id": [c for c, _ in _COMBOS],
        "creative_format": [f for _, f in _COMBOS],
        "browse_30m": [1, 2, 3],
        "page_type": ["home", "search", "home"],
    }))


def _feature_table(spark):
    return spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime([_TRAIN, _VAL]),
        "user_id": ["u1", "u1"],
        "total_aum": [100.0, 110.0],
    }))


class TestModelInputReadsTheSourceColumns:
    def test_labels_and_candidate_features_land_on_their_candidate(self, spark):
        preprocessor = {
            "feature_columns": ["item", "total_aum", "browse_30m", "page_type"],
            "categorical_columns": ["item", "page_type"],
            "category_mappings": {"item": list(_ITEMS), "page_type": ["home", "search"]},
            "drop_columns": ["snap_date", "user_id", "label"],
        }
        result = build_model_input(
            _keys(spark), _feature_table(spark),
            _labels(spark), preprocessor, _params(),
            _candidate_table(spark), months=[pd.Timestamp(_TRAIN)],
        )
        rows = {r["item"]: r for r in result.collect()}
        assert {i: rows[i]["label"] for i in _ITEMS} == {
            "c01-banner": 0, "c01-video": 0, "cmp-2-banner": 1,
        }
        assert {i: rows[i]["browse_30m"] for i in _ITEMS} == {
            "c01-banner": 1, "c01-video": 2, "cmp-2-banner": 3,
        }
        assert "campaign_id" not in result.columns

    def test_the_source_columns_are_not_features(self, spark):
        """Every non-identity column of the candidate table is a feature;
        combining drops the source columns before the fit sees them."""
        params = _params()
        preprocessor = fit_preprocessor_metadata(
            _feature_table(spark), params, _candidate_table(spark),
        )
        assert preprocessor["feature_columns"] == [
            "item", "total_aum", "browse_30m", "page_type",
        ]

    def test_the_zero_positive_filter_reads_the_combined_labels(self, spark):
        """``u1`` holds a positive (on ``cmp-2-banner``) and ``u2`` holds
        none; with a ratio of 0 only a group holding a positive survives."""
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime([_TRAIN] * 2),
            "user_id": ["u1", "u2"],
            "item": ["cmp-2-banner", "cmp-2-banner"],
        }))
        out = filter_train_keys(
            keys, _labels(spark), _params(train_zero_positive_group_ratio=0.0),
        )
        assert [r["user_id"] for r in out.collect()] == ["u1"]


def _gate(spark, pool_records, label_rows=None, params=None, candidate=None):
    pool = spark.createDataFrame(pd.DataFrame(pool_records))
    labels = spark.createDataFrame(pd.DataFrame(
        label_rows if label_rows is not None else pool_records
    ))
    validate_data_consistency(
        pool, labels, _feature_table(spark), params or _params(), candidate,
    )


class TestTheDataGate:
    def test_a_well_formed_multi_column_item_passes(self, spark):
        _gate(spark, _pool_records(n_users=2), candidate=_candidate_table(spark))

    def test_B1_compares_the_combined_values(self, spark):
        records = _pool_records(n_users=2)
        records.append({**records[0], "campaign_id": "c09"})
        with pytest.raises(DataConsistencyError, match=r"sample_pool has item value\(s\) \['c09-banner'\]"):
            _gate(spark, records)

    def test_B15_two_combinations_with_one_value(self, spark):
        """``a-b`` + ``c`` in sample_pool and ``a`` + ``b-c`` in label_table
        are both ``a-b-c``. Declared, so B1 alone would let it through."""
        pool = _pool_records(n_users=2)
        pool.append({**pool[0], "campaign_id": "a-b", "creative_format": "c"})
        labels = [{**pool[0], "campaign_id": "a", "creative_format": "b-c"}]
        params = _params()
        params["schema"]["categorical_values"]["item"].append("a-b-c")
        with pytest.raises(DataConsistencyError, match="B15: .*'a-b-c'"):
            _gate(spark, pool, labels, params)

    def test_B16_a_table_missing_a_source_column(self, spark):
        labels = [
            {k: v for k, v in r.items() if k != "creative_format"}
            for r in _pool_records(n_users=2)
        ]
        with pytest.raises(DataConsistencyError, match=r"B16: label_table is missing item column\(s\) \['creative_format'\]"):
            _gate(spark, _pool_records(n_users=2), labels)

    def test_B17_an_item_column_typed_differently_in_the_candidate_table(self, spark):
        """``7`` in sample_pool and ``7.0`` in the candidate table would combine
        to different items, and the candidate's features would silently be
        NULL."""
        candidate = _candidate_table(spark).withColumn(
            "creative_format", F.lit(1.0))
        with pytest.raises(DataConsistencyError, match="B17: item column 'creative_format'"):
            _gate(spark, _pool_records(n_users=2), candidate=candidate)

    def test_B16_a_candidate_table_that_already_has_item(self, spark):
        candidate = _candidate_table(spark).withColumn("item", F.lit("x"))
        with pytest.raises(DataConsistencyError, match="B16: candidate_feature_table already has a column named 'item'"):
            _gate(spark, _pool_records(n_users=2), candidate=candidate)

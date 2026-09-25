"""Every read of a source or landed table names the months it reads (ADR-0029
decisions 1-3).

Two kinds of test, because the two decisions fail differently:

- **Which months a read names** (decisions 1 and 2). The joins key on time, so
  another month's rows can never match: the output is the same with or without
  the filter, and no assertion on rows can see it go missing. What changes is
  the plan, so the plan is what is asserted on — the *analyzed* plan, which is
  what the code wrote. The optimized plan would not do: Spark infers month
  conditions from the other side of a join (constraint propagation, dynamic
  partition pruning) exactly where it happens to be able to, and a test reading
  that would pass for the reason ADR-0029 decision 1 refuses to rely on.
- **How a month is compared** (decision 3). A time column stored as STRING
  (how Hive hands back a partition column) used to match no month in some
  reads while matching in others. There the output does change, so rows are
  asserted on, against the same data with a DATE time column as the control.
"""

import re

import pandas as pd
import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan
from recsys_tfb.pipelines.dataset.nodes import (
    fit_preprocessor_metadata,
    select_train_keys,
    select_val_keys,
    validate_data_consistency,
)
from recsys_tfb.pipelines.dataset.steps.categoricals import count_items_in_months
from recsys_tfb.pipelines.dataset.steps.scoping import require_months_present

pytestmark = pytest.mark.spark

_TRAIN = ["2025-01-31", "2025-02-28"]
_VAL = ["2025-03-31"]
_TEST = ["2025-04-30"]
#: Months the source tables hold that this run's builds do not read: one
#: before the train months (history the table keeps), one after the test
#: month (a month not configured yet). ``2024-12-31`` doubles as a test month
#: an earlier run already landed in ``test_keys``.
_OTHER = ["2024-12-31", "2025-05-31"]
_ALL = sorted(_TRAIN + _VAL + _TEST + _OTHER)
_ENTITIES = ["C1", "C2"]
_ITEMS = ["a", "b"]


def _params(**dataset) -> dict:
    return {
        "random_seed": 42,
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            },
            "categorical_values": {"prod_name": list(_ITEMS)},
        },
        "dataset": {
            "train_snap_dates": list(_TRAIN),
            "val_snap_dates": list(_VAL),
            "test_snap_dates": list(_TEST),
            "sample_ratio": 1.0,
            "prepare_model_input": {
                # ``segment`` is the feature table's own categorical (the fit
                # tests below); declared here so the data gate's B6 has
                # nothing to say about it either.
                "categorical_columns": ["prod_name", "segment"],
                "drop_columns": ["snap_date", "cust_id", "label"],
            },
            **dataset,
        },
    }


#: What the fit would produce for the three tables below.
_PREPROCESSOR = {
    "feature_columns": ["prod_name", "f_entity", "f_candidate"],
    "categorical_columns": ["prod_name"],
    "category_mappings": {"prod_name": list(_ITEMS)},
    "drop_columns": ["snap_date", "cust_id", "label"],
}


def _candidate_rows(months) -> list[dict]:
    return [
        {"snap_date": pd.Timestamp(m), "cust_id": c, "prod_name": i}
        for m in months for c in _ENTITIES for i in _ITEMS
    ]


def _keys(spark, months):
    return spark.createDataFrame(pd.DataFrame(_candidate_rows(months)))


def _label_table(spark):
    """Every month, one positive per entity: each query group holds one."""
    return spark.createDataFrame(pd.DataFrame([
        {**row, "label": int(row["prod_name"] == _ITEMS[0])}
        for row in _candidate_rows(_ALL)
    ]))


def _preprocessed_feature_table(spark):
    """``f_entity`` is the column no other table has — how the read of this
    table is found in the plan."""
    return spark.createDataFrame(pd.DataFrame([
        {"snap_date": pd.Timestamp(m), "cust_id": c, "f_entity": 1.0}
        for m in _ALL for c in _ENTITIES
    ]))


def _candidate_feature_table(spark):
    return spark.createDataFrame(pd.DataFrame([
        {**row, "f_candidate": 2.0} for row in _candidate_rows(_ALL)
    ]))


def _catalog(spark, params, only_test_months=False) -> dict:
    """Every input any node below reads, keyed by its catalog name. The test
    keys hold a month an earlier run landed, as the persistent table does."""
    train_keys = _keys(spark, _TRAIN)
    return {
        "train_keys": train_keys,
        "train_dev_keys": train_keys,
        "train_keys_unfiltered": train_keys,
        "train_dev_keys_unfiltered": train_keys,
        "val_keys": _keys(spark, _VAL),
        "test_keys": _keys(spark, _TEST + _OTHER[:1]),
        "label_table": _label_table(spark),
        "preprocessed_feature_table": _preprocessed_feature_table(spark),
        "candidate_feature_table": _candidate_feature_table(spark),
        "preprocessor": _PREPROCESSOR,
        "parameters": params,
        "test_model_input_month_plan": SnapDatePlan(
            to_process=[pd.Timestamp(m) for m in _TEST],
            skipped=[pd.Timestamp(_OTHER[0])],
        ),
        "only_test_months": only_test_months,
    }


def _run(name: str, catalog: dict):
    """Run the node the pipeline registers under ``name``, its inputs bound by
    the names the pipeline gives them — the wiring is part of what is tested."""
    from recsys_tfb.pipelines.dataset.pipeline import create_pipeline

    node = next(n for n in create_pipeline().nodes if n.name == name)
    return node.func(*[catalog[i] for i in node.inputs])


def _months_read(frame: DataFrame, marker: str) -> list[set[str]]:
    """For each read of the table holding column ``marker``: the months named by
    the filters between that read and the first join above it.

    From the analyzed plan: what the code wrote, before the optimizer infers or
    pushes anything. A filter the code puts *after* the join is above it here,
    so it does not count — the read would still be the whole table.
    """
    found: list[set[str]] = []

    def walk(node, months: set[str]) -> None:
        name = node.nodeName()
        if name == "Join":
            months = set()
        elif name == "Filter":
            months = months | set(
                re.findall(r"\d{4}-\d{2}-\d{2}", node.condition().sql())
            )
        children = node.children()
        if children.size() == 0:
            output = node.output()
            if marker in {output.apply(i).name() for i in range(output.size())}:
                found.append(months)
            return
        for i in range(children.size()):
            walk(children.apply(i), months)

    walk(frame._jdf.queryExecution().analyzed(), set())
    assert found, f"the plan reads no table holding {marker!r}"
    return found


class TestEachBuildReadsOnlyItsSplitsMonths:
    """Decisions 1 and 2: train and train_dev read ``train_snap_dates``, val
    reads ``val_snap_dates``, test reads its plan's ``to_process`` — and the
    same answer serves all three right tables."""

    @pytest.mark.parametrize("node, months", [
        ("build_train_model_input", _TRAIN),
        ("build_train_dev_model_input", _TRAIN),
        ("build_val_model_input", _VAL),
        ("build_test_model_input", _TEST),
    ])
    @pytest.mark.parametrize("table, marker", [
        ("label_table", "label"),
        ("preprocessed_feature_table", "f_entity"),
        ("candidate_feature_table", "f_candidate"),
    ])
    def test_every_right_table_is_filtered_before_its_join(
        self, spark, node, months, table, marker,
    ):
        built = _run(node, _catalog(spark, _params()))

        assert _months_read(built, marker) == [set(months)], table

    def test_the_output_does_not_change(self, spark):
        """Why only the plan can show the filter: the joins key on time, so
        the rows of another month never matched. Every key keeps its own
        label and features."""
        built = _run("build_val_model_input", _catalog(spark, _params()))

        rows = {
            (r["snap_date"].strftime("%Y-%m-%d"), r["cust_id"], r["prod_name"]):
                (r["label"], r["f_entity"], r["f_candidate"])
            for r in built.collect()
        }
        assert rows == {
            (m, c, i): (int(i == _ITEMS[0]), 1.0, 2.0)
            for m in _VAL for c in _ENTITIES for i in _ITEMS
        }


class TestTheGroupDropReadsOnlyTheTrainMonths:
    """Decision 1: the narrow ``label_table`` the train-side group drop joins is
    filtered to the train months too; train_dev is carved out of them."""

    @pytest.mark.parametrize("node", ["filter_train_keys", "filter_train_dev_keys"])
    def test_its_label_table_is_filtered_before_the_join(self, spark, node):
        params = _params(train_zero_positive_group_ratio=0.5)

        kept = _run(node, _catalog(spark, params))

        assert _months_read(kept, "label") == [set(_TRAIN)]


class TestMonthPresenceAsksOnlyTheMonthsItChecks:
    """Decision 1: ``require_months_present`` filters to the months it checks
    before asking which months exist, instead of reading the whole time
    column."""

    def test_the_question_is_asked_of_those_months_only(self, spark, monkeypatch):
        collected: list[DataFrame] = []
        real_collect = DataFrame.collect

        def spy(frame):
            collected.append(frame)
            return real_collect(frame)

        monkeypatch.setattr(DataFrame, "collect", spy)

        require_months_present(
            _preprocessed_feature_table(spark), "snap_date",
            [pd.Timestamp(m) for m in _TRAIN], "train_snap_dates",
        )

        assert [_months_read(f, "f_entity") for f in collected] == [[set(_TRAIN)]]

    def test_a_missing_month_is_still_named(self, spark):
        with pytest.raises(ValueError, match=r"missing required train_snap_dates: \['2026-01-31'\]"):
            require_months_present(
                _preprocessed_feature_table(spark), "snap_date",
                [pd.Timestamp(m) for m in _TRAIN + ["2026-01-31"]],
                "train_snap_dates",
            )


# --- decision 3: one comparison, whatever the time column's type ------------


def _as(frame: DataFrame, time_type: str) -> DataFrame:
    """``frame`` with its time column as ``time_type``: DATE as the source
    tables here hold it, STRING as a Hive partition column comes back."""
    if time_type == "date":
        return frame.withColumn("snap_date", F.to_date("snap_date"))
    return frame.withColumn(
        "snap_date", F.date_format("snap_date", "yyyy-MM-dd"),
    )


def _sample_pool(spark, time_type, *extra_rows):
    """Every month's candidates, and ``late`` — an item offered only in a
    month no split reads."""
    rows = _candidate_rows(_ALL) + [
        {"snap_date": pd.Timestamp(_OTHER[-1]), "cust_id": "C1", "prod_name": "late"},
        *extra_rows,
    ]
    return _as(spark.createDataFrame(pd.DataFrame(rows)), time_type)


def _feature_table(spark, time_type):
    """``segment`` takes ``x``/``y`` in the train months and ``z`` elsewhere, so
    the vocabulary shows which months the fit read."""
    rows = [
        {"snap_date": pd.Timestamp(m), "cust_id": c,
         "segment": ({"C1": "x", "C2": "y"}[c] if m in _TRAIN else "z")}
        for m in _ALL for c in _ENTITIES
    ]
    return _as(spark.createDataFrame(pd.DataFrame(rows)), time_type)


@pytest.mark.parametrize("time_type", ["date", "string"])
class TestAStringTimeColumnReadsLikeADateOne:

    def test_the_fit_reads_the_train_months(self, spark, time_type):
        preprocessor, _ = fit_preprocessor_metadata(
            _feature_table(spark, time_type), _params(),
        )

        assert preprocessor["category_mappings"]["segment"] == ["x", "y"]

    def test_the_counted_item_list_reads_the_train_months(self, spark, time_type):
        items = count_items_in_months(
            _sample_pool(spark, time_type), get_schema(_params()),
            [pd.Timestamp(m) for m in _TRAIN],
        )

        assert items == _ITEMS

    @pytest.mark.parametrize("select, months", [
        (select_train_keys, _TRAIN), (select_val_keys, _VAL),
    ])
    def test_key_selection_reads_its_months(self, spark, time_type, select, months):
        keys = select(_sample_pool(spark, time_type), _params())

        assert keys.count() == len(months) * len(_ENTITIES) * len(_ITEMS)

    def test_the_data_gate_sees_the_dataset_months(self, spark, time_type):
        """B1 over the months the dataset reads: ``late`` sits in a month
        nobody reads, so only ``stray`` — offered in a val month and not
        declared — is reported."""
        pool = _sample_pool(spark, time_type, {
            "snap_date": pd.Timestamp(_VAL[0]), "cust_id": "C1", "prod_name": "stray",
        })

        with pytest.raises(DataConsistencyError) as raised:
            validate_data_consistency(
                pool, _as(_label_table(spark), time_type),
                _feature_table(spark, time_type), _params(),
            )

        assert "stray" in str(raised.value)
        assert "late" not in str(raised.value)

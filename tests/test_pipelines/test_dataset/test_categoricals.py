"""Tests for ``pipelines/dataset/steps/categoricals.py`` — the vocabulary fit.

The node-level behaviour (train months only, identity categoricals from the
schema, the B5 backstop) is tested in ``test_nodes.py``; this file pins what
``collect_vocabularies_from_data`` itself promises: the same vocabulary, in the
same order, as the per-column form it replaced — at a Spark cost that does not
grow with the number of columns.
"""

import datetime as dt
import uuid

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from recsys_tfb.pipelines.dataset.steps.categoricals import (
    collect_vocabularies_from_data,
)

pytestmark = pytest.mark.spark


def _per_column_reference(df, columns):
    """The per-column form this function had until 2026-09-19, kept as the oracle.

    A value's index in the vocabulary is its encoded value, so a trained model
    depends on the order. "Same list as the form it replaced" is the rewrite's
    whole correctness claim, stated as a comparison.
    """
    return {
        col: [
            r[col]
            for r in df.select(col)
            .filter(F.col(col).isNotNull())
            .distinct()
            .orderBy(col)
            .collect()
        ]
        for col in columns
    }


# Enough distinct strings that hash order and sorted order disagree, with the
# orderings a Python sort could get wrong against Spark's: case, digits that
# sort as text ("10" < "9"), accented and CJK characters.
_STRINGS = [
    "b", "a", "B", "A", "10", "9", "Ä", "é", "中", "文", "z", "zz", "",
    *[f"code_{i:02d}" for i in range(20)],
]

_SCHEMA = T.StructType([
    T.StructField("s", T.StringType()),
    T.StructField("i", T.IntegerType()),
    T.StructField("big", T.LongType()),
    T.StructField("flag", T.BooleanType()),
    T.StructField("d", T.DateType()),
    T.StructField("all_null", T.StringType()),
])
_COLUMNS = [f.name for f in _SCHEMA.fields]


@pytest.fixture
def mixed_types(spark):
    """Every discrete type B5 lets through, with duplicates and NULLs in each.

    Spread over several partitions so the partial per-partition sets really are
    merged, as they are on a cluster.
    """
    rows = []
    for k in range(600):
        rows.append((
            None if k % 7 == 0 else _STRINGS[(k * 5) % len(_STRINGS)],
            None if k % 11 == 0 else [3, -1, 0, 42, -7][k % 5],
            [2**40, -(2**35), 1][k % 3],
            None if k % 13 == 0 else bool(k % 2),
            dt.date(2024, 1 + k % 12, 1),
            None,
        ))
    return spark.createDataFrame(rows, schema=_SCHEMA).repartition(8)


def test_matches_the_per_column_form_on_every_discrete_type(mixed_types):
    got = collect_vocabularies_from_data(mixed_types, _COLUMNS)
    assert got == _per_column_reference(mixed_types, _COLUMNS)


def test_the_comparison_has_something_to_compare(mixed_types):
    """The paired half: the equality above is not two empty lists agreeing.

    Also pins the one ordering a reader is likeliest to get wrong by hand:
    strings sort as text, so "10" comes before "9".
    """
    got = collect_vocabularies_from_data(mixed_types, ["s", "i", "flag"])
    assert got["s"] == sorted(_STRINGS)
    assert got["s"].index("10") < got["s"].index("9")
    assert got["i"] == [-7, -1, 0, 3, 42]
    assert got["flag"] == [False, True]


def test_an_all_null_column_gets_an_empty_vocabulary(mixed_types):
    assert collect_vocabularies_from_data(mixed_types, ["all_null"]) == {"all_null": []}


def test_no_rows_gives_every_column_an_empty_vocabulary(mixed_types):
    empty = mixed_types.limit(0)
    assert collect_vocabularies_from_data(empty, ["s", "d"]) == {"s": [], "d": []}


def test_no_columns_is_an_empty_mapping(mixed_types):
    assert collect_vocabularies_from_data(mixed_types, []) == {}


def _jobs_run_by(spark, fn) -> int:
    """How many Spark jobs ``fn`` fired, read from the status tracker."""
    sc = spark.sparkContext
    group = f"vocab-{uuid.uuid4().hex}"
    sc.setJobGroup(group, group)
    try:
        fn()
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)
    # Job-start events reach the status store through the async listener bus;
    # drain it so the count cannot race the last job's registration.
    sc._jsc.sc().listenerBus().waitUntilEmpty()
    return len(sc.statusTracker().getJobIdsForGroup(group))


def test_spark_jobs_do_not_grow_with_the_column_count(spark, mixed_types):
    """The reason for the rewrite, asserted as structure rather than as time.

    The per-column form fired several jobs and one full scan of ``df`` per
    column; at the 20–50 categoricals a production feature_table carries, that
    is 20–50 scans where one does. A value comparison cannot see this — the
    per-column form returns the same lists — so the count is what pins it.
    """
    one = _jobs_run_by(spark, lambda: collect_vocabularies_from_data(mixed_types, ["s"]))
    six = _jobs_run_by(spark, lambda: collect_vocabularies_from_data(mixed_types, _COLUMNS))
    assert one > 0
    assert six == one

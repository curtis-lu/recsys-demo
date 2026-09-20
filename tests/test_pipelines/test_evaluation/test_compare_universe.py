"""Tests for steps.compare_universe — common_universe and restrict_to_common."""

import pytest
from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.pipelines.evaluation.steps.compare_universe import (
    common_universe,
    restrict_to_common,
)

_MONTH = "2026-01-31"


def _group_tuples(df: SparkDataFrame) -> set[tuple]:
    """Collect a query-group DataFrame in the test — fixtures here are tiny.

    Production never does this: the whole point of #275 is that the entity
    side stays in Spark. See ``test_entity_side_never_returns_to_driver``.
    """
    return {tuple(r) for r in df.collect()}


@pytest.fixture
def df_a(spark):
    return spark.createDataFrame(
        [
            (_MONTH, "c1", "p1"), (_MONTH, "c1", "p2"),
            (_MONTH, "c2", "p1"), (_MONTH, "c2", "p3"),
            (_MONTH, "c3", "p1"),
        ],
        ["snap_date", "cust_id", "prod_name"],
    )


@pytest.fixture
def df_b(spark):
    return spark.createDataFrame(
        [
            (_MONTH, "c2", "p1"), (_MONTH, "c2", "p2"),
            (_MONTH, "c3", "p2"), (_MONTH, "c3", "p3"),
            (_MONTH, "c4", "p1"),
        ],
        ["snap_date", "cust_id", "prod_name"],
    )


def test_intersection_query_groups_and_items(df_a, df_b):
    universe = common_universe(df_a, df_b, "snap_date", ["snap_date", "cust_id"], "prod_name")
    # The query-group DataFrame carries one row per query group: the time
    # column (as text) plus one column per schema.entity column — so callers
    # always join on the whole group.
    assert universe.common_query_groups.columns == ["snap_date", "cust_id"]
    assert _group_tuples(universe.common_query_groups) == {
        (_MONTH, "c2"), (_MONTH, "c3")}
    assert universe.common_items == {"p1", "p2", "p3"}
    # Each side's full item set rides along for coverage (ADR-0020 bug 14).
    assert universe.a_items == {"p1", "p2", "p3"}
    assert universe.b_items == {"p1", "p2", "p3"}


def test_intersection_uses_every_entity_column(spark):
    """Two entity columns: an entity is the pair, not its first column.

    ``b1`` appears on both sides and ``c1`` appears on both sides, yet the
    pair ``(b1, c1)`` exists only in A. Intersecting first columns would keep
    it; intersecting entities drops it.
    """
    a = spark.createDataFrame(
        [(_MONTH, "b1", "c1", "p1"), (_MONTH, "b1", "c2", "p1")],
        ["snap_date", "branch_id", "cust_id", "prod_name"],
    )
    b = spark.createDataFrame(
        [(_MONTH, "b1", "c2", "p1"), (_MONTH, "b2", "c1", "p1")],
        ["snap_date", "branch_id", "cust_id", "prod_name"],
    )
    universe = common_universe(
        a, b, "snap_date", ["snap_date", "branch_id", "cust_id"], "prod_name")
    assert universe.common_query_groups.columns == [
        "snap_date", "branch_id", "cust_id"]
    assert _group_tuples(universe.common_query_groups) == {(_MONTH, "b1", "c2")}
    assert universe.common_items == {"p1"}


def test_intersection_keeps_an_entity_only_in_the_months_both_sides_have(spark):
    """#374: several evaluated months. c1 is on both sides, but B scored it in
    January only, so (February, c1) is not common; c2 is common in both."""
    a = spark.createDataFrame(
        [(m, c, "p1") for m in ("2026-01-31", "2026-02-28") for c in ("c1", "c2")],
        ["snap_date", "cust_id", "prod_name"],
    )
    b = spark.createDataFrame(
        [("2026-01-31", "c1", "p1"), ("2026-01-31", "c2", "p1"),
         ("2026-02-28", "c2", "p1")],
        ["snap_date", "cust_id", "prod_name"],
    )
    universe = common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")
    assert _group_tuples(universe.common_query_groups) == {
        ("2026-01-31", "c1"), ("2026-01-31", "c2"), ("2026-02-28", "c2")}


def test_entity_side_never_returns_to_driver(df_a, df_b, monkeypatch):
    """The query-group intersection stays in Spark; only items come back (#275).

    Production entity populations are millions of rows, and a ``.collect()``
    of them lands in the driver's *Python* heap — the one
    ``spark.driver.memory`` does not protect. Items are 22 products
    (ADR-0010), so collecting those is correct and stays.

    Asserting on the intersection's *values* would not catch a regression
    here: the values are the same either way. So spy on every
    ``DataFrame.collect`` and assert which columns each one pulled back.
    """
    collected_columns: list[list[str]] = []
    original_collect = SparkDataFrame.collect

    def spy(self):
        collected_columns.append(list(self.columns))
        return original_collect(self)

    monkeypatch.setattr(SparkDataFrame, "collect", spy)

    universe = common_universe(df_a, df_b, "snap_date", ["snap_date", "cust_id"], "prod_name")

    assert isinstance(universe.common_query_groups, SparkDataFrame)
    assert collected_columns == [["prod_name"], ["prod_name"]]
    assert universe.common_items == {"p1", "p2", "p3"}


def test_empty_check_does_not_count_on_the_happy_path(df_a, df_b, monkeypatch):
    """``count()`` is a full Spark action — it may only run when raising.

    Deciding "is the intersection empty" with ``count()`` would put two extra
    passes over a million-row table on every successful compare.
    """
    counted: list[list[str]] = []
    original_count = SparkDataFrame.count

    def spy(self):
        counted.append(list(self.columns))
        return original_count(self)

    monkeypatch.setattr(SparkDataFrame, "count", spy)

    common_universe(df_a, df_b, "snap_date", ["snap_date", "cust_id"], "prod_name")

    assert counted == []


def _one_month(spark, rows):
    return spark.createDataFrame(
        [(_MONTH, *row) for row in rows],
        "snap_date string, cust_id string, prod_name string",
    )


def test_empty_query_group_intersection_raises(spark):
    a = _one_month(spark, [("c1", "p1")])
    b = _one_month(spark, [("c9", "p1")])
    with pytest.raises(DataConsistencyError, match="common_query_groups"):
        common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")


def test_same_entities_in_different_months_share_no_query_group(spark):
    """Both sides score c1, but A in January and B in February: no query group
    is common, so the gate raises instead of comparing two different months."""
    a = spark.createDataFrame([("2026-01-31", "c1", "p1")],
                              ["snap_date", "cust_id", "prod_name"])
    b = spark.createDataFrame([("2026-02-28", "c1", "p1")],
                              ["snap_date", "cust_id", "prod_name"])
    with pytest.raises(DataConsistencyError, match="common_query_groups"):
        common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")


def test_empty_query_group_message_still_reports_both_side_counts(spark):
    """The empty-universe message keeps its two population numbers (#275 must not drop them)."""
    a = _one_month(spark, [("c1", "p1"), ("c2", "p1")])
    b = _one_month(spark, [("c9", "p1")])
    with pytest.raises(DataConsistencyError) as excinfo:
        common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")
    assert "A has 2 query groups, B has 1 query groups" in str(excinfo.value)


def test_empty_item_intersection_raises(spark):
    a = _one_month(spark, [("c1", "p1")])
    b = _one_month(spark, [("c1", "p9")])
    with pytest.raises(DataConsistencyError, match="common_items"):
        common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")


def test_null_only_intersection_fails_the_gate_instead_of_passing_it(spark):
    """A null-keyed shared entity is not a usable common query group.

    ``intersect`` counts ``NULL == NULL`` as a match, so it would let this
    universe through the empty-universe gate — and the equi-join in ``restrict_to_common``
    would then drop the row, leaving both sides empty with no error raised
    anywhere. The gate uses the same join semantics as the restriction it
    guards, so the two cannot disagree and this fails loud.
    """
    a = _one_month(spark, [("c1", "p1"), (None, "p1")])
    b = _one_month(spark, [("c9", "p1"), (None, "p1")])
    with pytest.raises(DataConsistencyError, match="common_query_groups"):
        common_universe(a, b, "snap_date", ["snap_date", "cust_id"], "prod_name")


def _params() -> dict:
    """Single-column-entity parameters, nested where ``get_schema`` reads them.

    The column names must sit under ``schema`` → ``columns``. They used to sit
    directly under ``schema``, which ``get_schema`` ignores wholesale — the
    values here happen to equal the built-in defaults, so nothing broke, but
    anything copied from it that actually changed a column silently did not
    take effect. Multi-column-entity tests use the shared
    ``two_column_entity_params`` fixture in ``tests/conftest.py``.
    """
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "score": "score", "rank": "rank", "label": "label",
            },
            "categorical_values": {"prod_name": ["p1", "p2", "p3", "p4"]},
        },
    }


@pytest.fixture
def a_df(spark):
    """A has cust=c1,c2,c3, prod=p1,p2,p3,p4 — and a label column already."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c1", "2026-01-31", "p2", 0.7, 2, 0),
            ("c1", "2026-01-31", "p4", 0.5, 3, 0),  # p4 not in B
            ("c2", "2026-01-31", "p1", 0.8, 1, 0),
            ("c2", "2026-01-31", "p3", 0.6, 2, 1),
            ("c3", "2026-01-31", "p1", 0.7, 1, 0),  # c3 not in B
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )


@pytest.fixture
def b_df(spark):
    """B has cust=c1,c2, prod=p1,p2,p3 — no label column."""
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.6),
            ("c1", "2026-01-31", "p2", 0.8),
            ("c1", "2026-01-31", "p3", 0.5),
            ("c2", "2026-01-31", "p1", 0.9),
            ("c2", "2026-01-31", "p3", 0.7),
        ],
        ["cust_id", "snap_date", "prod_name", "score"],
    )


def test_restricts_to_common_entities_and_items(a_df, b_df):
    a_c, b_c, _ = restrict_to_common(a_df, b_df, _params())
    a_rows = sorted((r["cust_id"], r["prod_name"]) for r in a_c.collect())
    b_rows = sorted((r["cust_id"], r["prod_name"]) for r in b_c.collect())
    # common cust = {c1, c2}; common prod = {p1, p2, p3}
    expected = sorted([("c1", "p1"), ("c1", "p2"), ("c1", "p3"),
                       ("c2", "p1"), ("c2", "p3")])
    # A had no (c1, p3) — so A_common has it missing too; check A's reduced set
    a_expected = sorted([("c1", "p1"), ("c1", "p2"), ("c2", "p1"), ("c2", "p3")])
    assert a_rows == a_expected
    assert b_rows == expected


def _two_month_sides(spark, b_time_type="string"):
    """A: c1 and c2 in January and February. B: c2 in both months, c1 in
    January only. ``b_time_type`` is the type of B's time column."""
    months = ("2026-01-31", "2026-02-28")
    a = spark.createDataFrame(
        [(m, c, p, score, label)
         for m in months for c in ("c1", "c2")
         for p, score, label in (("p1", 0.9, 1), ("p2", 0.2, 0))],
        "snap_date string, cust_id string, prod_name string, score double, "
        "label int",
    )
    b_rows = [(m, c, p, score)
              for m, c in (("2026-01-31", "c1"), ("2026-01-31", "c2"),
                           ("2026-02-28", "c2"))
              for p, score in (("p1", 0.3), ("p2", 0.6))]
    b = spark.createDataFrame(
        b_rows,
        "snap_date string, cust_id string, prod_name string, score double",
    )
    if b_time_type != "string":
        from pyspark.sql import functions as F

        b = b.withColumn("snap_date", F.to_date("snap_date"))
    return a, b


def _keys(df):
    return sorted((str(r["snap_date"]), r["cust_id"], r["prod_name"])
                  for r in df.collect())


_COMMON_TWO_MONTH_KEYS = sorted(
    (m, c, p)
    for m, c in (("2026-01-31", "c1"), ("2026-01-31", "c2"), ("2026-02-28", "c2"))
    for p in ("p1", "p2")
)


def test_a_month_b_did_not_score_for_an_entity_is_dropped_from_a(spark):
    """#374: the common population is query groups, not entities. c1 is on
    both sides, but B scored it in January only; keeping A's February c1 rows
    would compare the two models on different populations."""
    a, b = _two_month_sides(spark)
    a_c, b_c, _ = restrict_to_common(a, b, _params())
    assert _keys(a_c) == _COMMON_TWO_MONTH_KEYS
    assert _keys(b_c) == _COMMON_TWO_MONTH_KEYS


def test_query_groups_match_when_the_time_columns_differ_in_type(spark):
    """An external_hive side can carry its time column as DATE while A's
    partition is STRING. The groups are matched on the time as text, so the
    result is the same as with two STRING columns, and B's labels still
    come from A."""
    a, b = _two_month_sides(spark, b_time_type="date")
    assert dict(b.dtypes)["snap_date"] == "date"
    a_c, b_c, _ = restrict_to_common(a, b, _params())
    assert _keys(a_c) == _COMMON_TWO_MONTH_KEYS
    assert _keys(b_c) == _COMMON_TWO_MONTH_KEYS
    labels = {(str(r["snap_date"]), r["cust_id"], r["prod_name"]): r["label"]
              for r in b_c.collect()}
    assert labels == {key: 1 if key[2] == "p1" else 0
                      for key in _COMMON_TWO_MONTH_KEYS}


def test_rank_recomputed_within_common(a_df, b_df):
    a_c, b_c, _ = restrict_to_common(a_df, b_df, _params())
    # B for c1 in common prods: scores p1=0.6, p2=0.8, p3=0.5 → ranks 2, 1, 3
    b_c1 = {r["prod_name"]: r["rank"] for r in b_c.filter("cust_id='c1'").collect()}
    assert b_c1 == {"p2": 1, "p1": 2, "p3": 3}


def test_b_label_follows_a_not_its_own_copy(a_df, b_df):
    """bug 7 (ADR-0020): B is scored against A's answer, whatever it brings.

    Two of the three model_version sources (``enriched_eval_predictions``, the
    default, and ``training_eval_predictions``) land with a label column,
    frozen at whatever ``label_table`` said when B was persisted. The stale
    fixture marks every B row positive, and A is moved to 1 on (c1, p2): the
    only way B shows exactly the labels below is by copying A. A bare B must
    come out identical.
    """
    from pyspark.sql import functions as F

    stale_b = b_df.withColumn("label", F.lit(1))
    a_moved = a_df.withColumn(
        "label",
        F.when((F.col("cust_id") == "c1") & (F.col("prod_name") == "p2"), 1)
        .otherwise(F.col("label")),
    )

    _, b_from_stale, _ = restrict_to_common(a_moved, stale_b, _params())
    _, b_from_bare, _ = restrict_to_common(a_moved, b_df, _params())

    def _rows(df):
        return sorted(tuple(r[c] for c in sorted(df.columns)) for r in df.collect())

    assert sorted(b_from_stale.columns) == sorted(b_from_bare.columns)
    assert _rows(b_from_stale) == _rows(b_from_bare)
    labels = {
        (r["cust_id"], r["prod_name"]): r["label"] for r in b_from_stale.collect()
    }
    assert labels == {
        ("c1", "p1"): 1, ("c1", "p2"): 1, ("c1", "p3"): 0,
        ("c2", "p1"): 0, ("c2", "p3"): 1,
    }


def test_b_row_a_did_not_score_counts_as_zero(a_df, b_df):
    """(c1, p3) survives restriction — c1 and p3 each appear on both sides —
    but A never scored that pair, so B has no answer to copy there."""
    _, b_c, _ = restrict_to_common(a_df, b_df, _params())
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    assert b_labels[("c1", "p3")] == 0


def test_a_preserves_existing_label(a_df, b_df):
    a_c, _, _ = restrict_to_common(a_df, b_df, _params())
    a_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in a_c.collect()}
    # A's c1,p1 label was 1 in source fixture — preserved (not re-joined)
    assert a_labels[("c1", "p1")] == 1


def _join_lines(df) -> list[str]:
    plan = df._jdf.queryExecution().executedPlan().toString()
    return [ln.strip() for ln in plan.splitlines() if "Join" in ln]


def test_entity_join_is_not_forced_to_broadcast(a_df, b_df, spark):
    """Spark picks the entity join strategy; the item join is still forced (#275).

    ``F.broadcast()`` is not a hint Spark may decline — it overrides
    ``spark.sql.autoBroadcastJoinThreshold`` outright. So switching the
    threshold off (-1) separates the two cases in one plan: a join that still
    comes out ``BroadcastHashJoin`` is being forced, and one that falls back to
    ``SortMergeJoin`` is being chosen.

    The item universe is bounded by config (invariant A3 declares it in
    ``schema.categorical_values``), so forcing its broadcast is correct and
    must stay. The entity universe is discovered from the data and has no
    such bound, so its strategy must be chosen, not forced. See
    ``steps/compare_universe.py`` for the full rule.
    """
    old = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        a_common, _, _ = restrict_to_common(a_df, b_df, _params())
        joins = _join_lines(a_common)
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", old)

    entity_joins = [ln for ln in joins if "LeftSemi" in ln]
    item_joins = [ln for ln in joins if "prod_name" in ln and "Inner" in ln]

    assert entity_joins, f"no entity join in plan: {joins}"
    assert not any("BroadcastHashJoin" in ln for ln in entity_joins), \
        f"entity join is still forced to broadcast: {entity_joins}"

    assert item_joins, f"no item join in plan: {joins}"
    assert all("BroadcastHashJoin" in ln for ln in item_joins), \
        f"item join lost its broadcast hint (bounded universe — keep it): {item_joins}"

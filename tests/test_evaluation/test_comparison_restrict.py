"""Tests for comparison.restrict — restrict_to_common."""

import pytest
from recsys_tfb.evaluation.comparison.restrict import restrict_to_common


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


@pytest.fixture
def label_table(spark):
    return spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 1),
            ("c1", "2026-01-31", "p2", 0),
            ("c1", "2026-01-31", "p3", 0),
            ("c2", "2026-01-31", "p1", 0),
            ("c2", "2026-01-31", "p3", 1),
        ],
        ["cust_id", "snap_date", "prod_name", "label"],
    )


def test_restricts_to_common_entities_and_items(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    a_rows = sorted((r["cust_id"], r["prod_name"]) for r in a_c.collect())
    b_rows = sorted((r["cust_id"], r["prod_name"]) for r in b_c.collect())
    # common cust = {c1, c2}; common prod = {p1, p2, p3}
    expected = sorted([("c1", "p1"), ("c1", "p2"), ("c1", "p3"),
                       ("c2", "p1"), ("c2", "p3")])
    # A had no (c1, p3) — so A_common has it missing too; check A's reduced set
    a_expected = sorted([("c1", "p1"), ("c1", "p2"), ("c2", "p1"), ("c2", "p3")])
    assert a_rows == a_expected
    assert b_rows == expected


def test_rank_recomputed_within_common(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    # B for c1 in common prods: scores p1=0.6, p2=0.8, p3=0.5 → ranks 2, 1, 3
    b_c1 = {r["prod_name"]: r["rank"] for r in b_c.filter("cust_id='c1'").collect()}
    assert b_c1 == {"p2": 1, "p1": 2, "p3": 3}


def test_b_gets_label_via_left_join(a_df, b_df, label_table):
    a_c, b_c = restrict_to_common(a_df, b_df, label_table, _params())
    assert "label" in b_c.columns
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    assert b_labels[("c1", "p1")] == 1
    assert b_labels[("c2", "p3")] == 1
    assert b_labels[("c1", "p2")] == 0


def test_b_missing_label_fillna_zero(a_df, b_df):
    spark = a_df.sparkSession
    sparse_labels = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 1)],
        ["cust_id", "snap_date", "prod_name", "label"],
    )
    _, b_c = restrict_to_common(a_df, b_df, sparse_labels, _params())
    b_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in b_c.collect()}
    # p2/p3 not in sparse_labels — must fill 0
    assert b_labels[("c1", "p2")] == 0
    assert b_labels[("c1", "p3")] == 0


def test_a_preserves_existing_label(a_df, b_df, label_table):
    a_c, _ = restrict_to_common(a_df, b_df, label_table, _params())
    a_labels = {(r["cust_id"], r["prod_name"]): r["label"] for r in a_c.collect()}
    # A's c1,p1 label was 1 in source fixture — preserved (not re-joined)
    assert a_labels[("c1", "p1")] == 1


def _join_lines(df) -> list[str]:
    plan = df._jdf.queryExecution().executedPlan().toString()
    return [ln.strip() for ln in plan.splitlines() if "Join" in ln]


def test_entity_join_is_not_forced_to_broadcast(a_df, b_df, label_table, spark):
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
    ``evaluation/comparison/alignment.py`` for the full rule.
    """
    old = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        a_common, _ = restrict_to_common(a_df, b_df, label_table, _params())
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

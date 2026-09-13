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
    ``evaluation/comparison/alignment.py`` for the full rule.
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

"""End-to-end tests for evaluation pipeline in compare modes."""

import pytest
from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    DataConsistencyError,
    compare_mutual_exclusive_errors,
)
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline


def test_default_pipeline_writes_the_enriched_table_from_prepare_eval_data():
    """ADR-0018 decision 1: the producer writes the table itself; the old
    pass-through node at the end of the DAG is gone."""
    pipeline = create_pipeline(post_training=False)
    writers = [n.name for n in pipeline.nodes
               if "enriched_eval_predictions" in n.outputs]
    assert writers == ["prepare_eval_data"]
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "persist_eval_predictions" not in node_names
    assert "load_compare_predictions" not in node_names


def test_compare_mode_adds_three_extra_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "load_compare_predictions" in node_names
    assert "restrict_to_common" in node_names
    assert "generate_comparison_report" in node_names
    # And the standard chain is still there, table written by its producer.
    assert "prepare_eval_data" in node_names


def test_compare_only_mode_skips_compute_nodes():
    src = {"kind": "model_version", "model_version": "v1", "label": "L"}
    pipeline = create_pipeline(post_training=False, compare_source=src, compare_only=True)
    node_names = [n.func.__name__ for n in pipeline.nodes]
    assert "validate_enriched_eval_predictions_present" in node_names
    assert "generate_comparison_report" in node_names
    # explicitly NOT present:
    assert "compute_metrics" not in node_names
    assert "compute_baseline_metrics" not in node_names
    assert "generate_report" not in node_names
    assert "persist_eval_predictions" not in node_names
    assert "prepare_eval_data" not in node_names


def test_a13_compare_and_compare_only_mutually_exclusive():
    errs = compare_mutual_exclusive_errors("x", "y")
    assert errs and "mutually exclusive" in errs[0].lower()


def _warehouse_table_dir(spark, db: str, table: str):
    """Return the local Path for a managed Spark table, stripping file:// prefix."""
    from pathlib import Path

    raw = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    # Spark reports a file:// URI; strip scheme prefix if present.
    if raw.startswith("file:"):
        raw = raw[len("file:"):]
    # Remove any extra leading slashes that would produce //<path> on macOS.
    return Path(raw) / f"{db}.db" / table


def _base_params_for_validator():
    """Minimal params dict the validator needs."""
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "score": "score", "rank": "rank", "label": "label",
            },
            "categorical_values": {"prod_name": ["p1"]},
        },
        "evaluation": {"snap_date": "2026-01-31"},
        "model_version": "MV_X",
        "hive": {"db": "ml_recsys"},
    }


def test_b4_validator_raises_when_partition_empty(spark):
    """Empty DataFrame in (simulates catalog filter returned nothing).
    Validator must raise DataConsistencyError tagged (B4).
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    empty = spark.createDataFrame(
        [],
        "cust_id STRING, snap_date STRING, prod_name STRING, "
        "score DOUBLE, rank INT, label INT",
    )
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(
            empty, _base_params_for_validator()
        )


def test_b4_validator_raises_when_snap_date_filter_yields_empty(spark):
    """DataFrame has rows but no rows match the configured evaluation.snap_date.
    Validator filters then raises B4.
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    params = _base_params_for_validator()
    params["evaluation"]["snap_date"] = "2099-01-01"  # mismatch
    with pytest.raises(DataConsistencyError, match="B4"):
        validate_enriched_eval_predictions_present(df, params)


def test_b4_validator_passes_when_partition_present(spark):
    """The evaluated month has a row → the gate returns, passing nothing on
    (zero-output since ADR-0018 decision 1; restrict_to_common restricts the
    table itself)."""
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        validate_enriched_eval_predictions_present,
    )

    df = spark.createDataFrame(
        [
            ("c1", "2026-01-31", "p1", 0.9, 1, 1),
            ("c2", "2025-12-31", "p1", 0.5, 1, 0),  # another evaluated month
        ],
        ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
    )
    assert validate_enriched_eval_predictions_present(
        df, _base_params_for_validator()
    ) is None


def test_enriched_table_catalog_roundtrip(spark):
    """End-to-end: HiveTableDataset saves what ``prepare_eval_data`` returns
    to the local warehouse with partition_filter(model_version) +
    partition_cols(snap_date); load reads back and drops model_version.

    Isolation: runs against a DEDICATED test database, never the production
    ``ml_recsys.enriched_eval_predictions``. The shared local warehouse holds
    real eval artifacts (see docs/operations/known-pitfalls.md §14); an earlier
    version of this test DROP-ed the production table, silently destroying them
    and breaking the local re-render workflow. The round-trip behaviour is
    table-name-agnostic, so a test DB exercises the identical HiveTableDataset
    code path without touching real data. A finally block removes the residue.
    """
    import shutil
    from recsys_tfb.io.hive_table_dataset import HiveTableDataset

    test_db = "test_persist_roundtrip"        # isolated: never ml_recsys
    test_table = "enriched_eval_predictions"

    def _clean():
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.{test_table}")
        d = _warehouse_table_dir(spark, test_db, test_table)
        if d.exists():
            shutil.rmtree(d)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    _clean()  # start clean if a previous run left residue
    try:
        # Mimic the catalog entry from conf/base/catalog.yaml, on the test DB.
        ds = HiveTableDataset(
            database=test_db,
            table=test_table,
            columns="auto",
            partition_filter={"model_version": "MV_X"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )

        df_in = spark.createDataFrame(
            [("c1", "2026-01-31", "p1", 0.9, 1, 1)],
            ["cust_id", "snap_date", "prod_name", "score", "rank", "label"],
        )

        # Framework auto-save flow: the node returns the frame, the runner
        # saves it through the catalog entry.
        ds.save(df_in)

        # Framework auto-load flow: catalog filters by partition_filter, drops mv
        out = ds.load()
        cols = set(out.columns)
        assert "model_version" not in cols
        assert {"cust_id", "snap_date", "prod_name", "score", "rank",
                "label"} <= cols

        rows = [(r["cust_id"], r["prod_name"], r["score"])
                for r in out.collect()]
        assert rows == [("c1", "p1", 0.9)]
    finally:
        _clean()


# ---------------------------------------------------------------------------
# restrict_to_common (node shim) with a two-column schema.entity
# ---------------------------------------------------------------------------
# Parameters come from the shared ``two_column_entity_params`` fixture
# (tests/conftest.py) so the two entity columns really reach get_schema; a
# locally-written params dict is the exact trap that makes such a test pass
# against single-column code. Entity is ``[branch_id, cust_id]`` and the data
# deliberately reuses ``cust_id`` across branches and ``branch_id`` across
# customers, so "first column only", "entity tuple" and "query group" are three
# different numbers.
#
# Those numbers need two months on side A, and the node keeps only the
# evaluated month of A (ADR-0018 decision 1). The entity tests therefore turn
# the month restriction off (``month_restriction_off``);
# ``test_side_a_is_restricted_to_the_evaluated_month`` covers it on its own.


@pytest.fixture
def month_restriction_off(monkeypatch):
    from recsys_tfb.pipelines.evaluation import comparison_nodes

    monkeypatch.setattr(comparison_nodes, "restrict_to_eval_snap_date",
                        lambda df, parameters: df)


def test_side_a_is_restricted_to_the_evaluated_month(
    spark, two_column_entity_params
):
    """Side A is the whole table for this model_version, every month it was
    evaluated on; B comes from its own source, already one month. Coverage
    counts A after the restriction: one month × 3 entities, not two months."""
    from recsys_tfb.pipelines.evaluation.comparison_nodes import restrict_to_common

    params = {**two_column_entity_params,
              "evaluation": {"snap_date": "2026-01-31"}}
    a_rows, b_rows = [], []
    for date in ("2026-01-31", "2026-02-28"):
        for branch, cust in (("b1", "c1"), ("b1", "c2"), ("b2", "c1")):
            a_rows.append((date, branch, cust, "p1", 0.9, 1))
            a_rows.append((date, branch, cust, "p2", 0.1, 0))
    for branch, cust in (("b1", "c1"), ("b1", "c2"), ("b2", "c1")):
        for item, score in (("p1", 0.6), ("p2", 0.5)):
            b_rows.append(("2026-01-31", branch, cust, item, score))
    a = spark.createDataFrame(a_rows, _LABELED_PREDS_DDL)
    b = spark.createDataFrame(b_rows, _PREDS_DDL)

    a_common, _b_common, coverage = restrict_to_common(a, b, params)

    assert {r["snap_date"] for r in a_common.collect()} == {"2026-01-31"}
    assert coverage["n_query_group_A_full"] == 3
    assert coverage["n_query_group_common"] == 3


@pytest.fixture
def two_col_a(spark):
    """A: 2 dates × entities {(b1,c1), (b1,c2), (b2,c1)} × items {p1, p2}.

    Scores are chosen so that ranking by ``[snap_date, branch_id]`` alone
    (the first-entity-column bug) interleaves c1's and c2's rows and shifts
    their ranks, while ranking by the full query group leaves each customer
    with ranks 1, 2.
    """
    rows = []
    for date in ("2026-01-31", "2026-02-28"):
        for branch, cust, top, bottom in (
            ("b1", "c1", 0.9, 0.1),
            ("b1", "c2", 0.8, 0.2),
            ("b2", "c1", 0.7, 0.3),
        ):
            rows.append((date, branch, cust, "p1", top, 1, 1))
            rows.append((date, branch, cust, "p2", bottom, 2, 0))
    return spark.createDataFrame(
        rows,
        ["snap_date", "branch_id", "cust_id", "prod_name", "score", "rank", "label"],
    )


@pytest.fixture
def two_col_b(spark):
    """B: 2 dates × entities {(b1,c1), (b1,c2)} × items {p1, p2, p3} — no label.

    (b2, c1) is absent, so the common entity set is 2 of A's 3. p3 is absent
    from A, so it lands in ``dropped_items_B``.
    """
    rows = []
    for date in ("2026-01-31", "2026-02-28"):
        for branch, cust in (("b1", "c1"), ("b1", "c2")):
            for item, score in (("p1", 0.6), ("p2", 0.5), ("p3", 0.4)):
                rows.append((date, branch, cust, item, score))
    return spark.createDataFrame(
        rows, ["snap_date", "branch_id", "cust_id", "prod_name", "score"]
    )


def test_two_column_entity_ranking_and_coverage(
    two_col_a, two_col_b, two_column_entity_params, month_restriction_off
):
    """One call, two behaviours: the re-ranking unit and the coverage unit.

    Both are downstream of the same "which columns make up an entity" decision,
    so splitting them into two calls would only pay Spark twice for one fact.
    """
    from collections import defaultdict

    from recsys_tfb.pipelines.evaluation.comparison_nodes import restrict_to_common

    a_common, _b_common, coverage = restrict_to_common(
        two_col_a, two_col_b, two_column_entity_params
    )

    # --- behaviour 1: re-ranking groups by time × EVERY entity column --------
    ranks = {
        (r["snap_date"], r["branch_id"], r["cust_id"], r["prod_name"]): r["rank"]
        for r in a_common.collect()
    }
    # (b1, c2) scores 0.8/0.2 sit below (b1, c1)'s 0.9 within branch b1. Group
    # by branch alone and this customer's items rank 2 and 3; group by the full
    # entity and they rank 1 and 2.
    assert ranks[("2026-01-31", "b1", "c2", "p1")] == 1
    assert ranks[("2026-01-31", "b1", "c2", "p2")] == 2
    # …and that holds for every group: each is ranked 1..n on its own.
    per_group = defaultdict(list)
    for (date, branch, cust, _item), rank in ranks.items():
        per_group[(date, branch, cust)].append(rank)
    assert per_group, "no rows survived restriction — nothing was asserted"
    for group, group_ranks in per_group.items():
        assert sorted(group_ranks) == list(range(1, len(group_ranks) + 1)), group

    # --- behaviour 2: coverage counts distinct query groups ------------------
    # A holds 2 dates × 3 entities. Counting the first entity column alone
    # gives 2 (branches); counting entity tuples gives 3; counting query groups
    # — the unit mAP divides by — gives 6.
    assert coverage["n_query_group_A_full"] == 6
    assert coverage["n_query_group_B_full"] == 4     # 2 dates × 2 entities
    assert coverage["n_query_group_common"] == 4     # (b2, c1) is A-only

    # item-side keys carry the schema role name too
    assert coverage["n_item_A_full"] == 2
    assert coverage["n_item_B_full"] == 3
    assert coverage["n_item_common"] == 2
    assert coverage["dropped_items_A"] == []
    assert coverage["dropped_items_B"] == ["p3"]


# Spark DDL schemas for the null-key / asymmetric fixtures below. Explicit
# types are needed because a None in an entity column cannot be inferred.
_QUERY_GROUP_DDL = "snap_date string, branch_id string, cust_id string"
_PREDS_DDL = f"{_QUERY_GROUP_DDL}, prod_name string, score double"
_LABELED_PREDS_DDL = f"{_PREDS_DDL}, label int"


def test_coverage_common_counts_what_restriction_kept_not_a_null_matching_intersect(
    spark, two_column_entity_params, month_restriction_off
):
    """bug 14 (ADR-0020): ``n_query_group_common`` is the query groups the
    restricted frames still hold.

    Both sides score entity (b1, NULL). ``intersect`` treats NULL == NULL as a
    match, so the old count took those groups as common; the restriction's
    equi-join drops null keys, so no metric ever saw them. Expected: 2 dates ×
    the 2 non-null shared entities.
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import restrict_to_common

    entities = (("b1", "c1"), ("b1", "c2"), ("b1", None))
    a_rows, b_rows = [], []
    for date in ("2026-01-31", "2026-02-28"):
        for branch, cust in entities:
            for item, score, label in (("p1", 0.9, 1), ("p2", 0.1, 0)):
                a_rows.append((date, branch, cust, item, score, label))
                b_rows.append((date, branch, cust, item, score))
    a = spark.createDataFrame(a_rows, _LABELED_PREDS_DDL)
    b = spark.createDataFrame(b_rows, _PREDS_DDL)

    a_common, b_common, coverage = restrict_to_common(
        a, b, two_column_entity_params
    )

    qg = ["snap_date", "branch_id", "cust_id"]
    assert a_common.select(*qg).distinct().count() == 4
    assert b_common.select(*qg).distinct().count() == 4
    assert coverage["n_query_group_common"] == 4
    # The full counts are each side's own frame, null-keyed entity included.
    assert coverage["n_query_group_A_full"] == 6
    assert coverage["n_query_group_B_full"] == 6


def test_coverage_common_excludes_a_group_only_one_side_kept(
    spark, two_column_entity_params, month_restriction_off
):
    """``n_query_group_common`` means "still there on both sides".

    Entity (b1, c2) is shared, but B scored it only on p3, which A lacks. The
    item restriction empties that group on B while A keeps it on p1 — so it is
    in A's metrics and not in B's, and not common.
    """
    from recsys_tfb.pipelines.evaluation.comparison_nodes import restrict_to_common

    date = "2026-01-31"
    a = spark.createDataFrame(
        [(date, "b1", "c1", "p1", 0.9, 1), (date, "b1", "c1", "p2", 0.1, 0),
         (date, "b1", "c2", "p1", 0.8, 1)],
        _LABELED_PREDS_DDL,
    )
    b = spark.createDataFrame(
        [(date, "b1", "c1", "p1", 0.6), (date, "b1", "c1", "p2", 0.5),
         (date, "b1", "c2", "p3", 0.4)],
        _PREDS_DDL,
    )

    a_common, b_common, coverage = restrict_to_common(
        a, b, two_column_entity_params
    )

    qg = ["snap_date", "branch_id", "cust_id"]
    assert a_common.select(*qg).distinct().count() == 2
    assert b_common.select(*qg).distinct().count() == 1
    assert coverage["n_query_group_common"] == 1


def test_restrict_node_collects_each_sides_items_once(
    two_col_a, two_col_b, two_column_entity_params, monkeypatch,
    month_restriction_off,
):
    """bug 14 (ADR-0020): the item sets reach the driver once per side.

    ``common_universe`` already collects them to build the item universe; the
    node used to collect the same two sets again for coverage. Counting
    ``DataFrame.collect`` calls catches a re-collect hidden in a helper too,
    which a source scan of the node body would miss.
    """
    from pyspark.sql import DataFrame

    from recsys_tfb.pipelines.evaluation.comparison_nodes import restrict_to_common

    collected = []
    real_collect = DataFrame.collect

    def _spy(self):
        collected.append(list(self.columns))
        return real_collect(self)

    monkeypatch.setattr(DataFrame, "collect", _spy)
    _a, _b, coverage = restrict_to_common(
        two_col_a, two_col_b, two_column_entity_params
    )
    monkeypatch.undo()

    assert collected == [["prod_name"], ["prod_name"]]
    assert coverage["dropped_items_B"] == ["p3"]  # item sets still reach coverage

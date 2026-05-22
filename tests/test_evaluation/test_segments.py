"""Tests for evaluation.segments — Hive-table segment-source join."""

import pytest

from recsys_tfb.evaluation.segments import join_segment_sources


def _df(spark):
    """Base frame the segment sources are joined onto."""
    return spark.createDataFrame(
        [("c0", "20240331", 1), ("c1", "20240331", 0), ("c2", "20240331", 1)],
        schema=["cust_id", "snap_date", "label"],
    )


def _view(spark, name, rows, cols):
    """Register rows as a temp view that spark.table() can resolve."""
    spark.createDataFrame(rows, schema=cols).createOrReplaceTempView(name)
    return name


def test_join_single_source(spark):
    df = _df(spark)
    _view(spark, "hc_tbl",
          [("c0", "20240331", "x"), ("c1", "20240331", "y"),
           ("c2", "20240331", "z")],
          ["cust_id", "snap_date", "holding_combo"])
    cfg = {"holding_combo": {"table": "hc_tbl",
                             "key_columns": ["cust_id", "snap_date"],
                             "segment_column": "holding_combo"}}
    out = join_segment_sources(df, cfg)
    assert "holding_combo" in out.columns
    assert out.count() == 3
    assert out.filter("holding_combo IS NULL").count() == 0


def test_dedup_prevents_fanout(spark):
    """A finer-grained source (multiple rows per key) must not fan out the
    input — dropDuplicates(key_columns) collapses it to one row per key."""
    df = _df(spark)
    # sample_pool-like: one row per (cust, snap, product); segment repeats.
    _view(spark, "pool_tbl",
          [("c0", "20240331", "mass", "A"), ("c0", "20240331", "mass", "B"),
           ("c1", "20240331", "rich", "A"), ("c1", "20240331", "rich", "B"),
           ("c2", "20240331", "mass", "A"), ("c2", "20240331", "mass", "B")],
          ["cust_id", "snap_date", "cust_segment_typ", "prod_name"])
    cfg = {"cust_segment_typ": {"table": "pool_tbl",
                                "key_columns": ["cust_id", "snap_date"],
                                "segment_column": "cust_segment_typ"}}
    out = join_segment_sources(df, cfg)
    assert out.count() == 3  # not 6


def test_missing_table_raises(spark):
    df = _df(spark)
    cfg = {"gone": {"table": "no_such_table",
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "seg"}}
    with pytest.raises(ValueError, match="no_such_table"):
        join_segment_sources(df, cfg)


def test_missing_column_raises(spark):
    df = _df(spark)
    _view(spark, "bad_tbl",
          [("c0", "20240331", "x")],
          ["cust_id", "snap_date", "other_col"])
    cfg = {"seg": {"table": "bad_tbl",
                   "key_columns": ["cust_id", "snap_date"],
                   "segment_column": "seg_col"}}
    with pytest.raises(ValueError, match="seg_col"):
        join_segment_sources(df, cfg)


def test_collision_drops_preexisting_column(spark):
    """When df already carries the segment_column, segment_sources is
    authoritative: the pre-existing column is dropped before the join."""
    df = spark.createDataFrame(
        [("c0", "20240331", 1, "STALE"), ("c1", "20240331", 0, "STALE")],
        schema=["cust_id", "snap_date", "label", "cust_segment_typ"],
    )
    _view(spark, "auth_tbl",
          [("c0", "20240331", "mass"), ("c1", "20240331", "rich")],
          ["cust_id", "snap_date", "cust_segment_typ"])
    cfg = {"cust_segment_typ": {"table": "auth_tbl",
                                "key_columns": ["cust_id", "snap_date"],
                                "segment_column": "cust_segment_typ"}}
    out = join_segment_sources(df, cfg)
    assert out.columns.count("cust_segment_typ") == 1
    vals = {r["cust_id"]: r["cust_segment_typ"] for r in out.collect()}
    assert vals == {"c0": "mass", "c1": "rich"}


def test_partial_join_is_left(spark):
    """Customers absent from the source get NULL (left join)."""
    df = _df(spark)
    _view(spark, "sparse_tbl",
          [("c0", "20240331", "high")],
          ["cust_id", "snap_date", "risk_level"])
    cfg = {"risk": {"table": "sparse_tbl",
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "risk_level"}}
    out = join_segment_sources(df, cfg)
    assert out.count() == 3
    assert out.filter("risk_level IS NOT NULL").count() == 1
    assert out.filter("risk_level IS NULL").count() == 2


def test_multiple_sources(spark):
    df = _df(spark)
    _view(spark, "m_a",
          [("c0", "20240331", "A"), ("c1", "20240331", "A"),
           ("c2", "20240331", "A")],
          ["cust_id", "snap_date", "holding_combo"])
    _view(spark, "m_b",
          [("c0", "20240331", "M"), ("c1", "20240331", "M"),
           ("c2", "20240331", "M")],
          ["cust_id", "snap_date", "risk_level"])
    cfg = {
        "holding_combo": {"table": "m_a",
                          "key_columns": ["cust_id", "snap_date"],
                          "segment_column": "holding_combo"},
        "risk_level": {"table": "m_b",
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "risk_level"},
    }
    out = join_segment_sources(df, cfg)
    assert "holding_combo" in out.columns
    assert "risk_level" in out.columns
    assert out.count() == 3

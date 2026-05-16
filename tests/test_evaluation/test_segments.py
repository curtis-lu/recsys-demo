"""Tests for evaluation.segments — single Spark segment-source join."""

from recsys_tfb.evaluation.segments import join_segment_sources


def _labels(spark):
    return spark.createDataFrame(
        [("c0", "20240331", 1), ("c1", "20240331", 0),
         ("c2", "20240331", 1)],
        schema=["cust_id", "snap_date", "label"],
    )


def _write(spark, tmp_path, name, rows, cols):
    p = str(tmp_path / f"{name}.parquet")
    spark.createDataFrame(rows, schema=cols).write.parquet(p)
    return p


def test_join_single_source(spark, tmp_path):
    labels = _labels(spark)
    path = _write(spark, tmp_path, "hc",
                  [("c0", "20240331", "x"), ("c1", "20240331", "y"),
                   ("c2", "20240331", "z")],
                  ["cust_id", "snap_date", "holding_combo"])
    cfg = {"holding_combo": {"filepath": path,
                             "key_columns": ["cust_id", "snap_date"],
                             "segment_column": "holding_combo"}}
    out = join_segment_sources(labels, cfg)
    assert "holding_combo" in out.columns
    assert out.filter("holding_combo IS NULL").count() == 0


def test_missing_file_skipped(spark, tmp_path):
    labels = _labels(spark)
    cfg = {"missing": {"filepath": str(tmp_path / "none.parquet"),
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "missing_col"}}
    out = join_segment_sources(labels, cfg)
    assert "missing_col" not in out.columns
    assert out.count() == 3


def test_partial_join_coverage(spark, tmp_path):
    labels = _labels(spark)
    path = _write(spark, tmp_path, "risk",
                  [("c0", "20240331", "high")],
                  ["cust_id", "snap_date", "risk_level"])
    cfg = {"risk": {"filepath": path,
                    "key_columns": ["cust_id", "snap_date"],
                    "segment_column": "risk_level"}}
    out = join_segment_sources(labels, cfg)
    assert out.filter("risk_level IS NOT NULL").count() == 1
    assert out.filter("risk_level IS NULL").count() == 2


def test_multiple_sources(spark, tmp_path):
    labels = _labels(spark)
    p1 = _write(spark, tmp_path, "a",
                [("c0", "20240331", "A"), ("c1", "20240331", "A"),
                 ("c2", "20240331", "A")],
                ["cust_id", "snap_date", "holding_combo"])
    p2 = _write(spark, tmp_path, "b",
                [("c0", "20240331", "M"), ("c1", "20240331", "M"),
                 ("c2", "20240331", "M")],
                ["cust_id", "snap_date", "risk_level"])
    cfg = {
        "holding_combo": {"filepath": p1,
                          "key_columns": ["cust_id", "snap_date"],
                          "segment_column": "holding_combo"},
        "risk_level": {"filepath": p2,
                       "key_columns": ["cust_id", "snap_date"],
                       "segment_column": "risk_level"},
    }
    out = join_segment_sources(labels, cfg)
    assert "holding_combo" in out.columns
    assert "risk_level" in out.columns

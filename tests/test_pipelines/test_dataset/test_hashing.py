"""Tests for the deterministic CRC32 hashing utilities used by sampling."""

import pandas as pd

from recsys_tfb.pipelines.dataset._hashing import (
    HASH_BUCKETS,
    pandas_bucket,
    ratio_to_threshold,
    spark_bucket,
)


def test_ratio_to_threshold_round_trip():
    assert ratio_to_threshold(0.0) == 0
    assert ratio_to_threshold(1.0) == HASH_BUCKETS
    assert ratio_to_threshold(0.5) == HASH_BUCKETS // 2
    assert ratio_to_threshold(0.123) == round(0.123 * HASH_BUCKETS)


def test_pandas_bucket_is_deterministic():
    df = pd.DataFrame({"cust_id": [f"C{i:04d}" for i in range(20)]})
    a = pandas_bucket(df, ["cust_id"], seed=42, site="x")
    b = pandas_bucket(df, ["cust_id"], seed=42, site="x")
    assert (a == b).all()
    assert a.min() >= 0 and a.max() < HASH_BUCKETS


def test_pandas_bucket_seed_changes_output():
    df = pd.DataFrame({"cust_id": [f"C{i:04d}" for i in range(50)]})
    a = pandas_bucket(df, ["cust_id"], seed=42, site="x")
    b = pandas_bucket(df, ["cust_id"], seed=43, site="x")
    assert not (a == b).all()


def test_pandas_bucket_site_isolates_sampling():
    df = pd.DataFrame({"cust_id": [f"C{i:04d}" for i in range(50)]})
    a = pandas_bucket(df, ["cust_id"], seed=42, site="sample_keys")
    b = pandas_bucket(df, ["cust_id"], seed=42, site="split_train_dev")
    assert not (a == b).all()


def test_pandas_and_spark_bucket_match(spark):
    df = pd.DataFrame(
        {
            "cust_id": [f"C{i:04d}" for i in range(50)],
            "snap_date": pd.to_datetime(["2024-01-31"] * 50),
            "prod_name": (["exchange_fx", "fund_stock"] * 25),
        }
    )
    cols = ["snap_date", "cust_id", "prod_name"]
    py = pandas_bucket(df, cols, seed=42, site="sample_keys")

    sdf = spark.createDataFrame(df)
    sdf = sdf.withColumn(
        "_bucket", spark_bucket(sdf, cols, seed=42, site="sample_keys"),
    )
    sp = (
        sdf.toPandas()
        .sort_values(cols)
        .reset_index(drop=True)["_bucket"]
        .to_numpy()
    )

    expected = (
        df.assign(_bucket=py)
        .sort_values(cols)
        .reset_index(drop=True)["_bucket"]
        .to_numpy()
    )
    assert (sp == expected).all()

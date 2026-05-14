"""Tests for predict_and_write_test_predictions — batched per-partition predict+write."""

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest


def _make_test_parquet(tmp_path: Path) -> Path:
    """Build a small partitioned parquet at ``tmp_path/test.parquet``.

    Layout: snap_date=*/prod_name=*/*.parquet (Hive-style, matches what the
    dataset pipeline produces after this PR's catalog change).

    Customers:
      c1 has label=1 only on prod_A (snap=2025-01) -> stays in positive set
      c2 has label=1 only on prod_B (snap=2025-01) -> stays in positive set
      c3 has no positives -> filtered out by Pass 0
      c4 has label=1 on prod_A in 2025-02 only       -> separate snap positive
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        # snap=2025-01-31
        ("c1", "2025-01-31", "prod_A", 1.0, 1),
        ("c1", "2025-01-31", "prod_B", 1.1, 0),
        ("c2", "2025-01-31", "prod_A", 2.0, 0),
        ("c2", "2025-01-31", "prod_B", 2.1, 1),
        ("c3", "2025-01-31", "prod_A", 3.0, 0),
        ("c3", "2025-01-31", "prod_B", 3.1, 0),
        # snap=2025-02-28
        ("c4", "2025-02-28", "prod_A", 4.0, 1),
        ("c4", "2025-02-28", "prod_B", 4.1, 0),
    ]
    df = pd.DataFrame(rows, columns=["cust_id", "snap_date", "prod_name", "feat_a", "label"])
    table = pa.Table.from_pandas(df, preserve_index=False)
    root = tmp_path / "test.parquet"
    pq.write_to_dataset(
        table, root_path=str(root), partition_cols=["snap_date", "prod_name"]
    )
    return root


def _make_prep_meta() -> dict:
    return {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }


def _make_parameters() -> dict:
    return {
        "model_version": "v_test_001",
        "hive": {"db": "ml_recsys"},
        "schema": {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
    }


def test_predict_and_write_passes_filters_no_positive_customers(tmp_path):
    """Pass 0 builds positive-customer set per snap_date; Pass 1 filters
    rows whose cust_id is not in the set. c3 has no positives and must
    not appear in any per-partition save call.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # Mock model: predict returns increasing scores; not calibrated
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    # Not a CalibratedModelAdapter (isinstance check fails -> raw == score)
    model.__class__.__name__ = "LightGBMAdapter"

    # Mock HiveTableDataset handle — capture every save() call
    saves: list[pd.DataFrame] = []

    def capture_save(spark_df):
        # Convert Spark DF to pandas for inspection
        saves.append(spark_df.toPandas() if hasattr(spark_df, "toPandas") else spark_df)

    write_ds = MagicMock()
    write_ds.save.side_effect = capture_save

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    # Expect 4 partitions: (2025-01-31, prod_A), (2025-01-31, prod_B),
    #                     (2025-02-28, prod_A), (2025-02-28, prod_B)
    assert write_ds.save.call_count == 4

    # c3 must never appear in any save
    all_written = pd.concat(saves, ignore_index=True)
    assert "c3" not in set(all_written["cust_id"])

    # c1 and c2 are written for 2025-01-31 partitions (both prods)
    snap_jan = all_written[all_written["snap_date"] == "2025-01-31"]
    assert set(snap_jan["cust_id"]) == {"c1", "c2"}

    # c4 is the only customer in 2025-02-28
    snap_feb = all_written[all_written["snap_date"] == "2025-02-28"]
    assert set(snap_feb["cust_id"]) == {"c4"}

    # Manifest reports the right shape
    assert set(manifest["snap_dates"]) == {"2025-01-31", "2025-02-28"}
    assert set(manifest["prods"]) == {"prod_A", "prod_B"}
    assert manifest["model_version"] == "v_test_001"
    assert manifest["n_rows_written"] == len(all_written)


def test_predict_and_write_score_uncalibrated_equals_score_when_not_calibrated(tmp_path):
    """When the model is not a CalibratedModelAdapter, score_uncalibrated
    must equal score row-for-row in every written partition.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    model = MagicMock()
    model.predict.side_effect = lambda X: np.array([0.42] * len(X))
    model.__class__.__name__ = "LightGBMAdapter"

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(
        df.toPandas() if hasattr(df, "toPandas") else df
    )

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    for df in saves:
        assert (df["score"] == df["score_uncalibrated"]).all()


def test_predict_and_write_calibrated_branch_calls_predict_uncalibrated(tmp_path):
    """When the model IS a CalibratedModelAdapter, predict_uncalibrated
    is called to populate score_uncalibrated separately from score.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # spec=CalibratedModelAdapter makes isinstance check pass
    model = MagicMock(spec=CalibratedModelAdapter)
    model.predict.side_effect = lambda X: np.array([0.9] * len(X))
    model.predict_uncalibrated.side_effect = lambda X: np.array([0.1] * len(X))

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(
        df.toPandas() if hasattr(df, "toPandas") else df
    )

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    # predict_uncalibrated must have been called once per partition
    assert model.predict_uncalibrated.call_count == 4

    for df in saves:
        assert (df["score"] == 0.9).all()
        assert (df["score_uncalibrated"] == 0.1).all()

"""Source-table fixtures shared by the dataset-pipeline test modules.

``test_nodes_spark.py`` and ``test_data_gate.py`` exercise different code but
need the same three source tables and the same ``parameters``, so the frames
live here rather than being duplicated — a second hand-built copy would drift
and the drift would show up as a test that stopped asking its question.

The lists and row rules the frames are generated from are in
``fixture_shape.py``; a test module imports those directly to derive its count
assertions.
"""

import pandas as pd
import pytest

from tests.test_pipelines.test_dataset.fixture_shape import (
    _ENTITIES,
    _PRODUCTS,
    _SNAP_DATES,
    _channel,
    _entity_index,
    _is_positive,
    _segment,
)


@pytest.fixture
def feature_table(spark):
    rows = []
    for snap in _SNAP_DATES:
        for cid in _ENTITIES:
            aum = 100.0 * (_entity_index(cid) + 1)
            rows.append({
                "snap_date": pd.Timestamp(snap),
                "cust_id": cid,
                "total_aum": aum,
                "fund_aum": aum / 10.0,
                "in_amt_sum_l1m": 5.0,
                "out_amt_sum_l1m": 3.0,
                "in_amt_ratio_l1m": 0.05,
                "out_amt_ratio_l1m": 0.03,
            })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def label_table(spark):
    rows = []
    for snap in _SNAP_DATES:
        snap_dt = pd.Timestamp(snap)
        for cid in _ENTITIES:
            for prod in _PRODUCTS:
                rows.append(
                    {
                        "snap_date": snap_dt,
                        "cust_id": cid,
                        "cust_segment_typ": _segment(cid),
                        "apply_start_date": snap_dt + pd.Timedelta(days=1),
                        "apply_end_date": snap_dt + pd.Timedelta(days=30),
                        "label": 1 if _is_positive(cid, prod) else 0,
                        "prod_name": prod,
                    }
                )
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def sample_pool(spark):
    rows = []
    for snap in _SNAP_DATES:
        snap_dt = pd.Timestamp(snap)
        for cid in _ENTITIES:
            for prod in _PRODUCTS:
                rows.append({
                    "snap_date": snap_dt,
                    "cust_id": cid,
                    "cust_segment_typ": _segment(cid),
                    "prod_name": prod,
                    "label": 1 if _is_positive(cid, prod) else 0,
                    "tenure_months": 12 * (_entity_index(cid) % 5 + 1),
                    "channel_preference": _channel(cid),
                })
    return spark.createDataFrame(pd.DataFrame(rows))


@pytest.fixture
def parameters():
    return {
        "random_seed": 42,
        "schema": {
            "categorical_values": {
                "prod_name": list(_PRODUCTS),
            },
        },
        "dataset": {
            "train_snap_dates": _SNAP_DATES[:3],
            "sample_ratio": 0.5,
            "sample_group_keys": ["cust_segment_typ", "prod_name"],
            "sample_ratio_overrides": {},
            "train_dev_ratio": 0.2,
            "enable_calibration": False,
            "calibration_snap_dates": [],
            "calibration_sample_ratio": 1.0,
            "val_snap_dates": _SNAP_DATES[3:4],
            "val_sample_ratio": 1.0,
            "test_snap_dates": _SNAP_DATES[4:5],
        },
    }

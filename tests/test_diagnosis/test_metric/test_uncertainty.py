"""bootstrap_per_item_ci：cluster bootstrap 的決定性、退化案例、覆蓋性質。"""

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci


def _params(n_boot=50, k=None, metric_extra=None, seed=42):
    metric = {"weight_alpha": 0.0, "k": k, "min_positives": 0, "shrinkage_k": 0}
    if metric_extra:
        metric.update(metric_extra)
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {
            "metric": metric,
            "diagnosis": {
                "sample": {"max_queries": 1000,
                           "min_pos_queries_per_item": 1, "seed": seed},
                "ci": {"enabled": True, "n_boot": n_boot},
            },
        },
    }


def _pdf(rows):
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


THREE_CUST = [
    # A：C0 rank1（contrib 1.0）、C1 rank2（0.5）→ AP 0.75, n_pos=2
    # B：C2 rank1（1.0）→ AP 1.0, n_pos=1；等權 macro = 0.875
    ("20240331", "C0", "A", 0.9, 1),
    ("20240331", "C0", "B", 0.1, 0),
    ("20240331", "C1", "A", 0.1, 1),
    ("20240331", "C1", "B", 0.9, 0),
    ("20240331", "C2", "A", 0.1, 0),
    ("20240331", "C2", "B", 0.9, 1),
]


def test_point_estimates_match_metric_family():
    out = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out["per_item"]["A"]["ap"] == pytest.approx(0.75)
    assert out["per_item"]["A"]["n_pos"] == 2
    assert out["per_item"]["B"]["ap"] == pytest.approx(1.0)
    assert out["macro"]["ap"] == pytest.approx(0.875)
    assert out["n_boot"] == 50


def test_ci_brackets_point_and_is_deterministic():
    out1 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    out2 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out1 == out2
    m = out1["macro"]
    assert m["ci_low"] <= m["ap"] <= m["ci_high"]
    a = out1["per_item"]["A"]
    assert a["ci_low"] <= a["ap"] <= a["ci_high"]


def test_single_cluster_degenerates_to_zero_width():
    rows = [
        ("20240331", "C0", "A", 0.9, 1),
        ("20240331", "C0", "B", 0.1, 0),
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params())
    a = out["per_item"]["A"]
    assert a["ap"] == a["ci_low"] == a["ci_high"] == pytest.approx(1.0)


def test_k_truncation_zeroes_deep_positive():
    rows = [
        ("20240331", "C0", "A", 0.9, 0),
        ("20240331", "C0", "B", 0.1, 1),   # rank 2；k=1 → contrib 0
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params(k=1))
    assert out["per_item"]["B"]["ap"] == pytest.approx(0.0)
    assert out["k"] == 1


def test_metric_params_flow_into_macro():
    out = bootstrap_per_item_ci(
        _pdf(THREE_CUST), _params(metric_extra={"weight_alpha": 1.0})
    )
    assert out["macro"]["ap"] == pytest.approx(5 / 6)

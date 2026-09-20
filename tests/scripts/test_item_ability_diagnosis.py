import argparse

import numpy as np
import pandas as pd
import pytest

from scripts.item_ability_diagnosis import (
    analyze_items,
    query_center_scores,
    resolve_snap_date,
    weighted_auc,
)


def test_weighted_auc_handles_ties_with_half_credit():
    scores = np.array([1.0, 1.0, 2.0, 2.0])
    y = np.array([0, 1, 0, 1])

    assert weighted_auc(scores, y) == 0.5


def test_query_centered_auc_removes_query_level_false_signal():
    pdf = pd.DataFrame([
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "A", "label": 0, "score": 8.0},
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "B", "label": 0, "score": 7.0},
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "J", "label": 1, "score": 6.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "A", "label": 0, "score": 9.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "B", "label": 0, "score": 8.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "J", "label": 1, "score": 7.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "A", "label": 0, "score": 3.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "B", "label": 0, "score": 2.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "J", "label": 0, "score": 1.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "A", "label": 0, "score": 4.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "B", "label": 0, "score": 3.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "J", "label": 0, "score": 2.0},
    ])
    schema = {
        "time": "snap_date",
        "entity": ["cust_id"],
        "item": "prod_name",
        "label": "label",
        "score": "score",
    }
    params = {
        "evaluation": {
            "metric": {
                "k": None,
                "weight_alpha": 0.0,
                "min_positives": 0,
                "shrinkage_k": 0.0,
            }
        }
    }

    result = analyze_items(
        pdf,
        params,
        schema,
        n_boot=0,
        seed=42,
        top_n=10,
    )
    by_item = {r["item"]: r for r in result["per_item"]}

    assert by_item["J"]["raw_within_item_auc"] == 1.0
    assert by_item["J"]["query_centered_auc"] == 0.5
    assert by_item["J"]["ap"] == 1.0 / 3.0
    assert by_item["J"]["median_positive_rank_percentile"] == 1.0


def test_query_center_scores_subtracts_group_mean():
    groups = np.array([0, 0, 0, 1, 1])
    z = np.array([8.0, 7.0, 6.0, 2.0, 4.0])

    centered = query_center_scores(groups, z)

    np.testing.assert_allclose(centered, np.array([1.0, 0.0, -1.0, -1.0, 1.0]))


def _args(snap_date=None):
    return argparse.Namespace(snap_date=snap_date)


def test_resolve_snap_date_single_string_is_used_as_is():
    parameters = {"evaluation": {"snap_date": "2026-01-31"}}

    assert resolve_snap_date(_args(), parameters) == "2026-01-31"


def test_resolve_snap_date_range_with_same_start_and_end_resolves_one_date():
    parameters = {
        "evaluation": {
            "snap_date": {
                "start": "2026-01-31",
                "end": "2026-01-31",
                "step": "month_end",
            }
        }
    }

    assert resolve_snap_date(_args(), parameters) == "2026-01-31"


def test_resolve_snap_date_multiple_dates_without_cli_override_fails_loud():
    parameters = {"evaluation": {"snap_date": ["2026-01-31", "2026-02-28"]}}

    with pytest.raises(ValueError) as exc_info:
        resolve_snap_date(_args(), parameters)

    message = str(exc_info.value)
    assert "一次只看一個日期" in message
    assert "2026-01-31" in message
    assert "2026-02-28" in message


def test_resolve_snap_date_cli_override_wins_over_multiple_configured_dates():
    parameters = {"evaluation": {"snap_date": ["2026-01-31", "2026-02-28"]}}

    assert resolve_snap_date(_args("2026-03-31"), parameters) == "2026-03-31"


def test_resolve_snap_date_missing_raises():
    parameters = {"evaluation": {}}

    with pytest.raises(ValueError, match="evaluation.snap_date is missing"):
        resolve_snap_date(_args(), parameters)

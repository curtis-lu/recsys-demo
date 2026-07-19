import numpy as np
import pandas as pd

from scripts.item_ability_diagnosis import (
    analyze_items,
    query_center_scores,
    weighted_auc,
)


def test_weighted_auc_handles_ties_with_half_credit():
    scores = np.array([1.0, 1.0, 2.0, 2.0])
    y = np.array([0, 1, 0, 1])

    assert weighted_auc(scores, y) == 0.5


def test_query_centered_auc_removes_query_level_false_signal():
    pdf = pd.DataFrame([
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "A", "label": 0, "score_uncalibrated": 8.0},
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "B", "label": 0, "score_uncalibrated": 7.0},
        {"snap_date": "2026-07-17", "cust_id": "Alice", "prod_name": "J", "label": 1, "score_uncalibrated": 6.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "A", "label": 0, "score_uncalibrated": 9.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "B", "label": 0, "score_uncalibrated": 8.0},
        {"snap_date": "2026-07-17", "cust_id": "Amy", "prod_name": "J", "label": 1, "score_uncalibrated": 7.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "A", "label": 0, "score_uncalibrated": 3.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "B", "label": 0, "score_uncalibrated": 2.0},
        {"snap_date": "2026-07-17", "cust_id": "Bob", "prod_name": "J", "label": 0, "score_uncalibrated": 1.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "A", "label": 0, "score_uncalibrated": 4.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "B", "label": 0, "score_uncalibrated": 3.0},
        {"snap_date": "2026-07-17", "cust_id": "Ben", "prod_name": "J", "label": 0, "score_uncalibrated": 2.0},
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

import numpy as np
import pandas as pd
import pytest

from scripts.suppression_ledger_diagnosis import analyze_suppression, validate_and_prepare


def _params(k=None):
    return {
        "evaluation": {
            "metric": {
                "k": k,
                "weight_alpha": 0.0,
                "min_positives": 0,
                "shrinkage_k": 0.0,
            },
        }
    }


def _schema():
    return {
        "time": "snap_date",
        "entity": ["cust_id"],
        "item": "prod_name",
        "label": "label",
        "score": "score",
    }


def _ledger_pdf():
    rows = [
        ("2026-01-31", 1, "B", 0, 9.0, "X"),
        ("2026-01-31", 1, "A", 1, 8.0, "X"),
        ("2026-01-31", 1, "C", 1, 7.0, "X"),
        ("2026-01-31", 2, "B", 0, 9.0, "Y"),
        ("2026-01-31", 2, "A", 1, 8.0, "Y"),
        ("2026-01-31", 2, "C", 0, 7.0, "Y"),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "snap_date",
            "cust_id",
            "prod_name",
            "label",
            "score_uncalibrated",
            "seg",
        ],
    )


def test_pair_ledger_matches_hand_computation():
    out = analyze_suppression(
        _ledger_pdf(),
        _params(),
        _schema(),
        top_examples=10,
    )
    rows = {
        (r["positive_item"], r["suppressor_item"]): r
        for r in out["pair_ledger"]
    }

    assert out["n_queries"] == 2
    assert out["n_positive_rows"] == 3
    assert out["n_misordered_pairs"] == 3
    assert rows[("A", "B")]["affected_positive_rows"] == 2
    np.testing.assert_allclose(rows[("A", "B")]["allocated_ap_gap"], 1.0)
    assert rows[("C", "B")]["affected_positive_rows"] == 1
    np.testing.assert_allclose(rows[("C", "B")]["allocated_ap_gap"], 1.0 / 3.0)
    np.testing.assert_allclose(out["macro_per_item_map"], 7.0 / 12.0)
    np.testing.assert_allclose(out["total_ap_gap_allocated_to_suppressors"], 4.0 / 3.0)


def test_target_summary_and_suppressor_summary():
    out = analyze_suppression(
        _ledger_pdf(),
        _params(),
        _schema(),
        top_examples=10,
    )
    targets = {r["positive_item"]: r for r in out["target_summary"]}
    suppressors = {r["suppressor_item"]: r for r in out["by_suppressor"]}

    assert targets["A"]["n_pos"] == 2
    assert targets["A"]["suppressed_positive_rate"] == 1.0
    assert targets["A"]["mean_negatives_above_positive"] == 1.0
    assert targets["A"]["top_suppressor"] == "B"
    assert targets["A"]["median_positive_rank_display"] == "2 of 3"
    np.testing.assert_allclose(targets["A"]["ap_gap"], 0.5)
    np.testing.assert_allclose(targets["A"]["ap_gap_from_suppressors"], 0.5)
    np.testing.assert_allclose(targets["A"]["overall_ap_gap_share"], 0.75)
    np.testing.assert_allclose(targets["C"]["ap_gap"], 1.0 / 3.0)
    np.testing.assert_allclose(targets["C"]["ap_gap_from_suppressors"], 1.0 / 3.0)

    top = {
        (r["positive_item"], r["suppressor_rank"]): r
        for r in out["top_suppressors_by_target"]
    }
    assert top[("A", 1)]["suppressor_item"] == "B"
    np.testing.assert_allclose(
        top[("A", 1)]["target_ap_gap_share"],
        1.0,
    )

    assert list(suppressors) == ["B"]
    assert suppressors["B"]["affected_positive_rows"] == 3
    assert suppressors["B"]["affected_positive_items"] == 2
    np.testing.assert_allclose(
        suppressors["B"]["overall_ap_gap_share"],
        1.0,
    )
    np.testing.assert_allclose(
        out["matrices"]["target_gap_share"]["A"]["B"],
        1.0,
    )
    np.testing.assert_allclose(
        out["matrices"]["affected_positive_rate"]["A"]["B"],
        1.0,
    )
    np.testing.assert_allclose(
        out["matrices"]["mean_logit_margin"]["A"]["B"],
        1.0,
    )
    np.testing.assert_allclose(
        out["matrices"]["suppressor_target_gap_share"]["B"]["A"],
        0.75,
    )
    np.testing.assert_allclose(
        out["matrices"]["suppressor_target_gap_share"]["B"]["C"],
        0.25,
    )


def test_k_truncation_changes_ap_gap_attribution():
    out = analyze_suppression(
        _ledger_pdf(),
        _params(k=1),
        _schema(),
        top_examples=10,
    )
    pairs = {
        (r["positive_item"], r["suppressor_item"]): r
        for r in out["pair_ledger"]
    }

    np.testing.assert_allclose(pairs[("A", "B")]["allocated_ap_gap"], 2.0)
    np.testing.assert_allclose(pairs[("C", "B")]["allocated_ap_gap"], 1.0)


def test_duplicate_query_item_fails_loud():
    pdf = pd.concat([_ledger_pdf(), _ledger_pdf().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="one row per query/item"):
        validate_and_prepare(pdf, _params(), _schema())

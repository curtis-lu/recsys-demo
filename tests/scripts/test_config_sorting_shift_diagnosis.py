import argparse

import pytest

from scripts.config_sorting_shift_diagnosis import resolve_snap_date


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


def test_the_offset_universe_of_a_counted_item_list_is_what_was_observed():
    """#379: the cell holds a string, not a list; the observed items are the
    universe, as when no list is declared."""
    import pandas as pd

    from recsys_tfb.core.schema import get_schema
    from scripts.config_sorting_shift_diagnosis import build_offset_frame

    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name"},
                   "categorical_values": {"prod_name": "from_train_data"}},
        "dataset": {"sample_group_keys": [], "sample_ratio": 1.0},
        "training": {"sample_weight_keys": []},
    }
    pdf = pd.DataFrame({"snap_date": ["2025-01-31"] * 2, "cust_id": ["c1", "c1"],
                        "prod_name": ["a", "b"], "label": [1, 0],
                        "score": [0.9, 0.1]})
    _frame, meta = build_offset_frame(pdf, params, get_schema(params))
    assert meta["items"] == ["a", "b"]

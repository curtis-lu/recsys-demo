import argparse

import pytest

from scripts.per_item_score_shift_diagnosis import resolve_snap_date


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

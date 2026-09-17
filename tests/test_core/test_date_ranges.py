"""Date settings written as a range (``{start, end, step}``) instead of a list.

The range is expanded into the list it stands for as the config is loaded, so
every reader — and every version hash — sees exactly what a hand-written list
would give (#374).
"""

import datetime

import pytest
import yaml

from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.date_ranges import as_date_list, dates_label


def _write_yaml(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


class TestLoaderExpandsRanges:
    def test_month_end_range_reads_as_the_list_it_stands_for(self, tmp_path):
        _write_yaml(
            tmp_path / "base" / "parameters_dataset.yaml",
            {"dataset": {"train_snap_dates": {
                "start": "2025-01-31", "end": "2025-04-30", "step": "month_end",
            }}},
        )
        loader = ConfigLoader(str(tmp_path), env="local")

        assert loader.get_parameters()["dataset"]["train_snap_dates"] == [
            "2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30",
        ]


class TestAsDateList:
    """One setting that holds either a single date or several."""

    def test_single_text_date_is_a_one_element_list(self):
        assert as_date_list(" 2026-01-31 ") == ["2026-01-31"]

    def test_list_keeps_its_order_and_strips_each_date(self):
        assert as_date_list(["2026-03-31", " 2026-01-31"]) == [
            "2026-03-31", "2026-01-31",
        ]

    def test_unquoted_yaml_date_reads_as_iso_text(self):
        assert as_date_list(datetime.date(2026, 1, 31)) == ["2026-01-31"]

    @pytest.mark.parametrize("value", [None, "", "  ", []])
    def test_nothing_configured_is_an_empty_list(self, value):
        assert as_date_list(value) == []


class TestDatesLabel:
    """The path segment an evaluation run's outputs are filed under."""

    def test_one_date_keeps_todays_yyyymmdd_segment(self):
        assert dates_label(["2026-01-31"]) == "20260131"

    def test_several_dates_name_the_first_and_last(self):
        assert dates_label(["2026-03-31", "2026-01-31", "2026-02-28"]) == (
            "20260131-20260331"
        )

    def test_no_dates_is_an_error_not_an_empty_segment(self):
        with pytest.raises(ValueError, match="no dates"):
            dates_label([])

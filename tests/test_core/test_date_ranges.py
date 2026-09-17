"""Date settings written as a range (``{start, end, step}``) instead of a list.

The range is expanded into the list it stands for as the config is loaded, so
every reader — and every version hash — sees exactly what a hand-written list
would give (#374).
"""

import datetime

import pytest
import yaml

from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.date_ranges import (
    DateRangeError,
    as_date_list,
    dates_label,
    expand_date_range,
)
from recsys_tfb.core.versioning import (
    compute_base_dataset_version,
    compute_train_variant_id,
)


def _write_yaml(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


def _month_end_range(start, end):
    return {"start": start, "end": end, "step": "month_end"}


class TestLoaderExpandsRanges:
    def test_month_end_range_reads_as_the_list_it_stands_for(self, tmp_path):
        _write_yaml(
            tmp_path / "base" / "parameters_dataset.yaml",
            {"dataset": {"train_snap_dates": _month_end_range("2025-01-31", "2025-04-30")}},
        )
        loader = ConfigLoader(str(tmp_path), env="local")

        assert loader.get_parameters()["dataset"]["train_snap_dates"] == [
            "2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30",
        ]

    def test_a_range_hashes_to_the_version_its_list_had_before_ranges_existed(
        self, tmp_path
    ):
        """The acceptance test of #374, pinned to values from before the change.

        ``tests/test_core/test_versioning.py`` pins ``0675afb8``／``913be727``
        for a literal fixture whose ``train_snap_dates`` lists the twelve month
        ends of 2023 (recorded from ``main`` at be2d95a). The same fixture is
        written here with that list as a range and read the way the dataset
        command reads it (``__main__.py`` hashes
        ``get_parameters_by_name("parameters_dataset")``). Comparing a range
        against a list computed today would pass even if both hashed wrongly;
        only a value carried over from before can testify that nothing moved.
        """
        dataset = {
            "train_snap_dates": _month_end_range("2023-01-31", "2023-12-31"),
            "val_snap_dates": ["2024-01-31"],
            "test_snap_dates": ["2024-02-29"],
            "sample_ratio": 0.1,
            "sample_ratio_overrides": {},
            "sample_group_keys": ["cust_segment_typ"],
            "train_dev_ratio": 0.1,
            "calibration_snap_dates": ["2024-02-29"],
            "calibration_sample_ratio": 1.0,
            "calibration_sample_ratio_overrides": {},
        }
        schema = {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "target",
            "identity_columns": ["snap_date", "cust_id", "prod_name"],
            "categorical_values": {"prod_name": ["a", "b", "c"]},
        }
        _write_yaml(tmp_path / "base" / "parameters_dataset.yaml", {"dataset": dataset})
        params = ConfigLoader(str(tmp_path), env="local").get_parameters_by_name(
            "parameters_dataset"
        )

        assert compute_base_dataset_version(params, schema) == "0675afb8"
        assert compute_train_variant_id(params) == "913be727"

    def test_the_same_days_listed_out_of_order_are_a_different_version(
        self, tmp_path
    ):
        """The documented limit of "same version": a range equals the list it
        stands for, which is ascending quoted text. Lists are hashed as written
        (normalising them would move every existing version ID), so this pins
        that a reordered list is NOT silently merged with the range — the docs
        tell users that rewriting such a list as a range rebuilds once."""
        base = {"val_snap_dates": ["2024-01-31"]}
        _write_yaml(tmp_path / "a" / "base" / "parameters_dataset.yaml", {"dataset": {
            **base, "train_snap_dates": _month_end_range("2023-01-31", "2023-02-28"),
        }})
        _write_yaml(tmp_path / "b" / "base" / "parameters_dataset.yaml", {"dataset": {
            **base, "train_snap_dates": ["2023-02-28", "2023-01-31"],
        }})
        schema = {"time": "t", "entity": ["e"], "item": "i"}

        def version(root):
            params = ConfigLoader(str(root), env="local").get_parameters_by_name(
                "parameters_dataset"
            )
            return compute_base_dataset_version(params, schema)

        assert version(tmp_path / "a") != version(tmp_path / "b")

    def test_every_dataset_split_and_the_evaluation_date_accept_a_range(
        self, tmp_path
    ):
        _write_yaml(
            tmp_path / "base" / "parameters_dataset.yaml",
            {"dataset": {
                "train_snap_dates": _month_end_range("2025-01-31", "2025-02-28"),
                "calibration_snap_dates": _month_end_range("2025-03-31", "2025-03-31"),
                "val_snap_dates": _month_end_range("2025-04-30", "2025-04-30"),
                "test_snap_dates": _month_end_range("2025-05-31", "2025-06-30"),
            }},
        )
        _write_yaml(
            tmp_path / "base" / "parameters_evaluation.yaml",
            {"evaluation": {"snap_date": _month_end_range("2025-05-31", "2025-06-30")}},
        )
        params = ConfigLoader(str(tmp_path), env="local").get_parameters()

        assert params["dataset"] == {
            "train_snap_dates": ["2025-01-31", "2025-02-28"],
            "calibration_snap_dates": ["2025-03-31"],
            "val_snap_dates": ["2025-04-30"],
            "test_snap_dates": ["2025-05-31", "2025-06-30"],
        }
        assert params["evaluation"]["snap_date"] == ["2025-05-31", "2025-06-30"]

    def test_inference_dates_accept_a_range(self, tmp_path):
        """Monitoring evaluation reads what inference scored, so the two are
        naturally written as the same range; unexpanded, inference crashed
        after Spark started (``snap_dates[0]`` on a dict)."""
        _write_yaml(
            tmp_path / "base" / "parameters_inference.yaml",
            {"inference": {"snap_dates": _month_end_range("2026-01-31", "2026-03-31")}},
        )
        params = ConfigLoader(str(tmp_path), env="local").get_parameters()

        assert params["inference"]["snap_dates"] == [
            "2026-01-31", "2026-02-28", "2026-03-31",
        ]

    def test_lists_and_single_dates_are_left_exactly_as_written(self, tmp_path):
        dataset = {
            "train_snap_dates": ["2025-03-31", "2025-01-31"],
            "test_snap_dates": ["2026-01-31"],
        }
        _write_yaml(tmp_path / "base" / "parameters_dataset.yaml", {"dataset": dataset})
        _write_yaml(
            tmp_path / "base" / "parameters_evaluation.yaml",
            {"evaluation": {"snap_date": "2026-01-31"}},
        )
        params = ConfigLoader(str(tmp_path), env="local").get_parameters()

        assert params["dataset"] == dataset
        assert params["evaluation"]["snap_date"] == "2026-01-31"

    def test_only_parameters_files_are_expanded(self, tmp_path):
        """A catalog entry that happens to share the key path is not a setting."""
        entry = {"train_snap_dates": _month_end_range("2025-01-31", "2025-02-28")}
        _write_yaml(tmp_path / "base" / "catalog.yaml", {"dataset": entry})

        loader = ConfigLoader(str(tmp_path), env="local")

        assert loader.get_catalog_config() == {"dataset": entry}

    def test_env_overlay_can_move_just_the_end(self, tmp_path):
        """Expansion runs after the overlay, so the overlay merges into the range."""
        _write_yaml(
            tmp_path / "base" / "parameters_dataset.yaml",
            {"dataset": {"train_snap_dates": _month_end_range("2025-01-31", "2025-02-28")}},
        )
        _write_yaml(
            tmp_path / "prod" / "parameters_dataset.yaml",
            {"dataset": {"train_snap_dates": {"end": "2025-03-31"}}},
        )
        params = ConfigLoader(str(tmp_path), env="prod").get_parameters()

        assert params["dataset"]["train_snap_dates"] == [
            "2025-01-31", "2025-02-28", "2025-03-31",
        ]

    def test_every_bad_range_is_reported_in_one_error(self, tmp_path):
        _write_yaml(
            tmp_path / "base" / "parameters_dataset.yaml",
            {"dataset": {
                "train_snap_dates": _month_end_range("2025-01-30", "2025-02-28"),
                "val_snap_dates": {"start": "2025-03-31", "end": "2025-03-31"},
            }},
        )
        with pytest.raises(DateRangeError) as exc_info:
            ConfigLoader(str(tmp_path), env="local")

        message = str(exc_info.value)
        assert message.startswith("2 個日期區間設定無法展開")
        assert (
            "parameters_dataset.yaml -> dataset.train_snap_dates.start = "
            "2025-01-30 is not a last day of a month"
        ) in message
        assert (
            "parameters_dataset.yaml -> dataset.val_snap_dates: a date range "
            "takes exactly start, end, step (missing ['step'])"
        ) in message

    def test_the_error_is_a_value_error_the_cli_already_reports(self):
        """``__main__._load_config_and_setup`` turns a ``ValueError`` into exit 1."""
        assert issubclass(DateRangeError, ValueError)


class TestBoundaries:
    """Both ends are included, and both must land on the step."""

    def test_day_includes_both_ends(self):
        assert expand_date_range(
            {"start": "2024-02-27", "end": "2024-03-01", "step": "day"}, "x"
        ) == ["2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01"]

    def test_week_counts_seven_days_from_start_and_includes_the_end(self):
        assert expand_date_range(
            {"start": "2025-01-06", "end": "2025-01-20", "step": "week"}, "x"
        ) == ["2025-01-06", "2025-01-13", "2025-01-20"]

    def test_month_start_crosses_a_year(self):
        assert expand_date_range(
            {"start": "2025-11-01", "end": "2026-02-01", "step": "month_start"}, "x"
        ) == ["2025-11-01", "2025-12-01", "2026-01-01", "2026-02-01"]

    def test_month_end_lands_on_a_leap_day(self):
        assert expand_date_range(
            _month_end_range("2024-01-31", "2024-03-31"), "x"
        ) == ["2024-01-31", "2024-02-29", "2024-03-31"]

    def test_start_equal_to_end_is_one_date(self):
        assert expand_date_range(
            _month_end_range("2026-01-31", "2026-01-31"), "x"
        ) == ["2026-01-31"]

    def test_unquoted_yaml_dates_are_accepted(self):
        assert expand_date_range(
            {"start": datetime.date(2025, 1, 31), "end": datetime.date(2025, 2, 28),
             "step": "month_end"},
            "x",
        ) == ["2025-01-31", "2025-02-28"]

    def test_week_end_off_the_step_is_an_error_not_a_shorter_range(self):
        with pytest.raises(DateRangeError, match="not a whole number of weeks"):
            expand_date_range(
                {"start": "2025-01-06", "end": "2025-01-19", "step": "week"}, "x"
            )

    def test_month_end_end_off_the_step_is_an_error(self):
        with pytest.raises(
            DateRangeError, match=r"x\.end = 2025-02-27 is not a last day of a month"
        ):
            expand_date_range(_month_end_range("2025-01-31", "2025-02-27"), "x")

    def test_month_start_start_off_the_step_is_an_error(self):
        with pytest.raises(
            DateRangeError, match=r"x\.start = 2025-01-02 is not a first day of a month"
        ):
            expand_date_range(
                {"start": "2025-01-02", "end": "2025-02-01", "step": "month_start"}, "x"
            )

    def test_end_before_start_is_an_error(self):
        with pytest.raises(DateRangeError, match="end 2025-01-31 is before start 2025-02-28"):
            expand_date_range(_month_end_range("2025-02-28", "2025-01-31"), "x")

    def test_unknown_step_is_an_error(self):
        with pytest.raises(DateRangeError, match=r"x\.step = 'ME'; expected one of"):
            expand_date_range({"start": "2025-01-31", "end": "2025-02-28", "step": "ME"}, "x")

    def test_extra_key_is_an_error(self):
        with pytest.raises(DateRangeError, match=r"unknown \['freq'\]"):
            expand_date_range(
                {"start": "2025-01-31", "end": "2025-02-28", "step": "month_end",
                 "freq": "M"},
                "x",
            )

    def test_text_that_is_not_a_date_is_an_error(self):
        with pytest.raises(DateRangeError, match=r"x\.start = '2025-13-31' is not a YYYY-MM-DD date"):
            expand_date_range(_month_end_range("2025-13-31", "2026-01-31"), "x")


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

    def test_a_date_written_twice_counts_once(self):
        """Readers count the dates (the evaluated-month postcondition) and name
        a path after them; a repeat would fail the first late and make the
        second read ``20260131-20260131``."""
        assert as_date_list(["2026-01-31", "2026-02-28", " 2026-01-31"]) == [
            "2026-01-31", "2026-02-28",
        ]

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

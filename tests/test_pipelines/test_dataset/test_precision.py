"""Tests for the B8 gate's mechanisms between its facts and its rule.

Which files the facts come from is tested with the rest of the footer facts
(``test_footer_facts.py``); the rule with its predicates
(``test_core/test_consistency.py``). What is left here is small but silent when
wrong: a column dropped from ``value_steps`` is a column the gate stops
checking, and nothing says so.
"""

from recsys_tfb.core.consistency import ColumnPrecision
from recsys_tfb.pipelines.dataset.steps.precision import (
    column_precisions,
    value_steps,
)


class TestValueSteps:
    def test_a_column_with_a_grid_is_kept_with_its_spacing(self):
        dtypes = {"n": "bigint", "amt": "decimal(18,2)", "flag": "boolean"}
        assert value_steps(dtypes, ["n", "amt", "flag"]) == {
            "n": 1.0, "amt": 0.01, "flag": 1.0}

    def test_float_and_double_state_no_grid_and_are_left_out(self):
        dtypes = {"n": "int", "f": "float", "d": "double"}
        assert value_steps(dtypes, ["n", "f", "d"]) == {"n": 1.0}

    def test_only_the_columns_asked_about(self):
        # The cast's selector decides the columns; a numeric column it did not
        # select is not the gate's to check.
        assert value_steps({"n": "int", "m": "int"}, ["n"]) == {"n": 1.0}


class TestColumnPrecisions:
    def test_pairs_each_checked_column_with_its_measurement(self):
        got = column_precisions(
            {"a": 5.0, "b": None, "unchecked": 9.0}, {"b": 1.0, "a": 0.01})
        assert got == {
            "a": ColumnPrecision(5.0, 0.01),
            "b": ColumnPrecision(None, 1.0),
        }
        assert list(got) == ["a", "b"]

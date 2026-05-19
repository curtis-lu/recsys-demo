"""Tests for the declarative search-space ParamSpec + build_trial_params."""

import pytest

from recsys_tfb.pipelines.training.search_space import (
    ParamSpec,
    build_trial_params,
    parse_search_space,
)


class FakeTrial:
    """Records suggest_* calls in order; returns the low/first deterministically."""

    def __init__(self):
        self.calls = []

    def suggest_int(self, name, low, high, step=1, log=False):
        self.calls.append(("int", name, low, high, step, log))
        return low

    def suggest_float(self, name, low, high, step=None, log=False):
        self.calls.append(("float", name, low, high, step, log))
        return low

    def suggest_categorical(self, name, choices):
        self.calls.append(("cat", name, tuple(choices)))
        return choices[0]


SPACE = [
    {"name": "learning_rate", "type": "float", "low": 0.001, "high": 0.1, "log": True},
    {"name": "num_leaves", "type": "int", "low": 4, "high": 64},
    {"name": "max_depth", "type": "int", "low": 3, "high": 8, "step": 1},
    {"name": "booster_kind", "type": "categorical", "choices": ["gbdt", "dart"]},
]


class TestParseSearchSpace:
    def test_parses_list_into_paramspecs(self):
        specs = parse_search_space(SPACE)
        assert [s.name for s in specs] == [
            "learning_rate", "num_leaves", "max_depth", "booster_kind"
        ]
        assert all(isinstance(s, ParamSpec) for s in specs)
        lr = specs[0]
        assert lr.type == "float" and lr.low == 0.001 and lr.high == 0.1 and lr.log is True
        assert specs[3].type == "categorical" and specs[3].choices == ["gbdt", "dart"]

    def test_when_field_parsed_but_stored(self):
        specs = parse_search_space([
            {"name": "x", "type": "int", "low": 1, "high": 9, "when": "num_leaves > 8"}
        ])
        assert specs[0].when == "num_leaves > 8"


class TestBuildTrialParams:
    def test_dispatches_in_list_order_with_kwargs(self):
        trial = FakeTrial()
        out = build_trial_params(trial, SPACE)
        assert out == {
            "learning_rate": 0.001, "num_leaves": 4,
            "max_depth": 3, "booster_kind": "gbdt",
        }
        assert trial.calls[0] == ("float", "learning_rate", 0.001, 0.1, None, True)
        assert trial.calls[1] == ("int", "num_leaves", 4, 64, 1, False)
        assert trial.calls[2] == ("int", "max_depth", 3, 8, 1, False)
        assert trial.calls[3] == ("cat", "booster_kind", ("gbdt", "dart"))

    def test_float_step_passed_when_set(self):
        trial = FakeTrial()
        build_trial_params(trial, [
            {"name": "ff", "type": "float", "low": 0.0, "high": 1.0, "step": 0.25}
        ])
        assert trial.calls[0] == ("float", "ff", 0.0, 1.0, 0.25, False)

    def test_name_is_both_suggest_name_and_return_key(self):
        trial = FakeTrial()
        out = build_trial_params(trial, [
            {"name": "min_child_samples", "type": "int", "low": 5, "high": 100}
        ])
        assert list(out.keys()) == ["min_child_samples"]
        assert trial.calls[0][1] == "min_child_samples"

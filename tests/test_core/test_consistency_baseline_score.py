"""A50 and the evaluation command's A21 (``--rebuild-dates``), #397."""

import pandas as pd
import pytest

from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    baseline_rebuild_dates_absent_errors,
    baseline_score_errors,
    resolved_baseline_rebuild_dates,
)


class TestBaselineScoreDomain:
    @pytest.mark.parametrize("block", [None, {}, {"lookback_months": 12},
                                       {"score": None}, {"score": "count"},
                                       {"score": "rate"}])
    def test_accepted(self, block):
        assert baseline_score_errors({"evaluation": {"baseline": block}}) == []

    def test_absent_evaluation_block(self):
        assert baseline_score_errors({}) == []

    @pytest.mark.parametrize("value", ["rates", "Rate", 1, True])
    def test_rejected(self, value):
        errors = baseline_score_errors(
            {"evaluation": {"baseline": {"score": value}}})
        assert len(errors) == 1 and errors[0].startswith("A50:")

    def test_an_unknown_key_is_rejected(self):
        """A typo'd key would otherwise keep the count mode silently."""
        errors = baseline_score_errors(
            {"evaluation": {"baseline": {"lookback_months": 12, "scroe": "rate"}}})
        assert len(errors) == 1 and "['scroe']" in errors[0]


class TestEvaluationRebuildDates:
    def _resolve(self, dates, rate_wired=True):
        return resolved_baseline_rebuild_dates(
            dates, rate_wired=rate_wired, eval_dates=["2025-01-31"],
            lookback_months=lambda: 12)

    def test_not_passed(self):
        assert self._resolve(None, rate_wired=False) == []

    def test_lookback_is_not_read_unless_the_flag_is_used_on_a_wired_run(self):
        def unreadable():
            raise AssertionError("lookback_months was read")

        for dates, wired in ((None, True), (["2024-06-30"], False)):
            try:
                resolved_baseline_rebuild_dates(
                    dates, rate_wired=wired, eval_dates=["2025-01-31"],
                    lookback_months=unreadable)
            except ConfigConsistencyError:
                pass

    def test_refused_when_rate_is_not_wired(self):
        with pytest.raises(ConfigConsistencyError, match="score: rate"):
            self._resolve(["2024-06-30"], rate_wired=False)

    def test_inside_the_window(self):
        assert self._resolve(["2024-06-30", "2024-01-31", "2024-06-30"]) == [
            "2024-01-31", "2024-06-30"]

    @pytest.mark.parametrize("date", ["2025-01-31", "2024-01-30"])
    def test_outside_the_window(self, date):
        with pytest.raises(ConfigConsistencyError, match="outside every"):
            self._resolve([date])

    def test_malformed(self):
        with pytest.raises(ConfigConsistencyError, match="non-ISO"):
            self._resolve(["2024/06/30"])

    def test_window_rule_matches_the_baseline_node(self):
        """The rule is written twice (core cannot import evaluation); pin it
        to the node's bounds so the two cannot drift apart."""
        from recsys_tfb.evaluation.baselines import _window_bounds

        lower, upper = _window_bounds("2025-03-31", 3)
        kwargs = dict(rate_wired=True, eval_dates=["2025-03-31"],
                      lookback_months=lambda: 3)
        assert resolved_baseline_rebuild_dates([lower], **kwargs) == [lower]
        with pytest.raises(ConfigConsistencyError):
            resolved_baseline_rebuild_dates([upper], **kwargs)
        day_before = str((pd.Timestamp(lower) - pd.Timedelta(days=1)).date())
        with pytest.raises(ConfigConsistencyError):
            resolved_baseline_rebuild_dates([day_before], **kwargs)


def test_a_rebuild_date_sample_pool_lacks_is_named():
    assert baseline_rebuild_dates_absent_errors(
        ["2024-06-30"], ["2024-06-30", "2024-07-31"]) == []
    errors = baseline_rebuild_dates_absent_errors(
        ["2024-06-15", "2024-06-30"], ["2024-06-30"])
    assert len(errors) == 1 and "['2024-06-15']" in errors[0]


def test_lookback_window_bounds_matches_pandas_month_offsets():
    """core's stdlib window rule against pd.DateOffset, month-end clamps
    included."""
    from recsys_tfb.core.date_ranges import lookback_window_bounds

    for day in pd.date_range("2024-01-01", "2025-12-31", freq="7D").append(
            pd.DatetimeIndex(["2025-03-31", "2024-02-29", "2025-05-31"])):
        for months in (1, 3, 12, 13):
            expected = (str((day - pd.DateOffset(months=months)).date()),
                        str(day.date()))
            assert lookback_window_bounds(str(day.date()), months) == expected


class TestPopularitySourceVersion:
    _SCHEMA = {"time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}
    _POOL = {"type": "HiveTableDataset", "database": "db", "table": "sample_pool"}
    _LABEL = {"type": "HiveTableDataset", "database": "db", "table": "label_table"}

    def _v(self, roles=None, pool=None, label=None):
        from recsys_tfb.core.versioning import compute_popularity_source_version

        return compute_popularity_source_version(
            roles or self._SCHEMA, pool or self._POOL, label or self._LABEL)

    def test_stable(self):
        assert self._v() == self._v()

    @pytest.mark.parametrize("change", [
        {"roles": {**_SCHEMA, "item": "ad"}},
        {"pool": {**_POOL, "table": "sample_pool_v2"}},
        {"pool": {**_POOL, "database": "prod"}},
        {"label": {**_LABEL, "table": "label_clicks"}},
    ])
    def test_moves_with_a_source_or_the_schema(self, change):
        assert self._v(**change) != self._v()

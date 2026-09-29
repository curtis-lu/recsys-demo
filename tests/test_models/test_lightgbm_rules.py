"""LightGBM's objective / metric rules, as the adapter declares them.

Moved from ``core/group_utils.py`` and ``core/consistency.py`` (ADR-0030
decision 3): the values are LightGBM's, so the adapter is where they live.
What every adapter's rules must satisfy is in ``test_adapter_contract.py``.
"""

import pytest

from recsys_tfb.models.base import AlgorithmRules
from recsys_tfb.models.lightgbm_adapter import LIGHTGBM_RULES, LightGBMAdapter


def test_the_adapter_declares_these_rules():
    assert LightGBMAdapter.rules is LIGHTGBM_RULES


class TestObjectiveClassification:
    def test_ranking_objectives_set(self):
        assert LIGHTGBM_RULES.ranking_objectives == frozenset(
            {"lambdarank", "rank_xendcg"})

    @pytest.mark.parametrize("obj", ["lambdarank", "rank_xendcg"])
    def test_is_ranking_true(self, obj):
        assert LIGHTGBM_RULES.is_ranking_objective(obj) is True

    @pytest.mark.parametrize("obj", ["binary", "regression", None, ""])
    def test_is_ranking_false(self, obj):
        assert LIGHTGBM_RULES.is_ranking_objective(obj) is False

    def test_ranking_metrics_set(self):
        assert LIGHTGBM_RULES.ranking_metrics == frozenset(
            {"ndcg", "map", "lambdarank"})


class TestDefaultMetricForObjective:
    def test_ranking_without_metric_defaults_ndcg(self):
        assert LIGHTGBM_RULES.default_metric_for_objective("lambdarank", None) == "ndcg"
        assert LIGHTGBM_RULES.default_metric_for_objective("rank_xendcg", "") == "ndcg"

    def test_ranking_with_metric_kept(self):
        assert LIGHTGBM_RULES.default_metric_for_objective("lambdarank", "ndcg") == "ndcg"
        assert LIGHTGBM_RULES.default_metric_for_objective("lambdarank", "map") == "map"

    def test_non_ranking_metric_unchanged(self):
        assert LIGHTGBM_RULES.default_metric_for_objective("binary", None) is None
        assert (
            LIGHTGBM_RULES.default_metric_for_objective("binary", "binary_logloss")
            == "binary_logloss"
        )


class TestObjectiveDropsZeroPositiveGroups:
    """Which objectives drop zero-positive query groups — derived, not configured."""

    def test_only_lambdarank_drops(self):
        assert LIGHTGBM_RULES.objective_drops_zero_positive_groups("lambdarank") is True

    @pytest.mark.parametrize("obj", ["rank_xendcg", "binary", "regression", None, ""])
    def test_everything_else_keeps_every_row(self, obj):
        assert LIGHTGBM_RULES.objective_drops_zero_positive_groups(obj) is False


class TestRulesRejectATableThatContradictsItself:
    def test_default_metric_must_be_a_ranking_metric(self):
        with pytest.raises(ValueError, match="default_ranking_metric"):
            AlgorithmRules(
                ranking_objectives=frozenset({"r"}),
                ranking_metrics=frozenset({"m"}),
                default_ranking_metric="other",
                zero_positive_group_dropping_objectives=frozenset(),
            )

    def test_only_a_ranking_objective_can_drop_groups(self):
        with pytest.raises(ValueError, match="not in ranking_objectives"):
            AlgorithmRules(
                ranking_objectives=frozenset({"r"}),
                ranking_metrics=frozenset({"m"}),
                default_ranking_metric="m",
                zero_positive_group_dropping_objectives=frozenset({"binary"}),
            )

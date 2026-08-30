"""Tests for steps/hpo_scoring.py: how one HPO trial is scored, and who
keeps the winner.

Its own file for the same reason ``test_search_space.py`` and
``test_hpo_resume.py`` are: this directory pairs one test module with one
non-``nodes`` module, so a reader looking for the trial-scoring tests does
not have to find them inside the 1100-line ``test_nodes.py``. The
end-to-end ``tune_hyperparameters`` tests stay there, because that is the
module they exercise.
"""

import types

import numpy as np
import pytest

from recsys_tfb.pipelines.training.steps import hpo_scoring
from recsys_tfb.pipelines.training.steps.hpo_scoring import _hpo_score


class TestHpoScore:
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_mean_ap_matches_compute_mean_ap(self):
        from recsys_tfb.evaluation.metrics import compute_mean_ap
        expected = compute_mean_ap(self.GROUPS, self.Y, self.SCORE)
        result = _hpo_score("mean_ap", self.GROUPS, None, self.Y, self.SCORE)
        assert result == pytest.approx(expected)

    def test_macro_per_item_map_matches_primitive(self):
        from recsys_tfb.evaluation.metrics import compute_macro_per_item_map
        expected = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        result = _hpo_score(
            "macro_per_item_map", self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(expected)

    def test_unknown_objective_raises_valueerror(self):
        with pytest.raises(ValueError, match="hpo_objective"):
            _hpo_score("not_a_metric", self.GROUPS, self.ITEMS, self.Y, self.SCORE)


class TestTrialScorer:
    """The scorer owns the search state; these pin who wins and who survives.

    Driven with fakes on purpose: the questions here are about bookkeeping
    across trials, and a real LightGBM fit would only make the same assertions
    slower and the scripted scores impossible to choose.
    """

    def _scorer(self, monkeypatch, scores):
        adapters = []

        class FakeAdapter:
            def __init__(self, tag):
                self.booster = type("B", (), {"best_iteration": 10 + tag})()

            def train(self, **kw):
                pass

            def predict(self, X):
                return np.zeros(len(X))

        class FakeHandle:
            def load(self, reference=None, params=None):
                return type("D", (), {"construct": lambda self_inner: self_inner})()

        def fake_get_adapter(algorithm):
            adapters.append(FakeAdapter(len(adapters)))
            return adapters[-1]

        calls = {"n": 0}

        def fake_score(*args, **kwargs):
            value = scores[calls["n"]]
            calls["n"] += 1
            return value

        monkeypatch.setattr(hpo_scoring, "get_adapter", fake_get_adapter)
        monkeypatch.setattr(
            hpo_scoring, "build_trial_params",
            lambda trial, search_space: {"n": trial.number},
        )
        monkeypatch.setattr(hpo_scoring, "_hpo_score", fake_score)

        scorer = hpo_scoring.TrialScorer(
            train_lgb_handle=FakeHandle(), train_dev_lgb_handle=FakeHandle(),
            X_val=np.zeros((4, 2)), y_val=np.array([1, 0, 1, 0]),
            groups_val=np.array([0, 0, 1, 1], dtype=np.int64), items_val=None,
            algorithm="lightgbm", algorithm_params={}, search_space=[],
            hpo_objective="mean_ap", seed=42, num_iterations=5,
            early_stopping_rounds=2, n_trials=len(scores),
            search_id="unit", study_dir=None,  # None = do not checkpoint
        )
        return scorer, adapters

    def test_winner_is_the_highest_scoring_trial_not_the_last(self, monkeypatch):
        """A later, worse trial must not displace the winner. Nothing would
        report it: a worse model is still a valid model, so the only symptom
        would be quietly shipping the wrong one."""
        scorer, adapters = self._scorer(monkeypatch, [0.10, 0.90, 0.50])

        for i in range(3):
            scorer(types.SimpleNamespace(number=i))

        assert scorer.best["score"] == pytest.approx(0.90)
        assert scorer.best["params"] == {"n": 1}
        assert scorer.best["model"] is adapters[1]
        assert scorer.best["iteration"] == 11

    def test_adopted_checkpoint_survives_a_worse_trial(self, monkeypatch):
        """What `adopt_checkpoint` is for: a resumed search must not let its
        first trial win by default over the previous run's better model — and
        then checkpoint over it."""
        scorer, _ = self._scorer(monkeypatch, [0.40])
        from_disk = object()
        scorer.adopt_checkpoint(
            {"score": 0.95, "model": from_disk, "iteration": 7,
             "params": {"n": "from-checkpoint"}}
        )

        scorer(types.SimpleNamespace(number=0))

        assert scorer.best["model"] is from_disk
        assert scorer.best["score"] == pytest.approx(0.95)
        assert scorer.best["params"] == {"n": "from-checkpoint"}
        assert scorer.best["iteration"] == 7

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
                self.predict_calls: list = []

            def train(self, **kw):
                pass

            def predict(self, X):
                self.predict_calls.append(len(X))
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


class TestPredictInRowBatches:
    """The val matrix is mapped from disk (#285), so predict must not ask for
    all of it at once — the mapping's whole point is that only the rows in
    flight are resident.

    LightGBM scores each row through the trees independently, so batching is
    an arithmetic no-op. That is a claim, not an assumption, which is why the
    first test compares bytes rather than approximate values.
    """

    class _RowAdapter:
        """Predicts a deterministic function of each row, and records the
        shape of every call so the batching itself is observable."""

        def __init__(self):
            self.call_rows: list = []

        def predict(self, X):
            self.call_rows.append(len(X))
            return X.sum(axis=1) * 3.0 - 1.0

    def _X(self, n_rows: int, n_cols: int = 4) -> np.ndarray:
        rng = np.random.default_rng(0)
        return rng.random((n_rows, n_cols)).astype(np.float32)

    def test_bit_identical_to_predicting_the_whole_matrix(self):
        X = self._X(97)
        whole = self._RowAdapter().predict(X)

        adapter = self._RowAdapter()
        batched = hpo_scoring._predict_in_row_batches(adapter, X, budget=48)

        assert np.array_equal(batched, whole)
        assert batched.dtype == whole.dtype

    def test_it_really_splits_into_batches(self):
        """Without this, the parity test above passes on a single call."""
        adapter = self._RowAdapter()
        # 4 columns x 4 B = 16 B/row -> 3 rows per batch
        hpo_scoring._predict_in_row_batches(adapter, self._X(97), budget=48)

        assert adapter.call_rows[:3] == [3, 3, 3]
        assert sum(adapter.call_rows) == 97
        assert max(adapter.call_rows) == 3

    def test_a_generous_budget_is_one_call(self):
        adapter = self._RowAdapter()
        hpo_scoring._predict_in_row_batches(adapter, self._X(97), budget=1 << 20)

        assert adapter.call_rows == [97]

    def test_a_memmap_is_handed_on_as_a_plain_array(self, tmp_path, monkeypatch):
        """The adapter is a model library's, not ours; it should never have to
        know the matrix came from a mapping."""
        from recsys_tfb.io import disk_matrix

        monkeypatch.setattr(disk_matrix, "SCRATCH_ROOT", tmp_path / "scratch")
        seen: list = []

        class TypeSpy:
            def predict(self, X):
                seen.append(type(X))
                return np.zeros(len(X))

        X = disk_matrix.open_disk_matrix((10, 4), np.dtype(np.float32), "unit")
        X[:] = 1.0
        hpo_scoring._predict_in_row_batches(TypeSpy(), X, budget=48)

        assert seen and all(t is np.ndarray for t in seen)

    def test_empty_val_still_returns_an_empty_prediction(self):
        adapter = self._RowAdapter()
        out = hpo_scoring._predict_in_row_batches(adapter, self._X(0), budget=48)

        assert len(out) == 0


class TestTrialScorerPredictsInBatches:
    def test_a_trial_never_predicts_the_whole_matrix_at_once(self, monkeypatch):
        """The scorer's public behaviour is unchanged (#285 keeps its
        semantics); what changes is that the matrix is walked, not handed over
        whole. A scored trial with one predict call would mean the mapping is
        fully resident for the length of the search."""
        scorer, adapters = TestTrialScorer()._scorer(monkeypatch, [0.5])
        monkeypatch.setattr(hpo_scoring, "PREDICT_BATCH_BYTES", 16)

        scorer(types.SimpleNamespace(number=0))

        # X_val is 4 rows x 2 float64 columns = 16 B/row -> 1 row per batch
        assert adapters[0].predict_calls == [1, 1, 1, 1]


class TestBatchedPredictAgainstRealLightGBM:
    """The fake above cannot fail the parity assertion — it is row-independent
    *by construction* (``X.sum(axis=1) * 3 - 1``), so batching it is trivially
    a no-op and the test proves the loop's bookkeeping, not the claim.

    The claim is about LightGBM: that a booster's score for a row does not
    depend on which other rows shared the call. That needs the real booster,
    so this trains a small one and compares the two ways of scoring it.
    ``np.array_equal``, not ``approx``: a float that moved at all would mean
    the batch size is an input to the model, and then HPO's chosen
    hyper-parameters would depend on ``PREDICT_BATCH_BYTES``.
    """

    def test_batched_equals_whole_matrix_bit_for_bit(self):
        import lightgbm as lgb

        rng = np.random.default_rng(7)
        X = rng.random((400, 12)).astype(np.float32)
        y = (X[:, 0] + X[:, 3] * 2 + rng.normal(0, 0.1, 400) > 1.0).astype(int)
        booster = lgb.train(
            {"objective": "binary", "num_leaves": 7, "verbose": -1, "seed": 0},
            lgb.Dataset(X, label=y),
            num_boost_round=15,
        )

        class Adapter:
            def predict(self, X_):
                return booster.predict(X_)

        whole = Adapter().predict(X)
        # 12 columns x 4 B = 48 B/row -> 3 rows per batch, ~134 calls
        batched = hpo_scoring._predict_in_row_batches(Adapter(), X, budget=144)

        assert np.array_equal(batched, whole)
        assert batched.dtype == whole.dtype

    def test_the_same_holds_for_a_matrix_mapped_from_disk(self, tmp_path, monkeypatch):
        """Production hands the scorer a ``np.memmap``, not an ndarray."""
        import lightgbm as lgb

        from recsys_tfb.io import disk_matrix

        monkeypatch.setattr(disk_matrix, "SCRATCH_ROOT", tmp_path / "scratch")
        rng = np.random.default_rng(11)
        rows = rng.random((300, 8)).astype(np.float32)
        y = (rows[:, 1] > 0.5).astype(int)
        booster = lgb.train(
            {"objective": "binary", "num_leaves": 5, "verbose": -1, "seed": 0},
            lgb.Dataset(rows, label=y),
            num_boost_round=10,
        )

        mapped = disk_matrix.open_disk_matrix((300, 8), np.dtype(np.float32), "unit")
        mapped[:] = rows

        class Adapter:
            def predict(self, X_):
                return booster.predict(X_)

        batched = hpo_scoring._predict_in_row_batches(Adapter(), mapped, budget=96)

        assert np.array_equal(batched, booster.predict(rows))

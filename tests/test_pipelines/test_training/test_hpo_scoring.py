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

        weighted: list = []

        class FakeDataset:
            def construct(self):
                return self

            def set_weight(self, w):
                weighted.append(w)

            def num_data(self):
                # Matches the length of the weight vectors handed to the
                # scorer below, so the length guard passes by default and a
                # test that wants it to fire changes the weights, not this.
                return 2

        class FakeHandle:
            def load(self, reference=None, params=None):
                return FakeDataset()

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
            train_weights=np.array([1.0, 3.0]),
            train_dev_weights=np.array([2.0, 4.0]),
            X_val=np.zeros((4, 2)), y_val=np.array([1, 0, 1, 0]),
            groups_val=np.array([0, 0, 1, 1], dtype=np.int64), items_val=None,
            algorithm="lightgbm", algorithm_params={}, search_space=[],
            hpo_objective="mean_ap", seed=42, num_iterations=5,
            early_stopping_rounds=2, n_trials=len(scores),
            search_id="unit", study_dir=None,  # None = do not checkpoint
        )
        scorer._weighted = weighted
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

    def test_every_trial_weights_both_datasets(self, monkeypatch):
        """Both splits get this run's weights, on every trial.

        The ``.bin`` carries none since #318, so a trial that skipped this
        would train unweighted — silently, under a model_version keyed by the
        very ``sample_weights`` it ignored. train_dev matters as much as
        train: it is the early-stopping valid set, and an unweighted stopping
        signal picks a different iteration for a weighted fit.
        """
        scorer, _ = self._scorer(monkeypatch, [0.1, 0.2])

        for i in range(2):
            scorer(types.SimpleNamespace(number=i))

        assert [w.tolist() for w in scorer._weighted] == [
            [1.0, 3.0], [2.0, 4.0], [1.0, 3.0], [2.0, 4.0],
        ]

    def test_a_wrong_length_weight_vector_stops_the_trial(self, monkeypatch):
        """The guard LightGBM does not provide.

        ``set_weight`` maps any all-ones array to ``None`` before its length is
        ever checked, and a length-zero array is vacuously all-ones — so the
        one shape a broken sidecar produces is exactly the one LightGBM
        accepts in silence. Without this the search would run to completion
        unweighted and publish under a model_version keyed by the weights it
        dropped.
        """
        scorer, _ = self._scorer(monkeypatch, [0.1])
        scorer.train_weights = np.array([])  # what a column-less sidecar gave

        with pytest.raises(ValueError, match="but the binary holds"):
            scorer(types.SimpleNamespace(number=0))

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


class TestWeightsReachTheTrainedModel:
    """The bug #318 fixes, asserted where it actually bit: the trained model.

    ``training.sample_weights`` feeds ``model_version`` and nothing in the
    lgb-binary cache path, so before this a second run with a different weight
    table hit the same ``.bin`` — weights baked in — and trained on the first
    run's weights. Every layer stayed quiet: the cache reported a hit, the
    booster trained, and the result was published under a model_version that
    named weights it had never seen.

    Real LightGBM and the real ``TrialScorer`` on purpose. The fakes above
    pin that ``set_weight`` is *called*; nothing but a fit can say the call
    changes the model, and "the call happens but does nothing" is exactly the
    shape a mock cannot rule out.
    """

    OBJECTIVES = ["binary", "lambdarank", "rank_xendcg"]

    def _parameters(self, objective, sample_weights):
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label"}},
            "training": {
                "algorithm_params": {"objective": objective, "verbosity": -1},
                "sample_weight_keys": ["prod_name"],
                "sample_weights": sample_weights,
            },
        }

    def _model_input(self, tmp_path):
        """One parquet pair a ranking objective can actually learn from.

        60 query groups of 4 items, one positive each. Small enough to fit in
        under a second, big enough that LightGBM grows real splits — with a
        handful of rows it grows none, every prediction is the same constant,
        and a test comparing two models passes whatever the weights did.
        """
        import pandas as pd
        from recsys_tfb.io.handles import ParquetHandle

        rng = np.random.default_rng(11)
        prods = ["a", "b", "c", "d"]

        def frame(n_cust, tag):
            rows = n_cust * len(prods)
            feat = rng.normal(size=rows)
            pos = rng.integers(0, len(prods), n_cust)
            label = np.zeros(rows, dtype=int)
            label[np.arange(n_cust) * len(prods) + pos] = 1
            pdf = pd.DataFrame({
                "cust_id": np.repeat([f"{tag}{i}" for i in range(n_cust)],
                                     len(prods)),
                "snap_date": pd.to_datetime(["2025-01-31"] * rows),
                "prod_name": prods * n_cust,
                "feat_a": feat + label * 0.8,
                "label": label,
            })
            pdf["feat_a"] = pdf["feat_a"].astype("float32")
            return pdf

        tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
        frame(60, "c").to_parquet(tr)
        frame(20, "d").to_parquet(dv)
        return ParquetHandle(str(tr)), ParquetHandle(str(dv))

    def _predictions(self, monkeypatch, tmp_path, objective, sample_weights):
        """One trial's model, trained the way the pipeline trains it."""
        from recsys_tfb.io.extract import extract_Xy_with_groups
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        prep = {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b", "c", "d"]},
        }
        train_h, dev_h = self._model_input(tmp_path)
        params = self._parameters(objective, sample_weights)
        # Same cache directory across both calls: that is the condition the
        # bug needed, and reusing it here is what makes this a regression
        # test rather than two unrelated fits.
        lgb_train, lgb_dev = LightGBMAdapter().prepare_train_inputs(
            train_h, dev_h, prep, params, str(tmp_path / "variant"))

        X_v, y_v, g_v = extract_Xy_with_groups(dev_h, prep, params)
        # Fixed hyper-parameters for both runs, so any difference in the
        # models is the weights and nothing else.
        monkeypatch.setattr(
            hpo_scoring, "build_trial_params", lambda trial, space: {})
        scorer = hpo_scoring.TrialScorer(
            train_lgb_handle=lgb_train, train_dev_lgb_handle=lgb_dev,
            train_weights=lgb_train.sample_weights(params, prep),
            train_dev_weights=lgb_dev.sample_weights(params, prep),
            X_val=X_v, y_val=y_v, groups_val=g_v, items_val=None,
            algorithm="lightgbm",
            algorithm_params=dict(params["training"]["algorithm_params"]),
            search_space=[], hpo_objective="mean_ap", seed=42,
            num_iterations=30, early_stopping_rounds=0, n_trials=1,
            search_id="weights", study_dir=None,
        )
        scorer(types.SimpleNamespace(number=0))
        return scorer.best["model"].predict(np.asarray(X_v))

    @pytest.mark.parametrize("objective", OBJECTIVES)
    def test_a_new_weight_table_trains_a_different_model(
        self, monkeypatch, tmp_path, objective
    ):
        plain = self._predictions(
            monkeypatch, tmp_path, objective, {"a": 1.0})
        heavy = self._predictions(
            monkeypatch, tmp_path, objective, {"a": 20.0})
        assert not np.allclose(plain, heavy), (
            "the second run reproduced the first run's model — weights did "
            "not reach training"
        )

    @pytest.mark.parametrize("objective", OBJECTIVES)
    def test_the_same_weight_table_trains_the_same_model(
        self, monkeypatch, tmp_path, objective
    ):
        """The other half: rebuilding must not be a source of drift.

        Without this the test above passes for a model that differs run to
        run for any reason at all, and would keep passing if weights stopped
        mattering entirely.
        """
        once = self._predictions(monkeypatch, tmp_path, objective, {"a": 4.0})
        twice = self._predictions(monkeypatch, tmp_path, objective, {"a": 4.0})
        assert np.array_equal(once, twice)


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

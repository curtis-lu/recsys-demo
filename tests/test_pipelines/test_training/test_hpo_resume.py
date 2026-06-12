"""Tests for hpo_resume: persistent study lifecycle + checkpoint."""

import optuna

from recsys_tfb.pipelines.training import hpo_resume


def _obj(trial):
    return trial.suggest_float("x", 0.0, 1.0)


class TestStudyLifecycle:
    def test_open_study_creates_journal_and_counts(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        study = hpo_resume.open_study(sd, "sid", seed=42)
        study.optimize(_obj, n_trials=1)
        assert (sd / hpo_resume.JOURNAL).exists()
        assert hpo_resume.count_completed(study) == 1

    def test_reload_sees_prior_trials(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        s1 = hpo_resume.open_study(sd, "sid", seed=42)
        s1.optimize(_obj, n_trials=2)
        del s1  # simulate crash: drop the in-memory study
        s2 = hpo_resume.open_study(sd, "sid", seed=42)
        assert hpo_resume.count_completed(s2) == 2

    def test_clear_study_dir(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.open_study(sd, "sid", seed=42).optimize(_obj, n_trials=1)
        assert sd.exists()
        hpo_resume.clear_study_dir(sd)
        assert not sd.exists()
        hpo_resume.clear_study_dir(sd)  # no error on missing

    def test_hpo_study_dir_path(self):
        from pathlib import Path
        assert hpo_resume.hpo_study_dir("abc") == Path("data") / "models" / "_hpo" / "abc"


def _tiny_adapter():
    import numpy as np
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    rng = np.random.RandomState(0)
    X = rng.rand(40, 3)
    y = (rng.rand(40) < 0.3).astype(float)
    a = LightGBMAdapter()
    a.train(
        X_train=X, y_train=y, X_val=X, y_val=y,
        params={"objective": "binary", "verbosity": -1, "num_iterations": 5},
    )
    return a


class TestCheckpoint:
    def test_round_trip(self, tmp_path):
        import numpy as np
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.write_checkpoint(
            sd, _tiny_adapter(), score=0.5, best_iteration=3,
            best_params={"learning_rate": 0.1}, trial_number=2, search_id="sid",
        )
        loaded = hpo_resume.load_checkpoint(sd, "lightgbm")
        assert loaded is not None
        assert loaded["score"] == 0.5
        assert loaded["iteration"] == 3          # from meta, not reloaded booster
        assert loaded["params"] == {"learning_rate": 0.1}
        assert loaded["trial_number"] == 2
        preds = loaded["model"].predict(np.random.RandomState(1).rand(5, 3))
        assert preds.shape == (5,)

    def test_load_missing_returns_none(self, tmp_path):
        assert hpo_resume.load_checkpoint(tmp_path / "nope", "lightgbm") is None

    def test_no_temp_files_left(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.write_checkpoint(
            sd, _tiny_adapter(), score=0.1, best_iteration=1,
            best_params={}, trial_number=0, search_id="sid",
        )
        leftovers = list((sd / "checkpoint").glob("*.tmp"))
        assert leftovers == []

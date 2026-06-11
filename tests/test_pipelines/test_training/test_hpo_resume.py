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

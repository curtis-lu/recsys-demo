import numpy as np
import pytest

from recsys_tfb.pipelines.training.staged_stage2 import tune_stage2

BASE = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
        "num_threads": 1, "num_leaves": 5, "learning_rate": 0.2,
        "num_iterations": 15, "early_stopping_rounds": 5}


def _toy(n=200, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X2 = np.column_stack([rng.normal(loc=y), rng.normal(size=n),
                          rng.random(n), (np.arange(n) % 2).astype(float)])
    qg = np.repeat(np.arange(n // 4), 4)
    items = np.array(["p1", "p2"] * (n // 2), dtype=object)
    return X2, y, qg, items


def _params(n_trials, tmp_path, **over):
    p = {
        "random_seed": 42,
        "search_id": "s2test01",           # 繞過 compute_search_id
        "hpo_checkpointing": True,
        "training": {
            "algorithm": "lightgbm",
            "n_trials": n_trials,
            "hpo_objective": "mean_ap",
            "search_space": [
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8},
            ],
        },
    }
    p.update(over)
    return p


class TestTuneStage2:
    def test_n_trials_zero_single_fit_no_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # hpo_study_dir 是相對路徑
        X2, y, qg, items = _toy()
        best, adapter, meta = tune_stage2(
            "binary", dict(BASE), [3], X2, y, None, qg,
            X2, y, None, qg, items, _params(0, tmp_path))
        assert best == {} and meta["n_trials"] == 0
        assert np.isfinite(adapter.predict(X2)).all()
        assert not (tmp_path / "data").exists()  # 零 trial 不落 study

    def test_hpo_runs_and_persists_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        best, adapter, meta = tune_stage2(
            "binary", dict(BASE), [3], X2, y, None, qg,
            X2, y, None, qg, items, _params(2, tmp_path))
        study_dir = tmp_path / "data" / "models" / "_hpo" / "s2test01"
        assert (study_dir / "study_journal.log").exists()
        assert (study_dir / "checkpoint" / "model.txt").exists()
        assert "num_leaves" in best and meta["score"] > -1.0

    def test_resume_runs_only_remaining_trials(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, None, qg, items, _params(2, tmp_path))
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        calls = {"n": 0}
        real = mod.fit_stage2
        def spy(*a, **kw):
            calls["n"] += 1
            return real(*a, **kw)
        monkeypatch.setattr(mod, "fit_stage2", spy)
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, None, qg, items, _params(3, tmp_path))
        assert calls["n"] == 1  # 已完成 2，目標 3 → 只補 1 trial

    def test_fresh_hpo_clears_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, None, qg, items, _params(2, tmp_path))
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        calls = {"n": 0}
        real = mod.fit_stage2
        def spy(*a, **kw):
            calls["n"] += 1
            return real(*a, **kw)
        monkeypatch.setattr(mod, "fit_stage2", spy)
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, None, qg, items,
                    _params(2, tmp_path, _fresh_hpo=True))
        assert calls["n"] == 2  # 清掉重搜

    def test_unknown_hpo_objective_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        p = _params(1, tmp_path)
        p["training"]["hpo_objective"] = "auc"
        with pytest.raises(ValueError, match="hpo_objective"):
            tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                        X2, y, None, qg, items, p)

    def test_trial_start_and_completed_logged(self, tmp_path, monkeypatch,
                                              caplog):
        # Observability 要求 #2：每 trial 起訖各一條
        import logging
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        with caplog.at_level(logging.INFO):
            tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                        X2, y, None, qg, items, _params(2, tmp_path))
        assert caplog.text.count("tune_stage2: trial=") >= 4  # 2 trial × 起訖
        assert "start params=" in caplog.text
        assert "completed score=" in caplog.text

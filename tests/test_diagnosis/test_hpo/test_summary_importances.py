import optuna

from recsys_tfb.diagnosis.hpo.collect import collect_trials
from recsys_tfb.diagnosis.hpo.summary import build_summary, compute_importances


def test_importances_real_study():
    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    study.optimize(
        lambda t: t.suggest_float("x", 0, 1) + 0.5 * t.suggest_float("y", 0, 1),
        n_trials=12,
    )
    imp = compute_importances(study)
    assert imp is not None
    assert set(imp) <= {"x", "y"}
    assert all(isinstance(v, float) for v in imp.values())


def test_importances_degenerate_returns_none():
    study = optuna.create_study(direction="maximize")
    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=1)  # 1 trial → raise → None
    assert compute_importances(study) is None


def test_build_summary_shape():
    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    study.optimize(lambda t: t.suggest_int("num_leaves", 20, 100), n_trials=6)
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    payload = collect_trials(
        study, ss, model_version="m", search_id="s", hpo_objective="mean_ap",
        seed=1, n_trials_target=6, best_iteration=0, generated_at="t",
    )
    s = build_summary(study, payload, patience=3, hi_thresh=0.98, lo_thresh=0.02)
    assert set(s) == {"convergence", "boundary", "importances"}
    assert "num_leaves" in s["boundary"]
    assert s["convergence"]["n_completed"] == 6

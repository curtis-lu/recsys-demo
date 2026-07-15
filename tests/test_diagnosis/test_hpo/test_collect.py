import optuna

from recsys_tfb.diagnosis.hpo.collect import collect_trials


def _tiny_study(n=4):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    s.optimize(lambda t: t.suggest_float("x", 0.0, 1.0), n_trials=n)
    return s


def test_collect_trials_schema():
    study = _tiny_study(4)
    ss = [{"name": "x", "type": "float", "low": 0.0, "high": 1.0}]
    payload = collect_trials(
        study, ss, model_version="mv1", search_id="sid",
        hpo_objective="mean_ap", seed=1, n_trials_target=10,
        best_iteration=42, generated_at="2026-07-15T00:00:00",
    )
    assert payload["schema_version"] == 1
    m = payload["meta"]
    assert m["model_version"] == "mv1"
    assert m["search_id"] == "sid"
    assert m["direction"] == "maximize"
    assert m["sampler"] == "TPESampler"
    assert m["n_completed"] == 4
    assert m["n_trials_target"] == 10
    assert m["search_space"] == ss
    assert m["generated_at"] == "2026-07-15T00:00:00"
    assert len(payload["trials"]) == 4
    row = payload["trials"][0]
    assert set(row) == {"number", "value", "state", "params", "duration_s"}
    assert row["state"] == "COMPLETE"
    assert payload["best"]["best_iteration"] == 42
    assert payload["best"]["value"] == study.best_value


def test_collect_trials_no_completed():
    study = optuna.create_study(direction="maximize")  # 0 trials
    payload = collect_trials(
        study, [], model_version="m", search_id="s", hpo_objective="mean_ap",
        seed=1, n_trials_target=5, best_iteration=0, generated_at="t",
    )
    assert payload["meta"]["n_completed"] == 0
    assert payload["best"] is None
    assert payload["trials"] == []

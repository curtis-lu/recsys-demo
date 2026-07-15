from recsys_tfb.diagnosis.hpo.summary import compute_boundary, compute_convergence


def _t(number, value, state="COMPLETE"):
    return {"number": number, "value": value, "state": state, "params": {}, "duration_s": 1.0}


def test_convergence_plateau_triggers():
    trials = [_t(i, 0.30) for i in range(5)]  # best at #0, never improves
    r = compute_convergence(trials, patience=3)
    assert r["plateau"] is True
    assert r["best_trial_number"] == 0
    assert r["trials_since_improvement"] == 4


def test_convergence_not_plateau():
    trials = [_t(0, 0.30), _t(1, 0.31), _t(2, 0.35)]  # best last
    r = compute_convergence(trials, patience=3)
    assert r["plateau"] is False
    assert r["best_trial_number"] == 2
    assert r["trials_since_improvement"] == 0


def test_convergence_ignores_incomplete():
    trials = [_t(0, 0.30), _t(1, None, state="FAIL"), _t(2, 0.40)]
    r = compute_convergence(trials, patience=1)
    assert r["n_completed"] == 2
    assert r["best_value"] == 0.40


def test_convergence_empty():
    r = compute_convergence([], patience=3)
    assert r["n_completed"] == 0
    assert r["plateau"] is False
    assert r["best_value"] is None


def test_boundary_widen_high_linear_int():
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    r = compute_boundary({"num_leaves": 99}, ss, hi_thresh=0.98, lo_thresh=0.02)
    b = r["num_leaves"]
    assert b["suggestion"] == "widen_high"
    assert b["at_high"] is True


def test_boundary_widen_low_log_scale():
    ss = [{"name": "lr", "type": "float", "low": 1e-3, "high": 1e-1, "log": True}]
    r = compute_boundary({"lr": 1.05e-3}, ss, hi_thresh=0.98, lo_thresh=0.02)
    b = r["lr"]
    assert b["suggestion"] == "widen_low"
    assert b["scale"] == "log"


def test_boundary_ok_middle():
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    r = compute_boundary({"num_leaves": 60}, ss, hi_thresh=0.98, lo_thresh=0.02)
    assert r["num_leaves"]["suggestion"] == "ok"


def test_boundary_categorical_no_suggestion():
    ss = [{"name": "bt", "type": "categorical", "choices": ["gbdt", "dart"]}]
    r = compute_boundary({"bt": "dart"}, ss, hi_thresh=0.98, lo_thresh=0.02)
    assert r["bt"]["suggestion"] == "n/a"

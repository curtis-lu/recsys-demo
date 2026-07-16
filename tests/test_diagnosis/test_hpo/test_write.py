import json

import optuna
import pytest

import recsys_tfb.diagnosis.hpo.write as W
from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics


def _study(n=8):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    s.optimize(
        lambda t: t.suggest_int("num_leaves", 20, 100) / 100
        + 0.3 * t.suggest_float("lr", 1e-3, 1e-1, log=True),
        n_trials=n,
    )
    return s


_SS = [
    {"name": "num_leaves", "type": "int", "low": 20, "high": 100},
    {"name": "lr", "type": "float", "low": 1e-3, "high": 1e-1, "log": True},
]


def _call(study, params):
    write_hpo_diagnostics(
        study, _SS, params, search_id="sid", hpo_objective="mean_ap",
        seed=1, n_trials_target=10, best_iteration=7,
    )


def test_write_end_to_end(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _call(_study(8), {"model_version": "mvE"})
    hpo = tmp_path / "data/models/mvE/diagnostics/hpo"
    assert json.loads((hpo / "hpo_trials.json").read_text())["schema_version"] == 1
    summary = json.loads((hpo / "hpo_summary.json").read_text())
    assert set(summary) == {"convergence", "boundary", "importances"}
    for f in ("optimization_history.html", "param_importances.html", "slice.html",
              "contour.html", "parallel_coordinate.html", "plotly.min.js"):
        assert (hpo / f).exists()


def test_write_disabled_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _call(_study(4), {"model_version": "mvD",
                      "diagnostics": {"hpo_search": {"enabled": False}}})
    assert not (tmp_path / "data/models/mvD/diagnostics/hpo").exists()


def test_write_json_survives_render_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError("render boom")

    monkeypatch.setattr(W, "render_charts", _boom)
    with pytest.raises(RuntimeError):
        _call(_study(4), {"model_version": "mvR"})
    hpo = tmp_path / "data/models/mvR/diagnostics/hpo"
    assert (hpo / "hpo_trials.json").exists()   # JSON 在 render 前已落地
    assert (hpo / "hpo_summary.json").exists()

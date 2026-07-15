import optuna

from recsys_tfb.diagnosis.hpo.render import render_charts


def _study(nparams=2, n=8):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )

    def obj(t):
        v = t.suggest_float("x", 0, 1)
        if nparams >= 2:
            v += 0.5 * t.suggest_float("y", 0, 1)
        return v

    s.optimize(obj, n_trials=n)
    return s


def test_render_all_five_with_shared_js(tmp_path):
    written = render_charts(_study(nparams=2, n=8), tmp_path)
    assert set(written) == {
        "optimization_history.html", "param_importances.html", "slice.html",
        "contour.html", "parallel_coordinate.html",
    }
    for f in written:
        assert (tmp_path / f).stat().st_size > 0
    # directory 模式：共用一份 plotly.min.js，各 HTML 才會是 KB 級
    assert (tmp_path / "plotly.min.js").exists()
    assert (tmp_path / "optimization_history.html").stat().st_size < 100_000


def test_render_degenerate_skips_importances(tmp_path):
    # 完成 trial <2 → plot_param_importances raise（其餘 4 張照畫）
    written = render_charts(_study(nparams=2, n=1), tmp_path)
    assert "param_importances.html" not in written
    assert "optimization_history.html" in written
    assert "slice.html" in written
    assert len(written) == 4

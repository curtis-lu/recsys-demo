"""compare_eval_runs: what it masks in the HTML, and what it must still see."""

import json

from scripts.local.compare_eval_runs import compare_dirs, main

_PLOT = ('<div id="{id}" class="plotly-graph-div"></div>'
         '<script>Plotly.newPlot("{id}", [])</script>')


def _html(generated_at: str, plot_id: str, value: str = "0.5000") -> str:
    return (
        "<table><tr><th>Generated At</th>"
        f"<td>{generated_at}</td></tr></table>"
        + _PLOT.format(id=plot_id)
        + f"<td>{value}</td>"
    )


def _run(root, name, *, generated_at="2026-10-22 23:44:01",
         plot_id="0b6f2c5e-1d1a-4c5e-9f00-2b7e6a1c9d10", value="0.5000",
         metrics=None, manifest_time="23:44"):
    run = root / name / "c8de1716" / "20260131"
    run.mkdir(parents=True)
    (run / "report.html").write_text(_html(generated_at, plot_id, value))
    (run / "metrics.json").write_text(
        json.dumps(metrics or {"map@1": 0.5, "recall@1": 0.4}, indent=2))
    (run / "manifest.json").write_text(json.dumps({"finished": manifest_time}))
    return root / name


def test_the_run_time_and_the_plot_ids_are_the_only_noise(tmp_path):
    a = _run(tmp_path, "a")
    b = _run(tmp_path, "b", generated_at="2026-10-23 00:12:59",
             plot_id="7d3a9b10-55c2-4e0f-8a61-0c2f9e7b4a21",
             manifest_time="00:13")
    assert {status for status, _ in compare_dirs(a, b)} == {"SAME"}
    assert main([str(a), str(b)]) == 0


def test_a_changed_number_in_the_html_is_reported(tmp_path):
    a = _run(tmp_path, "a")
    b = _run(tmp_path, "b", value="0.5001")
    assert ("DIFF", "c8de1716/20260131/report.html") in compare_dirs(a, b)
    assert main([str(a), str(b)]) == 1


def test_json_is_compared_byte_for_byte_key_order_included(tmp_path):
    a = _run(tmp_path, "a", metrics={"map@1": 0.5, "recall@1": 0.4})
    b = _run(tmp_path, "b", metrics={"recall@1": 0.4, "map@1": 0.5})
    assert ("DIFF", "c8de1716/20260131/metrics.json") in compare_dirs(a, b)


def test_a_file_on_one_side_only_is_reported(tmp_path):
    a = _run(tmp_path, "a")
    b = _run(tmp_path, "b")
    (b / "c8de1716" / "20260131" / "report_comparison.html").write_text("x")
    assert ("MISSING", "c8de1716/20260131/report_comparison.html") in \
        compare_dirs(a, b)

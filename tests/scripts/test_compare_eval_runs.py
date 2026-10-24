"""compare_eval_runs: what it masks in the HTML, and what it must still see."""

import json

from scripts.local.compare_eval_runs import compare_dirs, main

_PLOT = ('<div id="{id}" class="plotly-graph-div"></div>'
         '<script>if (document.getElementById("{id}")) '
         '{{Plotly.newPlot("{id}", [])}}</script>')


def _html(generated_at: str, plot_id: str, value: str = "0.5000",
          label: str = "Generated At") -> str:
    return (
        f"<table><tr><th>{label}</th>"
        f"<td>{generated_at}</td></tr></table>"
        + _PLOT.format(id=plot_id)
        + f"<td>{value}</td>"
    )


_ITEM_ID = "3f2c1a9e-8b7d-4c6e-9a1b-2d3e4f5a6b7c"


def _run(root, name, *, generated_at="2026-10-22 23:44:01",
         plot_id="0b6f2c5e-1d1a-4c5e-9f00-2b7e6a1c9d10", value="0.5000",
         metrics=None, created_at="2026-10-22T16:56:47", run_id="run-1",
         git_commit="395c9571", manifest_extra=None, label="Generated At"):
    run = root / name / "c8de1716" / "20260131"
    run.mkdir(parents=True)
    (run / "report.html").write_text(
        _html(generated_at, plot_id, value, label))
    (run / "metrics.json").write_text(
        json.dumps(metrics or {"map@1": 0.5, "recall@1": 0.4}, indent=2))
    (run / "manifest.json").write_text(json.dumps({
        "created_at": created_at, "git_commit": git_commit, "run_id": run_id,
        **(manifest_extra or {})}))
    return root / name


def test_the_run_time_and_the_plot_ids_are_the_only_noise(tmp_path):
    a = _run(tmp_path, "a")
    b = _run(tmp_path, "b", generated_at="2026-10-23 00:12:59",
             plot_id="7d3a9b10-55c2-4e0f-8a61-0c2f9e7b4a21",
             created_at="2026-10-22T16:57:55", run_id="run-2",
             git_commit="83affa56")
    assert {status for status, _ in compare_dirs(a, b)} == {"SAME"}
    assert main([str(a), str(b)]) == 0


def test_the_main_report_s_chinese_run_time_cell_is_masked_too(tmp_path):
    """#537: the main report's metadata says 產生時間; the comparison report
    still says Generated At."""
    a = _run(tmp_path, "a", label="產生時間")
    b = _run(tmp_path, "b", label="產生時間", generated_at="2026-10-23 00:12:59")
    assert {status for status, _ in compare_dirs(a, b)} == {"SAME"}


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


def test_a_uuid_shaped_value_outside_a_plot_id_is_still_compared(tmp_path):
    """Only the ids plotly declares for its figures are run-time noise; an
    item named by a uuid is data."""
    a = _run(tmp_path, "a", value=_ITEM_ID)
    b = _run(tmp_path, "b", value=_ITEM_ID.replace("3f2c", "4f2c"))
    assert ("DIFF", "c8de1716/20260131/report.html") in compare_dirs(a, b)


def test_the_manifest_is_compared_apart_from_its_run_fields(tmp_path):
    a = _run(tmp_path, "a", manifest_extra={"model_version": "c8de1716"})
    b = _run(tmp_path, "b", created_at="later", run_id="run-2",
             manifest_extra={"model_version": "deadbeef"})
    assert ("DIFF", "c8de1716/20260131/manifest.json") in compare_dirs(a, b)


def test_a_missing_or_empty_run_directory_fails(tmp_path):
    a = _run(tmp_path, "a")
    (tmp_path / "empty").mkdir()
    assert main([str(a), str(tmp_path / "no-such-run")]) == 1
    assert main([str(tmp_path / "empty"), str(tmp_path / "empty")]) == 1

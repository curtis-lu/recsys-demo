"""eval_parity: the temporary conf/local overlay and the source digest. The
Spark runs themselves are exercised by running the script."""

import pytest
import yaml

from scripts.local.eval_parity import overlay, overlay_files, src_digest

_CATALOG = {
    "training_eval_predictions": {
        "type": "HiveTableDataset",
        "columns": [{"name": "cust_id", "type": "STRING"},
                    {"name": "score", "type": "DOUBLE"},
                    {"name": "label", "type": "INT"}],
    },
}


@pytest.fixture
def conf(tmp_path):
    (tmp_path / "base").mkdir()
    (tmp_path / "base" / "catalog.yaml").write_text(yaml.safe_dump(_CATALOG))
    (tmp_path / "local").mkdir()
    (tmp_path / "local" / ".gitkeep").write_text("")
    return tmp_path


def test_the_catalog_overlay_keeps_every_base_column_and_adds_the_weight(conf):
    files = overlay_files(conf / "base", model_version="abc12345")
    columns = yaml.safe_load(files["catalog.yaml"])[
        "training_eval_predictions"]["columns"]
    assert [c["name"] for c in columns] == [
        "cust_id", "score", "label", "zero_positive_group_weight"]
    evaluation = yaml.safe_load(files["parameters_evaluation.yaml"])[
        "evaluation"]
    assert evaluation["compare_sources"]["parity_self"]["model_version"] \
        == "abc12345"
    # The family is the --prediction-quality mode, not a switch (A34).
    assert "report" not in evaluation


def test_the_overlay_is_removed_even_when_the_run_fails(conf):
    files = overlay_files(conf / "base", model_version=None)
    with pytest.raises(RuntimeError):
        with overlay(conf / "local", files):
            assert (conf / "local" / "catalog.yaml").is_file()
            raise RuntimeError("the evaluation failed")
    assert sorted(p.name for p in (conf / "local").iterdir()) == [".gitkeep"]


def test_an_existing_local_conf_is_refused_and_left_alone(conf):
    mine = conf / "local" / "parameters.yaml"
    mine.write_text("mine: 1\n")
    with pytest.raises(RuntimeError, match="parameters.yaml"):
        with overlay(conf / "local", overlay_files(conf / "base", None)):
            pass
    assert sorted(p.name for p in (conf / "local").iterdir()) == [
        ".gitkeep", "parameters.yaml"]
    assert mine.read_text() == "mine: 1\n"


def test_the_digest_follows_any_source_file(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "a.py").write_text("x = 1\n")
    before = src_digest(tmp_path)
    assert src_digest(tmp_path) == before
    (tmp_path / "pkg" / "a.py").write_text("x = 2\n")
    edited = src_digest(tmp_path)
    assert edited != before
    (tmp_path / "pkg" / "new.py").write_text("y = 2\n")
    assert src_digest(tmp_path) != edited


def _fake_root(tmp_path, conf):
    root = tmp_path / "root"
    (root / "conf").mkdir(parents=True)
    for sub in ("base", "local"):
        (root / "conf" / sub).symlink_to(conf / sub)
    (root / "src").mkdir()
    (root / "src" / "m.py").write_text("x = 1\n")
    return root


def test_a_file_the_new_run_did_not_write_is_not_carried_over(
        tmp_path, conf, monkeypatch):
    """The run's outputs are cleared first: a file the changed code stopped
    writing must show as missing, not as the previous run's copy."""
    import scripts.local.eval_parity as ep

    root = _fake_root(tmp_path, conf)
    out = root / "data" / "evaluation" / "abc12345" / "20260131"
    # Two commands a run: the standard one, then --prediction-quality.
    writes = iter([("metrics.json", "report_comparison.html"), (),
                   ("metrics.json",), ()])
    commands = []

    def fake_cli(_root, *args):
        commands.append(args)
        out.mkdir(parents=True, exist_ok=True)
        for name in next(writes):
            (out / name).write_text("{}")

    monkeypatch.setattr(ep, "_cli", fake_cli)
    monkeypatch.setattr(ep, "_latest_model", lambda _root: "abc12345")
    first = ep.run(root, "first")
    second = ep.run(root, "second")
    assert (first / "20260131" / "report_comparison.html").is_file()
    assert not (second / "20260131" / "report_comparison.html").exists()
    # Every page: the mode runs after the standard run, on what it landed.
    assert [("--compare" in c, "--prediction-quality" in c)
            for c in commands[:2]] == [(True, False), (False, True)]


@pytest.mark.parametrize("label", ["../src", "a/b", "..", ""])
def test_a_label_that_is_not_a_plain_name_is_refused(tmp_path, conf, label):
    import scripts.local.eval_parity as ep

    root = _fake_root(tmp_path, conf)
    with pytest.raises(SystemExit, match="label"):
        ep.run(root, label)
    assert (root / "src" / "m.py").is_file()

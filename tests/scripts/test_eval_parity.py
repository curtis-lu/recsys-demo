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
    assert evaluation["report"]["sections"]["prediction_quality"] is True


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

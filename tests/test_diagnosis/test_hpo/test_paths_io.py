import json

from recsys_tfb.diagnosis.hpo._io import atomic_write_json
from recsys_tfb.diagnosis.hpo.paths import hpo_dir


def test_hpo_dir_under_diagnostics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = hpo_dir({"model_version": "mv1"})
    assert d.as_posix() == "data/models/mv1/diagnostics/hpo"
    assert d.exists()


def test_atomic_write_json_roundtrip_and_overwrite(tmp_path):
    p = tmp_path / "sub" / "x.json"
    atomic_write_json(p, {"a": 1, "中": "文"})
    assert json.loads(p.read_text()) == {"a": 1, "中": "文"}
    atomic_write_json(p, {"a": 2})  # idempotent overwrite
    assert json.loads(p.read_text()) == {"a": 2}
    assert list((tmp_path / "sub").glob("*.tmp")) == []  # no leftover temp

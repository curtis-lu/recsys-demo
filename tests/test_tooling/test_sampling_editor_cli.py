import json
import importlib.util
from pathlib import Path

from typer.testing import CliRunner

SPEC = Path("/Users/curtislu/projects/recsys_tfb/.worktrees/"
            "sampling-overrides-editor-tool/scripts/sampling_overrides_editor.py")


def _load_app():
    spec = importlib.util.spec_from_file_location("soe", SPEC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.app


class TestToYamlCli:
    def test_to_yaml_prints_both_blocks(self, tmp_path):
        export = [{"segment": "mass", "product": "a", "ratio": 0.5, "weight": 1.0},
                  {"segment": "mass", "product": "b", "ratio": 1.0, "weight": 3.0}]
        jf = tmp_path / "e.json"
        jf.write_text(json.dumps(export))
        # minimal params yaml the command reads for A5/A9
        params = tmp_path / "p.yaml"
        params.write_text(
            "schema:\n  columns:\n    item: prod_name\n"
            "  categorical_values:\n    prod_name: [a, b]\n"
            "dataset:\n  prepare_model_input:\n"
            "    categorical_columns: [prod_name]\n"
            "  sample_group_keys: [cust_segment_typ, prod_name, label]\n")
        r = CliRunner().invoke(
            _load_app(), ["to-yaml", str(jf), "--params", str(params)])
        assert r.exit_code == 0, r.output
        assert "sample_ratio_overrides:" in r.output
        assert "mass|a|0" in r.output
        assert "sample_weights:" in r.output
        assert "mass|b" in r.output

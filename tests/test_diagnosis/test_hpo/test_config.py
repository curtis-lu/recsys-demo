from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]


def test_hpo_search_config_block_present():
    cfg = yaml.safe_load((REPO / "conf/base/parameters_training.yaml").read_text())
    hs = cfg["diagnostics"]["hpo_search"]
    assert hs["enabled"] is True
    assert hs["patience"] == 10
    assert hs["boundary_hi"] == 0.98
    assert hs["boundary_lo"] == 0.02

"""Regression: parameters_evaluation.yaml / parameters.yaml carry the refactor's new keys."""

from pathlib import Path

import yaml


def _load():
    p = Path("conf/base/parameters_evaluation.yaml")
    return yaml.safe_load(p.read_text())["evaluation"]


def _load_base():
    p = Path("conf/base/parameters.yaml")
    return yaml.safe_load(p.read_text())


def test_k_values_is_superset():
    assert _load()["k_values"] == [1, 2, 3, 4, 5, "all"]


def test_product_categories_block():
    # evaluation only holds the toggle; mapping lives in top-level parameters.yaml
    pc_eval = _load()["product_categories"]
    assert pc_eval["enabled"] is True
    assert "mapping" not in pc_eval
    assert "unmapped" not in pc_eval
    # shared mapping in parameters.yaml
    pc = _load_base()["product_categories"]
    assert pc["unmapped"] == "singleton"
    assert pc["mapping"]["fund"] == ["fund_stock", "fund_bond", "fund_mix"]


def test_report_display_and_sections():
    rep = _load()["report"]
    assert rep["sections"]["category"] is True
    assert rep["sections"]["per_item_attr"] is True
    assert rep["display"]["primary_map_k"] == [1, 3, 5, "all"]
    assert rep["display"]["guardrail_recall_k"] == [1, 2, 3, 4, 5]
    assert rep["diagnostics"]["sample_rows"] is None


def test_baseline_block_is_lookback_only():
    cfg = _load()["baseline"]
    assert cfg == {"lookback_months": 12}

"""Regression: parameters_evaluation.yaml carries the refactor's new keys."""

from pathlib import Path

import yaml


def _load():
    p = Path("conf/base/parameters_evaluation.yaml")
    return yaml.safe_load(p.read_text())["evaluation"]


def test_k_values_is_superset():
    assert _load()["k_values"] == [1, 2, 3, 4, 5, "all"]


def test_item_categories_block():
    cat_cfg = _load()["item_categories"]
    assert cat_cfg["enabled"] is True
    assert cat_cfg["unmapped"] == "singleton"
    assert cat_cfg["mapping"]["fund"] == ["fund_stock", "fund_bond", "fund_mix"]


def test_report_display_and_sections():
    rep = _load()["report"]
    # Exactly the switches the report reads (ADR-0019 decision 6, A34). Spelled
    # out here rather than read from EVALUATION_REPORT_SECTIONS, so this test
    # is an independent statement of the decision.
    assert rep["sections"] == {
        "data_stats": True, "ranking_metrics": True,
        "rank_distribution": True, "popularity_baseline": True,
    }
    assert rep["display"] == {"compare_k": [1, 3, 5, "all"]}
    # Diagnostics are aggregated in Spark now -> no row-sampling cap.
    assert "sample_rows" not in rep["diagnostics"]
    assert rep["diagnostics"]["include_distributions"] is True


def test_baseline_block_is_lookback_only():
    cfg = _load()["baseline"]
    assert cfg == {"lookback_months": 12}


def test_metric_block_and_no_diagnosis_block():
    ev = _load()
    assert ev["metric"] == {
        "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0,
    }
    # #532 removed the diagnoses; A61 refuses a conf that still has the block.
    assert "diagnosis" not in ev

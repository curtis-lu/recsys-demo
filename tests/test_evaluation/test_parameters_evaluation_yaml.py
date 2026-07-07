"""Regression: parameters_evaluation.yaml carries the refactor's new keys."""

from pathlib import Path

import yaml


def _load():
    p = Path("conf/base/parameters_evaluation.yaml")
    return yaml.safe_load(p.read_text())["evaluation"]


def test_k_values_is_superset():
    assert _load()["k_values"] == [1, 2, 3, 4, 5, "all"]


def test_product_categories_block():
    pc = _load()["product_categories"]
    assert pc["enabled"] is True
    assert pc["unmapped"] == "singleton"
    assert pc["mapping"]["fund"] == ["fund_stock", "fund_bond", "fund_mix"]


def test_report_display_and_sections():
    rep = _load()["report"]
    assert rep["sections"]["category"] is True
    assert rep["sections"]["per_item_attr"] is True
    assert rep["display"]["primary_map_k"] == [1, 3, 5, "all"]
    assert rep["display"]["guardrail_recall_k"] == [1, 2, 3, 4, 5]
    # Diagnostics are aggregated in Spark now -> no row-sampling cap.
    assert "sample_rows" not in rep["diagnostics"]
    assert rep["diagnostics"]["include_distributions"] is True


def test_baseline_block_is_lookback_only():
    cfg = _load()["baseline"]
    assert cfg == {"lookback_months": 12}


def test_metric_and_diagnosis_blocks():
    ev = _load()
    assert ev["metric"] == {
        "weight_alpha": 0.0, "k": None, "min_positives": 0, "shrinkage_k": 0,
    }
    diag = ev["diagnosis"]
    assert diag["sample"] == {
        "max_queries": 200000, "min_pos_queries_per_item": 50, "seed": 42,
    }
    assert diag["ci"] == {"enabled": True, "n_boot": 200}


def test_reconciliation_block():
    recon = _load()["diagnosis"]["reconciliation"]
    assert recon == {
        "enabled": True,
        "score_col": "score_uncalibrated",
        "explained_threshold": 0.3,
    }


def test_report_sections_include_reconciliation():
    assert _load()["report"]["sections"]["reconciliation"] is True

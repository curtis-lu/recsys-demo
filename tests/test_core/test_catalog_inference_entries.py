"""Catalog regression: inference output entries reflect the staging gate.

yaml.safe_load on catalog.yaml is an established pattern in this repo (the
${...} placeholders are plain string scalars). Pure-Python, no Spark.
"""

from pathlib import Path

import yaml


def _load_catalog():
    # tests/test_core/<this file> -> parents[2] == repo (worktree) root
    root = Path(__file__).resolve().parents[2]
    catalog_path = root / "conf" / "base" / "catalog.yaml"
    return yaml.safe_load(catalog_path.read_text())


def test_ranked_staging_entry_present():
    d = _load_catalog()
    assert "ranked_staging" in d
    assert d["ranked_staging"]["type"] == "HiveTableDataset"
    assert d["ranked_staging"]["table"] == "ranked_staging"


def test_validated_predictions_entry_removed():
    """validate 的 output 現在是 in-DAG MemoryDataset,不再是 Hive 表。"""
    d = _load_catalog()
    assert "validated_predictions" not in d


def test_ranked_predictions_still_declared():
    """production 輸出 + standalone evaluation 讀取入口必須保留宣告。"""
    d = _load_catalog()
    assert "ranked_predictions" in d
    assert d["ranked_predictions"]["table"] == "ranked_predictions"

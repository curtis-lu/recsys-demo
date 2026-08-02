from pathlib import Path

import yaml

CONF = Path(__file__).resolve().parents[3] / "conf" / "base"


def test_feature_table_enforces_its_primary_key():
    """``feature_table`` must actually enforce its declared primary key.

    ``OutputChecker`` runs the duplicate-key check only when BOTH
    ``primary_key`` and ``quality_checks.max_duplicate_key_ratio`` are present,
    so declaring the key alone enforces nothing — which is how ``feature_table``
    ended up with an unverified grain while ``sample_pool`` and ``label_table``
    were covered. (``SourceChecker`` is the sibling class for pre-ETL upstream
    freshness and does not touch duplicate keys.)

    Only the terminal table is checked, not the five that feed it: a duplicate
    key anywhere upstream fans out through the joins and still lands here, and
    ``feature_table`` is the one the dataset pipeline actually reads. The cost
    is one aggregate instead of six; what is given up is knowing *which*
    upstream table introduced the duplicate. See
    docs/adr/0006-data-quality-checks-belong-upstream.md.
    """
    cfg = yaml.safe_load((CONF / "parameters_feature_etl.yaml").read_text())
    tables = {t["name"]: t for t in cfg["feature_etl"]["tables"]}

    # Guard against the assertions below passing vacuously on a renamed table.
    assert "feature_table" in tables

    entry = tables["feature_table"]
    assert entry["primary_key"] == ["snap_date", "cust_id"]
    assert entry["quality_checks"]["max_duplicate_key_ratio"] == 0.0

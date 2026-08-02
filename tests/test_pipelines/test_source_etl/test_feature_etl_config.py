from pathlib import Path

import yaml

CONF = Path(__file__).resolve().parents[3] / "conf" / "base"


def test_feature_etl_tables_enforce_their_primary_key():
    """Every feature ETL output must actually enforce its declared primary key.

    ``SourceChecker`` runs the duplicate-key check only when BOTH
    ``primary_key`` and ``quality_checks.max_duplicate_key_ratio`` are present.
    Declaring the key alone enforces nothing, which is how ``feature_table``
    ended up with an unverified grain while ``sample_pool`` and ``label_table``
    were covered — see docs/adr/0006-data-quality-checks-belong-upstream.md.
    """
    cfg = yaml.safe_load((CONF / "parameters_feature_etl.yaml").read_text())
    tables = cfg["feature_etl"]["tables"]

    # Guard against the assertion below passing vacuously on an empty list.
    names = [t["name"] for t in tables]
    assert "feature_table" in names

    unenforced = [
        t["name"]
        for t in tables
        if not t.get("primary_key")
        or t.get("quality_checks", {}).get("max_duplicate_key_ratio") != 0.0
    ]
    assert unenforced == []

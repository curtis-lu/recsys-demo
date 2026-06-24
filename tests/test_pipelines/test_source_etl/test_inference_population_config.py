from pathlib import Path
import yaml

CONF = Path(__file__).resolve().parents[3] / "conf" / "base"


def test_inference_population_etl_config_shape():
    cfg = yaml.safe_load(
        (CONF / "parameters_inference_population_etl.yaml").read_text()
    )
    assert "inference_population_etl" in cfg
    tables = cfg["inference_population_etl"]["tables"]
    assert len(tables) == 1
    t = tables[0]
    assert t["name"] == "inference_population"
    assert t["sql_file"] == "inference_population/inference_population.sql"
    assert t["primary_key"] == ["snap_date", "cust_id"]
    assert t["quality_checks"]["max_duplicate_key_ratio"] == 0.0


def test_inference_population_catalog_entry():
    cat = yaml.safe_load((CONF / "catalog.yaml").read_text())
    entry = cat["inference_population"]
    assert entry["type"] == "HiveTableDataset"
    assert entry["table"] == "inference_population"
    assert entry["read_only"] is True

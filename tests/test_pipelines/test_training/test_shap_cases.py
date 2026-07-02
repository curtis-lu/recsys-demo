"""Tests for compute_quadrant_profiles (per-item×quadrant signed profile,純 python)."""
import numpy as np
import pandas as pd

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.diagnostics.shap_cases import compute_quadrant_profiles


def _trained_adapter(seed=1):
    rng = np.random.RandomState(seed)
    Xtr = rng.randn(400, 2)
    ytr = (Xtr[:, 0] > 0).astype(float)
    adapter = LightGBMAdapter()
    adapter.train(Xtr, ytr, None, None,
                  {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
                   "num_leaves": 4, "seed": 1, "num_iterations": 15, "early_stopping_rounds": 0})
    return adapter


def _pop_from_counts(counts, seed=0):
    rng = np.random.RandomState(seed)
    rows = []
    for (item, q), c in counts.items():
        for _ in range(c):
            rows.append((rng.randn(), rng.randn(), item, q))
    return pd.DataFrame(rows, columns=["f0", "f1", "prod_name", "quadrant"])


def _params(min_rows=10):
    return {"schema": {"item": "prod_name", "label": "label",
                       "time": "snap_date", "entity": ["cust_id"]},
            "diagnostics": {"shap": {"quadrant_enabled": True, "top_k": 2,
                                     "quadrant_min_rows": min_rows}}}


_PREP = {"feature_columns": ["f0", "f1"], "categorical_columns": [], "category_mappings": {}}


def test_quadrant_profiles_structure():
    adapter = _trained_adapter()
    pop = _pop_from_counts({(i, q): 15 for i in ("A", "B")
                            for q in ("TP", "FP", "FN", "TN")})
    out = compute_quadrant_profiles(adapter, pop, _PREP, _params())
    assert set(out) == {"A", "B"}
    for item in ("A", "B"):
        assert set(out[item]) == {"TP", "FP", "FN", "TN"}
        for q, cell in out[item].items():
            assert cell["n_sampled"] == 15
            assert cell["low_coverage"] is False           # 15 >= 10
            assert len(cell["top_features"]) == 2
            assert all({"feature", "mean_abs_shap", "mean_signed_shap"} <= set(r)
                       for r in cell["top_features"])


def test_quadrant_profiles_low_coverage_and_empty_cell():
    adapter = _trained_adapter()
    # A/TP=3(low)、A/FN=12、A/FP=12、A/TN=0(empty→缺席);B 各 12
    pop = _pop_from_counts({("A", "TP"): 3, ("A", "FN"): 12, ("A", "FP"): 12,
                            ("B", "TP"): 12, ("B", "FP"): 12, ("B", "FN"): 12, ("B", "TN"): 12})
    out = compute_quadrant_profiles(adapter, pop, _PREP, _params(min_rows=10))
    assert out["A"]["TP"]["low_coverage"] is True          # 3 < 10
    assert out["A"]["FN"]["low_coverage"] is False         # 12 >= 10
    assert "TN" not in out["A"]                            # 空格不出現


def test_quadrant_profiles_empty_or_disabled():
    adapter = _trained_adapter()
    assert compute_quadrant_profiles(adapter, None, _PREP, _params()) == {}
    empty = _pop_from_counts({})
    assert compute_quadrant_profiles(adapter, empty, _PREP, _params()) == {}
    pop = _pop_from_counts({("A", "TP"): 5})
    p = _params(); p["diagnostics"]["shap"]["quadrant_enabled"] = False
    assert compute_quadrant_profiles(adapter, pop, _PREP, p) == {}


# ---- P2b-2: compute_quadrant_cases ----

from recsys_tfb.pipelines.training.diagnostics.shap_cases import compute_quadrant_cases


def _case_rows(specs):
    """specs: list of (item, quadrant, role, cust, score, rank, label)。加 f0/f1 特徵。"""
    rng = np.random.RandomState(3)
    rows = []
    for (item, q, role, cust, score, rank, label) in specs:
        rows.append(("2024-01-31", cust, item, q, role, rank, float(score), int(label),
                     rng.randn(), rng.randn()))
    return pd.DataFrame(rows, columns=["snap_date", "cust_id", "prod_name", "quadrant",
                                       "role", "rank", "score", "label", "f0", "f1"])


def _cases_params():
    return {"schema": {"item": "prod_name", "label": "label",
                       "time": "snap_date", "entity": ["cust_id"]},
            "model_version": "testmv_cases",
            "diagnostics": {"shap": {"quadrant_enabled": True, "case_top_k": 2}}}


def test_quadrant_cases_manifest_complete_grid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    # A:TP 兩列(high/low 不同 cust)、TN 兩列;FP/FN 無列(空格)。
    rows = _case_rows([
        ("A", "TP", "high", "c1", 0.9, 1, 1), ("A", "TP", "low", "c2", 0.5, 1, 1),
        ("A", "TN", "high", "c3", 0.4, 2, 0), ("A", "TN", "low", "c4", 0.1, 2, 0)])
    out = compute_quadrant_cases(adapter, rows, _PREP, _cases_params())
    assert set(out) == {"A"}
    assert set(out["A"]) == {"TP", "FP", "FN", "TN"}          # 完整 4 象限
    assert out["A"]["TP"]["high"]["rendered"] is True
    assert out["A"]["TP"]["low"]["rendered"] is True
    assert out["A"]["FP"]["high"]["reason"] == "empty"        # 空格
    assert out["A"]["FP"]["low"]["reason"] == "empty"
    assert out["A"]["TP"]["high"]["cust_id"] == "c1"
    assert out["A"]["TP"]["high"]["png"] == "cases/A/TP_high.png"
    assert out["A"]["TP"]["high"]["score"] == 0.9
    # PNG 實際落地
    base = tmp_path / "data/models/testmv_cases/diagnostics/cases/A"
    assert (base / "TP_high.png").exists()
    assert (base / "TP_low.png").exists()
    assert not (base / "FP_high.png").exists()


def test_quadrant_cases_single_row_cell(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    # A/TP 單行格:high 與 low 同一 cust(c1)。
    rows = _case_rows([
        ("A", "TP", "high", "c1", 0.9, 1, 1), ("A", "TP", "low", "c1", 0.9, 1, 1)])
    out = compute_quadrant_cases(adapter, rows, _PREP, _cases_params())
    assert out["A"]["TP"]["high"]["rendered"] is True
    assert out["A"]["TP"]["low"]["rendered"] is False
    assert out["A"]["TP"]["low"]["reason"] == "single_row_same_as_high"
    base = tmp_path / "data/models/testmv_cases/diagnostics/cases/A"
    assert (base / "TP_high.png").exists()
    assert not (base / "TP_low.png").exists()               # 不產重複檔


def test_quadrant_cases_empty_or_disabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    adapter = _trained_adapter()
    assert compute_quadrant_cases(adapter, None, _PREP, _cases_params()) == {}
    empty = _case_rows([])
    assert compute_quadrant_cases(adapter, empty, _PREP, _cases_params()) == {}
    rows = _case_rows([("A", "TP", "high", "c1", 0.9, 1, 1),
                       ("A", "TP", "low", "c2", 0.5, 1, 1)])
    p = _cases_params(); p["diagnostics"]["shap"]["quadrant_enabled"] = False
    assert compute_quadrant_cases(adapter, rows, _PREP, p) == {}


# ---- Task 3: wiring (pipeline + catalog) ----


def test_pipeline_wires_quadrant_nodes():
    from recsys_tfb.pipelines.training.pipeline import create_pipeline
    pipe = create_pipeline()
    fns = {n.func.__name__ for n in pipe.nodes}
    assert "select_shap_population" in fns
    assert "compute_quadrant_profiles" in fns
    # log_experiment 依賴 quadrant_profiles(排序保證 per_quadrant.json 先寫)
    log_node = next(n for n in pipe.nodes if n.func.__name__ == "log_experiment")
    assert "quadrant_profiles" in log_node.inputs
    assert "compute_quadrant_cases" in fns
    assert "cases_manifest" in log_node.inputs
    sp = next(n for n in pipe.nodes if n.func.__name__ == "select_shap_population")
    assert sp.outputs == ["shap_population", "case_rows"]


def test_catalog_has_quadrant_profiles():
    from pathlib import Path

    import yaml

    # tests/test_pipelines/test_training/<this file> -> parents[3] == worktree root
    catalog_path = Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml"
    cat = yaml.safe_load(catalog_path.read_text())
    assert cat["quadrant_profiles"]["type"] == "JSONDataset"
    assert "per_quadrant.json" in cat["quadrant_profiles"]["filepath"]


def test_catalog_has_cases_manifest():
    from pathlib import Path

    import yaml

    catalog_path = Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml"
    cat = yaml.safe_load(catalog_path.read_text())
    assert cat["cases_manifest"]["type"] == "JSONDataset"
    assert "cases/cases_manifest.json" in cat["cases_manifest"]["filepath"]


def test_config_has_case_top_k():
    from pathlib import Path

    import yaml

    p = Path(__file__).resolve().parents[3] / "conf" / "base" / "parameters_training.yaml"
    cfg = yaml.safe_load(p.read_text())["diagnostics"]["shap"]
    assert cfg["case_top_k"] == 15

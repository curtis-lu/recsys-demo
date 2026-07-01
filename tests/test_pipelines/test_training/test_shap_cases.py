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


def test_catalog_has_quadrant_profiles():
    from pathlib import Path

    import yaml

    # tests/test_pipelines/test_training/<this file> -> parents[3] == worktree root
    catalog_path = Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml"
    cat = yaml.safe_load(catalog_path.read_text())
    assert cat["quadrant_profiles"]["type"] == "JSONDataset"
    assert "per_quadrant.json" in cat["quadrant_profiles"]["filepath"]

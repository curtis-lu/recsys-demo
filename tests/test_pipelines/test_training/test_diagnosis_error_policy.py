"""The training diagnostics' error policy, run through the real pipeline nodes.

ADR-0030 decision 4: a model that cannot answer a diagnosis (the fake
adapter from #482 has no trees and attributes nothing) skips it with a warning
and lands the "model cannot" shape — the run finishes and the evaluation
report names that as the reason. Anything else stops the run.

The nodes are taken from ``create_pipeline()`` and run by the ``Runner`` with
catalog entries of the production types, so the wiring (two outputs per
figure-drawing node, the figure datasets, log_experiment reading every
diagnosis) is what is exercised, not a hand-called function.
"""

import json
import logging

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.core.runner import Runner
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.pipelines.training.pipeline import create_pipeline
from tests.fake_adapter import FakeAdapter

DIAGNOSIS_NODES = {
    "compute_feature_importance", "compute_gain_ledger", "compute_shap_diagnostics",
    "compute_quadrant_profiles", "compute_quadrant_cases", "log_experiment",
}
FEATURES = ["f0", "f1"]


def _fake_model():
    rng = np.random.RandomState(0)
    X = rng.randn(60, len(FEATURES))
    model = FakeAdapter()
    model.train(model.build_train_data(X, (X[:, 0] > 0).astype(float),
                                       feature_names=FEATURES, categorical_features=[]),
                {}, num_iterations=5)
    return model


def _test_handle(tmp_path):
    rng = np.random.RandomState(1)
    pdf = pd.DataFrame({"f0": rng.randn(80), "f1": rng.randn(80),
                        "prod_name": ["A", "B"] * 40, "label": [0, 1] * 40})
    root = tmp_path / "test_cache"
    root.mkdir()
    pq.write_table(pa.Table.from_pandas(pdf), root / "part.parquet")
    (root / "_SUCCESS").touch()
    return ParquetHandle(path=str(root))


def _population():
    rows = [(0.1 * i, -0.1 * i, item, q, "2024-01-31", f"c{i}", "high", 1, 0.5, 1)
            for i, (item, q) in enumerate([("A", "TP"), ("B", "FN")])]
    return pd.DataFrame(rows, columns=["f0", "f1", "prod_name", "quadrant", "snap_date",
                                       "cust_id", "role", "rank", "score", "label"])


def _run(tmp_path, monkeypatch, model):
    monkeypatch.chdir(tmp_path)
    parameters = {
        "model_version": "mv_policy",
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label"}},
        "training": {},
        "diagnostics": {"shap": {"sample_rows": 40, "min_rows_per_item": 5}},
        # strict: log_experiment is best-effort by default and would swallow
        # a summary that chokes on the "model cannot" shape.
        "mlflow": {"tracking_uri": f"file:{tmp_path / 'mlruns'}", "strict": True},
    }
    diag = "data/models/mv_policy/diagnostics"
    catalog = DataCatalog({
        "feature_importance": {"type": "JSONDataset", "filepath": f"{diag}/feature_importance.json"},
        "gain_ledger": {"type": "JSONDataset", "filepath": f"{diag}/gain_ledger.json"},
        "shap_diagnostics": {"type": "JSONDataset", "filepath": f"{diag}/shap_diagnostics.json"},
        "quadrant_profiles": {"type": "JSONDataset", "filepath": f"{diag}/per_quadrant.json"},
        "cases_manifest": {"type": "JSONDataset", "filepath": f"{diag}/cases/cases_manifest.json"},
        "shap_summary_figures": {"type": "DiagnosticFiguresDataset", "filepath": f"{diag}/summary"},
        "case_figures": {"type": "DiagnosticFiguresDataset", "filepath": f"{diag}/cases"},
    })
    inputs = {
        "model": model,
        "parameters": parameters,
        "preprocessor": {"feature_columns": FEATURES, "categorical_columns": [],
                         "category_mappings": {"prod_name": ["A", "B"]}},
        "test_parquet_handle": _test_handle(tmp_path),
        "shap_population": _population(),
        "case_rows": _population(),
        "best_params": {}, "best_iteration": 5,
        "evaluation_results": {"overall_map": 0.5, "per_item_map_attr": {},
                               "n_queries": 1, "n_excluded_queries": 0},
        "feature_statistics": {},
    }
    for name, value in inputs.items():
        catalog.add(name, MemoryDataset(value))
    nodes = [n for n in create_pipeline().nodes if n.name in DIAGNOSIS_NODES]
    assert {n.name for n in nodes} == DIAGNOSIS_NODES
    Runner().run(Pipeline(nodes), catalog)
    return tmp_path / diag, parameters


def test_a_model_without_trees_finishes_the_run_and_says_why(tmp_path, monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        diag, parameters = _run(tmp_path, monkeypatch, _fake_model())

    for name in ("feature_importance.json", "gain_ledger.json", "shap_diagnostics.json",
                 "per_quadrant.json", "cases/cases_manifest.json"):
        landed = json.loads((diag / name).read_text())
        assert landed["enabled"] is True and landed["supported"] is False, name
    assert not list(diag.rglob("*.png"))
    assert sum("cannot" in r.getMessage() for r in caplog.records
               if r.levelno == logging.WARNING) == 5

    # The evaluation side reads the landed ledger and names the cause —
    # neither "evaluation ran alone" nor "switched off in training".
    from recsys_tfb.diagnosis.metric.model_capacity import compute

    report = compute(json.loads((diag / "gain_ledger.json").read_text()), None,
                     {"evaluation": {"diagnosis": {}}})
    assert report["available"] is False
    assert "模型做不到" in report["reason"]
    assert "tree" in report["reason"]          # the adapter's own words


def test_a_diagnosis_that_fails_for_another_reason_stops_the_run(tmp_path, monkeypatch):
    """Mutation target: the ``except UnsupportedCapability`` in
    compute_gain_ledger widened back to ``except Exception``."""
    model = _fake_model()

    def bug():
        raise KeyError("split_gain")

    monkeypatch.setattr(model, "tree_structure", bug)
    with pytest.raises(KeyError, match="split_gain"):
        _run(tmp_path, monkeypatch, model)

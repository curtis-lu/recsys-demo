"""End to end: a redraw recomputes nothing (ADR-0031 decision 1).

The one evaluation test that runs the pipeline whole, with the Runner, on
local Spark (#530's test decision: only for what no seam sees). What it sees
and the seams do not: across several nodes and a real catalog, a second run
of ``--only-node generate_report`` reads every result back from its landed
file, touches no Spark input, rewrites no result — and still refuses one a
changed setting made stale. The slice's shape alone is pinned cheaper in
``test_resume_contracts.py``; the CLI's choice to start no Spark for it in
``test_cli.py::TestARedrawStartsNoSpark``.
"""
import copy

import pytest

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.runner import Runner
from recsys_tfb.io.base import AbstractDataset
from recsys_tfb.io.json_dataset import JSONDataset
from recsys_tfb.io.text_dataset import TextDataset
from recsys_tfb.pipelines.evaluation import create_pipeline

#: Monitoring mode, every section that computes something of its own off: the
#: shortest main line that still runs Spark twice (row facts, metrics).
_PARAMS = {
    "model_version": "mv_e2e",
    "post_training": False,
    "schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
    "evaluation": {
        "snap_date": "20240331",
        "k_values": [1, "all"],
        "report": {
            "sections": {"data_stats": True, "ranking_metrics": True,
                         "rank_distribution": False,
                         "popularity_baseline": False,
                         "prediction_quality": False},
            "display": {"compare_k": [1]},
        },
    },
}

#: Read by no node of a redraw: the sources and the enriched table.
_SPARK_INPUTS = ("ranked_predictions", "label_table", "inference_population",
                 "enriched_eval_predictions")


class _Untouchable(AbstractDataset):
    """Present, but fails the test if anything reads or writes it."""

    def __init__(self, name):
        self._name = name

    def load(self):
        raise AssertionError(f"the redraw read {self._name}")

    def save(self, data):
        raise AssertionError(f"the redraw wrote {self._name}")

    def exists(self):
        return True


def _catalog(tmp_path, parameters, sources):
    catalog = DataCatalog()
    for name, value in sources.items():
        catalog.add(name, value)
    catalog.add("parameters", MemoryDataset(parameters))
    catalog.add("preprocessor_on_disk",
                JSONDataset(str(tmp_path / "none.json"), optional=True))
    for name in ("evaluation_segment_columns", "evaluation_item_categories",
                 "evaluation_row_facts", "evaluation_metrics",
                 "evaluation_display_ks"):
        catalog.add(name, JSONDataset(str(tmp_path / f"{name}.json")))
    catalog.add("evaluation_report", TextDataset(str(tmp_path / "report.html")))
    return catalog


def _sources(spark):
    rows = [("20240331", "c1", "A", 0.9, 1), ("20240331", "c1", "B", 0.1, 2),
            ("20240331", "c2", "A", 0.2, 2), ("20240331", "c2", "B", 0.8, 1)]
    return {
        "ranked_predictions": MemoryDataset(spark.createDataFrame(
            rows, ["snap_date", "cust_id", "prod_name", "score", "rank"])),
        "label_table": MemoryDataset(spark.createDataFrame(
            [("20240331", "c1", "A", 1), ("20240331", "c2", "B", 1)],
            ["snap_date", "cust_id", "prod_name", "label"])),
        "inference_population": MemoryDataset(spark.createDataFrame(
            [("20240331", "c1"), ("20240331", "c2")],
            ["snap_date", "cust_id"])),
        "enriched_eval_predictions": MemoryDataset(),
    }


def _redraw(tmp_path, parameters):
    """``--only-node generate_report`` against what the first run landed,
    with every Spark input untouchable. Returns the nodes the slice ran."""
    pipeline = create_pipeline(parameters=parameters)
    catalog = _catalog(tmp_path, parameters,
                       {name: _Untouchable(name) for name in _SPARK_INPUTS})
    sliced, _plan = pipeline.slice_only("generate_report", catalog.exists)
    Runner().run(sliced, catalog)
    return [n.name for n in sliced.nodes]


def test_a_redraw_reads_the_landed_results_and_recomputes_nothing(
        spark, tmp_path):
    Runner().run(create_pipeline(parameters=_PARAMS),
                 _catalog(tmp_path, _PARAMS, _sources(spark)))
    landed = {path.name: (path.stat().st_mtime_ns, path.read_text())
              for path in tmp_path.glob("evaluation_*.json")}
    assert "evaluation_metrics.json" in landed
    (tmp_path / "report.html").unlink()

    # A drawn key changed: the section goes, nothing is recomputed.
    drawn = copy.deepcopy(_PARAMS)
    drawn["evaluation"]["report"]["sections"]["data_stats"] = False
    assert _redraw(tmp_path, drawn) == ["generate_report"]
    assert (tmp_path / "report.html").exists()
    assert {path.name: (path.stat().st_mtime_ns, path.read_text())
            for path in tmp_path.glob("evaluation_*.json")} == landed

    # A setting only the metrics read changed: the redraw refuses the
    # metrics alone and names their node, not the row facts'.
    applied = copy.deepcopy(_PARAMS)
    applied["evaluation"]["k_values"] = [1, 2, "all"]
    with pytest.raises(ValueError) as exc:
        _redraw(tmp_path, applied)
    msg = str(exc.value)
    assert "evaluation_metrics (written by compute_metrics) is stale" in msg
    assert "evaluation_row_facts (written by" not in msg
    assert "--from-node compute_metrics," in msg

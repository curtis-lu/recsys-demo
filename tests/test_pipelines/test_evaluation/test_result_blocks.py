"""The evaluation pipeline's landed results hold the blocks they declare
(ADR-0031 principle 2, supplement 2).

Each node that lands a JSON result declares a ``Produces`` above itself in
``nodes.py``. A: the declarations cover every JSON result the pipeline lands,
in every mode. B: a monitoring run with every section on and a compare source
lands, for each result, exactly the declared blocks less a stated set the run
cannot reach. C: each block B cannot reach is written by a minimal node call.
D: the join of wiring, sections and declarations (``result_readers_table``).

B runs the whole pipeline with the Runner, so it is an end-to-end test in
#530's sense. The seams cannot see it: each node's input must be what the node
upstream really computed, and wiring node by node would redo the Runner.
"""
import copy
from pathlib import Path

import pytest
import yaml

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.consistency import EVALUATION_REPORT_SECTIONS
from recsys_tfb.core.runner import Runner
from recsys_tfb.pipelines.evaluation import create_pipeline, nodes
from recsys_tfb.pipelines.evaluation.pipeline import result_readers_table
from recsys_tfb.pipelines.evaluation.steps.result_blocks import declared_results

DECLARED = declared_results(nodes)

_CATALOG = yaml.safe_load(
    (Path(__file__).parents[3] / "conf" / "base" / "catalog.yaml").read_text())
_JSON_ENTRIES = {name for name, entry in _CATALOG.items()
                 if isinstance(entry, dict) and entry.get("type") == "JSONDataset"}

_ALL_SECTIONS = {name: True for name in EVALUATION_REPORT_SECTIONS}
_SOURCE = {"kind": "model_version", "model_version": "other"}


def _all_on_params():
    return {"evaluation": {"report": {"sections": dict(_ALL_SECTIONS)}}}


def _landed_names(**mode):
    pipeline = create_pipeline(parameters=_all_on_params(), **mode)
    return {out for node in pipeline.nodes for out in node.outputs}


# ---------------------------------------------------------------- A


def test_the_declarations_cover_every_json_result_of_every_mode():
    landed = set()
    for mode in ({}, {"post_training": True},
                 {"compare_source": _SOURCE},
                 {"compare_source": _SOURCE, "compare_only": True},
                 {"post_training": True, "baseline_rate": True,
                  "compare_source": _SOURCE}):
        landed |= _landed_names(**mode) & _JSON_ENTRIES
    assert set(DECLARED) == landed, (
        f"declared but never landed: {sorted(set(DECLARED) - landed)}; "
        f"landed but not declared: {sorted(landed - set(DECLARED))}")


def test_every_block_says_what_it_is_and_names_a_grain_once_per_result():
    for result, produces in DECLARED.items():
        names = [block.name for block in produces.blocks]
        assert len(names) == len(set(names)), f"{result}: repeated {names}"
        for block in produces.blocks:
            assert block.definition.strip(), f"{result}.{block.name}"
            assert block.grain.strip(), f"{result}.{block.name}"
            assert block.when is None or block.when.strip()


# ---------------------------------------------------------------- B

_PARAMS = {
    "model_version": "mv_blocks",
    "post_training": False,
    "schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"},
        "categorical_values": {"prod_name": ["A", "B", "C"]}},
    "evaluation": {
        "snap_date": "2024-03-31",
        "k_values": [1, 2, "all"],
        "segment_columns": ["seg"],
        "baseline": {"lookback_months": 12},
        "compare": _SOURCE,
        "item_categories": {"enabled": True, "unmapped": "singleton",
                            "mapping": {"ab": ["A", "B"]}},
        "report": {"sections": dict(_ALL_SECTIONS),
                   "display": {"compare_k": [1]}},
    },
}

#: Blocks a monitoring run with this setting cannot write, and why: the
#: first is rate-mode only, the second needs an optional schema role, the
#: third needs the item list counted from the data. Block C writes each.
_UNREACHED = {
    "baseline_metrics": {"popularity_rate", "n_tied_rows", "tied_row_share"},
    "evaluation_metrics": {"n_tied_rows", "tied_row_share"},
    "compare_metrics_a": {"n_tied_rows", "tied_row_share"},
    "compare_metrics_b": {"n_tied_rows", "tied_row_share"},
    "evaluation_item_categories": {"known_items"},
}


def _frames(spark):
    items = ["A", "B", "C"]
    scores = {"c1": [0.9, 0.5, 0.1], "c2": [0.2, 0.8, 0.3],
              "c3": [0.4, 0.3, 0.7]}
    ranked, compared = [], []
    for cust, row in scores.items():
        order = sorted(row, reverse=True)
        for item, score in zip(items, row):
            ranked.append(("2024-03-31", cust, item, score,
                           order.index(score) + 1))
            compared.append(("2024-03-31", cust, item, 1.0 - score))
    labels = [("2024-03-31", "c1", "A", 1), ("2024-03-31", "c2", "B", 1),
              ("2024-03-31", "c3", "C", 1), ("2024-03-31", "c3", "A", 0),
              ("2024-01-31", "h1", "A", 1), ("2024-01-31", "h2", "B", 1)]
    return {
        "ranked_predictions": spark.createDataFrame(
            ranked, ["snap_date", "cust_id", "prod_name", "score", "rank"]),
        "compare_predictions_raw": spark.createDataFrame(
            compared, ["snap_date", "cust_id", "prod_name", "score"]),
        "label_table": spark.createDataFrame(
            labels, ["snap_date", "cust_id", "prod_name", "label"]),
        "inference_population": spark.createDataFrame(
            [("2024-03-31", "c1", "x"), ("2024-03-31", "c2", "y"),
             ("2024-03-31", "c3", "x")], ["snap_date", "cust_id", "seg"]),
    }


def _run_monitoring_with_compare(spark):
    pipeline = create_pipeline(parameters=_PARAMS, compare_source=_SOURCE)
    # load_compare_predictions reads Hive: the frame goes in the catalog. The
    # two report nodes draw HTML and refuse a stale result first; what they
    # read is checked here, block by block.
    dropped = {"load_compare_predictions", "generate_report",
               "generate_comparison_report"}
    pipeline = type(pipeline)(
        [n for n in pipeline.nodes if n.name not in dropped])
    catalog = DataCatalog()
    for name, frame in _frames(spark).items():
        catalog.add(name, MemoryDataset(frame))
    catalog.add("parameters", MemoryDataset(copy.deepcopy(_PARAMS)))
    catalog.add("preprocessor_on_disk", MemoryDataset(None))
    for node in pipeline.nodes:
        for out in node.outputs:
            if not catalog.exists(out):
                catalog.add(out, MemoryDataset())
    Runner().run(pipeline, catalog)
    return catalog


def _blocks_missing_or_extra(result, landed, unreached):
    """Empty when ``landed`` holds exactly the declared blocks less
    ``unreached``; else a message naming the result and the keys."""
    expected = {b.name for b in DECLARED[result].blocks} - set(unreached)
    got = set(landed)
    if got == expected:
        return ""
    return (f"{result}: not written but declared {sorted(expected - got)}; "
            f"written but not declared {sorted(got - expected)}")


def test_what_cannot_be_reached_is_declared_conditional():
    for result, names in _UNREACHED.items():
        by_name = {b.name: b for b in DECLARED[result].blocks}
        for name in names:
            assert by_name[name].when, f"{result}.{name} has no `when`"


def test_a_run_lands_exactly_the_declared_blocks(spark):
    catalog = _run_monitoring_with_compare(spark)
    problems = [
        message for result in DECLARED
        if (message := _blocks_missing_or_extra(
            result, catalog.load(result), _UNREACHED.get(result, ())))]
    assert not problems, "\n".join(problems)


# ---------------------------------------------------------------- C


def _assert_blocks(result, landed, unreached):
    message = _blocks_missing_or_extra(result, landed, unreached)
    assert not message, message


def test_rate_mode_writes_popularity_rate(spark):
    from tests.test_pipelines.test_evaluation.test_popularity_rate import (
        _history,
        _plan,
        _rate_params,
        _rate_run,
    )

    params = _rate_params()
    pool, labels = _history(spark)
    result = _rate_run(spark, params, pool, labels, _plan(["2024-06-30"]))
    _assert_blocks("baseline_metrics", result,
                   {"n_tied_rows", "tied_row_share", "category"})


def test_a_declared_event_role_writes_the_tie_counts(spark):
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        stamp_partition_fingerprint,
    )
    from tests.test_evaluation.test_metrics_spark_overview import _event_params
    from tests.test_pipelines.test_evaluation.test_nodes import _no_segments

    params = _event_params()
    params["evaluation"] = {"snap_date": "2024-03-31", "k_values": [1, 2],
                            "baseline": {"lookback_months": 12}}
    # Two score ties in c1's group (the first two rows); the date is ISO
    # because the baseline reads its lookback window off it.
    frame = stamp_partition_fingerprint(spark.createDataFrame(
        [("2024-03-31", "c1", "A", "i1", 0.9, 1),
         ("2024-03-31", "c1", "A", "i2", 0.9, 0),
         ("2024-03-31", "c1", "A", "i3", 0.4, 0),
         ("2024-03-31", "c1", "B", "i4", 0.1, 0),
         ("2024-03-31", "c2", "A", "i5", 0.2, 0)],
        ["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"]),
        params, [])
    labels = spark.createDataFrame(
        [("2023-12-31", "h1", "A", 1), ("2023-12-31", "h2", "B", 1)],
        ["snap_date", "cust_id", "prod_name", "label"])
    segments = _no_segments(params)
    facts = nodes.compute_row_facts(frame, segments, params)
    metrics = nodes.compute_metrics(frame, facts, segments, params)
    baseline = nodes.compute_baseline_metrics(
        frame, facts, labels, segments, params)
    _a_facts, compare_a, _b_facts, compare_b = nodes.compute_compare_metrics(
        frame, frame, segments, params)
    _assert_blocks("evaluation_row_facts", facts, {"category"})
    _assert_blocks("evaluation_metrics", metrics, {"category"})
    assert metrics["n_tied_rows"] == 2
    _assert_blocks("baseline_metrics", baseline, {"popularity_rate", "category"})
    _assert_blocks("compare_metrics_a", compare_a, {"category"})
    _assert_blocks("compare_metrics_b", compare_b, {"category"})


def test_a_counted_item_list_writes_known_items(spark):
    from tests.test_pipelines.test_evaluation.test_item_categories import (
        _counted,
        _params,
        _prepare_counted,
    )

    params = _counted(_params(column=None, mapping={"ab": ["A", "B"]}))
    _frame, _segments, categories = _prepare_counted(spark, params)
    _assert_blocks("evaluation_item_categories", categories, ())


# ---------------------------------------------------------------- D


@pytest.fixture(scope="module")
def table():
    return result_readers_table()


def test_the_table_joins_wiring_sections_and_declarations(table):
    facts = table["evaluation_row_facts"]
    assert facts["producers"] == ["compute_row_facts"]
    assert "compute_metrics" in facts["reader_nodes"]
    assert ("evaluation_report", "data_stats", "data_stats") in \
        facts["reader_sections"]


def test_the_comparison_chain_is_listed_with_its_readers(table):
    comparison = table["source_comparison"]
    assert comparison["producers"] == ["compare_to_source"]
    assert "resolve_comparison_display_ks" in comparison["reader_nodes"]
    assert any(report == "evaluation_comparison_report"
               for report, _section, _switch in comparison["reader_sections"])


def test_every_declared_result_has_a_producer(table):
    assert [r for r, e in table.items() if not e["producers"]] == []

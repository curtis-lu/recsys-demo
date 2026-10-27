"""``steps.config_fingerprint``: which settings make a landed JSON stale.

The expected lists below are written out from ADR-0031 principle 5 and the
reads of each node, not derived from ``APPLIED_SETTINGS``: a test whose
expectation is read from the module under test agrees with whatever the
module says.
"""
import ast
import copy
import datetime
import json
from pathlib import Path

import pytest

from recsys_tfb.core.consistency import DATASET_TEST_RATIO_KEY
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    APPLIED_SETTINGS,
    COMPARISON_CHAIN,
    PARTITION_FINGERPRINT_KEYS,
    LoadedArtifact,
    fingerprint,
    partition_fingerprint,
    recorded_partition_fingerprint,
    require_computed_with_current_config,
    rerun_node,
)

_METRIC_READS = {
    "schema", "evaluation.snap_date", "evaluation.item_categories",
    "evaluation.query_filter", "evaluation.k_values", "evaluation.metric",
}

#: What each fingerprinted step reads (ADR-0031 principle 5). ``schema``
#: whole in every step that resolves a column role.
SPEC_APPLIED = {
    # post_training through its wiring: the predictions and population table.
    "prepare_eval_data": {
        "schema", "post_training", "evaluation.snap_date", "evaluation.segment_columns",
        "evaluation.segment_sources", "evaluation.item_categories.column",
    },
    "compute_row_facts": {
        "schema", "evaluation.snap_date", "evaluation.item_categories",
        "evaluation.query_filter",
    },
    "compute_report_aggregates": {
        "schema", "evaluation.snap_date", "evaluation.report.diagnostics",
    },
    "restrict_to_common": {"schema", "evaluation.snap_date"},
    "compute_metrics": _METRIC_READS,
    "compute_baseline_metrics": _METRIC_READS | {"evaluation.baseline"},
    "compute_compare_metrics": _METRIC_READS,
    "compare_to_baseline": set(),
    "compare_to_source": set(),
    # The switch through its wiring: off, no baseline comparison to cut.
    "resolve_display_ks": {
        "evaluation.k_values", "evaluation.report.display.compare_k",
        "evaluation.report.sections.popularity_baseline",
    },
    "resolve_comparison_display_ks": {
        "evaluation.k_values", "evaluation.report.display.compare_k",
    },
    "compute_prediction_quality": {
        # The evaluated model's r, as the CLI hands it on; k_values (the
        # per-item precision@K grid) and min_positives (every macro over
        # items).
        "schema", "post_training", DATASET_TEST_RATIO_KEY,
        "evaluation.snap_date", "evaluation.prediction_quality",
        "evaluation.k_values", "evaluation.metric.min_positives",
    },
}


def _params() -> dict:
    return {
        "model_version": "mv_test",
        "snap_date": "20260131",
        "post_training": False,
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name", "label": "label",
                        "score": "score", "rank": "rank"},
            "categorical_values": {"prod_name": ["p1", "p2"]},
        },
        "dataset": {"sample_ratio": 1.0},
        "evaluation": {
            "snap_date": "2026-01-31",
            "k_values": [5, "all"],
            "segment_columns": ["segment_a"],
            "segment_sources": {},
            "item_categories": {"enabled": False},
            "baseline": {"lookback_months": 12},
            "prediction_quality": {"n_bins": 1000, "n_display_bins": 10,
                                   "top_n": 30},
            "metric": {"weight_alpha": 0.0,
                       "min_positives": 0, "shrinkage_k": 0.0},
            "query_filter": {"drop_all_positive_groups": False},
            "report": {
                "sections": {"data_stats": True, "ranking_metrics": True,
                             "popularity_baseline": True,
                             "rank_distribution": True},
                "display": {"compare_k": [1, 3, 5, "all"]},
                "diagnostics": {"include_distributions": True},
            },
        },
    }


def _set(params: dict, path: str, value) -> dict:
    node = params
    *parents, leaf = path.split(".")
    for seg in parents:
        node = node.setdefault(seg, {})
    node[leaf] = value
    return params


def _delete(params: dict, path: str) -> dict:
    node = params
    *parents, leaf = path.split(".")
    for seg in parents:
        node = node[seg]
    node.pop(leaf, None)
    return params


def _changed(params: dict, path: str) -> dict:
    return _set(copy.deepcopy(params), path, {"changed": "value"})


def _landed(params: dict, node: str, *inputs) -> dict:
    """A result ``node`` lands from ``inputs``, as read back from disk."""
    return json.loads(json.dumps(
        {"config_fingerprint": fingerprint(params, node, inputs)}))


def _main_report(params: dict) -> list[LoadedArtifact]:
    """Every result the main report reads, landed under ``params`` the way
    the pipeline wires them, plus the prepared segment list."""
    seg = _landed(params, "prepare_eval_data")
    facts = _landed(params, "compute_row_facts", seg, seg)
    metrics = _landed(params, "compute_metrics", seg, seg, facts)
    baseline = _landed(params, "compute_baseline_metrics", seg, seg, facts)
    comparison = _landed(params, "compare_to_baseline", metrics, baseline)
    landed = {
        ("evaluation_segment_columns", "prepare_eval_data"): seg,
        ("evaluation_row_facts", "compute_row_facts"): facts,
        ("evaluation_metrics", "compute_metrics"): metrics,
        ("baseline_metrics", "compute_baseline_metrics"): baseline,
        ("baseline_comparison", "compare_to_baseline"): comparison,
        ("evaluation_display_ks", "resolve_display_ks"):
            _landed(params, "resolve_display_ks", metrics, comparison),
        ("evaluation_report_aggregates", "compute_report_aggregates"):
            _landed(params, "compute_report_aggregates", seg),
    }
    return [LoadedArtifact(catalog_name=name, payload=payload,
                           produced_by=producer)
            for (name, producer), payload in landed.items()]


def _stale(artifacts, params, **kwargs) -> set[str]:
    stale = set()
    for artifact in artifacts:
        try:
            require_computed_with_current_config([artifact], params, **kwargs)
        except ValueError:
            stale.add(artifact.catalog_name)
    return stale


def _refusal(artifacts, params, **kwargs) -> str:
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(artifacts, params, **kwargs)
    return str(exc.value)


# ------------------------------------------------------------- declarations


def test_each_step_declares_what_it_reads():
    assert {node: set(paths) for node, paths in APPLIED_SETTINGS.items()} \
        == SPEC_APPLIED


def test_a_result_records_only_what_its_step_applied():
    params = _params()
    recorded = fingerprint(params, "compute_report_aggregates")
    assert recorded == {
        "node": "compute_report_aggregates",
        "applied": {
            "schema": params["schema"],
            "evaluation.snap_date": "2026-01-31",
            "evaluation.report.diagnostics": {"include_distributions": True},
        },
        "inputs": {},
    }


def test_a_step_that_declares_nothing_cannot_land_a_fingerprint():
    with pytest.raises(KeyError):
        fingerprint(_params(), "a_new_step")


def test_a_result_records_what_each_of_its_inputs_applied():
    params = _params()
    seg = _landed(params, "prepare_eval_data")
    facts = _landed(params, "compute_row_facts", seg)
    recorded = fingerprint(params, "compute_metrics", (seg, facts, None))
    assert set(recorded["inputs"]) == {"prepare_eval_data", "compute_row_facts"}
    assert recorded["inputs"]["compute_row_facts"] == \
        facts["config_fingerprint"]["applied"]
    # Inherited further down: the comparison holds the metrics' inputs too.
    comparison = fingerprint(
        params, "compare_to_baseline",
        (_landed(params, "compute_metrics", seg, facts),))
    assert set(comparison["inputs"]) == {
        "prepare_eval_data", "compute_row_facts", "compute_metrics"}


def test_an_input_without_a_fingerprint_contributes_nothing():
    params = _params()
    assert fingerprint(params, "compute_metrics",
                       ({"joined": []}, None))["inputs"] == {}


# --------------------------------------------- one changed key, which results


#: Which of the main report's results a change to one key makes stale, and
#: where the re-run is sent. ADR-0031 principle 5: only the results of the
#: steps that read it, and of every step downstream of them.
STALE_AFTER = {
    "evaluation.report.diagnostics.include_distributions": (
        {"evaluation_report_aggregates"}, "compute_report_aggregates"),
    "evaluation.report.display.compare_k": (
        {"evaluation_display_ks"}, "resolve_display_ks"),
    "evaluation.report.sections.popularity_baseline": (
        {"evaluation_display_ks"}, "resolve_display_ks"),
    "evaluation.baseline.lookback_months": (
        {"baseline_metrics", "baseline_comparison", "evaluation_display_ks"},
        "compute_baseline_metrics"),
    "evaluation.k_values": (
        {"evaluation_metrics", "baseline_metrics", "baseline_comparison",
         "evaluation_display_ks"}, "compute_metrics"),
    "evaluation.metric.min_positives": (
        {"evaluation_metrics", "baseline_metrics", "baseline_comparison",
         "evaluation_display_ks"}, "compute_metrics"),
    "evaluation.query_filter.drop_all_positive_groups": (
        {"evaluation_row_facts", "evaluation_metrics", "baseline_metrics",
         "baseline_comparison", "evaluation_display_ks"}, "compute_row_facts"),
    "evaluation.item_categories.mapping": (
        {"evaluation_row_facts", "evaluation_metrics", "baseline_metrics",
         "baseline_comparison", "evaluation_display_ks"}, "compute_row_facts"),
}
_EVERY_RESULT = {artifact.catalog_name for artifact in _main_report(_params())}
# The category column too: the two partition readers inherit the prepared
# record whole. The partition rows do not depend on it, but the re-run starts
# at prepare_eval_data for it anyway, so the cost is nothing.
STALE_AFTER.update({
    path: (_EVERY_RESULT, "prepare_eval_data")
    for path in ("post_training", "evaluation.segment_columns",
                 "evaluation.segment_sources", "evaluation.snap_date",
                 "evaluation.item_categories.column",
                 # The column roles decide the query groups and the columns
                 # the partition rows hold; the item list decides the
                 # category pass and the item count.
                 "schema.columns.rank", "schema.categorical_values")
})
#: Drawn keys: no result goes stale.
DRAWN = ["evaluation.report.sections.data_stats",
         "evaluation.report.sections.ranking_metrics",
         "evaluation.report.sections.rank_distribution",
         "evaluation.compare"]


@pytest.mark.parametrize("path", sorted(STALE_AFTER))
def test_a_changed_key_makes_only_its_readers_results_stale(path):
    old = _params()
    expected, node = STALE_AFTER[path]
    new = _changed(old, path)
    artifacts = _main_report(old)
    assert _stale(artifacts, new) == expected
    assert f"--from-node {node}," in _refusal(artifacts, new)


#: The --prediction-quality mode's run order: its page is given it.
_PREDICTION_QUALITY_RUN = ["compute_prediction_quality",
                           "generate_prediction_quality_report"]


def _prediction_quality_page(params: dict) -> list[LoadedArtifact]:
    """What the mode's page reads, landed under ``params`` off the standard
    run's segment list."""
    seg = _landed(params, "prepare_eval_data")
    return [LoadedArtifact(
        catalog_name="prediction_quality_metrics",
        payload=_landed(params, "compute_prediction_quality", seg),
        produced_by="compute_prediction_quality")]


@pytest.mark.parametrize("path", [
    "evaluation.prediction_quality.n_bins",
    # With post_training, the evaluated model's r decides the row weights.
    DATASET_TEST_RATIO_KEY,
    "post_training", "evaluation.snap_date", "schema.columns.rank",
    "evaluation.k_values", "evaluation.metric.min_positives"])
def test_a_key_the_mode_applied_makes_its_result_stale(path):
    old = _params()
    artifacts = _prediction_quality_page(old)
    assert _stale(artifacts, _changed(old, path),
                  run_nodes=_PREDICTION_QUALITY_RUN) == {
        "prediction_quality_metrics"}
    assert "--from-node compute_prediction_quality," in _refusal(
        artifacts, _changed(old, path), run_nodes=_PREDICTION_QUALITY_RUN)


def test_the_standard_run_s_record_is_not_compared_in_the_mode():
    """prepare_eval_data is no node of the mode: no re-run of it could make
    the record current, and the partition check stands in for it."""
    old = _params()
    require_computed_with_current_config(
        _prediction_quality_page(old),
        _changed(old, "evaluation.segment_columns"),
        run_nodes=_PREDICTION_QUALITY_RUN)


@pytest.mark.parametrize("path", DRAWN)
def test_a_drawn_key_makes_nothing_stale(path):
    old = _params()
    require_computed_with_current_config(_main_report(old), _changed(old, path))


@pytest.mark.parametrize("path", ["evaluation.segment_sources",
                                  "evaluation.item_categories.column"])
def test_a_key_set_since_is_a_change(path):
    """Absent keys are not recorded, so the comparison walks the declared
    paths too: otherwise setting one would compare nothing."""
    absent = _delete(_params(), path)
    present = _set(copy.deepcopy(absent), path, "x")
    artifacts = _main_report(absent)
    assert _stale(artifacts, present) == _EVERY_RESULT
    msg = _refusal(artifacts, present)
    assert f"{path}: <absent> -> 'x'" in msg
    assert "--from-node prepare_eval_data," in msg


def test_an_empty_run_order_is_refused_not_skipped():
    """Left unfilled, the run order would skip every inherited record and
    pass a stale input."""
    old = _params()
    with pytest.raises(RuntimeError, match="empty run order"):
        require_computed_with_current_config(
            _main_report(old), _changed(old, "evaluation.k_values"),
            run_nodes=[])


# ------------------------------------------- the two parts, reported apart


def test_the_two_parts_are_reported_apart():
    old = _params()
    new = _set(copy.deepcopy(old), "evaluation.k_values", [1, 5])
    by_name = {a.catalog_name: a for a in _main_report(old)}

    own = _refusal([by_name["evaluation_metrics"]], new)
    assert "settings compute_metrics applied:\n" \
           "        evaluation.k_values: [5, 'all'] -> [1, 5]" in own
    assert "its input from" not in own

    inherited = _refusal([by_name["baseline_comparison"]], new)
    assert "settings compare_to_baseline applied" not in inherited
    assert "settings its input from compute_metrics was computed with:\n" \
           "        evaluation.k_values: [5, 'all'] -> [1, 5]" in inherited
    assert "settings its input from compute_baseline_metrics was computed " \
           "with:" in inherited


def test_collect_all_lists_every_stale_result_and_advises_one_re_run():
    old = _params()
    new = _set(copy.deepcopy(old),
               "evaluation.report.diagnostics.include_distributions", False)
    new = _set(new, "evaluation.k_values", [1, 5])
    msg = _refusal(_main_report(old), new)
    for name in ("evaluation_report_aggregates", "evaluation_metrics",
                 "baseline_comparison"):
        assert name in msg
    # Both moved; compute_report_aggregates sorts before compute_metrics.
    assert msg.count("--from-node") == 1
    assert "--from-node compute_report_aggregates," in msg


def test_the_message_names_where_the_declarations_live():
    """#368 item 1: the message names the module and table that declare
    what each step applies."""
    old = _params()
    msg = _refusal(_main_report(old), _changed(old, "evaluation.k_values"))
    assert "pipelines/evaluation/steps/config_fingerprint.py" in msg
    assert "config_fingerprint.py (APPLIED_SETTINGS)" in msg


def test_a_leaf_under_two_declared_paths_is_listed_once_per_part():
    old = _set(_params(), "evaluation.item_categories",
               {"enabled": True, "column": "family"})
    new = _set(copy.deepcopy(old), "evaluation.item_categories.column", "tier")
    metrics = {a.catalog_name: a for a in _main_report(old)}["evaluation_metrics"]
    msg = _refusal([metrics], new)
    # Once per part: what compute_metrics applied (its block), and each input
    # that applied it (prepare_eval_data the column, compute_row_facts the
    # block) — never twice within one, though both paths reach the leaf.
    assert msg.count(
        "evaluation.item_categories.column: 'family' -> 'tier'") == 3, msg
    assert "--from-node prepare_eval_data," in msg


# ---------------------------------------------------------- without a print


@pytest.mark.parametrize("payload", [
    {"enabled": False}, None, {"config_fingerprint": "x"},
    # A fingerprint in another shape (sha256 + values).
    {"config_fingerprint": {"sha256": "0" * 64, "values": {}}},
])
def test_a_result_without_a_fingerprint_is_stale(payload):
    msg = _refusal([LoadedArtifact(
        catalog_name="evaluation_report_aggregates", payload=payload,
        produced_by="compute_report_aggregates")], _params())
    assert "evaluation_report_aggregates" in msg
    assert "has no config_fingerprint in the shape this version writes" in msg
    assert "--from-node compute_report_aggregates," in msg


def test_several_without_a_fingerprint_advise_the_earliest_producer():
    msg = _refusal([
        LoadedArtifact(catalog_name="evaluation_display_ks", payload={},
                       produced_by="resolve_display_ks"),
        LoadedArtifact(catalog_name="evaluation_metrics", payload={},
                       produced_by="compute_metrics"),
    ], _params())
    assert "--from-node compute_metrics," in msg


def test_no_node_of_the_run_applies_it_advises_the_full_pipeline():
    msg = _refusal([LoadedArtifact(
        catalog_name="evaluation_metrics", payload={},
        produced_by="compute_metrics")], _params(),
        run_nodes=COMPARISON_CHAIN)
    assert "--from-node" not in msg
    assert "full pipeline" in msg


# --------------------------------------------------------------- the advice


#: A --compare run with every main section off: no main K cut in it.
_COMPARE_WITHOUT_THE_MAIN_CUT = (
    "prepare_eval_data", "load_compare_predictions", "compute_row_facts",
    "restrict_to_common", "compute_metrics", "compute_compare_metrics",
    "compare_to_source", "resolve_comparison_display_ks",
    "generate_report", "generate_comparison_report",
)


@pytest.mark.parametrize("changed, run_nodes, node", [
    (["evaluation.item_categories.column"], None, "prepare_eval_data"),
    (["evaluation.item_categories"], None, "prepare_eval_data"),
    (["evaluation.item_categories.mapping.a"], None, "compute_row_facts"),
    (["evaluation.k_values"], None, "compute_metrics"),
    (["evaluation.k_values"], COMPARISON_CHAIN, "compute_compare_metrics"),
    (["schema.columns.rank"], None, "prepare_eval_data"),
    # The comparison chain's first step resolves the roles too.
    (["schema.columns.rank"], COMPARISON_CHAIN, "restrict_to_common"),
    (["evaluation.report.display.compare_k"], None, "resolve_display_ks"),
    (["evaluation.report.display.compare_k"], COMPARISON_CHAIN,
     "resolve_comparison_display_ks"),
    (["evaluation.report.display.compare_k"], _COMPARE_WITHOUT_THE_MAIN_CUT,
     "resolve_comparison_display_ks"),
    # The run's order wins over the declarations' when the two disagree.
    (["compare_to_source", "resolve_display_ks"],
     ("resolve_display_ks", "compare_to_source"), "resolve_display_ks"),
    (["evaluation.baseline.score"], COMPARISON_CHAIN, None),
    (["dataset.sample_ratio"], None, None),
])
def test_the_re_run_is_the_run_s_first_step_applying_it(
        changed, run_nodes, node):
    assert rerun_node(changed, run_nodes=run_nodes) == node


# ----------------------------------------- a step the run does not have


def _comparison(params: dict) -> LoadedArtifact:
    seg = _landed(params, "prepare_eval_data")
    metrics = _landed(params, "compute_compare_metrics", seg, None)
    return LoadedArtifact(
        catalog_name="source_comparison",
        payload=_landed(params, "compare_to_source", metrics, metrics),
        produced_by="compare_to_source")


@pytest.mark.parametrize("path", ["post_training", "evaluation.segment_columns"])
def test_compare_only_leaves_the_prepared_record_to_the_directory(path):
    """A record of a step the run does not have is not compared: under
    --compare-only, the partitions an earlier run wrote, which today's run
    mode and segment settings say nothing about. The other modes compare
    it."""
    old = _params()
    new = _changed(old, path)
    require_computed_with_current_config(
        [_comparison(old)], new, run_nodes=COMPARISON_CHAIN)
    assert "--from-node prepare_eval_data," in _refusal([_comparison(old)], new)


def test_compare_only_still_compares_what_its_own_steps_applied():
    old = _params()
    new = _set(copy.deepcopy(old), "evaluation.k_values", [1, 5])
    msg = _refusal([_comparison(old)], new, run_nodes=COMPARISON_CHAIN)
    assert "settings its input from compute_compare_metrics was computed " \
           "with" in msg
    assert "--from-node compute_compare_metrics," in msg


def test_compare_only_refuses_a_comparison_computed_under_other_roles():
    """The prepared record is skipped under --compare-only, so the chain's
    own steps must hold ``schema`` for a role change to be caught."""
    old = _params()
    new = _set(copy.deepcopy(old), "schema.columns.rank", "position")
    msg = _refusal([_comparison(old)], new, run_nodes=COMPARISON_CHAIN)
    assert "schema.columns.rank: 'rank' -> 'position'" in msg
    assert "--from-node restrict_to_common," in msg


# ------------------------------------------------------------ normalisation


def test_absent_and_null_are_different_settings():
    """``sections.get("popularity_baseline", True)``: a missing key computes
    the baseline, an explicit null does not."""
    path = "evaluation.report.sections.popularity_baseline"
    null = _set(_params(), path, None)
    absent = _delete(_params(), path)
    assert path not in fingerprint(absent, "resolve_display_ks")["applied"]
    assert fingerprint(null, "resolve_display_ks")["applied"][path] is None
    display = LoadedArtifact(
        catalog_name="evaluation_display_ks",
        payload=_landed(absent, "resolve_display_ks"),
        produced_by="resolve_display_ks")
    assert _stale([display], null) == {"evaluation_display_ks"}


def test_a_result_survives_a_json_round_trip():
    """YAML turns an unquoted ``2026-01-31`` into a ``datetime.date``; the
    landed JSON holds a string. Reading it back must still compare equal."""
    params = _set(_params(), "evaluation.snap_date", datetime.date(2026, 1, 31))
    require_computed_with_current_config(_main_report(params), params)


def test_int_vs_str_dict_key_is_a_change():
    """YAML can write ``{1: 0.5}`` or ``{"1": 0.5}``, and a lookup keyed by
    str only ever matches the second: recording them alike would let that
    change pass."""
    int_keyed = _set(_params(), "evaluation.metric", {1: 0.5})
    str_keyed = _set(_params(), "evaluation.metric", {"1": 0.5})
    assert _stale(_main_report(int_keyed), str_keyed)


def test_an_int_keyed_config_round_trips_and_mixed_keys_do_not_raise():
    params = _set(_params(), "evaluation.metric", {1: 0.5, "a": 0.3})
    require_computed_with_current_config(_main_report(params), params)


def test_long_values_are_truncated_in_the_message():
    old = _params()
    new = _set(copy.deepcopy(old), "evaluation.segment_columns",
               [f"col_{i:03d}" for i in range(100)])
    msg = _refusal(_main_report(old), new)
    assert "col_099" not in msg
    line = next(ln for ln in msg.splitlines()
                if "evaluation.segment_columns:" in ln)
    assert len(line.split(" -> ", 1)[1]) <= 80


# ------------------------------------ what the enriched partition was made with


def test_the_partition_rows_depend_on_the_run_mode_schema_and_segment_settings():
    assert set(PARTITION_FINGERPRINT_KEYS) == {
        "schema", "post_training", "evaluation.segment_columns",
        "evaluation.segment_sources"}


def test_a_partition_written_under_other_column_roles_is_not_this_runs():
    """The roles name the columns the rows hold (here the rank column):
    another run's partition under another ``schema`` holds other columns."""
    one = _params()
    other = _set(copy.deepcopy(one), "schema.columns.rank", "position")
    assert partition_fingerprint(one, []) != partition_fingerprint(other, [])


def test_the_partition_rows_do_not_depend_on_the_category_column():
    """The rows are the same whichever column the table is read off, so a
    partition another run wrote under another column is still this run's."""
    one = _set(_params(), "evaluation.item_categories",
               {"enabled": True, "column": "family"})
    other = _set(copy.deepcopy(one), "evaluation.item_categories.column", "tier")
    assert partition_fingerprint(one, []) == partition_fingerprint(other, [])


def test_the_recorded_partition_print_is_the_one_the_run_stamped():
    params = _set(_params(), "post_training", True)
    seg = {"joined": ["segment_a"], **_landed(params, "prepare_eval_data")}
    assert recorded_partition_fingerprint(seg) == \
        partition_fingerprint(params, ["segment_a"])
    assert recorded_partition_fingerprint({"joined": []}) is None


# --------------------------------------------------------------- dependencies


def test_module_imports_no_project_or_spark_code():
    """Pure stdlib (ADR-0020 decision 2, dependency direction)."""
    import recsys_tfb.pipelines.evaluation.steps.config_fingerprint as mod

    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    bad = sorted(m for m in imported
                 if m.split(".")[0] in {"recsys_tfb", "pyspark"} or m == "")
    assert bad == []

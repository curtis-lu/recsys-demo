"""Tests for evaluation pipeline definition."""

import inspect

from recsys_tfb.pipelines.evaluation import create_pipeline


class TestEvaluationPipelineDefault:
    """Default (post_training=False) — monitoring scenario."""

    # Node count is pinned by test_node_names' full ordered name-list
    # assertion below, not by a separate magic-number test — a standalone
    # count assertion silently drifts (see class docstrings elsewhere in
    # this file that already went stale by 3) while adding no coverage a
    # name-list check doesn't already provide.

    def test_pipeline_reads_ranked_predictions(self):
        pipeline = create_pipeline()
        assert "ranked_predictions" in pipeline.inputs
        assert "training_eval_predictions" not in pipeline.inputs

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "eval_predictions", "diagnosis_sample", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions", "evaluation_metric_ci",
            "evaluation_offset_sweep", "evaluation_pair_ledger",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "draw_diagnosis_sample_node",
            "compute_metrics", "compute_baseline_metrics",
            "persist_eval_predictions",
            "compute_metric_ci", "compute_offset_sweep",
            "compute_pair_ledger",
            "generate_report",
        ]


class TestEvaluationPipelinePostTraining:
    """post_training=True — read from training_eval_predictions."""

    def test_node_names(self):
        pipeline = create_pipeline(post_training=True)
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "draw_diagnosis_sample_node",
            "compute_metrics", "compute_baseline_metrics",
            "persist_eval_predictions",
            "compute_metric_ci", "compute_offset_sweep",
            "compute_pair_ledger",
            "generate_report",
        ]

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_same_as_default(self):
        pipeline = create_pipeline(post_training=True)
        expected = {
            "eval_predictions", "diagnosis_sample", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions", "evaluation_metric_ci",
            "evaluation_offset_sweep", "evaluation_pair_ledger",
        }
        assert pipeline.outputs == expected


class TestEvaluationPipelineCompareMode:
    """compare_source set — 12 nodes total, both reports produced."""

    def test_full_node_name_order(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "load_compare_predictions",
            "draw_diagnosis_sample_node", "compute_metrics",
            "compute_baseline_metrics", "persist_eval_predictions",
            "restrict_to_common", "compute_metric_ci",
            "compute_offset_sweep", "compute_pair_ledger",
            "generate_comparison_report", "generate_report",
        ]

    def test_pipeline_outputs_include_comparison_report(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        assert "evaluation_comparison_report" in pipeline.outputs
        assert "evaluation_report" in pipeline.outputs

    def test_pipeline_node_names(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        names = [n.name for n in pipeline.nodes]
        assert "load_compare_predictions" in names
        assert "restrict_to_common" in names
        assert "generate_comparison_report" in names


class TestEvaluationPipelineCompareOnly:
    """compare_only=True — 4-node short pipeline reading from Hive."""

    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline(compare_only=True)
        assert len(pipeline.nodes) == 4

    def test_pipeline_outputs_only_comparison_report(self):
        pipeline = create_pipeline(compare_only=True)
        assert "evaluation_comparison_report" in pipeline.outputs
        assert "evaluation_report" not in pipeline.outputs

    def test_pipeline_node_names(self):
        pipeline = create_pipeline(compare_only=True)
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "validate_enriched_eval_predictions_present",
            "load_compare_predictions",
            "restrict_to_common",
            "generate_comparison_report",
        ]

    def test_pipeline_inputs(self):
        pipeline = create_pipeline(compare_only=True)
        assert "label_table" in pipeline.inputs
        assert "parameters" in pipeline.inputs


class TestGenerateReportNodeWiring:
    """core/runner.py binds Node inputs to the wrapped function purely by
    position (``node.func(*inputs)`` — no keyword matching, see
    src/recsys_tfb/core/runner.py). generate_report's tail parameters
    (baseline_metrics/metric_ci/offset_sweep/pair_ledger) are all
    ``Optional[dict]``, so if the Node's ``inputs=[...]`` list in
    pipeline.py drifts out of sync with the function signature's parameter
    order (e.g. two adjacent diagnostics dicts get swapped), one dict
    silently lands in the wrong parameter — Python raises no TypeError
    (both sides are dict-typed) and the corresponding report section just
    goes missing with no exception anywhere. This test pins that ordering.

    Catalog keys and parameter names aren't spelled identically: the first
    four line up exactly (eval_predictions, evaluation_metrics, parameters,
    baseline_metrics) but the last three catalog keys carry an
    "evaluation_" prefix the parameter names drop (evaluation_metric_ci ->
    metric_ci, evaluation_offset_sweep -> offset_sweep,
    evaluation_pair_ledger -> pair_ledger). So we can't assert plain string
    equality position-for-position; the strongest checkable property is:
    each catalog key equals its parameter name, optionally after stripping
    a leading "evaluation_", position-for-position.
    """

    def test_inputs_positionally_match_signature(self):
        pipeline = create_pipeline()
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        param_names = list(inspect.signature(node.func).parameters)

        assert len(node.inputs) == len(param_names), (
            f"generate_report takes {len(param_names)} params "
            f"{param_names} but the Node wires {len(node.inputs)} inputs "
            f"{node.inputs} — positional binding would misalign."
        )
        for position, (catalog_key, param_name) in enumerate(
            zip(node.inputs, param_names)
        ):
            stripped = catalog_key[len("evaluation_"):] \
                if catalog_key.startswith("evaluation_") else catalog_key
            assert catalog_key == param_name or stripped == param_name, (
                f"position {position}: catalog key {catalog_key!r} would "
                f"positionally bind to parameter {param_name!r} — inputs "
                f"list and function signature are out of sync."
            )

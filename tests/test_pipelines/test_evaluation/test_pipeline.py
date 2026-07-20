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
            "evaluation_config_shift",
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
            "compute_pair_ledger", "diagnose_config_shift",
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
            "compute_pair_ledger", "diagnose_config_shift",
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
            "evaluation_config_shift",
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
            "diagnose_config_shift", "generate_comparison_report",
            "generate_report",
        ]

    def test_pipeline_outputs_include_comparison_report(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        assert "evaluation_comparison_report" in pipeline.outputs
        assert "evaluation_report" in pipeline.outputs


class TestEvaluationPipelineCompareOnly:
    """compare_only=True — short pipeline reading from Hive.

    Node count is pinned by test_pipeline_node_names' full ordered name-list
    assertion below, not by a separate magic-number test — same reasoning as
    the comment at the top of this file (the old "4-node" wording in this
    docstring is exactly the drift that motivates it).
    """

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
    baseline_metrics) but the next three catalog keys carry an
    "evaluation_" prefix the parameter names drop (evaluation_metric_ci ->
    metric_ci, evaluation_offset_sweep -> offset_sweep,
    evaluation_pair_ledger -> pair_ledger). So we can't assert plain string
    equality position-for-position; the strongest checkable property is:
    each catalog key equals its parameter name, optionally after stripping
    a leading "evaluation_", position-for-position.

    The signature ends in ``*registry_diagnoses`` — the contract registry's
    diagnoses, collected by position. Named parameters can't cover that tail
    (one name per diagnosis is exactly the coupling the registry removes), so
    the tail is checked against a different invariant: the trailing inputs
    must be ``evaluation_<name>`` for each name in ``DIAGNOSES``, **in
    registry order**. That order is what ``generate_report`` zips against, so
    a mismatch hands one diagnosis's numbers to another's page title — every
    page still renders, which is why this needs pinning rather than trusting
    the derived comprehension in pipeline.py to stay derived.
    """

    def test_inputs_positionally_match_signature(self):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        pipeline = create_pipeline()
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        params = inspect.signature(node.func).parameters
        fixed = [
            name for name, p in params.items()
            if p.kind is not inspect.Parameter.VAR_POSITIONAL
        ]
        has_varargs = len(fixed) != len(params)

        assert len(node.inputs) == len(fixed) + len(DIAGNOSES), (
            f"generate_report takes {len(fixed)} fixed params {fixed} plus "
            f"{len(DIAGNOSES)} registry diagnoses but the Node wires "
            f"{len(node.inputs)} inputs {node.inputs} — positional binding "
            "would misalign."
        )
        for position, (catalog_key, param_name) in enumerate(
            zip(node.inputs, fixed)
        ):
            stripped = catalog_key[len("evaluation_"):] \
                if catalog_key.startswith("evaluation_") else catalog_key
            assert catalog_key == param_name or stripped == param_name, (
                f"position {position}: catalog key {catalog_key!r} would "
                f"positionally bind to parameter {param_name!r} — inputs "
                f"list and function signature are out of sync."
            )

        assert has_varargs, (
            "generate_report no longer ends in *registry_diagnoses; the "
            "registry tail below is checking inputs that now bind to named "
            "parameters instead."
        )
        assert node.inputs[len(fixed):] == [
            f"evaluation_{name}" for name in DIAGNOSES
        ], (
            f"registry diagnosis inputs {node.inputs[len(fixed):]} are out of "
            f"sync with DIAGNOSES {DIAGNOSES} — generate_report zips the two "
            "by position, so a mismatch silently retitles pages."
        )


class TestGenerateComparisonReportNodeWiring:
    """Same positional-binding hazard as TestGenerateReportNodeWiring, but a
    strictly more silent failure mode.

    generate_comparison_report's first two parameters — eval_predictions_common
    and compare_predictions_common — are *both* SparkDataFrame. Swapping them
    in the Node's ``inputs=[...]`` raises nothing anywhere: the report is still
    produced, still has every section, and every number in it is simply the
    other model's. generate_report's failure at least drops a section; this one
    silently relabels Model as Compare and vice versa.

    The wiring is duplicated at two call sites in pipeline.py (the
    ``compare_only=True`` short pipeline and the ``compare_source`` full
    pipeline), so both are checked — a fix applied to only one is the likely
    drift.

    Matching rule: the first two catalog keys equal their parameter names
    verbatim; the third carries a "compare_" prefix the parameter drops
    (compare_coverage_partial -> coverage_partial). So we accept a key that
    either equals its parameter or equals it after stripping a leading
    "compare_". Note this still catches the dangerous swap: putting
    compare_predictions_common at position 0 matches neither
    eval_predictions_common nor (stripped) predictions_common.
    """

    @staticmethod
    def _comparison_nodes():
        """The generate_comparison_report Node from every pipeline that wires it."""
        pipelines = {
            "compare_only": create_pipeline(compare_only=True),
            "compare_source": create_pipeline(
                compare_source={"kind": "hive", "model_version": "v1"}
            ),
        }
        nodes = {
            label: next(
                n for n in p.nodes if n.name == "generate_comparison_report"
            )
            for label, p in pipelines.items()
        }
        assert len(nodes) == 2, "both wire points must be covered"
        return nodes

    def test_inputs_positionally_match_signature(self):
        for label, node in self._comparison_nodes().items():
            param_names = list(inspect.signature(node.func).parameters)

            assert len(node.inputs) == len(param_names), (
                f"[{label}] generate_comparison_report takes "
                f"{len(param_names)} params {param_names} but the Node wires "
                f"{len(node.inputs)} inputs {node.inputs} — positional "
                f"binding would misalign."
            )
            for position, (catalog_key, param_name) in enumerate(
                zip(node.inputs, param_names)
            ):
                stripped = catalog_key[len("compare_"):] \
                    if catalog_key.startswith("compare_") else catalog_key
                assert catalog_key == param_name or stripped == param_name, (
                    f"[{label}] position {position}: catalog key "
                    f"{catalog_key!r} would positionally bind to parameter "
                    f"{param_name!r} — inputs list and function signature are "
                    f"out of sync. Note both prediction params are "
                    f"SparkDataFrame, so a swap raises nothing at runtime and "
                    f"only flips Model/Compare in the report."
                )

    def test_two_prediction_inputs_are_not_swapped(self):
        """Explicit, readable pin of the exact swap that raises nothing."""
        for label, node in self._comparison_nodes().items():
            assert node.inputs[0] == "eval_predictions_common", label
            assert node.inputs[1] == "compare_predictions_common", label


class TestConfigShiftNodeWiring:
    """診斷 1／5（config_shift）接上 evaluation pipeline。

    只驗接線，不驗計算——計算層的測試在 tests/test_diagnosis/。這裡要釘的是
    「它真的吃到共用的 diagnosis_sample」：五項診斷共用同一份樣本是一致性
    保證（不同母體的數字並排解讀會錯），一旦哪天有人把 inputs 改成
    eval_predictions 自己重抽，數字看起來仍然合理，只是不再可比。
    """

    def test_config_shift_node_wired_after_diagnosis_sample(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "diagnose_config_shift" in names
        assert (
            names.index("draw_diagnosis_sample_node")
            < names.index("diagnose_config_shift")
        )

    def test_config_shift_inputs_and_outputs(self):
        pipeline = create_pipeline()
        node = next(
            n for n in pipeline.nodes if n.name == "diagnose_config_shift"
        )
        assert node.inputs == ["diagnosis_sample", "parameters"]
        assert node.outputs == ["evaluation_config_shift"]

    def test_config_shift_wired_in_post_training_mode_too(self):
        """--post-training 走的是同一組診斷節點，只有預測來源不同。"""
        pipeline = create_pipeline(post_training=True)
        assert "evaluation_config_shift" in pipeline.outputs

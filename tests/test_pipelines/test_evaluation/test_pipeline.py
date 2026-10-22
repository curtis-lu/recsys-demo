"""Tests for evaluation pipeline definition."""

import ast
import inspect
import textwrap

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
            "evaluation_segment_columns", "evaluation_item_categories",
            "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions",
            "evaluation_report_aggregates",
            "prediction_quality_metrics",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data",
            "compute_metrics", "compute_baseline_metrics",
            "compute_prediction_quality",
            "compute_report_aggregates",
            "generate_report",
        ]


class TestNoDiagnosisInAnyMode:
    """#532 (ADR-0031 decision 2) removed every evaluation diagnosis: the four
    registry diagnoses, the shared sample they drew, their pages and the
    monitoring-mode stub that stood in for those pages. No mode may wire any of
    it back, under any name: a node or dataset whose name says "diagnos".

    The three ``--compare`` nodes are added outside the mode switch, so each
    mode with a comparison is listed on its own.
    """

    MODES = {
        "monitoring": {},
        "monitoring --compare": {
            "compare_source": {"kind": "hive", "model_version": "v1"},
        },
        "post-training": {"post_training": True},
        "post-training --compare": {
            "post_training": True,
            "compare_source": {"kind": "hive", "model_version": "v1"},
        },
        # #397: the positive-rate baseline adds a node that sorts before
        # compute_metrics; every mode-wide rule here must hold with it too.
        "post-training score: rate": {"post_training": True, "baseline_rate": True},
        "post-training score: rate --compare": {
            "post_training": True, "baseline_rate": True,
            "compare_source": {"kind": "hive", "model_version": "v1"},
        },
    }
    COMPARE_ONLY = {
        "compare_only": True,
        "compare_source": {"kind": "hive", "model_version": "v1"},
    }

    def test_no_node_or_dataset_is_a_diagnosis(self):
        for label, kwargs in {**self.MODES, "--compare-only": self.COMPARE_ONLY}.items():
            pipeline = create_pipeline(**kwargs)
            names = {n.name for n in pipeline.nodes}
            datasets = set(pipeline.inputs) | set(pipeline.outputs)
            for n in pipeline.nodes:
                datasets |= set(n.inputs) | set(n.outputs)
            leaked = sorted(x for x in names | datasets if "diagnos" in x)
            assert leaked == [], f"[{label}] {leaked}"


class TestFingerprintRerunNodes:
    """``COMPUTED_KEYS``' re-run nodes precede every fingerprinted producer.

    ``require_computed_with_current_config`` answers a changed computed key
    with ``--from-node <that key's node>``. The advice is only right if that
    node's forward slice contains every node that writes a JSON with a
    ``config_fingerprint``; otherwise following it leaves a stale artifact
    behind and the next report raises again (or, worse, one checked by nobody
    stays stale). The producer lists are written out here, not derived from
    the pipeline, so a new producer has to be added on purpose.

    ``prepare_eval_data`` is left out on purpose. Its
    ``evaluation_segment_columns`` is compared on ``PARTITION_CONTENT_KEYS``
    only, the rows already re-run from ``prepare_eval_data``, so no row's
    advice can leave it stale (``tests/test_pipelines/test_evaluation/
    test_config_fingerprint.py`` pins both directions).
    """

    FINGERPRINTED_PRODUCERS = {
        "compute_metrics", "compute_baseline_metrics",
        "compute_prediction_quality",
        "compute_report_aggregates",
    }
    MODES = TestNoDiagnosisInAnyMode.MODES

    def _producers(self, kwargs):
        return set(self.FINGERPRINTED_PRODUCERS)

    def test_every_rerun_node_exists_in_every_mode(self):
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import COMPUTED_KEYS

        for label, kwargs in self.MODES.items():
            names = [n.name for n in create_pipeline(**kwargs).nodes]
            missing = sorted({node for _, node in COMPUTED_KEYS} - set(names))
            assert missing == [], f"[{label}] {missing}"

    def test_rerun_nodes_are_listed_in_topological_order(self):
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import COMPUTED_KEYS

        for label, kwargs in self.MODES.items():
            names = [n.name for n in create_pipeline(**kwargs).nodes]
            positions = [names.index(node) for _, node in COMPUTED_KEYS]
            assert positions == sorted(positions), (
                f"[{label}] {[node for _, node in COMPUTED_KEYS]}"
            )

    def test_latest_rerun_node_slice_covers_every_fingerprinted_producer(self):
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import COMPUTED_KEYS

        for label, kwargs in self.MODES.items():
            pipeline = create_pipeline(**kwargs)
            names = [n.name for n in pipeline.nodes]
            latest = max({node for _, node in COMPUTED_KEYS}, key=names.index)
            sliced, _plan = pipeline.slice_from(latest, lambda _name: True)
            left_out = sorted(
                self._producers(kwargs) - {n.name for n in sliced.nodes}
            )
            assert left_out == [], (
                f"[{label}] --from-node {latest} does not re-run {left_out}"
            )


class TestEvaluationPipelinePostTraining:
    """post_training=True — read from training_eval_predictions."""

    def test_node_names(self):
        pipeline = create_pipeline(post_training=True)
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data",
            "compute_metrics", "compute_baseline_metrics",
            "compute_prediction_quality",
            "compute_report_aggregates",
            "generate_report",
        ]

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_match_monitoring(self):
        """Since #532 the modes differ only in the predictions table read."""
        assert (create_pipeline(post_training=True).outputs
                == create_pipeline().outputs)


class TestEvaluationPipelineCompareMode:
    """compare_source set (monitoring) — 3 compare nodes appended, both reports
    produced."""

    def test_full_node_name_order(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data",
            "load_compare_predictions",
            "compute_metrics",
            "compute_baseline_metrics", "compute_prediction_quality",
            "compute_report_aggregates",
            "restrict_to_common",
            "generate_report",
            "generate_comparison_report",
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
        # B copies A's label (ADR-0020 bug 7), so the short path never reads
        # label_table; an input here would be loaded for nothing.
        assert "label_table" not in pipeline.inputs
        assert "parameters" in pipeline.inputs

    def test_the_gate_passes_nothing_on_and_the_restriction_reads_the_table(self):
        """ADR-0018 decision 1: the enriched-partition gate is zero-output, and
        ``restrict_to_common`` reads ``enriched_eval_predictions`` itself."""
        nodes = {n.name: n for n in create_pipeline(compare_only=True).nodes}
        assert nodes["validate_enriched_eval_predictions_present"].outputs == []
        assert nodes["restrict_to_common"].inputs[0] == "enriched_eval_predictions"


class TestSegmentColumnsWiring:
    """ADR-0020 bug 6: segments follow the run mode's population, and what
    was joined lands as ``evaluation_segment_columns`` for every node that
    groups by segment. Node inputs bind by position, so the list's position
    is checked against the parameter named ``segment_columns``."""

    _COMPARE = {"kind": "model_version", "model_version": "v0", "label": "v0"}

    @staticmethod
    def _node(pipeline, name):
        return next(n for n in pipeline.nodes if n.name == name)

    def test_prepare_eval_data_reads_its_mode_population_and_lands_the_list(self):
        for kwargs, predictions, population, other in (
            ({}, "ranked_predictions", "inference_population", "sample_pool"),
            ({"post_training": True}, "training_eval_predictions",
             "sample_pool", "inference_population"),
        ):
            pipeline = create_pipeline(**kwargs)
            node = self._node(pipeline, "prepare_eval_data")
            assert node.inputs == [
                predictions, "label_table", population, "parameters",
                "preprocessor_on_disk"], kwargs
            assert node.outputs == [
                "enriched_eval_predictions", "evaluation_segment_columns",
                "evaluation_item_categories"], kwargs
            assert other not in pipeline.inputs, kwargs

    def test_every_segmenting_node_reads_the_list_at_the_right_position(self):
        cases = [
            (kwargs, name)
            for kwargs in ({}, {"post_training": True})
            for name in ("compute_metrics", "compute_baseline_metrics")
        ] + [
            ({"compare_source": self._COMPARE}, "generate_comparison_report"),
            ({"compare_source": self._COMPARE, "compare_only": True},
             "generate_comparison_report"),
        ]
        for kwargs, name in cases:
            node = self._node(create_pipeline(**kwargs), name)
            params = list(inspect.signature(node.func).parameters)
            assert "evaluation_segment_columns" in node.inputs, (kwargs, name)
            assert node.inputs.index("evaluation_segment_columns") == \
                params.index("segment_columns"), (kwargs, name, node.inputs)

    def test_every_category_reader_reads_the_table_at_the_right_position(self):
        """#379: the category table lands next to the segment list and is read
        by every node that ranks categories. Positional binding, so its
        position is checked against the parameter named ``item_categories``;
        in rate mode compute_baseline_metrics takes two more inputs after it."""
        cases = [
            (kwargs, name)
            for kwargs in ({}, {"post_training": True},
                           {"post_training": True, "baseline_rate": True})
            for name in ("compute_metrics", "compute_baseline_metrics")
        ] + [
            ({"compare_source": self._COMPARE}, "generate_comparison_report"),
            ({"compare_source": self._COMPARE, "compare_only": True},
             "generate_comparison_report"),
        ]
        for kwargs, name in cases:
            node = self._node(create_pipeline(**kwargs), name)
            params = list(inspect.signature(node.func).parameters)
            assert "evaluation_item_categories" in node.inputs, (kwargs, name)
            assert node.inputs.index("evaluation_item_categories") == \
                params.index("item_categories"), (kwargs, name, node.inputs)

    def test_the_category_table_is_an_optional_catalog_entry(self):
        """Optional: an evaluation directory written before #379 has no such
        file, and only column mode needs it (the CLI asks for it there)."""
        from pathlib import Path

        import yaml

        catalog = yaml.safe_load(
            (Path(__file__).parents[3] / "conf/base/catalog.yaml").read_text())
        assert catalog["evaluation_item_categories"] == {
            "type": "JSONDataset",
            "filepath": "data/evaluation/${model_version}/${snap_date}/"
                        "item_categories.json",
            "optional": True,
        }

    def test_the_list_has_a_catalog_entry_next_to_the_run_outputs(self):
        """Without an entry the catalog makes it a MemoryDataset: nothing
        lands, and --compare-only has nothing to read."""
        from pathlib import Path

        import yaml

        catalog = yaml.safe_load(
            (Path(__file__).parents[3] / "conf/base/catalog.yaml").read_text())
        assert catalog["evaluation_segment_columns"] == {
            "type": "JSONDataset",
            "filepath": "data/evaluation/${model_version}/${snap_date}/"
                        "segment_columns.json",
        }


def _readers_that_skip_the_month_restriction(pipeline):
    """Names of nodes wired to ``enriched_eval_predictions`` whose body has no
    used ``restrict_to_current_eval_partitions(<that input's parameter>, ...)``
    call.

    That call both keeps the evaluated dates and checks each date's partition
    was written under the settings the reader reads it for (#374); the plain
    ``restrict_to_eval_snap_dates`` does only the first, so it does not count.

    The body comes from the node's function object (``inspect.getsource``),
    not from a module path: node modules get renamed and split (ADR-0019), and
    a factory-built node's body is its inner function. The call must take the
    parameter the table binds to (inputs bind by position), so restricting
    some other frame does not count, and its result must be used: a bare call
    statement throws the restricted frame away. It checks that the call is
    there, not that it comes before every other use of the frame.
    """
    missing = []
    for node in pipeline.nodes:
        if "enriched_eval_predictions" not in node.inputs:
            continue
        params = list(inspect.signature(node.func).parameters)
        bound_to = params[node.inputs.index("enriched_eval_predictions")]
        body = ast.parse(textwrap.dedent(inspect.getsource(node.func)))
        discarded = {id(stmt.value) for stmt in ast.walk(body)
                     if isinstance(stmt, ast.Expr)}
        restricts = any(
            isinstance(call, ast.Call)
            and id(call) not in discarded
            and getattr(call.func, "id", getattr(call.func, "attr", None))
            == "restrict_to_current_eval_partitions"
            and call.args
            and isinstance(call.args[0], ast.Name)
            and call.args[0].id == bound_to
            for call in ast.walk(body)
        )
        if not restricts:
            missing.append(node.name)
    return missing


def _forgets_the_month(enriched_eval_predictions, parameters):
    return enriched_eval_predictions.count()


def _restricts_the_wrong_frame(other, enriched_eval_predictions, parameters):
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        restrict_to_current_eval_partitions,
    )

    other = restrict_to_current_eval_partitions(other, parameters, {}).frame
    return enriched_eval_predictions.count() + other.count()


def _discards_the_restriction(enriched_eval_predictions, parameters):
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        restrict_to_current_eval_partitions,
    )

    restrict_to_current_eval_partitions(enriched_eval_predictions, parameters, {})
    return enriched_eval_predictions.count()


def _restricts_without_checking_the_partitions(enriched_eval_predictions,
                                               parameters):
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        restrict_to_eval_snap_dates,
    )

    return restrict_to_eval_snap_dates(
        enriched_eval_predictions, parameters).count()


class TestEveryEnrichedReaderKeepsTheEvaluatedMonth:
    """ADR-0018 decision 1: ``enriched_eval_predictions`` holds every month a
    ``model_version`` was evaluated on, so each node reading it keeps the
    evaluated month first. Forgetting raises nothing and computes over all
    months; this is the guard against the next reader that forgets.
    """

    MODES = {
        **TestNoDiagnosisInAnyMode.MODES,
        "compare-only": {
            "compare_only": True,
            "compare_source": {"kind": "hive", "model_version": "v1"},
        },
    }

    def test_every_reader_in_every_mode_restricts_the_table(self):
        for label, kwargs in self.MODES.items():
            pipeline = create_pipeline(**kwargs)
            readers = [n.name for n in pipeline.nodes
                       if "enriched_eval_predictions" in n.inputs]
            # Guard the scan before trusting its empty answer.
            assert readers, f"[{label}] no node reads the table"
            assert _readers_that_skip_the_month_restriction(pipeline) == [], label

    def test_a_new_reader_that_forgets_is_named(self):
        from recsys_tfb.core.node import Node
        from recsys_tfb.core.pipeline import Pipeline

        pipeline = Pipeline([
            Node(_forgets_the_month,
                 inputs=["enriched_eval_predictions", "parameters"],
                 outputs="n_all_months"),
            Node(_restricts_the_wrong_frame,
                 inputs=["other_frame", "enriched_eval_predictions",
                         "parameters"],
                 outputs="n_mixed"),
            Node(_discards_the_restriction,
                 inputs=["enriched_eval_predictions", "parameters"],
                 outputs="n_discarded"),
            Node(_restricts_without_checking_the_partitions,
                 inputs=["enriched_eval_predictions", "parameters"],
                 outputs="n_unchecked"),
        ])
        assert sorted(_readers_that_skip_the_month_restriction(pipeline)) == [
            "_discards_the_restriction", "_forgets_the_month",
            "_restricts_the_wrong_frame",
            "_restricts_without_checking_the_partitions",
        ]


class TestGenerateReportNodeWiring:
    """generate_report is wired by name (ADR-0030 decision 8, ADR-0031
    principle 1): the Node binds each catalog entry to the parameter its
    ``inputs`` dict names, and the report keys every result by that name.

    So the property to pin is that every parameter is named after the entry
    it is fed: ``{"evaluation_metrics": "baseline_metrics"}`` binds without an
    error, and the report would draw the baseline where the model's metrics
    go. And no parameter has a default: an entry left out of the dict would
    then arrive as the default instead of failing where the Node is built
    (Plan 1.5; the 2026-07-20 production TypeError was a stale wiring that
    defaults let through).
    """

    def test_every_parameter_is_fed_the_entry_it_is_named_after(self):
        for kwargs in ({}, {"post_training": True}):
            node = next(n for n in create_pipeline(**kwargs).nodes
                        if n.name == "generate_report")
            params = inspect.signature(node.func).parameters
            assert node.keyword_inputs is not None, (
                "generate_report is wired by position again", kwargs)
            assert node.keyword_inputs == {name: name for name in params}, (
                kwargs, node.keyword_inputs)
            assert not [
                name for name, p in params.items()
                if p.default is not inspect.Parameter.empty
            ], "generate_report grew a defaulted parameter"

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
                stripped = catalog_key
                for prefix in ("compare_", "evaluation_"):
                    if stripped.startswith(prefix):
                        stripped = stripped[len(prefix):]
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

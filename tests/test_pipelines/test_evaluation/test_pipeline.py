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
            "eval_predictions", "evaluation_segment_columns",
            "diagnosis_sample", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions", "evaluation_metric_ci",
            "evaluation_diagnosis_pages",
            "evaluation_report_aggregates",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "no_diagnosis_pages",
            "draw_diagnosis_sample_node",
            "compute_metrics", "compute_baseline_metrics",
            "compute_report_aggregates",
            "persist_eval_predictions",
            "compute_metric_ci",
            "generate_report",
        ]


class TestRegistryDiagnosesFollowTheMode:
    """Registry diagnoses are wired in ``--post-training`` only (ADR-0018
    decision 5).

    They need ``score_uncalibrated``, which the monitoring source does not
    guarantee; wired in, the default mode crashed at the first diagnosis, in
    production too. Monitoring mode's ``evaluation_diagnosis_pages`` comes from
    the zero-read ``no_diagnosis_pages`` instead, not from
    ``render_diagnosis_pages``: that one requires one named, fingerprinted
    result per registry diagnosis, and this mode computes none (why the stub,
    see ``no_diagnosis_pages``' docstring).

    The three ``--compare`` nodes are added outside the mode switch, so each
    mode with a comparison must still match its own shape.
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
    }

    @staticmethod
    def _names(**kwargs):
        return [n.name for n in create_pipeline(**kwargs).nodes]

    def test_monitoring_modes_wire_no_registry_diagnosis(self):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        for label in ("monitoring", "monitoring --compare"):
            names = self._names(**self.MODES[label])
            leaked = [
                n for n in names
                if n in {f"diagnose_{d}" for d in DIAGNOSES}
                or n == "render_diagnosis_pages"
            ]
            assert leaked == [], f"[{label}] {leaked}"
            assert "no_diagnosis_pages" in names, label

    def test_post_training_modes_wire_every_registry_diagnosis(self):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        for label in ("post-training", "post-training --compare"):
            names = self._names(**self.MODES[label])
            for d in DIAGNOSES:
                assert f"diagnose_{d}" in names, f"[{label}] diagnose_{d}"
            assert "render_diagnosis_pages" in names, label
            assert "no_diagnosis_pages" not in names, label

    def test_exactly_one_producer_of_the_diagnosis_pages_in_every_mode(self):
        """``generate_report`` 位置綁定、六個必填輸入：每種模式都得有人產出它的
        第六個輸入，而且只能有一個。"""
        for label, kwargs in self.MODES.items():
            producers = [
                n.name for n in create_pipeline(**kwargs).nodes
                if "evaluation_diagnosis_pages" in n.outputs
            ]
            assert len(producers) == 1, f"[{label}] {producers}"

    def test_monitoring_draws_no_sample_for_diagnoses_it_does_not_wire(self):
        """With the metric CI off, monitoring mode has no consumer of the
        diagnosis sample left, so it must not draw one; post-training still
        does, for its registry diagnoses.

        The draw is a driver-side ``toPandas`` of up to
        ``diagnosis.sample.max_queries`` queries. Drawing it for nobody raises
        nothing and only shows up as a slower run. The registry diagnoses'
        ``enabled`` flags default to true in both modes, so the config cannot
        tell the modes apart; the pipeline has to.
        """
        from unittest.mock import patch

        import pandas as pd

        params = {"evaluation": {"diagnosis": {"ci": {"enabled": False}}}}
        outcome = {}
        for label in ("monitoring", "post-training"):
            node = next(
                n for n in create_pipeline(**self.MODES[label]).nodes
                if n.name == "draw_diagnosis_sample_node"
            )
            with patch(
                "recsys_tfb.diagnosis.metric.sample.draw_diagnosis_sample",
                return_value=(pd.DataFrame(), {"n_queries_sampled": 0}),
            ) as spy:
                result = node.func(None, {"joined": []}, params)
            outcome[label] = {"draws": spy.call_count, "sample": result is not None}
        assert outcome == {
            "monitoring": {"draws": 0, "sample": False},
            "post-training": {"draws": 1, "sample": True},
        }

    def test_monitoring_stub_reads_nothing_and_returns_no_pages(self):
        node = next(
            n for n in create_pipeline().nodes if n.name == "no_diagnosis_pages"
        )
        assert node.inputs == ["parameters"]
        assert node.outputs == ["evaluation_diagnosis_pages"]
        assert node.func({"evaluation": {}}) == []


class TestFingerprintRerunNodes:
    """``COMPUTED_KEYS``' re-run nodes precede every fingerprinted producer.

    ``require_computed_with_current_config`` answers a changed computed key
    with ``--from-node <that key's node>``. The advice is only right if that
    node's forward slice contains every node that writes a JSON with a
    ``config_fingerprint``; otherwise following it leaves a stale artifact
    behind and the next report raises again (or, worse, one checked by nobody
    stays stale). The producer lists are written out here, not derived from
    the pipeline, so a new producer has to be added on purpose.
    """

    FINGERPRINTED_PRODUCERS = {
        "compute_baseline_metrics", "compute_report_aggregates",
        "compute_metric_ci",
    }
    MODES = TestRegistryDiagnosesFollowTheMode.MODES

    def _producers(self, kwargs):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        producers = set(self.FINGERPRINTED_PRODUCERS)
        if kwargs.get("post_training"):
            producers |= {f"diagnose_{name}" for name in DIAGNOSES}
        return producers

    def test_every_rerun_node_exists_in_every_mode(self):
        from recsys_tfb.evaluation.config_fingerprint import COMPUTED_KEYS

        for label, kwargs in self.MODES.items():
            names = [n.name for n in create_pipeline(**kwargs).nodes]
            missing = sorted({node for _, node in COMPUTED_KEYS} - set(names))
            assert missing == [], f"[{label}] {missing}"

    def test_rerun_nodes_are_listed_in_topological_order(self):
        from recsys_tfb.evaluation.config_fingerprint import COMPUTED_KEYS

        for label, kwargs in self.MODES.items():
            names = [n.name for n in create_pipeline(**kwargs).nodes]
            positions = [names.index(node) for _, node in COMPUTED_KEYS]
            assert positions == sorted(positions), (
                f"[{label}] {[node for _, node in COMPUTED_KEYS]}"
            )

    def test_latest_rerun_node_slice_covers_every_fingerprinted_producer(self):
        from recsys_tfb.evaluation.config_fingerprint import COMPUTED_KEYS

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
            "prepare_eval_data", "draw_diagnosis_sample_node",
            "compute_metrics", "compute_baseline_metrics",
            "compute_report_aggregates",
            "persist_eval_predictions",
            "compute_metric_ci",
            "diagnose_config_shift",
            "diagnose_item_ability",
            "diagnose_suppression",
            "diagnose_model_capacity",
            "render_diagnosis_pages",
            "generate_report",
        ]

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_add_the_registry_diagnoses(self):
        pipeline = create_pipeline(post_training=True)
        expected = {
            "eval_predictions", "evaluation_segment_columns",
            "diagnosis_sample", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "enriched_eval_predictions", "evaluation_metric_ci",
            "evaluation_config_shift", "evaluation_item_ability",
            "evaluation_model_capacity", "evaluation_suppression",
            "evaluation_diagnosis_pages",
            "evaluation_report_aggregates",
        }
        assert pipeline.outputs == expected


class TestEvaluationPipelineCompareMode:
    """compare_source set (monitoring) — 3 compare nodes appended, both reports
    produced. Which diagnoses ride along is pinned per mode in
    ``TestRegistryDiagnosesFollowTheMode``."""

    def test_full_node_name_order(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "no_diagnosis_pages",
            "load_compare_predictions",
            "draw_diagnosis_sample_node", "compute_metrics",
            "compute_baseline_metrics", "compute_report_aggregates",
            "persist_eval_predictions",
            "restrict_to_common", "compute_metric_ci",
            "generate_comparison_report",
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
                predictions, "label_table", population, "parameters"], kwargs
            assert node.outputs == [
                "eval_predictions", "evaluation_segment_columns"], kwargs
            assert other not in pipeline.inputs, kwargs

    def test_every_segmenting_node_reads_the_list_at_the_right_position(self):
        cases = [
            (kwargs, name)
            for kwargs in ({}, {"post_training": True})
            for name in ("compute_metrics", "compute_baseline_metrics",
                         "draw_diagnosis_sample_node")
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


class TestGenerateReportNodeWiring:
    """core/runner.py binds Node inputs to the wrapped function purely by
    position (``node.func(*inputs)`` — no keyword matching, see
    src/recsys_tfb/core/runner.py). generate_report's parameters are all
    dict-typed, so if the Node's ``inputs=[...]`` list in pipeline.py drifts
    out of sync with the signature's parameter order, one dict silently lands
    in the wrong parameter — Python raises no TypeError and the corresponding
    report section just goes missing. This test pins that ordering.

    Catalog keys and parameter names aren't spelled identically: some carry an
    "evaluation_" prefix the parameter names drop (evaluation_metric_ci ->
    metric_ci). So the checkable property is: each catalog key equals its
    parameter name, optionally after stripping a leading "evaluation_",
    position-for-position.

    **The signature must have no varargs and no defaults** (Plan 1.5). Both
    properties are what makes a stale pipeline.py fail loudly: with 4 optional
    params plus ``*registry_diagnoses``, 6/7/8/9 inputs were all legal, which
    is exactly how the 2026-07-20 production TypeError happened — the count
    was fine, the positions were not. Diagnosis results no longer appear here
    at all; they are ``render_diagnosis_pages``' business.
    """

    def test_inputs_positionally_match_signature(self):
        for kwargs in ({}, {"post_training": True}):
            self._check_inputs_positionally_match_signature(
                create_pipeline(**kwargs)
            )

    @staticmethod
    def _check_inputs_positionally_match_signature(pipeline):
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        params = inspect.signature(node.func).parameters

        assert not any(
            p.kind is inspect.Parameter.VAR_POSITIONAL for p in params.values()
        ), (
            "generate_report grew varargs again — that reopens the "
            "'count is legal, positions are wrong' failure mode."
        )
        assert not [
            name for name, p in params.items()
            if p.default is not inspect.Parameter.empty
        ], (
            "generate_report grew a defaulted parameter — a stale inputs list "
            "would then bind silently instead of raising TypeError."
        )
        assert len(node.inputs) == len(params), (
            f"generate_report takes {list(params)} but the Node wires "
            f"{node.inputs} — positional binding would misalign."
        )
        for position, (catalog_key, param_name) in enumerate(
            zip(node.inputs, params)
        ):
            stripped = catalog_key[len("evaluation_"):] \
                if catalog_key.startswith("evaluation_") else catalog_key
            assert catalog_key == param_name or stripped == param_name, (
                f"position {position}: catalog key {catalog_key!r} would "
                f"positionally bind to parameter {param_name!r} — inputs "
                f"list and function signature are out of sync."
            )

    def test_no_diagnosis_input_reaches_generate_report(self):
        """加第六項診斷不得再動到 generate_report。

        這條是本次重構的宣稱本身。用 DIAGNOSES 動態導出，Plan 2-5 每加一項
        自動收緊。
        """
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        pipeline = create_pipeline(post_training=True)
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        leaked = [
            i for i in node.inputs
            if any(i == f"evaluation_{name}" for name in DIAGNOSES)
        ]
        assert leaked == [], (
            f"diagnosis results {leaked} are wired into generate_report "
            "again — they belong to render_diagnosis_pages."
        )


class TestRenderDiagnosisPagesNodeWiring:
    """診斷產物接到 ``render_diagnosis_pages``，而且只接到它。

    這個 node 畫的就是這些 inputs 的值（#342 之前按檔名讀磁碟、inputs 只當
    依賴宣告）。同一組 inputs 另外買到兩件事，測試分別對應：執行順序與切片
    擴張。

    只有 ``--post-training`` 組出這個 node（監控模式見
    ``TestRegistryDiagnosesFollowTheMode``），所以這裡都建 post-training 的 DAG。
    """

    def test_every_registry_diagnosis_is_wired_as_a_dependency(self):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        pipeline = create_pipeline(post_training=True)
        node = next(
            n for n in pipeline.nodes if n.name == "render_diagnosis_pages"
        )
        assert node.inputs == [
            "parameters", *(f"evaluation_{name}" for name in DIAGNOSES)
        ]

    def test_runs_after_every_diagnosis_node(self):
        """**主要理由**：拓撲排序只看 ``node.inputs``。

        拿掉診斷 inputs 的話這個 node 的 in-degree 是 0（``parameters`` 沒有
        生產者），Kahn 會把它排到診斷節點**之前**。#342 之前它按檔名讀，那樣
        會讀到上次留下的舊 JSON 而且照樣「成功」；現在它畫的是 inputs，排在
        所有診斷之後就是它拿得到這次結果的前提。這條測試釘的是「它在所有診斷
        之後」，不是 inputs 字串長什麼樣。
        """
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        names = [n.name for n in create_pipeline(post_training=True).nodes]
        me = names.index("render_diagnosis_pages")
        for diag in DIAGNOSES:
            assert names.index(f"diagnose_{diag}") < me, (
                f"render_diagnosis_pages 排在 diagnose_{diag} 之前，"
                "拿不到這次執行的診斷結果"
            )

    def test_slicing_pulls_in_the_diagnosis_nodes_when_nothing_is_on_disk(self):
        """次要理由：切片擴張。

        ``can_load`` 全回 False ＝ 什麼都還沒落地（全新 model_version），切片
        必須把整條上游拉回來。**注意這不是「防止讀到舊 JSON」**——診斷 JSON
        已落地時 ``can_load`` 為 True，切片刻意不重算，那正是 ``--only-node``
        想要的便宜重繪。
        """
        pipeline = create_pipeline(post_training=True)
        sliced, _plan = pipeline.slice_only(
            "render_diagnosis_pages", lambda name: False
        )
        names = [n.name for n in sliced.nodes]
        assert "diagnose_config_shift" in names
        assert "draw_diagnosis_sample_node" in names


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


class TestConfigShiftNodeWiring:
    """診斷 1／4（config_shift）接上 evaluation pipeline。

    只驗接線，不驗計算——計算層的測試在 tests/test_diagnosis/。這裡要釘的是
    「它真的吃到共用的 diagnosis_sample」：各診斷共用同一份樣本是一致性
    保證（不同母體的數字並排解讀會錯），一旦哪天有人把 inputs 改成
    eval_predictions 自己重抽，數字看起來仍然合理，只是不再可比。
    """

    def test_config_shift_node_wired_after_diagnosis_sample(self):
        pipeline = create_pipeline(post_training=True)
        names = [n.name for n in pipeline.nodes]
        assert "diagnose_config_shift" in names
        assert (
            names.index("draw_diagnosis_sample_node")
            < names.index("diagnose_config_shift")
        )

    def test_config_shift_inputs_and_outputs(self):
        pipeline = create_pipeline(post_training=True)
        node = next(
            n for n in pipeline.nodes if n.name == "diagnose_config_shift"
        )
        assert node.inputs == ["diagnosis_sample", "parameters"]
        assert node.outputs == ["evaluation_config_shift"]

    def test_config_shift_not_wired_in_monitoring_mode(self):
        """它要 score_uncalibrated；監控模式不組（ADR-0018 決定 5）。"""
        pipeline = create_pipeline()
        assert "evaluation_config_shift" not in pipeline.outputs

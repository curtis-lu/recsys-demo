"""Tests for inference pipeline definition."""

from pathlib import Path

from recsys_tfb.pipelines.inference import create_pipeline


class TestInferencePipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 5

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {
            "inference_population", "feature_table",
            "preprocessor", "model", "parameters",
            # Written via `writes=` and read back by rank_predictions. Write
            # targets are not inputs; this one appears because of the read.
            "unranked_predictions",
        }

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "inference_population_features", "score_manifest",
            "score_chunk_report",
            "ranked_staging", "validated_predictions", "ranked_predictions",
        }
        assert pipeline.outputs == expected

    def test_the_chunk_record_lands_and_the_manifest_does_not(self):
        """Two outputs on one node, and the split between them is the design.

        ``score_manifest`` must stay absent from ``conf/base/catalog.yaml`` so
        it keeps auto-creating as a MemoryDataset: a landed one would let
        ``--from-node rank_predictions`` load a previous run's copy rather than
        re-running the scoring node, and ``validate_predictions`` reads its
        ``expected_partitions`` / ``written_partitions`` by value
        (docs/pipelines/inference.md section 7.4). ``score_chunk_report``
        carries the same lists with no consumer, so landing it is free of that
        (issue #195).
        """
        import yaml

        catalog = yaml.safe_load(
            (Path(__file__).resolve().parents[3] / "conf/base/catalog.yaml")
            .read_text()
        )
        assert "score_manifest" not in catalog
        assert catalog["score_chunk_report"]["type"] == "JSONDataset"

        pipeline = create_pipeline()
        by_name = {node.name: node for node in pipeline.nodes}
        assert by_name["predict_and_write_scores"].outputs == [
            "score_manifest", "score_chunk_report",
        ]
        # No node reads it: that absence is what keeps slicing untouched.
        assert not any(
            "score_chunk_report" in node.inputs for node in pipeline.nodes
        )

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "build_inference_population_features",
            "predict_and_write_scores",
            "rank_predictions",
            "validate_predictions",
            "publish_predictions",
        ]

    def test_the_dropped_intermediates_are_gone(self):
        """``scoring_dataset`` and ``X_score`` landed nowhere and nobody read them.

        Two DAG names whose only consumer was the next box: topology without
        information. The merge that removed them is ADR-0010 section 4, and this
        pins that they do not come back as a convenience.
        """
        pipeline = create_pipeline()
        names = {name for node in pipeline.nodes for name in node.outputs}
        assert "scoring_dataset" not in names
        assert "X_score" not in names

    def test_scoring_does_not_read_the_source_feature_table(self):
        """Audit 1 of ADR-0010, as a structural fact rather than a measurement.

        The scoring stage's full scans of ``feature_table`` must be zero. It
        cannot scan a table it is not given, so the absence of the input *is*
        the guarantee — and putting it back turns this red.
        """
        pipeline = create_pipeline()
        by_name = {node.name: node for node in pipeline.nodes}
        assert "feature_table" not in by_name["predict_and_write_scores"].inputs

    def test_the_intermediate_table_is_built_without_a_model(self):
        """Which is what keeps it reusable across ``model_version``.

        A node that could see the model could trim the stored columns to the
        subset that model wants, and nothing downstream would go red
        (ADR-0010 section 5). Not having the input closes the road.
        """
        pipeline = create_pipeline()
        by_name = {node.name: node for node in pipeline.nodes}
        builder = by_name["build_inference_population_features"]
        assert "model" not in builder.inputs
        assert builder.outputs == ["inference_population_features"]

    def test_scoring_declares_its_own_write(self):
        """The side effect is on the pipeline definition, not inside the body.

        Registered in R1 of docs/agents/architecture-constraints.md. The data
        reaches Hive through this declaration; the node's own outputs are
        bookkeeping (asserted in
        :meth:`test_the_chunk_record_lands_and_the_manifest_does_not`), never
        the scores.
        """
        pipeline = create_pipeline()
        by_name = {node.name: node for node in pipeline.nodes}
        scoring = by_name["predict_and_write_scores"]
        assert scoring.writes == ["unranked_predictions"]
        assert "unranked_predictions" not in scoring.outputs

    def test_the_write_target_is_bound_last_and_by_name(self):
        """The Runner binds write targets by keyword, inputs positionally.

        So the parameter name must equal the dataset name, and it must sit after
        every input parameter — a new input appended after it would land in the
        wrong slot.
        """
        import inspect

        from recsys_tfb.pipelines.inference.nodes import (
            predict_and_write_scores,
        )

        params = list(inspect.signature(predict_and_write_scores).parameters)
        assert params[-1] == "unranked_predictions"

    def test_ordering_survives_writes_not_being_a_dag_edge(self):
        """``writes=`` creates no topological edge, so the manifest must.

        Without a real input dependency, ``rank_predictions`` could be scheduled
        before the partitions it reads exist.
        """
        pipeline = create_pipeline()
        by_output = {out: n for n in pipeline.nodes for out in n.outputs}
        assert "score_manifest" in by_output["ranked_staging"].inputs
        order = [n.name for n in pipeline.nodes]
        assert order.index("predict_and_write_scores") < order.index("rank_predictions")

    def test_staging_validate_publish_chain(self):
        """rank 寫 staging、validate 讀 staging、publish 寫 production —— 證明
        production ranked_predictions 在驗證閘門的下游。"""
        pipeline = create_pipeline()
        by_output = {out: n for n in pipeline.nodes for out in n.outputs}
        # predict reads the preprocessor directly: the encoding of the identity
        # categoricals the builder deferred is applied per chunk, against a view
        # built from the model's own declaration (ADR-0011 §5).
        assert "preprocessor" in by_output["score_manifest"].inputs
        # rank_predictions: unranked_predictions -> ranked_staging
        assert by_output["ranked_staging"].name == "rank_predictions"
        assert "unranked_predictions" in by_output["ranked_staging"].inputs
        # validate_predictions: ranked_staging -> validated_predictions
        assert by_output["validated_predictions"].name == "validate_predictions"
        assert "ranked_staging" in by_output["validated_predictions"].inputs
        # publish_predictions: validated_predictions -> ranked_predictions
        assert by_output["ranked_predictions"].name == "publish_predictions"
        assert "validated_predictions" in by_output["ranked_predictions"].inputs

    def test_validate_no_longer_reads_a_re_derived_frame(self):
        """The old row-count check read ``scoring_dataset``, re-running its join.

        That input is gone twice over: the frame no longer exists, and the
        un-exploded replacement is ``len(items)`` times shorter than the ranked
        output so the comparison would fail on every correct run. The manifest
        carries the partition bookkeeping instead (ADR-0011 §3).
        """
        pipeline = create_pipeline()
        by_output = {out: n for n in pipeline.nodes for out in n.outputs}
        validate = by_output["validated_predictions"]
        assert "scoring_dataset" not in validate.inputs
        assert "inference_population_features" not in validate.inputs
        assert "score_manifest" in validate.inputs

    def test_publish_runs_after_validate(self):
        """拓樸順序保證 production 寫入發生在驗證閘門之後。"""
        pipeline = create_pipeline()
        order = [n.name for n in pipeline.nodes]
        assert order.index("rank_predictions") < order.index("validate_predictions")
        assert order.index("validate_predictions") < order.index("publish_predictions")

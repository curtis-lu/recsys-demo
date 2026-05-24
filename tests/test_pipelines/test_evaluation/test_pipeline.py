"""Tests for evaluation pipeline definition."""

from recsys_tfb.pipelines.evaluation import create_pipeline


class TestEvaluationPipelineDefault:
    """Default (post_training=False) — monitoring scenario."""

    def test_pipeline_has_five_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 5

    def test_pipeline_reads_ranked_predictions(self):
        pipeline = create_pipeline()
        assert "ranked_predictions" in pipeline.inputs
        assert "training_eval_predictions" not in pipeline.inputs

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "eval_predictions_persisted_sentinel",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "compute_metrics",
            "compute_baseline_metrics", "persist_eval_predictions",
            "generate_report",
        ]


class TestEvaluationPipelinePostTraining:
    """post_training=True — read from training_eval_predictions."""

    def test_pipeline_has_five_nodes(self):
        pipeline = create_pipeline(post_training=True)
        assert len(pipeline.nodes) == 5

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_same_as_default(self):
        pipeline = create_pipeline(post_training=True)
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
            "eval_predictions_persisted_sentinel",
        }
        assert pipeline.outputs == expected


class TestEvaluationPipelineCompareMode:
    """compare_source set — 8 nodes total, both reports produced."""

    def test_pipeline_has_eight_nodes(self):
        pipeline = create_pipeline(compare_source={"kind": "hive", "model_version": "v1"})
        assert len(pipeline.nodes) == 8

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
            "load_eval_predictions_from_hive",
            "load_compare_predictions",
            "restrict_to_common",
            "generate_comparison_report",
        ]

    def test_pipeline_inputs(self):
        pipeline = create_pipeline(compare_only=True)
        assert "label_table" in pipeline.inputs
        assert "parameters" in pipeline.inputs

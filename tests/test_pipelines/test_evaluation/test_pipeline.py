"""Tests for evaluation pipeline definition."""

from recsys_tfb.pipelines.evaluation import create_pipeline


class TestEvaluationPipelineDefault:
    """Default (post_training=False) — monitoring scenario."""

    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 4

    def test_pipeline_reads_ranked_predictions(self):
        pipeline = create_pipeline()
        assert "ranked_predictions" in pipeline.inputs
        assert "training_eval_predictions" not in pipeline.inputs

    def test_pipeline_outputs_unchanged(self):
        pipeline = create_pipeline()
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert names == [
            "prepare_eval_data", "compute_metrics",
            "compute_baseline_metrics", "generate_report",
        ]


class TestEvaluationPipelinePostTraining:
    """post_training=True — read from training_eval_predictions."""

    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline(post_training=True)
        assert len(pipeline.nodes) == 4

    def test_pipeline_reads_training_eval_predictions(self):
        pipeline = create_pipeline(post_training=True)
        assert "training_eval_predictions" in pipeline.inputs
        assert "ranked_predictions" not in pipeline.inputs

    def test_pipeline_outputs_same_as_default(self):
        pipeline = create_pipeline(post_training=True)
        expected = {
            "eval_predictions", "evaluation_metrics",
            "baseline_metrics", "evaluation_report",
        }
        assert pipeline.outputs == expected

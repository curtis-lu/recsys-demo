"""Tests for evaluation pipeline definition."""

from recsys_tfb.pipelines.evaluation import create_pipeline


class TestEvaluationPipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 3

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {
            "ranked_predictions",
            "label_table",
            "parameters",
            "baseline_metrics",
        }

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {"eval_predictions", "evaluation_metrics", "evaluation_report"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "prepare_eval_data" in names
        assert "compute_metrics" in names
        assert "generate_report" in names

    def test_spark_backend(self):
        pipeline = create_pipeline(backend="spark")
        assert len(pipeline.nodes) == 3

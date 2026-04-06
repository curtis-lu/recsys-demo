"""Tests for baselines pipeline definition."""

from recsys_tfb.pipelines.baselines import create_pipeline


class TestBaselinesPipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 2

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"label_table", "parameters"}

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {"baseline_predictions", "baseline_metrics"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "compute_baselines" in names
        assert "compute_baseline_metrics" in names

    def test_spark_backend(self):
        pipeline = create_pipeline(backend="spark")
        assert len(pipeline.nodes) == 2

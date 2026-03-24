"""Tests for inference pipeline definition."""

from recsys_tfb.pipelines.inference import create_pipeline


class TestInferencePipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 5

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "parameters", "preprocessor", "model"}

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {"scoring_dataset", "X_score", "score_table", "ranked_predictions", "validated_predictions"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "build_scoring_dataset" in names
        assert "apply_preprocessor" in names
        assert "predict_scores" in names
        assert "rank_predictions" in names
        assert "validate_predictions" in names

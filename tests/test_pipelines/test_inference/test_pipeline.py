"""Tests for inference pipeline definition."""

from recsys_tfb.pipelines.inference import create_pipeline


class TestInferencePipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 6

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "parameters", "preprocessor", "model"}

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "scoring_dataset", "X_score", "score_table",
            "ranked_staging", "validated_predictions", "ranked_predictions",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "build_scoring_dataset" in names
        assert "apply_preprocessor" in names
        assert "predict_scores" in names
        assert "rank_predictions" in names
        assert "validate_predictions" in names
        assert "publish_predictions" in names

    def test_staging_validate_publish_chain(self):
        """rank 寫 staging、validate 讀 staging、publish 寫 production —— 證明
        production ranked_predictions 在驗證閘門的下游。"""
        pipeline = create_pipeline()
        by_output = {out: n for n in pipeline.nodes for out in n.outputs}
        # rank_predictions: score_table -> ranked_staging
        assert by_output["ranked_staging"].name == "rank_predictions"
        assert "score_table" in by_output["ranked_staging"].inputs
        # validate_predictions: ranked_staging -> validated_predictions
        assert by_output["validated_predictions"].name == "validate_predictions"
        assert "ranked_staging" in by_output["validated_predictions"].inputs
        # publish_predictions: validated_predictions -> ranked_predictions
        assert by_output["ranked_predictions"].name == "publish_predictions"
        assert "validated_predictions" in by_output["ranked_predictions"].inputs

    def test_publish_runs_after_validate(self):
        """拓樸順序保證 production 寫入發生在驗證閘門之後。"""
        pipeline = create_pipeline()
        order = [n.name for n in pipeline.nodes]
        assert order.index("rank_predictions") < order.index("validate_predictions")
        assert order.index("validate_predictions") < order.index("publish_predictions")

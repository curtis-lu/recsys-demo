"""Inference pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.inference.nodes_spark import (
        apply_preprocessor,
        build_scoring_dataset,
        predict_scores,
        publish_predictions,
        rank_predictions,
        validate_predictions,
    )

    return Pipeline(
        [
            Node(
                build_scoring_dataset,
                inputs=["inference_population", "feature_table", "parameters"],
                outputs="scoring_dataset",
            ),
            Node(
                apply_preprocessor,
                inputs=["scoring_dataset", "preprocessor", "parameters"],
                outputs="X_score",
            ),
            Node(
                predict_scores,
                inputs=["model", "X_score", "scoring_dataset", "parameters"],
                outputs="score_table",
            ),
            Node(
                rank_predictions,
                inputs=["score_table", "parameters"],
                outputs="ranked_staging",
            ),
            Node(
                validate_predictions,
                inputs=["ranked_staging", "scoring_dataset", "parameters"],
                outputs="validated_predictions",
            ),
            Node(
                publish_predictions,
                inputs=["validated_predictions", "parameters"],
                outputs="ranked_predictions",
            ),
        ]
    )

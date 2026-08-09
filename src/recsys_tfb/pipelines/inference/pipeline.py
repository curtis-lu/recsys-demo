"""Inference pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.inference.nodes_spark import (
        build_inference_population_features,
        predict_and_write_scores,
        publish_predictions,
        rank_predictions,
        validate_predictions,
    )

    return Pipeline(
        [
            Node(
                build_inference_population_features,
                inputs=[
                    "inference_population", "feature_table",
                    "preprocessor", "parameters",
                ],
                outputs="inference_population_features",
            ),
            Node(
                predict_and_write_scores,
                inputs=["model", "inference_population_features",
                        "preprocessor", "parameters"],
                # Chunked save: this node writes unranked_predictions itself,
                # one partition per .save(). Registered in R1 of
                # docs/agents/architecture-constraints.md. The Runner binds
                # write targets BY KEYWORD, so the signature's parameter name
                # must stay `unranked_predictions` -- and a new optional input
                # must NOT be appended after it.
                writes=["unranked_predictions"],
                outputs="score_manifest",
            ),
            Node(
                rank_predictions,
                # score_manifest is an ordering-only dependency: `writes=`
                # creates no topological edge, so without it this node could be
                # scheduled before the partitions it reads exist.
                inputs=["unranked_predictions", "score_manifest", "parameters"],
                outputs="ranked_staging",
            ),
            Node(
                validate_predictions,
                inputs=["ranked_staging", "score_manifest", "parameters"],
                outputs="validated_predictions",
            ),
            Node(
                publish_predictions,
                inputs=["validated_predictions", "parameters"],
                outputs="ranked_predictions",
            ),
        ]
    )

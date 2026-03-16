"""Inference pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(backend: str = "pandas") -> Pipeline:
    if backend == "spark":
        from recsys_tfb.pipelines.inference.nodes_spark import (
            apply_preprocessor,
            build_scoring_dataset,
            predict_scores,
            rank_predictions,
        )
    else:
        from recsys_tfb.pipelines.inference.nodes_pandas import (
            apply_preprocessor,
            build_scoring_dataset,
            predict_scores,
            rank_predictions,
        )

    return Pipeline(
        [
            Node(
                build_scoring_dataset,
                inputs=["feature_table", "parameters"],
                outputs="scoring_dataset",
            ),
            Node(
                apply_preprocessor,
                inputs=["scoring_dataset", "preprocessor"],
                outputs="X_score",
            ),
            Node(
                predict_scores,
                inputs=["model", "X_score", "scoring_dataset"],
                outputs="score_table",
            ),
            Node(
                rank_predictions,
                inputs=["score_table", "parameters"],
                outputs="ranked_predictions",
            ),
        ]
    )

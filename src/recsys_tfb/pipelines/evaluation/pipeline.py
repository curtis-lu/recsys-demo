"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(post_training: bool = False) -> Pipeline:
    """Build the evaluation pipeline.

    Args:
        post_training: When True, read predictions from `training_eval_predictions`
            (post-training evaluation). When False (default), read from
            `ranked_predictions` (monthly monitoring). Mirrors
            training/pipeline.py::create_pipeline(enable_calibration=...).
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )

    return Pipeline(
        [
            Node(
                prepare_eval_data,
                inputs=[predictions_input, "label_table", "parameters"],
                outputs="eval_predictions",
            ),
            Node(
                compute_metrics,
                inputs=["eval_predictions", "parameters"],
                outputs="evaluation_metrics",
            ),
            Node(
                generate_report,
                inputs=[
                    "eval_predictions",
                    "evaluation_metrics",
                    "parameters",
                    "baseline_metrics",
                ],
                outputs="evaluation_report",
            ),
        ]
    )

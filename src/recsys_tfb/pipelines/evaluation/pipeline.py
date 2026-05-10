"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )

    return Pipeline(
        [
            Node(
                prepare_eval_data,
                inputs=["ranked_predictions", "label_table", "parameters"],
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

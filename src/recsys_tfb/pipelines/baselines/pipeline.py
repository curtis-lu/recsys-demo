"""Baselines pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(backend: str = "pandas") -> Pipeline:
    if backend == "spark":
        from recsys_tfb.pipelines.baselines.nodes_spark import (
            compute_baseline_metrics,
            compute_baselines,
        )
    else:
        from recsys_tfb.pipelines.baselines.nodes_pandas import (
            compute_baseline_metrics,
            compute_baselines,
        )

    return Pipeline(
        [
            Node(
                compute_baselines,
                inputs=["label_table", "parameters"],
                outputs="baseline_predictions",
            ),
            Node(
                compute_baseline_metrics,
                inputs=["baseline_predictions", "label_table", "parameters"],
                outputs="baseline_metrics",
            ),
        ]
    )

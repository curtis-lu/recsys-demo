"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(
    post_training: bool = False,
    compare_source: dict | None = None,
    compare_only: bool = False,
) -> Pipeline:
    """Build the evaluation pipeline.

    Modes:
      * default (no flags) — 4 metrics/report nodes + persist_eval_predictions
        (auto-saved via catalog to ``enriched_eval_predictions``
        HiveTableDataset).
      * --compare X — adds 3 compare nodes; both standalone and comparison
        reports produced.
      * --compare-only X — short pipeline that catalog-auto-loads the
        previously-persisted ``enriched_eval_predictions``, validates the
        partition (B4), and only produces report_comparison.html.
    """
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
        compute_baseline_metrics,
        compute_metrics,
        generate_report,
        prepare_eval_data,
    )
    from recsys_tfb.pipelines.evaluation.comparison_nodes import (
        generate_comparison_report,
        load_compare_predictions,
        persist_eval_predictions,
        restrict_to_common,
        validate_enriched_eval_predictions_present,
    )

    if compare_only:
        # CLI A12 ensures compare_source is not None when compare_only is True.
        # First node consumes "enriched_eval_predictions" — catalog auto-loads
        # via HiveTableDataset.load() with WHERE model_version=${model_version},
        # then validator filters to current snap_date and raises B4 if empty.
        return Pipeline([
            Node(
                validate_enriched_eval_predictions_present,
                inputs=["enriched_eval_predictions", "parameters"],
                outputs="eval_predictions",
            ),
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            Node(
                restrict_to_common,
                inputs=["eval_predictions", "compare_predictions_raw",
                        "label_table", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ])

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )
    nodes = [
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
            compute_baseline_metrics,
            inputs=["eval_predictions", "label_table", "parameters"],
            outputs="baseline_metrics",
        ),
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics"],
            outputs="evaluation_report",
        ),
        # persist returns the same DF as-is; framework auto-saves via catalog
        # entry "enriched_eval_predictions" (HiveTableDataset). Catalog
        # injects model_version partition column + dynamic-partition overwrite.
        Node(
            persist_eval_predictions,
            inputs=["eval_predictions"],
            outputs="enriched_eval_predictions",
        ),
    ]
    if compare_source is not None:
        nodes += [
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            Node(
                restrict_to_common,
                inputs=["eval_predictions", "compare_predictions_raw",
                        "label_table", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ]
    return Pipeline(nodes)

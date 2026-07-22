"""Evaluation pipeline definition."""

import importlib

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.diagnosis.metric.contract import DIAGNOSES, inputs_for


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
        compute_metric_ci,
        compute_metrics,
        compute_report_aggregates,
        draw_diagnosis_sample_node,
        generate_report,
        make_diagnosis_node,
        prepare_eval_data,
        render_diagnosis_pages,
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
        # Draw the driver-side diagnosis sample ONCE; compute_metric_ci and
        # the registry diagnoses read this shared in-memory output instead of
        # each re-drawing it (same seed -> identical content).
        Node(
            draw_diagnosis_sample_node,
            inputs=["eval_predictions", "parameters"],
            outputs="diagnosis_sample",
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
            compute_report_aggregates,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_report_aggregates",
        ),
        Node(
            compute_metric_ci,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_metric_ci",
        ),
        # 各診斷的 Node 全部由 registry 導出。手寫的話 Plan 2-5 會產生四份
        # 只差模組名的複製品，而它們會各自漂移（見 make_diagnosis_node）。
        # inputs 不是寫死的 ["diagnosis_sample", "parameters"]：每項診斷宣告
        # 自己的 INPUTS（contract.inputs_for），多數診斷沒宣告就落回吃共用
        # 抽樣的預設值。不吃抽樣的診斷（例如讀 gain_ledger 的
        # model_capacity）就此能宣告自己的 node inputs，不必讓每項診斷都收
        # 寬簽章。
        *[
            Node(
                make_diagnosis_node(name),
                inputs=list(inputs_for(
                    importlib.import_module(
                        f"recsys_tfb.diagnosis.metric.{name}"
                    )
                )),
                outputs=f"evaluation_{name}",
            )
            for name in DIAGNOSES
        ],
        # inputs 裡的診斷產物**只當依賴宣告**，node 本身按檔名讀（見
        # nodes_spark.render_diagnosis_pages 的 docstring）。列出它們是為了
        # (1) 讓拓撲排序把這個 node 排在所有診斷之後、(2) 讓 --only-node 的
        # 切片擴張在 JSON 不存在時能往上拉到診斷節點。
        Node(
            render_diagnosis_pages,
            inputs=["parameters",
                    *(f"evaluation_{name}" for name in DIAGNOSES)],
            outputs="evaluation_diagnosis_pages",
        ),
        Node(
            generate_report,
            inputs=["evaluation_metrics", "parameters", "baseline_metrics",
                    "evaluation_metric_ci",
                    "evaluation_report_aggregates",
                    "evaluation_diagnosis_pages"],
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

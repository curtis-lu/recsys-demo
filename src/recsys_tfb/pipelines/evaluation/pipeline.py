"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.diagnosis.metric.contract import DIAGNOSES


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
        compute_offset_sweep,
        compute_pair_ledger,
        diagnose_config_shift,
        draw_diagnosis_sample_node,
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
        # Draw the driver-side diagnosis sample ONCE; the three diagnosis
        # consumers below read this shared in-memory output instead of each
        # re-drawing it (same seed -> identical content).
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
            compute_metric_ci,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_metric_ci",
        ),
        Node(
            compute_offset_sweep,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_offset_sweep",
        ),
        Node(
            compute_pair_ledger,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_pair_ledger",
        ),
        # 診斷 1／5（registry: diagnosis.metric.contract.DIAGNOSES）。與上面
        # 三個舊診斷讀同一份 diagnosis_sample —— 五項診斷的數字要能並排解讀，
        # 就必須算在同一批列上。報表接線是下一個 task，這裡先落 JSON 產物。
        Node(
            diagnose_config_shift,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_config_shift",
        ),
        # registry 診斷的 catalog 名字由 DIAGNOSES 導出（``evaluation_<name>``
        # 是既有慣例），**不是手寫清單**：generate_report 用 varargs 依 registry
        # 順序收這些結果，手寫的清單一旦與 registry 錯位，頁面標題會接到別項
        # 診斷的數字上——而那是靜默的錯（每頁看起來都很正常）。
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics", "evaluation_metric_ci",
                    "evaluation_offset_sweep", "evaluation_pair_ledger",
                    *(f"evaluation_{name}" for name in DIAGNOSES)],
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

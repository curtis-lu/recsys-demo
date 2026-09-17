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
      * default (no flags) — monitoring: ``prepare_eval_data`` writes this
        month's partition of the ``enriched_eval_predictions`` Hive table, so
        the join is computed once, and every metrics/report node reads the
        table back, keeping the evaluated month (ADR-0018 decision 1). No
        registry diagnosis: ``no_diagnosis_pages`` supplies
        ``generate_report``'s pages input.
      * --post-training — same chain read from ``training_eval_predictions``,
        plus every registry diagnosis and ``render_diagnosis_pages``.
      * --compare X — adds 3 compare nodes to either of the above; both
        standalone and comparison reports produced.
      * --compare-only X — short pipeline that reads the
        ``enriched_eval_predictions`` an earlier run wrote, gates on the
        evaluated month being there (zero-output), and only produces
        report_comparison.html. It also loads ``evaluation_segment_columns``,
        landed by the run that wrote that partition, since there is no
        ``prepare_eval_data`` here to say which segment columns the
        partition's rows were joined with.
    """
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_baseline_metrics,
        compute_metric_ci,
        compute_metrics,
        compute_report_aggregates,
        generate_comparison_report,
        generate_report,
        load_compare_predictions,
        make_diagnosis_node,
        make_draw_diagnosis_sample_node,
        make_prepare_eval_data_node,
        make_restrict_to_common_node,
        no_diagnosis_pages,
        render_diagnosis_pages,
        validate_enriched_eval_predictions_present,
    )

    if compare_only:
        # CLI A12 ensures compare_source is not None when compare_only is True.
        # The catalog loads "enriched_eval_predictions" with WHERE
        # model_version=${model_version}; the gate raises when the evaluated
        # month has no rows and passes nothing on, so restrict_to_common reads
        # the table and keeps the month itself, like every reader.
        # Both A-side readers also take evaluation_segment_columns: it records
        # the settings and the joined segment columns the run that wrote this
        # directory stamped on its partitions, which they check each partition
        # against (#374). Only this mode takes the settings from that JSON;
        # see make_restrict_to_common_node.
        return Pipeline([
            Node(
                validate_enriched_eval_predictions_present,
                inputs=["enriched_eval_predictions",
                        "evaluation_segment_columns", "parameters"],
            ),
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            Node(
                make_restrict_to_common_node(compare_only=True),
                inputs=["enriched_eval_predictions", "compare_predictions_raw",
                        "evaluation_segment_columns", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "evaluation_segment_columns",
                        "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ])

    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )
    # Segment columns come from the table the evaluated rows were drawn from
    # (ADR-0020 bug 6): the test set is drawn from sample_pool, monitoring
    # scores inference_population.
    population_input = "sample_pool" if post_training else "inference_population"
    # Every node below that reads enriched_eval_predictions reads the Hive
    # table back (every month this model_version was evaluated on) and keeps
    # the evaluated month first; test_pipeline.py fails one that does not.
    nodes = [
        # The join is computed here once and lands as this month's partition;
        # dynamic partition overwrite replaces that partition only.
        Node(
            make_prepare_eval_data_node(population_input),
            inputs=[predictions_input, "label_table", population_input,
                    "parameters"],
            outputs=["enriched_eval_predictions", "evaluation_segment_columns"],
        ),
        # Draw the driver-side diagnosis sample ONCE; compute_metric_ci and,
        # in --post-training, the registry diagnoses read this shared
        # in-memory output instead of each re-drawing it (same seed ->
        # identical content). The node is told the mode so that monitoring
        # mode, which wires no registry diagnosis, never draws for them.
        Node(
            make_draw_diagnosis_sample_node(
                registry_diagnoses_wired=post_training
            ),
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters"],
            outputs="diagnosis_sample",
        ),
        Node(
            compute_metrics,
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters"],
            outputs="evaluation_metrics",
        ),
        Node(
            compute_baseline_metrics,
            inputs=["enriched_eval_predictions", "label_table",
                    "evaluation_segment_columns", "parameters"],
            outputs="baseline_metrics",
        ),
        Node(
            compute_report_aggregates,
            # evaluation_segment_columns for its joined list: part of the
            # fingerprint each partition read here must carry (#374).
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters"],
            outputs="evaluation_report_aggregates",
        ),
        Node(
            compute_metric_ci,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_metric_ci",
        ),
    ]
    if post_training:
        nodes += [
            # 各診斷的 Node 全部由 registry 導出。手寫的話 Plan 2-5 會產生四份
            # 只差模組名的複製品，而它們會各自漂移（見 make_diagnosis_node）。
            # inputs 不是寫死的 ["diagnosis_sample", "parameters"]：每項診斷
            # 宣告自己的 INPUTS（contract.inputs_for），多數診斷沒宣告就落回吃
            # 共用抽樣的預設值。不吃抽樣的診斷（例如讀 gain_ledger 的
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
            # These inputs are the values that get drawn: the node draws the
            # diagnosis results it is given, one per DIAGNOSES entry in
            # registry order, and checks each result's name and config
            # fingerprint before drawing (see its docstring). The same inputs
            # also place it after every diagnosis node and let --only-node
            # pull a diagnosis whose JSON is missing back in.
            Node(
                render_diagnosis_pages,
                inputs=["parameters",
                        *(f"evaluation_{name}" for name in DIAGNOSES)],
                outputs="evaluation_diagnosis_pages",
            ),
        ]
    else:
        # Monitoring mode wires no registry diagnosis (ADR-0018 decision 5):
        # they need score_uncalibrated, which this mode's prediction source
        # does not guarantee. The mode decides the shape (ADR-0013), not a
        # config switch. generate_report still needs its sixth input, so the
        # zero-read stub supplies an empty list; why a stub, see its docstring.
        nodes.append(
            Node(
                no_diagnosis_pages,
                inputs=["parameters"],
                outputs="evaluation_diagnosis_pages",
            )
        )
    nodes += [
        Node(
            generate_report,
            inputs=["evaluation_metrics", "parameters", "baseline_metrics",
                    "evaluation_metric_ci",
                    "evaluation_report_aggregates",
                    "evaluation_diagnosis_pages"],
            outputs="evaluation_report",
        ),
    ]
    if compare_source is not None:
        nodes += [
            Node(
                load_compare_predictions,
                inputs=["parameters"],
                outputs="compare_predictions_raw",
            ),
            # Checks partitions against today's settings, not the directory
            # JSON's: a --only-node generate_comparison_report slice reads
            # partitions an earlier run wrote, whose JSON is as old (#374).
            Node(
                make_restrict_to_common_node(compare_only=False),
                inputs=["enriched_eval_predictions", "compare_predictions_raw",
                        "evaluation_segment_columns", "parameters"],
                outputs=["eval_predictions_common", "compare_predictions_common",
                         "compare_coverage_partial"],
            ),
            Node(
                generate_comparison_report,
                inputs=["eval_predictions_common", "compare_predictions_common",
                        "compare_coverage_partial", "evaluation_segment_columns",
                        "parameters"],
                outputs="evaluation_comparison_report",
            ),
        ]
    return Pipeline(nodes)

"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(
    post_training: bool = False,
    compare_source: dict | None = None,
    compare_only: bool = False,
    baseline_rate: bool = False,
) -> Pipeline:
    """Build the evaluation pipeline.

    Modes:
      * default (no flags) — monitoring: ``prepare_eval_data`` writes this
        month's partition of the ``enriched_eval_predictions`` Hive table, so
        the join is computed once, and every metrics/report node reads the
        table back, keeping the evaluated month (ADR-0018 decision 1).
      * --post-training — same chain read from ``training_eval_predictions``.
      * --compare X — adds 3 compare nodes to either of the above; both
        standalone and comparison reports produced.
      * --compare-only X — short pipeline that reads the
        ``enriched_eval_predictions`` an earlier run wrote, gates on the
        evaluated month being there (zero-output), and only produces
        report_comparison.html. It also loads ``evaluation_segment_columns``,
        landed by the run that wrote that partition, since there is no
        ``prepare_eval_data`` here to say which segment columns the
        partition's rows were joined with, and ``evaluation_item_categories``
        (#379) for the category table it read — optional, see its catalog
        entry.

    ``baseline_rate`` (the CLI's ``baseline_scores_by_rate``, #397: ``score:
    rate`` under ``--post-training``) adds ``build_popularity_period_counts``
    and hands its table and month plan to ``compute_baseline_metrics`` as a
    seventh and eighth input. Off (the default, and always in monitoring mode),
    the pipeline is node for node the one without the feature: no extra node,
    and monitoring mode reads no ``sample_pool``.
    """
    from recsys_tfb.pipelines.evaluation.nodes import (
        build_popularity_period_counts,
        compute_baseline_metrics,
        compute_metrics,
        compute_prediction_quality,
        compute_report_aggregates,
        compute_row_facts,
        generate_comparison_report,
        generate_report,
        load_compare_predictions,
        make_prepare_eval_data_node,
        make_restrict_to_common_node,
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
                        "parameters", "evaluation_item_categories"],
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
        # dynamic partition overwrite replaces that partition only. The
        # category table (#379) is a third output rather than a node of its
        # own: a node between this one and compute_row_facts would move
        # compute_row_facts after compute_prediction_quality /
        # compute_report_aggregates, and `--from-node compute_row_facts`
        # would then leave those stale.
        # `preprocessor_on_disk` last (#379): with the item list counted from
        # the data, the evaluated model's list is read off it and landed for a
        # hand mapping to be checked against. Optional, so an evaluation that
        # needs no list never starts needing the file.
        Node(
            make_prepare_eval_data_node(population_input),
            inputs=[predictions_input, "label_table", population_input,
                    "parameters", "preprocessor_on_disk"],
            outputs=["enriched_eval_predictions", "evaluation_segment_columns",
                     "evaluation_item_categories"],
        ),
        # The facts of the evaluated rows (ADR-0031 decision 4), landed once
        # for the model's metrics, the baseline's and the report. Before
        # both metrics nodes, which count off it: it is the first
        # fingerprinted producer, where COMPUTED_KEYS sends a re-run.
        Node(
            compute_row_facts,
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters", "evaluation_item_categories"],
            outputs="evaluation_row_facts",
        ),
        Node(
            compute_metrics,
            inputs=["enriched_eval_predictions", "evaluation_row_facts",
                    "evaluation_segment_columns", "parameters",
                    "evaluation_item_categories"],
            outputs="evaluation_metrics",
        ),
    ]
    # Two literal Node calls rather than one with a computed input list, so
    # the architecture tests' static scan can still read both (A5/A6).
    if baseline_rate:
        nodes += [
            # Model-independent counts per time value (#397). Its output is
            # the Hive table itself: the node returns the periods its plan
            # names and the dynamic partition overwrite adds them;
            # compute_baseline_metrics reads the whole table back (A1). The
            # plan is also in the CLI's month_plans, so a slice pulls this
            # node back while a period is missing (ADR-0012).
            Node(
                build_popularity_period_counts,
                inputs=["sample_pool", "label_table",
                        "popularity_period_counts_month_plan", "parameters"],
                outputs="popularity_period_counts",
            ),
            Node(
                compute_baseline_metrics,
                # The plan again: the table keeps every period it ever
                # counted, the baseline sums the ones sample_pool holds now.
                # The row facts are second and the category table sixth in
                # both wirings (positional binding; see
                # compute_baseline_metrics).
                inputs=["enriched_eval_predictions", "evaluation_row_facts",
                        "label_table",
                        "evaluation_segment_columns", "parameters",
                        "evaluation_item_categories",
                        "popularity_period_counts",
                        "popularity_period_counts_month_plan"],
                outputs="baseline_metrics",
            ),
        ]
    else:
        nodes.append(
            Node(
                compute_baseline_metrics,
                inputs=["enriched_eval_predictions", "evaluation_row_facts",
                        "label_table",
                        "evaluation_segment_columns", "parameters",
                        "evaluation_item_categories"],
                outputs="baseline_metrics",
            )
        )
    nodes += [
        # Every evaluated row as a binary prediction (ADR-0024); a stub unless
        # report.sections.prediction_quality is on. It takes the segment JSON
        # for the partition check only, like compute_report_aggregates.
        Node(
            compute_prediction_quality,
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters"],
            outputs="prediction_quality_metrics",
        ),
        Node(
            compute_report_aggregates,
            # evaluation_segment_columns for its joined list: part of the
            # fingerprint each partition read here must carry (#374).
            inputs=["enriched_eval_predictions", "evaluation_segment_columns",
                    "parameters"],
            outputs="evaluation_report_aggregates",
        ),
    ]
    nodes += [
        # By name (ADR-0030 decision 8): the report keys each result by its
        # catalog name, so every parameter is named after its entry.
        Node(
            generate_report,
            inputs={
                "evaluation_row_facts": "evaluation_row_facts",
                "evaluation_metrics": "evaluation_metrics",
                "baseline_metrics": "baseline_metrics",
                "evaluation_report_aggregates": "evaluation_report_aggregates",
                "prediction_quality_metrics": "prediction_quality_metrics",
                "parameters": "parameters",
            },
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
                        "parameters", "evaluation_item_categories"],
                outputs="evaluation_comparison_report",
            ),
        ]
    return Pipeline(nodes)

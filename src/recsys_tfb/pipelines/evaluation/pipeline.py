"""Evaluation pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def _comparison_nodes(compare_only: bool, run_nodes: list[str]) -> list[Node]:
    """The comparison chain, one copy for ``--compare`` and ``--compare-only``
    (#536): B's rows aligned with A's, both sides' facts and metrics landed,
    compared, K cut, drawn. The mode reaches the nodes whose behaviour
    depends on it through their factories: which settings side A's
    partitions are checked against (``restrict_to_common``), and where a
    stale input's refusal sends the re-run (the readers, through
    ``run_nodes``: see :func:`create_pipeline`).

    Only ``compute_compare_metrics`` runs Spark after the alignment; the
    nodes after it read landed JSON, so ``--only-node
    generate_comparison_report`` redraws without recomputing (ADR-0031
    decision 5). The comparison lands every K and the cut comes after it
    (decision 16).
    """
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_compare_metrics,
        load_compare_predictions,
        make_compare_to_source_node,
        make_generate_comparison_report_node,
        make_resolve_comparison_display_ks_node,
        make_restrict_to_common_node,
    )

    return [
        Node(
            load_compare_predictions,
            inputs=["parameters"],
            outputs="compare_predictions_raw",
        ),
        # Under --compare checks side A's partitions against today's
        # settings, under --compare-only against the directory JSON's (#374;
        # see make_restrict_to_common_node). The coverage lands; the two
        # aligned frames stay in memory.
        Node(
            make_restrict_to_common_node(compare_only=compare_only),
            inputs=["enriched_eval_predictions", "compare_predictions_raw",
                    "evaluation_segment_columns", "parameters"],
            outputs=["eval_predictions_common", "compare_predictions_common",
                     "compare_coverage"],
        ),
        # By name: the two frames are both Spark frames, and swapped by
        # position they would raise nothing and relabel the two sides.
        Node(
            compute_compare_metrics,
            inputs={
                "eval_predictions_common": "eval_predictions_common",
                "compare_predictions_common": "compare_predictions_common",
                "segment_columns": "evaluation_segment_columns",
                "parameters": "parameters",
                "item_categories": "evaluation_item_categories",
            },
            outputs=["compare_row_facts_a", "compare_metrics_a",
                     "compare_row_facts_b", "compare_metrics_b"],
        ),
        Node(
            make_compare_to_source_node(run_nodes),
            inputs={
                "compare_metrics_a": "compare_metrics_a",
                "compare_metrics_b": "compare_metrics_b",
                "parameters": "parameters",
            },
            outputs="source_comparison",
        ),
        Node(
            make_resolve_comparison_display_ks_node(run_nodes),
            inputs={
                "source_comparison": "source_comparison",
                "parameters": "parameters",
            },
            outputs="comparison_display_ks",
        ),
        # By name (ADR-0030 decision 8), like generate_report: every
        # parameter is named after the entry it reads.
        Node(
            make_generate_comparison_report_node(run_nodes),
            inputs={
                "compare_coverage": "compare_coverage",
                "compare_row_facts_a": "compare_row_facts_a",
                "source_comparison": "source_comparison",
                "comparison_display_ks": "comparison_display_ks",
                "parameters": "parameters",
            },
            outputs="evaluation_comparison_report",
        ),
    ]


#: The catalog names of the two reports: what a run of each mode is for.
MAIN_REPORT = "evaluation_report"
COMPARISON_REPORT = "evaluation_comparison_report"


def required_results(
    parameters: dict, *, main_report: bool, comparison_report: bool,
) -> dict[str, tuple[str, ...]]:
    """The requirement list (ADR-0031 principle 3): for each report this run
    draws, the results it reads — the ``READS`` of every section switched on
    (``report_sections.shared.sections_on``), in page order, each once.

    It collects declarations and names no computation: which node produces a
    result is the pipeline's wiring (``create_pipeline`` keeps only the nodes
    these results need). So a switched-off section's computation is not in
    the pipeline, and removing a section from its report's list removes its
    computation with it, with nothing else to keep in step.
    """
    from recsys_tfb.evaluation import report_builder
    from recsys_tfb.evaluation.comparison import report as comparison
    from recsys_tfb.evaluation.report_sections.shared import sections_on

    def reads(sections) -> tuple[str, ...]:
        return tuple(dict.fromkeys(
            name for section in sections_on(sections, parameters)
            for name in section.READS))

    required = {}
    if main_report:
        required[MAIN_REPORT] = reads(report_builder.SECTIONS)
    if comparison_report:
        required[COMPARISON_REPORT] = reads(comparison.SECTIONS)
    return required


def create_pipeline(
    *,
    parameters: dict,
    post_training: bool = False,
    compare_source: dict | None = None,
    compare_only: bool = False,
    baseline_rate: bool = False,
) -> Pipeline:
    """Build the evaluation pipeline.

    ``parameters`` decides which computations are in it (ADR-0031 principle
    3): the requirement list (:func:`required_results`) says what the
    switched-on report sections read, ``generate_report`` is wired to
    exactly that, and only the nodes those results need are kept — a
    switched-off section's computation is not in the pipeline at all, not a
    node writing ``{"enabled": false}`` (#538).

    Modes:
      * default (no flags) — monitoring: ``prepare_eval_data`` writes this
        month's partition of the ``enriched_eval_predictions`` Hive table, so
        the join is computed once, and every metrics/report node reads the
        table back, keeping the evaluated month (ADR-0018 decision 1).
      * --post-training — same chain read from ``training_eval_predictions``.
      * --compare X — adds the comparison chain (``_comparison_nodes``) to
        either of the above; both standalone and comparison reports produced.
      * --compare-only X — short pipeline that reads the
        ``enriched_eval_predictions`` an earlier run wrote, gates on the
        evaluated month being there (zero-output), and runs the same
        comparison chain, producing only report_comparison.html. It also
        loads ``evaluation_segment_columns``, landed by the run that wrote
        that partition, since there is no ``prepare_eval_data`` here to say
        which segment columns the partition's rows were joined with, and
        ``evaluation_item_categories`` (#379) for the category table it read
        — optional, see its catalog entry.

    The readers that refuse a stale result and are built here are given the
    run's nodes in topological order, to
    name the re-run from (``config_fingerprint.rerun_node``): which nodes a
    run has follows the mode and the switches, so no fixed list names one
    that is surely there. The list is filled once the pipeline is cut, before
    any node runs; a slice reuses the nodes, so it keeps the full run's order,
    which is what ``--from-node`` is resolved against.

    ``baseline_rate`` (the CLI's ``baseline_scores_by_rate``, #397: ``score:
    rate`` under ``--post-training``) adds ``build_popularity_period_counts``
    and hands its table and month plan to ``compute_baseline_metrics`` as a
    seventh and eighth input. Off (the default, and always in monitoring mode),
    the pipeline is node for node the one without the feature: no extra node,
    and monitoring mode reads no ``sample_pool``. Like every node, the two
    are kept only while a switched-on section reads what they lead to.
    """
    from recsys_tfb.evaluation.report_sections.shared import BASELINE_COMPARISON
    from recsys_tfb.pipelines.evaluation.nodes import (
        build_popularity_period_counts,
        compare_to_baseline,
        compute_baseline_metrics,
        compute_metrics,
        compute_prediction_quality,
        compute_report_aggregates,
        compute_row_facts,
        make_generate_report_node,
        make_prepare_eval_data_node,
        resolve_display_ks,
        validate_enriched_eval_predictions_present,
    )

    run_nodes: list[str] = []
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
        # No section of the comparison report has a switch, so the
        # requirement list keeps every node here.
        pipeline = Pipeline([
            Node(
                validate_enriched_eval_predictions_present,
                inputs=["enriched_eval_predictions",
                        "evaluation_segment_columns", "parameters"],
            ),
            *_comparison_nodes(compare_only=True, run_nodes=run_nodes),
        ])
        run_nodes[:] = [n.name for n in pipeline.nodes]
        return pipeline

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
        # category table (#379) is a third output; where it comes from:
        # prepare_eval_data.
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
        # both metrics nodes, which count off it.
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
        # Every evaluated row as a binary prediction (ADR-0024); kept only
        # while the prediction-quality section is on (the requirement list).
        # It takes the segment JSON for the partition check only, like
        # compute_report_aggregates.
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
        # The comparison layer's first pair (ADR-0031 decisions 4 and 5): the
        # model against its baseline, cell by cell, every K — no Spark, both
        # inputs landed. The report reads the differences off it.
        Node(
            compare_to_baseline,
            inputs=["evaluation_metrics", "baseline_metrics", "parameters"],
            outputs="baseline_comparison",
        ),
    ]
    required = required_results(
        parameters, main_report=True,
        comparison_report=compare_source is not None)
    main_reads = required[MAIN_REPORT]
    # The K each table shows, cut after the comparison (decision 16): a
    # changed display K re-runs this node and the report, nothing above. It
    # reads the comparison for the baseline section's lists, which also keeps
    # it after the comparison in every topological order — while that
    # section is on. Off, there is no comparison to read, and reading it
    # would pull the baseline's Spark pass back in. Two literal Node calls
    # rather than one with a computed input dict, so the architecture tests'
    # static scan can still read both (A5/A6).
    if BASELINE_COMPARISON in main_reads:
        nodes.append(Node(
            resolve_display_ks,
            inputs={"evaluation_metrics": "evaluation_metrics",
                    "baseline_comparison": "baseline_comparison",
                    "parameters": "parameters"},
            outputs="evaluation_display_ks",
        ))
    else:
        nodes.append(Node(
            resolve_display_ks,
            inputs={"evaluation_metrics": "evaluation_metrics",
                    "parameters": "parameters"},
            outputs="evaluation_display_ks",
        ))
    # By name (ADR-0030 decision 8): the report keys each result by its
    # catalog name, so every result is passed under its entry's name — the
    # ones the switched-on sections read, and no other: a switched-off
    # section's leftover file from an older run is never loaded. Each is
    # checked against today's settings with its producer named as the node
    # to re-run from, read off the wiring above.
    producers = {out: node.name for node in nodes for out in node.outputs}
    unproduced = [name for name in main_reads if name not in producers]
    if unproduced:
        raise ValueError(
            f"The main report's switched-on sections read {unproduced}, which "
            f"no node of this pipeline produces: a section's READS names a "
            f"result nothing computes. Wire its producer here, or fix the "
            f"section's READS."
        )
    nodes.append(Node(
        make_generate_report_node(
            {name: producers[name] for name in main_reads}, run_nodes),
        inputs={name: name for name in (*main_reads, "parameters")},
        outputs=MAIN_REPORT,
    ))
    if compare_source is not None:
        nodes += _comparison_nodes(compare_only=False, run_nodes=run_nodes)
    # Decision — keep only the nodes the drawn reports need: what produces
    # the results they read, and what those read in turn. A node with no
    # output (a gate like --compare-only's) is never anyone's producer and
    # would be cut here: such a gate on this line must be added back after
    # the cut, as --compare-only's skips the cut altogether.
    pipeline = Pipeline(nodes).only_nodes_with_outputs(list(required))
    run_nodes[:] = [n.name for n in pipeline.nodes]
    return pipeline

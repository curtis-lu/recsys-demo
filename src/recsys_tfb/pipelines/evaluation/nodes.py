"""Evaluation pipeline nodes, for every run mode ``pipeline.py`` wires.

Every node body lives in this module. Some are built by factories here
rather than defined at top level: ``prepare_eval_data``,
``restrict_to_common``, and the three comparison readers whose pre-check
advice depends on the compare mode (``compare_to_source``,
``resolve_comparison_display_ks``, ``generate_comparison_report``). The
architecture audit's AST scan reads top-level definitions only, so it does
not see those nodes' definitions (ADR-0019 decision 2).

The library also holds ``baselines``, although only these nodes call its
Spark functions: the library holds a module by its role, not by who calls it
(ADR-0031 principle 6, the 2026-10-25 correction).
"""

import logging
from collections.abc import Mapping, Sequence
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import (
    PREDICTION_QUALITY_DEFAULTS,
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    DataConsistencyError,
    item_category_column,
    item_category_conflict_errors,
    item_list_counted_from_data,
    test_carries_zero_positive_group_weight,
)
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.baselines import (
    baseline_score,
    build_baseline_frame,
    compute_period_candidate_counts,
    compute_positive_rates,
    compute_purchase_counts,
    popularity_window,
    resolve_lookback_months,
    restrict_to_periods,
)
from recsys_tfb.evaluation.metrics_spark import (
    compute_all_metrics,
    compute_row_facts as _count_row_facts,
)
from recsys_tfb.evaluation.prediction_quality import (
    aggregate_score_bins,
    build_payload,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    CELLS,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    DISPLAY_METRICS_SECTION,
    LEDGER_RERUN_NODE,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    THIS,
    drop_all_positive_groups,
    resolved_all_k,
)
from recsys_tfb.pipelines.evaluation.report.comparison.main import (
    assemble_comparison_report,
)
from recsys_tfb.pipelines.evaluation.report.main import assemble_report
from recsys_tfb.pipelines.evaluation.steps.all_positive_accounts import (
    log_all_positive_accounts,
)
from recsys_tfb.pipelines.evaluation.steps.comparison_layer import (
    compare_metric_results,
    ledger_keys_missing,
    populations_that_differ,
)
from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
    COMPARE_SOURCE_KEY,
    compare_source_identity,
    compare_source_table,
    load_compare_predictions as _load_compare,
)
from recsys_tfb.pipelines.evaluation.steps.compare_universe import (
    trim_to_common_with_coverage,
)
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    LoadedArtifact,
    fingerprint,
    require_computed_with_current_config,
)
from recsys_tfb.pipelines.evaluation.steps.display_ks import (
    baseline_comparison_block,
    comparison_cut,
    configured_compare_k,
    configured_k_values,
    evaluation_metrics_block,
)
from recsys_tfb.pipelines.evaluation.steps.fingerprint_checks import (
    COMPARISON_RERUN_NODE,
    require_comparison_inputs_current,
    require_prepared_with_current_config,
    require_row_facts_current,
)
from recsys_tfb.pipelines.evaluation.steps.item_categories import (
    category_of_each_item,
    landed_category_mapping,
    read_item_category_rows,
)
from recsys_tfb.pipelines.evaluation.steps.labels import (
    count_duplicated_keys,
    labels_in_months_of,
    left_join_labels_missing_as_zero,
)
from recsys_tfb.pipelines.evaluation.steps.model_version_scope import (
    restrict_to_model_version,
)
from recsys_tfb.pipelines.evaluation.steps.popularity_counts import (
    monthly_purchase_counts,
    positive_rate_block,
    purchase_counts_per_item,
)
from recsys_tfb.pipelines.evaluation.steps.ranks import rank_within_query_group
from recsys_tfb.pipelines.evaluation.steps.report_aggregates import (
    aggregate_report_diagnostics,
)
from recsys_tfb.pipelines.evaluation.steps.segments import (
    join_segment_columns,
    join_segment_sources,
    warn_segment_columns_skipped,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    EvalPartitionsNotCurrentError,
    eval_snap_dates,
    eval_snap_dates_without_rows,
    restrict_to_current_eval_partitions,
    restrict_to_eval_snap_dates,
    snap_dates_in_frame,
    stamp_partition_fingerprint,
)
from recsys_tfb.pipelines.evaluation.steps.zero_positive_group_weights import (
    require_zero_positive_group_weights,
)
from recsys_tfb.preprocessing import preprocessor_item_values
from recsys_tfb.utils.item_columns import combine_item_columns
from recsys_tfb.utils.spark import get_or_create_spark_session

logger = logging.getLogger(__name__)


def make_prepare_eval_data_node(population_name: str):
    """Build ``prepare_eval_data`` for one run mode's population table.

    ``population_name`` is the catalog entry ``create_pipeline`` wires as the
    node's third input: ``sample_pool`` for ``--post-training`` (the test set
    is drawn from it), ``inference_population`` for monitoring (it is what was
    scored). The node receives only a DataFrame, so the name comes in here for
    the warning, the report, ``evaluation_segment_columns`` and the category
    column's table. The mode decides it (ADR-0013).
    """
    def prepare_eval_data(
        ranked_predictions: SparkDataFrame,
        label_table: SparkDataFrame,
        population: SparkDataFrame,
        parameters: dict,
        preprocessor_on_disk: Optional[dict] = None,
    ) -> tuple[SparkDataFrame, dict, dict]:
        """Join ranked predictions with labels and segment columns using Spark.

        Returns ``(eval_predictions, segments, item_categories)``. The frame
        lands as this month's ``enriched_eval_predictions`` partition, where
        the join is computed once and every reader reads it back (ADR-0018
        decision 1). ``segments`` and ``item_categories`` land as
        ``evaluation_segment_columns`` and ``evaluation_item_categories``.

        Pre-checks. Each fails because something before this node did not
        supply what it needs:

        * ``parameters['model_version']`` is set (``RuntimeError``) and
          ``evaluation.snap_date`` is set (``ValueError``). These two read
          settings only, so they are runtime backstops: the CLI resolves the
          version, the config gives the month.
        * The predictions hold rows for every configured date (``ValueError``
          naming the dates without rows): the upstream run scored them.
        * ``label_table`` has no duplicated identity key in those months
          (``ValueError``, with the number of duplicated keys; why it raises
          rather than deduplicating is written at the check).
        * With a multi-column item, ``label_table`` carries every item
          column and no column named ``item`` (B16, raised by
          ``combine_item_columns``).
        * In column mode: the population is ``sample_pool`` (``RuntimeError``;
          A51 refuses anything else at the CLI entry), it has the column
          (``ValueError``), and no item has two categories over the evaluated
          months (B18, ``DataConsistencyError``).
        """
        schema = get_schema(parameters)
        time_col = schema["time"]
        identity_cols = schema["identity_columns"]
        label_col = schema["label"]

        eval_params = parameters.get("evaluation", {})

        # Decision — a multi-column item is combined on read (ADR-0027), so
        # label_table joins the predictions on the same `item` they carry.
        # A single declared column comes back untouched.
        labels = combine_item_columns(label_table, schema, "label_table")

        # Decision — which model_version: the one __main__.py resolved via
        # core.versioning.resolve_model_version, never every version the table
        # holds. Chosen wrong, one month's candidates would carry several
        # models' scores and every rank would mix them, with nothing raising.
        model_version = parameters.get("model_version")
        if model_version is None:
            raise RuntimeError(
                "parameters['model_version'] missing. CLI should resolve via "
                "core.versioning.resolve_model_version before pipeline run."
            )
        ranked_predictions = restrict_to_model_version(
            logger, ranked_predictions, model_version)

        # Decision — which months: evaluation.snap_date, one date or several,
        # in both pipeline modes (this node serves monitoring and
        # --post-training), failing loud when it is unset or when any one date
        # has no rows. Chosen wrong, the run would silently evaluate the whole
        # table, or silently drop a month from a several-date evaluation.
        snap_dates = eval_snap_dates(parameters)
        # The dates as the messages below print them: one date alone, several
        # as their list.
        snap_date_text = snap_dates[0] if len(snap_dates) == 1 else snap_dates
        logger.info("Filtering predictions to snap_date=%s", snap_date_text)
        # Per date, not on the filtered frame as a whole: with several dates the
        # whole frame has rows as soon as one month does.
        missing_dates = eval_snap_dates_without_rows(ranked_predictions, parameters)
        if missing_dates:
            available = sorted(snap_dates_in_frame(ranked_predictions, time_col))
            asked = (
                f"evaluation.snap_date={snap_date_text!r}"
                if len(snap_dates) == 1 else
                f"{len(missing_dates)} of {len(snap_dates)} evaluation.snap_date "
                f"dates: {missing_dates}"
            )
            raise ValueError(
                f"No predictions found for {asked} "
                f"(model_version={model_version}). snap_dates present in "
                f"predictions: {available}"
            )
        ranked_predictions = restrict_to_eval_snap_dates(
            ranked_predictions, parameters
        )

        labels = labels_in_months_of(labels, ranked_predictions, time_col)

        # Pre-check (input): label_table holds at most one row per identity key in
        # the evaluated month. The LEFT JOIN below copies a prediction row once per
        # matching label row, so a duplicated key silently inflates that query's
        # candidate set and shifts every rank in it; no count or metric raises.
        # Not dropDuplicates: that picks one answer arbitrarily and makes the row
        # counts line up, which hides the problem better than leaving it (bug 10).
        # Not core/consistency.py: a user-defined source table's quality is not a
        # framework invariant — the same boundary as inference_population's
        # uniqueness, a deliberate exception to the rule that new consistency
        # invariants go into `core/consistency.py`.
        # Checked on `labels` after the month join, so it counts exactly the
        # rows about to be joined. The message carries counts only, never key
        # values (they are entity ids).
        n_duplicated_keys = count_duplicated_keys(labels, identity_cols)
        if n_duplicated_keys:
            raise ValueError(
                f"{n_duplicated_keys} duplicated label_table key(s) on "
                f"{identity_cols} at evaluation.snap_date={snap_date_text!r}. Each extra "
                f"row would copy its prediction row in the join with the "
                f"predictions, inflating that query's candidates and shifting its "
                f"ranks. Deduplicate label_table upstream; evaluation does not "
                f"pick one of the rows for you."
            )

        # Decision — whose label under --post-training: the one
        # training_eval_predictions stores alongside `score`, not label_table's.
        # It is exactly what the model's test mAP was scored against. Taking
        # label_table's instead, post-training metrics would disagree with
        # training's wherever label_table's answer for a key has changed since,
        # with no error. One side has to lose it anyway: the merge join below
        # keys on identity_cols only, so a `label` on both sides ->
        # AnalysisException: reference 'label' is ambiguous.
        if label_col in ranked_predictions.columns and label_col in labels.columns:
            labels = labels.drop(label_col)
            logger.info(
                "prepare_eval_data: predictions already carry '%s'; dropped it "
                "from the label_table side to avoid an ambiguous join column",
                label_col,
            )

        # Decision — how labels attach: LEFT JOIN, a missing label counts as 0.
        # It preserves every prediction row so per-customer ranking is over
        # the model's full candidate set regardless of whether label_table
        # covers that (cust, prod) pair. label_table's per-group cust_pool
        # semantics (cust must have ≥1 apply event in the group to appear) means
        # an INNER JOIN here would silently shrink each customer's rank set to
        # their per-group sub-products, collapsing baseline / mAP metrics to a
        # per-group framing the business model never asked for. Missing labels
        # are 0 ("not bought"), as in build_model_input (LEFT + COALESCE(0)).
        eval_predictions = left_join_labels_missing_as_zero(
            ranked_predictions, labels, identity_cols, label_col)

        # Decision — a missing rank is computed here, a present one is trusted.
        # When the predictions source is training_eval_predictions
        # (--post-training mode), `rank` is absent because the table does not
        # store it; with the ranked_predictions source it is present.
        # Today both ways give the same ranks: the tie-break is inference's. The
        # choice shows once one side's rule changes: recomputing would silently
        # replace the ranking inference published; trusting means a `rank`
        # column ever added to training_eval_predictions is used as is,
        # tie-break included, with no message (ADR-0014 records that hazard).
        rank_col = schema["rank"]
        if rank_col not in eval_predictions.columns:
            # Decision — ranked within the query group, where inference ranks
            # within the base key. The two are the same until an `occasion` is
            # declared (ADR-0025 decision 2); declared, this side is the right
            # one: this branch only runs on training_eval_predictions, whose
            # rows each belong to the occasion they were ranked in. Monitoring
            # mode, the one place the two would meet, is refused at the CLI
            # entry while the role is declared (A40).
            eval_predictions = rank_within_query_group(eval_predictions, schema)
            logger.info(
                "prepare_eval_data: injected '%s' column via rank_within_query "
                "(predictions source did not provide it)",
                rank_col,
            )

        # Decision — where each segment column comes from (ADR-0020 bug 6):
        # its evaluation.segment_sources override when one is configured,
        # otherwise this run mode's population table, the table the evaluated
        # rows were drawn from, keyed by the base key -- it is an entity-level
        # table, so it has no column for anything finer. Chosen wrong (one
        # fixed table for both modes), a monitoring run's
        # entities missing from that table fell into a segment named "None"
        # that entered the per-segment average with equal weight. Joined onto
        # the final eval table, not label_table, so the label side stays
        # minimal.
        segment_columns = list(eval_params.get("segment_columns", []) or [])
        configured = eval_params.get("segment_sources", {}) or {}
        overrides = {c: configured[c] for c in segment_columns if c in configured}
        # Decision — a column the population table lacks is skipped, not
        # raised: a monitoring population is a user-defined table, and one
        # missing segment column must not stop the monthly report. A typo in
        # segment_columns lands here too (A10 runs before Spark and cannot see
        # the table), which is why the warning and the report name both the
        # table and the column. Reading .columns touches the metastore only.
        population_columns = set(population.columns)
        missing = {c: population_name for c in segment_columns
                   if c not in overrides and c not in population_columns}
        warn_segment_columns_skipped(logger, missing, population_columns)
        from_population = [c for c in segment_columns
                           if c not in overrides and c not in missing]
        if from_population:
            eval_predictions = join_segment_columns(
                eval_predictions, population, schema["base_key_columns"],
                from_population, source_name=population_name,
            )
        if overrides:
            eval_predictions = join_segment_sources(eval_predictions, overrides)

        # Landed so every consumer groups by what was joined here, never by
        # the frame's columns: enriched_eval_predictions is shared by both run
        # modes, so a column the other mode joined sits in the frame all NULL.
        # Its fingerprint is what every reader of the partition inherits: the
        # segmenting readers check it before reading the table
        # (require_prepared_with_current_config), because a slice starting
        # after this node reads the landed partition instead of re-joining.
        # --compare-only does not compare it with today's settings:
        # --post-training is inert on that path, and that run has no
        # prepare_eval_data (config_fingerprint.
        # require_computed_with_current_config, run_nodes).
        joined = [c for c in segment_columns if c not in missing]
        segments = {
            "joined": joined,
            "sources": {c: overrides[c]["table"] if c in overrides
                        else population_name for c in joined},
            "missing": missing,
            "config_fingerprint": fingerprint(parameters, "prepare_eval_data"),
        }

        # Decision — every row carries the settings its partition is written
        # under, and the segment columns actually joined. One date's
        # partition is written by runs over different date sets, whose JSON
        # above lands in different directories, so only the rows can tell a
        # later reader which run wrote them last; and a column the population
        # lacked is skipped, so the settings alone do not say what was joined.
        eval_predictions = stamp_partition_fingerprint(
            eval_predictions, parameters, joined)

        # Decision — where each item's category comes from: in column
        # mode, the sample_pool column evaluation.item_categories.column
        # names; otherwise the readers take the hand-written mapping from
        # parameters, so the table is landed empty. Read here, off the
        # population this node already holds: changing the column re-runs
        # from here (config_fingerprint.APPLIED_SETTINGS).
        category_column = item_category_column(parameters)
        item_categories = {"column": category_column, "mapping": {}}
        if category_column is not None:
            # Pre-check (runtime backstop of A51, which refuses column mode
            # outside --post-training at the CLI entry).
            if population_name != "sample_pool":
                raise RuntimeError(
                    f"evaluation.item_categories.column={category_column!r} "
                    f"is read off sample_pool, but this run's population is "
                    f"{population_name!r}; the CLI refuses that (A51)."
                )
            # Pre-check (input): a column the table lacks is a raise, not a
            # skip like a segment column — without it there is no category
            # to compute at all.
            if category_column not in population.columns:
                raise ValueError(
                    f"evaluation.item_categories.column={category_column!r}: "
                    f"{population_name} has no such column (it has "
                    f"{sorted(population.columns)})."
                )
            # Decision — which rows say an item's category: the evaluated
            # months' only, never the train months', whose rows can say
            # something the evaluated rows no longer do.
            evaluated_pool = restrict_to_eval_snap_dates(population, parameters)
            category_rows = read_item_category_rows(
                evaluated_pool, schema, category_column, population_name)
            # Pre-check (input): an item's category does not change with
            # time, within a month or across the evaluated ones (B18): the
            # category pass joins the table on the item alone, so an item in
            # two categories would be counted in both.
            conflicts = item_category_conflict_errors(
                category_column, category_rows)
            if conflicts:
                raise DataConsistencyError("\n".join(conflicts))
            # Decision — a NULL category is ignored where another row names
            # the item's category; an item with only NULLs is its own
            # category, as unmapped: singleton makes it.
            item_categories["mapping"] = category_of_each_item(category_rows)
            logger.info(
                "item categories read off %s.%s: %d items in %d categories",
                population_name, category_column,
                len(item_categories["mapping"]),
                len(set(item_categories["mapping"].values())),
            )
        # Decision — with the item list counted from the data, the known items
        # a hand mapping is checked against are the evaluated model's: its
        # preprocessor's list (#379 decision 6), not a recount — the model has
        # codes for exactly those. Landed only in that mode, so a listed
        # deployment's file does not change. Read from the optional entry: an
        # evaluation that needs no list must not start needing the file.
        if item_list_counted_from_data(parameters):
            item_categories["known_items"] = (
                None if preprocessor_on_disk is None
                else preprocessor_item_values(preprocessor_on_disk, schema["item"])
            )
        item_categories["config_fingerprint"] = fingerprint(
            parameters, "prepare_eval_data")

        logger.info("Eval data prepared via Spark join")
        return eval_predictions, segments, item_categories

    return prepare_eval_data


def compute_row_facts(
    eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
    item_categories: Optional[dict] = None,
) -> dict:
    """The facts of the evaluated rows, whatever their scores (ADR-0031
    decision 4): the dataset overview and the query group counts, per grain
    (``evaluation.metrics_spark.compute_row_facts``), plus the segment columns
    ``prepare_eval_data`` joined.

    Lands as ``evaluation_row_facts`` (``row_facts.json``) and carries
    ``config_fingerprint``. One result for the model and its popularity
    baseline: the baseline re-scores these same rows, so ``compute_metrics``
    and ``compute_baseline_metrics`` both take it instead of counting again
    (ADR-0031 decision 8).

    Copies ``joined`` / ``sources`` / ``missing`` of ``segment_columns`` as
    ``segments``.

    Pre-checks (inputs): the landed segment list and category table were
    prepared under today's settings (``require_prepared_with_current_config``,
    ``landed_category_mapping``), and so was each evaluated date's partition
    (``restrict_to_current_eval_partitions``).

    Postcondition: every configured date was evaluated, i.e. the number of
    months in the facts equals the number of configured dates. The
    restriction leaves at most that many, so what this refuses is fewer, an
    empty or never-written partition for some date; reading a Hive table
    raises nothing for either. The dates are evaluated together, as one set of
    query groups. It does not catch a reader that forgot to restrict (that
    reader is another node); the AST test in ``test_pipeline.py`` does.
    """
    require_prepared_with_current_config(
        segment_columns, parameters, item_categories)
    category_mapping = landed_category_mapping(item_categories, parameters)
    # Decision — count the configured dates only, from partitions written
    # under today's settings: the table holds every month this model_version
    # was evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame
    facts = _count_row_facts(
        eval_predictions, parameters,
        segment_columns=segment_columns["joined"],
        category_mapping=category_mapping,
    )
    n_snap_dates = facts["dataset_overview"]["totals"]["n_snap_dates"]
    snap_dates = eval_snap_dates(parameters)
    if n_snap_dates != len(snap_dates):
        one = len(snap_dates) == 1
        raise ValueError(
            f"compute_row_facts postcondition: {n_snap_dates} evaluated months "
            f"in enriched_eval_predictions for evaluation.snap_date="
            f"{snap_dates[0] if one else snap_dates!r} "
            f"(model_version={parameters.get('model_version')!r}), expected "
            f"exactly {len(snap_dates)}. "
            + ("That month's partition is empty or was never written"
               if one else
               "Some of those months' partitions are empty or were never "
               "written")
            + "; re-run with --from-node prepare_eval_data."
        )
    facts["segments"] = {
        k: segment_columns[k] for k in ("joined", "sources", "missing")
    }
    facts["config_fingerprint"] = fingerprint(
        parameters, "compute_row_facts", (segment_columns, item_categories))
    log_all_positive_accounts(logger, facts, drop_all_positive_groups(parameters))
    return facts


def compute_metrics(
    eval_predictions: SparkDataFrame,
    row_facts: dict,
    segment_columns: dict,
    parameters: dict,
    item_categories: Optional[dict] = None,
) -> dict:
    """Compute ranking metrics using the Spark-native pipeline.

    Thin wrapper over `evaluation.metrics_spark.compute_all_metrics`. All
    row-level work stays in Spark; only small aggregated dicts are collected.

    Segments by what ``prepare_eval_data`` joined (``segment_columns``, the
    landed ``evaluation_segment_columns``). Reads the query group counts and
    the item count off ``row_facts`` (``compute_row_facts``, landed just
    before), counted on these same rows; the result keeps none of them, only
    the population ledger of what it was computed on
    (``metrics.POPULATION_KEY``).

    The result lands as ``evaluation_metrics`` (``metrics.json``, ADR-0018
    decision 2) and carries ``config_fingerprint``.

    ``evaluation.query_filter.drop_all_positive_groups`` decides whether the
    all-positive query groups leave the metrics; how many there are is the row
    facts' to count and log.

    The category pass ranks by ``item_categories`` (the landed
    ``evaluation_item_categories``) in column mode, by the hand-written
    mapping in ``parameters`` otherwise (``landed_category_mapping``).

    Pre-checks (inputs): the landed segment list, category table and row facts
    were prepared under today's settings (``require_prepared_with_current_config``,
    ``landed_category_mapping``, ``require_row_facts_current``), and so was
    each evaluated date's partition (``restrict_to_current_eval_partitions``).
    That the row facts were counted on the same partitions is not checked:
    re-running this node alone after the partitions were rewritten under the
    same settings reads facts of rows that are gone (``--from-node
    compute_row_facts`` re-counts them).
    """
    require_prepared_with_current_config(
        segment_columns, parameters, item_categories)
    require_row_facts_current(row_facts, parameters)
    category_mapping = landed_category_mapping(item_categories, parameters)
    # Decision — evaluate the configured dates only, from partitions written
    # under today's settings: the table holds every month this model_version
    # was evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    # Decision — drop the all-positive query groups (the same positive label on
    # every row, so every per-query metric is the same whatever the order)
    # only when evaluation.query_filter.drop_all_positive_groups says so.
    # Read here and passed down, never inside metrics_spark: that module also
    # holds training's test scoring (compute_untruncated_ap), which must not
    # follow the switch.
    result = compute_all_metrics(
        eval_predictions, parameters, row_facts,
        segment_columns=segment_columns["joined"],
        drop_all_positive_groups=drop_all_positive_groups(parameters),
        category_mapping=category_mapping,
    )
    result["config_fingerprint"] = fingerprint(
        parameters, "compute_metrics",
        (segment_columns, item_categories, row_facts))
    logger.info(
        "Spark metrics computed: %d query groups scored, all resolves to %d",
        result[POPULATION_KEY]["n_scored_queries_with_positive"],
        result[POPULATION_KEY][ALL_K_KEY],
    )
    return result


def build_popularity_period_counts(
    sample_pool: SparkDataFrame,
    label_table: SparkDataFrame,
    popularity_period_counts_month_plan,
    parameters: dict,
) -> SparkDataFrame:
    """The periods of ``popularity_period_counts`` this run has to count.

    Wired only when the positive-rate baseline is (``baseline_scores_by_rate``:
    ``score: rate`` under ``--post-training``).
    ``popularity_period_counts_month_plan`` is the CLI's ``SnapDatePlan`` over the
    time values sample_pool holds in the lookback windows: ``to_process`` are
    the ones not landed yet plus those named by ``--rebuild-dates``. Only they
    are returned, and the catalog entry's dynamic partition overwrite replaces
    exactly those partitions (A1: the plan is an input, the new periods the
    output); ``compute_baseline_metrics`` reads the table back whole.

    The counts depend on sample_pool, label_table and the schema only, so the
    table is not partitioned by ``model_version``: another model, or the same
    dates evaluated again, recounts nothing.
    """
    plan = popularity_period_counts_month_plan
    periods = [d.strftime("%Y-%m-%d") for d in plan.to_process]
    logger.info(
        "[months] popularity_period_counts counted=%s skipped=%s",
        ",".join(periods) or "-",
        ",".join(d.strftime("%Y-%m-%d") for d in plan.skipped) or "-",
    )
    # Decision — a multi-column item is combined on read (ADR-0027): the
    # counts are per item, and both tables are the user's own.
    schema = get_schema(parameters)
    return compute_period_candidate_counts(
        combine_item_columns(sample_pool, schema, "sample_pool"),
        combine_item_columns(label_table, schema, "label_table"),
        periods, parameters)


def compute_baseline_metrics(
    eval_predictions: SparkDataFrame,
    row_facts: dict,
    label_table: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
    item_categories: Optional[dict] = None,
    popularity_period_counts: Optional[SparkDataFrame] = None,
    popularity_period_counts_month_plan=None,
) -> dict:
    """Popularity-baseline metrics, aligned row-for-row with eval_predictions.

    Re-scores each eval_predictions row with the product's historical
    purchase count — or, with ``evaluation.baseline.score: rate`` under
    ``--post-training``, its positive rate read off
    ``popularity_period_counts``, restricted to the periods the month plan
    lists (both wired as the seventh and eighth input only then) — then
    computes the model's metrics on it as ``compute_metrics`` does (ADR-0031
    decision 8): the same slices, the same macro. ``row_facts`` is the
    model's: the baseline re-scores those rows one for one, so it counts
    nothing of its own and produces no row facts.
    ``row_facts`` is the second input and ``item_categories`` the sixth in
    both wirings, not appended after the two rate inputs: inputs bind by
    position, and appended they would take ``popularity_period_counts``' slot
    in the wiring without them (``known-pitfalls.md`` §12). Which score ranks
    is the wiring's call (ADR-0013): monitoring with ``score: rate`` gets no
    counts and ranks by the count.
    In the pipeline only while the popularity-baseline section is on
    (ADR-0031 principle 3); it reads no switch itself.

    Pre-checks (inputs): the landed segment list, category
    table, row facts and each evaluated date's partition were prepared under
    today's settings (``require_prepared_with_current_config``,
    ``require_row_facts_current``, ``landed_category_mapping``,
    ``restrict_to_current_eval_partitions``).
    """
    require_prepared_with_current_config(
        segment_columns, parameters, item_categories)
    require_row_facts_current(row_facts, parameters)
    category_mapping = landed_category_mapping(item_categories, parameters)
    # Decision — score the evaluated dates only, from partitions written under
    # today's settings: the table holds every month this model_version was
    # evaluated on, and the lookback windows below are anchored on the months
    # found in the frame.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    schema = get_schema(parameters)
    time_col = schema["time"]
    # Decision — a multi-column item is combined on read (ADR-0027): every
    # count below is per item.
    label_table = combine_item_columns(label_table, schema, "label_table")
    # Single source of the default (bug 1, ADR-0020): the report's
    # popularity-baseline section must not default differently from this node,
    # or a run relying on the implicit default silently disagrees with itself
    # about what it used.
    lookback_months = resolve_lookback_months(parameters)

    snap_dates = snap_dates_in_frame(eval_predictions, time_col)
    # Decision — which score ranks the baseline: the positive rate when the
    # pipeline wired the period counts (score: rate under --post-training),
    # the count otherwise. Rate first, so an empty sample_pool window raises
    # with its own message before label_table's window is looked at.
    rate_mode = popularity_period_counts is not None
    if (not rate_mode and baseline_score(parameters) == "rate"
            and parameters.get("post_training") is True):
        raise RuntimeError(
            "evaluation.baseline.score is 'rate' under --post-training but "
            "compute_baseline_metrics got no popularity_period_counts: "
            "create_pipeline wires it when baseline_rate is set."
        )
    rate_scores = None
    if rate_mode:
        popularity_period_counts = restrict_to_periods(
            popularity_period_counts,
            [d.strftime("%Y-%m-%d") for d in (
                *popularity_period_counts_month_plan.to_process,
                *popularity_period_counts_month_plan.skipped)],
            parameters,
        )
        rate_scores = compute_positive_rates(
            popularity_period_counts, snap_dates, lookback_months, parameters)
    counts = compute_purchase_counts(
        label_table, snap_dates, lookback_months, parameters
    )
    # Summed over the evaluated dates' windows; one date reduces to its own.
    purchase_counts = purchase_counts_per_item(counts, parameters)
    # The same windows per calendar month.
    monthly_counts, window_months_covered = monthly_purchase_counts(
        label_table, snap_dates, lookback_months, parameters)
    baseline_frame = build_baseline_frame(
        eval_predictions, rate_scores if rate_mode else counts, parameters)
    # Decision — the model's metrics on the baseline's scores, called as
    # compute_metrics calls them (ADR-0031 decision 8): the same segment and
    # category slices, the same macro, and the all-positive query groups
    # dropped exactly when the model's are. The report reads the baseline
    # against the model, number for number.
    metrics = compute_all_metrics(
        baseline_frame, parameters, row_facts,
        segment_columns=segment_columns["joined"],
        drop_all_positive_groups=drop_all_positive_groups(parameters),
        category_mapping=category_mapping,
    )
    metrics["purchase_counts"] = purchase_counts
    metrics["monthly_counts"] = monthly_counts
    if rate_mode:
        metrics["popularity_rate"] = positive_rate_block(
            popularity_period_counts, snap_dates, lookback_months, parameters)
    # Decision — land the window as the report prints it: over the
    # table it prints (sample_pool's monthly positives in rate mode,
    # label_table's monthly counts otherwise), with each window's covered
    # months where they were counted per window.
    rate_block = metrics.get("popularity_rate")
    metrics[POPULARITY_WINDOW_KEY] = popularity_window(
        rate_block["monthly_positives"] if rate_block else monthly_counts,
        (rate_block["window_months_covered"] if rate_block
         else window_months_covered),
        snap_dates=snap_dates, lookback_months=lookback_months,
        score=baseline_score(parameters))
    metrics["config_fingerprint"] = fingerprint(
        parameters, "compute_baseline_metrics",
        (segment_columns, item_categories, row_facts))
    logger.info(
        "Baseline metrics computed for snap_dates=%s; "
        "purchase_counts has %d products, monthly_counts spans %d months",
        snap_dates, len(purchase_counts),
        len({mo for per in monthly_counts.values() for mo in per}),
    )
    return metrics


def compute_prediction_quality(
    eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> dict:
    """The prediction-quality family: every evaluated row as one binary
    prediction, binned by score (ADR-0024).

    Lands as ``prediction_quality_metrics`` (``prediction_quality.json``).
    What each number means and cannot mean is in
    ``evaluation/prediction_quality.py``'s docstring.

    In the pipeline only while ``evaluation.report.sections.prediction_quality``
    is true (ADR-0031 principle 3): the family costs one extra shuffle over
    every evaluated row, and the ranking deployments this framework ships
    with do not read it (ADR-0024 decision 1).

    Pre-check (inputs): each evaluated date's partition was written under
    today's settings (``restrict_to_current_eval_partitions``). It takes
    ``evaluation_segment_columns`` for that check's ``joined`` list only; it
    does not segment, so it does not compare that JSON's fingerprint
    (``compute_report_aggregates``' reason). Its result still inherits that
    JSON's record.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    # Decision — the evaluated dates only, from partitions written under
    # today's settings: the table holds every month this model_version was
    # evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    label_col = schema["label"]
    settings = {
        **PREDICTION_QUALITY_DEFAULTS,
        **(eval_params.get("prediction_quality") or {}),
    }
    # Decision — every row, query groups without a positive included: nothing
    # here filters on `total_rel`, unlike the ranking metrics, because a binary
    # metric computed only where someone responded is systematically larger
    # (ADR-0024 decision 3). Under --post-training the test table holds only
    # the share of those groups dataset.test_zero_positive_group_ratio kept
    # (filter_test_keys); A46 refuses the run when it kept none.
    #
    # Decision — under --post-training with that ratio above 0, every row
    # weighs its zero-positive group weight: a kept zero-positive group stands
    # for 1/r of them (ADR-0025 decision 3). Decided from the config, not from
    # whether the column is there: the table is `columns: "auto"`, so once any
    # run added the column, a partition written under ratio 0 holds it as
    # NULL, and summing NULL weights would drop those rows in silence.
    # Monitoring reads offline inference's output, which no dataset draw
    # touched: every row weighs 1.
    #
    # Decision — bins are global and equal-width over this run's own
    # min..max score, not a fixed [0, 1]: the scores crowd the low end and
    # [0, 1] bins would put nearly every row in the first few. Equal width keeps per-item bins summing to the overall ones
    # and fine bins merging into display bins; quantile bins would break both
    # silently (ADR-0024 decision 4).
    #
    # Decision — per-item bins for the `top_n` items with the most rows only
    # (ties by item): item x bin rows come back to the driver, and an ad
    # deployment can have thousands of items (ADR-0024 decision 6). Every
    # item still counts in the overall bins and in the item totals.
    weight_col = (
        ZERO_POSITIVE_GROUP_WEIGHT_COL
        if parameters.get("post_training")
        and test_carries_zero_positive_group_weight(parameters)
        else None
    )
    # Pre-check (input) — the rows must carry the weight the config promises.
    # The config says what *today's* dataset settings keep; the predictions
    # were written by whichever model --model-version (or `best`) names, whose
    # test ratio is baked into its model_version and may differ. A missing
    # column, or a NULL weight (training writes NULL when that model's test
    # kept no zero-positive group), means the two disagree: summing NULL
    # weights would drop those rows, and counting them would report a filtered
    # table as a weighted one. One narrow scan of one column, only when
    # weighting at all.
    if weight_col is not None:
        require_zero_positive_group_weights(eval_predictions, weight_col)
    aggregated = aggregate_score_bins(
        eval_predictions.select(
            item_col, score_col, label_col, *([weight_col] if weight_col else [])),
        item_col=item_col, score_col=score_col, label_col=label_col,
        n_bins=settings["n_bins"], top_n=settings["top_n"],
        weight_col=weight_col,
    )
    out = build_payload(
        aggregated, item_col=item_col, score_col=score_col,
        label_col=label_col, weight_col=weight_col,
        n_bins=settings["n_bins"],
        n_display_bins=settings["n_display_bins"],
        top_n=settings["top_n"],
    )
    out["config_fingerprint"] = fingerprint(
        parameters, "compute_prediction_quality", (segment_columns,))
    overall = out["overall"]["summary"] or {}
    logger.info(
        "Prediction quality computed: %s rows, %s positives, pr_auc=%s, "
        "roc_auc=%s, bin width=%s; %d of %d items listed",
        overall.get("n"), overall.get("n_pos"), overall.get("pr_auc"),
        overall.get("roc_auc"), out["bins"]["width"],
        len(out["per_item"]["listed"]), out["per_item"]["n_items"],
    )
    return out


def compute_report_aggregates(
    eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> dict:
    """The Spark aggregates of the main report's diagnostics, landed as JSON.

    A node of its own so ``generate_report`` stays pure and ``--only-node
    generate_report`` redraws without recomputing (ADR-0018 decision 2); a
    failure of these scans surfaces here, not in the report node.

    In the pipeline only while the rank-distribution section is on
    (ADR-0031 principle 3).

    Reads ``enriched_eval_predictions`` but does not compare the
    ``segment_columns.json`` fingerprint with today's settings: it does not
    segment. It takes that JSON for its ``joined`` list only, which with
    today's settings makes the partition fingerprint each evaluated date must
    carry (``restrict_to_current_eval_partitions``), so a slice running only
    this node on a stale partition raises (ADR-0020 bug 6). Its result
    inherits that JSON's record, so a later switch of the run mode makes it
    stale too.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}

    # Decision — aggregate the evaluated dates only, from partitions written
    # under today's settings: the table holds every month this model_version
    # was evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    rank_col, label_col = schema["rank"], schema["label"]
    needed = list(dict.fromkeys([item_col, score_col, rank_col, label_col]))
    # Each family is an action of its own: uncached, each rescans the table.
    sdf = eval_predictions.select(*needed).cache()
    try:
        out = aggregate_report_diagnostics(
            sdf, item_col=item_col, score_col=score_col,
            rank_col=rank_col, label_col=label_col,
            include_distributions=diag_cfg.get("include_distributions", True),
        )
    finally:
        sdf.unpersist()
    logger.info("report aggregates computed: %s", sorted(out))
    out["config_fingerprint"] = fingerprint(
        parameters, "compute_report_aggregates", (segment_columns,))
    return out


def compare_to_baseline(
    evaluation_metrics: dict,
    baseline_metrics: dict,
    parameters: dict,
) -> dict:
    """The model's metrics against its popularity baseline's, cell by cell:
    the comparison layer's first pair (ADR-0031 decisions 4 and 5). Lands as
    ``baseline_comparison`` (``baseline_comparison.json``); the report reads
    the difference off it instead of taking it itself. No Spark: both inputs
    are landed JSON.

    Every K the metrics hold is compared; the K a table shows is cut later,
    by ``resolve_display_ks`` (decision 16), so a change of display K never
    re-runs this node.

    Carries ``config_fingerprint`` like every landed result: it applies no
    setting itself, and records what both inputs were computed with. In the
    pipeline only while the popularity-baseline section is on (ADR-0031
    principle 3), like the baseline's metrics it reads.

    Pre-checks (inputs):

    * both results were computed under today's settings
      (``require_computed_with_current_config``) — unchecked, ``--only-node
      compare_to_baseline`` after a setting changed would compare the old
      metrics and stamp today's fingerprint on the result, which the report
      then accepts, exit code 0;
    * both carry the population ledger the layer reads
      (``comparison_layer.ledger_keys_missing``); a bundle missing a key was
      landed before the key existed. The advice is ``LEDGER_RERUN_NODE``.
    """
    require_computed_with_current_config([
        LoadedArtifact(catalog_name="evaluation_metrics",
                       payload=evaluation_metrics,
                       produced_by="compute_metrics"),
        LoadedArtifact(catalog_name="baseline_metrics",
                       payload=baseline_metrics,
                       produced_by="compute_baseline_metrics"),
    ], parameters)
    missing = [f"{name} ({key})"
               for name, bundle in (("evaluation_metrics", evaluation_metrics),
                                    ("baseline_metrics", baseline_metrics))
               for key in ledger_keys_missing(bundle)]
    if missing:
        raise ValueError(
            f"compare_to_baseline: the population ledger lacks "
            f"{'; '.join(missing)} — landed before #536. Re-compute — "
            f"`python -m recsys_tfb evaluation --from-node "
            f"{LEDGER_RERUN_NODE}`, "
            f"with the flags of the run that wrote them (e.g. "
            f"--post-training) — then re-draw."
        )
    # Decision — compare the model with the baseline scored on its own rows,
    # at every grain both computed, every K included; which cells a table
    # shows is resolve_display_ks' to cut.
    comparison = compare_metric_results(evaluation_metrics, baseline_metrics)
    comparison["config_fingerprint"] = fingerprint(
        parameters, "compare_to_baseline", (evaluation_metrics, baseline_metrics))
    # The baseline re-scores the model's rows and counts off their row facts,
    # so the two agree on all four annotations by construction (decision 4,
    # correction); one that does not is a bug upstream, and the report draws
    # no mark for it — say it here.
    differing = populations_that_differ(comparison)
    if differing:
        logger.warning(
            "The model and its baseline were computed on different "
            "populations (%s); the report compares them cell by cell all the "
            "same.", ", ".join(differing))
    logger.info(
        "Compared the model with its baseline: %d overall keys, %d items, "
        "%d segments%s",
        len(comparison["overall"][CELLS]),
        len(comparison["per_item"][CELLS]),
        len(comparison["per_segment"][CELLS]),
        ", and the category grain" if comparison["category"] else "",
    )
    return comparison


def resolve_display_ks(
    evaluation_metrics: dict,
    parameters: dict,
    *,
    baseline_comparison: Optional[dict] = None,
) -> dict:
    """The K columns each table of the main report shows, cut at what
    ``"all"`` stands for in the result it is drawn from (bug 8, ADR-0020;
    ADR-0031 decision 10: the cut is a computation, so it lands). Lands as
    ``evaluation_display_ks`` (``display_ks.json``, shape in
    ``steps/display_ks``); the report reads each list by name and cuts
    nothing. No Spark.

    After the comparison on purpose (decision 16): the comparison lands
    every K, so a change of display K re-runs this node and the report only
    — ``--from-node resolve_display_ks``, what the fingerprint check
    advises. Its fingerprint carries the display key it reads
    (``evaluation.report.display.compare_k``), so it alone goes stale when
    that changes; ``generate_report`` then refuses to draw. Unfingerprinted,
    ``--only-node generate_report`` would draw the old columns under the new
    setting, exit code 0. It also records the popularity-baseline switch,
    which decides whether it is given a comparison to cut
    (``config_fingerprint.APPLIED_SETTINGS``).

    ``baseline_comparison`` is wired only while the popularity-baseline
    section is on (ADR-0031 principle 3); without it there is no baseline
    block to cut, and the result's ``baseline_comparison`` is ``None``. Keyword-only: a call passing
    ``(metrics, comparison, parameters)`` must fail rather than read the
    comparison as the settings (``known-pitfalls.md`` §12).

    Pre-checks (inputs): the results it reads were computed under today's
    settings (``require_computed_with_current_config``), and the
    metrics carry the ledger the cut reads ``"all"`` off
    (``comparison_layer.ledger_keys_missing``, the comparison's own check,
    repeated here because without the baseline section no comparison runs
    it; re-run from ``compute_row_facts``, why in ``compare_to_baseline``).
    """
    loaded = [LoadedArtifact(catalog_name="evaluation_metrics",
                             payload=evaluation_metrics,
                             produced_by="compute_metrics")]
    if baseline_comparison is not None:
        loaded.append(LoadedArtifact(catalog_name="baseline_comparison",
                                     payload=baseline_comparison,
                                     produced_by="compare_to_baseline"))
    require_computed_with_current_config(loaded, parameters)
    missing = ledger_keys_missing(evaluation_metrics)
    if missing:
        raise ValueError(
            f"resolve_display_ks: evaluation_metrics' population ledger lacks "
            f"{'; '.join(missing)} — landed before #536. Re-compute — "
            f"`python -m recsys_tfb evaluation --from-node "
            f"{LEDGER_RERUN_NODE}`, "
            f"with the flags of the run that wrote it (e.g. --post-training) "
            f"— then re-draw."
        )
    compare_k = configured_compare_k(parameters)
    # Decision — the overview and the metrics section draw off the model's
    # metrics, so each grain's lists are cut at that grain's "all" there; the
    # metrics section shows the K the run computed (k_values ∪ "all").
    category = evaluation_metrics.get("category")
    model_block = evaluation_metrics_block(
        resolved_all_k(evaluation_metrics),
        resolved_all_k(category) if category else None,
        configured_k_values(parameters))
    # Decision — the popularity-baseline section draws off the comparison,
    # every table at compare_k, and both of its sides at the model's K: the
    # baseline re-scores the model's rows, so its "all" is the same number
    # (the comparison's all_k annotation says so). None while the section is
    # off: nothing is drawn.
    baseline_block = None
    if baseline_comparison is not None:
        compared_category = baseline_comparison["category"]
        baseline_block = baseline_comparison_block(
            baseline_comparison[POPULATION_KEY][ALL_K_KEY][THIS],
            (compared_category[POPULATION_KEY][ALL_K_KEY][THIS]
             if compared_category else None),
            compare_k)
    out = {"evaluation_metrics": model_block,
           "baseline_comparison": baseline_block,
           "config_fingerprint": fingerprint(
               parameters, "resolve_display_ks",
               (evaluation_metrics, baseline_comparison))}
    logger.info(
        "Display K resolved: metrics section %s (all = %d), baseline "
        "section %s",
        model_block[DISPLAY_METRICS_SECTION], model_block[DISPLAY_ALL_K],
        baseline_block[DISPLAY_COMPARE_K] if baseline_block else "off")
    return out


def make_generate_report_node(
    producers: Mapping[str, str], run_nodes: Optional[Sequence[str]] = None,
):
    """The main report's node, for the results this run's switched-on
    sections read: ``producers`` maps each to the node that lands it.
    ``run_nodes`` is the run's nodes in topological order, which a stale
    result's re-run is chosen from (``config_fingerprint.rerun_node``).

    Built per run because what it reads is (ADR-0031 principle 3): a
    switched-off section's result is neither computed nor loaded, so no
    ``{"enabled": false}`` stub stands in for it, and a file an older run
    left behind for it is never read.
    """

    def generate_report(parameters: dict, **results: dict) -> str:
        """Build the HTML report from the landed results, wired by name
        (ADR-0030 decision 8): each keyword is a catalog entry, and only the
        entries the switched-on sections read are passed. The
        rank-distribution section reads the already-aggregated Spark JSON
        from ``compute_report_aggregates`` so this function stays pure — no
        SparkDataFrame in the signature, no Spark action in the body. The
        report takes no difference and cuts no K itself (ADR-0031
        principle 1): the differences are ``baseline_comparison``'s and every
        table's K columns ``evaluation_display_ks``'.

        Pre-check (inputs): every result was computed with today's values
        of the settings its step and its inputs applied
        (``steps.config_fingerprint``). All of them are landed JSON
        (ADR-0018 decision 2), and ``--only-node generate_report`` stops at
        "the JSON exists", so without this a setting changed since the last
        run is drawn from the old JSON with exit code 0 (ADR-0020 bug 2);
        with it the run raises, naming the key and the node to
        ``--from-node``: ``resolve_display_ks`` when only the display key it
        reads moved. The other drawn keys still redraw without recomputing
        anything.
        """
        loaded = [
            LoadedArtifact(catalog_name=name, payload=payload,
                           produced_by=producers[name])
            for name, payload in results.items()
        ]
        require_computed_with_current_config(
            loaded, parameters, run_nodes=run_nodes)
        # Decision — hand the report one bundle of results keyed by catalog
        # name (ADR-0031 principle 1): each section takes the names it
        # declares, so a new result reaches its section without passing
        # through every signature on the way. The bundle lives only here;
        # each result already landed.
        return assemble_report(results, parameters)

    return generate_report


def load_compare_predictions(parameters: dict) -> SparkDataFrame:
    """Pipeline shim: resolve a SparkSession and dispatch to source loader."""
    spark = get_or_create_spark_session()
    return _load_compare(parameters, spark)


def make_restrict_to_common_node(compare_only: bool):
    """Build ``restrict_to_common`` for ``--compare`` or ``--compare-only``.

    The mode decides which settings side A's partitions are checked against
    (``restrict_to_current_eval_partitions``), so ``create_pipeline``
    passes it in, as it does for ``make_prepare_eval_data_node``:

    * ``--compare``: today's settings, like every other reader. The same run
      normally wrote the partitions, but a slice does not:
      ``--compare X --from-node compute_compare_metrics`` (where a changed
      compare source sends the re-run) pulls back only
      ``load_compare_predictions`` and this node, so after a segment setting
      changed or the run mode switched, the partitions and the directory's
      ``segment_columns.json`` both still say the old settings. Compared
      with that JSON, the old rows would pass.
    * ``--compare-only``: the settings that JSON records. ``--post-training``
      is inert on that path, so today's value of it says nothing about which
      population an earlier standard run wrote (ADR-0020 bug 6); compared
      with today's settings, a post-training run's
      partitions would be refused whenever the flag is left off.
    """
    def restrict_to_common(
        eval_predictions: SparkDataFrame,
        compare_predictions_raw: SparkDataFrame,
        segment_columns: dict,
        parameters: dict,
    ) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
        """Keep side A's evaluated dates, after checking its partitions (see
        ``make_restrict_to_common_node`` for against which settings), then
        restrict both sides to their common universe
        (``trim_to_common_with_coverage``). Side B is the compared run's own
        table and is not checked."""
        # Decision — compare the evaluated dates only, from partitions written
        # under the settings this mode reads them for: the table holds every
        # month this model_version was evaluated on.
        eval_predictions = restrict_to_current_eval_partitions(
            eval_predictions, parameters, segment_columns,
            recorded_settings=compare_only,
        ).frame
        return trim_to_common_with_coverage(
            eval_predictions, compare_predictions_raw, segment_columns,
            parameters)

    return restrict_to_common


#: The ``--compare`` shape, checked against today's settings.
restrict_to_common = make_restrict_to_common_node(compare_only=False)


def compute_compare_metrics(
    eval_predictions_common: SparkDataFrame,
    compare_predictions_common: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
    item_categories: Optional[dict] = None,
) -> tuple[dict, dict, dict, dict]:
    """The row facts and the metrics of both sides of a comparison, on their
    common universe (``restrict_to_common``): ``(row facts A′, metrics A′,
    row facts B′, metrics B′)``, landed as ``compare_row_facts_a``,
    ``compare_metrics_a``, ``compare_row_facts_b``, ``compare_metrics_b``
    (ADR-0031 decision 4). Each carries ``config_fingerprint`` and the
    compare source it was computed against (``COMPARE_SOURCE_KEY``).

    Each side's metrics count off that side's own row facts: the two frames
    are not the same rows, so the run's landed ``evaluation_row_facts`` fit
    neither.

    Both sides segment by the same columns, and B′ takes A′'s values for
    them (a segment column is the entity's attribute, not something the
    scoring model produced). Computed without them, B′ would have no
    per-segment numbers to compare and its ledger's ``segment_columns`` would
    always differ from A′'s.

    Pre-check (inputs): in column mode, the category table was landed and
    read off today's column (``landed_category_mapping``). The frames are
    ``restrict_to_common``'s, which checked side A's partitions.
    """
    category_mapping = landed_category_mapping(item_categories, parameters)
    # Decision — both sides drop the all-positive query groups, or neither,
    # per evaluation.query_filter.drop_all_positive_groups, read once.
    # Each side decides which of its groups are all-positive on its own
    # rows, so the Δ can still compare different sets of groups: one group's
    # candidate rows can differ between the frames, all-positive on one side
    # (dropped) and mixed on the other (kept). The report says so.
    drop_all_positive = drop_all_positive_groups(parameters)
    stamp = {"config_fingerprint": fingerprint(
                 parameters, "compute_compare_metrics",
                 (segment_columns, item_categories)),
             COMPARE_SOURCE_KEY: compare_source_identity(parameters)}
    landed = []
    # Decision — both sides segment by what prepare_eval_data joined (under
    # --compare-only the copy landed next to the partition): this run's
    # frame can hold a column the other run mode joined, all NULL, so the
    # frame's columns are not asked (ADR-0020 bug 6).
    segments = segment_columns["joined"]
    # Decision — the compared side takes this run's side's segment values,
    # one per query group, joined on the query group (owner, 2026-10-23): a
    # segment is the entity's attribute, whoever scored it. Its own copy of a
    # column, if its table carries one, is dropped first (another run, other
    # settings). restrict_to_common kept only the query groups both sides
    # hold, so every compared row finds one; the time is matched as the
    # label join in trim_to_common_universe matches it.
    compared = compare_predictions_common
    if segments:
        compared = join_segment_columns(
            compare_predictions_common, eval_predictions_common,
            get_schema(parameters)["query_group_columns"], segments,
            source_name="this run's side of the comparison",
        )
    # Both sides' category passes rank by one table: in column mode the one
    # this run's side landed (the compared side holds only the common
    # items, so it covers them), otherwise the hand-written mapping.
    for frame in (eval_predictions_common, compared):
        facts = _count_row_facts(
            frame, parameters, segment_columns=segments,
            category_mapping=category_mapping)
        metrics = compute_all_metrics(
            frame, parameters, facts, segment_columns=segments,
            drop_all_positive_groups=drop_all_positive,
            category_mapping=category_mapping,
        )
        landed += [{**facts, **stamp}, {**metrics, **stamp}]
    row_facts_a, metrics_a, row_facts_b, metrics_b = landed
    logger.info(
        "Comparison metrics computed: %d / %d query groups scored, all "
        "resolves to %d / %d (this run / compared source)",
        metrics_a[POPULATION_KEY]["n_scored_queries_with_positive"],
        metrics_b[POPULATION_KEY]["n_scored_queries_with_positive"],
        metrics_a[POPULATION_KEY][ALL_K_KEY],
        metrics_b[POPULATION_KEY][ALL_K_KEY],
    )
    return row_facts_a, metrics_a, row_facts_b, metrics_b


def make_compare_to_source_node(run_nodes: Optional[Sequence[str]] = None):
    """Build ``compare_to_source`` for one run: ``run_nodes`` (the run's
    nodes in topological order) decides only where its pre-check sends a
    re-run, and which inherited records it compares
    (``require_comparison_inputs_current``). Built without one, it uses the
    declarations' order and compares every record."""
    def compare_to_source(
        compare_metrics_a: dict,
        compare_metrics_b: dict,
        parameters: dict,
    ) -> dict:
        """This run's side against the compared source, cell by cell, every
        K: the comparison layer's second pair (ADR-0031 decisions 4 and 5),
        landed as ``source_comparison``. The comparison report reads the
        difference off it instead of taking it itself. No Spark.

        Carries ``config_fingerprint``, the compare source it was computed
        against (``COMPARE_SOURCE_KEY``), and the four annotations
        (``population``, decision 16). Which K a table shows is cut later
        (``resolve_comparison_display_ks``).

        Pre-check (inputs): both results were computed under today's
        settings and against today's source
        (``require_comparison_inputs_current``). No ledger check:
        ``compute_compare_metrics`` lands both with the full ledger.
        """
        require_comparison_inputs_current([
            LoadedArtifact(catalog_name="compare_metrics_a",
                           payload=compare_metrics_a,
                           produced_by=COMPARISON_RERUN_NODE),
            LoadedArtifact(catalog_name="compare_metrics_b",
                           payload=compare_metrics_b,
                           produced_by=COMPARISON_RERUN_NODE),
        ], parameters, run_nodes)
        # Decision — compare the two sides on their common universe, at every
        # grain both computed, every K included.
        comparison = compare_metric_results(compare_metrics_a, compare_metrics_b)
        comparison["config_fingerprint"] = fingerprint(
            parameters, "compare_to_source",
            (compare_metrics_a, compare_metrics_b))
        comparison[COMPARE_SOURCE_KEY] = compare_source_identity(parameters)
        # Unlike the baseline, the two sides need not agree: each keeps its
        # own rows (with event rows their "all" can differ). Their segment
        # columns are the same (compute_compare_metrics gives the compared
        # side this run's), so a difference there is a wiring error worth the
        # log line.
        differing = populations_that_differ(comparison)
        logger.info(
            "Compared this run with the compared source: %d overall keys, %d "
            "items%s; populations differ in: %s",
            len(comparison["overall"][CELLS]),
            len(comparison["per_item"][CELLS]),
            ", and the category grain" if comparison["category"] else "",
            ", ".join(differing) or "nothing",
        )
        return comparison

    return compare_to_source


def make_resolve_comparison_display_ks_node(
    run_nodes: Optional[Sequence[str]] = None,
):
    """Build ``resolve_comparison_display_ks`` for one run; ``run_nodes`` as
    for ``make_compare_to_source_node``."""
    def resolve_comparison_display_ks(
        source_comparison: dict,
        parameters: dict,
    ) -> dict:
        """The K columns every per-K table of the comparison report shows,
        at each grain cut at what this run's side's ``"all"`` stands for (bug
        8, ADR-0020; ADR-0031 decision 10), landed as
        ``comparison_display_ks``. After the comparison (decision 16): a
        changed display K re-runs this node and the report only. No Spark.

        Its fingerprint carries the display key it reads
        (``evaluation.report.display.compare_k``), so it alone goes stale
        when that changes. The re-run goes to the first node of the run
        applying it (``config_fingerprint.rerun_node``): ``resolve_display_ks``
        when the run has the main report's cut, this node otherwise. Also
        carries the compare source (``COMPARE_SOURCE_KEY``).

        Pre-check (inputs): the comparison was computed under today's
        settings and against today's source
        (``require_comparison_inputs_current``).
        """
        require_comparison_inputs_current([
            LoadedArtifact(catalog_name="source_comparison",
                           payload=source_comparison,
                           produced_by="compare_to_source"),
        ], parameters, run_nodes)
        compare_k = configured_compare_k(parameters)

        # Decision — every per-K table of a grain takes compare_k, cut at
        # this run's side's "all" only (steps/display_ks.comparison_cut says
        # why one bound): the per-item ones (map_attr, recall; decision 16
        # item 7) and whatever the section lists name. No table prints
        # every key, so no per-side cut of keys either.
        def cut(grain: dict) -> dict:
            return comparison_cut(
                grain[POPULATION_KEY][ALL_K_KEY][THIS], compare_k)

        category = source_comparison["category"]
        out = {**cut(source_comparison),
               "category": None if category is None else cut(category),
               "config_fingerprint": fingerprint(
                   parameters, "resolve_comparison_display_ks",
                   (source_comparison,)),
               COMPARE_SOURCE_KEY: compare_source_identity(parameters)}
        logger.info(
            "Comparison report K resolved: %s (all = @%d)%s",
            out[DISPLAY_COMPARE_K], out[DISPLAY_ALL_K],
            "" if out["category"] is None else
            f", category {out['category'][DISPLAY_COMPARE_K]}")
        return out

    return resolve_comparison_display_ks


def make_generate_comparison_report_node(
    run_nodes: Optional[Sequence[str]] = None,
):
    """Build ``generate_comparison_report`` for one run; ``run_nodes`` as
    for ``make_compare_to_source_node``."""
    def generate_comparison_report(
        compare_coverage: dict,
        compare_row_facts_a: dict,
        source_comparison: dict,
        comparison_display_ks: dict,
        parameters: dict,
    ) -> str:
        """Build ``report_comparison.html`` from the landed comparison
        results, wired by name (ADR-0030 decision 8): each parameter is named
        after its catalog entry. No Spark frame in the signature, no Spark
        in the body (ADR-0031 principle 1): the coverage, this run's side's
        row facts, the comparison and the K cut are all landed JSON, so a
        redraw — ``--only-node generate_comparison_report``, in either
        compare mode — computes nothing.

        Pre-check (inputs): every input was computed under today's values of
        the settings its step and its inputs applied, and against today's
        compare source
        (``require_comparison_inputs_current``). ``label`` is not part of
        that source: the report reads it off today's parameters, so a
        renamed source redraws.
        """
        loaded = [
            LoadedArtifact(catalog_name="compare_coverage",
                           payload=compare_coverage,
                           produced_by="restrict_to_common"),
            LoadedArtifact(catalog_name="compare_row_facts_a",
                           payload=compare_row_facts_a,
                           produced_by=COMPARISON_RERUN_NODE),
            LoadedArtifact(catalog_name="source_comparison",
                           payload=source_comparison,
                           produced_by="compare_to_source"),
            LoadedArtifact(catalog_name="comparison_display_ks",
                           payload=comparison_display_ks,
                           produced_by="resolve_comparison_display_ks"),
        ]
        require_comparison_inputs_current(loaded, parameters, run_nodes)
        return assemble_comparison_report(
            {artifact.catalog_name: artifact.payload for artifact in loaded},
            parameters,
        )

    return generate_comparison_report


def validate_enriched_eval_predictions_present(
    enriched_eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> None:
    """Fail loud if ``enriched_eval_predictions`` holds no rows for an
    evaluated date under this ``model_version``, or rows another run wrote.

    A zero-output gate: it passes nothing on. The catalog has already pruned
    the table to this ``model_version`` via ``partition_filter``; this node
    asserts each evaluated date has a row, otherwise raises
    ``DataConsistencyError`` naming the dates without rows and what to run
    first. Per date: with several dates configured, the kept rows are not
    empty as soon as one month has rows.

    It also refuses a date whose partition was rewritten since by a run under
    other settings, or with other segment columns joined, than
    ``segment_columns`` records (the JSON the run that wrote this directory
    landed; why that JSON and not today's settings, see
    ``make_restrict_to_common_node``), and a date whose partition carries no
    fingerprint. Both answers come from one Spark job
    (``restrict_to_current_eval_partitions``), and every problem of every kind
    is named in one ``DataConsistencyError``: dates without rows first, then
    the partitions not written for this directory.

    Slicing never pulls a zero-output node back in (R3 in
    ``docs/agents/architecture-constraints.md``), so ``--compare-only
    --from-node load_compare_predictions`` skips it. That does not open a
    missing-partition hole: the CLI checks the partition listing before any
    node runs (``__main__.py::_compare_only_input_errors``). What only this
    gate sees is a partition that is listed but holds no rows.

    In the other modes ``prepare_eval_data`` writes the partition earlier in
    the same run, so this gate cannot fire.

    Its raise is a pre-check (input): an earlier run was to write those rows,
    and nothing in this run computed them.
    """
    mv = parameters.get("model_version", "unknown")
    # The table the catalog read, not the entry name (conf/dev prefixes it).
    table = compare_source_table(parameters, "enriched_eval_predictions")

    # Collect-all: a partition not written for this directory must not hide
    # the dates without rows from the same check, nor the other way round.
    not_current = None
    try:
        missing = restrict_to_current_eval_partitions(
            enriched_eval_predictions, parameters, segment_columns,
            recorded_settings=True,
        ).dates_without_rows
    except EvalPartitionsNotCurrentError as error:
        not_current, missing = error, error.dates_without_rows
    problems = []
    if missing:
        asked = (
            f"evaluation.snap_date={missing[0]!r}"
            if len(eval_snap_dates(parameters)) == 1 else
            f"evaluation.snap_date date(s) {missing}"
        )
        problems.append(
            f"{table} has no partition "
            f"for {asked} "
            f"model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without "
            "--compare) first to populate the partition."
        )
    if not_current is not None:
        problems.append(str(not_current))
    if problems:
        raise DataConsistencyError("\n".join(problems)) from not_current

"""Evaluation pipeline nodes, for every run mode ``pipeline.py`` wires
(monitoring, ``--post-training``, ``--compare``, ``--compare-only``).

Every node body lives in this module. Four kinds are built by factories here
rather than defined at top level: ``prepare_eval_data``
(``make_prepare_eval_data_node``), ``draw_diagnosis_sample_node``
(``make_draw_diagnosis_sample_node``), ``restrict_to_common``
(``make_restrict_to_common_node``, #374) and one ``diagnose_<name>`` per
``contract.DIAGNOSES`` entry (``make_diagnosis_node``). The architecture
audit's AST scan reads top-level definitions only, so it does not see those
nodes' definitions (ADR-0019 decision 2).

The mechanisms these nodes call come from two places: ``steps/``, which nothing
outside this pipeline imports, and the ``recsys_tfb.evaluation`` library. The
library also holds ``baselines``, although only these nodes call its Spark
functions, because the shared report builder imports it (ADR-0019, correction
to the caller facts).
"""

import importlib
import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.date_ranges import as_date_list, dates_label
from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric import contract
from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci
from recsys_tfb.evaluation.baselines import (
    build_baseline_frame,
    compute_monthly_purchase_counts,
    compute_monthly_purchase_counts_by_window,
    compute_purchase_counts,
    resolve_lookback_months,
)
from recsys_tfb.evaluation.compare import build_comparison_result
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report
from recsys_tfb.evaluation.diagnostics_spark import aggregate_report_diagnostics
from recsys_tfb.evaluation.metrics_spark import (
    compute_all_metrics,
    compute_overall_per_item,
    rank_within_query,
)
from recsys_tfb.evaluation.report_builder import (
    assemble_diagnosis_pages,
    assemble_report,
)
from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
    load_compare_predictions as _load_compare,
)
from recsys_tfb.pipelines.evaluation.steps.compare_universe import (
    query_groups_with_text_time,
    restrict_to_common as _restrict,
)
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    PARTITION_CONTENT_KEYS,
    LoadedArtifact,
    fingerprint,
    require_computed_with_current_config,
)
from recsys_tfb.pipelines.evaluation.steps.segments import (
    join_segment_columns,
    join_segment_sources,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    EvalPartitionsNotCurrentError,
    eval_snap_dates,
    eval_snap_dates_without_rows,
    restrict_to_current_eval_partitions,
    restrict_to_eval_snap_dates,
    stamp_partition_fingerprint,
)
from recsys_tfb.utils.spark import get_or_create_spark_session

logger = logging.getLogger(__name__)


def _require_prepared_with_current_config(
    segment_columns: dict, parameters: dict
) -> None:
    """Pre-check (inputs): the ``evaluation_segment_columns`` this run's
    directory holds was landed under today's
    :data:`~recsys_tfb.pipelines.evaluation.steps.config_fingerprint.PARTITION_CONTENT_KEYS`.

    ``prepare_eval_data`` lands that JSON together with the partitions, and the
    readers group by its ``joined`` list. Unchecked, ``--from-node
    compute_metrics`` after a change to ``segment_columns`` segments by the old
    list and writes ``metrics.json`` under the new fingerprint, which
    ``generate_report`` then accepts, exit code 0. Only those rows are compared;
    why, see ``PARTITION_CONTENT_KEYS``.

    It no longer stands for the partitions themselves (#374): one date's
    partition is also written by runs over other dates, whose JSON lands in
    another directory. Each partition carries its own fingerprint, checked by
    ``restrict_to_current_eval_partitions``.
    """
    require_computed_with_current_config(
        [LoadedArtifact(
            catalog_name="evaluation_segment_columns (landed with the "
                         "enriched_eval_predictions partition)",
            payload=segment_columns,
            produced_by="prepare_eval_data",
            compared_keys=PARTITION_CONTENT_KEYS,
        )],
        parameters,
    )


def _ci_consumer_enabled(parameters: dict) -> bool:
    """Whether the metric-CI diagnosis (the one non-registry consumer of the
    shared sample) is enabled.

    ``draw_diagnosis_sample_node`` draws the sample iff this or any registry
    diagnosis is enabled; ``compute_metric_ci`` still checks its own flag.
    Reading it here with the exact same key/default as the consumer prevents
    gate/consumer drift.
    """
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    return bool((diag.get("ci", {}) or {}).get("enabled", True))


def _registry_diagnosis_enabled(parameters: dict) -> bool:
    """registry 診斷（``contract.DIAGNOSES``）裡**吃共用抽樣**的那些有任一啟用嗎。

    只在 ``--post-training`` 被問：監控模式不組 registry 診斷，那裡的抽樣閘門
    不看這個函式（見 ``make_draw_diagnosis_sample_node``）。

    與 ``_ci_consumer_enabled`` 分開的理由：既有的 ci（非 registry 消費者）
    與 registry 診斷的生命週期不同。合在一起
    的話 Plan 2–5 每加一項診斷都要改所有解包點，而那正是
    registry 要消除的東西——所以這裡回一個 bool，不回擴增的 tuple。

    判準是「這項診斷的 ``INPUTS`` 裡有沒有 ``diagnosis_sample``」，不是
    「有沒有在 ``DIAGNOSES`` 裡」：不吃抽樣的診斷（例如讀 ``gain_ledger`` 的
    ``model_capacity``）不該觸發這次抽樣——那是一次公司規模
    ``toPandas()``（≈25 萬 query × 22 item 收到 driver）的白付，而且沒有任何
    測試會轉紅、也不會有錯誤訊息，pipeline 只是安靜地變慢。

    鍵與預設值必須跟各消費節點自己讀的完全一致（``enabled``，預設 True），
    否則閘門與消費端會漂移：使用者關掉 ci、只開一項吃抽樣的 registry 診斷
    時，樣本不會被抽，消費節點拿到 None 而 fail-loud。

    ``contract.DIAGNOSES`` is read as a module attribute, not through
    ``from ... import DIAGNOSES``. ``contract`` is imported at module level, so
    only the attribute read is resolved on every call and sees a monkeypatched
    registry; ``from ... import DIAGNOSES`` would bind the tuple once, when
    this module is imported.
    """
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    sample_consumers = [
        name for name in contract.DIAGNOSES
        if "diagnosis_sample" in contract.inputs_for(
            importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        )
    ]
    return any(
        bool((diag.get(name, {}) or {}).get("enabled", True))
        for name in sample_consumers
    )


def make_prepare_eval_data_node(population_name: str):
    """Build ``prepare_eval_data`` for one run mode's population table.

    ``population_name`` is the catalog entry ``create_pipeline`` wires as the
    node's third input: ``sample_pool`` for ``--post-training`` (the test set
    is drawn from it), ``inference_population`` for monitoring (it is what was
    scored). The node receives only a DataFrame, so the name comes in here for
    the warning, the report and ``evaluation_segment_columns``. The mode
    decides it (ADR-0013), as with ``make_draw_diagnosis_sample_node``.
    """
    def prepare_eval_data(
        ranked_predictions: SparkDataFrame,
        label_table: SparkDataFrame,
        population: SparkDataFrame,
        parameters: dict,
    ) -> tuple[SparkDataFrame, dict]:
        """Join ranked predictions with labels and segment columns using Spark.

        Returns ``(eval_predictions, segments)``. The frame lands as this
        month's ``enriched_eval_predictions`` partition, where the join is
        computed once and every reader reads it back (ADR-0018 decision 1).
        ``segments`` lands as
        ``evaluation_segment_columns``: ``joined`` (the segment columns
        actually joined, in ``evaluation.segment_columns`` order), ``sources``
        (joined column -> table it came from), ``missing`` (column -> the
        population table that lacks it) and ``config_fingerprint``.

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
        """
        schema = get_schema(parameters)
        time_col = schema["time"]
        identity_cols = schema["identity_columns"]
        label_col = schema["label"]

        eval_params = parameters.get("evaluation", {})

        labels = label_table

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
        if "model_version" in ranked_predictions.columns:
            logger.info("Filtering predictions to model_version=%s", model_version)
            ranked_predictions = ranked_predictions.filter(
                F.col("model_version") == model_version
            )
        else:
            # HiveTableDataset drops partition_filter columns after applying the
            # WHERE clause. Both of this node's sources declare model_version as a
            # static partition_filter — training_eval_predictions always did,
            # ranked_predictions since #187 — so a CLI-loaded DataFrame is already
            # pruned even though the constant column is no longer present. The
            # branch above survives for callers that hand this node a frame they
            # built themselves (tests, --compare paths reading via spark.table).
            logger.info(
                "Predictions input has no model_version column; assuming catalog "
                "partition_filter already selected model_version=%s",
                model_version,
            )

        # Decision — which months: evaluation.snap_date, one date or several
        # (#374), in both pipeline modes (this node serves monitoring and
        # --post-training), failing loud when it is unset or when any one date
        # has no rows. Chosen wrong, the run would silently evaluate the whole
        # table, or silently drop a month from a several-date evaluation. Each
        # date is an ISO date string (YYYY-MM-DD); the snap_date partition
        # column on ranked_predictions / training_eval_predictions is STRING, so
        # .cast("string") is a no-op here and stays correct if it is ever DATE.
        # eval_snap_dates raises when none is configured.
        snap_dates = eval_snap_dates(parameters)
        # The dates as the messages below print them: one date prints as it
        # always did, several as their list.
        snap_date_text = snap_dates[0] if len(snap_dates) == 1 else snap_dates
        logger.info("Filtering predictions to snap_date=%s", snap_date_text)
        # Per date, not on the filtered frame as a whole: with several dates the
        # whole frame has rows as soon as one month does.
        missing_dates = eval_snap_dates_without_rows(ranked_predictions, parameters)
        if missing_dates:
            available = sorted(
                str(r[time_col])
                for r in ranked_predictions.select(time_col).distinct().collect()
            )
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

        # Filter labels to snap_dates in predictions
        pred_snap_dates = ranked_predictions.select(time_col).distinct()
        labels = labels.join(pred_snap_dates, on=time_col, how="inner")

        # Pre-check (input): label_table holds at most one row per identity key in
        # the evaluated month. The LEFT JOIN below copies a prediction row once per
        # matching label row, so a duplicated key silently inflates that query's
        # candidate set and shifts every rank in it; no count or metric raises.
        # Not dropDuplicates: that picks one answer arbitrarily and makes the row
        # counts line up, which hides the problem better than leaving it (bug 10).
        # Not core/consistency.py: a user-defined source table's quality is not a
        # framework invariant — the same boundary as inference_population's
        # uniqueness in deliberate-non-goals.md. Checked on `labels` after the
        # month join, so it counts exactly the rows about to be joined. The
        # message carries counts only, never key values (they are entity ids).
        # Cost: one Spark action per run, a groupBy over one month of label rows
        # (a shuffle of that month) ending in a count; only the count reaches the
        # driver, whatever the table size.
        n_duplicated_keys = (
            labels.groupBy(*identity_cols)
            .agg(F.count(F.lit(1)).alias("_n_label_rows"))
            .filter(F.col("_n_label_rows") > 1)
            .count()
        )
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
        # training_eval_predictions stores alongside `score` (written by
        # training's predict_and_write_test_predictions), not label_table's. It
        # is exactly what the model's test mAP was scored against. Taking
        # label_table's instead, post-training metrics would disagree with
        # training's wherever label_table's answer for a key has changed since,
        # with no error. One side has to lose it anyway: the merge join below
        # keys on identity_cols only, so a `label` on both sides ->
        # AnalysisException: reference 'label' is ambiguous. Monitoring mode
        # (ranked_predictions) has no `label`, so the condition is False there
        # and behaviour is unchanged.
        if label_col in ranked_predictions.columns and label_col in labels.columns:
            labels = labels.drop(label_col)
            logger.info(
                "prepare_eval_data: predictions already carry '%s'; dropped it "
                "from the label_table side to avoid an ambiguous join column",
                label_col,
            )

        # Decision — how labels attach: LEFT JOIN, a missing label counts as 0.
        # It preserves every prediction row so per-customer ranking is over
        # the model's full candidate set (in dev: cust × 8 prod) regardless of
        # whether label_table covers that (cust, prod) pair. label_table's
        # per-group cust_pool semantics (conf/sql/etl/label/label_{ccard,exchange,
        # fund}.sql; cust must have ≥1 apply event in the group to appear) means
        # an INNER JOIN here would silently shrink each customer's rank set to
        # their per-group sub-products, collapsing baseline / mAP metrics to a
        # per-group framing the business model never asked for. Missing labels are
        # filled with 0 ("not bought"), matching the existing build_model_input
        # convention (pipelines/dataset/steps/model_input.py, LEFT + COALESCE(0)).
        eval_predictions = ranked_predictions.join(labels, on=identity_cols, how="left")
        if label_col in eval_predictions.columns:
            # INT, the type training_eval_predictions declares for `label`. The
            # two modes take the label from different tables — that one in
            # --post-training, the user-defined label_table in monitoring (the
            # example's synthetic one is BIGINT) — and both write the same
            # enriched_eval_predictions, whose schema never casts. Same failure as
            # `rank` below (bug 15); it surfaced on the first real monitoring run.
            eval_predictions = eval_predictions.fillna({label_col: 0}).withColumn(
                label_col, F.col(label_col).cast("int")
            )

        # Decision — a missing rank is computed here, a present one is trusted.
        # Downstream report rendering selects schema["rank"] from eval_predictions.
        # When the predictions source is
        # training_eval_predictions (--post-training mode), `rank` is absent because
        # the table no longer stores it (Spark mAP recomputes rank internally via
        # rank_within_query). Add it here when missing so downstream stays uniform;
        # when present (ranked_predictions source), trust the upstream value.
        # Today both ways give the same ranks: rank_within_query breaks ties the
        # way inference does (see below). The choice shows once one side's rule
        # changes: recomputing would silently replace the ranking inference
        # published; trusting means a `rank` column ever added to
        # training_eval_predictions is used as is, tie-break included, with no
        # message (ADR-0014 records that hazard).
        rank_col = schema["rank"]
        if rank_col not in eval_predictions.columns:
            score_col = schema["score"]
            entity_cols = schema["entity"]
            query_cols = [time_col] + entity_cols
            # rank_within_query adds a "pos" 1-based rank within each query
            # group, by score desc with ties by item asc — the rule inference
            # publishes `rank` with, so both modes rank the same rows alike.
            eval_predictions = rank_within_query(
                eval_predictions, query_cols, score_col, schema["item"]
            )
            # BIGINT, the type ranked_predictions declares for `rank`. Both modes
            # write the same enriched_eval_predictions (columns: "auto"), whose
            # schema is fixed by the first write and never cast afterwards, so
            # row_number()'s INT here against the monitoring side's BIGINT is a
            # type conflict on whichever run comes second (bug 15).
            eval_predictions = eval_predictions.withColumn(
                rank_col, F.col("pos").cast("bigint")
            ).drop("pos")
            logger.info(
                "prepare_eval_data: injected '%s' column via rank_within_query "
                "(predictions source did not provide it)",
                rank_col,
            )

        # Decision — where each segment column comes from (ADR-0020 bug 6):
        # its evaluation.segment_sources override when one is configured,
        # otherwise this run mode's population table, the table the evaluated
        # rows were drawn from, keyed by (time, entity). Chosen wrong (the one
        # fixed table both modes used before bug 6), a monitoring run's
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
        for col, table in missing.items():
            logger.warning(
                "segment column %r: population table %r has no such column "
                "(it has %s); per-segment metrics skip it this run",
                col, table, sorted(population_columns),
            )
        from_population = [c for c in segment_columns
                           if c not in overrides and c not in missing]
        if from_population:
            eval_predictions = join_segment_columns(
                eval_predictions, population, [time_col, *schema["entity"]],
                from_population, source_name=population_name,
            )
        if overrides:
            eval_predictions = join_segment_sources(eval_predictions, overrides)

        # Landed so every consumer groups by what was joined here, never by
        # the frame's columns: enriched_eval_predictions is shared by both run
        # modes, so a column the other mode joined sits in the frame all NULL.
        # Its fingerprint also stands for the partition written with it: the
        # segmenting readers check it before reading the table
        # (_require_prepared_with_current_config), because a slice starting
        # after this node reads the landed partition instead of re-joining.
        # --compare-only does not check it: --post-training is inert on that
        # path, so a check there would refuse every partition a post-training
        # run wrote.
        joined = [c for c in segment_columns if c not in missing]
        segments = {
            "joined": joined,
            "sources": {c: overrides[c]["table"] if c in overrides
                        else population_name for c in joined},
            "missing": missing,
            "config_fingerprint": fingerprint(parameters),
        }

        # Decision — every row carries the settings its partition is written
        # under, and the segment columns actually joined (#374). One date's
        # partition is written by runs over different date sets, whose JSON
        # above lands in different directories, so only the rows can tell a
        # later reader which run wrote them last; and a column the population
        # lacked is skipped, so the settings alone do not say what was joined.
        eval_predictions = stamp_partition_fingerprint(
            eval_predictions, parameters, joined)

        logger.info("Eval data prepared via Spark join")
        return eval_predictions, segments

    return prepare_eval_data


def make_draw_diagnosis_sample_node(registry_diagnoses_wired: bool):
    """Build the node that draws the shared driver-side diagnosis sample.

    Which consumers exist is a property of the pipeline's mode, not of the
    config: ``--post-training`` wires ``compute_metric_ci`` plus every registry
    diagnosis, monitoring mode only ``compute_metric_ci`` (ADR-0018 decision 5).
    The registry diagnoses' ``enabled`` flags default to true in both modes, so
    a gate reading only the config would, in monitoring mode with the CI
    switched off, draw a sample nobody reads: a driver-side ``toPandas`` of up
    to ``diagnosis.sample.max_queries`` queries, no error, only a slower run.
    ``create_pipeline`` passes the mode in instead.

    Both modes get the node name ``draw_diagnosis_sample_node``, so
    ``--from-node`` and the docs name one node whichever mode is running.
    """
    def draw_diagnosis_sample_node(
        eval_predictions: SparkDataFrame,
        segment_columns: dict,
        parameters: dict,
    ) -> Optional[tuple]:
        """Draw the shared driver-side diagnosis sample ONCE per run.

        ``compute_metric_ci`` plus, in ``--post-training``, every registry
        diagnosis (``contract.DIAGNOSES``, e.g. ``diagnose_config_shift``) all
        consume this single sample instead of each re-drawing it (same seed ->
        identical content; N Spark scans collapse to 1). Sharing one sample is
        also a correctness property, not just a speed one: numbers computed on
        different populations must not be read side by side. Returns ``None``
        only when *every* wired consumer is disabled.

        The sample carries the segment columns ``prepare_eval_data`` joined
        (``segment_columns["joined"]``), not the configured ones found in the
        frame (ADR-0020 bug 6).

        Pre-check (inputs): the partition was prepared under today's settings
        (``_require_prepared_with_current_config``).
        """
        ci_on = _ci_consumer_enabled(parameters)
        registry_on = (
            registry_diagnoses_wired and _registry_diagnosis_enabled(parameters)
        )
        if not (ci_on or registry_on):
            logger.info(
                "diagnosis sample: every wired consumer (ci%s) disabled — "
                "skipping sample draw",
                " + registry diagnoses" if registry_diagnoses_wired else "",
            )
            return None

        _require_prepared_with_current_config(segment_columns, parameters)
        # Decision — sample the evaluated dates only, and only partitions
        # written under today's settings: the table holds every month this
        # model_version was evaluated on, each rewritten by whichever run
        # covered it last.
        eval_predictions = restrict_to_current_eval_partitions(
            eval_predictions, parameters, segment_columns).frame

        sample_pdf, sample_meta = draw_diagnosis_sample(
            eval_predictions, parameters,
            segment_columns=segment_columns["joined"],
        )
        # deep=False keeps this a free observation: rows/cols are exact and the
        # bytes figure is a shallow estimate. deep=True would scan every string
        # cell (O(n_cells)) on the already-materialised sample — accurate but
        # not "free", which is the constraint for this always-on instrumentation.
        log_data_volume(logger, "diagnosis.sample_pdf", sample_pdf, deep=False)
        logger.info(
            "diagnosis sample drawn once (ci_enabled=%s, registry diagnoses "
            "enabled=%s): %d queries sampled",
            ci_on, registry_on, sample_meta["n_queries_sampled"],
        )
        return sample_pdf, sample_meta

    return draw_diagnosis_sample_node


#: The ``--post-training`` shape, every registry diagnosis wired. Kept as a
#: module-level name for callers that exercise the node outside
#: ``create_pipeline``.
draw_diagnosis_sample_node = make_draw_diagnosis_sample_node(
    registry_diagnoses_wired=True
)


def compute_metrics(
    eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> dict:
    """Compute ranking metrics using the Spark-native pipeline.

    Thin wrapper over `evaluation.metrics_spark.compute_all_metrics`. All
    row-level work stays in Spark; only small aggregated dicts are collected.

    Segments by what ``prepare_eval_data`` joined (``segment_columns``, the
    landed ``evaluation_segment_columns``), and copies its ``joined`` /
    ``sources`` / ``missing`` into the result as ``segments``: the report says
    which table each column came from and which one lacked a column.

    The result lands as ``evaluation_metrics`` (``metrics.json``, ADR-0018
    decision 2) and carries ``config_fingerprint``, which ``generate_report``
    checks before drawing from it.

    Pre-checks (inputs): the landed segment list was prepared under today's
    settings (``_require_prepared_with_current_config``), and so was each
    evaluated date's partition (``restrict_to_current_eval_partitions``).

    Postcondition: every configured date was evaluated, i.e. the number of
    months in the result equals the number of configured dates. The
    restriction leaves at most that many, so what this refuses is fewer, an
    empty or never-written partition for some date; reading a Hive table
    raises nothing for either. The dates are evaluated together, as one set of
    query groups (#374). It does not catch a reader that forgot to restrict
    (that reader is another node); the AST test in ``test_pipeline.py`` does.
    """
    _require_prepared_with_current_config(segment_columns, parameters)
    # Decision — evaluate the configured dates only, from partitions written
    # under today's settings: the table holds every month this model_version
    # was evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    result = compute_all_metrics(
        eval_predictions, parameters,
        segment_columns=segment_columns["joined"],
    )
    n_snap_dates = result["dataset_overview"]["totals"]["n_snap_dates"]
    snap_dates = eval_snap_dates(parameters)
    if n_snap_dates != len(snap_dates):
        one = len(snap_dates) == 1
        raise ValueError(
            f"compute_metrics postcondition: {n_snap_dates} evaluated months "
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
    result["segments"] = {
        k: segment_columns[k] for k in ("joined", "sources", "missing")
    }
    result["config_fingerprint"] = fingerprint(parameters)
    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        result["n_queries"],
        result["n_excluded_queries"],
    )
    return result


def compute_baseline_metrics(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> dict:
    """Popularity-baseline metrics, aligned row-for-row with eval_predictions.

    Re-scores each eval_predictions row with the product's historical
    purchase count, then runs the slim metrics path (overall + per_item).
    When the baseline report section is disabled the second metrics pass is
    skipped entirely and a stub ``{"enabled": False, "config_fingerprint":
    ...}`` is returned. Not ``None`` (the old return): a ``null`` has nowhere
    to carry the fingerprint, so ``generate_report`` could not tell "switched
    off under the current settings" from "left over from an older run"
    (ADR-0018 decision 2, ADR-0020 decision 2).

    Returns dict with keys:
      - overall:        dict[str, float]   slim metrics
      - per_item:       dict[str, dict]    per-product slim metrics
      - purchase_counts: dict[str, int]    per-product popularity count
            aggregated across eval snap_dates (sum). Drives the report's
            popularity-composition table; consumers must treat absence
            as backward-compatible (older results may omit it).
      - monthly_counts: dict[str, dict[str, int]]  the same counts per
            item per calendar month, summed over the windows.
      - window_months_covered: dict[str, int]  only when several dates are
            evaluated (#374): per evaluated date, the months with label rows
            in its lookback window. Absent means one date.
      - config_fingerprint: the computed settings it was made with
            (``steps.config_fingerprint``), checked by
            ``generate_report``.

    Pre-checks (inputs), past the stub: the landed segment list and each
    evaluated date's partition were prepared under today's settings
    (``_require_prepared_with_current_config``,
    ``restrict_to_current_eval_partitions``).
    """
    eval_params = parameters.get("evaluation", {}) or {}
    sections = (eval_params.get("report", {}) or {}).get("sections", {}) or {}
    if not sections.get("baseline", True):
        logger.info(
            "Baseline report section disabled — skipping baseline metrics"
        )
        return {"enabled": False, "config_fingerprint": fingerprint(parameters)}

    _require_prepared_with_current_config(segment_columns, parameters)
    # Decision — score the evaluated dates only, from partitions written under
    # today's settings: the table holds every month this model_version was
    # evaluated on, and the lookback windows below are anchored on the months
    # found in the frame.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    # Single source of the default (bug 1, ADR-0020): before this helper
    # existed, this node defaulted to 12 while build_baseline_section
    # defaulted to None (printing nothing), so a run relying on the implicit
    # default silently disagreed with itself about what it had used.
    lookback_months = resolve_lookback_months(parameters)

    snap_dates = [
        str(r[time_col])
        for r in eval_predictions.select(time_col).distinct().collect()
    ]
    counts = compute_purchase_counts(
        label_table, snap_dates, lookback_months, parameters
    )
    # Aggregate per-product count across eval snap_dates (sum). Single-snap
    # evaluation reduces to that snap's value. cast to int for clean JSON
    # serialisation in manifests / reports.
    purchase_counts = {
        str(r[item_col]): int(r[score_col])
        for r in counts.groupBy(item_col)
        .agg(F.sum(F.col(score_col)).alias(score_col))
        .collect()
    }
    # Per-(month, item) breakdown of the same windows → report's monthly
    # popularity trend. Summed over months, each item reconciles with
    # purchase_counts (both sum the same per-snap-per-month counts).
    monthly_counts: dict[str, dict[str, int]] = {}
    window_months_covered: Optional[dict[str, int]] = None
    if len(snap_dates) == 1:
        monthly = compute_monthly_purchase_counts(
            label_table, snap_dates, lookback_months, parameters
        )
        for r in (
            monthly.groupBy("month", item_col)
            .agg(F.sum(F.col(score_col)).alias(score_col))
            .collect()
        ):
            monthly_counts.setdefault(str(r[item_col]), {})[str(r["month"])] = int(
                r[score_col]
            )
    else:
        # Several evaluated dates (#374), one lookback window each. The same
        # counts kept apart per window (one scan of label_table), so one
        # collect gives both the trend (summed over windows) and how many
        # months with label rows each window had. The report divides the
        # per-month average by the sum of those, not by one window's lookback:
        # purchase_counts is summed over all windows. Written only here, so a
        # single-date result stays exactly what it was.
        months_by_window: dict[str, set] = {s: set() for s in snap_dates}
        summed: dict[str, dict[str, float]] = {}
        for r in compute_monthly_purchase_counts_by_window(
            label_table, snap_dates, lookback_months, parameters
        ).collect():
            month = str(r["month"])
            months_by_window[r["window_date"]].add(month)
            per_item = summed.setdefault(str(r[item_col]), {})
            per_item[month] = per_item.get(month, 0.0) + r[score_col]
        monthly_counts = {
            item: {month: int(v) for month, v in per_month.items()}
            for item, per_month in summed.items()
        }
        window_months_covered = {
            s: len(months_by_window[s]) for s in sorted(snap_dates)
        }
    baseline_frame = build_baseline_frame(eval_predictions, counts, parameters)
    # per_segment / category slices for the report's by-segment / 大類 vs
    # baseline comparison. Gated by what turns them on for the model (the
    # segment columns prepare_eval_data joined / item_categories maps items),
    # so the baseline pays for a slice only when the model computed its match.
    metrics = compute_overall_per_item(
        baseline_frame,
        parameters,
        segment_columns=segment_columns["joined"],
        with_category=True,
    )
    metrics["purchase_counts"] = purchase_counts
    metrics["monthly_counts"] = monthly_counts
    if window_months_covered is not None:
        metrics["window_months_covered"] = window_months_covered
    metrics["config_fingerprint"] = fingerprint(parameters)
    logger.info(
        "Baseline metrics computed (overall + per_item) for snap_dates=%s; "
        "purchase_counts has %d products, monthly_counts spans %d months",
        snap_dates, len(purchase_counts),
        len({mo for per in monthly_counts.values() for mo in per}),
    )
    return metrics


def compute_metric_ci(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """診斷抽樣＋cluster bootstrap CI（spec §3 Phase 1）。

    抽樣改由 ``draw_diagnosis_sample_node`` 一次抽好、經 ``diagnosis_sample``
    傳入（同 seed→內容與各自重抽相同）。停用時回傳 stub（catalog 仍寫出
    ``{"enabled": false}``）。輸出含 ``sample`` metadata——CI 是抽樣估計，
    報表必須標示樣本規模。

    Both the stub and the full result carry ``config_fingerprint``: the JSON
    lands, and ``generate_report`` refuses one computed under other settings.

    Pre-check (input): with the CI enabled, ``diagnosis_sample`` is not
    ``None`` (``ValueError``). ``None`` there means
    ``draw_diagnosis_sample_node``'s gate disagrees with this node's flag.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    ci_cfg = ((eval_params.get("diagnosis", {}) or {}).get("ci", {}) or {})
    if not ci_cfg.get("enabled", True):
        logger.info("metric CI disabled — writing stub")
        return {"enabled": False, "config_fingerprint": fingerprint(parameters)}

    if diagnosis_sample is None:
        raise ValueError(
            "compute_metric_ci: diagnosis_sample is None while "
            "evaluation.diagnosis.ci.enabled is true — draw_diagnosis_sample_node "
            "gate is out of sync with the consumer enable flag"
        )

    sample_pdf, sample_meta = diagnosis_sample
    out = bootstrap_per_item_ci(sample_pdf, parameters)
    out["sample"] = sample_meta
    out["config_fingerprint"] = fingerprint(parameters)
    logger.info(
        "metric CI computed on %d sampled queries (n_boot=%d)",
        sample_meta["n_queries_sampled"], out["n_boot"],
    )
    return out


def make_diagnosis_node(name: str):
    """為 registry 裡的一項診斷造一個薄 node 函式。

    Plan 2-5 的診斷 node 長得幾乎一樣：讀 ``enabled``、停用寫 stub、若吃共用
    抽樣則樣本是 ``None`` 就 fail-loud、否則轉呼叫模組的 ``compute``。手寫多份
    的問題不是行數，是那些副本會各自漂移——尤其「停用時回什麼」與「樣本
    ``None`` 時 raise 還是靜默」這兩件事，寫錯了 pipeline 照樣跑得完。

    node inputs 不是寫死的 ``["diagnosis_sample", "parameters"]``：每次呼叫都
    向 ``contract.inputs_for`` 問這個模組宣告了什麼（多數診斷沒宣告，落回
    ``DEFAULT_INPUTS``）。``diagnosis_sample`` 的 fail-loud 守衛只在它真的出現
    在 ``INPUTS`` 裡時才適用——不吃抽樣的診斷（例如 ``model_capacity``）沒有
    這個守衛，因為它們的第一個 input 從來就不是抽樣。

    個數檢查（``len(node_inputs) != len(declared)``）是這裡的核心宣稱之一：
    Plan 1.5 的教訓是「寬簽章讓個數不對不再是錯誤」，所以這裡刻意用
    ``*node_inputs`` 接、立刻核對個數，不用 ``*args`` 直接轉呼叫——後者會讓
    少給一個 input 靜默地把某個位置參數錯當成 ``parameters``。

    Both of ``_run``'s own raises are pre-checks (inputs): a wrong input count
    (``TypeError``, mis-wired inputs) and a ``None`` sample for a diagnosis
    that consumes it (``ValueError``, the sample gate out of sync with this
    diagnosis's flag). Neither means this node computed something wrong. The
    upstream-fingerprint check described below is the third pre-check.

    ``parameters`` 一律取 ``node_inputs[-1]``——這是 ``INPUTS`` 的不變量
    （§3 之一，contract 測試守著）：宣告了 ``INPUTS`` 的模組必須把
    ``"parameters"`` 放在最後一格。

    registry 診斷的 ``compute`` 吃的是 ``INPUTS`` 宣告的每個 input 本身（吃
    ``diagnosis_sample`` 的診斷拿到的是整個 ``(sample_pdf, sample_meta)``
    tuple，不是解包後的 ``sample_pdf``）——契約在
    ``diagnosis.metric.contract.compute_params_for`` 釘住。

    ``__name__`` 明設：``Node.name`` 預設取 ``func.__name__``
    （``core/node.py:8``），不設的話多個 node 同名，``--only-node`` 指不到、
    log 分不出誰是誰，而 pipeline 照樣跑得完。

    **前置檢查（precondition）**：宣告的 input 裡，凡是 ``evaluation_<upstream>``
    形狀且 ``<upstream>`` 本身也在 ``contract.DIAGNOSES`` 裡的（目前只有
    ``model_capacity`` 讀 ``evaluation_item_ability``），在呼叫 ``compute`` 之前
    先用 :func:`require_computed_with_current_config` 驗它的指紋。理由：這個
    factory 底下所有診斷共用同一份 body，``--only-node diagnose_model_capacity``
    會把落地的舊 ``evaluation_item_ability`` JSON 當 input 讀進來計算，而下面
    的 ``stamp`` 只會蓋上**這次**的指紋——若不在這裡另外驗上游，往後
    ``render_diagnosis_pages`` 看到的是一份蓋著新指紋、內容卻算在舊
    ``item_ability`` 結果上的產物，檢查不出來。寫成通用迴圈（掃 ``declared``
    找符合形狀的 input），不是 model_capacity 專用分支：未來任何診斷讀另一項
    診斷的落地結果都自動被涵蓋。非 registry 診斷的 ``evaluation_*`` input（目前
    不存在）不會被這段檢查到——它只認得出「這個名字對應 ``DIAGNOSES`` 裡的
    某一項」。

    Every output, the disabled stub included, gets two keys added here rather
    than in the four ``_compute.py`` files: this factory is the one exit all
    diagnosis nodes share, so one place covers every diagnosis present and
    future (ADR-0020 bug 9).

    * ``"diagnosis": name`` lets ``render_diagnosis_pages`` check that its
      i-th input really is ``DIAGNOSES[i]``. Every result is a dict, so a
      count or type check alone lets reordered inputs through.
    * ``"config_fingerprint"`` covers the shared computed settings plus the
      module's own ``EXTRA_CONFIG_KEYS`` (``contract.extra_config_keys_for``),
      so changing a ``dataset.*`` key that only ``config_shift`` reads marks
      only that JSON stale.
    """
    def _run(*node_inputs) -> dict:
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        declared = contract.inputs_for(mod)
        if len(node_inputs) != len(declared):
            raise TypeError(
                f"diagnose_{name}: expected {len(declared)} inputs "
                f"({', '.join(declared)}), got {len(node_inputs)}"
            )
        parameters = node_inputs[-1]
        stamp = {
            "diagnosis": name,
            "config_fingerprint": fingerprint(
                parameters, contract.extra_config_keys_for(mod)),
        }
        cfg = (((parameters.get("evaluation", {}) or {})
                .get("diagnosis", {}) or {}).get(name, {}) or {})
        if not cfg.get("enabled", True):
            logger.info("%s disabled — writing stub", name)
            return {"enabled": False, **stamp}
        if "diagnosis_sample" in declared:
            sample_idx = declared.index("diagnosis_sample")
            if node_inputs[sample_idx] is None:
                raise ValueError(
                    f"diagnose_{name}: diagnosis_sample is None while "
                    f"evaluation.diagnosis.{name}.enabled is true — "
                    "draw_diagnosis_sample_node gate out of sync with the "
                    "consumer flag"
                )

        # Pre-check (input): any declared input that is itself another
        # registry diagnosis's landed result must have been computed with
        # today's settings, the same freshness check render_diagnosis_pages
        # runs before drawing. Without this, `--only-node diagnose_{name}`
        # loads a stale evaluation_<upstream> JSON, computes on it, and this
        # node's own `stamp` below records the CURRENT fingerprint — so the
        # later render check sees a fresh-looking result and old numbers
        # reach the page (model_capacity reads evaluation_item_ability this
        # way).
        #
        # Generic over the registry, not a model_capacity special case:
        # any declared input named `evaluation_<upstream>` where <upstream>
        # is itself in contract.DIAGNOSES is checked the same way.
        # `gain_ledger` (a training artifact, no fingerprint) and
        # `diagnosis_sample` (memory-only, checked above instead) are not
        # registry diagnoses and so are left alone. A non-registry
        # `evaluation_*` input would likewise not be checked — none exists
        # today, and adding one back would need this loop taught about it.
        for input_name, value in zip(declared, node_inputs):
            if not input_name.startswith("evaluation_"):
                continue
            upstream = input_name[len("evaluation_"):]
            if upstream not in contract.DIAGNOSES:
                continue
            upstream_mod = importlib.import_module(
                f"recsys_tfb.diagnosis.metric.{upstream}")
            require_computed_with_current_config(
                [LoadedArtifact(
                    catalog_name=input_name,
                    payload=value,
                    produced_by=f"diagnose_{upstream}",
                    extra_keys=contract.extra_config_keys_for(upstream_mod),
                )],
                parameters,
            )

        out = mod.compute(*node_inputs)
        # 純量鍵通用地印出來，不為每項診斷各寫一句摘要：那樣 Plan 2-5 每加
        # 一項就要多一段格式化字串，而它們沒有任何測試守著格式。
        scalars = {
            k: v for k, v in out.items()
            if isinstance(v, (int, float, str, bool))
        }
        logger.info("%s computed: %s", name, scalars)
        out.update(stamp)
        return out

    _run.__name__ = f"diagnose_{name}"
    _run.__qualname__ = f"diagnose_{name}"
    return _run


def _diagnosis_pages_dir(parameters: dict):
    """診斷頁的輸出目錄，對齊 catalog 的
    ``data/evaluation/${model_version}/${snap_date}/diagnosis/``。

    **為什麼可以在這裡重算這條路徑**：``__main__`` 把 ``runtime_params``
    （含 dash 已剝掉的 ``snap_date``）併進 node 拿到的 ``parameters``，再拿同
    一份 dict 去做 catalog 的 ``${...}`` 代換——所以這裡取的是 catalog 代換用
    的**同一組值**，不是另外猜一次。同樣的做法見
    ``diagnosis.model.paths.diagnostics_dir``。

    runtime 的值原樣使用：它就是 catalog 代換進路徑的那個字串。退回
    ``evaluation.snap_date`` 是給單元測試用的（那裡沒有 runtime_params），走的是
    CLI 算路徑段的同一個 ``dates_label``：一個日期＝剝掉 dash 的 ``YYYYMMDD``，
    多個日期＝``<最早>-<最晚>``（#374）。以前這裡對兩者都剝 dash；多個日期的
    路徑段裡那個 dash 是名字的一部分，剝掉就對不上 catalog 的目錄。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    snap = parameters.get("snap_date")
    if not snap:
        dates = as_date_list(eval_params.get("snap_date"))
        snap = dates_label(dates) if dates else "unknown"
    return (Path("data") / "evaluation"
            / str(parameters.get("model_version", "unknown"))
            / str(snap)
            / "diagnosis")


def render_diagnosis_pages(parameters: dict, *diagnosis_results) -> list[str]:
    """Draw the diagnosis pages from this run's results; return written paths.

    **Why it draws its inputs instead of reading ``diagnosis/<name>.json``.**
    What is drawn must be what this run computed. Reading the directory by
    file name drew whatever sat there: a diagnosis this run did not compute
    still got a page, and a link in the main report, from an earlier run's
    JSON, with exit code 0 (ADR-0020 bug 9). The inputs are the
    ``evaluation_<name>`` results, loaded from the catalog when slicing skips
    their nodes; a result left over from other settings is caught by the
    fingerprint pre-check below.

    **Why varargs stay.** The registry's length is dynamic and the Runner
    binds inputs by position (``core/runner.py``), so a fixed signature would
    change with every diagnosis added.

    **Why the pre-check reads content, not only the count.** ``parameters``
    and every result are dicts, so a reordered inputs list matches in count
    and in type and would draw one diagnosis under another's title; a
    matching count is not a fix (``docs/operations/known-pitfalls.md`` §12).
    ``make_diagnosis_node`` stamps each result with its name, so the i-th
    input must name ``DIAGNOSES[i]``.

    Pre-checks (inputs), both raising before any page is written:

    1. Wiring, ``TypeError``: ``parameters`` is a dict with an ``evaluation``
       key; there is one result per registry diagnosis; the i-th is a dict
       whose ``"diagnosis"`` is ``DIAGNOSES[i]``.
    2. Freshness, ``ValueError``: every result's ``config_fingerprint``
       matches the current settings (the shared ``COMPUTED_KEYS`` plus that
       diagnosis's ``EXTRA_CONFIG_KEYS``); the message names the key that
       changed and the node to ``--from-node``.

    **What reading by file name used to guard for free**: a registry
    diagnosis with no catalog entry. The catalog then makes a MemoryDataset,
    the page is drawn and no JSON lands, so offline redraw and slice resumes
    never see it. ``tests/test_diagnosis/test_metric/test_contract.py::
    test_every_registry_diagnosis_has_a_catalog_entry`` guards that now.

    The inputs still order the DAG as well: they are what places this node
    after every diagnosis node, and what lets ``--only-node`` pull a
    diagnosis whose JSON is missing back in.

    ``contract.DIAGNOSES`` is read as a module attribute, as in
    ``_registry_diagnosis_enabled``, so a monkeypatched registry is the one
    both checked and drawn. Page-writing errors are not swallowed: a red run
    is easier to spot than a report that silently lost its diagnosis link.
    """
    names = contract.DIAGNOSES
    if not (isinstance(parameters, dict) and "evaluation" in parameters):
        raise TypeError(
            "render_diagnosis_pages: the first input must be the parameters "
            "dict (a dict with an 'evaluation' key), got "
            f"{_describe_node_input(parameters)}. Check the order of this "
            "node's inputs in pipeline.py."
        )
    if len(diagnosis_results) != len(names):
        raise TypeError(
            f"render_diagnosis_pages: expected {len(names)} diagnosis results "
            f"({', '.join(names)}), got {len(diagnosis_results)}"
        )
    for i, (name, result) in enumerate(zip(names, diagnosis_results)):
        if isinstance(result, dict) and result.get("diagnosis") == name:
            continue
        raise TypeError(
            f"render_diagnosis_pages: diagnosis input {i + 1} should be the "
            f"result of {name!r} (contract.DIAGNOSES[{i}]), got "
            f"{_describe_node_input(result)}. Check the order of this node's "
            "inputs in pipeline.py."
        )

    require_computed_with_current_config(
        [
            LoadedArtifact(
                catalog_name=f"evaluation_{name}",
                payload=result,
                produced_by=f"diagnose_{name}",
                extra_keys=contract.extra_config_keys_for(
                    importlib.import_module(
                        f"recsys_tfb.diagnosis.metric.{name}")),
            )
            for name, result in zip(names, diagnosis_results)
        ],
        parameters,
    )

    out_dir = _diagnosis_pages_dir(parameters)
    pages = assemble_diagnosis_pages(
        dict(zip(names, diagnosis_results)), parameters, out_dir)
    logger.info(
        "diagnosis pages written to %s (%d files from %d results)",
        out_dir, len(pages), len(diagnosis_results),
    )
    return [str(p) for p in pages]


def _describe_node_input(value) -> str:
    """What a mis-wired input is, for ``render_diagnosis_pages``' messages."""
    if not isinstance(value, dict):
        return f"a {type(value).__name__}"
    if "diagnosis" in value:
        return f"the result of {value['diagnosis']!r}"
    if "evaluation" in value:
        return "the parameters dict"
    if "config_fingerprint" not in value:
        # Neither key present: not a mis-ordered input (those still carry
        # config_fingerprint), but a JSON written before #342 gave diagnosis
        # results a name and a fingerprint at all. A --from-node on the
        # diagnosis nodes alone would recompute them but leave
        # baseline_metrics / metric_ci / report_aggregates on their old,
        # unfingerprinted disk state, so the fix is the whole pipeline, not a
        # slice.
        return (
            "a dict with neither a 'diagnosis' nor a 'config_fingerprint' "
            "key: this looks like a result written before results carried "
            "their name and fingerprint (before #342). Re-run the whole "
            "pipeline (no --from-node / --only-node) — from-node-ing just "
            "the diagnoses would still leave metric CI / report aggregates "
            "unfingerprinted, costing extra Spark rounds"
        )
    # Fingerprint but no name: name and fingerprint arrived together, so this
    # is not an old JSON but another fingerprinted artifact (metric CI, report
    # aggregates) wired into a diagnosis slot.
    return ("a dict with a 'config_fingerprint' but no 'diagnosis' key: a "
            "fingerprinted artifact that is not a registry diagnosis result, "
            "wired into this slot")


def no_diagnosis_pages(parameters: dict) -> list[str]:
    """Monitoring mode's ``evaluation_diagnosis_pages``: always empty, reads nothing.

    Monitoring mode wires no registry diagnosis (ADR-0018 decision 5), yet
    ``generate_report`` still takes a sixth input. Of the three ways to supply
    it, this is the one that cannot go wrong:

    * **Not wiring it**: ``core/runner.py`` binds inputs by position, so the
      Runner raises "requires input … not produced by any prior node" before
      anything runs.
    * **A default for ``generate_report``'s ``diagnosis_pages``**: a trailing
      default swallows arity errors, and its six required parameters are what
      ``known-pitfalls.md`` §12 fixed.
    * **Reusing ``render_diagnosis_pages`` with only ``parameters``**: it
      requires one named, fingerprinted result per registry diagnosis and
      raises ``TypeError`` otherwise, and this mode computes none of them.
      Feeding it results would mean wiring the diagnosis nodes back in, which
      is what decision 5 removed. (Before #342 it read the pages directory by
      file name, which made this option worse still: after an earlier
      ``--post-training`` run of the same ``(model_version, snap_date)`` the
      monitoring report linked to that run's pages, with exit code 0.)

    ``parameters`` is taken (unread) because ADR-0018 fixes this signature. It
    reads neither the disk nor that value, which is also why its place in the
    topological order does not matter.
    """
    return []


def compute_report_aggregates(
    eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> dict:
    """主報表診斷區的 Spark 聚合，落地成 JSON。

    從 ``generate_report`` 拆出來（Plan 1.5）。理由不只是效能：它讓
    ``generate_report`` 變成純函式；指標與 baseline 也落地之後（ADR-0018 決定 2），
    ``--only-node generate_report`` 不重算任何指標就能重繪主報表。也把這 6 次全掃的
    失敗點從 pipeline 的**最後一個 node** 往上游移。

    Both the stub and the full result carry ``config_fingerprint``: the JSON
    lands, and ``generate_report`` refuses one computed under other settings.

    Reads ``enriched_eval_predictions`` but does not compare the
    ``segment_columns.json`` fingerprint with today's settings: it does not
    segment. It takes that JSON (#374) for its ``joined`` list only, which with
    today's settings makes the partition fingerprint each evaluated date must
    carry (``restrict_to_current_eval_partitions``). That closes the hole
    ADR-0020 bug 6 (#352 correction) had accepted for a slice running only this
    node on a stale partition.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}
    if not sections_cfg.get("diagnostics", True):
        logger.info("report diagnostics section disabled — writing stub")
        return {"enabled": False, "config_fingerprint": fingerprint(parameters)}

    # Decision — aggregate the evaluated dates only, from partitions written
    # under today's settings: the table holds every month this model_version
    # was evaluated on.
    eval_predictions = restrict_to_current_eval_partitions(
        eval_predictions, parameters, segment_columns).frame

    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    rank_col, label_col = schema["rank"], schema["label"]
    needed = list(dict.fromkeys([item_col, score_col, rank_col, label_col]))
    # 每個家族各是一次 action，不 cache 就是 6 次全掃。
    sdf = eval_predictions.select(*needed).cache()
    try:
        out = aggregate_report_diagnostics(
            sdf, item_col=item_col, score_col=score_col,
            rank_col=rank_col, label_col=label_col,
            include_distributions=diag_cfg.get("include_distributions", True),
            include_calibration=diag_cfg.get("include_calibration", True),
            n_calibration_bins=diag_cfg.get("n_calibration_bins", 10),
        )
    finally:
        # 原本的寫法在例外時不會 unpersist。行為上這是純改善：輸出不變。
        sdf.unpersist()
    out["enabled"] = True
    logger.info("report aggregates computed: %s", sorted(out))
    out["config_fingerprint"] = fingerprint(parameters)
    return out


def generate_report(
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: dict,
    metric_ci: dict,
    report_aggregates: dict,
    diagnosis_pages: Optional[list],
) -> str:
    """Build the HTML report. Metrics dicts drive §0–§8; the diagnostics
    section (when enabled) reads the already-aggregated Spark JSON from
    ``compute_report_aggregates`` (Plan 1.5) so this function stays pure —
    no SparkDataFrame in the signature, no Spark action in the body.

    診斷頁由 ``render_diagnosis_pages`` 產生（Plan 1.5 拆出），這裡只收它回傳
    的路徑清單、放一個連結進主報表。

    Pre-check (inputs): ``evaluation_metrics``, ``baseline_metrics``,
    ``metric_ci`` and ``report_aggregates`` were computed with the current
    computed settings (``steps.config_fingerprint``). All four are
    landed JSON (ADR-0018 decision 2), and ``--only-node generate_report``
    stops at "the JSON exists", so without this a setting changed since the
    last run is drawn from the old JSON with exit code 0 (ADR-0020 bug 2);
    with it the run raises, naming the key and the node to ``--from-node``.
    Only computed settings count, so changing ``report.display.*`` and the
    other drawn keys still redraws without recomputing anything.

    Not in the check: ``diagnosis_pages``, a list of paths whose sources
    ``render_diagnosis_pages`` has already checked.
    """
    require_computed_with_current_config(
        [
            LoadedArtifact(catalog_name="evaluation_metrics",
                           payload=evaluation_metrics,
                           produced_by="compute_metrics"),
            LoadedArtifact(catalog_name="baseline_metrics",
                           payload=baseline_metrics,
                           produced_by="compute_baseline_metrics"),
            LoadedArtifact(catalog_name="evaluation_metric_ci",
                           payload=metric_ci,
                           produced_by="compute_metric_ci"),
            LoadedArtifact(catalog_name="evaluation_report_aggregates",
                           payload=report_aggregates,
                           produced_by="compute_report_aggregates"),
        ],
        parameters,
    )
    return assemble_report(
        evaluation_metrics, parameters,
        baseline_metrics=baseline_metrics,
        report_aggregates=report_aggregates,
        metric_ci=metric_ci,
        diagnosis_pages=diagnosis_pages,
    )


def load_compare_predictions(parameters: dict) -> SparkDataFrame:
    """Pipeline shim: resolve a SparkSession and dispatch to source loader."""
    spark = get_or_create_spark_session()
    return _load_compare(parameters, spark)


def make_restrict_to_common_node(compare_only: bool):
    """Build ``restrict_to_common`` for ``--compare`` or ``--compare-only``.

    The mode decides which settings side A's partitions are checked against
    (``restrict_to_current_eval_partitions``, #374), so ``create_pipeline``
    passes it in, as it does for ``make_prepare_eval_data_node``:

    * ``--compare``: today's settings, like every other reader. The same run
      normally wrote the partitions, but a slice does not:
      ``--compare X --only-node generate_comparison_report`` runs only
      ``load_compare_predictions``, this node and the report, so after a
      segment setting changed or the run mode switched, the partitions and
      the directory's ``segment_columns.json`` both still say the old
      settings. Compared with that JSON, the old rows would pass.
    * ``--compare-only``: the settings that JSON records. ``--post-training``
      is inert on that path, so today's value of it says nothing about which
      population an earlier standard run wrote (ADR-0020 bug 6, #352
      correction); compared with today's settings, a post-training run's
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
        restrict both sides to their common universe (``_restrict_to_common``).
        Side B is the compared run's own table and is not checked."""
        # Decision — compare the evaluated dates only, from partitions written
        # under the settings this mode reads them for: the table holds every
        # month this model_version was evaluated on.
        eval_predictions = restrict_to_current_eval_partitions(
            eval_predictions, parameters, segment_columns,
            recorded_settings=compare_only,
        ).frame
        return _restrict_to_common(
            eval_predictions, compare_predictions_raw, parameters)

    return restrict_to_common


def _restrict_to_common(
    eval_predictions: SparkDataFrame,
    compare_predictions_raw: SparkDataFrame,
    parameters: dict,
) -> tuple[SparkDataFrame, SparkDataFrame, dict]:
    """Call the pure restrict function + capture coverage dict.

    ``eval_predictions`` is already kept to the evaluated dates and checked
    (``make_restrict_to_common_node``).

    Returns ``(a_common, b_common, coverage_partial)`` — coverage_partial
    carries full-universe sizes, the common sizes and dropped item lists so
    the report can show what was filtered.

    Population size is counted in **query groups** — distinct ``[time] +
    entity`` combinations, every column of ``schema.entity`` — because that is
    the unit mAP divides by. Reporting it in any other unit puts two different
    scales side by side in one table with nothing telling the reader they
    differ. See ``docs/adr/0015-compare-population-counted-in-query-groups.md``.

    The common count is taken from what the restriction kept, not re-derived
    from the raw frames (ADR-0020 bug 14). It used to ``intersect`` the raw
    query groups, which matches ``NULL == NULL``, while the restriction's
    equi-join drops null keys — so it counted groups no metric ever saw.

    The node does not check the ``segment_columns.json`` fingerprint against
    today's settings: under ``--compare`` the same run's segmenting readers do,
    and ``--compare-only`` is left unchecked on purpose (ADR-0020 bug 6, #352
    correction: ``post_training`` is inert there).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    query_group_cols = [time_col, *schema["entity"]]

    # Decision — B is scored against A's labels: not the label B landed with,
    # and not a fresh label_table join, since A's own label is not the current
    # label_table under --post-training / --compare-only. One answer for both
    # sides in every mode (ADR-0020 bug 7; the full why is in
    # steps/compare_universe.py).
    a_common, b_common, universe = _restrict(
        eval_predictions, compare_predictions_raw, parameters
    )

    a_groups_full = eval_predictions.select(*query_group_cols).distinct().count()
    b_groups_full = compare_predictions_raw.select(*query_group_cols).distinct().count()
    # Groups both restricted frames still hold. A left-semi equi-join, so null
    # keys never match — the same null rule as the restriction's own joins. On
    # symmetric candidate sets this equals either side's kept group count; when
    # one side scored a common entity only on items the other lacks, that group
    # is in one side's metrics but not in "common". The time is matched as
    # text, by the same helper the restriction uses (B's time column may be
    # DATE while A's partition is STRING).
    entity_cols = schema["entity"]
    groups_common = (
        query_groups_with_text_time(a_common, time_col, entity_cols)
        .join(
            query_groups_with_text_time(b_common, time_col, entity_cols),
            on=query_group_cols, how="left_semi",
        )
        .count()
    )
    common_items = universe.common_items

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    coverage_partial = {
        "kind_a": "model_version",
        "model_version_a": parameters.get("model_version", "(this run)"),
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_query_group_A_full": a_groups_full,
        "n_query_group_B_full": b_groups_full,
        "n_query_group_common": groups_common,
        "n_item_A_full": len(universe.a_items),
        "n_item_B_full": len(universe.b_items),
        "n_item_common": len(common_items),
        "dropped_items_A": sorted(universe.a_items - common_items),
        "dropped_items_B": sorted(universe.b_items - common_items),
    }
    return a_common, b_common, coverage_partial


#: The ``--compare`` shape, checked against today's settings. Kept as a
#: module-level name for callers that exercise the node outside
#: ``create_pipeline``.
restrict_to_common = make_restrict_to_common_node(compare_only=False)


def generate_comparison_report(
    eval_predictions_common: SparkDataFrame,
    compare_predictions_common: SparkDataFrame,
    coverage_partial: dict,
    segment_columns: dict,
    parameters: dict,
) -> str:
    """Run compute_all_metrics on both sides + assemble HTML.

    Only this run's side segments, by what ``prepare_eval_data`` joined
    (``segment_columns``; under ``--compare-only`` the copy landed next to the
    enriched partition). Its frame can hold a column the other run mode joined,
    all NULL, so the frame's columns are not asked (ADR-0020 bug 6). The
    compared side is another prediction table with no segment columns.
    """
    metrics_a = compute_all_metrics(
        eval_predictions_common, parameters,
        segment_columns=segment_columns["joined"],
    )
    metrics_b = compute_all_metrics(
        compare_predictions_common, parameters, segment_columns=[]
    )

    src = (parameters.get("evaluation", {}) or {}).get("compare", {}) or {}
    label_a = "Model"
    label_b = src.get("label", "Compare")
    comparison = build_comparison_result(metrics_a, metrics_b, label_a, label_b)

    return assemble_comparison_report(
        metrics_a, metrics_b, comparison, coverage_partial, parameters
    )


def validate_enriched_eval_predictions_present(
    enriched_eval_predictions: SparkDataFrame,
    segment_columns: dict,
    parameters: dict,
) -> None:
    """Fail loud if ``enriched_eval_predictions`` holds no rows for an
    evaluated date under this ``model_version``, or rows another run wrote.

    A zero-output gate: it passes nothing on. ``restrict_to_common`` reads the
    table and keeps the evaluated dates itself, like every other reader
    (ADR-0018 decision 1). The catalog has already pruned the table to this
    ``model_version`` via ``partition_filter``; this node keeps the evaluated
    dates and asserts each one has a row, otherwise raises
    ``DataConsistencyError`` naming the dates without rows and what to run
    first. Per date: with several dates configured, the kept rows are not
    empty as soon as one month has rows (#374).

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

    Used only in ``--compare-only`` mode. In the other modes
    ``prepare_eval_data`` writes the partition earlier in the same run, so this
    gate cannot fire.

    Its raise is a pre-check (input): an earlier run was to write those rows,
    and nothing in this run computed them.
    """
    mv = parameters.get("model_version", "unknown")
    hive_db = (parameters.get("hive") or {}).get("db", "ml_recsys")

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
            f"{hive_db}.enriched_eval_predictions has no partition "
            f"for {asked} "
            f"model_version={mv!r}. "
            "Run `python -m recsys_tfb evaluation` (with or without "
            "--compare) first to populate the partition."
        )
    if not_current is not None:
        problems.append(str(not_current))
    if problems:
        raise DataConsistencyError("\n".join(problems)) from not_current

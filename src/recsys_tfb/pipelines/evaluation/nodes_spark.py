"""Evaluation pipeline nodes — Spark backend."""

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.calibration import plot_calibration_curves
from recsys_tfb.evaluation.diagnostics_spark import (
    calibration_bins,
    positive_rank_count_matrix,
    positive_rate_matrix,
    rank_count_matrix,
    score_box_stats,
    score_box_stats_by_label,
    score_histogram_counts,
)
from recsys_tfb.evaluation.distributions import (
    plot_positive_rank_heatmap,
    plot_positive_rate_rank_heatmap,
    plot_rank_heatmap,
    plot_score_boxplot,
    plot_score_boxplot_by_label,
    plot_score_histogram,
)
from recsys_tfb.evaluation.report_builder import assemble_report

logger = logging.getLogger(__name__)


def _sample_consumer_flags(parameters: dict) -> tuple[bool, bool, bool]:
    """Return (ci_enabled, offset_sweep_enabled, pair_ledger_enabled).

    Single source of truth for the enable flags of the three diagnosis nodes
    that consume the shared sample. ``draw_diagnosis_sample_node`` draws iff any
    is True; each consumer still checks its own flag. Reading them here with the
    exact same keys/defaults as the consumers prevents gate/consumer drift.
    """
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    ci = (diag.get("ci", {}) or {}).get("enabled", True)
    sweep = (diag.get("offset_sweep", {}) or {}).get("enabled", True)
    ledger = (diag.get("pair_ledger", {}) or {}).get("enabled", True)
    return bool(ci), bool(sweep), bool(ledger)


def prepare_eval_data(
    ranked_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Join ranked predictions with labels using Spark.

    For external segment sources, delegates to
    ``segments.join_segment_sources`` (storage backend isolated behind its
    source seam).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_cols = schema["identity_columns"]
    label_col = schema["label"]

    eval_params = parameters.get("evaluation", {})

    labels = label_table

    # Filter predictions to the resolved model_version (resolved upstream by
    # __main__.py via core.versioning.resolve_model_version).
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
        # WHERE clause. training_eval_predictions uses model_version as a
        # static partition_filter, so its CLI-loaded DataFrame is already
        # pruned even though the constant column is no longer present.
        logger.info(
            "Predictions input has no model_version column; assuming catalog "
            "partition_filter already selected model_version=%s",
            model_version,
        )

    # Filter predictions to the configured evaluation snap_date. evaluation.
    # snap_date is an ISO date string (YYYY-MM-DD); the snap_date partition
    # column on ranked_predictions / training_eval_predictions is STRING, so
    # .cast("string") is a no-op here and stays correct if it is ever DATE.
    # Applies to both pipeline modes (this node serves monitoring and
    # --post-training). Fails loud — never silently evaluates the whole table.
    snap_date = str(eval_params.get("snap_date") or "").strip()
    if not snap_date:
        raise ValueError(
            "evaluation.snap_date not configured. Set evaluation.snap_date "
            "(ISO YYYY-MM-DD) in conf/base/parameters_evaluation.yaml."
        )
    logger.info("Filtering predictions to snap_date=%s", snap_date)
    predictions_at_snap = ranked_predictions.filter(
        F.col(time_col).cast("string") == snap_date
    )
    if predictions_at_snap.isEmpty():
        available = sorted(
            str(r[time_col])
            for r in ranked_predictions.select(time_col).distinct().collect()
        )
        raise ValueError(
            f"No predictions found for evaluation.snap_date={snap_date!r} "
            f"(model_version={model_version}). snap_dates present in "
            f"predictions: {available}"
        )
    ranked_predictions = predictions_at_snap

    # Filter labels to snap_dates in predictions
    pred_snap_dates = ranked_predictions.select(time_col).distinct()
    labels = labels.join(pred_snap_dates, on=time_col, how="inner")

    # In --post-training mode the predictions source is training_eval_predictions,
    # which already stores `label` alongside `score` (written by the training
    # `predict` node). The merge join below keys on identity_cols only, so a
    # `label` on the label_table side would survive as a second `label` column
    # -> AnalysisException: reference 'label' is ambiguous. Drop it from the
    # label_table side: the predictions table's own label is exactly what the
    # model's test mAP was scored against, keeping post-training metrics
    # consistent with the training pipeline. The label_table join is still
    # required for segment columns. Monitoring mode (ranked_predictions) has no
    # `label`, so the condition is False there and behaviour is unchanged.
    if label_col in ranked_predictions.columns and label_col in labels.columns:
        labels = labels.drop(label_col)
        logger.info(
            "prepare_eval_data: predictions already carry '%s'; dropped it "
            "from the label_table side to avoid an ambiguous join column",
            label_col,
        )

    # LEFT JOIN — preserve every prediction row so per-customer ranking is over
    # the model's full candidate set (in dev: cust × 8 prod) regardless of
    # whether label_table covers that (cust, prod) pair. label_table's
    # per-group cust_pool semantics (conf/sql/etl/label/label_{ccard,exchange,
    # fund}.sql; cust must have ≥1 apply event in the group to appear) means
    # an INNER JOIN here would silently shrink each customer's rank set to
    # their per-group sub-products, collapsing baseline / mAP metrics to a
    # per-group framing the business model never asked for. Missing labels are
    # filled with 0 ("not bought"), matching the existing build_model_input
    # convention (preprocessing/_spark.py:369-372 LEFT + COALESCE(0)).
    eval_predictions = ranked_predictions.join(labels, on=identity_cols, how="left")
    if label_col in eval_predictions.columns:
        eval_predictions = eval_predictions.fillna({label_col: 0})

    # Downstream report rendering selects schema["rank"] from eval_predictions.
    # When the predictions source is
    # training_eval_predictions (--post-training mode), `rank` is absent because
    # the table no longer stores it (Spark mAP recomputes rank internally via
    # rank_within_query). Add it here when missing so downstream stays uniform;
    # when present (ranked_predictions source), trust the upstream value.
    rank_col = schema["rank"]
    if rank_col not in eval_predictions.columns:
        from recsys_tfb.evaluation.metrics_spark import rank_within_query
        score_col = schema["score"]
        entity_cols = schema["entity"]
        query_cols = [time_col] + entity_cols
        # rank_within_query adds a "pos" 1-based rank within each
        # (snap_date, cust_id), ordered by score desc.
        eval_predictions = rank_within_query(eval_predictions, query_cols, score_col)
        eval_predictions = eval_predictions.withColumnRenamed("pos", rank_col)
        logger.info(
            "prepare_eval_data: injected '%s' column via rank_within_query "
            "(predictions source did not provide it)",
            rank_col,
        )

    # Join segment sources onto the final eval table (Hive-table sources;
    # source seam inside segments). Done here — not on label_table — so the
    # label side stays minimal and segment columns are a pure enrichment.
    segment_sources = eval_params.get("segment_sources", {})
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        eval_predictions = join_segment_sources(eval_predictions, segment_sources)

    logger.info("Eval data prepared via Spark join")
    return eval_predictions


def draw_diagnosis_sample_node(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> Optional[tuple]:
    """Draw the shared driver-side diagnosis sample ONCE per run.

    ``compute_metric_ci`` / ``compute_offset_sweep`` / ``compute_pair_ledger``
    all consume this single sample instead of each re-drawing it (same seed ->
    identical content; 3 Spark scans collapse to 1). Returns ``None`` when none
    of the three consumers is enabled — matching the previous behaviour of
    drawing zero samples in that case.
    """
    ci_on, sweep_on, ledger_on = _sample_consumer_flags(parameters)
    if not (ci_on or sweep_on or ledger_on):
        logger.info(
            "diagnosis sample: all consumers (ci/offset_sweep/pair_ledger) "
            "disabled — skipping sample draw"
        )
        return None

    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
    sample_pdf, sample_meta = draw_diagnosis_sample(eval_predictions, parameters)
    logger.info(
        "diagnosis sample drawn once for %d consumer(s): %d queries sampled "
        "(shared by metric_ci/offset_sweep/pair_ledger)",
        sum((ci_on, sweep_on, ledger_on)), sample_meta["n_queries_sampled"],
    )
    return sample_pdf, sample_meta


def compute_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking metrics using the Spark-native pipeline.

    Thin wrapper over `evaluation.metrics_spark.compute_all_metrics`. All
    row-level work stays in Spark; only small aggregated dicts are collected.
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    result = compute_all_metrics(eval_predictions, parameters)
    logger.info(
        "Spark metrics computed: n_queries=%d, n_excluded=%d",
        result["n_queries"],
        result["n_excluded_queries"],
    )
    return result


def compute_baseline_metrics(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> Optional[dict]:
    """Popularity-baseline metrics, aligned row-for-row with eval_predictions.

    Re-scores each eval_predictions row with the product's historical
    purchase count, then runs the slim metrics path (overall + per_item).
    Returns None when the baseline report section is disabled — the second
    metrics pass is then skipped entirely.

    Returns dict with keys:
      - overall:        dict[str, float]   slim metrics
      - per_item:       dict[str, dict]    per-product slim metrics
      - purchase_counts: dict[str, int]    per-product popularity count
            aggregated across eval snap_dates (sum). Drives the report's
            popularity-composition table; consumers must treat absence
            as backward-compatible (older results may omit it).
    """
    from recsys_tfb.evaluation.baselines import (
        build_baseline_frame,
        compute_purchase_counts,
    )
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    eval_params = parameters.get("evaluation", {}) or {}
    sections = (eval_params.get("report", {}) or {}).get("sections", {}) or {}
    if not sections.get("baseline", True):
        logger.info(
            "Baseline report section disabled — skipping baseline metrics"
        )
        return None

    schema = get_schema(parameters)
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    lookback_months = (eval_params.get("baseline", {}) or {}).get(
        "lookback_months", 12
    )

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
    baseline_frame = build_baseline_frame(eval_predictions, counts, parameters)
    metrics = compute_overall_per_item(baseline_frame, parameters)
    metrics["purchase_counts"] = purchase_counts
    logger.info(
        "Baseline metrics computed (overall + per_item) for snap_dates=%s; "
        "purchase_counts has %d products",
        snap_dates, len(purchase_counts),
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
    """
    eval_params = parameters.get("evaluation", {}) or {}
    ci_cfg = ((eval_params.get("diagnosis", {}) or {}).get("ci", {}) or {})
    if not ci_cfg.get("enabled", True):
        logger.info("metric CI disabled — writing stub")
        return {"enabled": False}

    if diagnosis_sample is None:
        raise ValueError(
            "compute_metric_ci: diagnosis_sample is None while "
            "evaluation.diagnosis.ci.enabled is true — draw_diagnosis_sample_node "
            "gate is out of sync with the consumer enable flag"
        )

    from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

    sample_pdf, sample_meta = diagnosis_sample
    out = bootstrap_per_item_ci(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "metric CI computed on %d sampled queries (n_boot=%d)",
        sample_meta["n_queries_sampled"], out["n_boot"],
    )
    return out


def compute_reconciliation(
    eval_predictions: Optional[SparkDataFrame],
    parameters: dict,
) -> dict:
    """對帳層薄 node（spec §3 Phase 2）：理論偏移 vs 實測校準差距。

    領域邏輯全在 ``diagnosis.metric.reconciliation``。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("reconciliation", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("reconciliation disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None:
        raise ValueError(
            "compute_reconciliation: eval_predictions is required when "
            "evaluation.diagnosis.reconciliation.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.reconciliation import reconcile
    out = reconcile(eval_predictions, parameters)
    logger.info(
        "reconciliation computed: %d items, all_explained=%s (score_col=%s)",
        len(out["by_item"]), out["all_explained"], out["score_col_used"],
    )
    return out


def compute_quadrant(
    eval_predictions: Optional[SparkDataFrame],
    label_table: Optional[SparkDataFrame],
    metric_ci: Optional[dict],
    reconciliation: Optional[dict],
    parameters: dict,
) -> dict:
    """象限層薄 node（框架診斷項目 3/5/10）。

    領域邏輯全在 ``diagnosis.metric.quadrant``。停用時寫 stub；上游診斷
    產物（metric_ci/reconciliation）是停用 stub 時 best-effort 降級不失敗。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {}).get("quadrant", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("quadrant disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None or label_table is None:
        raise ValueError(
            "compute_quadrant: eval_predictions and label_table are required "
            "when evaluation.diagnosis.quadrant.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary
    out = build_quadrant_summary(
        eval_predictions, label_table, metric_ci, reconciliation, parameters
    )
    logger.info(
        "quadrant computed: %d items, %d aggressors",
        len(out["by_item"]),
        sum(1 for v in out["by_item"].values() if v["is_aggressor"]),
    )
    return out


def compute_offset_sweep(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """分流層薄 node（spec §3 Phase 4；框架診斷項目 6）。

    領域邏輯全在 ``diagnosis.metric.offset_sweep``（driver 端 numpy）。抽樣改由
    ``draw_diagnosis_sample_node`` 共用（同 seed→內容相同）。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("offset_sweep", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("offset sweep disabled — writing stub")
        return {"enabled": False}
    if diagnosis_sample is None:
        raise ValueError(
            "compute_offset_sweep: diagnosis_sample is None while "
            "evaluation.diagnosis.offset_sweep.enabled is true — "
            "draw_diagnosis_sample_node gate out of sync with the consumer flag"
        )
    from recsys_tfb.diagnosis.metric.offset_sweep import sweep

    sample_pdf, sample_meta = diagnosis_sample
    out = sweep(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "offset sweep computed: %d items, rounds=%d converged=%s, "
        "holdout mAP zero=%s star=%s",
        len(out.get("delta_star", {})), out.get("n_rounds_run"),
        out.get("converged"),
        (out.get("map_holdout") or {}).get("zero"),
        (out.get("map_holdout") or {}).get("star"),
    )
    return out


def compute_pair_ledger(
    diagnosis_sample: Optional[tuple],
    parameters: dict,
) -> dict:
    """壓制帳本薄 node（spec §3 Phase 4b；框架診斷項目 7）。

    領域邏輯全在 ``diagnosis.metric.pair_ledger``（driver 端 numpy）。抽樣改由
    ``draw_diagnosis_sample_node`` 共用（同 seed→內容相同）。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("pair_ledger", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("pair ledger disabled — writing stub")
        return {"enabled": False}
    if diagnosis_sample is None:
        raise ValueError(
            "compute_pair_ledger: diagnosis_sample is None while "
            "evaluation.diagnosis.pair_ledger.enabled is true — "
            "draw_diagnosis_sample_node gate out of sync with the consumer flag"
        )
    from recsys_tfb.diagnosis.metric.pair_ledger import pair_ledger

    sample_pdf, sample_meta = diagnosis_sample
    out = pair_ledger(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "pair ledger computed: %d mis-ordered pairs, %d suppressors, "
        "map_current=%s",
        out.get("n_mis_ordered_pairs", 0),
        len(out.get("by_suppressor", {})),
        out.get("map_current"),
    )
    return out


def assemble_triage_summary(quadrant: Optional[dict], reconciliation: Optional[dict],
                            offset_sweep: Optional[dict], gain_ledger: Optional[dict],
                            parameters: dict) -> dict:
    """Triage 總表 node：純 dict 合成，gain_ledger 缺席 best-effort 降級。"""
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    if not (diag.get("triage", {}) or {}).get("enabled", True):
        return {"enabled": False}
    from recsys_tfb.diagnosis.metric.triage import triage
    return triage(quadrant, reconciliation, offset_sweep, gain_ledger, parameters)


def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
    metric_ci: Optional[dict] = None,
    reconciliation: Optional[dict] = None,
    quadrant: Optional[dict] = None,
    offset_sweep: Optional[dict] = None,
    pair_ledger: Optional[dict] = None,
    triage: Optional[dict] = None,
) -> str:
    """Build the HTML report. Metrics dicts drive §0–§8; the diagnostics
    section (when enabled) is aggregated in Spark into small frames so its
    figures embed bounded summaries rather than raw per-row arrays.
    """
    schema = get_schema(parameters)
    score_col = schema["score"]
    rank_col = schema["rank"]
    label_col = schema["label"]
    item_col = schema["item"]

    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}

    diagnostics_frames = None
    if sections_cfg.get("diagnostics", True):
        # Aggregate diagnostics in Spark so each figure embeds bounded summaries
        # (bin counts / quartiles / rank matrices), not raw per-row arrays.
        # eval_predictions is scanned once per aggregation, so project the
        # needed columns and cache before the multiple passes.
        needed = list(
            dict.fromkeys([item_col, score_col, rank_col, label_col])
        )
        sdf = eval_predictions.select(*needed).cache()
        figs = []
        if diag_cfg.get("include_distributions", True):
            figs.append(plot_score_histogram(
                score_histogram_counts(sdf, item_col, score_col),
                item_col=item_col,
            ))
            figs.append(plot_score_boxplot(
                score_box_stats(sdf, item_col, score_col),
                item_col=item_col,
            ))
            figs.append(plot_score_boxplot_by_label(
                score_box_stats_by_label(sdf, item_col, score_col, label_col),
                item_col=item_col, label_col=label_col,
            ))
            figs.append(plot_rank_heatmap(
                rank_count_matrix(sdf, item_col, rank_col)
            ))
            figs.append(plot_positive_rank_heatmap(
                positive_rank_count_matrix(sdf, item_col, rank_col, label_col)
            ))
            figs.append(plot_positive_rate_rank_heatmap(
                positive_rate_matrix(sdf, item_col, rank_col, label_col)
            ))
        if diag_cfg.get("include_calibration", True):
            figs.append(plot_calibration_curves(
                calibration_bins(
                    sdf, item_col, score_col, label_col,
                    n_bins=diag_cfg.get("n_calibration_bins", 10),
                ),
                item_col=item_col,
            ))
        sdf.unpersist()
        diagnostics_frames = {"figures": figs}

    return assemble_report(
        evaluation_metrics, parameters,
        baseline_metrics=baseline_metrics,
        diagnostics_frames=diagnostics_frames,
        metric_ci=metric_ci,
        reconciliation=reconciliation,
        quadrant=quadrant,
        offset_sweep=offset_sweep,
        pair_ledger=pair_ledger,
        triage=triage,
    )

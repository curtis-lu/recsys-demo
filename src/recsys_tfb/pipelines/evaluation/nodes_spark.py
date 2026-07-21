"""Evaluation pipeline nodes — Spark backend."""

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.diagnostics_spark import aggregate_report_diagnostics
from recsys_tfb.evaluation.report_builder import (
    assemble_diagnosis_pages,
    assemble_report,
    build_diagnostics_figures,
)

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


def _registry_diagnosis_enabled(parameters: dict) -> bool:
    """registry 診斷（``contract.DIAGNOSES``）有任一啟用嗎。

    與 ``_sample_consumer_flags`` 分開的理由：舊三項（ci／offset_sweep／
    pair_ledger）是即將被取代的既有診斷、新五項走 registry，兩組生命週期
    不同。合在一起的話 Plan 2–5 每加一項診斷都要改所有解包點，而那正是
    registry 要消除的東西——所以這裡回一個 bool，不回擴增的 tuple。

    鍵與預設值必須跟各消費節點自己讀的完全一致（``enabled``，預設 True），
    否則閘門與消費端會漂移：使用者關掉舊三項、只開一項 registry 診斷時，
    樣本不會被抽，消費節點拿到 None 而 fail-loud。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    return any(
        bool((diag.get(name, {}) or {}).get("enabled", True))
        for name in DIAGNOSES
    )


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
    plus every registry diagnosis (``contract.DIAGNOSES``, e.g.
    ``diagnose_config_shift``) all consume this single sample instead of each
    re-drawing it (same seed -> identical content; N Spark scans collapse to
    1). Sharing one sample is also a correctness property, not just a speed
    one: numbers computed on different populations must not be read side by
    side. Returns ``None`` only when *every* consumer is disabled.
    """
    ci_on, sweep_on, ledger_on = _sample_consumer_flags(parameters)
    registry_on = _registry_diagnosis_enabled(parameters)
    if not (ci_on or sweep_on or ledger_on or registry_on):
        logger.info(
            "diagnosis sample: all consumers (ci/offset_sweep/pair_ledger + "
            "registry diagnoses) disabled — skipping sample draw"
        )
        return None

    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
    sample_pdf, sample_meta = draw_diagnosis_sample(eval_predictions, parameters)
    # deep=False keeps this a free observation: rows/cols are exact and the
    # bytes figure is a shallow estimate. deep=True would scan every string
    # cell (O(n_cells)) on the already-materialised sample — accurate but not
    # "free", which is the constraint for this always-on instrumentation.
    log_data_volume(logger, "diagnosis.sample_pdf", sample_pdf, deep=False)
    logger.info(
        "diagnosis sample drawn once for %d legacy consumer(s) + registry "
        "diagnoses(enabled=%s): %d queries sampled",
        sum((ci_on, sweep_on, ledger_on)), registry_on,
        sample_meta["n_queries_sampled"],
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


def make_diagnosis_node(name: str):
    """為 registry 裡的一項診斷造一個薄 node 函式。

    Plan 2-5 的五項診斷 node 長得一模一樣：讀 ``enabled``、停用寫 stub、樣本是
    ``None`` 就 fail-loud、否則轉呼叫模組的 ``compute``。手寫五份的問題不是
    行數，是那五份會各自漂移——尤其「停用時回什麼」與「樣本 None 時 raise 還
    是靜默」這兩件事，寫錯了 pipeline 照樣跑得完。

    registry 診斷的 ``compute`` 吃的是整個 ``diagnosis_sample`` tuple
    （``(sample_pdf, sample_meta)``），不是解包後的 ``sample_pdf``——契約在
    ``diagnosis.metric.contract._SIGNATURES`` 釘住。

    ``__name__`` 明設：``Node.name`` 預設取 ``func.__name__``
    （``core/node.py:8``），不設的話五個 node 同名，``--only-node`` 指不到、
    log 分不出誰是誰，而 pipeline 照樣跑得完。
    """
    def _run(diagnosis_sample: Optional[tuple], parameters: dict) -> dict:
        cfg = (((parameters.get("evaluation", {}) or {})
                .get("diagnosis", {}) or {}).get(name, {}) or {})
        if not cfg.get("enabled", True):
            logger.info("%s disabled — writing stub", name)
            return {"enabled": False}
        if diagnosis_sample is None:
            raise ValueError(
                f"diagnose_{name}: diagnosis_sample is None while "
                f"evaluation.diagnosis.{name}.enabled is true — "
                "draw_diagnosis_sample_node gate out of sync with the "
                "consumer flag"
            )
        import importlib

        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        out = mod.compute(diagnosis_sample, parameters)
        # 純量鍵通用地印出來，不為每項診斷各寫一句摘要：那樣 Plan 2-5 每加
        # 一項就要多一段格式化字串，而它們沒有任何測試守著格式。
        scalars = {
            k: v for k, v in out.items()
            if isinstance(v, (int, float, str, bool))
        }
        logger.info("%s computed: %s", name, scalars)
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

    退回 ``evaluation.snap_date`` 是給單元測試用的（那裡沒有 runtime_params）；
    dash 一律剝掉，因為 catalog 拿到的就是剝過的值。
    """
    from pathlib import Path

    eval_params = parameters.get("evaluation", {}) or {}
    snap = parameters.get("snap_date") or eval_params.get("snap_date", "unknown")
    return (Path("data") / "evaluation"
            / str(parameters.get("model_version", "unknown"))
            / str(snap).replace("-", "")
            / "diagnosis")


def render_diagnosis_pages(parameters: dict, *_dag_deps) -> list[str]:
    """把已落地的診斷 JSON 組成多頁 HTML，回傳寫出的檔案路徑。

    ``*_dag_deps`` **刻意不讀值**。它們是 ``evaluation_<name>`` 那些診斷產物，
    在這裡只當 DAG 的 happens-before 邊，買到兩件事：

    1. **執行順序**（主要理由）。拓撲排序只看 ``node.inputs``
       （``core/pipeline.py:69-73``）。只宣告 ``parameters`` 的話這個 node
       的 in-degree 是 0——``parameters`` 沒有生產者——於是 Kahn 會把它排在
       **診斷節點之前**，整條 pipeline 正常跑時它就會先執行，讀到上一次執行
       留下的舊 JSON，或者什麼都讀不到。而它照樣「成功」。
    2. **切片擴張**。``Pipeline._slice_with_expansion``（``core/pipeline.py:154``）
       沿 ``node.inputs`` 往上找生產者，只在 ``can_load`` 為 False 時拉進來。
       所以 ``--only-node render_diagnosis_pages`` 在**診斷 JSON 已落地**時
       不會重算（那正是想要的行為：便宜地重繪），在 JSON 不存在時（全新
       model_version、或清過檔）才自動補算。

    結果本身按**檔名**讀（``diagnosis/<name>.json``），與離線工具
    ``scripts/render_diagnosis.py`` 共用 ``diagnosis.metric.results.load_results``。
    為什麼不用位置對應：Plan 2-5 每加一項診斷都要補一條 ``catalog.yaml``
    entry；忘了補的話位置對應會安靜地走記憶體——頁面正常產出、磁碟上卻沒有那
    份 JSON，離線重繪少一頁而沒有任何訊息。按檔名讀則當場進 ``missing``。

    **刻意不吞例外**：寫頁失敗直接紅，比「報表產出了、但少了診斷入口」好認
    ——後者要比對兩次執行的 HTML 才看得出來。
    """
    from recsys_tfb.diagnosis.metric.results import load_results

    out_dir = _diagnosis_pages_dir(parameters)
    results, missing, unknown = load_results(out_dir)
    if missing:
        logger.info(
            "diagnosis results not on disk, no page for: %s",
            ", ".join(missing),
        )
    if unknown:
        logger.info(
            "JSON files outside the diagnosis registry, ignored: %s",
            ", ".join(unknown),
        )
    pages = assemble_diagnosis_pages(results, parameters, out_dir)
    logger.info(
        "diagnosis pages written to %s (%d files from %d results)",
        out_dir, len(pages), len(results),
    )
    return [str(p) for p in pages]


def compute_report_aggregates(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """主報表診斷區的 Spark 聚合，落地成 JSON。

    從 ``generate_report`` 拆出來（Plan 1.5）。理由不只是效能：它讓
    ``generate_report`` 變成純函式，主報表因此能離線重繪；也把這 6 次全掃的
    失敗點從 pipeline 的**最後一個 node** 往上游移。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}
    if not sections_cfg.get("diagnostics", True):
        logger.info("report diagnostics section disabled — writing stub")
        return {"enabled": False}

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
    return out


def generate_report(
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict],
    metric_ci: Optional[dict],
    offset_sweep: Optional[dict],
    pair_ledger: Optional[dict],
    report_aggregates: Optional[dict],
    diagnosis_pages: Optional[list],
) -> str:
    """Build the HTML report. Metrics dicts drive §0–§8; the diagnostics
    section (when enabled) reads the already-aggregated Spark JSON from
    ``compute_report_aggregates`` (Plan 1.5) so this function stays pure —
    no SparkDataFrame in the signature, no Spark action in the body.

    診斷頁由 ``render_diagnosis_pages`` 產生（Plan 1.5 拆出），這裡只收它回傳
    的路徑清單、放一個連結進主報表。
    """
    figures = build_diagnostics_figures(report_aggregates)
    diagnostics_frames = {"figures": figures} if figures else None

    return assemble_report(
        evaluation_metrics, parameters,
        baseline_metrics=baseline_metrics,
        diagnostics_frames=diagnostics_frames,
        metric_ci=metric_ci,
        offset_sweep=offset_sweep,
        pair_ledger=pair_ledger,
        diagnosis_pages=diagnosis_pages,
    )

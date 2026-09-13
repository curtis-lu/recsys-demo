"""Evaluation pipeline nodes — Spark backend."""

import logging
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.config_fingerprint import (
    LoadedArtifact,
    fingerprint,
    require_computed_with_current_config,
)
from recsys_tfb.evaluation.diagnostics_spark import aggregate_report_diagnostics
from recsys_tfb.evaluation.report_builder import (
    assemble_diagnosis_pages,
    assemble_report,
)

logger = logging.getLogger(__name__)


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

    ``contract.DIAGNOSES`` 走**模組屬性**存取（``contract.DIAGNOSES``），不是
    ``from ... import DIAGNOSES``。兩種寫法在這裡都能被 monkeypatch（本函式
    的 import 在呼叫當下才執行，兩者都是每次呼叫重新解析），但模組屬性存取
    讀起來更明確是「當下的 registry 值」，不必先確認 import 是模組層級還是
    函式內才敢信任 monkeypatch 生效。
    """
    import importlib

    from recsys_tfb.diagnosis.metric import contract

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


def prepare_eval_data(
    ranked_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Join ranked predictions with labels using Spark.

    For external segment sources, delegates to
    ``segments.join_segment_sources`` (storage backend isolated behind its
    source seam).

    Pre-check (input): ``label_table`` has no duplicated identity key in the
    evaluated month; raises with the number of duplicated keys (why it raises
    rather than deduplicating is written at the check).
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
            f"{identity_cols} at evaluation.snap_date={snap_date!r}. Each extra "
            f"row would copy its prediction row in the join with the "
            f"predictions, inflating that query's candidates and shifting its "
            f"ranks. Deduplicate label_table upstream; evaluation does not "
            f"pick one of the rows for you."
        )

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

    # Join segment sources onto the final eval table (Hive-table sources;
    # source seam inside segments). Done here — not on label_table — so the
    # label side stays minimal and segment columns are a pure enrichment.
    segment_sources = eval_params.get("segment_sources", {})
    if segment_sources:
        from recsys_tfb.evaluation.segments import join_segment_sources
        eval_predictions = join_segment_sources(eval_predictions, segment_sources)

    logger.info("Eval data prepared via Spark join")
    return eval_predictions


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

        from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
        sample_pdf, sample_meta = draw_diagnosis_sample(
            eval_predictions, parameters
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
      - config_fingerprint: the computed settings it was made with
            (``evaluation.config_fingerprint``), checked by
            ``generate_report``.
    """
    from recsys_tfb.evaluation.baselines import (
        build_baseline_frame,
        compute_monthly_purchase_counts,
        compute_purchase_counts,
    )
    from recsys_tfb.evaluation.metrics_spark import compute_overall_per_item

    eval_params = parameters.get("evaluation", {}) or {}
    sections = (eval_params.get("report", {}) or {}).get("sections", {}) or {}
    if not sections.get("baseline", True):
        logger.info(
            "Baseline report section disabled — skipping baseline metrics"
        )
        return {"enabled": False, "config_fingerprint": fingerprint(parameters)}

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
    # Per-(month, item) breakdown of the same windows → report's monthly
    # popularity trend. Summed over months, each item reconciles with
    # purchase_counts (both sum the same per-snap-per-month counts).
    monthly = compute_monthly_purchase_counts(
        label_table, snap_dates, lookback_months, parameters
    )
    monthly_counts: dict[str, dict[str, int]] = {}
    for r in (
        monthly.groupBy("month", item_col)
        .agg(F.sum(F.col(score_col)).alias(score_col))
        .collect()
    ):
        monthly_counts.setdefault(str(r[item_col]), {})[str(r["month"])] = int(
            r[score_col]
        )
    baseline_frame = build_baseline_frame(eval_predictions, counts, parameters)
    # per_segment / category slices for the report's by-segment / 大類 vs
    # baseline comparison. Gated by the same config that turns them on for the
    # model (segment_columns present / item_categories maps items), so the
    # baseline pays for a slice only when the model already computed its match.
    metrics = compute_overall_per_item(
        baseline_frame,
        parameters,
        with_segment=bool(eval_params.get("segment_columns")),
        with_category=True,
    )
    metrics["purchase_counts"] = purchase_counts
    metrics["monthly_counts"] = monthly_counts
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

    from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

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
        import importlib

        from recsys_tfb.diagnosis.metric import contract

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
    import importlib

    from recsys_tfb.diagnosis.metric import contract

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
    parameters: dict,
) -> dict:
    """主報表診斷區的 Spark 聚合，落地成 JSON。

    從 ``generate_report`` 拆出來（Plan 1.5）。理由不只是效能：它讓
    ``generate_report`` 變成純函式，主報表因此能離線重繪；也把這 6 次全掃的
    失敗點從 pipeline 的**最後一個 node** 往上游移。

    Both the stub and the full result carry ``config_fingerprint``: the JSON
    lands, and ``generate_report`` refuses one computed under other settings.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}
    if not sections_cfg.get("diagnostics", True):
        logger.info("report diagnostics section disabled — writing stub")
        return {"enabled": False, "config_fingerprint": fingerprint(parameters)}

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

    Pre-check (inputs): ``baseline_metrics``, ``metric_ci`` and
    ``report_aggregates`` were computed with the current computed settings
    (``evaluation.config_fingerprint``). ``--only-node generate_report``
    stops at "the JSON exists", so without this a setting changed since the
    last run is drawn from the old JSON with exit code 0 (ADR-0020 bug 2);
    with it the run raises, naming the key and the node to ``--from-node``.
    Only computed settings count, so changing ``report.display.*`` and the
    other drawn keys still redraws in seconds.

    Not in the check: ``evaluation_metrics``, which is memory-only and
    carries no fingerprint yet (it is recomputed on every slice that reaches
    this node; it joins the list once #339 lands it), and
    ``diagnosis_pages``, a list of paths whose sources
    ``render_diagnosis_pages`` has already checked.
    """
    require_computed_with_current_config(
        [
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

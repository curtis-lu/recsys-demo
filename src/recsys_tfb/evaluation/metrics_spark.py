"""Spark-native ranking metrics — first-principles design.

Layered structure (each layer has a single responsibility):

    Layer 1: row-level enrichment              (Spark DF → Spark DF, +columns)
        rank_within_query              (Window: pos)
        add_query_total_rel            (Window: total_rel)
        add_row_contributions          (cum_rel, prec_at_pos, dcg_term,
                                        top_k@K, ap_contrib@K, ndcg_contrib@K)

    Layer 2: per-query metrics                 (Spark DF → Spark DF)
        compute_per_query_metrics      one row per query;
                                       columns: group_cols, total_rel,
                                                map@K, ndcg@K,
                                                precision@K, recall@K

    Layer 3: aggregations                      (Spark DF → Python dict)
        aggregate_overall              query-equal-weight mean of per-query
        aggregate_per_segment          query-equal-weight mean within segment
        aggregate_per_item             row-equal-weight mean over P-positive
                                       rows; emits hit_rate@K / map_attr@K /
                                       ndcg_attr@K / mean_pos

    Layer 4: orchestrator
        compute_all_metrics            wires everything; reads parameters

Naming convention (intentional, NOT pandas-mirroring):
    * ``@K`` keys (map@K, ndcg@K, precision@K, recall@K) live in
      ``overall`` / ``per_segment`` — they are *per-query metrics aggregated
      across queries*. K names the per-query truncation point.
    * ``_attr@K`` keys (map_attr@K, ndcg_attr@K) live in ``per_item`` —
      they are *per-row attribution contributions averaged across all rows
      where the item appears as a positive*. The "attribution" suffix marks
      them as fragments of per-query metrics, not per-query metrics
      themselves.
    * ``hit_rate@K`` (per_item) is the marginal recall of the item:
      ``P(rank(item) ≤ K | item is positive)``. Same conditional probability
      as ``recall@K`` in ``overall``, but aggregated row-equal-weight rather
      than query-equal-weight; the different key name disambiguates.
    * ``mean_pos`` (per_item) is the mean rank position over P-positive rows.

Degenerate cases (documented, not pruned):
    At ``K >= n_products`` every row has ``top_k@K == 1`` →
        ``precision@K`` collapses to the per-query base rate
                       ``total_rel / n_products``.  Mean across queries gives
                       segment / overall base rate. Useful as a sanity check
                       for label density but NOT a ranking metric.
        ``recall@K``   collapses to ``1.0`` for every query (all positives
                       are necessarily ranked in top-all). Mean across
                       queries == 1.0.
    Both are retained in output for compatibility with downstream tooling
    that iterates ``k_values``, but consumers must be aware of these
    interpretations.

All row-level work stays in Spark; only the final small per-dim dicts are
collected to the driver.
"""

from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import macro_from_per_item

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# k_values resolution
# ---------------------------------------------------------------------------


def _resolve_k_values(raw: Iterable, n_products: int) -> list[int]:
    """Resolve mixed int / 'all' k_values to a sorted unique int list.

    'all' (case-insensitive) resolves to ``n_products``. Duplicates after
    resolution are collapsed.
    """
    out: set[int] = set()
    for k in raw:
        if isinstance(k, str) and k.lower() == "all":
            out.add(n_products)
        else:
            out.add(int(k))
    return sorted(out)


def _build_category_mapping(parameters: dict) -> dict[str, str] | None:
    """Resolve {prod_name: category}. None when categories disabled.

    Fail-loud (ValueError) if a mapped product is not in
    ``schema.categorical_values[item_col]``. Products absent from every
    mapping list become their own singleton category when
    ``unmapped == 'singleton'`` (the only supported mode).
    """
    eval_params = parameters.get("evaluation", {}) or {}
    pc = eval_params.get("product_categories", {}) or {}
    if not pc.get("enabled"):
        return None

    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
    known_set = set(known)

    mapping: dict[str, str] = {}
    for category, prods in (pc.get("mapping", {}) or {}).items():
        for prod in prods:
            if prod not in known_set:
                raise ValueError(
                    f"product_categories.mapping references unknown product "
                    f"'{prod}' (not in schema.categorical_values['{item_col}'])"
                )
            mapping[prod] = category

    unmapped = pc.get("unmapped", "singleton")
    if unmapped != "singleton":
        raise ValueError(
            f"product_categories.unmapped='{unmapped}' unsupported; "
            f"only 'singleton' is implemented"
        )
    for prod in known:
        mapping.setdefault(prod, prod)
    return mapping


def collapse_to_categories(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> SparkDataFrame:
    """Collapse fine-grained predictions to category grain (no UDF).

    For each (time, entity..., category): score = max(child score),
    label = max(child label), segment columns via F.first. The category
    column is emitted under the schema item_col name so the collapsed DF
    is shape-compatible with compute_all_metrics. ``max(score)`` re-ranking
    is equivalent to taking the best child rank (pos is score-desc derived).
    """
    mapping = _build_category_mapping(parameters)
    if mapping is None:
        raise ValueError("collapse_to_categories called with categories disabled")

    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {}) or {}
    segment_columns = [
        c for c in (eval_params.get("segment_columns", []) or [])
        if c in eval_predictions.columns
    ]

    spark = eval_predictions.sparkSession
    map_rows = [(p, c) for p, c in mapping.items()]
    map_df = spark.createDataFrame(map_rows, [item_col, "_category"])

    joined = eval_predictions.join(F.broadcast(map_df), on=item_col, how="inner")

    aggs = [
        F.max(F.col(score_col)).alias(score_col),
        F.max(F.col(label_col)).alias(label_col),
    ]
    for seg in segment_columns:
        aggs.append(F.first(F.col(seg)).alias(seg))

    collapsed = (
        joined.groupBy(*group_cols, "_category")
        .agg(*aggs)
        .withColumnRenamed("_category", item_col)
    )
    return collapsed


def compute_dataset_overview(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    item_col_override: str | None = None,
) -> dict:
    """Dataset profiling for the report §1. Pure Spark agg, small collect.

    ``item_col_override`` lets the caller profile the collapsed
    category-grain DF (item column still named after schema item_col, but
    semantics = category).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = item_col_override or schema["item"]
    label_col = schema["label"]

    n_rows = eval_predictions.count()
    n_customers = eval_predictions.select(*entity_cols).distinct().count()
    n_products = eval_predictions.select(item_col).distinct().count()
    n_snap_dates = eval_predictions.select(time_col).distinct().count()
    n_positives = int(
        eval_predictions.agg(F.sum(F.col(label_col))).collect()[0][0] or 0
    )
    positive_rate = (n_positives / n_rows) if n_rows else 0.0
    avg_pos_per_customer = (n_positives / n_customers) if n_customers else 0.0

    def _group(col: str) -> dict:
        rows = (
            eval_predictions.groupBy(col)
            .agg(
                F.count(F.lit(1)).alias("n_rows"),
                F.sum(F.col(label_col)).alias("n_positives"),
                F.countDistinct(*entity_cols).alias("n_customers"),
            )
            .collect()
        )
        out = {}
        for r in rows:
            key = r[col] if isinstance(r[col], str) else str(r[col])
            nr = int(r["n_rows"])
            npos = int(r["n_positives"] or 0)
            out[key] = {
                "n_rows": nr,
                "n_positives": npos,
                "n_customers": int(r["n_customers"]),
                "positive_rate": (npos / nr) if nr else 0.0,
            }
        return out

    return {
        "totals": {
            "n_rows": n_rows,
            "n_customers": n_customers,
            "n_products": n_products,
            "n_snap_dates": n_snap_dates,
            "n_positives": n_positives,
            "positive_rate": positive_rate,
            "avg_positives_per_customer": avg_pos_per_customer,
        },
        "by_snap_date": _group(time_col),
        "by_item": _group(item_col),
    }


# ---------------------------------------------------------------------------
# Layer 1 — row-level enrichment
# ---------------------------------------------------------------------------


def rank_within_query(
    df: SparkDataFrame, group_cols: list[str], score_col: str
) -> SparkDataFrame:
    """Assign ``pos``: 1-based rank within each ``group_cols`` group, by ``score`` desc.

    Tie-breaking among equal scores is undefined (Spark's row_number choice).
    """
    w = Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())
    return df.withColumn("pos", F.row_number().over(w))


def add_query_total_rel(
    df: SparkDataFrame, group_cols: list[str], label_col: str
) -> SparkDataFrame:
    """Add ``total_rel`` = sum(label) per query; constant within each query."""
    w = Window.partitionBy(*group_cols)
    return df.withColumn("total_rel", F.sum(F.col(label_col)).over(w))


def add_row_contributions(
    df: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> SparkDataFrame:
    """Add per-row contribution columns. Requires upstream ``pos`` + ``total_rel``.

    Always added:
        cum_rel       cumulative positives up to & including this position
        prec_at_pos   cum_rel / pos
        dcg_term      label / log2(pos + 1)

    Per K:
        top_k@K       1.0 if pos <= K else 0.0
        ap_contrib@K  prec_at_pos * label * top_k@K
                      (sum over a query's label=1 rows = numerator of AP@K)
        ndcg_contrib@K
                      (dcg_term * top_k@K) / iDCG@K
                      iDCG@K = sum_{i=1..min(total_rel, K)} 1 / log2(i + 1)
                      computed inline with Spark's aggregate(sequence(...));
                      no UDF, no collect-and-broadcast.
                      Sum over a query's rows = nDCG@K for that query.
    """
    w_cum = (
        Window.partitionBy(*group_cols)
        .orderBy(F.col("pos"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn("cum_rel", F.sum(F.col(label_col)).over(w_cum))
    df = df.withColumn("prec_at_pos", F.col("cum_rel") / F.col("pos"))
    df = df.withColumn(
        "dcg_term", F.col(label_col) / F.log2(F.col("pos") + F.lit(1))
    )

    for k in k_values:
        df = df.withColumn(
            f"top_k@{k}", (F.col("pos") <= F.lit(k)).cast("double")
        )
        df = df.withColumn(
            f"ap_contrib@{k}",
            F.col("prec_at_pos") * F.col(label_col) * F.col(f"top_k@{k}"),
        )
        idcg_at_k = F.aggregate(
            F.sequence(F.lit(1), F.least(F.col("total_rel"), F.lit(k))),
            F.lit(0.0),
            lambda acc, i: acc + F.lit(1.0) / F.log2(i.cast("double") + F.lit(1.0)),
        )
        df = df.withColumn(
            f"ndcg_contrib@{k}",
            F.when(
                idcg_at_k > 0,
                F.col("dcg_term") * F.col(f"top_k@{k}") / idcg_at_k,
            ).otherwise(F.lit(0.0)),
        )
    return df


# ---------------------------------------------------------------------------
# Layer 2 — per-query metrics
# ---------------------------------------------------------------------------


_PER_QUERY_KINDS = ("map", "ndcg", "precision", "recall")


def _per_query_metric_cols(k_values: list[int]) -> list[str]:
    """Names of metric columns produced by compute_per_query_metrics."""
    return [f"{kind}@{k}" for k in k_values for kind in _PER_QUERY_KINDS]


def compute_per_query_metrics(
    enriched: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
    carry_cols: list[str] | None = None,
) -> SparkDataFrame:
    """Reduce row-level contributions to one row per query.

    Output columns:
        group_cols, total_rel,
        map@K, ndcg@K, precision@K, recall@K for each K,
        plus any column in ``carry_cols`` (taken via F.first within the query —
        valid for per-customer attributes constant within a query, e.g. segment).

    Per-query formulas:
        map@K        = sum(ap_contrib@K)             / total_rel
        ndcg@K       = sum(ndcg_contrib@K)            -- already iDCG-normalized
        precision@K  = sum(label * top_k@K) / K
        recall@K     = sum(label * top_k@K) / total_rel

    See module docstring for the degenerate behaviour of precision@K and
    recall@K when K >= n_products.
    """
    carry_cols = list(carry_cols or [])

    sums = [F.first("total_rel").alias("total_rel")]
    for c in carry_cols:
        sums.append(F.first(c).alias(c))
    for k in k_values:
        sums.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(f"ndcg_contrib@{k}").alias(f"_ndcg_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )

    per_query = enriched.groupBy(*group_cols).agg(*sums)

    for k in k_values:
        per_query = (
            per_query
            .withColumn(f"map@{k}", F.col(f"_ap_sum_{k}") / F.col("total_rel"))
            .withColumn(f"ndcg@{k}", F.col(f"_ndcg_sum_{k}"))
            .withColumn(f"precision@{k}", F.col(f"_hits_{k}") / F.lit(k))
            .withColumn(f"recall@{k}", F.col(f"_hits_{k}") / F.col("total_rel"))
            .drop(f"_ap_sum_{k}", f"_ndcg_sum_{k}", f"_hits_{k}")
        )

    keep = list(group_cols) + ["total_rel"] + carry_cols + _per_query_metric_cols(k_values)
    return per_query.select(*keep)


# ---------------------------------------------------------------------------
# Layer 3 — aggregations
# ---------------------------------------------------------------------------


def aggregate_overall(
    per_query: SparkDataFrame, k_values: list[int]
) -> dict[str, float]:
    """Equal-query weight mean of per-query metrics.

    Returns flat dict ``{map@K, ndcg@K, precision@K, recall@K}`` for each K.
    Caller is responsible for interpreting precision@K / recall@K when
    K >= n_products (see module docstring).
    """
    metric_cols = _per_query_metric_cols(k_values)
    row = per_query.agg(*[F.mean(c).alias(c) for c in metric_cols]).collect()[0]
    return {c: float(row[c]) for c in metric_cols}


def aggregate_per_segment(
    per_query: SparkDataFrame, seg_col: str, k_values: list[int]
) -> dict[str, dict[str, float]]:
    """Equal-query weight mean of per-query metrics, grouped by ``seg_col``.

    ``per_query`` must carry ``seg_col`` (pass it via ``carry_cols`` when
    building per_query). Returns ``{seg_value: {map@K, ndcg@K, precision@K,
    recall@K}}``; the seg_value is stringified when not already a string.
    """
    metric_cols = _per_query_metric_cols(k_values)
    rows = (
        per_query.groupBy(seg_col)
        .agg(*[F.mean(c).alias(c) for c in metric_cols])
        .collect()
    )
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        raw_key = r[seg_col]
        key = raw_key if isinstance(raw_key, str) else str(raw_key)
        out[key] = {c: float(r[c]) for c in metric_cols}
    return out


def _per_item_metric_cols(k_values: list[int]) -> list[str]:
    """Metric column names emitted by aggregate_per_item (per K + mean_pos)."""
    cols = ["mean_pos"]
    for k in k_values:
        cols.extend([f"hit_rate@{k}", f"map_attr@{k}", f"ndcg_attr@{k}"])
    return cols


def aggregate_per_item(
    enriched: SparkDataFrame,
    dim_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict[str, dict[str, float]]:
    """Row-equal-weight mean of per-row contributions over label=1 rows.

    ``dim_cols`` can be ``[item_col]`` for per_item or
    ``[item_col, seg_col]`` for per_item_segment.

    Output per dim key (str-joined with '_' when multi-column):

        hit_rate@K   = mean(top_k@K) over P-positive rows
                       = P(rank(P) <= K | P is positive)
                         (item-level marginal recall; row-equal-weight)
        map_attr@K   = mean(ap_contrib@K) over P-positive rows
                       (per-row AP@K contribution averaged across all
                        queries where P is positive; carries cumulative-
                        precision weighting)
        ndcg_attr@K  = mean(ndcg_contrib@K) over P-positive rows
                       (per-row nDCG@K contribution; log-discount weighted,
                        normalized by query-level iDCG@K)
        mean_pos     = mean(pos) over P-positive rows
                       (average ranked position of P when it is the truth)
        n_pos        = count of P-positive rows (weight source for macro_average)

    NOTE: no precision@K / recall@K here on purpose. Their query-level
    definitions don't carry over cleanly to the per-item dimension
    (the K denominator is a per-query concept). Use ``hit_rate@K`` for
    the item-level recall analogue.
    """
    rel = enriched.filter(F.col(label_col) == 1)

    aggs = [
        F.mean(F.col("pos").cast("double")).alias("mean_pos"),
        F.count(F.lit(1)).alias("n_pos"),
    ]
    for k in k_values:
        aggs.extend(
            [
                F.mean(f"top_k@{k}").alias(f"hit_rate@{k}"),
                F.mean(f"ap_contrib@{k}").alias(f"map_attr@{k}"),
                F.mean(f"ndcg_contrib@{k}").alias(f"ndcg_attr@{k}"),
            ]
        )
    rows = rel.groupBy(*dim_cols).agg(*aggs).collect()

    metric_cols = _per_item_metric_cols(k_values)
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        if len(dim_cols) == 1:
            raw_key = r[dim_cols[0]]
            key = raw_key if isinstance(raw_key, str) else str(raw_key)
        else:
            key = "_".join(str(r[c]) for c in dim_cols)
        out[key] = {c: float(r[c]) for c in metric_cols}
        out[key]["n_pos"] = int(r["n_pos"])
    return out


# ---------------------------------------------------------------------------
# Macro average (plain Python over collected dicts)
# ---------------------------------------------------------------------------


_N_POS_KEY = "n_pos"


def macro_average(
    per_dim: dict[str, dict[str, float]],
    *,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> dict[str, float]:
    """Parameterized macro over dim keys (defaults = equal weight, 現行為).

    ``n_pos`` is the reserved weight-source key — never averaged into the
    output. Non-default params require every inner dict to carry ``n_pos``;
    otherwise raises ValueError (fail loud, no silent equal-weight fallback).
    Per-metric combine goes through ``metrics.macro_from_per_item``; a metric
    whose items are all excluded by ``min_positives`` is omitted.

    Default params take the ORIGINAL sum/len code path — bit-identical to the
    pre-parameterization behavior (the real-run regression gate compares
    report values verbatim; ``np.dot`` with uniform weights can differ from
    ``sum/len`` in the last ulp).
    """
    if not per_dim:
        return {}
    params_active = (
        weight_alpha != 0.0 or min_positives > 0 or shrinkage_k > 0
    )
    if not params_active:
        accum: dict[str, list[float]] = {}
        for metrics in per_dim.values():
            for k, v in metrics.items():
                if k == _N_POS_KEY:
                    continue
                accum.setdefault(k, []).append(float(v))
        return {k: sum(v) / len(v) for k, v in accum.items()}

    missing = [k for k, m in per_dim.items() if _N_POS_KEY not in m]
    if missing:
        raise ValueError(
            f"macro_average: weight_alpha/min_positives/shrinkage_k need "
            f"'n_pos' in every per-item dict; missing for {missing}. "
            f"Upstream must be aggregate_per_item (which emits n_pos)."
        )

    pairs_by_metric: dict[str, list[tuple[float, float]]] = {}
    for metrics in per_dim.values():
        n = float(metrics[_N_POS_KEY])
        for k, v in metrics.items():
            if k == _N_POS_KEY:
                continue
            pairs_by_metric.setdefault(k, []).append((float(v), n))
    out: dict[str, float] = {}
    for k, pairs in pairs_by_metric.items():
        values = np.array([p[0] for p in pairs])
        n_pos = np.array([p[1] for p in pairs])
        combined = macro_from_per_item(
            values, n_pos, weight_alpha, min_positives, shrinkage_k
        )
        if combined is not None:
            out[k] = combined
    return out


# ---------------------------------------------------------------------------
# Layer 4 — orchestrator
# ---------------------------------------------------------------------------


_EMPTY_RESULT = {
    "overall": {},
    "per_segment": {},
    "per_item": {},
    "per_item_segment": {},
    "macro_avg": {},
    "observation_items": [],
}


def _compute_core(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """The fine-grained metric bundle (overall/per_item/per_segment/...).

    Body identical to the pre-refactor compute_all_metrics — no category,
    no dataset_overview. Used for both fine-grained and (on a collapsed DF)
    category-grain passes.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {}) or {}
    k_values_raw = eval_params.get("k_values", [5, "all"])
    segment_columns = eval_params.get("segment_columns", []) or []
    metric_cfg = eval_params.get("metric", {}) or {}
    metric_params = {
        "weight_alpha": float(metric_cfg.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(metric_cfg.get("min_positives", 0) or 0),
        "shrinkage_k": float(metric_cfg.get("shrinkage_k", 0) or 0.0),
    }

    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = _resolve_k_values(k_values_raw, n_products)
    n_queries_total = eval_predictions.select(*group_cols).distinct().count()

    # ---- Layer 1: row-level enrichment ----
    df = rank_within_query(eval_predictions, group_cols, score_col)
    df = add_query_total_rel(df, group_cols, label_col)

    df_with_pos = df.filter(F.col("total_rel") > 0)
    n_queries_with_pos = df_with_pos.select(*group_cols).distinct().count()
    n_excluded_queries = n_queries_total - n_queries_with_pos

    if n_queries_with_pos == 0:
        logger.warning("No queries with positive labels found")
        return {
            **_EMPTY_RESULT,
            "n_queries": n_queries_total,
            "n_excluded_queries": n_excluded_queries,
        }

    enriched = add_row_contributions(df_with_pos, group_cols, label_col, k_values)
    enriched = enriched.cache()
    try:
        # ---- Detect active segment column ----
        active_seg_col: str | None = None
        for seg in segment_columns:
            if seg in enriched.columns:
                active_seg_col = seg
                break

        # ---- Layer 2: per-query metrics (carries seg for per_segment) ----
        carry = [active_seg_col] if active_seg_col else []
        per_query = compute_per_query_metrics(
            enriched, group_cols, label_col, k_values, carry_cols=carry
        ).cache()
        try:
            # ---- Layer 3: aggregations ----
            overall = aggregate_overall(per_query, k_values)
            per_item = aggregate_per_item(
                enriched, [item_col], label_col, k_values
            )

            per_segment: dict = {}
            per_item_segment: dict = {}
            if active_seg_col:
                per_segment = aggregate_per_segment(
                    per_query, active_seg_col, k_values
                )
                per_item_segment = aggregate_per_item(
                    enriched, [item_col, active_seg_col], label_col, k_values
                )

            macro_avg: dict = {"by_item": macro_average(per_item, **metric_params)}
            if per_segment:
                macro_avg["by_segment"] = macro_average(per_segment)
            if per_item_segment:
                # (item, segment) cell 亦受 min_positives 過濾（cell 的
                # n_pos < 門檻即移出 by_item_segment macro）；觀察名單只在
                # item 粒度回報，cell 層級不另列。
                macro_avg["by_item_segment"] = macro_average(
                    per_item_segment, **metric_params
                )

            observation_items = sorted(
                it for it, m in per_item.items()
                if m.get("n_pos", 0) < metric_params["min_positives"]
            ) if metric_params["min_positives"] > 0 else []

            return {
                "overall": overall,
                "per_segment": per_segment,
                "per_item": per_item,
                "per_item_segment": per_item_segment,
                "macro_avg": macro_avg,
                "observation_items": observation_items,
                "n_queries": n_queries_total,
                "n_excluded_queries": n_excluded_queries,
            }
        finally:
            per_query.unpersist()
    finally:
        enriched.unpersist()


def compute_overall_per_item(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Slim metric bundle: ``overall`` + ``per_item`` only.

    Composes the same Layer-1/2/3 building blocks as ``_compute_core`` but
    skips per-segment, per-item-segment, macro_avg, category collapse, and
    dataset_overview. Used by the popularity baseline, whose report section
    consumes only these two keys.

    Returns ``{"overall": {...}, "per_item": {...}}``; both empty when no
    query has a positive label.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = [time_col] + entity_cols

    eval_params = parameters.get("evaluation", {}) or {}
    k_values_raw = eval_params.get("k_values", [5, "all"])
    n_products = eval_predictions.select(item_col).distinct().count()
    k_values = _resolve_k_values(k_values_raw, n_products)

    df = rank_within_query(eval_predictions, group_cols, score_col)
    df = add_query_total_rel(df, group_cols, label_col)
    df_with_pos = df.filter(F.col("total_rel") > 0)
    if df_with_pos.limit(1).count() == 0:
        logger.warning("No queries with positive labels found")
        return {"overall": {}, "per_item": {}}

    enriched = add_row_contributions(
        df_with_pos, group_cols, label_col, k_values
    ).cache()
    try:
        per_query = compute_per_query_metrics(
            enriched, group_cols, label_col, k_values, carry_cols=[]
        )
        overall = aggregate_overall(per_query, k_values)
        per_item = aggregate_per_item(enriched, [item_col], label_col, k_values)
        return {"overall": overall, "per_item": per_item}
    finally:
        enriched.unpersist()


def compute_all_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Full bundle: fine-grained core + dataset_overview + optional category.

    Required columns: ``time``, ``entity``, ``item``, ``label``, ``score``
    (column names resolved from parameters['schema']).
    Optional: any column listed in ``parameters['evaluation']['segment_columns']``
    will be used for per-segment slicing if present.

    Backward compatible: every pre-existing top-level key is unchanged;
    ``dataset_overview`` is always added; ``category`` (same shape as the
    top level, plus its own ``dataset_overview``, never re-nested) is added
    only when ``product_categories.enabled``.

    Returns::

        {
          "overall":          {map@K, ndcg@K, precision@K, recall@K, ...},
          "per_segment":      {seg_value: {map@K, ndcg@K, precision@K, recall@K, ...}},
          "per_item":         {item: {hit_rate@K, map_attr@K, ndcg_attr@K, mean_pos, ...}},
          "per_item_segment": {item_seg: {hit_rate@K, map_attr@K, ndcg_attr@K, mean_pos, ...}},
          "macro_avg": {
              "by_segment":      {map@K, ndcg@K, precision@K, recall@K, ...},
              "by_item":         {hit_rate@K, map_attr@K, ndcg_attr@K, mean_pos, ...},
              "by_item_segment": {hit_rate@K, map_attr@K, ndcg_attr@K, mean_pos, ...},
          },
          "observation_items": [item, ...]（n_pos < evaluation.metric.min_positives 的 item；additive，預設空）
          "n_queries":          int  (total distinct queries before filtering),
          "n_excluded_queries": int  (queries with zero positives → dropped),
          "dataset_overview": {
              "totals":       {n_rows, n_customers, n_products, n_snap_dates,
                               n_positives, positive_rate,
                               avg_positives_per_customer},
              "by_snap_date": {snap: {n_rows, n_positives, n_customers,
                                      positive_rate}},
              "by_item":      {item: {n_rows, n_positives, n_customers,
                                      positive_rate}},
          },
          "category":  (only when product_categories.enabled)
              same shape as the top level (overall / per_item / per_segment /
              per_item_segment / macro_avg / n_queries / n_excluded_queries)
              PLUS its own "dataset_overview"; never contains "category".
        }

    Queries with zero positives are excluded from the metric computation
    (AP and nDCG are undefined when total_rel = 0).
    """
    result = _compute_core(eval_predictions, parameters)
    result["dataset_overview"] = compute_dataset_overview(
        eval_predictions, parameters
    )

    if _build_category_mapping(parameters) is not None:
        collapsed = collapse_to_categories(eval_predictions, parameters)
        cat = _compute_core(collapsed, parameters)
        cat["dataset_overview"] = compute_dataset_overview(
            collapsed, parameters
        )
        result["category"] = cat

    return result

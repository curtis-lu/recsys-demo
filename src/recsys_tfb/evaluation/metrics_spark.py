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

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

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

    NOTE: no precision@K / recall@K here on purpose. Their query-level
    definitions don't carry over cleanly to the per-item dimension
    (the K denominator is a per-query concept). Use ``hit_rate@K`` for
    the item-level recall analogue.
    """
    rel = enriched.filter(F.col(label_col) == 1)

    aggs = [F.mean(F.col("pos").cast("double")).alias("mean_pos")]
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
    return out


# ---------------------------------------------------------------------------
# Macro average (plain Python over collected dicts)
# ---------------------------------------------------------------------------


def macro_average(per_dim: dict[str, dict[str, float]]) -> dict[str, float]:
    """Equal-dim-key weight mean of inner metric dicts.

    Empty input → empty dict. Missing keys in some inner dicts are
    handled per-metric (each metric averages only over the dim keys that
    have it).
    """
    if not per_dim:
        return {}
    accum: dict[str, list[float]] = {}
    for metrics in per_dim.values():
        for k, v in metrics.items():
            accum.setdefault(k, []).append(float(v))
    return {k: sum(v) / len(v) for k, v in accum.items()}


# ---------------------------------------------------------------------------
# Layer 4 — orchestrator
# ---------------------------------------------------------------------------


_EMPTY_RESULT = {
    "overall": {},
    "per_segment": {},
    "per_item": {},
    "per_item_segment": {},
    "macro_avg": {},
}


def compute_all_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """Compute the full metric bundle on ``eval_predictions``.

    Required columns: ``time``, ``entity``, ``item``, ``label``, ``score``
    (column names resolved from parameters['schema']).
    Optional: any column listed in ``parameters['evaluation']['segment_columns']``
    will be used for per-segment slicing if present.

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
          "n_queries":          int  (total distinct queries before filtering),
          "n_excluded_queries": int  (queries with zero positives → dropped),
        }

    Queries with zero positives are excluded from the metric computation
    (AP and nDCG are undefined when total_rel = 0).
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

            macro_avg: dict = {"by_item": macro_average(per_item)}
            if per_segment:
                macro_avg["by_segment"] = macro_average(per_segment)
            if per_item_segment:
                macro_avg["by_item_segment"] = macro_average(per_item_segment)

            return {
                "overall": overall,
                "per_segment": per_segment,
                "per_item": per_item,
                "per_item_segment": per_item_segment,
                "macro_avg": macro_avg,
                "n_queries": n_queries_total,
                "n_excluded_queries": n_excluded_queries,
            }
        finally:
            per_query.unpersist()
    finally:
        enriched.unpersist()

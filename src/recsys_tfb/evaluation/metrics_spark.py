"""Spark-native ranking metrics.

Layered structure (each layer has a single responsibility):

    Layer 1: row-level enrichment              (Spark DF → Spark DF, +columns)
        rank_within_query              (Window: pos)
        add_query_total_rel            (Window: total_rel)
        add_row_contributions          (cum_rel, prec_at_pos,
                                        top_k@K, ap_contrib@K)

    Layer 2: per-query metrics                 (Spark DF → Spark DF)
        compute_per_query_metrics      one row per query;
                                       columns: group_cols, total_rel, and
                                                every query-group entry of
                                                metric_catalog at each K

    Layer 3: aggregations                      (Spark DF → Python dict)
        aggregate_overall              query-equal-weight mean of per-query
        aggregate_per_segment          query-equal-weight mean within segment
        aggregate_item_shared_numbers  per item: sums over all its rows
                                       (n_top@K, hits@K, ap_sum@K, …)
        item_metrics                   the catalog's item formulas on those
                                       sums: recall@K / map_attr@K / mean_pos

    Layer 4: orchestrators
        compute_row_facts              the rows' overview + query group counts
        compute_all_metrics            wires everything, counting off the
                                       row facts; reads parameters

    Training's test scoring (ADR-0028), apart from the four layers
        count_query_groups_by_time     query group counts + time values present
        compute_untruncated_ap         mAP and per-item attribution, no K
        compute_pooled_average_precision / compute_macro_per_item_average_precision
                                       the binary-prediction metrics, exact,
                                       as HPO scores them on val

NDCG is not computed (ADR-0018 decision 4). LightGBM's own ``ndcg``
early-stopping metric (``training.algorithm_params.metric``) is a different
thing and is not affected.

Naming convention (intentional, NOT pandas-mirroring):
    * ``@K`` keys (map@K, precision@K, recall@K) in ``overall`` /
      ``per_segment`` are *per-query metrics aggregated across queries*. K
      names the per-query truncation point.
    * ``_attr@K`` keys (map_attr@K) live in ``per_item`` —
      they are *per-row attribution contributions averaged across all rows
      where the item appears as a positive*. The "attribution" suffix marks
      them as fragments of per-query metrics, not per-query metrics
      themselves.
    * ``recall@K`` in ``per_item`` is the marginal recall of the item:
      ``P(rank(item) ≤ K | item is positive)``. One name in two methods
      (ADR-0031 decision 6): the same conditional probability as ``recall@K``
      in ``overall``, aggregated row-equal-weight rather than
      query-equal-weight; the bundle block, not the key, tells them apart.
    * ``mean_pos`` (per_item) is the mean rank position over P-positive rows.

Degenerate cases (documented, not pruned):
    At ``K >= n_items`` every row has ``top_k@K == 1`` →
        ``precision@K`` collapses to the per-query base rate
                       ``total_rel / n_items``.  Mean across queries gives
                       segment / overall base rate. Useful as a sanity check
                       for label density but NOT a ranking metric.
        ``recall@K``   collapses to ``1.0`` for every query (all positives
                       are necessarily ranked in top-all). Mean across
                       queries == 1.0.
        ``hit_rate@K`` collapses to ``1.0`` likewise, and ``f1@K`` to
                       ``2 · total_rel / (n_items + total_rel)``, a function
                       of label density alone.
    All are kept in the output for downstream tooling that iterates
    ``k_values``.

All row-level work stays in Spark; only the final small per-dim dicts are
collected to the driver.
"""

from __future__ import annotations

import functools
import logging
import operator
from typing import Iterable, Mapping, NamedTuple, Sequence

import numpy as np
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

from recsys_tfb.core.consistency import (
    EVALUATION_K_VALUES_DEFAULT,
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    item_category_column,
    item_list_counted_from_data,
)
from recsys_tfb.core.schema import declares_optional_role, get_schema
from recsys_tfb.evaluation.metric_catalog import (
    ITEM,
    ITEM_SHARED_NUMBERS,
    ITEM_SHARED_NUMBERS_PER_K,
    QUERY_GROUP,
    Expr,
    Ratio,
    metrics_of,
    shared,
)
from recsys_tfb.evaluation.metrics import (
    all_positive_accounts,
    macro_from_per_item,
    n_groups_with_positive,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    ALL_POSITIVE_KEY,
    MACRO_ITEMS_KEY,
    N_SCORED_WITH_POSITIVE_KEY,
    N_SCORED_WITHOUT_POSITIVE_KEY,
    POPULATION_KEY,
    SEGMENT_COLUMNS_KEY,
    WIDEST_GROUP_KEY,
    metric_params,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT, segment_key
from recsys_tfb.utils.ranking import rank_by_score_then_item, score_then_item_window

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# k_values resolution
# ---------------------------------------------------------------------------


def _resolve_k_values(raw: Iterable, all_k: int) -> list[int]:
    """Resolve mixed int / 'all' k_values to a sorted unique int list.

    'all' (case-insensitive) resolves to ``all_k`` (``_resolve_all_k``).
    """
    out: set[int] = set()
    for k in raw:
        if isinstance(k, str) and k.lower() == "all":
            out.add(all_k)
        else:
            out.add(int(k))
    return sorted(out)


def _resolve_all_k(
    frame: SparkDataFrame,
    group_cols: Sequence[str],
    item_col: str,
    event_cols: Sequence[str],
    *,
    n_items: int | None = None,
    widest_group_rows: int | None = None,
) -> int:
    """What ``k_values: "all"`` means for this frame: the K that truncates
    nothing. It depends on the rows of **this frame**, not on the declared
    roles (``event_cols`` is what the caller ranks this frame with, see
    ``_compute_core``):

    * **The frame holds one row per event** (``event_cols`` non-empty): the
      largest number of rows any one query group holds. The item count would
      truncate: one entity can be shown the same twelve creatives thirty times
      in a week, which is thirty rows in one query group against twelve
      distinct items, and ``map@12`` on a 30-row ranking silently answers a
      different question from the one ``"all"`` names.
    * **Otherwise**: the distinct item count. Items are unique within a query
      group (the duplicate checks guarantee it with no role or with
      ``occasion`` only), so no group holds more rows than there are items.
      This is also every frame the category pass ranks:
      ``collapse_to_categories`` leaves one row per (query group, category),
      so the answer there is the category count.

    Not the widest group everywhere, though it truncates nothing either: the
    item count is what every existing deployment's ``"all"`` already resolves
    to, so its artifacts stay value-for-value.

    Readers do not recompute this; they read what the producer used through
    ``resolved_all_k``. ``_compute_core`` writes it into the bundle in the
    first case; in the second it is ``dataset_overview.totals.n_items`` of the
    same frame.

    Costs one action over the frame unless the caller passes the number it
    needs: ``n_items`` (``dataset_overview.totals.n_items`` of the same frame:
    ``countDistinct(struct(item))`` counts a NULL item once, as
    ``distinct().count()`` does) or ``widest_group_rows`` (the last number
    ``_count_query_groups`` returns). Passed, they must be counted on this
    same frame: the category pass's item count is the category count, and
    handing either pass the other's would key every ``@all`` metric at the
    wrong K with no error.
    """
    if not event_cols:
        if n_items is None:
            n_items = frame.select(item_col).distinct().count()
        return n_items
    if widest_group_rows is None:
        widest_group_rows = (
            frame.groupBy(*group_cols)
            .agg(F.count(F.lit(1)).alias("_n_rows"))
            .agg(F.max("_n_rows").alias("_max_rows"))
            .collect()[0]["_max_rows"]
        )
    # An empty frame has no widest group. 0 is what the item-count branch
    # returns for the same frame, so the two branches degrade alike.
    return int(widest_group_rows or 0)


def _resolve_k_grids(
    parameters: dict, all_k: int
) -> tuple[list[int], list[int]]:
    """``(query_ks, item_ks)``: the per-query and the per-item K grids, one
    list: ``evaluation.k_values`` resolved ∪ {``all_k``}.

    ``all_k`` is added whatever ``k_values`` lists: the two model-selection
    metrics are read at "all" — overall ``map@{all_k}`` (per-query axis) and
    the macro ``map_attr@{all_k}`` (per-item axis), the numbers HPO selects by
    (``metric_registry``'s ``mean_ap`` and ``macro_per_item_map``, both
    untruncated). A ``k_values`` that leaves ``"all"`` out would otherwise
    leave either cell blank, and nothing would raise.

    Still returned as a pair because the per-query and the per-item layers
    take their grids as separate arguments.

    The model and its baseline both resolve through this one function, in
    ``_compute_core``, so the two sides line up. An unwritten ``k_values`` is
    ``core.consistency.EVALUATION_K_VALUES_DEFAULT``, the list A62 checks
    ``evaluation.report.display.compare_k`` against.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    ks = sorted(set(_resolve_k_values(
        eval_params.get("k_values", list(EVALUATION_K_VALUES_DEFAULT)), all_k,
    )) | {all_k})
    return ks, list(ks)


def resolve_item_k_grid(frame: SparkDataFrame, parameters: dict) -> list[int]:
    """The per-item K grid of ``frame`` as the main run resolves it
    (:func:`_resolve_k_grids`, "all" by :func:`_resolve_all_k` with
    ``schema.columns.event``), for a caller with no row facts at hand: one
    action over ``frame``. Pass the rows the ranks were taken over, not a
    subset of them, or "all" resolves smaller."""
    schema = get_schema(parameters)
    all_k = _resolve_all_k(frame, schema["query_group_columns"],
                           schema["item"], schema.get("event", []))
    return _resolve_k_grids(parameters, all_k)[1]


def hand_category_mapping(
    parameters: dict, known_items: Sequence | None = None
) -> dict[str, str] | None:
    """Resolve {item value: category} from the hand-written
    ``item_categories.mapping``. None when categories disabled.

    Fail-loud (ValueError) if a mapped product is not a known item: one of
    ``schema.categorical_values[item_col]``, or — when the item list is
    counted from the data — of ``known_items``, the evaluated model's list,
    which ``prepare_eval_data`` read off its preprocessor. Counted without
    ``known_items`` is refused too: the conf holds no list then. Products
    absent from every mapping list become their own singleton category when
    ``unmapped == 'singleton'`` (the only supported mode); an item no known
    list holds (a new item) becomes one in the collapse
    (``collapse_to_categories``).

    Also fail-loud in column mode (``item_categories.column``): there the
    table is read off the data by ``prepare_eval_data`` and has to be passed in
    (``category_mapping``, see :func:`_resolve_category_mapping`). Falling
    through would give every item its own category — a report that looks fine
    and ranks the wrong thing.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cat_cfg = eval_params.get("item_categories", {}) or {}
    if not cat_cfg.get("enabled"):
        return None
    if item_category_column(parameters) is not None:
        raise ValueError(
            f"item_categories.column={cat_cfg['column']!r}: each item's "
            f"category is read off the data by prepare_eval_data "
            f"(evaluation_item_categories) and has to be passed as "
            f"category_mapping; there is no mapping in the conf to fall "
            f"back to."
        )

    schema = get_schema(parameters)
    item_col = schema["item"]
    if item_list_counted_from_data(parameters):
        if known_items is None:
            raise ValueError(
                f"schema.categorical_values[{item_col!r}] is counted from the "
                f"train months, so the known items are the evaluated model's "
                f"(its preprocessor's list, landed by prepare_eval_data as "
                f"evaluation_item_categories.known_items); none was passed."
            )
        known = list(known_items)
        known_from = "the evaluated model's item list (counted from its train months)"
    else:
        known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
        known_from = f"schema.categorical_values['{item_col}']"
    known_set = set(known)

    mapping: dict[str, str] = {}
    for category, items in (cat_cfg.get("mapping", {}) or {}).items():
        for item in items:
            if item not in known_set:
                raise ValueError(
                    f"item_categories.mapping references unknown item "
                    f"'{item}' (not in {known_from})"
                )
            mapping[item] = category

    unmapped = cat_cfg.get("unmapped", "singleton")
    if unmapped != "singleton":
        raise ValueError(
            f"item_categories.unmapped='{unmapped}' unsupported; "
            f"only 'singleton' is implemented"
        )
    for item in known:
        mapping.setdefault(item, item)
    return mapping


def _resolve_category_mapping(
    parameters: dict, category_mapping: Mapping[str, str] | None
) -> dict[str, str] | None:
    """The {item: category} table a category pass collapses by, or None when
    there is none to compute.

    ``category_mapping`` is what a column-mode run's ``prepare_eval_data``
    read off the data (``evaluation_item_categories``); it wins. ``None``
    means "read the hand-written mapping from ``parameters``", which refuses
    column mode (:func:`hand_category_mapping`).
    """
    if category_mapping is not None:
        return dict(category_mapping)
    return hand_category_mapping(parameters)


def _require_segment_columns_in_frame(
    df: SparkDataFrame, segment_columns: Sequence[str]
) -> None:
    """Pre-check (input): every passed segment column is in the frame.

    ``segment_columns`` is what ``prepare_eval_data`` actually joined this run
    (the landed ``evaluation_segment_columns``), not the configured list, and
    it alone decides what is segmented by. The frame's columns prove nothing:
    ``enriched_eval_predictions`` is shared by both run modes, so a column the
    other mode joined sits in this run's rows all NULL, and picking it grows a
    fake unmatched group (ADR-0020 bug 6). A passed column the frame lacks
    means the list and the frame come from different runs: a wiring error
    that skipping the column would hide.
    """
    missing = [c for c in segment_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"segment columns {missing} are not in the frame (it has "
            f"{df.columns}). They were passed as joined by prepare_eval_data "
            f"(evaluation_segment_columns), so the frame and that list come "
            f"from different runs."
        )


def collapse_to_categories(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    *,
    segment_columns: Sequence[str] = (),
    category_mapping: Mapping[str, str] | None = None,
) -> SparkDataFrame:
    """Collapse fine-grained predictions to category grain (no UDF).

    For each (time, entity..., category): score = max(child score),
    label = max(child label), ``segment_columns`` via F.first. The category
    column is emitted under the schema item_col name so the collapsed DF
    is shape-compatible with compute_all_metrics. ``max(score)`` re-ranking
    is equivalent to taking the best child rank (pos is score-desc derived).

    ``category_mapping``: see :func:`_resolve_category_mapping`.
    """
    mapping = _resolve_category_mapping(parameters, category_mapping)
    if mapping is None:
        raise ValueError("collapse_to_categories called with categories disabled")

    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = schema["query_group_columns"]

    _require_segment_columns_in_frame(eval_predictions, segment_columns)

    # Decision — an item the table does not cover is its own category (#379
    # decision 7), as unmapped: singleton makes it: a left join and a
    # coalesce, not an inner join. Dropped, a new item's rows would leave the
    # category pass alone, so the two passes would count different groups.
    # Where the table covers every item, the two joins keep the same rows.
    if mapping:
        spark = eval_predictions.sparkSession
        map_rows = [(p, c) for p, c in mapping.items()]
        map_df = spark.createDataFrame(map_rows, [item_col, "_category"])
        joined = eval_predictions.join(
            F.broadcast(map_df), on=item_col, how="left"
        ).withColumn("_category", F.coalesce(F.col("_category"), F.col(item_col)))
    else:
        # An empty table maps nothing, so every item is its own category —
        # the join's answer, taken directly: createDataFrame on zero rows
        # raises "can not infer schema from empty dataset".
        joined = eval_predictions.withColumn("_category", F.col(item_col))

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
    *,
    segment_columns: Sequence[str] = (),
) -> dict:
    """Dataset profiling for the report §1.

    Cost: one ``agg`` + ``collect`` for every total, then one ``groupBy`` +
    ``collect`` per grouping column (time, item, and the segment column when
    there is one) — a fixed number of actions, whatever the data size
    (ADR-0018 decision 3). What reaches the driver is bounded by the number
    of distinct time / item / segment values, not by rows.

    With ``occasion`` declared ``totals`` also holds ``n_time_entity_combos``
    (distinct time x entity combinations), with ``event`` declared
    ``n_query_group_item_combos`` (distinct (query group, item) combinations);
    undeclared, the key is absent.

    ``item_col_override`` profiles the collapsed category-grain DF (item
    column still named after schema item_col, but semantics = category).
    ``by_segment`` groups by the first of ``segment_columns``.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = item_col_override or schema["item"]
    label_col = schema["label"]
    group_cols = schema["query_group_columns"]

    # Same segment column (the first) as per_segment, so the by_segment keys match.
    active_seg_col = segment_columns[0] if segment_columns else None

    # Distinct totals count a struct, never the bare columns: bare
    # countDistinct drops every row with a NULL in any of its columns, while a
    # struct is never NULL itself, so NULL counts as one value.
    total_aggs = [
        F.count(F.lit(1)).alias("n_rows"),
        F.countDistinct(F.struct(*entity_cols)).alias("n_entities"),
        F.countDistinct(F.struct(item_col)).alias("n_items"),
        F.countDistinct(F.struct(time_col)).alias("n_snap_dates"),
        F.sum(F.col(label_col)).alias("n_positives"),
    ]
    if active_seg_col:
        total_aggs.append(
            F.countDistinct(F.struct(*group_cols)).alias("n_queries")
        )
    # Only a declared optional role lands its count (ADR-0031 decision 16
    # item 14); an undeclared one writes no key. Both are structs, as above.
    declared_occasion = bool(schema.get("occasion"))
    declared_event = bool(schema.get("event"))
    if declared_occasion:
        # An entity at a time: the base key, which occasion does not widen
        # (core.schema; architecture-constraints S7).
        total_aggs.append(
            F.countDistinct(F.struct(*schema["base_key_columns"])).alias(
                "n_time_entity_combos"))
    if declared_event:
        total_aggs.append(
            F.countDistinct(F.struct(*group_cols, item_col)).alias(
                "n_query_group_item_combos"))
    totals = eval_predictions.agg(*total_aggs).collect()[0]

    n_rows = totals["n_rows"]
    n_entities = totals["n_entities"]
    n_items = totals["n_items"]
    n_snap_dates = totals["n_snap_dates"]
    n_positives = int(totals["n_positives"] or 0)
    positive_rate = (n_positives / n_rows) if n_rows else 0.0
    avg_pos_per_entity = (n_positives / n_entities) if n_entities else 0.0

    _require_segment_columns_in_frame(eval_predictions, segment_columns)
    total_queries = totals["n_queries"] if active_seg_col else 0

    def _group(col: str, with_queries: bool = False, to_key=None) -> dict:
        # Inside a group, n_entities / n_queries count a struct too, as the
        # totals do: a bare countDistinct there would drop keys with a NULL
        # that the totals kept, so query_share would sum below 1.
        aggs = [
            F.count(F.lit(1)).alias("n_rows"),
            F.sum(F.col(label_col)).alias("n_positives"),
            F.countDistinct(F.struct(*entity_cols)).alias("n_entities"),
        ]
        if with_queries:
            aggs.append(
                F.countDistinct(F.struct(*group_cols)).alias("n_queries"))
        rows = eval_predictions.groupBy(col).agg(*aggs).collect()
        out = {}
        for r in rows:
            if to_key is not None:
                key = to_key(r[col])
            else:
                key = r[col] if isinstance(r[col], str) else str(r[col])
            nr = int(r["n_rows"])
            npos = int(r["n_positives"] or 0)
            cell = {
                "n_rows": nr,
                "n_positives": npos,
                "n_entities": int(r["n_entities"]),
                "positive_rate": (npos / nr) if nr else 0.0,
            }
            if with_queries:
                nq = int(r["n_queries"])
                cell["n_queries"] = nq
                cell["query_share"] = (nq / total_queries) if total_queries else 0.0
            out[key] = cell
        # Sorted, so the landed order does not follow the execution plan.
        return dict(sorted(out.items()))

    result = {
        "totals": {
            "n_rows": n_rows,
            "n_entities": n_entities,
            "n_items": n_items,
            "n_snap_dates": n_snap_dates,
            "n_positives": n_positives,
            "positive_rate": positive_rate,
            "avg_positives_per_entity": avg_pos_per_entity,
        },
        "by_snap_date": _group(time_col),
        "by_item": _group(item_col),
    }
    if declared_occasion:
        result["totals"]["n_time_entity_combos"] = int(
            totals["n_time_entity_combos"])
    if declared_event:
        result["totals"]["n_query_group_item_combos"] = int(
            totals["n_query_group_item_combos"])
    if active_seg_col:
        result["by_segment"] = _group(
            active_seg_col, with_queries=True, to_key=segment_key
        )
    return result


def _tied_rows(
    frame: SparkDataFrame, parameters: dict, n_rows: int,
) -> dict:
    """``{"n_tied_rows", "tied_row_share"}`` of ``frame``'s scores, or ``{}``.

    The share of rows whose rank the tie-break decided rather than the score.
    Counted ONLY when an optional role is declared, so every other artifact
    stays value-for-value what it was; the extra shuffle is likewise only
    paid by a deployment that asked for the role.

    With only `occasion` declared, items are unique within a query group
    and the tie-break is the item alone (spec #426 decision D), but the query
    groups are small, so a tie decides a larger share of each ranking.

    Why it is reported at all: with `event` declared and no per-impression
    features attached, every row of one item in one query group necessarily
    scores the same, so *everything* ties and the rank is decided entirely by
    `event` ascending. That direction is not neutral — an event timestamp
    ranks the earlier impression first. The framework does not choose for the
    deployment (ADR-0025 rejected hashing the identity instead); it prints the
    number that says how much the choice could be worth here.

    A metrics key, not a row fact: it counts ties of these scores, so the
    popularity baseline's — one score per item — differ from the model's on
    the same rows. ``n_rows`` is the row facts' count of ``frame``.
    """
    schema = get_schema(parameters)
    if not declares_optional_role(schema):
        return {}
    n_tied = (
        frame.groupBy(*schema["query_group_columns"], schema["score"])
        .agg(F.count(F.lit(1)).alias("_n"))
        .filter(F.col("_n") > 1)
        .agg(F.sum("_n").alias("_tied"))
        .collect()[0]["_tied"]
    )
    n_tied = int(n_tied or 0)
    return {"n_tied_rows": n_tied,
            "tied_row_share": (n_tied / n_rows) if n_rows else 0.0}


# ---------------------------------------------------------------------------
# Layer 1 — row-level enrichment
# ---------------------------------------------------------------------------


def rank_within_query(
    df: SparkDataFrame,
    group_cols: list[str],
    score_col: str,
    item_col: str,
    event_cols: list[str] | None = None,
) -> SparkDataFrame:
    """Assign ``pos``: 1-based rank within each ``group_cols`` group, by ``score`` desc.

    Ties go by ``item_col`` ascending, then by each of ``event_cols`` — the
    rule inference publishes ``rank`` with
    (``utils.ranking.rank_by_score_then_item``), so re-ranking here gives the
    same rows the same positions.

    ``event_cols`` is needed here and not in inference because only this side
    can see more than one row per item in a query group: inference builds its
    candidates as entity x item and ignores the role entirely (ADR-0025
    decision 1).
    """
    return df.withColumn(
        "pos",
        rank_by_score_then_item(
            group_cols, score_col, item_col, event_cols or (),
        ),
    )


def add_query_total_rel(
    df: SparkDataFrame, group_cols: list[str], label_col: str
) -> SparkDataFrame:
    """Add ``total_rel`` = sum(label) per query; constant within each query."""
    w = Window.partitionBy(*group_cols)
    return df.withColumn("total_rel", F.sum(F.col(label_col)).over(w))


def _all_positive_group(label_col: str, window: WindowSpec | None = None):
    """True for a query group whose every row carries the same positive label:
    the *all-positive* groups. An aggregate over the group's rows — per group
    inside ``groupBy().agg``, or on every row over ``window``.

    The one definition both the count (``_count_query_groups``) and the filter
    (``_keep_scored_query_groups``) use: ``min(label) == max(label) > 0``, a
    NULL label read as 0 for the minimum.

    * **The same positive label, not just a positive one.** The label comes
      from the deployment's own ``label_table`` and nothing holds it to 0/1.
      Rows labelled 2 and 3 are all positive, yet which one ranks first moves
      ``map@K`` and ``precision@K``; only equal labels leave every order with
      the same label sequence, hence every per-query metric the same.
    * **Row by row, not item by item.** With ``event`` declared a query group
      can hold one item several times; one item shown twice with one response
      is a group whose AP depends on the order, so it is not all-positive.
    * **Not ``total_rel == row count``**: a graded 2 next to a 0 would pass it.
    * **A NULL label is not positive** — ``total_rel`` (a sum) skips it, so
      where the positive rows rank still moves AP. ``min`` alone would skip it
      too and call the group all-positive; hence the ``coalesce``.
    """
    lowest = F.min(F.coalesce(F.col(label_col), F.lit(0)))
    highest = F.max(F.col(label_col))
    if window is not None:
        lowest, highest = lowest.over(window), highest.over(window)
    return (lowest > 0) & (lowest == highest)


def _count_query_groups(
    frame: SparkDataFrame, group_cols: Sequence[str], label_col: str
) -> tuple[int, int, int, int]:
    """``(all, holding a positive, all-positive)`` query groups and the row
    count of the widest one, in one action.

    One ``groupBy``. The first two numbers match what distinct counts would
    give: ``groupBy`` groups NULL keys together as ``distinct`` does, and
    "holding a positive" is ``sum(label) > 0``, the ``total_rel > 0`` the
    filter keeps.

    An all-positive group holds a positive by construction (every group has a
    row); the ``AND`` only keeps the subset relation that
    ``metrics.all_positive_share`` divides by explicit.

    The widest group is what ``"all"`` resolves to on a frame of event rows
    (``_resolve_all_k``); 0 on an empty frame, as there.
    """
    has_positive = F.col("_total_rel") > 0
    row = (
        frame.groupBy(*group_cols)
        .agg(
            F.sum(F.col(label_col)).alias("_total_rel"),
            _all_positive_group(label_col).alias("_all_positive"),
            F.count(F.lit(1)).alias("_n_rows"),
        )
        .agg(
            F.count(F.lit(1)).alias("n_queries"),
            F.sum(F.when(has_positive, 1).otherwise(0)).alias("n_with_positive"),
            F.sum(
                F.when(has_positive & F.col("_all_positive"), 1).otherwise(0)
            ).alias("n_all_positive"),
            F.max("_n_rows").alias("widest_group_rows"),
        )
        .collect()[0]
    )
    return (
        int(row["n_queries"]),
        int(row["n_with_positive"] or 0),
        int(row["n_all_positive"] or 0),
        int(row["widest_group_rows"] or 0),
    )


def _keep_scored_query_groups(
    df: SparkDataFrame,
    group_cols: Sequence[str],
    label_col: str,
    *,
    drop_all_positive_groups: bool,
) -> SparkDataFrame:
    """The rows of the query groups the ranking metrics score. Requires
    upstream ``total_rel``.

    Always the groups holding a positive: AP is undefined at ``total_rel ==
    0``. With ``drop_all_positive_groups`` also not the all-positive ones
    (``_all_positive_group``): every per-query metric of such a group is the
    same whatever the order, so it cannot tell two rankings apart.

    Switched off this is exactly the plain positive filter: no extra column,
    the same plan, so the metrics stay value-for-value what they were. Switched
    on, the extra window has the same spec as ``total_rel``'s, which Spark
    evaluates in the same pass.

    Every metrics pass filters here, the popularity baseline's included, so
    the two sides of the report's comparison cannot drop different groups.
    """
    has_positive = F.col("total_rel") > 0
    if not drop_all_positive_groups:
        return df.filter(has_positive)
    all_positive = "_all_positive_group"
    return (
        df.withColumn(
            all_positive,
            _all_positive_group(label_col, Window.partitionBy(*group_cols)),
        )
        .filter(has_positive & ~F.col(all_positive))
        .drop(all_positive)
    )


def add_row_contributions(
    df: SparkDataFrame,
    group_cols: list[str],
    label_col: str,
    k_values: list[int],
    *,
    score_col: str,
    item_col: str,
    event_cols: Sequence[str],
) -> SparkDataFrame:
    """Add per-row contribution columns. Requires upstream ``pos`` + ``total_rel``.

    ``pos`` must come from ``rank_within_query`` with the same ``score_col``,
    ``item_col`` and ``event_cols``: the running sum orders by that rule
    (``score_then_item_window``) rather than by ``pos``, because ordered by
    ``pos`` Spark cannot see the two orders are the same and sorts the whole
    scored frame a second time. A mismatch raises nothing: ``cum_rel``
    is counted in one order while ``pos`` numbers another, so ``prec_at_pos``
    and every ``ap_contrib`` come out wrong. Hence no defaults, so each call
    site shows its three arguments next to the ``rank_within_query`` call
    they must match.

    Always added:
        cum_rel       cumulative positives up to & including this position
        prec_at_pos   cum_rel / pos

    Per K:
        top_k@K       1.0 if pos <= K else 0.0
        ap_contrib@K  prec_at_pos * label * top_k@K
                      (sum over a query's label=1 rows = numerator of AP@K)
    """
    w_cum = score_then_item_window(
        group_cols, score_col, item_col, event_cols,
    ).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("cum_rel", F.sum(F.col(label_col)).over(w_cum))
    df = df.withColumn("prec_at_pos", F.col("cum_rel") / F.col("pos"))

    for k in k_values:
        df = df.withColumn(
            f"top_k@{k}", (F.col("pos") <= F.lit(k)).cast("double")
        )
        df = df.withColumn(
            f"ap_contrib@{k}",
            F.col("prec_at_pos") * F.col(label_col) * F.col(f"top_k@{k}"),
        )
    return df


# ---------------------------------------------------------------------------
# Layer 2 — per-query metrics
# ---------------------------------------------------------------------------


def _per_query_metric_cols(k_values: list[int]) -> list[str]:
    """Names of metric columns produced by compute_per_query_metrics: every
    query-group entry of ``metric_catalog``, at each K."""
    return [m.key(k) for k in k_values for m in metrics_of(QUERY_GROUP)]


def _query_group_number(name: str, k: int) -> F.Column:
    """A shared number of the query-group method (``metric_catalog.
    QUERY_GROUP_SHARED_NUMBERS``) as a column of the per-query frame at ``k``."""
    if name == "total_rel":
        return F.col("total_rel")
    if name == "K":
        return F.lit(k)
    return F.col(f"_{name}_{k}")


def _spark_least(a, b) -> F.Column:
    return F.least(*(v if isinstance(v, F.Column) else F.lit(v)
                     for v in (a, b)))


#: The operators of a ``metric_catalog.Expr`` on Spark columns. Spark's
#: division by 0 is NULL: the metric is undefined there.
_SPARK_OPS = {"+": operator.add, "*": operator.mul, "/": operator.truediv,
              "min": _spark_least}

_ARITHMETIC = {"+": operator.add, "*": operator.mul, "/": operator.truediv,
               "min": min}


def _driver_apply(op: str, a, b):
    """``op`` on collected numbers; ``None``, the metric undefined, for a
    division by 0 and for anything combined with ``None``."""
    if a is None or b is None or (op == "/" and b == 0):
        return None
    return _ARITHMETIC[op](a, b)


#: The same operators on an item's collected cell.
_DRIVER_OPS = {op: functools.partial(_driver_apply, op) for op in _ARITHMETIC}


def _evaluate(formula: Ratio | Expr, number, ops: Mapping):
    """``formula`` with each shared number read by ``number(name)`` and each
    operator by ``ops`` — :data:`_SPARK_OPS` per query group,
    :data:`_DRIVER_OPS` on an item's collected cell. A ``Ratio`` is
    ``numerator / denominator``."""
    if isinstance(formula, Ratio):
        formula = shared(formula.numerator) / shared(formula.denominator)
    if formula.op == "shared":
        return number(formula.args[0])
    if formula.op == "constant":
        return formula.args[0]
    left, right = (_evaluate(a, number, ops) for a in formula.args)
    return ops[formula.op](left, right)


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
        every query-group entry of ``metric_catalog`` for each K,
        plus any column in ``carry_cols`` (taken via F.first within the query —
        valid for per-customer attributes constant within a query, e.g. segment).

    Shared numbers, summed per query and K, that the formulas combine:
        ap_sum@K     = sum(ap_contrib@K)
        hits@K       = sum(label * top_k@K)
    Each entry's formula (a ``metric_catalog.Ratio`` or ``Expr``) combines
    them, ``total_rel`` and ``K``.

    See module docstring for the degenerate behaviour of the metrics when
    K >= n_items.
    """
    carry_cols = list(carry_cols or [])

    sums = [F.first("total_rel").alias("total_rel")]
    for c in carry_cols:
        sums.append(F.first(c).alias(c))
    for k in k_values:
        sums.extend(
            [
                F.sum(f"ap_contrib@{k}").alias(f"_ap_sum_{k}"),
                F.sum(F.col(label_col) * F.col(f"top_k@{k}")).alias(f"_hits_{k}"),
            ]
        )

    per_query = enriched.groupBy(*group_cols).agg(*sums)

    for k in k_values:
        for metric in metrics_of(QUERY_GROUP):
            per_query = per_query.withColumn(
                metric.key(k),
                _evaluate(metric.formula,
                          lambda name: _query_group_number(name, k),
                          _SPARK_OPS),
            )
        per_query = per_query.drop(f"_ap_sum_{k}", f"_hits_{k}")

    keep = list(group_cols) + ["total_rel"] + carry_cols + _per_query_metric_cols(k_values)
    return per_query.select(*keep)


# ---------------------------------------------------------------------------
# Layer 3 — aggregations
# ---------------------------------------------------------------------------


def aggregate_overall(
    per_query: SparkDataFrame, k_values: list[int]
) -> dict[str, float]:
    """Equal-query weight mean of per-query metrics.

    Returns a flat dict: every query-group entry of ``metric_catalog`` at
    each K (``map@K``, ``precision@K``, …). The metrics at K >= n_items: see
    module docstring.
    """
    metric_cols = _per_query_metric_cols(k_values)
    row = per_query.agg(*[F.mean(c).alias(c) for c in metric_cols]).collect()[0]
    return {c: float(row[c]) for c in metric_cols}


def aggregate_per_segment(
    per_query: SparkDataFrame, seg_col: str, k_values: list[int]
) -> dict[str, dict[str, float]]:
    """Equal-query weight mean of per-query metrics, grouped by ``seg_col``.

    ``per_query`` must carry ``seg_col`` (pass it via ``carry_cols`` when
    building per_query). Returns ``{seg_value: {metric@K: value}}``, the
    metrics as :func:`aggregate_overall`'s; the seg_value is stringified when
    not already a string. Keys come out sorted: ``macro_average`` sums
    ``per_segment`` in key order (:func:`_nest_by_dims`).
    """
    metric_cols = _per_query_metric_cols(k_values)
    rows = (
        per_query.groupBy(seg_col)
        .agg(*[F.mean(c).alias(c) for c in metric_cols])
        .collect()
    )
    out = {segment_key(r[seg_col]): {c: float(r[c]) for c in metric_cols}
           for r in rows}
    return dict(sorted(out.items()))


def _item_shared_number_aggs(
    label_col: str, k_values: list[int]
) -> list[tuple[str, F.Column]]:
    """``(key, aggregate)`` for every item shared number (``metric_catalog.
    ITEM_SHARED_NUMBERS``): the ones without K first, then the per-K ones at
    each K, the order the metric cells use. Counts are integer sums, so they
    come back as ``int``; ``ap_sum`` as ``float``."""
    positive = F.col(label_col) == 1

    def count(condition: F.Column) -> F.Column:
        return F.sum(F.when(condition, 1).otherwise(0))

    def top(k: int) -> F.Column:
        return F.col(f"top_k@{k}") == 1

    # One aggregate per shared number, at K (ignored by the ones not per K).
    aggregate = {
        "n_pos": lambda k: count(positive),
        "pos_sum": lambda k: F.sum(F.when(positive, F.col("pos")).otherwise(0)),
        "n_top": lambda k: count(top(k)),
        "hits": lambda k: count(positive & top(k)),
        "ap_sum": lambda k: F.sum(
            F.when(positive, F.col(f"ap_contrib@{k}")).otherwise(0.0)),
    }
    out = [(name, aggregate[name](None)) for name in ITEM_SHARED_NUMBERS
           if name not in ITEM_SHARED_NUMBERS_PER_K]
    for k in k_values:
        out.extend((f"{name}@{k}", aggregate[name](k))
                   for name in ITEM_SHARED_NUMBERS
                   if name in ITEM_SHARED_NUMBERS_PER_K)
    return out


def _nest_by_dims(rows, dim_cols: list[str], cell_of) -> dict[str, dict]:
    """``{item: cell}`` for ``[item_col]``, ``{item: {segment: cell}}`` for
    ``[item_col, seg_col]``.

    Two levels rather than a joined string key (ADR-0020 bug 12):
    ``"_".join`` turned ``(item="a", seg="b_c")`` and ``(item="a_b",
    seg="c")`` into the same key and the later row silently overwrote the
    earlier one. Dim values are stringified.

    Keys come out sorted, items first, then segments within an item: a
    collect's row order follows the execution plan, and unsorted the last
    digit of a macro over these cells would move with it (``macro_average``
    sums in key order).
    """
    def keys(r) -> tuple[str, ...]:
        item = str(r[dim_cols[0]])
        if len(dim_cols) == 1:
            return (item,)
        # The second dim is the segment; NULL there is the unmatched group.
        return item, segment_key(r[dim_cols[1]])

    out: dict[str, dict] = {}
    for key, r in sorted(((keys(r), r) for r in rows), key=lambda p: p[0]):
        if len(key) == 1:
            out[key[0]] = cell_of(r)
        else:
            out.setdefault(key[0], {})[key[1]] = cell_of(r)
    return out


def aggregate_item_shared_numbers(
    enriched: SparkDataFrame,
    dim_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict[str, dict]:
    """The item method's shared numbers (``metric_catalog.
    ITEM_SHARED_NUMBERS``, ADR-0031 decision 7), one cell per item or per
    (item, segment), over **every** row of it in ``enriched`` — positive or
    not. ``enriched`` holds the query groups the query-group method scores and
    the ``pos`` / ``top_k@K`` / ``ap_contrib@K`` columns of
    ``add_row_contributions`` at every K of ``k_values``.

    ``dim_cols`` is ``[item_col]`` or ``[item_col, seg_col]``; the output nests
    one level per column (:func:`_nest_by_dims`):

        [item_col]          → {item: {n_pos, pos_sum, n_top@K, hits@K, …}}
        [item_col, seg_col] → {item: {segment: {n_pos, …}}}

    Any other column count raises ``ValueError``. An item never positive here
    still gets a cell: ``n_top@K`` over all of its rows is the times it was
    recommended, whether or not any of them was a hit.

    One ``groupBy`` and one collect; what reaches the driver is a handful of
    numbers per item and K.
    """
    if len(dim_cols) not in (1, 2):
        raise ValueError(
            f"aggregate_item_shared_numbers supports 1 or 2 dim_cols, got "
            f"{dim_cols!r}"
        )
    aggs = _item_shared_number_aggs(label_col, k_values)
    rows = enriched.groupBy(*dim_cols).agg(
        *[agg.alias(key) for key, agg in aggs]).collect()

    return _nest_by_dims(
        rows, dim_cols, lambda r: {key: r[key] for key, _ in aggs})


def _item_number(cell: dict, name: str, k: int) -> float:
    """One shared number of a cell of :func:`aggregate_item_shared_numbers`,
    at ``k`` when it is per K."""
    return float(cell[f"{name}@{k}" if name in ITEM_SHARED_NUMBERS_PER_K
                      else name])


def _item_metric_cell(
    cell: dict, k_values: list[int], method: str = ITEM,
) -> dict | None:
    """Every ``method`` entry of ``metric_catalog`` (:data:`ITEM`, or
    ``ITEM_EVERY_GROUP``, whose cells hold the same names) computed from one
    cell of shared numbers by its formula, the ones without K first, then the
    per-K ones at each K; plus ``n_pos``, the weight ``macro_average`` reads.

    A metric that divides by 0 is undefined and left out of the cell, and a
    cell left with no metric is ``None``.
    """
    def value(metric, k: int | None) -> float | None:
        return _evaluate(metric.formula,
                         lambda name: _item_number(cell, name, k), _DRIVER_OPS)

    entries = [(m.key(), value(m, None))
               for m in metrics_of(method) if not m.per_k]
    for k in k_values:
        entries.extend((m.key(k), value(m, k))
                       for m in metrics_of(method) if m.per_k)
    out = {key: v for key, v in entries if v is not None}
    if not out:
        return None
    out[_N_POS_KEY] = cell["n_pos"]
    return out


def item_metrics(
    shared: dict[str, dict], k_values: list[int], n_dims: int,
    method: str = ITEM,
) -> dict[str, dict]:
    """The per-item metric cells of :func:`aggregate_item_shared_numbers`'
    output, nested the same way (``n_dims`` levels) — or of
    ``prediction_quality.item_top_k_metrics``' counts, ``method``
    ``ITEM_EVERY_GROUP``. Plain Python: the formulas divide numbers already
    on the driver."""
    if n_dims == 1:
        return {item: m for item, cell in shared.items()
                if (m := _item_metric_cell(cell, k_values, method)) is not None}
    out: dict[str, dict] = {}
    for item, by_seg in shared.items():
        cells = item_metrics(by_seg, k_values, 1, method)
        if cells:
            out[item] = cells
    return out


def aggregate_per_item(
    enriched: SparkDataFrame,
    dim_cols: list[str],
    label_col: str,
    k_values: list[int],
) -> dict[str, dict]:
    """The per-item metric cells straight from ``enriched``:
    :func:`aggregate_item_shared_numbers`, then :func:`item_metrics`.

    Metrics per cell — every item entry of ``metric_catalog``, a ratio of the
    item's shared numbers:

        recall@K     = hits@K / n_pos
                       = P(rank(P) <= K | P is positive)
                         (row-equal-weight over the item's positive rows)
        map_attr@K   = ap_sum@K / n_pos
                       (per-row AP@K contribution averaged across all
                        queries where P is positive)
        mean_pos     = pos_sum / n_pos
                       (average ranked position of P when it is the truth)
        n_pos        = count of P-positive rows (weight source for macro_average)

    NOTE: ``recall@K`` is also a query-group metric (in ``overall``). One name
    in two methods (ADR-0031 decision 6): both are the share of positives
    ranked within the top K, over one query group's positives there and over
    one item's here. A per-item precision would divide by ``n_top@K`` — the
    times the item was recommended — not by K (decision 7).
    """
    return item_metrics(
        aggregate_item_shared_numbers(enriched, dim_cols, label_col, k_values),
        k_values, len(dim_cols))


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
    """Parameterized macro over dim keys (defaults = equal weight).

    ``n_pos`` is the reserved weight-source key — never averaged into the
    output. Non-default params require every inner dict to carry ``n_pos``;
    otherwise raises ValueError (fail loud, no silent equal-weight fallback).
    Per-metric combine goes through ``metrics.macro_from_per_item``; a metric
    whose items are all excluded by ``min_positives`` is omitted.

    Default params take the plain sum/len path, not ``np.dot``: with uniform
    weights ``np.dot`` can differ from ``sum/len`` in the last ulp, and the
    real-run regression gate compares report values verbatim.
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
            f"Upstream must be item_metrics (which emits n_pos)."
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
    "per_item_shared_numbers": {},
    "per_item_segment_shared_numbers": {},
}


def _compute_core(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    segment_columns: Sequence[str],
    *,
    event_cols: Sequence[str],
    drop_all_positive_groups: bool,
    facts: Mapping,
) -> dict:
    """The fine-grained metric bundle (overall/per_item/per_segment/...).

    No category, no row facts. Runs on the fine-grained frame and, on a
    collapsed DF, the category pass.

    ``drop_all_positive_groups`` has no default for the reason given for
    ``event_cols`` below: a default would leave the category pass silently
    unfiltered when the switch is on. Unlike ``event_cols`` the two passes get
    the same value; what differs is the frame the all-positive groups are
    decided on (see ``compute_all_metrics``).

    ``facts`` is this grain's row facts (``compute_row_facts``), counted on
    this same frame or on one holding the same rows under other scores: the
    query group counts decide whether anything is left to score, and the item
    count and widest group what ``"all"`` resolves to. Counted on another
    frame — the category pass handed the item grain's, a baseline handed
    another month's — every ``@all`` metric is keyed at the wrong K and no
    error is raised. Switched on, the bundle scores the groups holding a
    positive less the all-positive ones; when that is 0 the bundle is the
    empty one.

    The bundle records what it was computed on (``metrics.POPULATION_KEY``),
    the empty one included.

    ``event_cols`` is passed in rather than read from ``parameters``, and has
    no default, because **the two passes need different answers and the frame
    does not say which**. The fine-grained pass gets ``schema.columns.event``;
    the category pass gets ``()``. A default would make the category pass wrong
    by omission: it would ask for the ``event`` column on a frame
    ``collapse_to_categories`` has already aggregated it away from.

    ``event_cols`` also decides what ``"all"`` resolves to (``_resolve_all_k``):
    it is the one argument that says whether this frame's rows are events.
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    group_cols = schema["query_group_columns"]

    eval_params = parameters.get("evaluation", {}) or {}
    _require_segment_columns_in_frame(eval_predictions, segment_columns)
    # One segment dimension per run: the first joined column.
    active_seg_col = segment_columns[0] if segment_columns else None
    macro_params = metric_params(parameters)

    n_queries_with_pos = n_groups_with_positive(facts)
    n_all_positive = facts[ALL_POSITIVE_KEY]
    all_k = _resolve_all_k(
        eval_predictions, group_cols, item_col, event_cols,
        n_items=facts["dataset_overview"]["totals"]["n_items"],
        widest_group_rows=facts[WIDEST_GROUP_KEY],
    )
    query_ks, item_ks = _resolve_k_grids(parameters, all_k)
    # Before the empty returns, which carry it too: the ties are of the
    # scores, whether or not any group is left to score.
    ties = _tied_rows(
        eval_predictions, parameters,
        facts["dataset_overview"]["totals"]["n_rows"])

    def population(per_item: dict) -> dict:
        # The groups scored are the ones holding a positive (AP is undefined
        # without one), less the all-positive ones when the switch drops
        # them; none without a positive, so that count is 0 by construction.
        # The macro averages the per-item cells kept by min_positives, the
        # rule macro_average applies. The segment columns are the ones this
        # call was handed: a side computed with other ones, or none, says so.
        return {POPULATION_KEY: {
            ALL_K_KEY: all_k,
            MACRO_ITEMS_KEY: sorted(
                item for item, m in per_item.items()
                if m.get(_N_POS_KEY, 0) >= macro_params["min_positives"]),
            N_SCORED_WITH_POSITIVE_KEY: (
                n_queries_with_pos
                - (n_all_positive if drop_all_positive_groups else 0)),
            N_SCORED_WITHOUT_POSITIVE_KEY: 0,
            SEGMENT_COLUMNS_KEY: list(segment_columns),
        }}

    # ---- Layer 1: row-level enrichment ----
    df = rank_within_query(
        eval_predictions, group_cols, score_col, item_col, event_cols,
    )
    df = add_query_total_rel(df, group_cols, label_col)

    scored = _keep_scored_query_groups(
        df, group_cols, label_col,
        drop_all_positive_groups=drop_all_positive_groups,
    )

    if n_queries_with_pos == 0:
        logger.warning("No queries with positive labels found")
        return {**_EMPTY_RESULT, **population({}), **ties}
    if drop_all_positive_groups and n_all_positive == n_queries_with_pos:
        logger.warning(
            "No query group left to score: all %d holding a positive are "
            "all-positive, and evaluation.query_filter."
            "drop_all_positive_groups drops them",
            n_all_positive,
        )
        return {**_EMPTY_RESULT, **population({}), **ties}

    enriched = add_row_contributions(
        scored, group_cols, label_col, item_ks,
        score_col=score_col, item_col=item_col, event_cols=event_cols,
    )
    enriched = enriched.cache()
    try:
        # ---- Layer 2: per-query metrics (carries seg for per_segment) ----
        carry = [active_seg_col] if active_seg_col else []
        # Cached with or without a segment, though without one only
        # aggregate_overall reads it: skipping the cache there measured no
        # difference.
        per_query = compute_per_query_metrics(
            enriched, group_cols, label_col, query_ks, carry_cols=carry
        ).cache()
        try:
            # ---- Layer 3: aggregations ----
            overall = aggregate_overall(per_query, query_ks)
            per_item_shared = aggregate_item_shared_numbers(
                enriched, [item_col], label_col, item_ks
            )
            per_item = item_metrics(per_item_shared, item_ks, 1)

            per_segment: dict = {}
            per_item_segment: dict = {}
            per_item_segment_shared: dict = {}
            if active_seg_col:
                per_segment = aggregate_per_segment(
                    per_query, active_seg_col, query_ks
                )
                per_item_segment_shared = aggregate_item_shared_numbers(
                    enriched, [item_col, active_seg_col], label_col, item_ks
                )
                per_item_segment = item_metrics(
                    per_item_segment_shared, item_ks, 2)

            macro_avg: dict = {"by_item": macro_average(per_item, **macro_params)}
            # The unmatched group stays in per_segment / per_item_segment (the
            # report shows and counts it) but in no macro: it is not a segment.
            if per_segment:
                macro_avg["by_segment"] = macro_average({
                    seg: m for seg, m in per_segment.items()
                    if seg != UNMATCHED_SEGMENT
                })
            if per_item_segment:
                # per_item_segment is two-level {item: {segment: cell}}:
                # flatten to every (item, segment) cell except the unmatched
                # ones, then macro with one vote per cell. Cells are also
                # subject to min_positives; observation_items is reported at
                # item grain only, cells are not listed.
                cells = {
                    (item, seg): cell
                    for item, by_seg in per_item_segment.items()
                    for seg, cell in by_seg.items()
                    if seg != UNMATCHED_SEGMENT
                }
                macro_avg["by_item_segment"] = macro_average(
                    cells, **macro_params
                )

            observation_items = sorted(
                it for it, m in per_item.items()
                if m.get("n_pos", 0) < macro_params["min_positives"]
            ) if macro_params["min_positives"] > 0 else []

            return {
                "overall": overall,
                "per_segment": per_segment,
                "per_item": per_item,
                "per_item_segment": per_item_segment,
                "macro_avg": macro_avg,
                "observation_items": observation_items,
                **population(per_item),
                **ties,
                "per_item_shared_numbers": per_item_shared,
                "per_item_segment_shared_numbers": per_item_segment_shared,
            }
        finally:
            per_query.unpersist()
    finally:
        enriched.unpersist()


def _grain_row_facts(
    frame: SparkDataFrame, parameters: dict, segment_columns: Sequence[str],
) -> dict:
    """The row facts of one grain: the overview, the query group counts and
    the all-positive accounts.

    The counts are ``_count_query_groups``' one action; ``WIDEST_GROUP_KEY``
    is what ``"all"`` resolves to on a frame of event rows
    (``_resolve_all_k``), recorded on every grain so the metrics never count
    it again. The accounts (``metrics.all_positive_accounts``) are computed
    here off those counts, so every caller's facts carry them and the report
    and the log read them instead of computing them.
    """
    schema = get_schema(parameters)
    n_queries, n_with_positive, n_all_positive, widest_group_rows = (
        _count_query_groups(
            frame, schema["query_group_columns"], schema["label"]))
    facts = {
        "dataset_overview": compute_dataset_overview(
            frame, parameters, segment_columns=segment_columns),
        "n_queries": n_queries,
        "n_excluded_queries": n_queries - n_with_positive,
        ALL_POSITIVE_KEY: n_all_positive,
        WIDEST_GROUP_KEY: widest_group_rows,
    }
    facts.update(all_positive_accounts(facts, parameters))
    return facts


def compute_row_facts(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    *,
    segment_columns: Sequence[str] = (),
    with_category: bool = True,
    category_mapping: Mapping[str, str] | None = None,
) -> dict:
    """What the rows hold whatever their scores (ADR-0031 decision 4): the
    dataset overview and the query group counts, at the item grain and — when
    ``item_categories`` maps the items — at the category grain under
    ``category``.

    One result for every side ranked on these rows: the model and its
    popularity baseline are the same rows with other scores.

    The counts are of the rows, before any filter: ``n_queries`` every group,
    ``n_excluded_queries`` the ones holding no positive, and
    ``metrics.ALL_POSITIVE_KEY`` the all-positive ones among those holding a
    positive, whatever ``evaluation.query_filter.drop_all_positive_groups``
    says — which of them the metrics scored is the metrics' own ledger
    (``metrics.POPULATION_KEY``). Beside them, per grain, the all-positive
    accounts (``metrics.all_positive_accounts``): the groups holding a
    positive, the all-positive share, and whether it warns — the one fact that
    reads the switch (it warns only with the switch off).

    The category grain collapses the frame as ``compute_all_metrics`` does,
    with the same ``category_mapping`` (``None`` reads the hand-written
    mapping, :func:`_resolve_category_mapping`), so the two grains' facts
    describe the frames the two grains' metrics rank.
    """
    _require_segment_columns_in_frame(eval_predictions, segment_columns)
    facts = _grain_row_facts(eval_predictions, parameters, segment_columns)
    mapping = (_resolve_category_mapping(parameters, category_mapping)
               if with_category else None)
    if mapping is not None:
        collapsed = collapse_to_categories(
            eval_predictions, parameters, segment_columns=segment_columns,
            category_mapping=mapping,
        )
        facts["category"] = _grain_row_facts(
            collapsed, parameters, segment_columns)
    return facts


def compute_all_metrics(
    eval_predictions: SparkDataFrame,
    parameters: dict,
    row_facts: Mapping,
    *,
    segment_columns: Sequence[str] = (),
    drop_all_positive_groups: bool = False,
    with_category: bool = True,
    category_mapping: Mapping[str, str] | None = None,
) -> dict:
    """Full bundle: fine-grained core + optional category.

    Required columns: ``time``, ``entity``, ``item``, ``label``, ``score``
    (column names resolved from parameters['schema']), plus every column in
    ``segment_columns``. Per-segment slices group by the first of those; pass
    what ``prepare_eval_data`` joined (``evaluation_segment_columns``), never
    ``evaluation.segment_columns`` filtered by the frame's columns (see
    ``_require_segment_columns_in_frame``). Empty means no per-segment slice. A NULL
    segment value is the ``segments.UNMATCHED_SEGMENT`` group: present in
    ``per_segment`` and ``per_item_segment``, absent from every macro.

    ``row_facts`` is :func:`compute_row_facts` of these rows, with the same
    ``segment_columns``, ``with_category`` and ``category_mapping``: the
    metrics read their query group counts, item count and widest group off
    it rather than counting them again (ADR-0031 principle 4). Its scores do
    not matter, its rows do: see ``_compute_core`` for what goes wrong
    without an error otherwise. The facts are not copied into the bundle; it
    records only what it was computed on (``metrics.POPULATION_KEY``).

    ``category`` (same shape as the top level, never re-nested) is added only
    when ``item_categories.enabled`` and ``with_category``.

    ``category_mapping`` is the table a column-mode evaluation run read off
    the data; ``None`` reads the hand-written mapping from ``parameters``
    (:func:`_resolve_category_mapping`). ``with_category=False`` skips the
    category pass without reading ``item_categories`` at all, for a caller
    that reads only the fine-grained keys and has no column-mode table to
    pass.

    Returns::

        {
          "overall":          {map@K, precision@K, recall@K, ...},
          "per_segment":      {seg_value: {map@K, precision@K, recall@K, ...}},
          "per_item":         {item: {recall@K, map_attr@K, mean_pos, ...}},
          "per_item_segment": {item: {segment: {recall@K, map_attr@K, mean_pos, n_pos}}},
          "macro_avg": {
              "by_segment":      {map@K, precision@K, recall@K, ...},
              "by_item":         {recall@K, map_attr@K, mean_pos, ...},
              "by_item_segment": {recall@K, map_attr@K, mean_pos, ...},
          },
          "observation_items": [item, ...]  items with n_pos < evaluation.metric.min_positives; empty by default
          "population": (metrics.POPULATION_KEY) what the numbers were
              computed on, at this grain: "all_k" (metrics.ALL_K_KEY, the K
              ``"all"`` resolved to — read it through
              ``resolved_all_k``, never by counting items),
              "macro_items" (the items ``macro_avg.by_item`` averaged over),
              "n_scored_queries_with_positive" / "..._without_positive" (the
              query groups scored: those holding a positive, less the
              all-positive ones when ``drop_all_positive_groups``; none
              without a positive), "segment_columns" (the
              ``segment_columns`` passed here),
          "per_item_shared_numbers":
              {item: {n_pos, pos_sum, n_top@K, hits@K, ap_sum@K, ...}}
              what the per_item metrics are computed from, for every
              item with a row in the scored groups — never positive included
              (``aggregate_item_shared_numbers``),
          "per_item_segment_shared_numbers":
              {item: {segment: {...}}}  the same per (item, segment); empty
              without a segment column,
          "category":  (only when item_categories.enabled)
              same shape as the top level (overall / per_item / per_segment /
              per_item_segment / macro_avg / population / the two
              shared-number blocks); never contains "category".
        }

    Queries with zero positives are excluded from the metric computation
    (AP is undefined when total_rel = 0).

    ``drop_all_positive_groups`` (default off) also excludes the
    *all-positive* queries: the same positive label on every row, so every
    per-query metric is the same whatever the order. The caller reads the
    switch — ``result_readers.drop_all_positive_groups`` — and this function
    never reads it from ``parameters``: the switch is the evaluation report's.
    (Training's test mAP must not move with it either; it scores through
    ``compute_untruncated_ap``, which never drops those groups.) Each grain
    decides on its own frame: at the category grain a query is all-positive when
    every category it holds gets the same positive label (the max of its
    children), which a query mixed row by row can be. Off, every key is
    value-for-value what it was. Known effect, not a bug: per-item keys such
    as ``map_attr@K`` and ``recall@K`` still depend on the order inside an
    all-positive query when K is below its row count (``ap_contrib@K =
    prec_at_pos × label × top_k@K``: which rows reach the top K decides which
    items get the credit); switched on, those queries leave them too.
    """
    # The segment check the core opens with runs here too, so a missing
    # column still fails before any scan.
    _require_segment_columns_in_frame(eval_predictions, segment_columns)
    # The fine-grained pass ranks the rows as they were written: one per
    # candidate, so `event` is what tells two rows of one item apart.
    result = _compute_core(
        eval_predictions, parameters, segment_columns,
        event_cols=get_schema(parameters).get("event", []),
        drop_all_positive_groups=drop_all_positive_groups,
        facts=row_facts,
    )

    mapping = (_resolve_category_mapping(parameters, category_mapping)
               if with_category else None)
    if mapping is not None:
        if "category" not in row_facts:
            raise ValueError(
                "compute_all_metrics: item_categories maps the items, but the "
                "row facts passed hold no category grain — they were counted "
                "with another category setting than these metrics. Count them "
                "with the same with_category / category_mapping "
                "(compute_row_facts).")
        collapsed = collapse_to_categories(
            eval_predictions, parameters, segment_columns=segment_columns,
            category_mapping=mapping,
        )
        # The category pass ranks a DIFFERENT frame:
        # `collapse_to_categories` groups by (query group, category) and takes
        # max(score) / max(label), so it emits exactly one row per pair and
        # the `event` columns are aggregated away. The category column already
        # decides every tie, and asking for `event` here is asking for a
        # column the frame does not have. The switch is the same; which
        # queries it drops is decided on this frame (label = max of the
        # children), so a query mixed row by row can be all-positive here —
        # hence the category grain's own facts.
        # Not cached, though every action of this pass collapses again:
        # cached, the pass measured slower, not faster. The cache holds
        # spark.sql.shuffle.partitions partitions (AQE does not coalesce a
        # cached plan's output), and while one groupBy read it faster than a
        # fresh collapse, compute_dataset_overview as a whole took longer on
        # it. Not traced stage by stage; measured on 2.4M local rows, so
        # re-measure before caching it at production scale.
        result["category"] = _compute_core(
            collapsed, parameters, segment_columns, event_cols=(),
            drop_all_positive_groups=drop_all_positive_groups,
            facts=row_facts["category"],
        )

    return result


# ---------------------------------------------------------------------------
# Training's test scoring (ADR-0028 decision 2)
# ---------------------------------------------------------------------------
#
# These two functions compute only what training needs from its test scoring:
# three Spark actions in all, no K, and no ``evaluation.*`` setting read.


class QueryGroupCounts(NamedTuple):
    """What :func:`count_query_groups_by_time` counts, in one action.

    ``times`` is the set of time values present, as text: the scored months a
    caller asked for and did not get are the ones missing from it.
    """

    n_queries: int
    n_with_positive: int
    times: frozenset


def count_query_groups_by_time(
    frame: SparkDataFrame, parameters: dict,
) -> QueryGroupCounts:
    """Every query group, those holding a positive, and the time values
    present — one ``groupBy`` over the query groups, one action.

    The first two are the counts ``_count_query_groups`` gives:
    ``groupBy`` groups NULL keys together as ``distinct`` does, and "holding a
    positive" is ``sum(label) > 0``. The time column is one of the query
    group's columns, so collecting its values after the first ``groupBy``
    costs no extra pass; the set is at most one entry per scored month. Not
    counted: the all-positive groups — training never drops them.
    """
    schema = get_schema(parameters)
    label_col = schema["label"]
    time_col = schema["time"]
    row = (
        frame.groupBy(*schema["query_group_columns"])
        .agg(F.sum(F.col(label_col)).alias("_total_rel"))
        .agg(
            F.count(F.lit(1)).alias("n_queries"),
            F.sum(F.when(F.col("_total_rel") > 0, 1).otherwise(0))
            .alias("n_with_positive"),
            F.collect_set(F.col(time_col).cast("string")).alias("times"),
        )
        .collect()[0]
    )
    return QueryGroupCounts(
        n_queries=int(row["n_queries"]),
        n_with_positive=int(row["n_with_positive"] or 0),
        times=frozenset(row["times"] or ()),
    )


class UntruncatedAp(NamedTuple):
    """What :func:`compute_untruncated_ap` returns.

    ``overall_map`` is the mean over query groups holding a positive of each
    group's AP (``mean_ap``); ``per_item_map_attr`` is, per item, the mean of
    its positive rows' AP contributions (the per-item attribution
    ``macro_per_item_map`` averages).
    """

    overall_map: float
    per_item_map_attr: dict


def compute_untruncated_ap(
    frame: SparkDataFrame, parameters: dict,
) -> UntruncatedAp:
    """Per-query AP with no truncation, averaged over query groups, and each
    item's attribution — two actions over one cached frame.

    The same building blocks and arithmetic ``compute_all_metrics`` produces
    ``map@{"all"}`` and ``map_attr@{"all"}`` with, minus the K: at the K
    ``"all"`` resolves to, no row ranks past it, so ``top_k`` is 1.0 on every
    row and ``ap_contrib = prec_at_pos * label`` exactly. Nothing reads K, so
    nothing reads ``evaluation.k_values``, and the category pass and the
    dataset overview are not computed.

    Groups holding no positive are skipped (AP is undefined there); the
    all-positive groups are kept whatever
    ``evaluation.query_filter.drop_all_positive_groups`` says. With no group
    holding a positive: ``0.0`` and ``{}``.
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    label_col = schema["label"]
    group_cols = schema["query_group_columns"]

    score_col = schema["score"]
    event_cols = schema.get("event", [])
    df = rank_within_query(frame, group_cols, score_col, item_col, event_cols)
    df = add_query_total_rel(df, group_cols, label_col)
    scored = _keep_scored_query_groups(
        df, group_cols, label_col, drop_all_positive_groups=False,
    )
    enriched = (
        add_row_contributions(
            scored, group_cols, label_col, [],
            score_col=score_col, item_col=item_col, event_cols=event_cols,
        )
        .withColumn("_ap_contrib", F.col("prec_at_pos") * F.col(label_col))
        .cache()
    )
    try:
        overall = (
            enriched.groupBy(*group_cols)
            .agg(F.sum("_ap_contrib").alias("_ap_sum"),
                 F.first("total_rel").alias("_total_rel"))
            .withColumn("_ap", F.col("_ap_sum") / F.col("_total_rel"))
            .agg(F.mean("_ap").alias("_map"))
            .collect()[0]["_map"]
        )
        per_item = (
            enriched.filter(F.col(label_col) == 1)
            .groupBy(item_col)
            .agg(F.mean("_ap_contrib").alias("_attr"))
            .collect()
        )
    finally:
        enriched.unpersist()
    return UntruncatedAp(
        overall_map=float(overall) if overall is not None else 0.0,
        per_item_map_attr={str(r[item_col]): float(r["_attr"]) for r in per_item},
    )


# ---------------------------------------------------------------------------
# The binary-prediction metrics on test (ADR-0028 decision 4)
# ---------------------------------------------------------------------------
#
# HPO scores these on val with scikit-learn's ``average_precision_score``
# (``evaluation/metrics.py``); test's predictions are already in Hive, so the
# same definition is computed here, exactly, with Spark's built-in functions.
# Every candidate row is one yes/no prediction, query groups ignored. Per
# distinct score s, from the highest down:
#
#     precision(s) = (positive weight scored >= s) / (weight scored >= s)
#     AP           = sum over s of  positive weight at s * precision(s)
#                    / total positive weight
#
# which is scikit-learn's ``sum((R_n - R_{n-1}) * P_n)``: a tied score is one
# threshold, whatever order its rows arrive in, and each row weighs its
# ``zero_positive_group_weight`` (1, or 1 / r on a kept group holding no
# positive). A row's share of its threshold's term is ``positive weight of the
# row * precision(s)``, so summing that over the rows gives AP's numerator; a
# RANGE window frame gives each row its threshold's precision
# (``_scored_at_or_above``).


def _binary_prediction_rows(
    frame: SparkDataFrame, parameters: dict, *keep: str, weighted: bool = True,
) -> SparkDataFrame:
    """Score, weight and positive weight per row, plus the columns ``keep``.

    ``weighted`` (training's test, and evaluation's ``--post-training``): the
    weight is ``ZERO_POSITIVE_GROUP_WEIGHT_COL``, never 1. These metrics are
    only computed when test kept query groups holding no positive (A54), and
    the column is how those groups count ``1 / r`` times, as they do on val.
    Its absence means the table was not written under a positive test ratio
    (A45 makes the catalog declare it), so it raises rather than count each
    row once — a number for a different population, with nothing to say so.

    Not ``weighted`` (the ``--prediction-quality`` mode on monitoring rows,
    which no dataset draw touched): every row weighs 1, and no weight column
    is read.
    """
    schema = get_schema(parameters)
    if not weighted:
        return frame.select(
            *keep,
            F.col(schema["label"]).alias("_label"),
            F.col(schema["score"]).cast("double").alias("_score"),
            F.lit(1.0).alias("_w"),
            F.col(schema["label"]).cast("double").alias("_pos_w"),
        )
    if ZERO_POSITIVE_GROUP_WEIGHT_COL not in frame.columns:
        raise ValueError(
            f"the binary-prediction metrics weight every row by "
            f"{ZERO_POSITIVE_GROUP_WEIGHT_COL!r}, and the test predictions have "
            f"no such column: they were not written under a positive "
            f"dataset.test_zero_positive_group_ratio (A45 makes the catalog "
            f"entry declare it)."
        )
    weight = F.col(ZERO_POSITIVE_GROUP_WEIGHT_COL).cast("double")
    return frame.select(
        *keep,
        F.col(schema["label"]).alias("_label"),
        F.col(schema["score"]).cast("double").alias("_score"),
        weight.alias("_w"),
        (F.col(schema["label"]).cast("double") * weight).alias("_pos_w"),
    )


def _scored_at_or_above(*partition_cols: str) -> WindowSpec:
    """Each row's frame: the rows of its partition scored at or above it, ties
    included — what one threshold's precision is summed over. RANGE, not ROWS:
    a ROWS frame would split a tie into as many thresholds as it has rows, and
    the two agree on any data without ties."""
    return (
        Window.partitionBy(*partition_cols).orderBy(F.col("_score").desc())
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )


def _null_weight_count() -> "F.Column":
    """Rows whose weight is NULL — counted inside an aggregation the metric
    runs anyway, so the check costs no action of its own."""
    return F.sum(F.when(F.col("_w").isNull(), 1).otherwise(0)).alias("_n_null_w")


def _require_weights_and_a_positive(n_null_w, total_pos, metric: str) -> None:
    """Pre-check on the rows: every one weighted, and some positive.

    A NULL weight is what training writes when test kept no zero-positive
    group while the catalog declares the column; summing it would drop that
    row in silence. No positive row leaves average precision undefined, and
    the val side raises there too (``metrics._require_a_positive_row``).
    """
    if n_null_w:
        raise ValueError(
            f"{metric}: {int(n_null_w)} test prediction row(s) hold a NULL "
            f"{ZERO_POSITIVE_GROUP_WEIGHT_COL}: they were written while test "
            f"kept no query group holding no positive, so they carry no design "
            f"weight to score by."
        )
    if not total_pos:
        raise ValueError(
            f"{metric}: test holds no positive row, so average precision is "
            f"undefined."
        )


#: The most segments ``compute_pooled_average_precision`` cuts the scores into.
#: A row's segment is one CASE WHEN branch per cut point; at 2000 branches the
#: generated code outgrew Spark's 64 KB method limit and fell back to
#: interpreted evaluation with an ERROR in the log, at 1000 it did not
#: (measured 2026-09-24, PySpark 3.3.2, 20,000 rows; the metric came out exact
#: either way). Half of that leaves room, and the per-row cost of finding a
#: segment grows with the count.
MAX_SCORE_SEGMENTS = 512


def compute_pooled_average_precision(
    frame: SparkDataFrame, parameters: dict, *, weighted: bool = True,
) -> float:
    """``pooled_average_precision`` on test: every row ranked by score in one
    pool — the definition ``metrics.compute_pooled_average_precision`` gives
    val. Three actions, and no partition holding every row.

    Each threshold's precision needs the weight scored above it across the
    whole table. One window over the table would give it, but a window with
    no ``partitionBy`` moves every row into one partition. So:

    1. Cut the scores, highest first, into as many segments as
       ``spark.sql.shuffle.partitions`` (at most :data:`MAX_SCORE_SEGMENTS`),
       at approximate quantiles (action 1).
       A row's segment is a function of its score alone, so a tied score is
       never split across two; only balance depends on the approximation.
    2. Each segment's positive weight and weight, to the driver (action 2).
    3. On the driver, each segment's head start: the sums over every segment
       above it.
    4. Inside each segment, the window's running sums plus the head start give
       every threshold's precision; each row's term is summed (action 3).

    The driver handles the cut points, two sums per segment and one number.
    Pre-check: a NULL weight or no positive row raises (in action 2).
    ``weighted``: :func:`_binary_prediction_rows`.
    """
    rows = _binary_prediction_rows(frame, parameters, weighted=weighted).cache()
    try:
        n_segments = min(
            int(frame.sparkSession.conf.get("spark.sql.shuffle.partitions")),
            MAX_SCORE_SEGMENTS,
        )
        cuts = _descending_score_cuts(rows, n_segments)
        segment = F.lit(len(cuts))
        if cuts:
            branches = F.when(F.col("_score") >= F.lit(cuts[0]), F.lit(0))
            for i, cut in enumerate(cuts[1:], start=1):
                branches = branches.when(F.col("_score") >= F.lit(cut), F.lit(i))
            segment = branches.otherwise(F.lit(len(cuts)))
        segmented = rows.withColumn("_seg", segment)

        totals = sorted(
            segmented.groupBy("_seg")
            .agg(F.sum("_pos_w").alias("_pos"), F.sum("_w").alias("_all"),
                 _null_weight_count())
            .collect(),
            key=lambda r: r["_seg"],
        )
        _require_weights_and_a_positive(
            sum(r["_n_null_w"] for r in totals),
            sum(r["_pos"] or 0.0 for r in totals),
            "pooled_average_precision",
        )

        pos_before: list = []
        all_before: list = []
        pos_run = all_run = 0.0
        for r in totals:
            pos_before += [F.lit(r["_seg"]), F.lit(pos_run)]
            all_before += [F.lit(r["_seg"]), F.lit(all_run)]
            pos_run += r["_pos"]
            all_run += r["_all"]

        above = _scored_at_or_above("_seg")
        seg_key = F.col("_seg")
        precision = (
            (F.sum("_pos_w").over(above) + F.create_map(*pos_before)[seg_key])
            / (F.sum("_w").over(above) + F.create_map(*all_before)[seg_key])
        )
        numerator = (
            segmented.select((F.col("_pos_w") * precision).alias("_term"))
            .agg(F.sum("_term").alias("_sum"))
            .collect()[0]["_sum"]
        )
    finally:
        rows.unpersist()
    return float(numerator) / pos_run


def _descending_score_cuts(rows: SparkDataFrame, n_segments: int) -> list:
    """The ``n_segments - 1`` score quantiles, distinct and highest first —
    one action, none for a single segment.

    The relative error only moves how evenly rows spread over the segments,
    never the metric; a quarter of a segment's share keeps them within about
    a quarter of their size.
    """
    if n_segments <= 1:
        return []
    quantiles = rows.approxQuantile(
        "_score", [i / n_segments for i in range(1, n_segments)],
        0.25 / n_segments,
    )
    return sorted(set(quantiles or []), reverse=True)


def compute_macro_per_item_average_precision(
    frame: SparkDataFrame, parameters: dict, *, weighted: bool = True,
) -> float:
    """``macro_per_item_average_precision`` on test: each item's rows ranked
    by score on their own, one average precision per item, then the plain
    mean — the definition ``metrics.compute_macro_per_item_average_precision``
    gives val. One action.

    An item with no positive row is left out of the mean, as on val. Only each
    item's two sums leave the executors, and only the mean reaches the driver.
    Pre-check: a NULL weight or no item with a positive raises.
    ``weighted``: :func:`_binary_prediction_rows`.
    """
    item_col = get_schema(parameters)["item"]
    rows = _binary_prediction_rows(frame, parameters, item_col,
                                   weighted=weighted)
    above = _scored_at_or_above(item_col)
    precision = F.sum("_pos_w").over(above) / F.sum("_w").over(above)
    per_item = (
        rows.withColumn("_term", F.col("_pos_w") * precision)
        .groupBy(item_col)
        .agg(F.sum("_term").alias("_numerator"),
             F.sum("_pos_w").alias("_pos"),
             F.max("_label").alias("_max_label"),
             _null_weight_count())
    )
    has_positive = F.col("_max_label") > 0
    row = per_item.agg(
        F.avg(F.when(has_positive, F.col("_numerator") / F.col("_pos")))
        .alias("_macro"),
        F.sum(F.when(has_positive, 1).otherwise(0)).alias("_n_items"),
        F.sum("_n_null_w").alias("_n_null_w"),
    ).collect()[0]
    _require_weights_and_a_positive(
        row["_n_null_w"], row["_n_items"], "macro_per_item_average_precision")
    return float(row["_macro"])

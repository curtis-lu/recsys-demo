"""The results the main report reads, computed — not written by hand — on one
small table of predictions, by the functions the evaluation nodes call:
``compute_row_facts`` and ``compute_all_metrics`` (``metrics_spark``),
``aggregate_report_diagnostics`` (``steps/report_aggregates``),
``aggregate_score_bins`` and ``build_payload`` (``prediction_quality``), and
the K cut node ``resolve_display_ks`` itself. A section test draws what a run
on these rows would land, so it reads the keys the computation really writes.

The rows (:data:`ROWS`): one snap date, six entities, four items, every
entity scored on every item (a full grid). Entities C0–C2 are in segment
``seg_1``, C3–C5 in ``seg_2`` (a second segment column, ``region``, splits
them otherwise); C3 has no positive (a group the ranking metrics skip). Positives per item: A 3, B 2, C 2, D 1 — so
``evaluation.metric.min_positives`` 2 leaves D out of the item macro, and 3
leaves B, C and D out. Categories: ``cat_X`` = A + B, ``cat_Y`` = C + D
(positives 4 and 2 at the category grain; C2 is all-positive there).

``evaluation.k_values`` is ``[1, 2, 3, "all"]``: "all" is 4 at the item grain
and 2 at the category grain — 5 at the item grain with ``event`` declared,
where C0 holds a second row of A (:data:`EXTRA_EVENT_ROW`), so "all" is not
the item count.
"""
from __future__ import annotations

import copy

from recsys_tfb.core.consistency import (
    DATASET_TEST_RATIO_KEY,
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
)

ITEMS = ("A", "B", "C", "D")

#: (entity, segment, {item: (score, label)}).
ROWS = (
    ("C0", "seg_1", {"A": (0.9, 1), "B": (0.5, 0), "C": (0.3, 0), "D": (0.1, 0)}),
    ("C1", "seg_1", {"A": (0.2, 0), "B": (0.8, 1), "C": (0.6, 0), "D": (0.4, 0)}),
    ("C2", "seg_1", {"A": (0.7, 1), "B": (0.3, 0), "C": (0.5, 1), "D": (0.2, 0)}),
    ("C3", "seg_2", {"A": (0.6, 0), "B": (0.4, 0), "C": (0.3, 0), "D": (0.2, 0)}),
    ("C4", "seg_2", {"A": (0.5, 1), "B": (0.9, 1), "C": (0.1, 0), "D": (0.3, 0)}),
    ("C5", "seg_2", {"A": (0.8, 0), "B": (0.2, 0), "C": (0.6, 1), "D": (0.4, 1)}),
)

SEGMENT_COL = "segment"

#: A second segment column, for the label of a bundle sliced by more than
#: one (``per_segment`` slices by the first).
REGION_COL = "region"
REGION = {"C0": "north", "C1": "south", "C2": "north", "C3": "south",
          "C4": "north", "C5": "south"}

#: The entity whose segment is NULL when ``unmatched`` (the
#: ``segment_keys.UNMATCHED_SEGMENT`` group).
UNMATCHED_ENTITY = "C1"
CATEGORIES = {"cat_X": ["A", "B"], "cat_Y": ["C", "D"]}

#: With ``event`` declared: a second impression of A for C0, scored last.
EXTRA_EVENT_ROW = ("C0", "A", 0.05, 0)
EVENT_COL = "imp_id"

#: With ``all_positive``: one more entity whose every row is a positive (an
#: all-positive query group at the item grain, #376).
ALL_POSITIVE_ENTITY = ("C6", "seg_2", {item: (0.1 * (i + 1), 1)
                                       for i, item in enumerate(ITEMS)})
REGION_OF_C6 = "north"

#: The kept share of the zero-positive test groups in the weighted case
#: (``--post-training``): C3's rows weigh 1 / 0.3, so the weighted counts are
#: not whole numbers.
KEPT_RATIO = 0.3


def params(*, min_positives: int = 0, segments: tuple = (SEGMENT_COL,),
           category: bool = True, top_n: int = 3, drop: bool | None = None,
           post_training: bool | None = None, ratio: float | None = None,
           unmatched: bool = False, filtered: bool = False,
           event: bool = False, all_positive: bool = False) -> dict:
    """The settings the results are computed and drawn under. ``unmatched``,
    ``filtered`` and ``all_positive`` change the rows, not a setting
    (:func:`_frame`); ``event`` declares the event role and adds a row."""
    p = {
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name", "label": "label",
                        "score": "score", "rank": "rank"},
            "categorical_values": {"prod_name": list(ITEMS)},
        },
        "evaluation": {
            "k_values": [1, 2, 3, "all"],
            "segment_columns": list(segments),
            "metric": {"min_positives": min_positives},
            "item_categories": {"enabled": category, "unmapped": "singleton",
                                "mapping": copy.deepcopy(CATEGORIES)},
            "prediction_quality": {"n_bins": 20, "n_display_bins": 5,
                                   "top_n": top_n},
            "report": {"display": {"compare_k": [1, 3, "all"]}},
        },
    }
    if event:
        p["schema"]["columns"]["event"] = EVENT_COL
    if drop is not None:
        p["evaluation"]["query_filter"] = {"drop_all_positive_groups": drop}
    if post_training is not None:
        p["post_training"] = post_training
    if ratio is not None:
        p[DATASET_TEST_RATIO_KEY] = ratio
    return p


def _frame(spark, *, weighted: bool, unmatched: bool, filtered: bool,
           event: bool, all_positive: bool):
    """The predictions, ranked within each entity by score (ties by item, as
    ``rank_by_score_then_item``): with the weight column when ``weighted``;
    :data:`UNMATCHED_ENTITY`'s segment NULL when ``unmatched``; without the
    group holding no positive when ``filtered`` — the test table of
    ``--post-training`` with ``dataset.test_zero_positive_group_ratio`` 0
    (``filter_test_keys``); with :data:`EXTRA_EVENT_ROW` and an event column
    when ``event``; with :data:`ALL_POSITIVE_ENTITY` when ``all_positive``."""
    records = []
    rows = ROWS + ((ALL_POSITIVE_ENTITY,) if all_positive else ())
    region = {**REGION, ALL_POSITIVE_ENTITY[0]: REGION_OF_C6}
    for entity, segment, cells in rows:
        has_positive = any(label for _, label in cells.values())
        if filtered and not has_positive:
            continue
        if unmatched and entity == UNMATCHED_ENTITY:
            segment = None
        weight = 1.0 if has_positive else 1 / KEPT_RATIO
        cells = [(item, score, label, "i1")
                 for item, (score, label) in cells.items()]
        if event and entity == EXTRA_EVENT_ROW[0]:
            cells.append((*EXTRA_EVENT_ROW[1:], "i2"))
        ranked = sorted(cells, key=lambda c: (-c[1], c[0], c[3]))
        for rank, (item, score, label, imp) in enumerate(ranked, 1):
            row = ["20240331", entity, item, score, label, segment,
                   region[entity], rank]
            if event:
                row.append(imp)
            if weighted:
                row.append(weight)
            records.append(tuple(row))
    columns = ["snap_date", "cust_id", "prod_name", "score", "label",
               SEGMENT_COL, REGION_COL, "rank"]
    if event:
        columns.append(EVENT_COL)
    if weighted:
        columns.append(ZERO_POSITIVE_GROUP_WEIGHT_COL)
    return spark.createDataFrame(records, schema=columns)


def _stamped(result: dict, parameters: dict) -> dict:
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        fingerprint,
    )

    return {**result, "config_fingerprint": fingerprint(
        parameters, "compute_metrics")}


_CACHE: dict = {}


def results(spark, **settings) -> tuple[dict, dict]:
    """``(results by catalog name, parameters)`` for :func:`params`
    ``settings``: what ``generate_report`` hands the report, the baseline
    off. Computed once per setting and handed out as a deep copy, so a test
    may change what it gets."""
    key = tuple(sorted(settings.items()))
    if key not in _CACHE:
        rows = {name: settings.pop(name)
                for name in ("unmatched", "filtered", "all_positive")
                if name in settings}
        _CACHE[key] = _compute(spark, params(**settings), **rows)
    return copy.deepcopy(_CACHE[key])


def _compute(spark, p: dict, *, unmatched: bool = False,
             filtered: bool = False,
             all_positive: bool = False) -> tuple[dict, dict]:
    from recsys_tfb.evaluation import metrics_spark as ms
    from recsys_tfb.pipelines.evaluation.steps.report_aggregates import (
        aggregate_report_diagnostics,
    )
    from recsys_tfb.evaluation.prediction_quality import (
        aggregate_score_bins,
        build_payload,
        item_top_k_metrics,
    )
    from recsys_tfb.evaluation.result_readers import drop_all_positive_groups
    from recsys_tfb.pipelines.evaluation.nodes import resolve_display_ks

    weighted = bool(p.get("post_training") and p.get(DATASET_TEST_RATIO_KEY))
    frame = _frame(spark, weighted=weighted, unmatched=unmatched,
                   filtered=filtered, all_positive=all_positive,
                   event="event" in p["schema"]["columns"])
    segments = p["evaluation"]["segment_columns"]
    with_category = p["evaluation"]["item_categories"]["enabled"]
    kw = {"segment_columns": segments, "with_category": with_category,
          "category_mapping": ms.hand_category_mapping(p)}
    facts = ms.compute_row_facts(frame, p, **kw)
    metrics = ms.compute_all_metrics(
        frame, p, facts, drop_all_positive_groups=drop_all_positive_groups(p),
        **kw)
    aggregates = {**aggregate_report_diagnostics(
        frame, item_col="prod_name", score_col="score", rank_col="rank",
        label_col="label"), "enabled": True}
    pq_cfg = p["evaluation"]["prediction_quality"]
    weight_col = ZERO_POSITIVE_GROUP_WEIGHT_COL if weighted else None
    binned = aggregate_score_bins(
        frame, item_col="prod_name", score_col="score", label_col="label",
        n_bins=pq_cfg["n_bins"], top_n=pq_cfg["top_n"], weight_col=weight_col)
    rows = frame.select("prod_name", "score", "label",
                        *([weight_col] if weight_col else []))
    quality = {**build_payload(
        binned, item_col="prod_name", score_col="score", label_col="label",
        weight_col=weight_col, n_bins=pq_cfg["n_bins"],
        n_display_bins=pq_cfg["n_display_bins"], top_n=pq_cfg["top_n"]),
        # What compute_prediction_quality adds: the one group without a
        # positive (C3, unless ``filtered``) is the kept one; the raw-score
        # APs and every item's precision@K over every group (#577).
        "population": _population(p, facts), "scope_definition": None,
        "average_precision": {
            "overall": ms.compute_pooled_average_precision(
                rows, p, weighted=weighted),
            "macro": ms.compute_macro_per_item_average_precision(
                rows, p, weighted=weighted)},
        "per_item_top_k": item_top_k_metrics(
            frame, item_col="prod_name", label_col="label", rank_col="rank",
            k_values=ms.resolve_item_k_grid(frame, p),
            weight_col=weight_col)}
    display_ks = resolve_display_ks(_stamped(metrics, p), p)
    return {
        "evaluation_row_facts": facts,
        "evaluation_metrics": metrics,
        "evaluation_report_aggregates": aggregates,
        "prediction_quality_metrics": quality,
        "evaluation_display_ks": display_ks,
    }, p


def _population(p: dict, facts: dict) -> dict:
    # The settings name no date: the page's metadata cell reads none.
    dates = []
    if not p.get("post_training"):
        return {"source": "monitoring", "evaluated_dates": dates,
                "test_zero_positive_group_ratio": None,
                "n_zero_positive_groups": None, "scope": None}
    return {"source": "post_training", "evaluated_dates": dates,
            "test_zero_positive_group_ratio": p.get(DATASET_TEST_RATIO_KEY),
            "n_zero_positive_groups": facts["n_excluded_queries"],
            "scope": None}


def empty_quality(spark) -> dict:
    """The prediction-quality payload of an evaluation with no candidate row:
    ``aggregate_score_bins`` finds no score range, and ``build_payload``
    lands no summary."""
    from recsys_tfb.evaluation.prediction_quality import (
        aggregate_score_bins,
        build_payload,
    )

    frame = spark.createDataFrame(
        [], schema="prod_name string, score double, label int")
    binned = aggregate_score_bins(
        frame, item_col="prod_name", score_col="score", label_col="label",
        n_bins=20, top_n=3)
    return {**build_payload(
        binned, item_col="prod_name", score_col="score", label_col="label",
        weight_col=None, n_bins=20, n_display_bins=5, top_n=3),
        "population": {"source": "monitoring", "evaluated_dates": [],
                       "test_zero_positive_group_ratio": None,
                       "n_zero_positive_groups": None, "scope": None},
        "scope_definition": None}

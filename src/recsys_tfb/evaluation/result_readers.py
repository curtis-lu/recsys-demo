"""What a report may import to read the landed evaluation results (ADR-0031
principle 1): the keys the results land under, the readers of the settings a
report draws by, the readers of a metrics bundle's ledger, and the JSON form
of a small frame. Nothing here computes a result.

The report modules (``pipelines/evaluation/report/``) read through this
module instead of importing the modules that compute the results
(``metrics``, ``metrics_spark``, ``baselines``, ``prediction_quality``,
``steps/report_aggregates``). Those modules do not re-export these names:
every caller imports them from here.

Imports nothing from the project and not pyspark, so it cannot close an
import cycle and loading it costs a report nothing. ``pandas`` is imported
inside :func:`frame_from_json`, the one function that builds a frame:
``metrics`` imports this module and calls itself a numpy leaf.
"""

from __future__ import annotations

import math


# ---------------------------------------------------------------------------
# The row facts (``metrics_spark.compute_row_facts``, ``row_facts.json``)
# ---------------------------------------------------------------------------

#: Top-level key of the row facts (``metrics_spark.compute_row_facts``,
#: ``row_facts.json``) and of their ``category`` grain: how many of the query
#: groups holding a positive are *all-positive* — every row carries the same
#: positive label (the rule is ``metrics_spark._all_positive_group``), so
#: every per-query ranking metric (``map@K`` / ``precision@K`` /
#: ``recall@K``) comes out the same whatever the order. The per-item metrics
#: (``map_attr@K`` / ``recall@K`` / ``mean_pos``) do not: with K below the row
#: count, which rows reach the top K still decides which items get the
#: credit. Always written, whether or not
#: ``evaluation.query_filter.drop_all_positive_groups`` drops them, because the
#: report reads only the written facts. ``n_excluded_queries`` keeps counting
#: only the zero-positive groups.
ALL_POSITIVE_KEY = "n_all_positive_queries"

#: Row facts key (``metrics_spark.compute_row_facts``): the row count of the
#: widest query group, what ``evaluation.k_values: "all"`` resolves to on a
#: frame of event rows.
WIDEST_GROUP_KEY = "widest_group_rows"

#: Row facts key, at each grain: the query groups holding a positive,
#: ``n_queries - n_excluded_queries`` (``metrics.n_groups_with_positive``).
#: ``None`` only when either count is missing.
N_WITH_POSITIVE_KEY = "n_groups_with_positive"

#: Row facts key, at each grain: the all-positive groups' share of the
#: groups holding a positive (``metrics.all_positive_share``); ``None`` when
#: no group holds a positive.
ALL_POSITIVE_SHARE_KEY = "all_positive_share"

#: Row facts key, at each grain: whether that share warns
#: (``metrics.all_positive_share_warns``) — only with
#: ``evaluation.query_filter.drop_all_positive_groups`` off, and only strictly
#: above :data:`ALL_POSITIVE_WARN_SHARE`. The one fact of the row facts that
#: reads a setting; ``evaluation.query_filter`` is a setting
#: ``compute_row_facts`` applies, so a changed switch re-counts the facts.
ALL_POSITIVE_WARNS_KEY = "all_positive_warns"

#: With the switch off, the log and the report warn when the all-positive
#: groups are more than this share of the groups holding a positive.
#: A constant, not a setting. Here because the report prints it beside the
#: landed warning, and its share format reads it (:func:`format_share`).
ALL_POSITIVE_WARN_SHARE = 0.10


def format_share(share: float) -> str:
    """An all-positive share as the log and the report print it: ``5.3%``.

    One decimal, unless the share is above :data:`ALL_POSITIVE_WARN_SHARE`
    and one decimal rounds it down to the threshold — 1004 of 10000 printed
    as ``10.0%`` beside "above 10%". Then more decimals, at most four, until
    the printed value is above the threshold too (``10.04%``). One function,
    so the log and the report cannot print the same share two ways.
    Here, not in the report's modules: the evaluation node's log and the
    report both print it, and neither side may import the other.
    """
    for digits in range(1, 5):
        text = f"{share:.{digits}%}"
        if (not share > ALL_POSITIVE_WARN_SHARE
                or float(text[:-1]) / 100 > ALL_POSITIVE_WARN_SHARE):
            break
    return text


#: The node an outdated result (one landed without the row facts or the
#: ledger) is re-computed from, named in every message that refuses such a
#: result: the first node of the main line whose slice re-lands the row facts,
#: the metrics and the baseline. One name, so a rename edits one line.
LEDGER_RERUN_NODE = "compute_row_facts"


# ---------------------------------------------------------------------------
# A metrics bundle (``metrics_spark.compute_all_metrics``)
# ---------------------------------------------------------------------------

#: Top-level key of a ``metrics_spark.compute_all_metrics`` bundle, at each
#: grain: the population ledger (ADR-0031 decision 4), what this bundle's
#: numbers were computed on, so two bundles joined by metric name can be told
#: comparable or not. Holds :data:`ALL_K_KEY`; :data:`MACRO_ITEMS_KEY`, the
#: items the per-item macro averaged over;
#: :data:`N_SCORED_WITH_POSITIVE_KEY` / :data:`N_SCORED_WITHOUT_POSITIVE_KEY`,
#: the query groups the ranking metrics scored; and
#: :data:`SEGMENT_COLUMNS_KEY`, the columns it sliced by. The facts of the
#: rows themselves are the row facts', not here
#: (``metrics_spark.compute_row_facts``).
POPULATION_KEY = "population"

#: Population ledger key: the K ``evaluation.k_values: "all"`` resolved to
#: (``resolved_all_k``).
ALL_K_KEY = "all_k"

#: Population ledger key: the items ``macro_avg.by_item`` averaged over,
#: sorted — every item of ``per_item`` with ``n_pos`` at least
#: ``evaluation.metric.min_positives`` (``metrics_spark._compute_core``). The
#: reports count it (:func:`macro_item_count`) instead of applying that rule
#: a second time.
MACRO_ITEMS_KEY = "macro_items"

#: Population ledger keys: the query groups the ranking metrics scored —
#: those holding a positive, less the all-positive ones when
#: ``evaluation.query_filter.drop_all_positive_groups`` drops them — and the
#: ones without a positive, 0 by construction (AP is undefined there).
N_SCORED_WITH_POSITIVE_KEY = "n_scored_queries_with_positive"
N_SCORED_WITHOUT_POSITIVE_KEY = "n_scored_queries_without_positive"

#: Population ledger key: the segment columns the bundle was computed with,
#: the ``segment_columns`` ``compute_all_metrics`` was handed — its
#: ``per_segment`` slices by the first. The comparison layer reads it to tell
#: two sides computed with different segment columns (ADR-0031 decision 4's
#: fourth case). The compared side of ``--compare`` borrows this run's side's
#: (``compute_compare_metrics``).
SEGMENT_COLUMNS_KEY = "segment_columns"


# ---------------------------------------------------------------------------
# The popularity baseline (``compute_baseline_metrics``)
# ---------------------------------------------------------------------------

#: Top-level key of ``baseline_metrics.json``: the lookback window the
#: popularity was counted over, as the report prints it, in every mode
#: (count or rate, one evaluated date or several) —
#: ``baselines.popularity_window``. Holds ``lookback_months`` and ``score``
#: (the settings it was counted with), ``months_covered`` (the calendar
#: months the printed monthly table holds), ``window_months_covered``
#: (``{evaluated date: months with rows in its window}``),
#: ``full_window_months``, ``per_month_divisor`` and ``window_partial``.
POPULARITY_WINDOW_KEY = "popularity_window"


# ---------------------------------------------------------------------------
# A comparison (``pipelines/evaluation/steps/comparison_layer``)
# ---------------------------------------------------------------------------
# The full shape is in that module's docstring. In short: per grain, a block
# per metrics block (:data:`COMPARED_BLOCKS`), each ``{ENTRIES: {THIS, REFERENCE},
# CELLS: ...}``; a cell is ``{THIS, REFERENCE, DIFF}``; and the four
# annotations (:data:`COMPARED_ANNOTATIONS`) under :data:`POPULATION_KEY`,
# each ``{THIS, REFERENCE, SAME}``.

#: The two sides of a comparison and their difference, ``THIS − REFERENCE``
#: (ADR-0031 decision 16 item 8: 這次、對照、差).
THIS = "this"
REFERENCE = "reference"
DIFF = "diff"

#: Annotation key: whether the two sides' values are equal.
SAME = "same"

#: Block keys: how many entries each side's block held (``None`` where that
#: side had no such block), and the joined cells.
ENTRIES = "entries"
CELLS = "cells"

#: The blocks of each grain, named after where they sit in a metrics bundle
#: (``macro_by_item`` is ``macro_avg.by_item``).
COMPARED_BLOCKS = ("overall", "per_item", "macro_by_item", "per_segment")

#: The K label of the cell pairing each side's ``"all"`` (``name@all``): the
#: two sides' ``"all"`` can resolve to different K (:data:`ALL_K_KEY`), so
#: their keys never pair up by name.
ALL_LABEL = "all"

#: Annotation key: the query groups each side scored,
#: ``{N_SCORED_WITH_POSITIVE_KEY, N_SCORED_WITHOUT_POSITIVE_KEY}``.
SCORED_QUERIES_KEY = "scored_queries"

#: The four things two results must share to be compared as they are
#: (ADR-0031 decision 4 and decision 15): what ``"all"`` resolved to, the
#: items in the macro, the query groups scored, the segment columns.
COMPARED_ANNOTATIONS = (ALL_K_KEY, MACRO_ITEMS_KEY, SCORED_QUERIES_KEY,
                        SEGMENT_COLUMNS_KEY)


# ---------------------------------------------------------------------------
# The K each table shows (``pipelines/evaluation/steps/display_ks``)
# ---------------------------------------------------------------------------
# One block per result the tables are drawn from, keyed by its catalog name
# (``evaluation_metrics``, ``baseline_comparison``); the comparison report's
# own cut (``comparison_display_ks``) is one such block for the item grain,
# with the category grain's under ``category``. See that module.

#: Block key: the K ``"all"`` stands for in the result the block cuts for.
#: The lists below never hold an int K above it (bug 8, ADR-0020).
DISPLAY_ALL_K = ALL_K_KEY

#: Block key: ``evaluation.report.display.compare_k``, cut — in the main
#: report's blocks and at each grain of the comparison report's cut (there
#: as ascending integers, ``"all"`` written as :data:`DISPLAY_ALL_K`).
DISPLAY_COMPARE_K = "compare_k"

#: Block key: the K columns of every metrics-section table, cut.
DISPLAY_METRICS_SECTION = "metrics_section"


# ---------------------------------------------------------------------------
# The prediction-quality payload (``prediction_quality.build_payload``)
# ---------------------------------------------------------------------------

#: Key under ``overall`` and ``per_item`` of ``prediction_quality.json``: the
#: report's display bin table (``prediction_quality.coarse_bin_table``), as
#: :func:`frame_to_json` long frames; under ``per_item`` one table for every
#: listed item, the item column first.
DISPLAY_BINS_KEY = "display_bins"

#: Key under ``overall`` of ``prediction_quality.json``: the threshold sweep
#: the report draws (``prediction_quality.threshold_sweep``, one row per
#: non-empty fine bin), as a :func:`frame_to_json` long frame.
THRESHOLD_SWEEP_KEY = "threshold_sweep"

#: Key of ``prediction_quality.json`` beside ``build_payload``'s: which rows
#: the numbers are on (ADR-0032 decisions 3-5), the page's opening lines.
#: A dict of :data:`POPULATION_SOURCE_KEY`, :data:`POPULATION_DATES_KEY`,
#: :data:`POPULATION_TEST_RATIO_KEY` and
#: :data:`POPULATION_ZERO_POSITIVE_GROUPS_KEY`.
PREDICTION_QUALITY_POPULATION_KEY = "population"

#: Under :data:`PREDICTION_QUALITY_POPULATION_KEY`: the run mode the rows
#: were evaluated in, :data:`POST_TRAINING_POPULATION` or
#: :data:`MONITORING_POPULATION` — also the output directory's segment
#: (``prediction_quality_population``, set by the CLI).
POPULATION_SOURCE_KEY = "source"
POST_TRAINING_POPULATION = "post_training"
MONITORING_POPULATION = "monitoring"

#: Under :data:`PREDICTION_QUALITY_POPULATION_KEY`: the evaluated dates, in
#: configured order.
POPULATION_DATES_KEY = "evaluated_dates"

#: Under :data:`PREDICTION_QUALITY_POPULATION_KEY`: the evaluated model's
#: test ratio r under ``--post-training``, ``None`` in monitoring mode.
POPULATION_TEST_RATIO_KEY = "test_zero_positive_group_ratio"

#: Under :data:`PREDICTION_QUALITY_POPULATION_KEY`: how many query groups
#: holding no positive the evaluated rows hold under ``--post-training``
#: (the ones the dataset kept), ``None`` in monitoring mode.
POPULATION_ZERO_POSITIVE_GROUPS_KEY = "n_zero_positive_groups"


# ---------------------------------------------------------------------------
# Settings a report draws by
# ---------------------------------------------------------------------------


def metric_params(parameters: dict) -> dict:
    """Read ``evaluation.metric`` → ``{"weight_alpha", "min_positives", "shrinkage_k"}``.

    The one reader of that block that the metrics / report code uses
    (ADR-0020 design H): the Spark main metrics and the report call this
    instead of keeping their own copy. A15
    (``core.consistency.evaluation_metric_param_errors``) reads the same block
    separately, only to validate value domains; it resolves no fallbacks.

    Fallback: a missing ``evaluation`` / ``metric`` block, a missing key, or
    an explicit ``None`` value all resolve to ``weight_alpha=0.0``,
    ``min_positives=0``, ``shrinkage_k=0.0`` — the plain equal-weight macro.

    No truncation depth: the macro is read untruncated, at "all" (A63 refuses
    the old key). Any other key under the block is dropped here.
    """
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    return {
        "weight_alpha": float(m.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(m.get("min_positives", 0) or 0),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0) or 0.0),
    }


def drop_all_positive_groups(parameters: dict) -> bool:
    """Read ``evaluation.query_filter.drop_all_positive_groups``; off unless ``True``.

    The one reader of the switch. A missing block, a missing key or an
    explicit ``None`` all mean off. Not under ``evaluation.metric``:
    :func:`metric_params` returns three fixed keys and drops any other, so a
    switch written there would silently do nothing. The value domain (a bool)
    is validated by A49 in ``core/consistency.py``, not here.
    """
    qf = ((parameters.get("evaluation", {}) or {}).get("query_filter", {}) or {})
    return bool(qf.get("drop_all_positive_groups", False) or False)


# ---------------------------------------------------------------------------
# A metrics bundle's ledger
# ---------------------------------------------------------------------------


def resolved_all_k(bundle: dict) -> int:
    """The K a metrics bundle's ``"all"`` values are stored at (``map@{K}``).

    The one reader of that number. ``"all"`` has one producer
    (``metrics_spark._resolve_all_k``) and several readers. A reader counting
    the items itself misses with ``event`` declared: the producer's K is the
    widest query group, not the item count, so every lookup misses and
    nothing raises (a blank ``map@all`` cell).

    Read off the bundle's own population ledger (:data:`POPULATION_KEY`).
    The comparison report reads each side at its own; the main report reads
    the baseline at the model's, which is the same K by construction — the
    baseline re-scores the model's rows and counts off their row facts. The
    keys stay the resolved number, not ``map@all`` (ADR-0031 decision 15):
    two sides whose ``"all"`` resolved to different K then never pair up under
    one name.

    Raises ``KeyError`` on a bundle with no ledger: the report refuses such a
    file first (``pipelines/evaluation/report/sections/shared.refuse_pre_ledger``),
    with the node to re-run.
    """
    return int(bundle[POPULATION_KEY][ALL_K_KEY])


def macro_item_count(bundle: dict) -> int:
    """How many items a metrics bundle's per-item macro averaged over: the
    length of its ledger's :data:`MACRO_ITEMS_KEY`.

    bug 5 (ADR-0020): an item with no positive this period has no
    ``per_item`` cell, and ``evaluation.metric.min_positives`` drops more, so
    the macro's denominator moves month to month; the reports disclose it.
    Counted off the list the computation landed, not by applying
    ``min_positives`` to ``per_item`` again. Raises ``KeyError`` on a bundle
    with no ledger, like :func:`resolved_all_k`.
    """
    return len(bundle[POPULATION_KEY][MACRO_ITEMS_KEY])


# ---------------------------------------------------------------------------
# A small frame as landed JSON
# ---------------------------------------------------------------------------

#: The two shapes :func:`frame_to_json` lands.
FRAME_LONG = "long"
FRAME_MATRIX = "matrix"


def _no_nan(value):
    """NaN → None。理由見 :func:`frame_to_json` 的 docstring。"""
    return None if isinstance(value, float) and math.isnan(value) else value


def frame_to_json(df, kind: str) -> dict:
    """把聚合出來的小 frame 轉成可以落地的 dict。

    ``kind`` 明寫而不是從 index 型別推斷：長格式的 index 是無意義的
    ``RangeIndex``（丟掉），矩陣的 index 是 item 名稱（丟了 heatmap 就沒有
    y 軸標籤）。推斷會在「item 剛好是 0, 1, 2」時猜錯，而那不會有任何測試轉紅。

    ``to_dict("split")`` **已經**把 numpy 純量轉成 Python 原生型別（實測
    ``int64`` → ``int``、object index → ``str``），所以這裡不逐格轉型。但它
    **不處理 NaN**：``JSONDataset`` 用預設 ``allow_nan=True``，NaN 會寫成
    ``NaN`` 這個非合法 JSON 的字面值，Python 讀得回來、別的工具不行——而這些
    檔案的用途正是被拷到別的環境去讀。所以在這裡換成 ``None``。
    """
    if kind not in (FRAME_LONG, FRAME_MATRIX):
        raise ValueError(
            f"kind must be {FRAME_LONG!r} or {FRAME_MATRIX!r}, got {kind!r}"
        )
    split = df.to_dict("split")
    out = {
        "kind": kind,
        "columns": list(split["columns"]),
        "data": [[_no_nan(v) for v in row] for row in split["data"]],
    }
    if kind == FRAME_MATRIX:
        out["index"] = list(split["index"])
    return out


def frame_from_json(payload: dict):
    """:func:`frame_to_json` 的反向。回傳的 frame 直接餵給繪圖函式。"""
    import pandas as pd

    df = pd.DataFrame(payload["data"], columns=payload["columns"])
    if payload.get("kind") == FRAME_MATRIX:
        df.index = payload["index"]
    return df

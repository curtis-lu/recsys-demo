"""Every metric HPO can target or training can score on test, in one table
(ADR-0028 decision 2).

One row per metric: its name, its family, how val scores it (the numpy
function HPO calls on the driver) and which Spark pass scores it on test. The
readers are HPO's choices and A25's domain (``core/consistency.py``), the
trial scorer (``pipelines/training/steps/hpo_scoring.py``), the test scoring
node (``compute_test_metrics``) and the ``test_metrics`` checks. Before this
table the names lived in a tuple in ``core/consistency.py`` and the dispatch
in a hand-written ``if`` chain in ``hpo_scoring.py``: adding an objective to
the tuple and forgetting the chain was only found at run time. Adding a metric
is now adding a row.

**Two families.**

* ``RANKING`` — ranks inside each query group; a group holding no positive
  cannot rank well or badly and is skipped. ``mean_ap`` gives each query group
  one vote, ``macro_per_item_map`` each item.
* ``BINARY_PREDICTION`` — every candidate row is one yes/no prediction, query
  groups ignored; groups holding no positive count, weighted by
  ``zero_positive_group_weight``.

**What a test pass is.** Metrics that come out of the same Spark computation
share it: asking for a second one costs nearly nothing, so the cost is
counted per pass, not per metric. Both ranking metrics share
:data:`RANKING_PASS` (``metrics_spark.compute_untruncated_ap``). The
binary-prediction metrics have no test-side pass yet (#452 lands the ranking
side first): a row with ``test_pass=None`` is reported as not computed, with
:data:`NO_TEST_ALGORITHM` as the reason, never as a number.

**The same definition twice.** val scores through numpy on the driver (the
predictions are born there), test through Spark (they are already in Hive).
Each ranking row's two halves agree by construction of the formulas — AP with
no truncation, ties by item — and ``tests/test_pipelines/test_training/
test_compute_test_metrics.py`` checks the test side against the val side's
function.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

import numpy as np

from recsys_tfb.evaluation.metrics import (
    compute_macro_per_item_average_precision,
    compute_macro_per_item_map,
    compute_mean_ap,
    compute_pooled_average_precision,
)

if TYPE_CHECKING:
    from recsys_tfb.evaluation.metrics_spark import UntruncatedAp

RANKING = "ranking"
BINARY_PREDICTION = "binary_prediction"

#: The one Spark pass both ranking metrics come out of.
RANKING_PASS = "ranking"

#: Why a requested metric has no value on test, for a row with no test pass.
NO_TEST_ALGORITHM = "no test-side algorithm implemented yet"

#: ``training.hpo_objective`` when the key is absent: ``tune_hyperparameters``'
#: own default. The conf ships ``macro_per_item_map``; the two differ on
#: purpose (ADR-0028 decision 1 — changing this default would not change
#: ``search_id``, so a resumed search would mix two scores).
DEFAULT_HPO_OBJECTIVE = "mean_ap"

ValScorer = Callable[..., float]


class Metric(NamedTuple):
    """One row of the table.

    ``val_score(groups, items, y_true, y_score, event_keys, weights)`` scores
    val predictions: the ranking rows read ``groups`` / ``items`` /
    ``event_keys`` (ties by item, then event) and never ``weights``; the
    binary-prediction rows read ``items`` / ``weights`` and never ``groups``.
    ``test_pass`` names the Spark pass (:func:`run_test_pass`) that scores it
    on test, ``None`` when there is none yet; ``test_value`` reads the metric
    off that pass's result.
    """

    name: str
    family: str
    val_score: ValScorer
    test_pass: Optional[str]
    test_value: Optional[Callable[[Any], float]]


def _macro_over_items(result: UntruncatedAp) -> float:
    """``macro_per_item_map`` on test: the plain mean of the per-item
    attributions, in item order, as ``compute_macro_per_item_map``'s
    ``np.unique`` walks them; 0.0 with no positive row, as it returns."""
    attr = result.per_item_map_attr
    if not attr:
        return 0.0
    return float(np.mean([attr[item] for item in sorted(attr)]))


def _rows() -> dict[str, Metric]:
    rows = [
        Metric(
            "mean_ap", RANKING,
            lambda g, i, y, s, e, w: compute_mean_ap(g, i, y, s, e),
            RANKING_PASS, lambda r: r.overall_map,
        ),
        Metric(
            "macro_per_item_map", RANKING,
            lambda g, i, y, s, e, w: compute_macro_per_item_map(
                g, i, y, s, event_keys=e),
            RANKING_PASS, _macro_over_items,
        ),
        Metric(
            "pooled_average_precision", BINARY_PREDICTION,
            lambda g, i, y, s, e, w: compute_pooled_average_precision(y, s, w),
            None, None,
        ),
        Metric(
            "macro_per_item_average_precision", BINARY_PREDICTION,
            lambda g, i, y, s, e, w: compute_macro_per_item_average_precision(
                i, y, s, w),
            None, None,
        ),
    ]
    return {row.name: row for row in rows}


METRICS: dict[str, Metric] = _rows()

#: Every registered name, in table order: HPO's choices and A25's domain.
METRIC_NAMES: tuple[str, ...] = tuple(METRICS)

#: The two ranking metrics, always scored on test (ADR-0028 decision 1).
RANKING_METRICS: tuple[str, ...] = tuple(
    n for n, m in METRICS.items() if m.family == RANKING)

#: The binary-prediction metrics: the ones that need val (A48) and test to
#: keep the query groups holding no positive.
BINARY_PREDICTION_METRICS: tuple[str, ...] = tuple(
    n for n, m in METRICS.items() if m.family == BINARY_PREDICTION)

def run_test_pass(name: str, frame, parameters: Mapping):
    """Run the Spark pass ``name`` (a ``Metric.test_pass``) on the scored
    months' rows and return its result.

    ``metrics_spark`` is imported here, not at the top: this table is read at
    CLI entry (A25 / A48 / A53 in ``core/consistency.py``), where neither
    pyspark nor the Spark metrics are otherwise needed, and ``metrics_spark``
    imports ``core/consistency`` itself.
    """
    from recsys_tfb.evaluation.metrics_spark import compute_untruncated_ap

    passes = {RANKING_PASS: compute_untruncated_ap}
    if name not in passes:
        raise ValueError(f"no test pass named {name!r}; known: {sorted(passes)}")
    return passes[name](frame, parameters)


def _unknown(name: str, setting: str) -> ValueError:
    return ValueError(
        f"unknown {setting} {name!r}; allowed: {', '.join(METRIC_NAMES)}"
    )


def score_val(
    name: str,
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    event_keys: Sequence[np.ndarray] = (),
    weights: Optional[np.ndarray] = None,
) -> float:
    """Score val predictions under metric ``name`` — what one HPO trial is
    judged by. An unknown ``name`` raises ``ValueError``: A25 rejects it at
    CLI entry, so reaching this means ``parameters`` skipped that gate."""
    if name not in METRICS:
        raise _unknown(name, "training.hpo_objective")
    return METRICS[name].val_score(
        groups, items, y_true, y_score, event_keys, weights)


def effective_hpo_objective(parameters: Mapping) -> str:
    """``training.hpo_objective`` as ``tune_hyperparameters`` reads it: the
    configured value, or :data:`DEFAULT_HPO_OBJECTIVE` when the key is
    absent."""
    training = parameters.get("training") or {}
    return training.get("hpo_objective", DEFAULT_HPO_OBJECTIVE)


def selection_metric(parameters: Mapping) -> str:
    """The metric promote ranks model versions by: ``test_metrics.
    selection_metric``, or the effective HPO objective when it is not set
    (ADR-0028 decision 3), so the training goal is written once."""
    configured = (parameters.get("test_metrics") or {}).get("selection_metric")
    return configured if configured is not None else effective_hpo_objective(
        parameters)


def requested_test_metrics(parameters: Mapping) -> list[str]:
    """What training scores on test: the ranking metrics, the selection
    metric, the HPO objective and ``test_metrics.metrics``, each once, in
    that order (ADR-0028 decision 1)."""
    extra = (parameters.get("test_metrics") or {}).get("metrics") or []
    wanted: list[str] = []
    for name in [*RANKING_METRICS, selection_metric(parameters),
                 effective_hpo_objective(parameters), *extra]:
        if name not in wanted:
            wanted.append(name)
    return wanted


def passes_for(names: Sequence[str]) -> list[str]:
    """The test passes ``names`` need, each once, in first-needed order;
    names with no test pass need none."""
    needed: list[str] = []
    for name in names:
        if name not in METRICS:
            raise _unknown(name, "test metric")
        test_pass = METRICS[name].test_pass
        if test_pass is not None and test_pass not in needed:
            needed.append(test_pass)
    return needed


def read_test_values(
    names: Sequence[str], pass_results: Mapping[str, Any],
) -> tuple[dict[str, float], dict[str, str]]:
    """``(values, not_computed)`` for ``names``: each metric read off its
    pass's result, or the reason it has no value (``NO_TEST_ALGORITHM``).

    ``pass_results`` holds one result per name in :func:`passes_for`
    ``(names)``."""
    values: dict[str, float] = {}
    not_computed: dict[str, str] = {}
    for name in names:
        row = METRICS[name]
        if row.test_pass is None:
            not_computed[name] = NO_TEST_ALGORITHM
            continue
        values[name] = float(row.test_value(pass_results[row.test_pass]))
    return values, not_computed

"""Sampling budget planner — profile.json + a row budget -> keep-rates and weights.

Reads the per-cell positive / negative counts that
``scripts/profile_sample_pool.py`` landed, and turns a sampling budget (how
many train + train_dev rows the training environment can hold) into two
config snippets:

- ``dataset.sample_ratio_overrides``: a keep-rate for every sampling cell and
  class that has rows, so the budget is spent on the rows that carry the most
  information;
- ``training.sample_weights`` (+ ``training.sample_weight_keys``): the weights
  that undo the bias the keep-rates introduce (Horvitz-Thompson: weight =
  target weight / keep-rate, times one constant).

Every key left out — a class or a combination with no rows — is counted in
both snippets' headers and named, with why, in the report (``missing_entries``).

The computation is specified step by step in issue #525 ("計算規格"); the step
numbers in the comments below are that spec's. Terms (抽樣預算, 抽樣格, 分層欄,
保留率, 目標權重, 樣本權重) are CONTEXT.md's "抽樣與權重" section.

Pure Python and seconds-fast: change the budget or a knob and rerun only this,
never the Spark profile.

Self-contained dev tool (logic + CLI in one file), mirroring
scripts/sampling_overrides_editor.py — unit-tested via
``from scripts.sampling_budget_planner import ...`` in tests/scripts/. Not part
of the production DAG.

Usage (from the directory holding conf/, the repo root as a rule; config read
as ``--env`` reads it for the pipelines):
  python scripts/sampling_budget_planner.py plan \\
      data/profiling/sample_pool_profile/profile.json --budget 10000000 --env <env>
"""

from __future__ import annotations

import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import typer

from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.consistency import (
    item_list_counted_from_data,
    override_unknown_item_errors,
    resolved_env_dir,
    resolved_item_values,
    weight_key_arity_mismatch,
    weight_key_columns_unavailable,
    weight_unknown_item_errors,
)
from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import HASH_BUCKETS

# profile.json's layout, written by scripts/profile_sample_pool.py. Kept here
# as well (the two scripts stay self-contained); the profile test pins that
# both say the same number.
PROFILE_FORMAT_VERSION = 1
DEFAULT_OUT_DIR = Path("data/profiling/sampling_budget_plan")
# Combinations with no rows at all that the report lists; every one is counted.
NO_ROWS_SHOWN = 100

# The label component of a key: the framework's label is 0/1 and every key
# part is the value cast to string (sampling.py's with_effective_sample_ratio).
_NEG_LABEL = "0"
_POS_LABEL = "1"


@dataclass(frozen=True)
class Cell:
    """One sampling cell: a ``sample_group_keys`` value combination minus label.

    ``values`` follow ``sample_group_keys`` order with the label left out;
    ``item`` is the item component of those values.
    """

    values: tuple
    item: str
    n_pos: int
    n_neg: int


@dataclass(frozen=True)
class Knobs:
    """The planner's knobs (#525 "旋鈕與預設").

    ``weight_t`` / ``weight_alpha`` default to ``t`` / ``alpha``. Changing only
    them changes the weights, never the keep-rates.
    """

    t: float = 1 / 6
    alpha: float = 0.5
    gamma: float = 1.0
    eta: float = 0.2
    pos_floor: float = 0.6
    weight_t: float | None = None
    weight_alpha: float | None = None

    @property
    def weight_t_used(self) -> float:
        return self.t if self.weight_t is None else self.weight_t

    @property
    def weight_alpha_used(self) -> float:
        return self.alpha if self.weight_alpha is None else self.weight_alpha


@dataclass(frozen=True)
class ItemTarget:
    """Step 1 for one item: target weights at item level only."""

    P: int
    N: int
    v: float
    A: float
    q_pos: float
    q_neg: float


@dataclass
class CellPlan:
    """One cell through the steps. ``None`` marks a class the cell lacks."""

    cell: Cell
    p_tilde: float
    value_pos: float
    value_neg: float
    M: float
    r_pos: float | None = None
    r_neg: float | None = None
    b_pos: int | None = None
    b_neg: int | None = None
    r_eff_pos: float | None = None
    r_eff_neg: float | None = None
    w_pos: float | None = None
    w_neg: float | None = None


@dataclass
class Plan:
    """``lam`` is None when the budget covers the whole population.

    ``max_reachable`` is the most rows any lambda keeps; below ``budget`` it
    means the budget cannot be spent (see :func:`_solve_lambda`).
    """

    items: dict
    cells: list
    lam: float | None
    budget: float
    population: int
    keeps_everything: bool
    max_reachable: float
    weight_items: dict
    k: float
    knobs: Knobs


class PlanError(ValueError):
    """A plan that cannot be made; the message says why and how to fix it."""


def item_targets(cells: list[Cell], t: float, alpha: float) -> dict[str, ItemTarget]:
    """Step 1: per-item target weights.

    ``v`` folds an item's negatives so its weighted positive rate is ``t``;
    ``A = (P_min / P)^alpha`` is the attention a colder item gets relative to
    the hottest. Item level only — never per stratum (#525 explains why: a
    stratum-only weight changes no ranking inside a query group, and a
    stratum x item weight flattens exactly what the model should learn).
    """
    P: dict[str, int] = {}
    N: dict[str, int] = {}
    for c in cells:
        P[c.item] = P.get(c.item, 0) + c.n_pos
        N[c.item] = N.get(c.item, 0) + c.n_neg
    p_min = min(P.values())
    out = {}
    for item in P:
        # An item with no negatives has nothing to fold; its v is never read.
        v = P[item] * (1 - t) / (t * N[item]) if N[item] else 1.0
        A = (p_min / P[item]) ** alpha
        out[item] = ItemTarget(P=P[item], N=N[item], v=v, A=A, q_pos=A, q_neg=A * v)
    return out


def _cell_value(cell: Cell, target: ItemTarget) -> tuple[float, float, float, float]:
    """Steps 2-3: ``(p_tilde, value_pos, value_neg, M)`` of one cell.

    ``p_tilde`` is the cell's purchase rate in the model's (weighted) world;
    a row's value is its target weight times the logloss gradient size
    ``|y - p_tilde|``. ``M = n+ * value+``, which equals ``n- * value-``.
    """
    denom = cell.n_pos + cell.n_neg * target.v
    p_tilde = cell.n_pos / denom if denom else 0.0
    value_pos = target.q_pos * (1 - p_tilde)
    value_neg = target.q_neg * p_tilde
    return p_tilde, value_pos, value_neg, cell.n_pos * value_pos


def retention(
    cells: list[CellPlan], lam: float, knobs: Knobs,
) -> list[tuple[float | None, float | None]]:
    """Steps 4-6: continuous ``(r+, r-)`` per cell at threshold ``lam``.

    ``None`` for a class the cell does not have (it gets no key at all).

    - Step 4: ``r_IS = M^gamma / (lam * n)`` — gamma=1 keeps rows in
      proportion to M (scheme E), gamma=0.5 in proportion to sqrt(M).
    - Step 5: positives ``max(f, min(1, r_IS+))``.
    - Step 6: negatives mix in ``eta`` of the item's average rate (defensive
      mixture), so no cell's negative weight exceeds 1/eta times the item
      average; uncapped, the item's negative total is unchanged.
    """
    base = [c.M ** knobs.gamma / lam if c.M > 0 else 0.0 for c in cells]
    r_is_neg = [b / c.cell.n_neg if c.cell.n_neg else None for b, c in zip(base, cells)]

    kept_neg: dict[str, float] = {}
    total_neg: dict[str, int] = {}
    for c, r in zip(cells, r_is_neg):
        if r is not None:
            kept_neg[c.cell.item] = kept_neg.get(c.cell.item, 0.0) + c.cell.n_neg * min(1.0, r)
            total_neg[c.cell.item] = total_neg.get(c.cell.item, 0) + c.cell.n_neg
    r_bar = {item: kept_neg[item] / total_neg[item] for item in kept_neg}

    # Step 8's one-bucket floor is applied here already, so that lambda is
    # solved with the rows it forces: applied only after the solve, a class
    # held at one bucket would spend rows on top of the budget.
    one_bucket = 1 / HASH_BUCKETS
    out = []
    for b, c, r_neg in zip(base, cells, r_is_neg):
        r_pos = (max(knobs.pos_floor, one_bucket, min(1.0, b / c.cell.n_pos))
                 if c.cell.n_pos else None)
        if r_neg is not None:
            r_neg = max(one_bucket, min(
                1.0, (1 - knobs.eta) * r_neg + knobs.eta * r_bar[c.cell.item]))
        out.append((r_pos, r_neg))
    return out


def to_bucket(rate: float) -> int:
    """Step 8: a keep-rate as a whole number of hash buckets, in [1, 100000].

    Rounded half up. Never 0: a class with no bucket vanishes from the sample,
    and a weight of target / 0 cannot undo that.
    """
    return min(HASH_BUCKETS, max(1, math.floor(rate * HASH_BUCKETS + 0.5)))


def ratio_literal(bucket: int) -> str:
    """The YAML text for ``bucket`` buckets, exact under the pipeline's reading.

    The pipeline keeps a row when ``crc32 % 100000 < int(ratio * 100000)``
    (``keep_rows_drawn_under_ratio``): truncation, so ``0.00003`` can come
    back as 2.9999999999999996 buckets and lose one. Write ``b / 100000``
    when it survives that, else ``(b + 0.5) / 100000``, which is half a bucket
    clear of both edges. Fixed-point, never ``1e-05``: PyYAML reads an
    exponent without a dot as a string.
    """
    text = f"{bucket / HASH_BUCKETS:.5f}"
    if int(float(text) * HASH_BUCKETS) != bucket:
        text = f"{(bucket + 0.5) / HASH_BUCKETS:.6f}"
    return text


def _expected_rows(cells: list[CellPlan], rates) -> float:
    return sum(c.cell.n_pos * (r_pos or 0.0) + c.cell.n_neg * (r_neg or 0.0)
               for c, (r_pos, r_neg) in zip(cells, rates))


def _min_rows(cells: list[Cell], pos_floor: float) -> float:
    """The fewest rows any lambda can keep: the positive floor, and one bucket
    for every class a cell has (step 8 never lets a class vanish)."""
    one_bucket = 1 / HASH_BUCKETS
    return sum(c.n_pos * max(pos_floor, one_bucket) + c.n_neg * one_bucket for c in cells)


def _solve_lambda(cells: list[CellPlan], budget: float, knobs: Knobs) -> tuple[float, float]:
    """Step 7: ``(lambda, the most rows any lambda can keep)``.

    The expected row count falls monotonically as lambda grows, so bisection
    (geometric, since lambda spans orders of magnitude) finds the budget.

    The bottom of the range, ``lo``, is where every class with value is kept
    whole: r_IS >= 1 / (1 - eta) there, so even the mixed negative rate
    ``(1 - eta) r_IS + eta rbar`` reaches 1 (with eta = 1 the rate is rbar
    itself, already at its largest once every r_IS >= 1). Nothing smaller
    keeps more. That can still be short of the budget: a cell with no
    positives has M = 0 and only ever gets ``eta * rbar`` of its negatives.
    Then the budget cannot be spent; ``lo`` is returned and the caller says so.
    """
    valued = [c for c in cells if c.M > 0]
    if not valued:
        lam = 1.0
        return lam, _expected_rows(cells, retention(cells, lam, knobs))
    saturate = 1 - knobs.eta if knobs.eta < 1 else 1.0
    lo = saturate * min(c.M ** knobs.gamma / max(c.cell.n_pos, c.cell.n_neg) for c in valued)

    def total(lam: float) -> float:
        return _expected_rows(cells, retention(cells, lam, knobs))

    reachable = total(lo)
    if reachable <= budget:
        return lo, reachable
    hi = lo * 2
    while total(hi) > budget and hi < 1e300:
        hi *= 2
    for _ in range(200):
        mid = math.sqrt(lo * hi)
        if mid in (lo, hi):
            break
        if total(mid) > budget:
            lo = mid
        else:
            hi = mid
    return hi, reachable


def _knob_errors(knobs: Knobs) -> list[str]:
    errors = []
    if not 0 < knobs.gamma <= 1:
        errors.append(f"--gamma {knobs.gamma} is outside (0, 1]: 1 keeps rows in "
                      "proportion to value, 0.5 to its square root; 0 would ignore value.")
    for name, flag, value in (("eta", "--eta", knobs.eta),
                              ("pos_floor", "--pos-floor", knobs.pos_floor)):
        if not 0 <= value <= 1:
            errors.append(f"{flag} ({name}) {value} is outside [0, 1]: it is a share of rows.")
    for name, flag, value in (("t", "--t", knobs.t),
                              ("weight_t", "--weight-t", knobs.weight_t_used)):
        if not 0 < value < 1:
            errors.append(f"{flag} ({name}) {value} is outside (0, 1): it is the weighted "
                          "positive rate every item is set to, and v = P(1-t)/(tN) "
                          "is undefined at the ends.")
    return errors


def plan(cells: list[Cell], budget: float, knobs: Knobs) -> Plan:
    """Steps 1-9 for ``cells`` under ``budget`` rows (train + train_dev)."""
    errors = _knob_errors(knobs)
    if errors:
        raise PlanError("\n".join(errors))
    positives: dict[str, int] = {}
    for c in cells:
        positives[c.item] = positives.get(c.item, 0) + c.n_pos
    no_positives = sorted(item for item, p in positives.items() if p == 0)
    if no_positives:
        raise PlanError(
            f"item(s) {no_positives} have no positive rows in the profiled train "
            f"months, so their target weight is undefined (A = (P_min / P)^alpha "
            f"and v = P(1-t)/(tN) both need P > 0). Widen dataset.train_snap_dates "
            f"so every item has sold at least once, or take the item out of "
            f"sample_pool for these months; then rerun profile_sample_pool.py."
        )
    targets = item_targets(cells, knobs.t, knobs.alpha)
    cell_plans = []
    for c in cells:
        p_tilde, value_pos, value_neg, M = _cell_value(c, targets[c.item])
        cell_plans.append(CellPlan(cell=c, p_tilde=p_tilde, value_pos=value_pos,
                                   value_neg=value_neg, M=M))

    population = sum(c.n_pos + c.n_neg for c in cells)
    keeps_everything = budget >= population
    if keeps_everything:
        lam = None
        rates = [(1.0 if c.n_pos else None, 1.0 if c.n_neg else None) for c in cells]
        reachable = float(population)
    else:
        floor_rows = _min_rows(cells, knobs.pos_floor)
        if budget < floor_rows:
            raise PlanError(
                f"budget {budget:,.0f} is below the fewest rows any plan can keep, "
                f"{math.ceil(floor_rows):,}: the positive floor keeps "
                f"{knobs.pos_floor} of every positive, and every class of every "
                f"cell keeps at least 1 of {HASH_BUCKETS} hash buckets. Raise "
                f"--budget or lower --pos-floor."
            )
        lam, reachable = _solve_lambda(cell_plans, budget, knobs)
        rates = retention(cell_plans, lam, knobs)
    for c, (r_pos, r_neg) in zip(cell_plans, rates):
        c.r_pos, c.r_neg = r_pos, r_neg
        # Step 8: from here on only the bucketed rate is real — it is what the
        # pipeline will apply, so it is what the weights must divide by.
        if r_pos is not None:
            c.b_pos = to_bucket(r_pos)
            c.r_eff_pos = c.b_pos / HASH_BUCKETS
        if r_neg is not None:
            c.b_neg = to_bucket(r_neg)
            c.r_eff_neg = c.b_neg / HASH_BUCKETS

    # Step 9. The weight target is recomputed with its own knobs; the
    # keep-rates above never read them, so changing only those leaves the
    # overrides byte-identical — and HT needs nothing more than w = q / r.
    weight_targets = item_targets(cells, knobs.weight_t_used, knobs.weight_alpha_used)
    kept = sum(_kept_split(cell_plans))
    target_mass = sum(c.cell.n_pos * weight_targets[c.cell.item].q_pos
                      + c.cell.n_neg * weight_targets[c.cell.item].q_neg
                      for c in cell_plans)
    # k makes the expected mean weight of the drawn rows 1: LightGBM's
    # min_sum_hessian_in_leaf and the like are absolute, so the scale matters.
    k = kept / target_mass
    for c in cell_plans:
        wt = weight_targets[c.cell.item]
        if c.r_eff_pos is not None:
            c.w_pos = k * wt.q_pos / c.r_eff_pos
        if c.r_eff_neg is not None:
            c.w_neg = k * wt.q_neg / c.r_eff_neg
    return Plan(items=targets, cells=cell_plans, lam=lam, budget=budget,
                population=population, keeps_everything=keeps_everything,
                max_reachable=reachable, weight_items=weight_targets, k=k,
                knobs=knobs)


def cell_key(values: tuple, group_keys: list[str], label_col: str, label: str) -> str:
    """``values`` (``group_keys`` order, label left out) as an override key:
    joined with ``|``, the label component in the label's own position."""
    rest = iter(values)
    return "|".join(label if k == label_col else str(next(rest)) for k in group_keys)


def override_entries(result: Plan, group_keys: list[str], label_col: str) -> dict[str, str]:
    """Every cell and class that has rows as ``{key: YAML ratio text}``, so no
    row of the profiled population is left to ``dataset.sample_ratio``. The
    keys left out are :func:`missing_entries`'."""
    out = {}
    for c in result.cells:
        if c.b_pos is not None:
            out[cell_key(c.cell.values, group_keys, label_col, _POS_LABEL)] = ratio_literal(c.b_pos)
        if c.b_neg is not None:
            out[cell_key(c.cell.values, group_keys, label_col, _NEG_LABEL)] = ratio_literal(c.b_neg)
    return out


def weight_entries(result: Plan, group_keys: list[str], label_col: str) -> dict[str, float]:
    """Every cell and class as ``{key: weight}``, keys identical to the
    overrides' — ``sample_weight_keys`` is set to ``sample_group_keys``."""
    out = {}
    for c in result.cells:
        if c.w_pos is not None:
            out[cell_key(c.cell.values, group_keys, label_col, _POS_LABEL)] = c.w_pos
        if c.w_neg is not None:
            out[cell_key(c.cell.values, group_keys, label_col, _NEG_LABEL)] = c.w_neg
    return out


_NO_POS_ROWS, _NO_NEG_ROWS = "no positive rows", "no negative rows"


@dataclass(frozen=True)
class Missing:
    """The keys the snippets leave out (:func:`missing_entries`), so that a
    reader can tell them from keys a config forgot.

    ``one_class``: ``(key, why)`` for each cell that has rows of one class
    only. ``no_rows``: how many combinations have no rows at all, each leaving
    out both its keys; ``no_rows_shown`` lists the first :data:`NO_ROWS_SHOWN`
    of them as ``(negative key, positive key)``.
    """

    one_class: list
    no_rows: int
    no_rows_shown: list

    @property
    def keys(self) -> int:
        return len(self.one_class) + 2 * self.no_rows


def missing_entries(result: Plan, overrides: dict[str, str], group_keys: list[str],
                    label_col: str, extra_levels: dict[str, list] | None = None) -> Missing:
    """Every key of the sampling grid that ``overrides`` — and so the weights,
    which have the same keys — leaves out.

    The grid crosses, for each sampling key, every value the profile has, plus
    ``extra_levels`` (the CLI passes the declared items, so that an item with
    no rows at all is named too). A key is left out because it has no rows in
    the profiled train months: there is nothing to keep or to weigh. Rows
    under it that reach the draw later are kept at ``dataset.sample_ratio`` and
    weighted 1.0 (``with_effective_sample_ratio``; ``io/extract.py``).
    """
    one_class = sorted(
        (key, why)
        for c in result.cells
        for label, why in ((_NEG_LABEL, _NO_NEG_ROWS), (_POS_LABEL, _NO_POS_ROWS))
        if (key := cell_key(c.cell.values, group_keys, label_col, label)) not in overrides
    )
    dims = [k for k in group_keys if k != label_col]
    levels = [sorted({c.cell.values[i] for c in result.cells}
                     | {str(v) for v in (extra_levels or {}).get(dim, [])})
              for i, dim in enumerate(dims)]
    seen = {c.cell.values for c in result.cells}
    unseen = (v for v in itertools.product(*levels) if v not in seen)
    shown = [tuple(cell_key(v, group_keys, label_col, label) for label in (_NEG_LABEL, _POS_LABEL))
             for v in itertools.islice(unseen, NO_ROWS_SHOWN)]
    return Missing(one_class=one_class, no_rows=math.prod(map(len, levels)) - len(seen),
                   no_rows_shown=shown)


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def _spread(values: list[float]) -> float:
    """Relative spread ``max / min - 1`` of positive numbers; 0 when all equal."""
    return max(values) / min(values) - 1 if values else 0.0


def self_checks(
    result: Plan,
    overrides: dict[str, str],
    weights: dict[str, float],
    group_keys: list[str],
    label_col: str,
    tol: float = 1e-9,
) -> list[Check]:
    """#525 acceptance 3, run on the emitted entries rather than the plan's floats.

    A keep-rate is read back the way the pipeline reads it — the YAML text,
    truncated to whole buckets — so a literal that lost a bucket shows up here.
    Expected counts are ``n * r_eff``; weighted ones ``n * r_eff * w``.
    """
    pair: dict[tuple[str, str], list[float]] = {}   # (item, label) -> r_eff * w per cell
    kept = {"rows": 0.0, "weight": 0.0}
    item_mass: dict[str, dict[str, float]] = {}
    for c in result.cells:
        mass = item_mass.setdefault(c.cell.item, {"pos": 0.0, "neg": 0.0})
        for label, n, side in ((_POS_LABEL, c.cell.n_pos, "pos"), (_NEG_LABEL, c.cell.n_neg, "neg")):
            if not n:
                continue
            key = cell_key(c.cell.values, group_keys, label_col, label)
            r_eff = int(float(overrides[key]) * HASH_BUCKETS) / HASH_BUCKETS
            w = weights[key]
            pair.setdefault((c.cell.item, label), []).append(r_eff * w)
            kept["rows"] += n * r_eff
            kept["weight"] += n * r_eff * w
            mass[side] += n * r_eff * w

    worst_pair = max(_spread(v) for v in pair.values())
    # An item with no negatives has no positive rate to set, and its loss is
    # P * A rather than P * A / t; both checks skip it.
    with_negatives = [i for i in item_mass if result.items[i].N]
    t_w, alpha_w = result.knobs.weight_t_used, result.knobs.weight_alpha_used
    rate_err = max(
        (abs(item_mass[i]["pos"] / (item_mass[i]["pos"] + item_mass[i]["neg"]) / t_w - 1)
         for i in with_negatives),
        default=0.0,
    )
    shares = [(item_mass[i]["pos"] + item_mass[i]["neg"]) / result.items[i].P ** (1 - alpha_w)
              for i in with_negatives]
    mean_weight = kept["weight"] / kept["rows"]
    return [
        Check("keep_rate_x_weight_equal_across_strata", worst_pair <= tol,
              f"largest relative spread of r_eff*w within an item and class: {worst_pair:.2e}"),
        Check("weighted_positive_rate_is_weight_t", rate_err <= tol,
              f"largest relative error of an item's weighted positive rate: {rate_err:.2e}"),
        Check("loss_share_follows_P_to_the_1_minus_weight_alpha", _spread(shares) <= tol,
              f"relative spread of loss / P^(1-weight_alpha) across items: {_spread(shares):.2e}"),
        Check("mean_weight_is_one", abs(mean_weight - 1) <= tol,
              f"expected mean weight of the drawn rows: {mean_weight:.12f}"),
    ]


# ---------------------------------------------------------------------------
# profile.json and config: what the plan is made from, and what it must fit.
# ---------------------------------------------------------------------------
def _train_months(dataset_cfg: dict) -> list[str]:
    """``dataset.train_snap_dates`` as ``YYYY-MM-DD`` strings (ConfigLoader
    has already expanded a ``{start, end, step}`` range into its list)."""
    return [str(m)[:10] for m in dataset_cfg.get("train_snap_dates") or []]


def cells_from_profile(profile: dict, parameters: dict) -> list[Cell]:
    """The profile's cells summed up to sampling cells.

    The profile is counted at ``(sample_group_keys ∪ sample_weight_keys) \\
    {label}`` (the editor's granularity, so both tools can read one file);
    the plan only needs ``sample_group_keys \\ {label}``. Values come back in
    ``sample_group_keys`` order, which is the order the keys are joined in.
    """
    if profile.get("format_version") != PROFILE_FORMAT_VERSION:
        raise PlanError(
            f"profile format_version {profile.get('format_version')!r} is not "
            f"{PROFILE_FORMAT_VERSION}, the one this planner reads. Rerun "
            f"profile_sample_pool.py with this checkout."
        )
    schema = get_schema(parameters)
    label_col, item_col = schema["label"], schema["item"]
    group_keys = list((parameters.get("dataset") or {}).get("sample_group_keys") or [])
    dims = [k for k in group_keys if k != label_col]
    missing = [d for d in dims if d not in profile["granularity"]]
    if missing:
        raise PlanError(
            f"the profile was counted by {profile['granularity']}, which does not "
            f"split by {missing}; dataset.sample_group_keys is {group_keys}. "
            f"rerun profile_sample_pool.py against the current config."
        )
    summed: dict[tuple, list[int]] = {}
    for row in profile["cells"]:
        values = tuple(row["values"][d] for d in dims)
        counts = summed.setdefault(values, [0, 0])
        counts[0] += row["n_pos"]
        counts[1] += row["n_neg"]
    item_at = dims.index(item_col) if item_col in dims else None
    return [
        Cell(values=values, item=values[item_at] if item_at is not None else "",
             n_pos=n_pos, n_neg=n_neg)
        for values, (n_pos, n_neg) in sorted(summed.items())
    ]


def config_problems(parameters: dict, profile: dict) -> tuple[list[str], list[str]]:
    """``(errors, warnings)`` about the config and the profile (#525 "檢查").

    Errors refuse the plan; warnings go into the report and the terminal.
    """
    errors: list[str] = []
    warnings: list[str] = []
    schema = get_schema(parameters)
    label_col, item_col = schema["label"], schema["item"]
    ds = parameters.get("dataset") or {}
    group_keys = list(ds.get("sample_group_keys") or [])

    for role, col in (("label", label_col), ("item", item_col)):
        if col not in group_keys:
            errors.append(
                f"the {role} column {col!r} is not in dataset.sample_group_keys "
                f"{group_keys}. The planner keeps a rate per item and class, so "
                f"both must be sampling keys: add {col!r} to sample_group_keys."
            )
    # The weights are keyed exactly like the overrides (HT needs weight keys at
    # least as fine as the sampling keys), so every stratum must be a column
    # train model_input carries — A9a, asked of sample_group_keys.
    probe = {**parameters, "training": {**(parameters.get("training") or {}),
                                        "sample_weight_keys": group_keys}}
    unavailable = weight_key_columns_unavailable(probe)
    if unavailable:
        errors.append(
            f"A9a: stratum column(s) {unavailable} cannot be a "
            f"training.sample_weight_keys column — train model_input only carries "
            f"identity columns, the label, dataset.carry_columns and declared "
            f"categoricals, so a weight keyed on it would silently stay 1.0. Add "
            f"it to dataset.carry_columns (and, if feature_table has it too, to "
            f"dataset.prepare_model_input.drop_columns — B7)."
        )

    profiled = list(profile.get("train_snap_dates") or [])
    configured = _train_months(ds)
    if profiled != configured:
        warnings.append(
            f"dataset.train_snap_dates {configured} differs from the months the "
            f"profile counted {profiled}. Changing the train months means "
            f"rerunning profile_sample_pool.py; this plan is for the old ones."
        )
    nulls = profile.get("null_rows") or {}
    if nulls.get("rows"):
        warnings.append(
            f"the profile found {nulls['rows']:,} row(s) with a NULL in a key "
            f"column (by column: {nulls.get('by_column')}). Their key matches no "
            f"override, so the draw keeps them at dataset.sample_ratio "
            f"({ds.get('sample_ratio')}) and training weights them 1.0 — outside "
            f"this budget and unweighted next to rows weighted far above 1. Fix "
            f"the source table, or accept them knowingly."
        )
    objective = ((parameters.get("training") or {}).get("algorithm_params") or {}).get("objective")
    if objective is not None and objective != "binary":
        warnings.append(
            f"training.algorithm_params.objective is {objective!r}. These weights "
            f"undo the sampling bias only under a pointwise loss: a ranking "
            f"objective weights pairs, and a per-row draw breaks query groups apart."
        )
    zero_pos = ds.get("train_zero_positive_group_ratio")
    if zero_pos is not None and zero_pos != 1:
        warnings.append(
            f"dataset.train_zero_positive_group_ratio is {zero_pos}: it drops whole "
            f"query groups after this draw, and no weight here makes up for them, "
            f"so neither the budget nor the unbiasedness holds."
        )
    return errors, warnings


def output_problems(parameters: dict, overrides: dict, weights: dict) -> list[str]:
    """A5 / A9b / A9c on the two tables about to be written.

    The same predicates the CLI entry runs, so a pasted table cannot fail the
    pipeline's own gate. With an item list counted from the data (#379) there is
    no list before the dataset run; A5 / A9c then return nothing here and run
    where the list is counted.
    """
    group_keys = list((parameters.get("dataset") or {}).get("sample_group_keys") or [])
    probe = {
        **parameters,
        "dataset": {**(parameters.get("dataset") or {}), "sample_ratio_overrides": overrides},
        "training": {**(parameters.get("training") or {}),
                     "sample_weight_keys": group_keys, "sample_weights": weights},
    }
    errors = override_unknown_item_errors(probe) + weight_unknown_item_errors(probe)
    arity = weight_key_arity_mismatch(probe)
    if arity:
        errors.append(f"A9b: weight key(s) {arity[:5]} do not have "
                      f"{len(group_keys)} '|'-separated parts.")
    return errors


# ---------------------------------------------------------------------------
# Rendering: two pasteable YAML snippets and a markdown report.
# ---------------------------------------------------------------------------
def yaml_float(x: float) -> str:
    """``x`` as YAML text that reads back as exactly ``x``.

    ``repr`` round-trips, but PyYAML (YAML 1.1) reads ``1e-05`` — an exponent
    with no dot — as a string, so a dot is put in.
    """
    text = repr(float(x))
    mantissa, _, exponent = text.partition("e")
    if exponent and "." not in mantissa:
        text = f"{mantissa}.0e{exponent}"
    return text


def _knob_line(knobs: Knobs) -> str:
    return (f"keep-rate knobs: t={knobs.t} alpha={knobs.alpha} gamma={knobs.gamma} "
            f"eta={knobs.eta} pos_floor={knobs.pos_floor}")


def _source_lines(profile_path: str, profile: dict, budget: int) -> list[str]:
    return [
        f"profile: {profile_path} (generated {profile.get('generated_at')} "
        f"from {profile.get('source')})",
        f"budget: {budget:,} rows (train + train_dev)",
    ]


def _missing_line(missing: Missing) -> str:
    """One line on the keys left out, for both snippets' headers."""
    if not missing.keys:
        return ("Every key of every combination has an entry: none is left to "
                "dataset.sample_ratio.")
    return (f"{missing.keys} key(s) have no entry: no such rows in the train months "
            f"profiled (report.md lists them). Rows under such a key, should any reach "
            f"the draw, are kept at dataset.sample_ratio and weighted 1.0.")


def render_ratio_yaml(overrides: dict[str, str], profile_path: str, profile: dict,
                      budget: int, knobs: Knobs, missing: Missing) -> str:
    """The overrides snippet, indented to paste under ``dataset:``.

    The header names only what decides these numbers: the weight knobs are
    left out so that changing only them leaves this file byte-identical.
    """
    header = [
        "Generated by scripts/sampling_budget_planner.py plan.",
        "Paste under `dataset:` in conf/base/parameters_dataset.yaml, replacing "
        "sample_ratio_overrides.",
        *_source_lines(profile_path, profile, budget),
        _knob_line(knobs),
        "weight_t / weight_alpha do not reach this file: change only them and it "
        "stays byte-identical.",
        _missing_line(missing),
    ]
    body = ["  sample_ratio_overrides:"] + [
        f"    {json.dumps(k, ensure_ascii=False)}: {overrides[k]}" for k in sorted(overrides)]
    return "".join(f"# {line}\n" for line in header) + "\n".join(body) + "\n"


def render_weight_yaml(weights: dict[str, float], group_keys: list[str], profile_path: str,
                       profile: dict, budget: int, knobs: Knobs, missing: Missing) -> str:
    """The weights snippet, indented to paste under ``training:``."""
    header = [
        "Generated by scripts/sampling_budget_planner.py plan.",
        "Paste under `training:` in conf/base/parameters_training.yaml, replacing "
        "sample_weight_keys and sample_weights.",
        *_source_lines(profile_path, profile, budget),
        _knob_line(knobs),
        f"weight knobs: weight_t={knobs.weight_t_used} "
        f"weight_alpha={knobs.weight_alpha_used}",
        "sample_weight_keys = dataset.sample_group_keys: HT needs weight keys at "
        "least as fine as the sampling keys.",
        _missing_line(missing),
    ]
    body = (["  sample_weight_keys:"]
            + [f"    - {json.dumps(k, ensure_ascii=False)}" for k in group_keys]
            + ["  sample_weights:"]
            + [f"    {json.dumps(k, ensure_ascii=False)}: {yaml_float(weights[k])}"
               for k in sorted(weights)])
    return "".join(f"# {line}\n" for line in header) + "\n".join(body) + "\n"


def _limit_counts(result: Plan) -> dict[str, int]:
    """How many cell-classes a limit, rather than value, decided."""
    capped = floor = one_bucket = 0
    for c in result.cells:
        for r, b in ((c.r_pos, c.b_pos), (c.r_neg, c.b_neg)):
            if r is None:
                continue
            capped += r == 1.0
            one_bucket += r == 1 / HASH_BUCKETS     # retention() floors it there
        floor += (c.r_pos is not None and 0 < result.knobs.pos_floor < 1
                  and c.r_pos == result.knobs.pos_floor)
    return {"capped at 1": capped, "positive floor": floor, "1-bucket floor": one_bucket}


def _kept_split(cells: list[CellPlan]) -> tuple[float, float]:
    """Expected ``(positive, negative)`` rows the draw keeps: n x bucketed rate."""
    pos = sum(c.cell.n_pos * (c.r_eff_pos or 0.0) for c in cells)
    neg = sum(c.cell.n_neg * (c.r_eff_neg or 0.0) for c in cells)
    return pos, neg


def _kept_sd(cells: list[CellPlan]) -> float:
    """Standard deviation of the kept row count: each row is its own draw."""
    return math.sqrt(sum(
        n * r * (1 - r)
        for c in cells
        for n, r in ((c.cell.n_pos, c.r_eff_pos), (c.cell.n_neg, c.r_eff_neg))
        if r is not None
    ))


def summary_lines(result: Plan) -> list[str]:
    """λ, k, expected rows against the budget, and the limit counts."""
    pos, neg = _kept_split(result.cells)
    lines = []
    if result.keeps_everything:
        lines.append(f"budget {result.budget:,} >= population {result.population:,}: "
                     f"every keep-rate is 1 (no λ).")
    else:
        lines.append(f"λ = {result.lam:.6g}")
        if result.max_reachable < result.budget:
            lines.append(
                f"the budget cannot be spent: at most {result.max_reachable:,.0f} rows "
                f"can be kept, because a cell with no positives only ever keeps eta "
                f"of its item's average negative rate. Raise --eta to spend more.")
    lines += [
        f"k = {result.k:.6g}",
        f"expected rows: {pos:,.0f} positive + {neg:,.0f} negative = {pos + neg:,.0f} "
        f"(budget {result.budget:,}, difference {pos + neg - result.budget:+,.2f}; "
        f"the draw's standard deviation ≈ {_kept_sd(result.cells):,.0f} rows)",
        "cell-classes held by a limit: " + ", ".join(
            f"{name} {n}" for name, n in _limit_counts(result).items()),
    ]
    return lines


def _missing_section(missing: Missing) -> list[str]:
    """The report's list of the keys left out, each with why. A list, not a
    table: keys hold ``|``, which a markdown table would split on."""
    lines = ["## Keys with no entry", "",
             "Combinations cross every value each sampling key has in the profile, "
             "plus every declared item. A key below has no rows in the train months "
             "profiled, so there is nothing to keep or weigh and the snippets leave "
             "it out. Rows under it that reach the draw later (new data, other "
             "months) are kept at `dataset.sample_ratio` and weighted 1.0; rerun the "
             "profile then.", ""]
    if not missing.keys:
        return lines + ["None.", ""]
    lines += [f"- `{key}` — {why}" for key, why in missing.one_class]
    lines += [f"- `{neg}`, `{pos}` — no rows at all" for neg, pos in missing.no_rows_shown]
    unlisted = missing.no_rows - len(missing.no_rows_shown)
    if unlisted:
        lines += ["", f"{unlisted:,} more combination(s) with no rows at all are not listed."]
    return lines + [""]


def render_report(result: Plan, checks: list[Check], warnings: list[str],
                  profile_path: str, profile: dict, missing: Missing) -> str:
    lines = ["# Sampling budget plan", ""]
    lines += [f"- {line}" for line in _source_lines(profile_path, profile, result.budget)]
    lines += [f"- train months profiled: {', '.join(profile.get('train_snap_dates') or [])}",
              f"- {_knob_line(result.knobs)}",
              f"- weight knobs: weight_t={result.knobs.weight_t_used} "
              f"weight_alpha={result.knobs.weight_alpha_used}", ""]
    lines += ["## Summary", ""] + [f"- {line}" for line in summary_lines(result)] + [""]
    if warnings:
        lines += ["## Warnings", ""] + [f"- {w}" for w in warnings] + [""]
    lines += ["## Per item", "",
              "| item | P | kept positives | positive keep % | kept negatives | neg:pos | A | v |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for item in sorted(result.items):
        target = result.items[item]
        kept_pos = sum(c.cell.n_pos * (c.r_eff_pos or 0.0)
                       for c in result.cells if c.cell.item == item)
        kept_neg = sum(c.cell.n_neg * (c.r_eff_neg or 0.0)
                       for c in result.cells if c.cell.item == item)
        lines.append(
            f"| {item} | {target.P:,} | {kept_pos:,.0f} | {100 * kept_pos / target.P:.1f}% "
            f"| {kept_neg:,.0f} | {kept_neg / kept_pos:.2f} | {target.A:.4g} | {target.v:.4g} |")
    lines += [""] + _missing_section(missing)
    lines += ["## Self-checks", ""]
    lines += [f"- [{'PASS' if c.passed else 'FAIL'}] {c.name} — {c.detail}" for c in checks]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Typer CLI (thin orchestration around the logic above).
# ---------------------------------------------------------------------------
app = typer.Typer(help="Sampling budget planner: profile.json + budget -> "
                       "sample_ratio_overrides and sample_weights",
                  add_completion=False)


# The CLI's defaults are Knobs' own, so the two cannot drift apart.
_DEFAULT_KNOBS = Knobs()


@app.callback()
def _main() -> None:
    """Keeps ``plan`` a named subcommand (Typer collapses a lone command)."""


def _load_parameters(env: str) -> dict:
    """The parameters the pipelines read for ``--env``: conf/base with
    conf/<env> deep-merged over it, from the working directory's ``conf/``.

    A conf/<env> that does not exist is refused (A30), as the pipelines refuse
    it: the loader would read it as an empty overlay and plan on conf/base
    alone. Same text in profile_sample_pool.py and sampling_overrides_editor.py
    (each script stays self-contained). Raises ``ValueError``.
    """
    conf_dir = Path.cwd() / "conf"
    resolved_env_dir(conf_dir, env)
    return ConfigLoader(str(conf_dir), env=env).get_parameters()


def _fail(messages: list[str]) -> None:
    for m in messages:
        typer.echo(f"ERROR: {m}", err=True)
    raise typer.Exit(code=1)


@app.command("plan")
def plan_command(
    profile_json: Path = typer.Argument(..., help="profile.json from profile_sample_pool.py"),
    budget: int = typer.Option(..., help="sampling budget: train + train_dev rows"),
    t: float = typer.Option(_DEFAULT_KNOBS.t, help="weighted positive rate every item is set to"),
    alpha: float = typer.Option(_DEFAULT_KNOBS.alpha, help="attention to cold items (0 = none)"),
    gamma: float = typer.Option(
        _DEFAULT_KNOBS.gamma, help="budget across cells: 1 ∝ value, 0.5 ∝ sqrt(value)"),
    eta: float = typer.Option(
        _DEFAULT_KNOBS.eta, help="negative safety floor (defensive mixture share)"),
    pos_floor: float = typer.Option(_DEFAULT_KNOBS.pos_floor, help="lowest positive keep-rate"),
    weight_t: Optional[float] = typer.Option(None, help="t for the weights only (default: --t)"),
    weight_alpha: Optional[float] = typer.Option(
        None, help="alpha for the weights only (default: --alpha)"),
    env: str = typer.Option(
        "local", "--env", "-e",
        help="config environment, as for the pipelines: conf/base with conf/<env> over it"),
    out_dir: Path = typer.Option(DEFAULT_OUT_DIR, help="where the snippets and report go"),
) -> None:
    knobs = Knobs(t=t, alpha=alpha, gamma=gamma, eta=eta, pos_floor=pos_floor,
                  weight_t=weight_t, weight_alpha=weight_alpha)
    profile = json.loads(profile_json.read_text())
    try:
        parameters = _load_parameters(env)
    except ValueError as e:
        _fail([str(e)])
    group_keys = list(parameters["dataset"].get("sample_group_keys") or [])
    label_col = get_schema(parameters)["label"]

    errors, warnings = config_problems(parameters, profile)
    if errors:
        _fail(errors)
    try:
        cells = cells_from_profile(profile, parameters)
        result = plan(cells, budget=budget, knobs=knobs)
    except PlanError as e:
        _fail([str(e)])
    overrides = override_entries(result, group_keys, label_col)
    weights = weight_entries(result, group_keys, label_col)
    errors = output_problems(parameters, overrides, weights)
    if errors:
        _fail(errors)
    checks = self_checks(result, overrides, weights, group_keys, label_col)
    # With an item list counted from the data (#379) none is declared: the
    # items the profile has are the list.
    declared = [] if item_list_counted_from_data(parameters) else resolved_item_values(parameters)
    missing = missing_entries(result, overrides, group_keys, label_col,
                              {get_schema(parameters)["item"]: declared})

    for w in warnings:
        typer.echo(f"WARNING: {w}", err=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    report = out_dir / "report.md"
    report.write_text(render_report(result, checks, warnings, str(profile_json), profile,
                                    missing))
    typer.echo(f"profile: {profile_json} — {len(cells)} sampling cell(s)")
    for line in summary_lines(result):
        typer.echo(line)
    typer.echo(f"keys with no entry: {missing.keys}"
               + (" — no such rows in the train months; report.md lists them"
                  if missing.keys else ""))
    passed = sum(c.passed for c in checks)
    typer.echo(f"self-checks: {passed}/{len(checks)} passed")
    if passed != len(checks):
        _fail([f"self-check failed, so no snippet was written; see {report}"]
              + [f"{c.name}: {c.detail}" for c in checks if not c.passed])
    ratio_path = out_dir / "sample_ratio_overrides.yaml"
    weight_path = out_dir / "sample_weights.yaml"
    ratio_path.write_text(render_ratio_yaml(overrides, str(profile_json), profile, budget, knobs,
                                            missing))
    weight_path.write_text(render_weight_yaml(weights, group_keys, str(profile_json), profile,
                                              budget, knobs, missing))
    typer.echo(f"wrote {ratio_path} (under dataset:), {weight_path} (under training:), {report}")


if __name__ == "__main__":
    app()

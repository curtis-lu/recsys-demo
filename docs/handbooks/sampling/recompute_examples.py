"""Recompute every number in sampling_budget_principles.md.

The worked example (2 items x 2 strata) goes through the budget planner's own
``plan`` (scripts/sampling_budget_planner.py); the chapter 5 simulation is
plain numpy on the keep-rates ``plan`` returns. Output is grouped by chapter,
in the order the handbook uses the numbers.

Run from the repo root:
  PYTHONPATH=src .venv/bin/python docs/handbooks/sampling/recompute_examples.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from scripts.sampling_budget_planner import (  # noqa: E402
    HASH_BUCKETS, Cell, Knobs, plan, ratio_literal,
)

CELLS = [
    Cell(("seg_1", "prod_A"), "prod_A", 2_000, 8_000),
    Cell(("seg_2", "prod_A"), "prod_A", 500, 39_500),
    Cell(("seg_1", "prod_B"), "prod_B", 10, 9_990),
    Cell(("seg_2", "prod_B"), "prod_B", 40, 39_960),
]
BUDGET = 3_000
PURE = Knobs(eta=0, pos_floor=0)


def name(c) -> str:
    values = c.values if isinstance(c, Cell) else c.cell.values
    return f"{values[0]} × {values[1]}"


def noise(p) -> list[float]:
    """Relative variance of each cell's HT-estimated log-odds, (1-r)/(n r) per class."""
    return [(1 - c.r_eff_pos) / (c.cell.n_pos * c.r_eff_pos)
            + (1 - c.r_eff_neg) / (c.cell.n_neg * c.r_eff_neg) for c in p.cells]


def excess(p) -> tuple[float, float]:
    """Expected excess loss: one shared parameter, and one parameter per cell."""
    M = [c.M for c in p.cells]
    nz = noise(p)
    shared = 0.5 * sum(m * m * z for m, z in zip(M, nz)) / sum(M)
    per_cell = 0.5 * sum(m * z for m, z in zip(M, nz))
    return shared, per_cell


def show_plan(title: str, p) -> None:
    print(f"\n{title}: λ = {p.lam:.4f}, k = {p.k:.4f}")
    for c in p.cells:
        print(f"  {name(c)}: r+ = {c.r_eff_pos:.5f} (K+ = {c.cell.n_pos * c.r_eff_pos:7.1f}),"
              f" r- = {c.r_eff_neg:.5f} (K- = {c.cell.n_neg * c.r_eff_neg:7.1f}),"
              f" w+ = {c.w_pos:.3f}, w- = {c.w_neg:.3f}")
    kept = sum(c.cell.n_pos * c.r_eff_pos + c.cell.n_neg * c.r_eff_neg for c in p.cells)
    s, pc = excess(p)
    print(f"  expected rows = {kept:.1f}; excess loss: shared = {s:.4f}, per cell = {pc:.4f}")


def simulate(p, reps: int = 20_000, seed: int = 0) -> tuple[float, float]:
    """Mean excess loss over ``reps`` Bernoulli draws at ``p``'s keep-rates."""
    rng = np.random.default_rng(seed)
    n_pos = np.array([c.cell.n_pos for c in p.cells])
    n_neg = np.array([c.cell.n_neg for c in p.cells])
    q_pos = np.array([p.items[c.cell.item].q_pos for c in p.cells])
    q_neg = np.array([p.items[c.cell.item].q_neg for c in p.cells])
    r_pos = np.array([c.r_eff_pos for c in p.cells])
    r_neg = np.array([c.r_eff_neg for c in p.cells])
    mass_pos, mass_neg = n_pos * q_pos, n_neg * q_neg
    best = np.log(mass_pos / mass_neg)

    def loss(F):
        return mass_pos * np.logaddexp(0, -F) + mass_neg * np.logaddexp(0, F)

    hat_pos = q_pos * rng.binomial(n_pos, r_pos, size=(reps, len(n_pos))) / r_pos
    hat_neg = q_neg * rng.binomial(n_neg, r_neg, size=(reps, len(n_neg))) / r_neg
    per_cell = (loss(np.log(hat_pos / hat_neg)) - loss(best)).sum(axis=1)
    theta = np.zeros(reps)
    for _ in range(30):                     # Newton on the one shared shift
        s = 1 / (1 + np.exp(-(best + theta[:, None])))
        theta -= ((hat_neg * s - hat_pos * (1 - s)).sum(axis=1)
                  / ((hat_pos + hat_neg) * s * (1 - s)).sum(axis=1))
    shared = (loss(best + theta[:, None]) - loss(best)).sum(axis=1)
    return shared.mean(), per_cell.mean()


def main() -> None:
    population = sum(c.n_pos + c.n_neg for c in CELLS)
    print(f"population = {population:,}, budget = {BUDGET:,}")

    print("\n== Ch 1: keep every row at the same rate")
    r = BUDGET / population
    for c in CELLS:
        rate = c.n_pos / (c.n_pos + c.n_neg)
        print(f"  {name(c)}: purchase rate = {rate:.4f}, kept positives = {c.n_pos * r:.1f},"
              f" kept negatives = {c.n_neg * r:.0f}")

    print("\n== Ch 2: target weights (t = 1/6, α = 0.5)")
    p = plan(CELLS, BUDGET, PURE)
    for item, it in p.items.items():
        print(f"  {item}: P = {it.P}, N = {it.N}, v = {it.v:.6f}, A = {it.A:.6f},"
              f" weighted positive rate = {it.P / (it.P + it.N * it.v):.4f},"
              f" -log v = {-math.log(it.v):.3f}")
    for alpha in (0.0, 0.5, 1.0):
        its = plan(CELLS, BUDGET, Knobs(eta=0, pos_floor=0, alpha=alpha)).items
        mass = {i: it.A * (it.P + it.N * it.v) for i, it in its.items()}
        print(f"  α = {alpha}: loss share " + ", ".join(
            f"{i} {m / sum(mass.values()):.3f}" for i, m in mass.items()))
    for c in p.cells:
        rate = c.cell.n_pos / (c.cell.n_pos + c.cell.n_neg)
        print(f"  {name(c)}: p = {rate:.5f}, p̃ = {c.p_tilde:.5f}")

    print("\n== Ch 3: weights keyed coarser than the draw (prod_B, seg_1 negatives kept 0.5)")
    seg_1, seg_2 = CELLS[2], CELLS[3]
    odds_1 = seg_1.n_pos / (seg_1.n_neg * 0.5)
    odds_2 = seg_2.n_pos / seg_2.n_neg
    print(f"  odds seen: seg_1 = {odds_1:.6f}, seg_2 = {odds_2:.6f}, ratio = {odds_1 / odds_2:.2f}")
    print(f"  HT-weighted seg_1 odds = {seg_1.n_pos / (seg_1.n_neg * 0.5 / 0.5):.6f}")

    print("\n== Ch 4: one cell, 1,000 rows kept")
    for k_pos in (500, 200, 100):
        print(f"  K+ = {k_pos}, K- = {1000 - k_pos}: 1/K+ + 1/K- = {1 / k_pos + 1 / (1000 - k_pos):.4f}")

    print("\n== Ch 5: value per row and per cell")
    for c in p.cells:
        print(f"  {name(c)}: value+ = {c.value_pos:.5f}, value- = {c.value_neg:.6f},"
              f" M = {c.M:.3f} (n- × value- = {c.cell.n_neg * c.value_neg:.3f})")
    show_plan("γ = 1, η = 0, f = 0", p)
    p_half = plan(CELLS, BUDGET, Knobs(eta=0, pos_floor=0, gamma=0.5))
    show_plan("γ = 0.5, η = 0, f = 0", p_half)
    for title, q in (("γ = 1", p), ("γ = 0.5", p_half)):
        s, pc = simulate(q)
        print(f"  simulated {title}: shared = {s:.4f}, per cell = {pc:.4f}")

    print("\n== Ch 6: the two safeguards")
    for title, kn in (("η = 0.2, f = 0", Knobs(pos_floor=0)),
                      ("η = 0, f = 0.6", Knobs(eta=0)),
                      ("η = 0.2, f = 0.6 (defaults)", Knobs())):
        show_plan(title, plan(CELLS, BUDGET, kn))
    for title, kn in (("η = 0", PURE), ("η = 0.2", Knobs(pos_floor=0))):
        q = plan(CELLS, BUDGET, kn)
        a = [c for c in q.cells if c.cell.item == "prod_A"]
        r_bar = sum(c.cell.n_neg * c.r_eff_neg for c in a) / q.items["prod_A"].N
        w_bar = q.k * q.items["prod_A"].q_neg / r_bar
        print(f"  {title}: prod_A average negative rate = {r_bar:.6f}, its weight = {w_bar:.3f},"
              f" largest negative weight = {max(c.w_neg for c in a):.3f}"
              f" ({max(c.w_neg for c in a) / w_bar:.2f} times)")

    print("\n== Ch 7: buckets of the default plan")
    d = plan(CELLS, BUDGET, Knobs())
    for c in d.cells:
        for label, r_cont, b in (("+", c.r_pos, c.b_pos), ("-", c.r_neg, c.b_neg)):
            print(f"  {name(c)} {label}: continuous r = {r_cont:.7f}, buckets = {b},"
                  f" YAML = {ratio_literal(b)}")
    kept = sum(c.cell.n_pos * c.r_eff_pos + c.cell.n_neg * c.r_eff_neg for c in d.cells)
    mass = sum(c.cell.n_pos * d.weight_items[c.cell.item].q_pos
               + c.cell.n_neg * d.weight_items[c.cell.item].q_neg for c in d.cells)
    print(f"  k = {kept:.1f} / {mass:.2f} = {d.k:.4f}; one bucket = {1 / HASH_BUCKETS}")

    print("\n== Ch 8: change only the weight knobs (t_w = 0.25)")
    w = plan(CELLS, BUDGET, Knobs(weight_t=0.25))
    same = all((a.b_pos, a.b_neg) == (b.b_pos, b.b_neg) for a, b in zip(d.cells, w.cells))
    print(f"  buckets unchanged: {same}")
    for c in w.cells:
        print(f"  {name(c)}: w+ = {c.w_pos:.3f}, w- = {c.w_neg:.3f}")


if __name__ == "__main__":
    main()

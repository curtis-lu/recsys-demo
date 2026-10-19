"""Tests for scripts/sampling_budget_planner.py (#525).

The hand-worked example below is the source of truth for steps 1-9: every
expected number is a fraction worked out on paper from the spec in #525, not
recomputed the way the code computes it.

    item A: s1 n+=10k n-=90k   s2 n+=10k n-=10k      P_A=20k N_A=100k
    item B: s1 n+=40k n-=360k  s2 n+=0   n-=40k      P_B=40k N_B=400k

    t=1/2, alpha=1:
      v_A = 20k*(1/2) / ((1/2)*100k) = 1/5      v_B = 40k*(1/2) / ((1/2)*400k) = 1/10
      A_A = (20k/20k)^1 = 1                     A_B = (20k/40k)^1 = 1/2
      q+_A = 1, q-_A = 1/5                      q+_B = 1/2, q-_B = 1/20

      p~(A,s1) = 10k/(10k + 90k/5)  = 5/14      M(A,s1) = 10k * 1 * 9/14   = 45000/7
      p~(A,s2) = 10k/(10k + 10k/5)  = 5/6       M(A,s2) = 10k * 1 * 1/6    = 5000/3
      p~(B,s1) = 40k/(40k + 360k/10)= 10/19     M(B,s1) = 40k * 1/2 * 9/19 = 180000/19
      p~(B,s2) = 0                              M(B,s2) = 0
"""

from __future__ import annotations

import json

import pytest
import yaml
from typer.testing import CliRunner

from recsys_tfb.utils.hashing import HASH_BUCKETS
from scripts.sampling_budget_planner import (
    NO_ROWS_SHOWN,
    PROFILE_FORMAT_VERSION,
    app,
    Cell,
    Knobs,
    PlanError,
    cells_from_profile,
    config_problems,
    missing_entries,
    output_problems,
    override_entries,
    plan,
    ratio_literal,
    retention,
    self_checks,
    summary_lines,
    to_bucket,
    weight_entries,
    yaml_float,
)

HAND_CELLS = [
    Cell(values=("s1", "A"), item="A", n_pos=10_000, n_neg=90_000),
    Cell(values=("s2", "A"), item="A", n_pos=10_000, n_neg=10_000),
    Cell(values=("s1", "B"), item="B", n_pos=40_000, n_neg=360_000),
    Cell(values=("s2", "B"), item="B", n_pos=0, n_neg=40_000),
]
HAND_KNOBS = Knobs(t=0.5, alpha=1.0, gamma=1.0, eta=0.0, pos_floor=0.0)
HAND_M = {
    ("s1", "A"): 45000 / 7,
    ("s2", "A"): 5000 / 3,
    ("s1", "B"): 180000 / 19,
    ("s2", "B"): 0.0,
}
HAND_P_TILDE = {
    ("s1", "A"): 5 / 14,
    ("s2", "A"): 5 / 6,
    ("s1", "B"): 10 / 19,
    ("s2", "B"): 0.0,
}


def _by_values(result):
    return {c.cell.values: c for c in result.cells}


class TestTargetWeightsAndCellValue:
    """Steps 1-3: item-level target weights, model-world purchase rate, value."""

    def test_item_target_weights_match_the_hand_example(self):
        result = plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS)
        a, b = result.items["A"], result.items["B"]
        assert (a.P, a.N, b.P, b.N) == (20_000, 100_000, 40_000, 400_000)
        assert a.v == pytest.approx(1 / 5)
        assert b.v == pytest.approx(1 / 10)
        assert a.A == pytest.approx(1.0)
        assert b.A == pytest.approx(1 / 2)
        assert (a.q_pos, a.q_neg) == pytest.approx((1.0, 1 / 5))
        assert (b.q_pos, b.q_neg) == pytest.approx((1 / 2, 1 / 20))

    def test_cell_purchase_rate_and_value_match_the_hand_example(self):
        cells = _by_values(plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS))
        for values, m in HAND_M.items():
            assert cells[values].p_tilde == pytest.approx(HAND_P_TILDE[values])
            assert cells[values].M == pytest.approx(m)

    def test_value_is_the_same_whether_counted_from_positives_or_negatives(self):
        # M_c = n+ * value+ = n- * value- (step 3): the identity that makes
        # gamma=1 keep a cell's two classes 1:1.
        for c in plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS).cells:
            if c.cell.n_pos and c.cell.n_neg:
                assert c.cell.n_pos * c.value_pos == pytest.approx(c.M)
                assert c.cell.n_neg * c.value_neg == pytest.approx(c.M)


class TestRetentionAtAFixedLambda:
    """Steps 4-6 at lambda=1, where r_IS = M^gamma / n is a fraction on paper.

        gamma=1, lambda=1:
          r_IS+(A,s1) = 9/14   r_IS-(A,s1) = 1/14
          r_IS+(A,s2) = 1/6    r_IS-(A,s2) = 1/6
          r_IS+(B,s1) = 9/38   r_IS-(B,s1) = 1/38
                               r_IS-(B,s2) = 0       (no positives -> M = 0)
        eta=1/2: rbar_A = (90k/14 + 10k/6) / 100k = 17/210
                 rbar_B = (360k/38 + 0) / 400k   = 9/380
    """

    def _retention(self, **knob_overrides):
        knobs = Knobs(**{**HAND_KNOBS.__dict__, **knob_overrides})
        cells = plan(HAND_CELLS, budget=10**9, knobs=knobs).cells
        return {c.cell.values: r for c, r in zip(cells, retention(cells, lam=1.0, knobs=knobs))}

    def test_pure_value_rates_without_floors(self):
        r = self._retention(eta=0.0, pos_floor=0.0)
        assert r[("s1", "A")] == pytest.approx((9 / 14, 1 / 14))
        assert r[("s2", "A")] == pytest.approx((1 / 6, 1 / 6))
        assert r[("s1", "B")] == pytest.approx((9 / 38, 1 / 38))
        # No positives: no positive rate at all, and with eta=0 nothing left
        # for the negatives but the 1-bucket floor.
        assert r[("s2", "B")] == (None, 1 / HASH_BUCKETS)

    def test_positive_floor_and_negative_defensive_mixture(self):
        r = self._retention(eta=0.5, pos_floor=0.5)
        # r+ = max(f, min(1, r_IS+))
        assert r[("s1", "A")][0] == pytest.approx(9 / 14)
        assert r[("s2", "A")][0] == pytest.approx(0.5)
        assert r[("s1", "B")][0] == pytest.approx(0.5)
        # r- = (1 - eta) r_IS- + eta rbar_i
        assert r[("s1", "A")][1] == pytest.approx(1 / 28 + 17 / 420)   # 8/105
        assert r[("s2", "A")][1] == pytest.approx(1 / 12 + 17 / 420)   # 13/105
        assert r[("s1", "B")][1] == pytest.approx(1 / 76 + 9 / 760)    # 1/40
        assert r[("s2", "B")] == (None, pytest.approx(9 / 760))

    def test_rates_are_capped_at_one(self):
        knobs = Knobs(**{**HAND_KNOBS.__dict__, "eta": 0.0})
        cells = plan(HAND_CELLS, budget=10**9, knobs=knobs).cells
        r = dict(zip([c.cell.values for c in cells], retention(cells, lam=0.5, knobs=knobs)))
        assert r[("s1", "A")] == pytest.approx((1.0, 1 / 7))   # r_IS+ = 9/7 -> 1
        assert r[("s2", "A")] == pytest.approx((1 / 3, 1 / 3))

    def test_the_item_average_counts_a_capped_cell_at_its_cap(self):
        # lambda=1/10: r_IS-(A,s1) = 10/14, r_IS-(A,s2) = 10/6 -> kept whole.
        # rbar_A = (90k * 10/14 + 10k * 1) / 100k = 26/35, not the 17/21 an
        # uncapped average gives; it is what the item really keeps.
        knobs = Knobs(**{**HAND_KNOBS.__dict__, "eta": 0.5})
        cells = plan(HAND_CELLS, budget=10**9, knobs=knobs).cells
        r = dict(zip([c.cell.values for c in cells], retention(cells, lam=0.1, knobs=knobs)))
        assert r[("s1", "A")][1] == pytest.approx(0.5 * 10 / 14 + 0.5 * 26 / 35)   # 51/70
        assert r[("s2", "A")][1] == 1.0

    def test_gamma_takes_the_value_to_a_power_before_dividing(self):
        r = self._retention(gamma=0.5, eta=0.0, pos_floor=0.0)
        assert r[("s2", "A")] == pytest.approx(((5000 / 3) ** 0.5 / 10_000,) * 2)


# A less tidy pool for the properties: three items far apart in popularity,
# three strata whose purchase rates differ several-fold, one stratum where the
# coldest item never sold.
MIXED_CELLS = [
    Cell(values=(seg, item), item=item, n_pos=pos, n_neg=neg)
    for seg, item, pos, neg in [
        ("seg_1", "prod_A", 30_000, 970_000),
        ("seg_2", "prod_A", 12_000, 188_000),
        ("seg_3", "prod_A", 2_000, 498_000),
        ("seg_1", "prod_B", 4_000, 996_000),
        ("seg_2", "prod_B", 3_000, 197_000),
        ("seg_3", "prod_B", 500, 499_500),
        ("seg_1", "prod_C", 300, 999_700),
        ("seg_2", "prod_C", 150, 199_850),
        ("seg_3", "prod_C", 0, 500_000),
    ]
]
MIXED_POPULATION = sum(c.n_pos + c.n_neg for c in MIXED_CELLS)


def _continuous_total(result):
    return sum(c.cell.n_pos * (c.r_pos or 0) + c.cell.n_neg * (c.r_neg or 0)
               for c in result.cells)


def _uncapped(c, knobs):
    return (c.r_pos is not None and c.r_neg is not None
            and c.r_pos < 1 and c.r_neg < 1 and c.r_pos > knobs.pos_floor)


class TestLambda:
    """Step 7: lambda is the one threshold that spends the budget."""

    def test_hand_example_lambda_is_twice_the_total_value_over_the_budget(self):
        # gamma=1, eta=0, f=0 and nothing capped: each cell keeps M/lambda rows
        # of each class, so the budget is 2 * sum(M) / lambda — plus the one
        # bucket the never-sold (s2, B) keeps of its 40k negatives, 0.4 rows.
        result = plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS)
        floor_rows = 40_000 / HASH_BUCKETS
        assert result.lam == pytest.approx(2 * sum(HAND_M.values()) / (20_000 - floor_rows))
        cells = _by_values(result)
        assert cells[("s1", "A")].r_pos == pytest.approx(HAND_M[("s1", "A")] / (result.lam * 10_000))
        assert cells[("s1", "B")].r_neg == pytest.approx(HAND_M[("s1", "B")] / (result.lam * 360_000))

    @pytest.mark.parametrize("knobs", [
        Knobs(),
        Knobs(gamma=0.5, eta=0.5, pos_floor=0.0),
        Knobs(t=0.3, alpha=0.0, gamma=0.8, eta=0.0, pos_floor=0.9),
    ], ids=["defaults", "sqrt-gamma", "high-pos-floor"])
    def test_expected_rows_equal_the_budget(self, knobs):
        result = plan(MIXED_CELLS, budget=200_000, knobs=knobs)
        assert _continuous_total(result) == pytest.approx(200_000, rel=1e-9)
        # What the report prints: the bucketed rates. Rounding to the nearest
        # bucket moves each class by at most half a bucket.
        half_bucket_per_row = sum(c.n_pos + c.n_neg for c in MIXED_CELLS) * 0.5 / HASH_BUCKETS
        assert abs(_kept(result) - 200_000) <= half_bucket_per_row

    def test_classes_held_at_one_bucket_are_inside_the_budget(self):
        # seg_2 never sold: with eta=0 its 10M negatives keep only the
        # 1-bucket floor, 100 rows. Spent outside the solve, those 100 rows
        # would land on top of a 300-row budget.
        cells = [Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=1_000, n_neg=1_000_000),
                 Cell(values=("seg_2", "prod_A"), item="prod_A", n_pos=0, n_neg=10_000_000)]
        result = plan(cells, budget=300, knobs=Knobs(eta=0.0, pos_floor=0.0))
        half_bucket_per_row = 11_001_000 * 0.5 / HASH_BUCKETS
        assert abs(_kept(result) - 300) <= half_bucket_per_row

    def test_gamma_one_without_floors_keeps_both_classes_one_to_one(self):
        knobs = Knobs(gamma=1.0, eta=0.0, pos_floor=0.0)
        result = plan(MIXED_CELLS, budget=10_000, knobs=knobs)
        uncapped = [c for c in result.cells if _uncapped(c, knobs)]
        assert len(uncapped) >= 5
        for c in uncapped:
            assert c.cell.n_pos * c.r_pos == pytest.approx(c.cell.n_neg * c.r_neg)

    def test_gamma_half_keeps_rows_in_proportion_to_the_square_root_of_value(self):
        knobs = Knobs(gamma=0.5, eta=0.0, pos_floor=0.0)
        result = plan(MIXED_CELLS, budget=10_000, knobs=knobs)
        uncapped = [c for c in result.cells if _uncapped(c, knobs)]
        assert len(uncapped) >= 5
        ratios = [c.cell.n_pos * c.r_pos / c.M ** 0.5 for c in uncapped]
        assert ratios == pytest.approx([ratios[0]] * len(ratios))
        # ... and not in proportion to M itself, or the test could not tell.
        linear = [c.cell.n_pos * c.r_pos / c.M for c in uncapped]
        assert max(linear) / min(linear) > 2

    def test_defensive_mixture_leaves_each_items_negative_total_unchanged(self):
        # Two mixtures rather than none vs some: with eta=0 the never-sold cell
        # sits on the 1-bucket floor, and a floor is not value-driven.
        light = plan(MIXED_CELLS, budget=200_000, knobs=Knobs(eta=0.25, pos_floor=0.0))
        heavy = plan(MIXED_CELLS, budget=200_000, knobs=Knobs(eta=0.5, pos_floor=0.0))
        assert all(1 / HASH_BUCKETS < (c.r_neg or 0.5) < 1 for c in heavy.cells + light.cells)

        def neg_total(result, item):
            return sum(c.cell.n_neg * c.r_neg for c in result.cells
                       if c.cell.item == item and c.r_neg is not None)

        for item in ("prod_A", "prod_B", "prod_C"):
            assert neg_total(heavy, item) == pytest.approx(neg_total(light, item))
        # It does move negatives between cells: the zero-positive cell gains.
        zero_pos = ("seg_3", "prod_C")
        assert _by_values(heavy)[zero_pos].r_neg > _by_values(light)[zero_pos].r_neg

    def test_a_budget_at_least_the_population_keeps_everything(self):
        result = plan(MIXED_CELLS, budget=MIXED_POPULATION, knobs=Knobs())
        assert result.keeps_everything
        for c in result.cells:
            assert c.r_pos in (None, 1.0) and c.r_neg == 1.0

    def test_a_budget_below_the_floors_is_refused(self):
        # f * (all positives) alone is 0.6 * 51,950 = 31,170 rows.
        with pytest.raises(PlanError, match="budget"):
            plan(MIXED_CELLS, budget=30_000, knobs=Knobs(pos_floor=0.6))

    def test_a_budget_below_one_bucket_per_class_is_refused(self):
        # f=0, so only the 1-bucket floor is left: 5,000,000 rows / 100000.
        with pytest.raises(PlanError, match="budget"):
            plan(MIXED_CELLS, budget=40, knobs=Knobs(pos_floor=0.0))

    def test_the_most_rows_any_plan_can_keep_is_found_not_underestimated(self):
        # (10, 100) has value, (0, 1000) never sold; eta = 0.2. As lambda -> 0
        # the first cell is kept whole (110 rows) and the second keeps
        # eta * rbar = 0.2 * 100/1100 of its 1000 negatives: 128.18 rows.
        cells = [Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=10, n_neg=100),
                 Cell(values=("seg_2", "prod_A"), item="prod_A", n_pos=0, n_neg=1000)]
        knobs = Knobs(eta=0.2, pos_floor=0.0)
        assert plan(cells, budget=1100, knobs=knobs).max_reachable == pytest.approx(
            110 + 1000 * 0.2 * 100 / 1100)
        result = plan(cells, budget=120, knobs=knobs)
        assert _continuous_total(result) == pytest.approx(120, rel=1e-9)

    def test_a_budget_the_cells_cannot_absorb_is_reported_not_faked(self):
        # Every class with value already at 1; the zero-positive cell's
        # negatives can only ever reach eta * rbar <= eta. The rest of the
        # budget has nowhere to go.
        result = plan(MIXED_CELLS, budget=MIXED_POPULATION - 100, knobs=Knobs(eta=0.2))
        assert not result.keeps_everything
        assert result.max_reachable < MIXED_POPULATION - 100
        assert _continuous_total(result) == pytest.approx(result.max_reachable)


class TestBuckets:
    """Step 8: the framework keeps a row when crc32 % 100000 <
    int(ratio * 100000) — truncation, not rounding (``sampling.py``'s
    ``keep_rows_drawn_under_ratio``)."""

    def test_every_bucket_count_survives_yaml_and_the_pipelines_truncation(self):
        buckets = range(1, HASH_BUCKETS + 1)
        texts = [ratio_literal(b) for b in buckets]
        # One document, so the parser runs once rather than 100000 times.
        loaded_all = yaml.safe_load("[" + ", ".join(texts) + "]")
        for b, text, loaded in zip(buckets, texts, loaded_all, strict=True):
            assert isinstance(loaded, float), text
            # The pipeline hands float(ratio) to Spark through its repr.
            assert float(repr(loaded)) == loaded
            assert int(loaded * HASH_BUCKETS) == b, text

    def test_the_naive_literal_would_have_lost_buckets(self):
        # Why ratio_literal exists: b / 100000 written plainly truncates to
        # b - 1 for some b. If none did, the fallback above is untested.
        lost = [b for b in range(1, HASH_BUCKETS + 1)
                if int(float(f"{b / HASH_BUCKETS:.5f}") * HASH_BUCKETS) != b]
        assert lost

    @pytest.mark.parametrize("rate, bucket", [
        (0.0, 1),            # never 0 buckets: the class would vanish, HT breaks
        (1e-9, 1),
        (0.123454, 12345),
        (0.123456, 12346),
        (0.999999, HASH_BUCKETS),
        (1.0, HASH_BUCKETS),
    ])
    def test_rate_to_bucket_rounds_and_keeps_at_least_one(self, rate, bucket):
        assert to_bucket(rate) == bucket

    def test_plan_carries_buckets_and_the_rates_they_really_mean(self):
        cells = _by_values(plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS))
        zero_pos = cells[("s2", "B")]
        assert zero_pos.b_pos is None and zero_pos.r_eff_pos is None
        assert zero_pos.b_neg == 1                       # eta=0 left it 0
        assert zero_pos.r_eff_neg == 1 / HASH_BUCKETS
        a1 = cells[("s1", "A")]
        assert a1.b_pos == to_bucket(a1.r_pos)
        assert a1.r_eff_pos == a1.b_pos / HASH_BUCKETS


def _kept(result):
    return sum(c.cell.n_pos * (c.r_eff_pos or 0) + c.cell.n_neg * (c.r_eff_neg or 0)
               for c in result.cells)


class TestWeights:
    """Step 9: w = k * q_w / r_eff, k making the expected mean weight 1.

    Hand example, weight knobs = keep-rate knobs (t=1/2, alpha=1):
      sum(n * q) = A: 20k*1 + 100k*(1/5) = 40k;  B: 40k*(1/2) + 400k*(1/20) = 40k
    weight_t=1/4, weight_alpha=0:
      v_w_A = 20k*(3/4) / ((1/4)*100k) = 3/5   v_w_B = 40k*(3/4) / ((1/4)*400k) = 3/10
      sum(n * q_w) = A: 20k + 100k*(3/5) = 80k;  B: 40k + 400k*(3/10) = 160k
    """

    def test_hand_example_weights(self):
        result = plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS)
        assert result.k == pytest.approx(_kept(result) / 80_000)
        cells = _by_values(result)
        a1, b1, b2 = cells[("s1", "A")], cells[("s1", "B")], cells[("s2", "B")]
        assert a1.w_pos == pytest.approx(result.k * 1 / a1.r_eff_pos)
        assert a1.w_neg == pytest.approx(result.k * (1 / 5) / a1.r_eff_neg)
        assert b1.w_pos == pytest.approx(result.k * (1 / 2) / b1.r_eff_pos)
        assert b2.w_pos is None
        assert b2.w_neg == pytest.approx(result.k * (1 / 20) / (1 / HASH_BUCKETS))

    def test_weight_knobs_change_the_target_not_the_keep_rates(self):
        knobs = Knobs(**{**HAND_KNOBS.__dict__, "weight_t": 0.25, "weight_alpha": 0.0})
        result = plan(HAND_CELLS, budget=20_000, knobs=knobs)
        base = plan(HAND_CELLS, budget=20_000, knobs=HAND_KNOBS)
        assert [(c.b_pos, c.b_neg) for c in result.cells] == \
            [(c.b_pos, c.b_neg) for c in base.cells]
        a, b = result.weight_items["A"], result.weight_items["B"]
        assert (a.v, b.v, a.A, b.A) == pytest.approx((3 / 5, 3 / 10, 1.0, 1.0))
        assert result.k == pytest.approx(_kept(result) / 240_000)
        a1 = _by_values(result)[("s1", "A")]
        assert a1.w_neg == pytest.approx(result.k * (3 / 5) / a1.r_eff_neg)


GROUP_KEYS = ["seg", "label", "prod"]   # label in the middle on purpose
GROUP_KEYS_SPL = ["seg", "prod", "label"]   # the order _params() configures


class TestEntries:
    def test_a_cell_without_negatives_gets_no_negative_key(self):
        cells = [Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=10, n_neg=90),
                 Cell(values=("seg_2", "prod_A"), item="prod_A", n_pos=5, n_neg=0)]
        result = plan(cells, budget=50, knobs=Knobs())
        overrides = override_entries(result, GROUP_KEYS, "label")
        weights = weight_entries(result, GROUP_KEYS, "label")
        assert "seg_2|1|prod_A" in overrides and "seg_2|0|prod_A" not in overrides
        assert set(weights) == set(overrides)

    def test_keys_follow_sample_group_keys_with_the_label_component_in_place(self):
        cells = [Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=10, n_neg=90),
                 Cell(values=("seg_2", "prod_A"), item="prod_A", n_pos=0, n_neg=50)]
        result = plan(cells, budget=10**9, knobs=Knobs())
        overrides = override_entries(result, GROUP_KEYS, "label")
        weights = weight_entries(result, GROUP_KEYS, "label")
        assert set(overrides) == {"seg_1|1|prod_A", "seg_1|0|prod_A", "seg_2|0|prod_A"}
        assert set(weights) == set(overrides)
        assert overrides["seg_1|1|prod_A"] == "1.00000"


class TestMissingEntries:
    """Every key the snippets leave out is named, with why — so a reader can
    tell a key left out on purpose from one the config forgot."""

    def _missing(self, cells, group_keys=GROUP_KEYS_SPL, **kwargs):
        result = plan(cells, budget=200_000, knobs=Knobs())
        return missing_entries(result, override_entries(result, group_keys, "label"),
                               group_keys, "label", **kwargs)

    def test_a_class_with_no_rows_is_named(self):
        # seg_3 never bought prod_C: its positive key has nothing to keep.
        missing = self._missing(MIXED_CELLS)
        assert missing.one_class == [("seg_3|prod_C|1", "no positive rows")]
        assert (missing.no_rows, missing.keys) == (0, 1)

    def test_a_combination_with_no_rows_names_both_keys(self):
        # (seg_2, prod_B) is in neither class: seg_2 and prod_B each have rows
        # elsewhere, so the grid seg x prod has it, and the profile does not.
        cells = [c for c in MIXED_CELLS if c.values != ("seg_2", "prod_B")]
        missing = self._missing(cells, group_keys=GROUP_KEYS)   # label in the middle
        assert missing.one_class == [("seg_3|1|prod_C", "no positive rows")]
        assert missing.no_rows == 1
        assert missing.no_rows_shown == [("seg_2|0|prod_B", "seg_2|1|prod_B")]
        assert missing.keys == 3

    def test_a_declared_item_without_rows_is_a_combination_with_no_rows(self):
        missing = self._missing(MIXED_CELLS, extra_levels={
            "prod": ["prod_A", "prod_B", "prod_C", "prod_D"]})
        assert missing.no_rows == 3
        assert [neg for neg, _ in missing.no_rows_shown] == [
            "seg_1|prod_D|0", "seg_2|prod_D|0", "seg_3|prod_D|0"]
        assert missing.values_without_rows == {"prod": ["prod_D"]}

    def test_a_declared_item_without_rows_is_named_even_when_the_listing_is_full(self):
        # Cities nested in regions: crossing them makes 600 combinations that
        # cannot occur, which fill the listing before prod_Z's would come up.
        cells = [Cell(values=("prod_A", f"r{r}", f"c{r}{c:02d}"), item="prod_A",
                      n_pos=10, n_neg=90)
                 for r in range(5) for c in range(30)]
        missing = self._missing(cells, group_keys=["prod", "region", "city", "label"],
                                extra_levels={"prod": ["prod_A", "prod_Z"]})
        assert missing.no_rows == 2 * 5 * 150 - 150
        assert not any("prod_Z" in neg for neg, _ in missing.no_rows_shown)
        assert missing.values_without_rows == {"prod": ["prod_Z"]}

    def test_only_declared_values_can_be_without_rows(self):
        assert self._missing(MIXED_CELLS).values_without_rows == {}

    def test_every_key_of_the_grid_is_written_or_named(self):
        # The grid by hand: 3 segs x 4 items x 2 labels = 24 keys.
        cells = [c for c in MIXED_CELLS if c.values != ("seg_2", "prod_B")]
        result = plan(cells, budget=200_000, knobs=Knobs())
        overrides = override_entries(result, GROUP_KEYS_SPL, "label")
        missing = missing_entries(result, overrides, GROUP_KEYS_SPL, "label",
                                  extra_levels={"prod": ["prod_D"]})
        named = {k for k, _ in missing.one_class} | {
            k for pair in missing.no_rows_shown for k in pair}
        grid = {f"{s}|{p}|{y}" for s in ("seg_1", "seg_2", "seg_3")
                for p in ("prod_A", "prod_B", "prod_C", "prod_D") for y in "01"}
        assert set(overrides) | named == grid and not set(overrides) & named
        assert len(overrides) + missing.keys == 24

    def test_only_the_first_combinations_are_listed_but_all_are_counted(self):
        many = [f"prod_{i:03d}" for i in range(200)]
        missing = self._missing(MIXED_CELLS, extra_levels={"prod": many})
        assert missing.no_rows == 3 * 200          # every declared item is new
        assert len(missing.no_rows_shown) == NO_ROWS_SHOWN


class TestSelfChecks:
    """#525 acceptance 3: the checks the report prints, run on the emitted
    entries (the text that gets pasted), not on the plan's own floats."""

    @pytest.mark.parametrize("knobs", [
        Knobs(),
        Knobs(gamma=0.5, eta=0.5, weight_t=0.3, weight_alpha=0.8),
    ], ids=["defaults", "own-weight-knobs"])
    def test_all_pass_on_a_real_plan(self, knobs):
        result = plan(MIXED_CELLS, budget=200_000, knobs=knobs)
        checks = self_checks(
            result,
            override_entries(result, ["seg", "prod", "label"], "label"),
            weight_entries(result, ["seg", "prod", "label"], "label"),
            ["seg", "prod", "label"], "label",
        )
        assert [c.name for c in checks] == [
            "keep_rate_x_weight_equal_across_strata",
            "weighted_positive_rate_is_weight_t",
            "loss_share_follows_P_to_the_1_minus_weight_alpha",
            "mean_weight_is_one",
        ]
        assert all(c.passed for c in checks), [c for c in checks if not c.passed]

    def test_a_keep_rate_is_read_back_as_the_pipeline_truncates_it(self):
        # (b + 0.5) / 100000 is the fallback literal: as a float it is half a
        # bucket off b / 100000, truncated it is exactly b. The weights were
        # divided by b / 100000, so only the truncating reading agrees.
        keys = ["seg", "prod", "label"]
        result = plan(MIXED_CELLS, budget=200_000, knobs=Knobs())
        overrides = {
            key: f"{(round(float(text) * HASH_BUCKETS) + 0.5) / HASH_BUCKETS:.6f}"
            for key, text in override_entries(result, keys, "label").items()
        }
        checks = self_checks(result, overrides, weight_entries(result, keys, "label"),
                             keys, "label")
        assert all(c.passed for c in checks), [c for c in checks if not c.passed]

    @pytest.mark.parametrize("victims, failing", [
        # one stratum's negatives off: everything about that item moves
        (["seg_2|prod_B|0"], {0, 1, 2, 3}),
        # every positive of one item off, evenly: strata still agree
        (["seg_1|prod_A|1", "seg_2|prod_A|1", "seg_3|prod_A|1"], {1, 2, 3}),
        # one item's whole mass off, evenly: its positive rate still holds
        ([f"seg_{s}|prod_A|{y}" for s in (1, 2, 3) for y in (0, 1)], {2, 3}),
    ], ids=["one-stratum", "one-items-positives", "one-items-mass"])
    def test_a_wrong_weight_fails_exactly_its_checks(self, victims, failing):
        keys = ["seg", "prod", "label"]
        result = plan(MIXED_CELLS, budget=200_000, knobs=Knobs())
        weights = weight_entries(result, keys, "label")
        for key in victims:
            weights[key] *= 1.5
        checks = self_checks(result, override_entries(result, keys, "label"),
                             weights, keys, "label")
        assert {i for i, c in enumerate(checks) if not c.passed} == failing


class TestPlanRefuses:
    def test_an_item_without_positives(self):
        cells = MIXED_CELLS + [Cell(values=("seg_1", "prod_D"), item="prod_D",
                                    n_pos=0, n_neg=1000)]
        with pytest.raises(PlanError, match=r"prod_D.*no positive"):
            plan(cells, budget=200_000, knobs=Knobs())

    @pytest.mark.parametrize("knob, value", [
        ("gamma", 0.0), ("gamma", 1.01),
        ("eta", -0.1), ("eta", 1.1),
        ("pos_floor", -0.1), ("pos_floor", 1.1),
        ("t", 0.0), ("t", 1.0), ("weight_t", 1.0),
    ])
    def test_a_knob_out_of_range(self, knob, value):
        with pytest.raises(PlanError, match=knob):
            plan(MIXED_CELLS, budget=200_000, knobs=Knobs(**{knob: value}))

    @pytest.mark.parametrize("knob, value", [
        ("gamma", 1.0), ("eta", 0.0), ("eta", 1.0), ("pos_floor", 0.0), ("pos_floor", 1.0),
    ])
    def test_the_range_edges_are_allowed(self, knob, value):
        plan(MIXED_CELLS, budget=200_000, knobs=Knobs(**{knob: value}))


def _profile(cells, granularity=("seg", "prod"), **overrides):
    """A profile.json payload as scripts/profile_sample_pool.py writes it."""
    return {
        "format_version": PROFILE_FORMAT_VERSION,
        "generated_at": "2026-10-19T12:00:00",
        "source": "db.sample_pool",
        "granularity": list(granularity),
        "label_column": "label",
        "item_column": "prod",
        "time_column": "snap_date",
        "train_snap_dates": ["2025-01-31", "2025-02-28"],
        "skipped_weight_columns": [],
        "dropped_null_entity_rows": 0,
        "null_rows": {"rows": 0, "by_column": {}},
        "cells": [{"values": dict(zip(granularity, values)), "n_pos": p, "n_neg": n}
                  for values, p, n in cells],
        **overrides,
    }


def _params(**dataset_overrides):
    return {
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod", "label": "label"},
            "categorical_values": {"prod": ["prod_A", "prod_B", "prod_C"]},
        },
        "dataset": {
            "sample_group_keys": ["seg", "prod", "label"],
            "carry_columns": ["seg"],
            "train_snap_dates": ["2025-01-31", "2025-02-28"],
            "sample_ratio": 1.0,
            **dataset_overrides,
        },
        "training": {"algorithm_params": {"objective": "binary"}},
    }


class TestCellsFromProfile:
    def test_a_finer_profile_is_summed_up_to_the_sampling_cells(self):
        # Profiled with an extra weight-key column the planner does not use.
        profile = _profile([
            (("seg_1", "prod_A", "x"), 3, 30),
            (("seg_1", "prod_A", "y"), 2, 20),
            (("seg_2", "prod_A", "x"), 1, 10),
        ], granularity=("seg", "prod", "channel"))
        cells = cells_from_profile(profile, _params())
        assert sorted(cells, key=lambda c: c.values) == [
            Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=5, n_neg=50),
            Cell(values=("seg_2", "prod_A"), item="prod_A", n_pos=1, n_neg=10),
        ]

    def test_values_follow_sample_group_keys_order_not_the_profiles(self):
        profile = _profile([(("prod_A", "seg_1"), 3, 30)], granularity=("prod", "seg"))
        assert cells_from_profile(profile, _params())[0].values == ("seg_1", "prod_A")

    def test_a_profile_too_coarse_for_the_sampling_cells_is_refused(self):
        profile = _profile([(("prod_A",), 3, 30)], granularity=("prod",))
        with pytest.raises(PlanError, match=r"seg.*rerun.*profile"):
            cells_from_profile(profile, _params())

    def test_an_unknown_format_version_is_refused(self):
        with pytest.raises(PlanError, match="format_version"):
            cells_from_profile(_profile([], format_version=99), _params())


class TestConfigProblems:
    """#525 "檢查": refusals come back as errors, the rest as warnings."""

    def _problems(self, params=None, profile=None):
        return config_problems(params or _params(), profile or _profile([]))

    def test_a_clean_config_has_no_problems(self):
        assert self._problems() == ([], [])

    @pytest.mark.parametrize("group_keys, missing", [
        (["seg", "prod"], "label"),
        (["seg", "label"], "prod"),
    ])
    def test_label_and_item_must_be_sampling_keys(self, group_keys, missing):
        errors, _ = self._problems(_params(sample_group_keys=group_keys))
        assert any(missing in e and "sample_group_keys" in e for e in errors), errors

    def test_a_stratum_that_cannot_be_a_weight_key_is_refused(self):
        # seg drops out of carry_columns: train model_input would not carry it,
        # so a weight keyed on it would silently stay 1.0 (A9a).
        errors, _ = self._problems(_params(carry_columns=[]))
        assert any("A9a" in e and "seg" in e and "carry_columns" in e for e in errors), errors

    def test_changed_train_months_warn(self):
        _, warnings = self._problems(_params(train_snap_dates=["2025-01-31"]))
        assert any("train_snap_dates" in w and "profile" in w for w in warnings), warnings

    def test_a_ranking_objective_warns(self):
        params = _params()
        params["training"]["algorithm_params"]["objective"] = "lambdarank"
        _, warnings = self._problems(params)
        assert any("lambdarank" in w and "pointwise" in w for w in warnings), warnings

    @pytest.mark.parametrize("ratio, warns", [(0.5, True), (1.0, False), (None, False)])
    def test_a_zero_positive_group_draw_warns(self, ratio, warns):
        extra = {} if ratio is None else {"train_zero_positive_group_ratio": ratio}
        _, warnings = self._problems(_params(**extra))
        assert any("train_zero_positive_group_ratio" in w for w in warnings) == warns

    def test_null_rows_in_the_profile_are_repeated(self):
        profile = _profile([], null_rows={"rows": 1234, "by_column": {"seg": 1234}})
        _, warnings = self._problems(profile=profile)
        assert any("1,234" in w and "seg" in w for w in warnings), warnings


class TestOutputProblems:
    def test_an_item_outside_the_declared_list_is_refused_on_both_tables(self):
        cells = [Cell(values=("seg_1", "prod_Z"), item="prod_Z", n_pos=5, n_neg=50)]
        result = plan(cells, budget=10**9, knobs=Knobs())
        keys = ["seg", "prod", "label"]
        errors = output_problems(_params(), override_entries(result, keys, "label"),
                                 weight_entries(result, keys, "label"))
        assert any("A5" in e and "prod_Z" in e for e in errors), errors
        assert any("A9c" in e and "prod_Z" in e for e in errors), errors

    def test_known_items_pass(self):
        result = plan(MIXED_CELLS, budget=200_000, knobs=Knobs())
        keys = ["seg", "prod", "label"]
        assert output_problems(_params(), override_entries(result, keys, "label"),
                               weight_entries(result, keys, "label")) == []


class TestSummary:
    def test_reports_the_draws_standard_deviation(self):
        # One item, every row positive: nothing has value, so the positives
        # sit on the 0.5 floor. 100,000 rows each kept with probability 1/2:
        # sd = sqrt(100,000 * 0.5 * 0.5) = 158.1.
        cells = [Cell(values=("seg_1", "prod_A"), item="prod_A", n_pos=100_000, n_neg=0)]
        result = plan(cells, budget=50_000, knobs=Knobs(pos_floor=0.5))
        line = next(x for x in summary_lines(result) if x.startswith("expected rows"))
        assert "standard deviation ≈ 158 rows" in line

    def test_counts_the_classes_each_limit_decided(self):
        # Hand example, eta=0, f=0: only the never-sold (s2, B) negatives are
        # held at one bucket.
        line = next(x for x in summary_lines(plan(HAND_CELLS, 20_000, HAND_KNOBS))
                    if x.startswith("cell-classes"))
        assert line.endswith("capped at 1 0, positive floor 0, 1-bucket floor 1")
        # f=1/2, budget 40k: if every positive sits on the floor they take
        # 0.5 * 60k = 30k rows, leaving 10k - 0.4 for the negatives' M/lambda,
        # so lambda = sum(M) / 9,999.6 = 1.757. Every pure-value positive rate
        # (9/14, 1/6, 9/38 divided by lambda) is then under 1/2: all three
        # positive classes are held by the floor.
        knobs = Knobs(**{**HAND_KNOBS.__dict__, "pos_floor": 0.5})
        result = plan(HAND_CELLS, 40_000, knobs)
        assert result.lam == pytest.approx(sum(HAND_M.values()) / (10_000 - 0.4))
        line = next(x for x in summary_lines(result) if x.startswith("cell-classes"))
        assert "positive floor 3" in line and "1-bucket floor 1" in line

    def test_keeping_everything_has_no_spread(self):
        result = plan(MIXED_CELLS, budget=MIXED_POPULATION, knobs=Knobs())
        line = next(x for x in summary_lines(result) if x.startswith("expected rows"))
        assert "standard deviation ≈ 0 rows" in line


class TestYamlFloat:
    @pytest.mark.parametrize("x", [1.0, 0.5, 1e-05, 3.2e-07, 12345.678, 1e+20, 2.5e-300])
    def test_every_weight_reads_back_as_the_same_float(self, x):
        loaded = yaml.safe_load(f"k: {yaml_float(x)}")["k"]
        assert isinstance(loaded, float) and loaded == x


def _write_conf(root, params=None, *, env="dev", env_params=None):
    """A ``conf/`` tree under ``root`` the way the pipelines read it: ``params``
    as conf/base plus the ``env`` overlay (``env_params``, {file stem: content})."""
    params = params or _params()
    base = root / "conf" / "base"
    base.mkdir(parents=True, exist_ok=True)
    (base / "parameters.yaml").write_text(yaml.safe_dump({"schema": params["schema"]}))
    (base / "parameters_dataset.yaml").write_text(yaml.safe_dump({"dataset": params["dataset"]}))
    (base / "parameters_training.yaml").write_text(
        yaml.safe_dump({"training": params["training"]}))
    overlay = root / "conf" / env
    overlay.mkdir(exist_ok=True)
    for stem, content in (env_params or {}).items():
        (overlay / f"{stem}.yaml").write_text(yaml.safe_dump(content))


# conf/<env> naming only the train months: the rest stays conf/base's.
ONE_MONTH_OVERLAY = {"parameters_dataset": {"dataset": {"train_snap_dates": ["2025-01-31"]}}}


def _mixed_profile():
    return _profile([(c.values, c.n_pos, c.n_neg) for c in MIXED_CELLS])


class TestCli:
    """``plan`` end to end: a fake profile.json and a conf/ tree in, files out.
    Run from ``tmp_path``: conf/ is read from the working directory, as the
    pipelines read it."""

    @pytest.fixture(autouse=True)
    def _in_tmp(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

    def _run(self, tmp_path, *extra, params=None, profile=None, out="out", budget=200_000,
             env_params=None, env="dev", env_args=("--env", "dev")):
        profile_path = tmp_path / "profile.json"
        profile_path.write_text(json.dumps(profile or _mixed_profile()))
        _write_conf(tmp_path, params, env=env, env_params=env_params)
        return CliRunner().invoke(app, [
            "plan", str(profile_path), "--budget", str(budget),
            "--out-dir", str(tmp_path / out), *env_args, *extra,
        ])

    def test_reads_conf_base_with_the_env_merged_over_it(self, tmp_path):
        # conf/dev sets only the train months. They are read (the profile
        # counted two months, so the planner warns), and so is everything
        # conf/dev leaves out: base's sample_group_keys still give 17 entries.
        result = self._run(tmp_path, env_params=ONE_MONTH_OVERLAY)
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "dataset.train_snap_dates ['2025-01-31'] differs" in result.stderr
        ratio_text = (tmp_path / "out" / "sample_ratio_overrides.yaml").read_text()
        assert len(yaml.safe_load("dataset:\n" + ratio_text)["dataset"]
                   ["sample_ratio_overrides"]) == 17

    def test_an_env_without_a_conf_directory_is_refused(self, tmp_path):
        # A typo must not quietly plan on conf/base alone (A30, as the pipelines).
        result = self._run(tmp_path, env_args=("-e", "dve"))
        assert result.exit_code == 1
        assert "(A30) --env='dve'" in result.stderr
        assert not (tmp_path / "out").exists()

    def test_a_train_month_range_is_compared_as_the_months_it_stands_for(self, tmp_path):
        params = _params(train_snap_dates={"start": "2025-01-31", "end": "2025-02-28",
                                           "step": "month_end"})
        result = self._run(tmp_path, params=params)
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "differs" not in result.stderr

    def test_env_defaults_to_local_as_for_the_pipelines(self, tmp_path):
        result = self._run(tmp_path, env="local", env_params=ONE_MONTH_OVERLAY, env_args=())
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "dataset.train_snap_dates ['2025-01-31'] differs" in result.stderr

    @staticmethod
    def _headers(tmp_path):
        return ["".join(line for line in (tmp_path / "out" / name).read_text().splitlines(True)
                        if line.startswith("#"))
                for name in ("sample_ratio_overrides.yaml", "sample_weights.yaml")]

    def test_keys_left_out_are_said_in_both_snippets_the_report_and_the_terminal(
            self, tmp_path):
        # seg_3 never bought prod_C: one key. prod_D is declared but has no
        # rows at all: three combinations, two keys each.
        params = _params()
        params["schema"]["categorical_values"]["prod"].append("prod_D")
        result = self._run(tmp_path, params=params)
        assert result.exit_code == 0, result.stdout + result.stderr
        counts = "1 from cells with one class only, 6 from 3 combination(s) with no rows at all"
        for header in self._headers(tmp_path):
            assert "# 7 key(s) have no entry: sample_pool has no such rows" in header, header
            assert f"{counts}; report.md lists them." in header
            assert "# Declared value(s) with no rows at all: prod: prod_D." in header
            assert ("# A row drawn under such a key is kept at dataset.sample_ratio; a row "
                    "trained under one is weighted 1.0 (as when its label_table label "
                    "differs from sample_pool's).") in header
        report = (tmp_path / "out" / "report.md").read_text()
        section = report.split("## Keys with no entry\n")[1].split("\n## ")[0]
        assert "- `prod`: prod_D\n" in section
        assert "- `seg_3|prod_C|1` — no positive rows\n" in section
        assert "- `seg_1|prod_D|0`, `seg_1|prod_D|1` — no rows at all\n" in section
        assert f"keys with no entry: 7 — {counts}; report.md lists them" in result.stdout
        assert "declared value(s) with no rows at all: prod: prod_D" in result.stdout

    def test_when_no_key_is_left_out_the_snippets_say_so(self, tmp_path):
        full = _profile([(c.values, c.n_pos or 1, c.n_neg) for c in MIXED_CELLS])
        result = self._run(tmp_path, profile=full)
        assert result.exit_code == 0, result.stdout + result.stderr
        for header in self._headers(tmp_path):
            assert "# Every key of every combination has an entry.\n" in header, header
            assert "no entry" not in header and "no rows" not in header
        assert "keys with no entry: 0\n" in result.stdout

    def test_a_listing_cut_short_says_so_in_the_headers(self, tmp_path):
        params = _params()
        params["schema"]["categorical_values"]["prod"] += [f"prod_{i:03d}" for i in range(40)]
        result = self._run(tmp_path, params=params)
        assert result.exit_code == 0, result.stdout + result.stderr
        for header in self._headers(tmp_path):
            assert (f"report.md lists them, the first {NO_ROWS_SHOWN} combinations only."
                    in header), header
            # The first ten of the 40 declared items without rows, then a count.
            assert "prod: prod_000, prod_001," in header and "prod_009 and 30 more." in header

    def test_an_item_list_counted_from_the_data_adds_no_declared_items(self, tmp_path):
        # #379: no list is declared, so only the profile's items make the grid.
        params = _params()
        params["schema"]["categorical_values"]["prod"] = "from_train_data"
        result = self._run(tmp_path, params=params)
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "keys with no entry: 1 — 1 from cells with one class only" in result.stdout

    def test_writes_two_pasteable_snippets_and_a_report(self, tmp_path):
        result = self._run(tmp_path)
        assert result.exit_code == 0, result.stdout + result.stderr
        out = tmp_path / "out"
        # Pasted under the section they belong to, exactly as the header says.
        ratio_text = (out / "sample_ratio_overrides.yaml").read_text()
        weight_text = (out / "sample_weights.yaml").read_text()
        assert "under `dataset:`" in ratio_text and "under `training:`" in weight_text
        dataset = yaml.safe_load("dataset:\n" + ratio_text)["dataset"]
        training = yaml.safe_load("training:\n" + weight_text)["training"]
        overrides = dataset["sample_ratio_overrides"]
        assert training["sample_weight_keys"] == ["seg", "prod", "label"]
        assert set(training["sample_weights"]) == set(overrides)
        # 9 cells, one of them without positives: 17 entries, none left to
        # sample_ratio.
        assert len(overrides) == 17
        assert all(isinstance(v, float) for v in overrides.values())
        # Same numbers the plan computed.
        expected = plan(MIXED_CELLS, budget=200_000, knobs=Knobs())
        assert overrides == {k: float(v) for k, v in
                             override_entries(expected, GROUP_KEYS_SPL, "label").items()}
        assert training["sample_weights"] == pytest.approx(
            weight_entries(expected, GROUP_KEYS_SPL, "label"))

    def test_headers_record_every_input(self, tmp_path):
        self._run(tmp_path, "--t", "0.2", "--weight-alpha", "0.3")
        ratio_text = (tmp_path / "out" / "sample_ratio_overrides.yaml").read_text()
        weight_text = (tmp_path / "out" / "sample_weights.yaml").read_text()
        for text in (ratio_text, weight_text):
            header = "".join(line for line in text.splitlines(True) if line.startswith("#"))
            for needle in ("profile.json", "200,000", "t=0.2", "alpha=0.5", "gamma=1.0",
                           "eta=0.2", "pos_floor=0.6"):
                assert needle in header, (needle, header)
        assert "weight_t=0.2" in weight_text and "weight_alpha=0.3" in weight_text

    def test_changing_only_the_weight_knobs_leaves_the_overrides_byte_identical(self, tmp_path):
        self._run(tmp_path, out="a")
        self._run(tmp_path, "--weight-t", "0.3", "--weight-alpha", "0.9", out="b")
        a, b = tmp_path / "a", tmp_path / "b"
        assert (a / "sample_ratio_overrides.yaml").read_bytes() == \
            (b / "sample_ratio_overrides.yaml").read_bytes()
        assert (a / "sample_weights.yaml").read_bytes() != (b / "sample_weights.yaml").read_bytes()

    def test_report_has_the_summary_the_item_table_and_passing_self_checks(self, tmp_path):
        result = self._run(tmp_path)
        report = (tmp_path / "out" / "report.md").read_text()
        for needle in ("λ", "k", "budget", "| item |", "prod_A", "prod_C",
                       "neg:pos", "capped at 1", "positive floor", "1-bucket floor"):
            assert needle in report, needle
        assert report.count("[PASS]") == 4 and "[FAIL]" not in report
        assert "self-checks: 4/4 passed" in result.stdout

    def test_a_budget_covering_the_population_is_noted(self, tmp_path):
        result = self._run(tmp_path, budget=MIXED_POPULATION)
        assert result.exit_code == 0, result.stderr
        assert "every keep-rate is 1" in (tmp_path / "out" / "report.md").read_text()

    def test_a_refusal_exits_non_zero_and_writes_no_snippet(self, tmp_path):
        result = self._run(tmp_path, params=_params(sample_group_keys=["seg", "prod"]))
        assert result.exit_code == 1
        assert "label" in result.stderr and "sample_group_keys" in result.stderr
        assert not (tmp_path / "out" / "sample_ratio_overrides.yaml").exists()

    def test_an_infeasible_budget_exits_non_zero(self, tmp_path):
        result = self._run(tmp_path, budget=100)
        assert result.exit_code == 1 and "budget" in result.stderr
        assert not (tmp_path / "out" / "sample_ratio_overrides.yaml").exists()

    def test_an_unknown_item_exits_non_zero(self, tmp_path):
        profile = _profile([(("seg_1", "prod_Z"), 5, 50)])
        result = self._run(tmp_path, profile=profile)
        assert result.exit_code == 1 and "prod_Z" in result.stderr

    def test_warnings_reach_the_terminal_and_the_report(self, tmp_path):
        params = _params()
        params["training"]["algorithm_params"]["objective"] = "lambdarank"
        result = self._run(tmp_path, params=params)
        assert result.exit_code == 0
        assert "lambdarank" in result.stderr
        assert "lambdarank" in (tmp_path / "out" / "report.md").read_text()

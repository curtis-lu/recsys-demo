"""Tests for evaluation.metrics — numpy-only HPO primitives.

Scope: only ``compute_ap`` + ``compute_mean_ap``. The dict-shaped
``compute_all_metrics`` and all per-dimension helpers have moved to
``recsys_tfb.evaluation.metrics_spark``; see ``test_metrics_spark.py``.
"""

import inspect

import numpy as np
import pytest

from recsys_tfb.evaluation.metrics import (
    align_positive_row_weights,
    compute_ap,
    compute_macro_per_item_map,
    compute_mean_ap,
    macro_from_per_item,
    positive_row_contributions,
)


def _items(groups: np.ndarray) -> np.ndarray:
    """A distinct item per row, for fixtures whose scores never tie inside a
    group — the item only breaks ties, so its values do not matter there."""
    return np.arange(len(groups)).astype(str)


class TestComputeAP:
    def test_known_values(self):
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        ap = compute_ap(y_true, y_score)
        # Precision at pos 1: 1/1=1.0, pos 3: 2/3
        # AP = (1.0 + 2/3) / 2 = 5/6
        assert ap == pytest.approx(5 / 6)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) is None

    def test_all_positives(self):
        y_true = np.array([1, 1, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) == pytest.approx(1.0)

    def test_worst_case(self):
        y_true = np.array([0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        # Only positive at rank 3: precision@3 = 1/3
        assert compute_ap(y_true, y_score) == pytest.approx(1 / 3)


class TestComputeMeanAP:
    def test_two_groups_mixed(self):
        # group 0: y=[1,0,1,0], score=[0.9,0.8,0.7,0.6] → AP = 5/6
        # group 1: y=[0,0,1], score=[0.9,0.8,0.7] → AP = 1/3
        groups = np.array([0, 0, 0, 0, 1, 1, 1])
        y_true = np.array([1, 0, 1, 0, 0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7])
        expected = (5 / 6 + 1 / 3) / 2
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == pytest.approx(expected)

    def test_skips_no_positive_group(self):
        # group 0: AP = 1.0  (single positive at top)
        # group 1: no positives → skipped
        # group 2: AP = 1.0  (single positive at top)
        groups = np.array([0, 0, 1, 1, 2, 2])
        y_true = np.array([1, 0, 0, 0, 1, 0])
        y_score = np.array([0.9, 0.1, 0.5, 0.4, 0.9, 0.1])
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == pytest.approx(1.0)

    def test_all_no_positive_returns_zero(self):
        groups = np.array([0, 0, 1, 1])
        y_true = np.array([0, 0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == 0.0

    def test_single_group_equals_single_ap(self):
        groups = np.array([7, 7, 7, 7])
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == pytest.approx(
            compute_ap(y_true, y_score)
        )

    def test_empty_inputs_return_zero(self):
        groups = np.array([], dtype=np.int64)
        y_true = np.array([], dtype=np.int64)
        y_score = np.array([], dtype=np.float64)
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == 0.0

    def test_ties_within_a_group_are_broken_by_item(self):
        """Tied scores rank by item ascending — the rule the Spark metrics rank
        with (``utils.ranking``) — not by where the rows sit in the input.

        One group, two tied pairs, items chosen so that input order and item
        order disagree:
          score 0.9: "b"(y=1), "a"(y=0)  → item order a, b
          score 0.5: "d"(y=1), "c"(y=0)  → item order c, d

        sorted y   = [0, 1, 0, 1]
        precisions = [0, 1/2, 1/3, 1/2]
        AP = (1/2 + 1/2) / 2 = 1/2   (input order would give 5/6)
        """
        groups = np.array([0, 0, 0, 0])
        items = np.array(["b", "a", "d", "c"])
        y_score = np.array([0.9, 0.9, 0.5, 0.5])
        y_true = np.array([1, 0, 1, 0])
        assert compute_mean_ap(groups, items, y_true, y_score) == pytest.approx(0.5)

    def test_groups_unsorted_input(self):
        """Group ids in input do not need to be contiguous or sorted — the
        impl re-orders internally. Interleaved (group, score) input must
        yield the same AP as the contiguous-group form."""
        # Same data as test_two_groups_mixed but interleaved by group.
        # group 0 rows: y=[1,0,1,0], score=[0.9,0.8,0.7,0.6] → AP = 5/6
        # group 1 rows: y=[0,0,1],   score=[0.9,0.8,0.7]    → AP = 1/3
        groups = np.array([0, 1, 0, 1, 0, 1, 0])
        y_true = np.array([1, 0, 0, 0, 1, 1, 0])
        y_score = np.array([0.9, 0.9, 0.8, 0.8, 0.7, 0.7, 0.6])
        expected = (5 / 6 + 1 / 3) / 2
        assert compute_mean_ap(groups, _items(groups), y_true, y_score) == pytest.approx(expected)

    def test_random_many_groups_matches_naive_reference(self):
        """Random multi-group correctness check at moderate scale.

        Verifies the vectorized impl matches a slow per-group naive impl on
        random inputs with **distinct** scores (so tie-breaking is moot)."""
        rng = np.random.default_rng(42)
        n_groups = 200
        group_sizes = rng.integers(5, 30, size=n_groups)
        groups = np.repeat(np.arange(n_groups), group_sizes)
        n_rows = int(groups.shape[0])
        y_true = rng.integers(0, 2, size=n_rows).astype(np.int64)
        y_score = rng.uniform(size=n_rows)  # distinct floats — no ties

        expected_aps: list[float] = []
        for g in np.unique(groups):
            mask = groups == g
            y_g, s_g = y_true[mask], y_score[mask]
            if y_g.sum() == 0:
                continue
            order = np.argsort(-s_g)
            y_sorted = y_g[order]
            cumsum = np.cumsum(y_sorted)
            pos = np.arange(1, len(y_sorted) + 1)
            precisions = cumsum / pos
            expected_aps.append(
                float(np.sum(precisions * y_sorted) / np.sum(y_g))
            )
        expected = float(np.mean(expected_aps)) if expected_aps else 0.0

        actual = compute_mean_ap(groups, _items(groups), y_true, y_score)
        assert actual == pytest.approx(expected, rel=1e-12)


class TestComputeMacroPerItemMap:
    # Two customers, three products (mirrors the metrics_spark
    # _two_customer_raw fixture so the parity test shares the math):
    #   C0: A(0.9,1) B(0.5,0) C(0.1,1)  ranking A,B,C -> prec A=1.0, C=2/3
    #   C1: B(0.8,1) C(0.6,0) A(0.3,0)  ranking B,C,A -> prec B=1.0
    # per-item map_attr@all: A=1.0, B=1.0, C=2/3
    # macro = (1.0 + 1.0 + 2/3) / 3 = 8/9
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_full_map_macro_over_items(self):
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(8 / 9)

    def test_k_truncation_zeros_contrib_beyond_k(self):
        # k=1: C0 A pos1 -> 1.0, C pos3 -> 0.0 ; C1 B pos1 -> 1.0
        # per-item: A=1.0, B=1.0, C=0.0 ; macro = 2/3
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, k=1
        )
        assert result == pytest.approx(2 / 3)

    def test_skips_group_with_no_positives(self):
        # group 2 has no positives -> contributes nothing; A and C each 1.0
        groups = np.array([0, 0, 1, 1, 2, 2])
        items = np.array(["A", "C", "A", "C", "A", "C"])
        y = np.array([1, 0, 0, 1, 0, 0])
        score = np.array([0.9, 0.1, 0.4, 0.5, 0.9, 0.1])
        # C0: A(0.9,1) C(0.1,0) -> A prec 1.0 ; C1: C(0.5,1) A(0.4,0) -> C prec 1.0
        # per-item: A=1.0, C=1.0 ; macro = 1.0
        result = compute_macro_per_item_map(groups, items, y, score)
        assert result == pytest.approx(1.0)

    def test_all_no_positives_returns_zero(self):
        groups = np.array([0, 0, 1, 1])
        items = np.array(["A", "B", "A", "B"])
        y = np.array([0, 0, 0, 0])
        score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_macro_per_item_map(groups, items, y, score) == 0.0

    def test_empty_inputs_return_zero(self):
        empty = np.array([], dtype=np.int64)
        assert (
            compute_macro_per_item_map(
                empty, np.array([]), empty, np.array([], dtype=np.float64)
            )
            == 0.0
        )


class TestMacroFromPerItem:
    # 兩個 item：A 值 0.75、n=2；B 值 1.0、n=1。pooled = (2*0.75+1*1.0)/3 = 5/6
    VALUES = np.array([0.75, 1.0])
    N_POS = np.array([2, 1])

    def test_defaults_equal_plain_mean(self):
        assert macro_from_per_item(self.VALUES, self.N_POS) == pytest.approx(0.875)

    def test_weight_alpha_one_weights_by_n_pos(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, weight_alpha=1.0)
        assert r == pytest.approx(5 / 6)

    def test_min_positives_drops_cold_item(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, min_positives=2)
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_none(self):
        assert macro_from_per_item(self.VALUES, self.N_POS, min_positives=3) is None

    def test_shrinkage_known_value(self):
        # pooled=5/6；A'=(2*0.75+5/6)/3=7/9；B'=(1.0+5/6)/2=11/12；mean=61/72
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1.0)
        assert r == pytest.approx(61 / 72)

    def test_shrinkage_large_k_approaches_pooled(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1e9)
        assert r == pytest.approx(5 / 6, abs=1e-6)


class TestComputeMacroPerItemMapParams:
    # 3 queries、2 items。A 正例 2 列（contrib 1.0、0.5 → AP 0.75）、B 1 列（1.0）
    GROUPS = np.array([0, 0, 1, 1, 2, 2])
    ITEMS = np.array(["A", "B", "A", "B", "A", "B"])
    Y = np.array([1, 0, 1, 0, 0, 1])
    SCORE = np.array([0.9, 0.1, 0.1, 0.9, 0.1, 0.9])

    def test_defaults_unchanged(self):
        r = compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)
        assert r == pytest.approx(0.875)

    def test_weight_alpha(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weight_alpha=1.0
        )
        assert r == pytest.approx(5 / 6)

    def test_min_positives(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=2
        )
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_zero(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=3
        )
        assert r == 0.0

    def test_shrinkage(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, shrinkage_k=1.0
        )
        assert r == pytest.approx(61 / 72)


class TestWeightedMap:
    """Optional query-level ``weights`` (Horvitz–Thompson inclusion weights).

    Same fixture as ``TestComputeMacroPerItemMapParams``:
      q0: A(0.9,y=1) B(0.1,y=0) -> A prec@1 = 1.0
      q1: A(0.1,y=1) B(0.9,y=0) -> A prec@2 = 0.5
      q2: A(0.1,y=0) B(0.9,y=1) -> B prec@1 = 1.0
    unweighted per-item: A = (1.0+0.5)/2 = 0.75, B = 1.0 ; macro = 0.875
    """

    GROUPS = np.array([0, 0, 1, 1, 2, 2])
    ITEMS = np.array(["A", "B", "A", "B", "A", "B"])
    Y = np.array([1, 0, 1, 0, 0, 1])
    SCORE = np.array([0.9, 0.1, 0.1, 0.9, 0.1, 0.9])

    @staticmethod
    def _duplicate_query(groups, items, y, score, query_id, times):
        """Append ``times - 1`` verbatim copies of ``query_id``'s rows as NEW
        queries. Cloning a query into a fresh group id (rather than growing the
        existing one) is what a duplicated *sampling unit* means: within-query
        ranking is untouched, only the query's multiplicity changes."""
        mask = groups == query_id
        next_gid = int(groups.max()) + 1
        g, it, yy, sc = [groups], [items], [y], [score]
        for j in range(times - 1):
            g.append(np.full(int(mask.sum()), next_gid + j, dtype=groups.dtype))
            it.append(items[mask])
            yy.append(y[mask])
            sc.append(score[mask])
        return (
            np.concatenate(g), np.concatenate(it),
            np.concatenate(yy), np.concatenate(sc),
        )

    # --- 1. backward compatibility: omitted == explicit None (exact) ---

    def test_omitted_weights_bit_identical_to_none(self):
        a = compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)
        b = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=None
        )
        assert a == b  # exact, not approx — the None path must not be rerouted

    def test_omitted_weights_bit_identical_to_none_with_params(self):
        kwargs = dict(k=2, weight_alpha=1.0, min_positives=1, shrinkage_k=1.0)
        a = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, **kwargs
        )
        b = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=None, **kwargs
        )
        assert a == b

    def test_none_path_never_materializes_a_weight_vector(self, monkeypatch):
        """The ``weights=None`` path must run the ORIGINAL unweighted
        aggregation, not ``np.ones``-filled weighted aggregation.

        Why this is a structural test and not a numeric one: on any small
        fixture ``contrib * 1.0`` is exact and ``np.bincount`` accumulates in
        the same order, so an ``np.ones`` fill-in produces bit-identical
        output here and NO equality assertion can detect it. Verified: swapping
        the None branch for an ``np.ones`` reroute leaves every numeric test in
        this class green. The contract we actually need to defend is that the
        main metric path (shared with HPO and the Spark parity tests) keeps its
        exact float op-order, so we assert on the op-order itself — the
        per-item count must come from an UNWEIGHTED ``np.bincount``.
        """
        import recsys_tfb.evaluation.metrics as metrics_mod

        real_bincount = np.bincount
        seen_weights = []

        def spy(x, weights=None, minlength=0):
            seen_weights.append(weights)
            return real_bincount(x, weights=weights, minlength=minlength)

        monkeypatch.setattr(metrics_mod.np, "bincount", spy)
        compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)

        assert seen_weights, "expected the aggregation to use np.bincount"
        assert any(w is None for w in seen_weights), (
            "weights=None must reach an unweighted np.bincount; an np.ones "
            "fill-in silently reroutes the main metric path through float "
            "weighting and can shift results in the last ulp"
        )

    def test_weighted_path_does_weight_every_bincount(self, monkeypatch):
        """Mirror of the above: when weights ARE supplied, neither the
        numerator nor the denominator may fall back to an unweighted count."""
        import recsys_tfb.evaluation.metrics as metrics_mod

        real_bincount = np.bincount
        seen_weights = []

        def spy(x, weights=None, minlength=0):
            seen_weights.append(weights)
            return real_bincount(x, weights=weights, minlength=minlength)

        monkeypatch.setattr(metrics_mod.np, "bincount", spy)
        compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE,
            weights=np.where(self.GROUPS == 0, 2.0, 1.0),
        )

        assert seen_weights, "expected the aggregation to use np.bincount"
        assert all(w is not None for w in seen_weights), (
            "every bincount on the weighted path must carry weights; an "
            "unweighted denominator divides weighted mass by raw support"
        )

    def test_contributions_return_arity_is_always_two(self):
        """positive_row_contributions must never vary its return arity: a
        caller writing ``a, b = f(...)`` has to keep working for every
        argument combination, including the empty-input early returns."""
        empty = np.array([], dtype=np.int64)
        for args in (
            (self.GROUPS, self.ITEMS, self.Y, self.SCORE),
            (self.GROUPS, self.ITEMS, self.Y, self.SCORE, 2),          # k truncation
            (self.GROUPS, self.ITEMS, np.zeros_like(self.Y), self.SCORE),  # no positives
            (empty, np.array([]), empty, np.array([], dtype=np.float64)),  # empty input
        ):
            out = positive_row_contributions(*args)
            assert isinstance(out, tuple) and len(out) == 2, args
        # ...and it takes no weights argument at all — weight broadcasting
        # lives in align_positive_row_weights.
        assert "weights" not in inspect.signature(
            positive_row_contributions
        ).parameters

    def test_contributions_break_ties_by_item(self):
        """The tied fixture of ``test_ties_within_a_group_are_broken_by_item``:
        item order puts the positives "b" (row 0) and "d" (row 2) at ranks 2
        and 4. Input order would put them at 1 and 3."""
        contrib, row_idx = positive_row_contributions(
            np.array([0, 0, 0, 0]), np.array(["b", "a", "d", "c"]),
            np.array([1, 0, 1, 0]), np.array([0.9, 0.9, 0.5, 0.5]),
        )
        assert row_idx.tolist() == [0, 2]
        assert contrib.tolist() == pytest.approx([1 / 2, 2 / 4])

    # --- 2. uniform weights == unweighted ---

    def test_uniform_weights_equal_unweighted(self):
        unweighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE,
            weights=np.ones(len(self.GROUPS)),
        )
        assert weighted == pytest.approx(unweighted)

    def test_uniform_weights_equal_unweighted_with_params(self):
        kwargs = dict(k=2, weight_alpha=1.0, min_positives=1, shrinkage_k=1.0)
        unweighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, **kwargs
        )
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE,
            weights=np.ones(len(self.GROUPS)), **kwargs
        )
        assert weighted == pytest.approx(unweighted)

    # --- 3. THE core semantic: weight 2 == the query counted twice ---

    def test_weight_two_equals_duplicating_that_query(self):
        w = np.where(self.GROUPS == 0, 2.0, 1.0)
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=w
        )
        dg, di, dy, ds = self._duplicate_query(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, query_id=0, times=2
        )
        duplicated = compute_macro_per_item_map(dg, di, dy, ds)
        assert weighted == pytest.approx(duplicated, rel=1e-12)
        # sanity: this is a real change, not a no-op equality
        assert weighted != pytest.approx(
            compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)
        )
        assert weighted == pytest.approx((2.5 / 3 + 1.0) / 2)

    def test_weight_three_equals_tripling_that_query(self):
        w = np.where(self.GROUPS == 1, 3.0, 1.0)
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=w
        )
        dg, di, dy, ds = self._duplicate_query(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, query_id=1, times=3
        )
        assert weighted == pytest.approx(
            compute_macro_per_item_map(dg, di, dy, ds), rel=1e-12
        )

    def test_duplication_equivalence_holds_for_param_family(self):
        """The weighted path must also reproduce duplication once
        min_positives / shrinkage_k / weight_alpha are active — i.e. the
        weighted per-item n_pos, not the raw row count, must reach the
        macro combine."""
        w = np.where(self.GROUPS == 0, 2.0, 1.0)
        dg, di, dy, ds = self._duplicate_query(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, query_id=0, times=2
        )
        for kwargs in (
            dict(weight_alpha=1.0),
            dict(min_positives=2),
            dict(min_positives=3),
            dict(shrinkage_k=1.0),
            dict(weight_alpha=1.0, min_positives=2, shrinkage_k=1.0),
        ):
            weighted = compute_macro_per_item_map(
                self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=w, **kwargs
            )
            duplicated = compute_macro_per_item_map(dg, di, dy, ds, **kwargs)
            assert weighted == pytest.approx(duplicated, rel=1e-12), kwargs

    def test_duplication_equivalence_at_scale_random(self):
        """Duplication equivalence on random multi-query/multi-item data with
        heterogeneous integer weights — the 6-row fixture above is too small
        to distinguish several plausible-but-wrong aggregations."""
        rng = np.random.default_rng(7)
        n_queries, n_items = 60, 8
        sizes = rng.integers(3, n_items + 1, size=n_queries)
        groups = np.repeat(np.arange(n_queries), sizes)
        items = np.concatenate([
            rng.choice(n_items, size=s, replace=False) for s in sizes
        ]).astype(str)
        y = rng.integers(0, 2, size=groups.shape[0]).astype(np.int64)
        score = rng.uniform(size=groups.shape[0])  # distinct -> no ties
        mult = rng.integers(1, 4, size=n_queries)  # per-query multiplicity
        w = mult[groups].astype(np.float64)

        # explicit replay: emit each query `mult` times as fresh group ids
        dg, di, dy, ds, next_gid = [], [], [], [], 0
        for q in range(n_queries):
            m = groups == q
            for _ in range(int(mult[q])):
                dg.append(np.full(int(m.sum()), next_gid))
                di.append(items[m]); dy.append(y[m]); ds.append(score[m])
                next_gid += 1
        dg = np.concatenate(dg); di = np.concatenate(di)
        dy = np.concatenate(dy); ds = np.concatenate(ds)

        for kwargs in (
            {}, dict(k=3), dict(weight_alpha=1.0),
            dict(min_positives=5), dict(k=3, shrinkage_k=2.0),
        ):
            weighted = compute_macro_per_item_map(
                groups, items, y, score, weights=w, **kwargs
            )
            duplicated = compute_macro_per_item_map(dg, di, dy, ds, **kwargs)
            assert weighted == pytest.approx(duplicated, rel=1e-12), kwargs

    # --- 4. weights really reach the macro aggregate ---

    def test_weights_change_macro(self):
        w = np.where(self.GROUPS == 0, 3.0, 1.0)
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=w
        )
        unweighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        # A = (3*1.0 + 1*0.5)/4 = 0.875 ; B = 1.0 ; macro = 0.9375
        assert weighted == pytest.approx(0.9375)
        assert unweighted == pytest.approx(0.875)
        assert weighted != pytest.approx(unweighted)

    def test_downweighting_a_query_also_moves_macro(self):
        """Weights below 1 must move the estimate the other way — guards
        against an implementation that only ever adds mass."""
        w = np.where(self.GROUPS == 0, 0.5, 1.0)
        weighted = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weights=w
        )
        # A = (0.5*1.0 + 1*0.5)/1.5 = 2/3 ; B = 1.0 ; macro = 5/6
        assert weighted == pytest.approx(5 / 6)

    # --- the shared broadcast helper aligns weights onto positive rows ---

    def test_align_helper_selects_the_positive_row_weights(self):
        w = np.array([2.0, 2.0, 1.0, 1.0, 5.0, 5.0])
        contrib, row_idx = positive_row_contributions(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        w_pos = align_positive_row_weights(w, len(self.GROUPS), row_idx)
        assert w_pos.shape == contrib.shape
        assert np.array_equal(w_pos, w[row_idx])

    def test_align_helper_rejects_wrong_length(self):
        _contrib, row_idx = positive_row_contributions(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        with pytest.raises(ValueError, match="row-aligned"):
            align_positive_row_weights(
                np.ones(len(self.GROUPS) - 1), len(self.GROUPS), row_idx
            )

    def test_weights_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            compute_macro_per_item_map(
                self.GROUPS, self.ITEMS, self.Y, self.SCORE,
                weights=np.ones(len(self.GROUPS) - 1),
            )

    def test_weighted_empty_input_returns_zero(self):
        empty = np.array([], dtype=np.int64)
        assert compute_macro_per_item_map(
            empty, np.array([]), empty, np.array([], dtype=np.float64),
            weights=np.array([], dtype=np.float64),
        ) == 0.0


# ---------------------------------------------------------------------------
# metric_params — the shared reader of `evaluation.metric` (ADR-0020 design H)
# ---------------------------------------------------------------------------


class TestMetricParams:
    """Imports live inside each test, so the rest of this file still collected
    while the shared helper did not exist yet."""

    DEFAULTS = {
        "k": None, "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0.0,
    }

    def test_missing_block_falls_back(self):
        from recsys_tfb.evaluation.metrics import metric_params

        assert metric_params({}) == self.DEFAULTS
        assert metric_params({"evaluation": None}) == self.DEFAULTS
        assert metric_params({"evaluation": {"metric": None}}) == self.DEFAULTS

    def test_missing_keys_fall_back(self):
        from recsys_tfb.evaluation.metrics import metric_params

        assert metric_params({"evaluation": {"metric": {}}}) == self.DEFAULTS

    def test_none_values_fall_back(self):
        from recsys_tfb.evaluation.metrics import metric_params

        cfg = {"k": None, "weight_alpha": None, "min_positives": None,
               "shrinkage_k": None}
        assert metric_params({"evaluation": {"metric": cfg}}) == self.DEFAULTS

    def test_values_are_type_normalised(self):
        from recsys_tfb.evaluation.metrics import metric_params

        cfg = {"k": 3, "weight_alpha": 1, "min_positives": 2, "shrinkage_k": 5}
        out = metric_params({"evaluation": {"metric": cfg}})
        assert out == {"k": 3, "weight_alpha": 1.0, "min_positives": 2,
                       "shrinkage_k": 5.0}
        assert type(out["k"]) is int
        assert type(out["weight_alpha"]) is float
        assert type(out["min_positives"]) is int
        assert type(out["shrinkage_k"]) is float


def test_metric_params_is_defined_exactly_once():
    """Exactly one `def metric_params` across src/ and scripts/ (design H).

    Copies drifting apart is what design H removes: fixing only the report
    note while leaving several readers would let the fallback semantics fork
    again. Both directories are asserted to exist first, so a wrong path that
    scans zero files cannot pass as "only one left".
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    pattern = re.compile(r"^\s*def metric_params\(", re.MULTILINE)
    hits: list[str] = []
    for sub in ("src", "scripts"):
        base = root / sub
        assert base.is_dir(), base
        for path in sorted(base.rglob("*.py")):
            n = len(pattern.findall(path.read_text(encoding="utf-8")))
            hits.extend([path.relative_to(root).as_posix()] * n)
    assert len(hits) == 1, hits
    assert hits == ["src/recsys_tfb/evaluation/metrics.py"]


# ---------------------------------------------------------------------------
# resolved_all_k — the one reader of what "all" resolved to (#434)
# ---------------------------------------------------------------------------


class TestResolvedAllK:
    def test_the_recorded_k_wins_over_the_item_count(self):
        from recsys_tfb.evaluation.metrics import ALL_K_KEY, resolved_all_k

        bundle = {ALL_K_KEY: 17,
                  "dataset_overview": {"totals": {"n_items": 12}}}
        assert resolved_all_k(bundle) == 17

    def test_nothing_recorded_means_the_item_count(self):
        """Every bundle computed without ``event``: nothing new is written,
        and the producer's K was the item count of the same frame."""
        from recsys_tfb.evaluation.metrics import resolved_all_k

        assert resolved_all_k(
            {"dataset_overview": {"totals": {"n_items": 12}}}) == 12

    def test_neither_means_unknown(self):
        """A slim baseline bundle carries no overview. 0 is "unknown", the
        reading ``report_builder.count_items`` already gives it."""
        from recsys_tfb.evaluation.metrics import resolved_all_k

        assert resolved_all_k({"overall": {"map@1": 0.5}}) == 0
        assert resolved_all_k({}) == 0


# ---------------------------------------------------------------------------
# All-positive query groups (#376): the switch reader, the share, the warning
# ---------------------------------------------------------------------------


class TestDropAllPositiveGroups:
    def test_missing_block_or_none_means_off(self):
        from recsys_tfb.evaluation.metrics import drop_all_positive_groups

        assert drop_all_positive_groups({}) is False
        assert drop_all_positive_groups({"evaluation": None}) is False
        assert drop_all_positive_groups(
            {"evaluation": {"query_filter": None}}) is False
        assert drop_all_positive_groups(
            {"evaluation": {"query_filter": {
                "drop_all_positive_groups": None}}}) is False

    def test_true_turns_it_on(self):
        from recsys_tfb.evaluation.metrics import drop_all_positive_groups

        assert drop_all_positive_groups(
            {"evaluation": {"query_filter": {
                "drop_all_positive_groups": True}}}) is True

    def test_not_read_from_evaluation_metric(self):
        """``metric_params`` silently drops unknown keys of
        ``evaluation.metric``, so a switch written there would do nothing."""
        from recsys_tfb.evaluation.metrics import drop_all_positive_groups

        assert drop_all_positive_groups(
            {"evaluation": {"metric": {
                "drop_all_positive_groups": True}}}) is False


class TestAllPositiveShare:
    def test_denominator_is_the_groups_with_a_positive(self):
        """The mAP denominator: ``n_queries - n_excluded_queries``, where
        ``n_excluded_queries`` counts only the zero-positive groups."""
        from recsys_tfb.evaluation.metrics import (
            ALL_POSITIVE_KEY, all_positive_share,
        )

        bundle = {"n_queries": 1000, "n_excluded_queries": 50,
                  ALL_POSITIVE_KEY: 95}
        assert all_positive_share(bundle) == pytest.approx(95 / 950)

    def test_unknown_when_the_count_is_missing_or_no_group_has_a_positive(self):
        """A bundle written before #376 has no count; a run where no group
        holds a positive has no denominator."""
        from recsys_tfb.evaluation.metrics import (
            ALL_POSITIVE_KEY, all_positive_share,
        )

        assert all_positive_share(
            {"n_queries": 10, "n_excluded_queries": 0}) is None
        assert all_positive_share(
            {"n_queries": 10, "n_excluded_queries": 10,
             ALL_POSITIVE_KEY: 0}) is None


class TestNGroupsWithPositive:
    def test_is_n_queries_less_the_zero_positive_groups(self):
        from recsys_tfb.evaluation.metrics import n_groups_with_positive

        assert n_groups_with_positive(
            {"n_queries": 1000, "n_excluded_queries": 50}) == 950
        assert n_groups_with_positive(
            {"n_queries": 10, "n_excluded_queries": 10}) == 0

    def test_unknown_when_either_count_is_missing(self):
        """Never a guess at the missing count: 0 in its place would read as
        "every group holds a positive"."""
        from recsys_tfb.evaluation.metrics import n_groups_with_positive

        assert n_groups_with_positive({"n_excluded_queries": 5}) is None
        assert n_groups_with_positive({"n_queries": 10}) is None
        assert n_groups_with_positive(
            {"n_queries": None, "n_excluded_queries": 0}) is None
        assert n_groups_with_positive({}) is None


class TestFormatShare:
    def test_one_decimal_by_default(self):
        from recsys_tfb.evaluation.metrics import format_share

        assert format_share(0.053) == "5.3%"
        assert format_share(50 / 950) == "5.3%"
        assert format_share(0.101) == "10.1%"
        assert format_share(0.10) == "10.0%"   # at the threshold: not above

    def test_above_the_threshold_never_prints_as_the_threshold(self):
        """1004 / 10000 is above 10% and would print 10.0% beside "above
        10%"; more decimals until the printed value is above it too."""
        from recsys_tfb.evaluation.metrics import format_share

        assert format_share(1004 / 10000) != "10.0%"
        assert format_share(1004 / 10000) == "10.04%"
        assert format_share(21 / 209) == "10.05%"
        assert format_share(100001 / 1000000) == "10.0001%"


class TestAllPositiveShareWarns:
    ON = {"evaluation": {"query_filter": {"drop_all_positive_groups": True}}}

    @staticmethod
    def _bundle(n_all_positive: int, n_with_positive: int = 100) -> dict:
        from recsys_tfb.evaluation.metrics import ALL_POSITIVE_KEY

        return {"n_queries": n_with_positive + 7, "n_excluded_queries": 7,
                ALL_POSITIVE_KEY: n_all_positive}

    def test_warns_above_the_threshold_when_the_switch_is_off(self):
        from recsys_tfb.evaluation.metrics import all_positive_share_warns

        assert all_positive_share_warns(self._bundle(11), {}) is True

    def test_exactly_the_threshold_does_not_warn(self):
        from recsys_tfb.evaluation.metrics import (
            ALL_POSITIVE_WARN_SHARE, all_positive_share_warns,
        )

        assert ALL_POSITIVE_WARN_SHARE == 0.10
        assert all_positive_share_warns(self._bundle(10), {}) is False

    def test_no_warning_when_the_switch_already_drops_them(self):
        from recsys_tfb.evaluation.metrics import all_positive_share_warns

        assert all_positive_share_warns(self._bundle(60), self.ON) is False

    def test_no_warning_when_the_share_is_unknown(self):
        from recsys_tfb.evaluation.metrics import all_positive_share_warns

        assert all_positive_share_warns(
            {"n_queries": 10, "n_excluded_queries": 0}, {}) is False


class TestPooledAveragePrecision:
    """The HPO objective ``pooled_average_precision`` (#430): every val row one
    binary prediction, all rows in one pool, scored exactly as scikit-learn
    scores them.

    Rows 0-3 are a query group holding a positive (weight 1); rows 4-7 a kept
    zero-positive group at r = 0.5 (weight 1/r = 2). Its negatives outrank one
    of the positives, so their weight moves the value: 9/14 weighted against
    0.7 unweighted.
    """

    Y = np.array([1, 0, 1, 0, 0, 0, 0, 0])
    SCORE = np.array([0.9, 0.8, 0.4, 0.3, 0.85, 0.5, 0.2, 0.1])
    W = np.array([1, 1, 1, 1, 2, 2, 2, 2], dtype=float)

    def test_equals_scikit_learn_under_the_weights(self):
        from sklearn.metrics import average_precision_score

        from recsys_tfb.evaluation.metrics import compute_pooled_average_precision

        expected = average_precision_score(self.Y, self.SCORE, sample_weight=self.W)
        assert expected == pytest.approx(9 / 14)
        assert expected != pytest.approx(
            average_precision_score(self.Y, self.SCORE)
        ), "fixture no longer makes the weights matter"
        assert compute_pooled_average_precision(
            self.Y, self.SCORE, self.W) == expected

    def test_no_positive_row_raises_instead_of_minus_zero(self):
        """scikit-learn answers an all-zero ``y_true`` with ``-0.0`` and a
        ``UserWarning``. In the HPO loop that constant would score every trial
        alike: the first trial wins and nothing says why. The warning is
        turned into an error so a check that runs *after* scikit-learn cannot
        pass this test."""
        import warnings

        from recsys_tfb.evaluation.metrics import compute_pooled_average_precision

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with pytest.raises(ValueError, match="holds no positive row"):
                compute_pooled_average_precision(
                    np.zeros(4), np.array([0.4, 0.3, 0.2, 0.1]), np.ones(4))


class TestMacroPerItemAveragePrecision:
    """The HPO objective ``macro_per_item_average_precision`` (#430): one
    scikit-learn average precision per item, then the plain mean over the
    items that hold a positive.

    Rows are interleaved across items on purpose, so an implementation that
    assumed rows arrive grouped by item would slice the wrong rows. Item "c"
    holds no positive. Weights of 2 sit on negatives that outrank a positive,
    so they move both per-item values.
    """

    ITEMS = np.array(["a", "b", "c", "a", "b", "a", "b", "c", "a", "b"])
    Y = np.array([1, 0, 0, 0, 1, 0, 0, 0, 1, 0])
    SCORE = np.array([0.9, 0.6, 0.9, 0.8, 0.5, 0.7, 0.4, 0.1, 0.2, 0.3])
    W = np.array([1, 1, 2, 2, 1, 2, 2, 2, 1, 2], dtype=float)

    def _expected(self, weights):
        from sklearn.metrics import average_precision_score

        per_item = [
            average_precision_score(
                self.Y[self.ITEMS == item], self.SCORE[self.ITEMS == item],
                sample_weight=None if weights is None
                else weights[self.ITEMS == item],
            )
            for item in ("a", "b")
        ]
        return float(np.mean(per_item))

    def test_mean_of_scikit_learn_per_item_over_items_with_a_positive(self):
        """Warnings are errors here: scoring item "c" at all would make
        scikit-learn warn, so this also pins that "c" is left out rather than
        scored as -0.0 and averaged in."""
        import warnings

        from recsys_tfb.evaluation.metrics import (
            compute_macro_per_item_average_precision,
        )

        expected = self._expected(self.W)
        assert expected != pytest.approx(self._expected(None)), (
            "fixture no longer makes the weights matter")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = compute_macro_per_item_average_precision(
                self.ITEMS, self.Y, self.SCORE, self.W)
        assert result == pytest.approx(expected, rel=1e-12)

    def test_no_item_with_a_positive_raises(self):
        from recsys_tfb.evaluation.metrics import (
            compute_macro_per_item_average_precision,
        )

        with pytest.raises(ValueError, match="holds no positive row"):
            compute_macro_per_item_average_precision(
                self.ITEMS, np.zeros(len(self.ITEMS)), self.SCORE, self.W)

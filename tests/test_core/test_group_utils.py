"""Tests for recsys_tfb.core.group_utils."""

import numpy as np
import pytest

from recsys_tfb.core.group_utils import (
    RANKING_OBJECTIVES,
    default_metric_for_objective,
    is_ranking_objective,
    objective_cache_key,
    to_contiguous_groups,
)


class TestObjectiveClassification:
    def test_ranking_objectives_set(self):
        assert RANKING_OBJECTIVES == frozenset({"lambdarank", "rank_xendcg"})

    @pytest.mark.parametrize("obj", ["lambdarank", "rank_xendcg"])
    def test_is_ranking_true(self, obj):
        assert is_ranking_objective(obj) is True

    @pytest.mark.parametrize("obj", ["binary", "regression", None, ""])
    def test_is_ranking_false(self, obj):
        assert is_ranking_objective(obj) is False

    def test_objective_cache_key_separates_the_two_ranking_objectives(self):
        """lambdarank and rank_xendcg must NOT share a cache key.

        They build the same .bin today; the split lands ahead of the
        follow-up that gives lambdarank a different training matrix, so the
        model_version-blind cache never gets a window in which it can serve
        the wrong .bin. Full rationale in ``objective_cache_key``.
        """
        assert objective_cache_key("lambdarank") == "lambdarank"
        assert objective_cache_key("rank_xendcg") == "rank_xendcg"
        assert objective_cache_key("lambdarank") != objective_cache_key(
            "rank_xendcg"
        )

    def test_objective_cache_key_non_ranking_share_one_key(self):
        """Non-ranking objectives deliberately collide onto "binary".

        They build a byte-identical .bin (same X/y/weight, no group vector)
        with no divergence planned, so sharing is safe — and it keeps every
        existing ``lgb/binary/`` cache dir valid with no migration.
        """
        assert objective_cache_key("binary") == "binary"
        assert objective_cache_key(None) == "binary"
        assert objective_cache_key("regression") == "binary"


class TestDefaultMetricForObjective:
    def test_ranking_without_metric_defaults_ndcg(self):
        assert default_metric_for_objective("lambdarank", None) == "ndcg"
        assert default_metric_for_objective("rank_xendcg", "") == "ndcg"

    def test_ranking_with_metric_kept(self):
        assert default_metric_for_objective("lambdarank", "ndcg") == "ndcg"
        assert default_metric_for_objective("lambdarank", "map") == "map"

    def test_non_ranking_metric_unchanged(self):
        assert default_metric_for_objective("binary", None) is None
        assert default_metric_for_objective("binary", "binary_logloss") == "binary_logloss"


class TestToContiguousGroups:
    def test_empty_input(self):
        perm, counts = to_contiguous_groups(np.array([], dtype=np.int64))
        assert perm.shape == (0,)
        assert counts.shape == (0,)
        assert perm.dtype == np.int64
        assert counts.dtype == np.int64

    def test_already_contiguous(self):
        ids = np.array([0, 0, 1, 2, 2, 2], dtype=np.int64)
        perm, counts = to_contiguous_groups(ids)
        np.testing.assert_array_equal(perm, np.array([0, 1, 2, 3, 4, 5]))
        np.testing.assert_array_equal(counts, np.array([2, 1, 3]))
        assert int(counts.sum()) == len(ids)

    def test_interleaved_ids_made_contiguous_stably(self):
        # group 2 (rows 0,1), group 0 (rows 2,3), group 1 (row 4)
        ids = np.array([2, 2, 0, 0, 1], dtype=np.int64)
        perm, counts = to_contiguous_groups(ids)
        # stable sort by id -> rows of id 0 (orig 2,3), id 1 (orig 4), id 2 (orig 0,1)
        np.testing.assert_array_equal(perm, np.array([2, 3, 4, 0, 1]))
        np.testing.assert_array_equal(counts, np.array([2, 1, 2]))
        sorted_ids = ids[perm]
        # each group is now a single contiguous run
        np.testing.assert_array_equal(sorted_ids, np.array([0, 0, 1, 2, 2]))
        assert int(counts.sum()) == len(ids)

    def test_perm_applies_to_X_and_y(self):
        ids = np.array([1, 0, 1, 0], dtype=np.int64)
        X = np.array([[10], [20], [30], [40]], dtype=float)
        y = np.array([1, 0, 0, 1])
        perm, counts = to_contiguous_groups(ids)
        np.testing.assert_array_equal(ids[perm], np.array([0, 0, 1, 1]))
        np.testing.assert_array_equal(X[perm].ravel(), np.array([20, 40, 10, 30]))
        np.testing.assert_array_equal(y[perm], np.array([0, 1, 1, 0]))
        np.testing.assert_array_equal(counts, np.array([2, 2]))

    def test_rejects_non_1d_input(self):
        # algorithm-agnostic contract (reused by Phase 4 XGBoost): a 2-D
        # array must fail loudly, not silently mis-sort per-axis.
        bad = np.array([[0, 1], [1, 0]], dtype=np.int64)
        with pytest.raises(ValueError, match="1-D"):
            to_contiguous_groups(bad)

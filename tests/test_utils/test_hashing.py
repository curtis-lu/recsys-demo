"""Tests for the deterministic CRC32 hashing utilities used by sampling."""

from recsys_tfb.utils.hashing import (
    HASH_BUCKETS,
    ratio_to_threshold,
)


def test_ratio_to_threshold_round_trip():
    assert ratio_to_threshold(0.0) == 0
    assert ratio_to_threshold(1.0) == HASH_BUCKETS
    assert ratio_to_threshold(0.5) == HASH_BUCKETS // 2
    assert ratio_to_threshold(0.123) == round(0.123 * HASH_BUCKETS)

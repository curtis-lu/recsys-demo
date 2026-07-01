"""Tests for diagnostics.sampling._stratified_item_sample (item_values 簽名)."""
import numpy as np

from recsys_tfb.pipelines.training.diagnostics.sampling import _stratified_item_sample


def test_returns_sorted_unique_valid_indices():
    items = np.array(["A", "B", "A", "A", "B", "C", "A", "B"])
    idx = _stratified_item_sample(items, total=6, min_per_item=1, seed=42)
    assert list(idx) == sorted(idx)
    assert len(set(idx.tolist())) == len(idx)
    assert idx.min() >= 0 and idx.max() < len(items)


def test_deterministic_same_seed():
    items = np.array(["A", "B"] * 20)
    a = _stratified_item_sample(items, total=8, min_per_item=1, seed=42)
    b = _stratified_item_sample(items, total=8, min_per_item=1, seed=42)
    assert list(a) == list(b)


def test_min_per_item_take_all_when_scarce():
    # C has only 1 row; with min_per_item=3 it is taken in full (take-all).
    items = np.array(["A", "A", "A", "A", "B", "B", "B", "B", "C"])
    idx = _stratified_item_sample(items, total=9, min_per_item=3, seed=0)
    taken = items[idx]
    assert (taken == "C").sum() == 1  # the single C row present


def test_per_item_floor_from_total():
    # 3 items, total=9 -> per_item=max(min, 9//3)=3 each (all have >=3 rows).
    items = np.array(["A"] * 5 + ["B"] * 5 + ["C"] * 5)
    idx = _stratified_item_sample(items, total=9, min_per_item=1, seed=1)
    taken = items[idx]
    assert (taken == "A").sum() == 3
    assert (taken == "B").sum() == 3
    assert (taken == "C").sum() == 3


from recsys_tfb.pipelines.training.diagnostics.sampling import _positive_item_sample


def test_positive_sample_only_picks_positives():
    items = np.array(["A", "B", "A", "B", "A", "B"])
    labels = np.array([1, 0, 1, 0, 0, 1])
    idx = _positive_item_sample(items, labels, per_item=10, seed=42)
    assert list(labels[idx]) == [1] * len(idx)          # 只抽到正例
    assert set(items[idx]) <= {"A", "B"}


def test_positive_sample_per_item_cap_and_take_all():
    # A 有 3 正例、B 有 1 正例;per_item=2 → A 取 2、B 取 1(不足全取)
    items = np.array(["A", "A", "A", "A", "B", "B"])
    labels = np.array([1, 1, 1, 0, 1, 0])
    idx = _positive_item_sample(items, labels, per_item=2, seed=0)
    picked = items[idx]
    assert (picked == "A").sum() == 2
    assert (picked == "B").sum() == 1
    assert list(idx) == sorted(idx)


def test_positive_sample_empty_when_no_positives():
    items = np.array(["A", "B", "A"])
    labels = np.array([0, 0, 0])
    idx = _positive_item_sample(items, labels, per_item=5, seed=1)
    assert len(idx) == 0


def test_positive_sample_deterministic():
    items = np.array(["A", "B"] * 20)
    labels = np.array([1, 0] * 20)
    a = _positive_item_sample(items, labels, per_item=5, seed=42)
    b = _positive_item_sample(items, labels, per_item=5, seed=42)
    assert list(a) == list(b)

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.partition import (
    group_labels, group_seed, group_slug, routing_keys,
)


class TestGroupLabels:
    def test_single_key(self):
        pdf = pd.DataFrame({"seg": ["a", "b", "a"]})
        labels = group_labels(pdf, ["seg"])
        assert labels.tolist() == ["a", "b", "a"]

    def test_composite_key_pipe_joined(self):
        pdf = pd.DataFrame({"seg": ["a"], "prod": ["x"]})
        assert group_labels(pdf, ["seg", "prod"]).tolist() == ["a|x"]

    def test_missing_column_raises(self):
        with pytest.raises(KeyError):
            group_labels(pd.DataFrame({"seg": ["a"]}), ["nope"])


class TestRoutingKeys:
    def test_same_as_group_labels_as_numpy(self):
        pdf = pd.DataFrame({"seg": ["a", "b"]})
        keys = routing_keys(pdf, ["seg"])
        assert isinstance(keys, np.ndarray)
        assert keys.tolist() == ["a", "b"]


class TestGroupSlug:
    def test_safe_chars_kept_and_suffix_stable(self):
        assert group_slug("fund_stock") == group_slug("fund_stock")
        assert group_slug("fund_stock") != group_slug("fund_bond")

    def test_unsafe_chars_sanitized_but_distinct(self):
        # 消毒後字面相同的兩個 key 仍須因 crc 後綴而不同
        a, b = group_slug("a/b"), group_slug("a|b")
        assert "/" not in a and "|" not in b
        assert a != b


class TestGroupSeed:
    def test_deterministic_and_distinct(self):
        assert group_seed(42, "a") == group_seed(42, "a")
        assert group_seed(42, "a") != group_seed(42, "b")
        assert group_seed(41, "a") != group_seed(42, "a")

    def test_in_valid_range(self):
        s = group_seed(42, "any-key")
        assert 0 <= s < 2**31 - 1

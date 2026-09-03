"""Regression: conf/base declares version-feeding keys without activating them.

Every key here defaults to something, and writing one into ``conf/base`` — even
at its default value — changes nothing about behaviour but does change the
version payload it feeds, orphaning every artifact under that ID. The
declaration therefore has to live in comments. That distinction is invisible in
a diff review ("it's just the default"), so it is asserted here instead.

Two families, one failure mode: the split-unit keys (``train_split_keys`` /
``val_sample_keys``, ADR-0016) and the numeric storage declaration
(``numeric_feature_storage_type``, which feeds ``base_dataset_version``).
"""

from pathlib import Path

import yaml

from recsys_tfb.core.schema import ENTITY_GROUPING_KEYS

_PATH = Path("conf/base/parameters_dataset.yaml")


def _dataset_block() -> dict:
    return yaml.safe_load(_PATH.read_text())["dataset"]


def test_neither_split_unit_key_is_a_live_yaml_key():
    ds = _dataset_block()
    for key in ENTITY_GROUPING_KEYS:
        assert key not in ds, (
            f"conf/base/parameters_dataset.yaml activates dataset.{key}. Even "
            f"set to its default this busts a version ID and orphans existing "
            f"artifacts. Keep the declaration commented out."
        )


def test_both_keys_are_documented_in_comments():
    # The other half of the same contract: commented out, but present enough
    # for a reader of conf/ to learn the keys exist without grepping src/.
    text = _PATH.read_text()
    for key in ENTITY_GROUPING_KEYS:
        assert f"# {key}:" in text, f"{key} has no commented declaration"


def test_the_numeric_storage_type_is_not_a_live_yaml_key():
    # It feeds base_dataset_version, so writing it — at any value, the default
    # included — rebuilds preprocessor / val / test and every train variant
    # under them, for a config that behaves identically. Same trap as the
    # split-unit keys above.
    assert "numeric_feature_storage_type" not in _dataset_block(), (
        "conf/base/parameters_dataset.yaml activates dataset."
        "numeric_feature_storage_type. Even at its default this busts "
        "base_dataset_version and orphans every existing dataset artifact. "
        "Keep the declaration commented out."
    )


def test_the_numeric_storage_keys_are_documented_in_comments():
    text = _PATH.read_text()
    for key in ("numeric_feature_storage_type", "numeric_precision_policy"):
        assert f"# {key}:" in text, f"{key} has no commented declaration"


def test_the_policy_key_is_the_one_that_could_have_been_live():
    # Not a style assertion — it pins why the pair is two keys. The policy key
    # is stripped from the hash (GATE_POLICY_KEYS), so activating it would cost
    # nothing; it stays commented only so the pair reads as one block. If this
    # ever stops holding, the split into two keys has lost its point.
    from recsys_tfb.core.versioning import GATE_POLICY_KEYS

    assert "numeric_precision_policy" in GATE_POLICY_KEYS

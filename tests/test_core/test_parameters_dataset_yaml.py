"""Regression: conf/base declares the split-unit keys without activating them.

Both keys default to the whole ``schema.entity``, so writing either one into
``conf/base`` — even at its default value — changes nothing about the split but
does change the version payload it feeds, orphaning every artifact under it.
The declaration therefore has to live in comments. That distinction is invisible
in a diff review ("it's just the default"), so it is asserted here instead.
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

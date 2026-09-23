"""Training with the item list counted from the data (#379): A9c — a
``training.sample_weights`` key naming an item the list lacks — waits for the
list, which training first holds in ``select_features``."""

import pytest

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import ITEM_LIST_FROM_TRAIN_DATA
from recsys_tfb.pipelines.training.nodes import select_features

PREPROCESSOR = {
    "feature_columns": ["prod_name", "total_aum"],
    "categorical_columns": ["prod_name"],
    "category_mappings": {"prod_name": ["a", "b"]},
    "drop_columns": [],
}


def _params(weight_key, item_values=ITEM_LIST_FROM_TRAIN_DATA):
    return {
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name"},
            "categorical_values": {"prod_name": item_values},
        },
        "training": {"sample_weight_keys": ["prod_name"],
                     "sample_weights": {weight_key: 2.0}},
    }


def test_a_weight_on_an_item_the_list_lacks_is_refused():
    """With the text the CLI entry raises for a listed list: one
    predicate's message at both sites."""
    from recsys_tfb.core.consistency import weight_unknown_item_errors

    with pytest.raises(DataConsistencyError, match="'c'") as excinfo:
        select_features(PREPROCESSOR, _params("c"))
    assert str(excinfo.value) == weight_unknown_item_errors(
        _params("c"), items=["a", "b"])[0]
    assert str(excinfo.value).startswith("A9c: ")


def test_a_weight_on_a_counted_item_passes():
    assert select_features(PREPROCESSOR, _params("a")) == PREPROCESSOR


def test_a_listed_item_list_is_left_to_the_cli_entry():
    """Checked at the entry already (validate_config_consistency); the node
    does not repeat it."""
    assert select_features(PREPROCESSOR, _params("c", ["a", "b"])) == PREPROCESSOR

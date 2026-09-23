"""The item list counted from the train months' data (#379):
``schema.categorical_values[<item>]: from_train_data``.

What the config layer does with it: the shape check lets the item cell (and
only it) hold the value, every reader that used to read the declared list
takes its own branch instead of iterating a string, and ``inference.products``
may not be written next to it.
"""

import pytest

from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    inference_grid_errors,
    item_coverage_errors,
    item_list_counted_from_data,
    override_unknown_items,
    resolved_item_values,
    validate_config_consistency,
    weight_unknown_items,
)
from recsys_tfb.core.schema import (
    ITEM_LIST_FROM_TRAIN_DATA,
    get_schema_for_hash,
    validate_schema_config,
)

ITEMS = ["fund_bond", "exchange_fx", "ccard_bill"]


def _params(item_values=ITEM_LIST_FROM_TRAIN_DATA, **extra):
    params = {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": item_values},
        },
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
    }
    for key, value in extra.items():
        params.setdefault(key, {}).update(value)
    return params


def test_the_value_is_spelled_from_train_data():
    assert ITEM_LIST_FROM_TRAIN_DATA == "from_train_data"


class TestShape:
    def test_the_item_cell_may_hold_it(self):
        validate_schema_config(_params())

    def test_another_cell_may_not(self):
        params = _params(ITEMS)
        params["schema"]["categorical_values"]["tier"] = ITEM_LIST_FROM_TRAIN_DATA
        with pytest.raises(ValueError, match="only schema.categorical_values"):
            validate_schema_config(params)

    def test_any_other_string_is_still_refused(self):
        with pytest.raises(ValueError, match="must be a non-empty list"):
            validate_schema_config(_params("from_train"))


class TestTheListIsNotAString:
    def test_counted_from_data(self):
        assert item_list_counted_from_data(_params()) is True
        assert item_list_counted_from_data(_params(ITEMS)) is False

    def test_resolved_item_values_refuses_instead_of_spelling_letters(self):
        """``sorted("from_train_data")`` is a list of letters; a reader that
        forgot the branch must hear about it."""
        with pytest.raises(ConfigConsistencyError, match="from_train_data"):
            resolved_item_values(_params())

    def test_a_listed_item_list_is_unchanged(self):
        assert resolved_item_values(_params(ITEMS)) == sorted(ITEMS)

    def test_the_hash_payload_of_a_listed_item_list_is_the_one_main_hashes(self):
        """Written out as main (4e3df4e3) produced it: the list keeps the
        order it is written in."""
        assert get_schema_for_hash(_params(ITEMS)) == {
            "categorical_values": {"prod_name": ["fund_bond", "exchange_fx",
                                                 "ccard_bill"]},
            "entity": ["cust_id"], "item": "prod_name", "label": "label",
            "rank": "rank", "score": "score", "time": "snap_date",
        }


class TestInferenceProductsA52:
    def test_counted_list_with_products_is_refused(self):
        params = _params(inference={"products": ITEMS})
        with pytest.raises(ConfigConsistencyError, match="A52"):
            validate_config_consistency(params)

    def test_counted_list_without_products_passes(self):
        validate_config_consistency(_params())

    def test_listed_items_keep_a4(self):
        params = _params(ITEMS, inference={"products": ITEMS[:2]})
        with pytest.raises(ConfigConsistencyError, match="only_in_categorical"):
            validate_config_consistency(params)

    def test_a27_does_not_ask_for_products(self):
        params = _params(inference={"snap_dates": ["2026-01-31"]})
        assert inference_grid_errors(params) == []

    def test_a27_still_asks_for_products_of_a_listed_item_list(self):
        params = _params(ITEMS, inference={"snap_dates": ["2026-01-31"]})
        assert any("inference.products" in e for e in inference_grid_errors(params))


class TestA5A9cWaitForTheList:
    """#379: with nothing to compare against before the run, the two checks
    move to where the list exists, and there a typo raises as before."""

    def _overrides(self, key):
        return _params(dataset={"sample_group_keys": ["prod_name"],
                                "sample_ratio_overrides": {key: 0.5}})

    def _weights(self, key):
        return _params(training={"sample_weight_keys": ["prod_name"],
                                 "sample_weights": {key: 2.0}})

    def test_config_time_is_quiet(self):
        assert override_unknown_items(self._overrides("fund_bnd")) == []
        assert weight_unknown_items(self._weights("fund_bnd")) == []

    def test_against_the_counted_list_a_typo_is_named(self):
        assert override_unknown_items(
            self._overrides("fund_bnd"), items=ITEMS) == ["fund_bnd"]
        assert weight_unknown_items(
            self._weights("fund_bnd"), items=ITEMS) == ["fund_bnd"]

    def test_against_the_counted_list_a_known_item_passes(self):
        assert override_unknown_items(self._overrides("fund_bond"), items=ITEMS) == []
        assert weight_unknown_items(self._weights("fund_bond"), items=ITEMS) == []

    def test_the_cli_entry_names_each_check_by_its_code(self):
        """A listed list is checked at the CLI entry, a counted one at run
        time; both sites raise one text, and it carries the code, as the
        runtime one always did."""
        params = _params(
            ITEMS,
            dataset={"sample_group_keys": ["prod_name"],
                     "sample_ratio_overrides": {"fund_bnd": 0.5}},
            training={"sample_weight_keys": ["prod_name"],
                      "sample_weights": {"fund_bnd": 2.0}},
        )
        with pytest.raises(ConfigConsistencyError) as excinfo:
            validate_config_consistency(params)
        message = str(excinfo.value)
        assert ("A5: dataset.sample_ratio_overrides references item value(s) "
                "['fund_bnd'] absent from schema.categorical_values[item]") \
            in message, message
        assert ("A9c: training.sample_weights references item value(s) "
                "['fund_bnd'] absent from schema.categorical_values[item]") \
            in message, message


class TestB1WithACountedList:
    """With no declared list, sample_pool is not compared with one (its
    val/test items the train months lack are new items, warned about where
    they are encoded); label_table's items must still be candidates."""

    def test_sample_pool_items_are_not_compared(self):
        assert item_coverage_errors(
            "prod_name", None, {"a", "b", "new"}, {"a"}) == []

    def test_a_label_item_sample_pool_never_holds_is_refused(self):
        errors = item_coverage_errors("prod_name", None, {"a", "b"}, {"a", "ghost"})
        assert len(errors) == 1
        assert "['ghost']" in errors[0] and "sample_pool" in errors[0]


class TestWhenTheCategoryPassNeedsTheLandedTable:
    """``--compare-only`` asks for item_categories.json only when the category
    pass reads it."""

    @staticmethod
    def _with_categories(params, **block):
        params["evaluation"] = {"item_categories": {"enabled": True, **block}}
        return params

    def test_a_counted_list_with_a_hand_mapping_needs_it(self):
        from recsys_tfb.core.consistency import category_table_needed

        assert category_table_needed(
            self._with_categories(_params(), mapping={"x": ["a"]})) is True

    def test_a_listed_list_with_a_hand_mapping_does_not(self):
        from recsys_tfb.core.consistency import category_table_needed

        assert category_table_needed(
            self._with_categories(_params(ITEMS), mapping={"x": ["a"]})) is False

    def test_categories_off_never_need_it(self):
        from recsys_tfb.core.consistency import category_table_needed

        params = self._with_categories(_params(), mapping={"x": ["a"]})
        params["evaluation"]["item_categories"]["enabled"] = False
        assert category_table_needed(params) is False

    def test_column_mode_needs_it(self):
        from recsys_tfb.core.consistency import category_table_needed

        assert category_table_needed(
            self._with_categories(_params(ITEMS), column="family")) is True

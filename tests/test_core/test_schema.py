"""Tests for core.schema — centralized column schema."""

import copy

import pytest

from recsys_tfb.core.schema import (
    ENTITY_GROUPING_KEYS,
    get_entity_grouping,
    get_schema,
)


def _params(**over) -> dict:
    """The example deployment's three required roles, with overrides.

    Every role in ``over`` replaces its counterpart; passing ``None`` drops
    one, for the tests that are about the missing-role rule itself. Spelled
    out rather than left to a default because since #328 ``get_schema`` has no
    default for ``time`` / ``entity`` / ``item`` -- see
    :data:`recsys_tfb.core.schema._REQUIRED_ROLES`.
    """
    columns = {"time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}
    columns.update(over)
    return {"schema": {"columns": {k: v for k, v in columns.items() if v is not None}}}


class TestRequiredRolesHaveNoDefault:
    """``get_schema`` refuses the three roles that name the user's own columns.

    ``validate_schema_config`` refuses them too, at the CLI entry, and that is
    what a real run hits first. This class covers the second gate: the one
    that catches a *test* whose parameters never went through the CLI. Without
    it such a test would resolve to the example deployment's spellings and pass
    against code that handles no others (#274, #328).
    """

    def test_no_schema_section_raises(self):
        with pytest.raises(ValueError, match="Missing schema.columns"):
            get_schema({})

    @pytest.mark.parametrize("role", ["time", "entity", "item"])
    def test_a_single_missing_role_is_named(self, role):
        with pytest.raises(ValueError, match=rf"Missing schema\.columns.*{role}"):
            get_schema(_params(**{role: None}))

    def test_every_missing_role_is_reported_at_once(self):
        with pytest.raises(ValueError) as exc:
            get_schema({"schema": {"columns": {"label": "y"}}})
        assert "time, entity, item" in str(exc.value)

    def test_the_framework_produced_roles_still_default(self):
        result = get_schema(_params())
        assert result["label"] == "label"
        assert result["score"] == "score"
        assert result["rank"] == "rank"

    def test_identity_columns_from_a_complete_declaration(self):
        result = get_schema(_params())
        assert result["identity_columns"] == ["snap_date", "cust_id", "prod_name"]


class TestGetSchemaPartialOverride:
    def test_override_time_only(self):
        result = get_schema(_params(time="month_end"))
        assert result["time"] == "month_end"
        assert result["entity"] == ["cust_id"]
        assert result["item"] == "prod_name"

    def test_override_item_only(self):
        result = get_schema(_params(item="product_code"))
        assert result["item"] == "product_code"
        assert result["time"] == "snap_date"


class TestGetSchemaFullOverride:
    def test_all_keys_overridden(self):
        params = {
            "schema": {
                "columns": {
                    "time": "dt",
                    "entity": ["branch_id", "cust_id"],
                    "item": "product_code",
                    "label": "target",
                    "score": "prob",
                    "rank": "position",
                }
            }
        }
        result = get_schema(params)
        assert result["time"] == "dt"
        assert result["entity"] == ["branch_id", "cust_id"]
        assert result["item"] == "product_code"
        assert result["label"] == "target"
        assert result["score"] == "prob"
        assert result["rank"] == "position"


class TestEntityNormalization:
    def test_entity_string_to_list(self):
        result = get_schema(_params(entity="cust_id"))
        assert result["entity"] == ["cust_id"]

    def test_entity_list_unchanged(self):
        result = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert result["entity"] == ["branch_id", "cust_id"]


class TestIdentityColumnsDerivation:
    def test_single_entity_identity(self):
        result = get_schema(_params())
        assert result["identity_columns"] == ["snap_date", "cust_id", "prod_name"]

    def test_multi_entity_identity(self):
        result = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert result["identity_columns"] == [
            "snap_date", "branch_id", "cust_id", "prod_name"
        ]


class TestPureFunction:
    def test_input_not_mutated(self):
        params = _params(time="month_end")
        original = copy.deepcopy(params)
        get_schema(params)
        assert params == original

    def test_repeated_calls_same_result(self):
        params = _params(entity="cust_id")
        assert get_schema(params) == get_schema(params)


class TestCategoricalValues:
    def test_default_empty_when_absent(self):
        result = get_schema(_params())
        assert result["categorical_values"] == {}

    def test_returned_from_schema_section(self):
        params = _params()
        params["schema"]["categorical_values"] = {"prod_name": ["a", "b", "c"]}
        result = get_schema(params)
        assert result["categorical_values"] == {"prod_name": ["a", "b", "c"]}

    def test_deep_copied(self):
        values = ["a", "b"]
        params = _params()
        params["schema"]["categorical_values"] = {"prod_name": values}
        result = get_schema(params)
        result["categorical_values"]["prod_name"].append("c")
        assert values == ["a", "b"]


class TestTwoColumnEntityFixture:
    """Meta test for the shared ``two_column_entity_params`` fixture.

    Multi-column-entity tests are only meaningful if the parameters they run
    on actually resolve to more than one entity column. This class is what
    makes that a checked fact instead of an assumption.
    """

    def test_get_schema_sees_both_entity_columns(self, two_column_entity_params):
        schema = get_schema(two_column_entity_params)
        assert schema["entity"] == ["branch_id", "cust_id"]

    def test_identity_columns_carry_both_entity_columns(
        self, two_column_entity_params
    ):
        schema = get_schema(two_column_entity_params)
        assert schema["identity_columns"] == [
            "snap_date", "branch_id", "cust_id", "prod_name",
        ]

    def test_mis_nested_shape_raises(self, two_column_entity_params):
        """Pins the trap the fixture exists to avoid — and that it is now loud.

        Moving the column names one level up — straight under ``schema``,
        skipping ``columns`` — makes ``get_schema`` ignore the whole block.
        Before #328 it then returned the one-column built-in default: no error,
        no warning, and a test copied onto such a dict ran single-entity data
        against single-entity code and passed no matter what. With the default
        gone, the same mis-nesting raises instead.
        """
        columns = two_column_entity_params["schema"]["columns"]
        mis_nested = {"schema": dict(columns)}
        with pytest.raises(ValueError, match="Missing schema.columns"):
            get_schema(mis_nested)


class TestGetEntityGrouping:
    _PARAMS = _params(entity=["branch_id", "cust_id"])

    def test_undeclared_falls_back_to_the_whole_entity(self):
        for key in ENTITY_GROUPING_KEYS:
            assert get_entity_grouping(self._PARAMS, key) == ["branch_id", "cust_id"]

    def test_declared_value_wins(self):
        params = {**self._PARAMS, "dataset": {"train_split_keys": ["branch_id"]}}
        assert get_entity_grouping(params, "train_split_keys") == ["branch_id"]
        # The other key is unaffected — that separation is the whole point.
        assert get_entity_grouping(params, "val_sample_keys") == ["branch_id", "cust_id"]

    def test_duplicates_are_collapsed(self):
        params = {**self._PARAMS, "dataset": {"val_sample_keys": ["cust_id", "cust_id"]}}
        assert get_entity_grouping(params, "val_sample_keys") == ["cust_id"]

    def test_an_unknown_key_name_raises_instead_of_defaulting(self):
        # A typo at the call site would otherwise resolve to "nothing declared"
        # and return the full entity: a plausible answer that ignores the user's
        # config, from code A29 never inspects.
        with pytest.raises(ValueError, match="not an entity-grouping key"):
            get_entity_grouping(self._PARAMS, "train_split_key")


class TestRenamedSchemaFixture:
    """Meta test for the shared ``renamed_schema_params`` fixture.

    A test that claims to prove "renaming works" is only meaningful if the
    parameters it runs on really carry the renamed columns. This class is what
    makes that a checked fact instead of an assumption.
    """

    def test_get_schema_returns_the_renamed_names(self, renamed_schema_params):
        schema = get_schema(renamed_schema_params)
        assert schema["time"] == "as_of_month"
        assert schema["entity"] == ["store_id"]
        assert schema["item"] == "sku"

    def test_identity_columns_carry_the_renamed_names(self, renamed_schema_params):
        schema = get_schema(renamed_schema_params)
        assert schema["identity_columns"] == ["as_of_month", "store_id", "sku"]

    def test_no_example_column_name_survives(self, renamed_schema_params):
        """The point of the fixture: nothing it resolves to may collide with
        the example deployment's spellings, or code that hardcodes one of them
        keeps passing."""
        schema = get_schema(renamed_schema_params)
        resolved = {schema["time"], schema["item"], *schema["entity"]}
        assert resolved.isdisjoint({"snap_date", "cust_id", "prod_name"})

    def test_categorical_values_are_keyed_by_the_renamed_item(
        self, renamed_schema_params
    ):
        """A category list keyed by the old item name is invisible to every
        consumer, and invariant A3 would then reject the config for a reason
        that has nothing to do with what the test was about."""
        schema = get_schema(renamed_schema_params)
        assert set(schema["categorical_values"]) == {schema["item"]}

    def test_mis_nested_shape_raises(self, renamed_schema_params):
        """Pins the trap the fixture exists to avoid — and that it is now loud.

        Moving the column names one level up — straight under ``schema``,
        skipping ``columns`` — makes ``get_schema`` ignore the whole block.
        Before #328 it then handed back the built-in example names, so a test
        copied onto such a dict never exercised a renamed column at all and
        still passed. With the defaults gone, the mis-nesting raises, and the
        message names the three roles it could not find.
        """
        columns = renamed_schema_params["schema"]["columns"]
        mis_nested = {"schema": dict(columns)}
        with pytest.raises(ValueError) as exc:
            get_schema(mis_nested)
        assert "time, entity, item" in str(exc.value)

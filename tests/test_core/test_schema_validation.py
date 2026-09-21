"""Tests for schema config validation and source_etl consistency checks."""

import pytest

from recsys_tfb.core.schema import (
    get_schema_for_hash,
    validate_schema_config,
)


def _columns(**over) -> dict:
    """The three required roles, with per-test overrides.

    ``validate_schema_config`` rejects a config that omits ``time`` / ``entity``
    / ``item`` (#326), and it reports that before any of the shape checks below
    run. So a shape test written on a bare ``{"entity": []}`` would pass on the
    missing-role message instead of the one it names — green for the wrong
    reason. Passing ``None`` drops a role, for the tests that are about the
    missing-role rule itself.
    """
    columns = {"time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}
    columns.update(over)
    return {k: v for k, v in columns.items() if v is not None}


class TestRequiredRoles:
    """``time`` / ``entity`` / ``item`` name columns in the user's own tables.

    A default for them is the framework assuming the deployment is the example
    one. The three the framework produces itself (``label`` / ``score`` /
    ``rank``) keep theirs.
    """

    @pytest.mark.parametrize("role", ["time", "entity", "item"])
    def test_a_missing_role_raises_and_is_named(self, role):
        with pytest.raises(ValueError, match=f"Missing schema.columns.*{role}"):
            validate_schema_config({"schema": {"columns": _columns(**{role: None})}})

    def test_declaring_no_columns_at_all_raises(self):
        with pytest.raises(ValueError, match="Missing schema.columns"):
            validate_schema_config({"schema": {"columns": {}}})

    def test_a_config_with_no_schema_block_raises(self):
        with pytest.raises(ValueError, match="Missing schema.columns"):
            validate_schema_config({})

    def test_every_missing_role_is_reported_at_once(self):
        """One run, one message — not three runs to find three omissions."""
        with pytest.raises(ValueError) as exc:
            validate_schema_config({"schema": {"columns": {"label": "y"}}})
        assert "time, entity, item" in str(exc.value)

    def test_the_message_says_where_to_declare_them(self):
        with pytest.raises(ValueError) as exc:
            validate_schema_config({})
        assert "parameters.yaml" in str(exc.value)
        assert "columns" in str(exc.value)

    @pytest.mark.parametrize("role", ["label", "score", "rank"])
    def test_a_framework_produced_role_may_be_omitted(self, role):
        columns = dict(_columns(), label="y", score="s", rank="r")
        del columns[role]
        validate_schema_config({"schema": {"columns": columns}})


class TestValidateSchemaConfig:
    def test_only_the_required_roles_is_ok(self):
        validate_schema_config({"schema": {"columns": _columns()}})

    def test_full_valid_ok(self):
        validate_schema_config(
            {
                "schema": {
                    "columns": {
                        "time": "snap_date",
                        "entity": ["cust_id"],
                        "item": "prod_name",
                        "label": "label",
                        "score": "score",
                        "rank": "rank",
                    }
                }
            }
        )

    def test_entity_string_ok(self):
        validate_schema_config({"schema": {"columns": _columns(entity="cust_id")}})

    def test_entity_empty_list_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": _columns(entity=[])}})

    def test_entity_empty_string_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": _columns(entity="")}})

    def test_entity_list_with_empty_element_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config(
                {"schema": {"columns": _columns(entity=["cust_id", ""])}})

    def test_entity_list_with_non_string_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config(
                {"schema": {"columns": _columns(entity=["cust_id", 123])}})

    def test_entity_wrong_type_raises(self):
        with pytest.raises(ValueError, match="entity"):
            validate_schema_config({"schema": {"columns": _columns(entity=42)}})

    def test_time_non_string_raises(self):
        with pytest.raises(ValueError, match="time"):
            validate_schema_config({"schema": {"columns": _columns(time=123)}})

    def test_item_empty_string_raises(self):
        with pytest.raises(ValueError, match="item"):
            validate_schema_config({"schema": {"columns": _columns(item="")}})

    def test_identity_columns_duplicate_raises(self):
        # entity equals item → identity columns contain duplicate
        with pytest.raises(ValueError, match="duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(entity=["cust_id"], item="cust_id")}})

    def test_columns_not_a_mapping_raises(self):
        with pytest.raises(ValueError, match="mapping"):
            validate_schema_config({"schema": {"columns": ["a", "b"]}})

    def test_categorical_values_wrong_type_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {
                    "columns": _columns(), "categorical_values": ["prod_name"]}}
            )

    def test_categorical_values_empty_list_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {
                    "columns": _columns(), "categorical_values": {"prod_name": []}}}
            )

    def test_categorical_values_non_list_value_raises(self):
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(
                {"schema": {
                    "columns": _columns(),
                    "categorical_values": {"prod_name": "a,b,c"}}}
            )

    def test_identity_categorical_missing_declaration_raises(self):
        params = {
            "schema": {"columns": _columns(), "categorical_values": {}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["prod_name"],
                }
            },
        }
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(params)

    def test_identity_categorical_with_declaration_ok(self):
        params = {
            "schema": {
                "columns": _columns(),
                "categorical_values": {"prod_name": ["a", "b"]}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["prod_name"],
                }
            },
        }
        validate_schema_config(params)

    def test_non_identity_categorical_does_not_require_declaration(self):
        # cust_segment_typ is not in identity_columns; no declaration required.
        params = {
            "schema": {"columns": _columns(), "categorical_values": {}},
            "dataset": {
                "prepare_model_input": {
                    "categorical_columns": ["cust_segment_typ"],
                }
            },
        }
        validate_schema_config(params)


class TestSchemaValidationDelegatesA3:
    def test_identity_cat_missing_values_still_raises_valueerror(self):
        # behaviour preserved after delegation to consistency.resolved_item_values
        p = {
            "schema": {"columns": _columns(item="prod_name")},
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        with pytest.raises(ValueError, match="categorical_values"):
            validate_schema_config(p)


class TestGetSchemaForHash:
    def test_returns_canonical_keys_with_categorical_values(self):
        schema = get_schema_for_hash({"schema": {"columns": _columns()}})
        assert set(schema.keys()) == {
            "time", "entity", "item", "label", "score", "rank",
            "categorical_values",
        }
        assert "identity_columns" not in schema

    def test_reflects_overrides(self):
        schema = get_schema_for_hash({"schema": {"columns": _columns(
            time="month_end", entity="customer_id")}})
        assert schema["time"] == "month_end"
        assert schema["entity"] == ["customer_id"]

    def test_includes_categorical_values(self):
        schema = get_schema_for_hash({"schema": {
            "columns": _columns(),
            "categorical_values": {"prod_name": ["a", "b"]},
        }})
        assert schema["categorical_values"] == {"prod_name": ["a", "b"]}

    def test_the_payload_keys_are_the_role_keys_not_the_defaults(self):
        """Since #328 the two are different dicts, and the hash follows roles.

        ``_DEFAULTS`` lost three entries; if the payload were still built from
        it, ``base_dataset_version`` would move for every existing user. Pinned
        by name because that is exactly the drift a later cleanup would cause.
        """
        from recsys_tfb.core.schema import _DEFAULTS, _ROLE_KEYS

        schema = get_schema_for_hash({"schema": {"columns": _columns()}})
        assert set(schema) == set(_ROLE_KEYS) | {"categorical_values"}
        assert set(_DEFAULTS) < set(_ROLE_KEYS)


class TestEventRoleShape:
    """``event`` has ``entity``'s shape, checked by the same helper.

    One helper rather than a second copy of the four checks: the two roles
    really do accept the same thing, and two copies would be two messages to
    keep in step. The role name is interpolated, so the user still reads about
    the key they wrote.
    """

    def test_a_string_is_ok(self):
        validate_schema_config(
            {"schema": {"columns": _columns(event="impression_id")}}
        )

    def test_a_list_is_ok(self):
        validate_schema_config(
            {"schema": {"columns": _columns(event=["event_ts", "impression_id"])}}
        )

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="'event' list must not be empty"):
            validate_schema_config({"schema": {"columns": _columns(event=[])}})

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="'event' string must not be empty"):
            validate_schema_config({"schema": {"columns": _columns(event="  ")}})

    def test_a_non_string_element_raises(self):
        with pytest.raises(ValueError, match="'event' element at index 1"):
            validate_schema_config(
                {"schema": {"columns": _columns(event=["impression_id", 7])}}
            )

    def test_wrong_type_raises(self):
        with pytest.raises(ValueError, match="'event' must be a string or list"):
            validate_schema_config({"schema": {"columns": _columns(event=7)}})


class TestRolesMayNotOverlap:
    """One column cannot hold two roles, and identity uniqueness is what says so.

    Deliberately not a second rule: a column declared twice appears twice in
    ``identity_columns``, so the duplicate check already in place catches every
    overlap between ``time`` / ``entity`` / ``item`` / ``event``. A separate
    overlap rule would be a second definition to keep in step with this one.
    """

    def test_event_colliding_with_item_raises(self):
        with pytest.raises(ValueError, match="identity_columns contain duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(event="prod_name")}}
            )

    def test_event_colliding_with_entity_raises(self):
        with pytest.raises(ValueError, match="identity_columns contain duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(event=["cust_id"])}}
            )

    def test_event_colliding_with_time_raises(self):
        with pytest.raises(ValueError, match="identity_columns contain duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(event="snap_date")}}
            )

    def test_two_event_columns_spelling_one_name_raises(self):
        with pytest.raises(ValueError, match="identity_columns contain duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(event=["imp_id", "imp_id"])}}
            )

    @pytest.mark.parametrize("other", [
        {"event": "request_id"},       # the two optional roles
        {"item": "request_id"},
        {"entity": ["request_id"]},
        {"time": "request_id"},
    ])
    def test_occasion_colliding_with_another_role_raises(self, other):
        with pytest.raises(ValueError, match="identity_columns contain duplicates"):
            validate_schema_config(
                {"schema": {"columns": _columns(occasion="request_id", **other)}}
            )


class TestOccasionRoleShape:
    """``occasion`` goes through the same helper as ``entity`` and ``event``;
    the message names the key the user wrote."""

    def test_a_list_is_ok(self):
        validate_schema_config(
            {"schema": {"columns": _columns(occasion=["page_view_id", "request_id"])}}
        )

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="'occasion' list must not be empty"):
            validate_schema_config({"schema": {"columns": _columns(occasion=[])}})

    def test_wrong_type_raises(self):
        with pytest.raises(ValueError, match="'occasion' must be a string or list"):
            validate_schema_config({"schema": {"columns": _columns(occasion=7)}})


class TestUnknownColumnKeysAtTheCliGate:
    """The CLI entry refuses an unrecognised ``schema.columns`` key too.

    ``get_schema`` refuses it as well; this is the gate a real run hits first,
    seconds in, before a Spark session exists — the same two-gate arrangement
    the required roles have.
    """

    def test_an_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown key"):
            validate_schema_config(
                {"schema": {"columns": _columns(evnet="impression_id")}}
            )

    def test_every_unknown_key_is_reported_at_once(self):
        with pytest.raises(ValueError) as exc:
            validate_schema_config(
                {"schema": {"columns": _columns(evnet="a", occassion="b")}}
            )
        message = str(exc.value)
        assert "evnet" in message and "occassion" in message

    def test_reported_before_a_missing_role(self):
        """A typo'd role IS a missing role, so the omission message would send
        the user to add a key they already wrote. The typo has to come first."""
        with pytest.raises(ValueError) as exc:
            validate_schema_config(
                {"schema": {"columns": {"time": "snap_date", "itme": "prod_name"}}}
            )
        assert "Unknown key" in str(exc.value)

    def test_the_framework_defaults_conf_is_unaffected(self):
        """The no-op half, on the real config rather than a fixture: every key
        this framework ships names a real role, so the gate changes nothing
        for an existing deployment."""
        from pathlib import Path

        import yaml

        root = Path(__file__).resolve().parents[2]
        params = yaml.safe_load((root / "conf/base/parameters.yaml").read_text())
        validate_schema_config(params)

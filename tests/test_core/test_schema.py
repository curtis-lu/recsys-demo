"""Tests for core.schema — centralized column schema."""

import copy

import pytest

from recsys_tfb.core.schema import (
    ENTITY_GROUPING_KEYS,
    _DERIVED_KEYS,
    get_entity_grouping,
    get_schema,
    get_schema_for_hash,
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


class TestDerivedColumnLists:
    """The three derived lists, and the one rule that is not about values.

    ``query_group_columns`` and ``base_key_columns`` hold the same columns
    today and that is the whole reason they are two keys: the twenty-odd sites
    that used to spell ``[time] + entity`` by hand meant one or the other, and
    nothing in the code said which. ``occasion`` (ADR-0025) widens the first
    and must never widen the second, so the split has to exist *before* the
    values differ — after they differ it is too late to tell the sites apart.
    """

    def test_query_group_is_time_plus_entity(self):
        schema = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert schema["query_group_columns"] == [
            "snap_date", "branch_id", "cust_id"
        ]

    def test_base_key_is_time_plus_entity(self):
        schema = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert schema["base_key_columns"] == ["snap_date", "branch_id", "cust_id"]

    def test_the_two_agree_under_todays_roles(self):
        schema = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert schema["query_group_columns"] == schema["base_key_columns"]

    def test_each_list_is_its_own_object(self):
        """Mutating one must not reach the others.

        They are equal today, so a shared object would go unnoticed until a
        caller appended to what it thought was its own list and silently moved
        another site's join key.
        """
        schema = get_schema(_params())
        schema["query_group_columns"].append("occasion_id")
        assert schema["base_key_columns"] == ["snap_date", "cust_id"]
        assert schema["identity_columns"] == ["snap_date", "cust_id", "prod_name"]

    def test_get_schema_returns_every_declared_derived_key(self):
        """``_DERIVED_KEYS`` is the list, not this test's own copy.

        It is also what S5 mirrors to refuse declaring one, so a key added
        there and never derived would leave that constraint guarding a field
        nothing produces.
        """
        schema = get_schema(_params())
        assert set(_DERIVED_KEYS) <= set(schema)

    def test_no_derived_key_reaches_the_version_hash(self):
        """Each is a function of roles already hashed, so hashing one would
        move every existing user's ``base_dataset_version`` for nothing."""
        hashed = get_schema_for_hash(_params())
        assert set(_DERIVED_KEYS) & set(hashed) == set()

    def test_derived_lists_are_not_settable(self):
        """Declaring one under ``schema.columns`` raises, naming every one.

        Until #378 the ``_ROLE_KEYS`` filter dropped it instead: no error and
        no effect, the silent drop S5 exists to refuse for
        ``identity_columns``. S5 is a static scan of Python literals, so a
        ``conf/`` YAML that declares one needs this runtime half.
        """
        params = _params()
        params["schema"]["columns"]["query_group_columns"] = ["nonsense"]
        params["schema"]["columns"]["base_key_columns"] = ["nonsense"]
        with pytest.raises(ValueError) as exc:
            get_schema(params)
        assert "query_group_columns" in str(exc.value)
        assert "base_key_columns" in str(exc.value)


class TestIdentityColumnOrderIsARule:
    """``identity_columns``' order is part of the spec, not a spelling.

    Deterministic sampling buckets rows by hashing these columns joined in
    order (``spark_bucket`` / the dataset pipeline's ``_bucket``), so the same
    data reordered draws a *different* sample -- silently, with no error and no
    shape change. ADR-0025 decision 1 fixes the order for that reason and
    forbids any later widening (``occasion``, ``event``, #394's multi-column
    item) from moving the columns already in it.

    This is the test that fails if someone "tidies" the derivation.
    """

    def test_order_is_time_then_entity_then_item(self):
        schema = get_schema(_params(entity=["branch_id", "cust_id"]))
        assert schema["identity_columns"] == [
            "snap_date", "branch_id", "cust_id", "prod_name",
        ]

    def test_entity_columns_keep_declaration_order(self):
        """Not sorted, not de-duplicated into a set -- as declared."""
        schema = get_schema(_params(entity=["zz_last", "aa_first"]))
        assert schema["identity_columns"] == [
            "snap_date", "zz_last", "aa_first", "prod_name",
        ]

    def test_identity_starts_with_the_query_group(self):
        """Identity extends the query group; it does not re-order it.

        The prefix relation is what lets a site that groups and a site that
        joins be told apart by name rather than by re-deriving both.
        """
        schema = get_schema(_params(entity=["branch_id", "cust_id"]))
        qg = schema["query_group_columns"]
        assert schema["identity_columns"][: len(qg)] == qg


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
        # Since #378 the message names the real mistake — the roles are one
        # level too high — instead of reporting them as missing, which sent
        # the reader off to add keys they had already written.
        with pytest.raises(ValueError, match="Misplaced key"):
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
        # Since #378 the message names the real mistake — the roles are one
        # level too high — instead of listing them as missing, which sent the
        # reader off to add keys they had already written.
        message = str(exc.value)
        assert "Misplaced key(s) in schema" in message
        for role in ("time", "entity", "item"):
            assert role in message


class TestOptionalEventRole:
    """``event`` is absent unless declared, and widens identity when it is.

    The role exists so a query group can hold the same item more than once —
    one row per impression, each with its own realtime features (ADR-0021,
    ADR-0025 decision 1). Everything here is about the *declared vs absent*
    split, because absent is the state every deployment that predates #378 is
    in and the one that must not move.
    """

    def test_absent_unless_declared(self):
        assert "event" not in get_schema(_params())

    def test_identity_is_unchanged_when_absent(self):
        assert get_schema(_params())["identity_columns"] == [
            "snap_date", "cust_id", "prod_name",
        ]

    def test_a_declared_string_normalises_to_a_list(self):
        """Same normalisation ``entity`` gets, so callers never branch on type."""
        assert get_schema(_params(event="impression_id"))["event"] == [
            "impression_id",
        ]

    def test_a_declared_list_is_kept(self):
        schema = get_schema(_params(event=["event_ts", "impression_id"]))
        assert schema["event"] == ["event_ts", "impression_id"]

    def test_event_is_appended_after_item(self):
        """ADR-0025 decision 1 fixes the position: the columns already in
        ``identity_columns`` do not move, so an existing deployment that adds
        ``event`` keeps drawing the same deterministic sample for the rows it
        already had."""
        schema = get_schema(_params(event=["event_ts", "impression_id"]))
        assert schema["identity_columns"] == [
            "snap_date", "cust_id", "prod_name", "event_ts", "impression_id",
        ]

    def test_event_does_not_widen_the_query_group(self):
        """Two impressions of one item compete for rank inside one ranking;
        they do not form two rankings. ``occasion`` is the role that widens
        the query group (``TestOptionalOccasionRole``)."""
        schema = get_schema(_params(event="impression_id"))
        assert schema["query_group_columns"] == ["snap_date", "cust_id"]

    def test_event_does_not_widen_the_base_key(self):
        """An entity-level table (a feature table) has no column for an
        impression, so the key it joins by must not grow one."""
        schema = get_schema(_params(event="impression_id"))
        assert schema["base_key_columns"] == ["snap_date", "cust_id"]


class TestOptionalOccasionRole:
    """``occasion`` is absent unless declared; declared, it widens the query
    group and identity — and never the base key.

    The role says "these rows were ranked together, at one moment" (one
    request, say), so ranks are compared within it: query group ＝ ``time`` ＋
    ``entity`` ＋ ``occasion`` (ADR-0025 decision 1). The base key is what an
    entity-level table joins by, and such a table has no column for a request
    — widening it would make every feature join match nothing.
    """

    def test_absent_unless_declared(self):
        assert "occasion" not in get_schema(_params())

    def test_a_declared_string_normalises_to_a_list(self):
        assert get_schema(_params(occasion="request_id"))["occasion"] == [
            "request_id",
        ]

    def test_occasion_widens_the_query_group(self):
        schema = get_schema(
            _params(entity=["branch_id", "cust_id"],
                    occasion=["page_view_id", "request_id"])
        )
        assert schema["query_group_columns"] == [
            "snap_date", "branch_id", "cust_id", "page_view_id", "request_id",
        ]

    def test_occasion_does_not_widen_the_base_key(self):
        schema = get_schema(_params(occasion="request_id"))
        assert schema["base_key_columns"] == ["snap_date", "cust_id"]

    def test_occasion_sits_between_entity_and_item_in_identity(self):
        """ADR-0025 decision 1 fixes the position. Between entity and item —
        not appended — because that keeps identity's prefix equal to the query
        group, and ``time``/``entity``/``item`` keep their relative order."""
        schema = get_schema(_params(occasion="request_id"))
        assert schema["identity_columns"] == [
            "snap_date", "cust_id", "request_id", "prod_name",
        ]

    def test_both_roles_declared_order(self):
        schema = get_schema(
            _params(occasion="request_id", event="impression_id")
        )
        assert schema["identity_columns"] == [
            "snap_date", "cust_id", "request_id", "prod_name", "impression_id",
        ]
        assert schema["query_group_columns"] == [
            "snap_date", "cust_id", "request_id",
        ]

    def test_identity_still_starts_with_the_query_group(self):
        schema = get_schema(_params(occasion="request_id", event="impression_id"))
        qg = schema["query_group_columns"]
        assert schema["identity_columns"][: len(qg)] == qg


class TestOptionalRolesAndTheVersionHash:
    """Declared ⇒ hashed; absent ⇒ not a key of the payload at all.

    Both halves are load-bearing. An undeclared ``event`` that reached the
    payload as ``None`` would move ``base_dataset_version`` for every
    deployment that never asked for the role; a declared one that stayed out
    would let a deployment switch to one row per impression and silently read
    back the artifacts of the old shape.
    """

    def test_absent_event_adds_no_key_to_the_payload(self):
        assert "event" not in get_schema_for_hash(_params())

    def test_absent_event_leaves_the_payload_untouched(self):
        """Equality against the payload keys, not a subset check: a subset
        check passes just as happily when a key was added."""
        assert list(get_schema_for_hash(_params())) == [
            "time", "entity", "item", "label", "score", "rank",
            "categorical_values",
        ]

    def test_a_declared_event_is_in_the_payload(self):
        payload = get_schema_for_hash(_params(event="impression_id"))
        assert payload["event"] == ["impression_id"]

    def test_declaring_event_changes_the_payload(self):
        assert get_schema_for_hash(_params()) != get_schema_for_hash(
            _params(event="impression_id")
        )

    def test_absent_occasion_adds_no_key_to_the_payload(self):
        assert "occasion" not in get_schema_for_hash(_params(event="impression_id"))

    def test_an_event_only_payload_is_unmoved_by_occasion_existing(self):
        """A deployment that declared ``event`` under #378 keeps its
        ``base_dataset_version``: the payload keys are exactly what they were."""
        assert list(get_schema_for_hash(_params(event="impression_id"))) == [
            "time", "entity", "item", "label", "score", "rank", "event",
            "categorical_values",
        ]

    def test_a_declared_occasion_is_in_the_payload(self):
        payload = get_schema_for_hash(_params(occasion="request_id"))
        assert payload["occasion"] == ["request_id"]

    def test_declaring_occasion_changes_the_payload(self):
        assert get_schema_for_hash(_params()) != get_schema_for_hash(
            _params(occasion="request_id")
        )


class TestUnknownSchemaColumnKeys:
    """A key naming no role raises, and every one is reported at once.

    Before #378 it was dropped by the merge filter: no message, and — because
    a dropped key never reaches ``get_schema_for_hash`` — no version ID moved
    either, so ``evnet: impression_id`` produced a run that finished and
    ranked one row per item while the operator believed otherwise.
    """

    def test_an_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown key"):
            get_schema(_params(evnet="impression_id"))

    def test_the_message_names_the_offending_key(self):
        with pytest.raises(ValueError) as exc:
            get_schema(_params(evnet="impression_id"))
        assert "evnet" in str(exc.value)

    def test_every_unknown_key_is_reported_at_once(self):
        """One re-run per typo is the cost this collects away."""
        with pytest.raises(ValueError) as exc:
            get_schema(_params(evnet="a", itme="b"))
        message = str(exc.value)
        assert "evnet" in message and "itme" in message

    def test_the_message_lists_the_recognised_roles(self):
        with pytest.raises(ValueError) as exc:
            get_schema(_params(evnet="a"))
        message = str(exc.value)
        for role in ("time", "entity", "item", "label", "score", "rank", "event"):
            assert role in message

    def test_a_config_with_only_known_roles_is_accepted(self):
        """The no-op half: naming every settable role at once must pass, or
        the gate would be rejecting valid configs rather than typos."""
        schema = get_schema(
            _params(label="y", score="s", rank="r", event="impression_id")
        )
        assert schema["label"] == "y"
        assert schema["event"] == ["impression_id"]


class TestRoleWrittenOneLevelTooHigh:
    """A role under ``schema:`` instead of ``schema.columns:`` raises.

    S5 (docs/agents/architecture-constraints.md) already refuses this shape,
    but it is an AST scan of Python literals under ``src/`` and ``tests/`` —
    and the place a deployment writes its schema is ``conf/*.yaml``, which no
    scan reads. Until #378's review the runtime half did not exist, so
    ``schema: {event: ..., columns: {...}}`` resolved to the undeclared shape:
    no message, no version ID moved, and the run ranked one row per item.
    """

    @staticmethod
    def _one_level_up(**over):
        return {"schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name"},
            **over,
        }}

    def test_an_optional_role_one_level_up_raises(self):
        with pytest.raises(ValueError, match="Misplaced key"):
            get_schema(self._one_level_up(event="impression_id"))

    def test_a_required_role_one_level_up_raises(self):
        with pytest.raises(ValueError, match="Misplaced key"):
            get_schema(self._one_level_up(item="other"))

    def test_a_derived_key_one_level_up_raises(self):
        """Declaring one is meaningless wherever it is written; up here it was
        the one spelling neither gate saw."""
        with pytest.raises(ValueError, match="Misplaced key"):
            get_schema(self._one_level_up(identity_columns=["a"]))

    def test_every_misplaced_key_is_reported_at_once(self):
        with pytest.raises(ValueError) as exc:
            get_schema(self._one_level_up(event="i", score="s"))
        message = str(exc.value)
        assert "event" in message and "score" in message

    def test_the_message_says_where_they_belong(self):
        with pytest.raises(ValueError) as exc:
            get_schema(self._one_level_up(event="i"))
        assert "columns" in str(exc.value)

    def test_the_legitimate_schema_keys_still_pass(self):
        """The discriminating case: ``categorical_values`` sits under
        ``schema`` by design, so a gate that refused every non-``columns`` key
        would reject the shape this framework ships."""
        schema = get_schema({"schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name"},
            "categorical_values": {"prod_name": ["a", "b"]},
        }})
        assert schema["categorical_values"] == {"prod_name": ["a", "b"]}

    def test_the_framework_defaults_conf_passes(self):
        """The no-op half, on the real config."""
        from pathlib import Path

        import yaml

        root = Path(__file__).resolve().parents[2]
        params = yaml.safe_load((root / "conf/base/parameters.yaml").read_text())
        get_schema(params)


class TestMultiColumnItem:
    """``item`` may name several columns; they are combined into one column
    called ``item`` on read, so everything downstream sees one column (#394,
    ADR-0027 decision 1).

    The single-column spellings must stay byte-for-byte what they were: that is
    what keeps every existing deployment's ``base_dataset_version`` (and so its
    ``model_version``) where it is.
    """

    def test_a_single_column_is_unchanged(self):
        schema = get_schema(_params())
        assert schema["item"] == "prod_name"
        assert schema["item_source_columns"] == ["prod_name"]

    def test_a_one_element_list_is_the_single_column(self):
        schema = get_schema(_params(item=["prod_name"]))
        assert schema["item"] == "prod_name"
        assert schema["item_source_columns"] == ["prod_name"]
        assert schema["identity_columns"] == ["snap_date", "cust_id", "prod_name"]

    def test_a_one_element_list_hashes_like_the_string(self):
        assert get_schema_for_hash(_params(item=["prod_name"])) == get_schema_for_hash(
            _params()
        )

    def test_several_columns_resolve_to_the_combined_column(self):
        schema = get_schema(_params(item=["campaign_id", "creative_format"]))
        assert schema["item"] == "item"
        assert schema["item_source_columns"] == ["campaign_id", "creative_format"]

    def test_identity_holds_the_combined_column_not_the_sources(self):
        schema = get_schema(
            _params(item=["campaign_id", "creative_format"], occasion="request_id")
        )
        assert schema["identity_columns"] == [
            "snap_date", "cust_id", "request_id", "item",
        ]

    def test_the_hash_carries_the_declared_columns(self):
        """``item`` alone would collide with a deployment whose single item
        column is literally named ``item`` — two different datasets, one
        version ID."""
        combined = get_schema_for_hash(_params(item=["campaign_id", "creative_format"]))
        literal = get_schema_for_hash(_params(item="item"))
        assert combined["item"] == ["campaign_id", "creative_format"]
        assert combined != literal

    def test_column_order_is_part_of_the_hash(self):
        """``[a, b]`` and ``[b, a]`` combine to different values."""
        assert get_schema_for_hash(
            _params(item=["campaign_id", "creative_format"])
        ) != get_schema_for_hash(_params(item=["creative_format", "campaign_id"]))

    def test_the_source_list_is_a_copy(self):
        params = _params(item=["campaign_id", "creative_format"])
        get_schema(params)["item_source_columns"].append("x")
        assert get_schema(params)["item_source_columns"] == [
            "campaign_id", "creative_format",
        ]

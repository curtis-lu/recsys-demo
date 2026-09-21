"""Offline inference ignores the optional column roles (ADR-0025 decision 1)."""

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.inference.steps.identity import scored_row_columns


def _schema(event=None, occasion=None):
    columns = {"time": "snap_date", "entity": ["user_id", "slot_id"],
               "item": "ad_creative"}
    if event is not None:
        columns["event"] = event
    if occasion is not None:
        columns["occasion"] = occasion
    return get_schema({"schema": {"columns": columns}})


def test_without_an_optional_role_it_is_the_whole_identity():
    schema = _schema()
    assert scored_row_columns(schema) == schema["identity_columns"]


def test_a_declared_event_column_is_dropped():
    """Inference manufactures its candidates as entity x item, so no row it
    writes has an impression to name. Asking a frame for that column raised
    `Column 'impression_id' does not exist` inside
    build_inference_population_features — caught by the ad example's
    end-to-end run, not by any unit test."""
    assert scored_row_columns(_schema(event="impression_id")) == [
        "snap_date", "user_id", "slot_id", "ad_creative",
    ]


def test_every_declared_event_column_is_dropped():
    assert scored_row_columns(_schema(event=["event_ts", "impression_id"])) == [
        "snap_date", "user_id", "slot_id", "ad_creative",
    ]


def test_a_declared_occasion_column_is_dropped():
    """Inference has no request either: it ranks every configured item for
    each entity at a time, so its query group stays ``time`` + ``entity``
    (ADR-0025 decision 2) and its rows carry no occasion column (#428)."""
    assert scored_row_columns(_schema(occasion="request_id")) == [
        "snap_date", "user_id", "slot_id", "ad_creative",
    ]


def test_both_roles_are_dropped_together():
    assert scored_row_columns(
        _schema(occasion="request_id", event="impression_id")
    ) == ["snap_date", "user_id", "slot_id", "ad_creative"]


def test_what_is_left_is_the_base_key_plus_item_under_occasion():
    """The value coincides with ``base_key_columns + [item]`` — the key the
    pipeline's rank partition and completeness check use. If it ever did not,
    inference would rank on one key and identify rows by another."""
    schema = _schema(occasion="request_id")
    cols = scored_row_columns(schema)
    assert [c for c in cols if c != schema["item"]] == schema["base_key_columns"]


def test_the_surviving_order_is_identitys_order():
    """Not rebuilt in some order of its own: the list is identity with entries
    removed, so what is left keeps identity's order — which ADR-0025 decision 1
    makes a rule rather than a spelling."""
    schema = _schema(event="impression_id")
    survivors = scored_row_columns(schema)
    assert survivors == [c for c in schema["identity_columns"] if c in survivors]


def test_the_three_inference_call_sites_use_it():
    """The discriminating check: a fourth site that reads
    schema["identity_columns"] directly would ask for a column the frame does
    not have, and only an end-to-end run would notice."""
    import inspect

    from recsys_tfb.pipelines.inference import nodes
    from recsys_tfb.pipelines.inference.steps import validation

    for module in (nodes, validation):
        # Comment lines dropped: the three call sites each carry a comment
        # saying what they are NOT reading, and matching those would make this
        # test pass or fail on prose.
        code = [
            line for line in inspect.getsource(module).splitlines()
            if not line.lstrip().startswith("#")
        ]
        assert not [
            line for line in code if 'schema["identity_columns"]' in line
        ], module.__name__

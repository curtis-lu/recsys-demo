"""B15 / B16 — the invariants a multi-column ``item`` adds (#394, ADR-0027).

B15: two different combinations of the item columns combine to one value
(``a-b`` + ``c`` and ``a`` + ``b-c`` are both ``a-b-c``), so two items would be
treated as one. B16: a table that carries items lacks one of the item columns,
or already has a column named ``item`` that combining would overwrite.
"""

from recsys_tfb.core.consistency import (
    combined_item_collision_errors,
    item_source_column_errors,
)
from recsys_tfb.core.schema import get_schema


def _schema(item):
    return get_schema({"schema": {"columns": {
        "time": "snap_date", "entity": ["user_id"], "item": item,
    }}})


MULTI = _schema(["campaign_id", "creative_format"])


class TestCombinedItemCollisionB15:
    def test_two_combinations_with_one_value_are_reported(self):
        errors = combined_item_collision_errors(
            ["campaign_id", "creative_format"],
            [(("a-b", "c"), "a-b-c"), (("a", "b-c"), "a-b-c"), (("x", "y"), "x-y")],
        )
        assert len(errors) == 1
        assert errors[0].startswith("B15:")
        assert "'a-b-c'" in errors[0]
        assert "('a', 'b-c')" in errors[0]
        assert "('a-b', 'c')" in errors[0]

    def test_a_value_holding_a_dash_without_a_collision_passes(self):
        assert combined_item_collision_errors(
            ["campaign_id", "creative_format"],
            [(("cmp-01", "banner"), "cmp-01-banner"), (("cmp-02", "video"), "cmp-02-video")],
        ) == []

    def test_the_same_combination_seen_twice_is_not_a_collision(self):
        """sample_pool and label_table both hold ``c01-banner``: one item."""
        assert combined_item_collision_errors(
            ["campaign_id", "creative_format"],
            [(("c01", "banner"), "c01-banner"), (("c01", "banner"), "c01-banner")],
        ) == []

    def test_a_null_value_is_not_a_collision(self):
        """A null part makes the value null; nulls combine nothing."""
        assert combined_item_collision_errors(
            ["campaign_id", "creative_format"],
            [(("a", None), None), ((None, "b"), None)],
        ) == []

    def test_every_colliding_value_is_reported_at_once(self):
        errors = combined_item_collision_errors(
            ["campaign_id", "creative_format"],
            [
                (("a-b", "c"), "a-b-c"), (("a", "b-c"), "a-b-c"),
                (("p-q", "r"), "p-q-r"), (("p", "q-r"), "p-q-r"),
            ],
        )
        assert len(errors) == 2


class TestItemSourceColumnsB16:
    def test_a_single_column_item_is_not_checked(self):
        assert item_source_column_errors(_schema("prod_name"), "sample_pool", []) == []

    def test_all_columns_present_passes(self):
        assert item_source_column_errors(
            MULTI, "sample_pool", ["snap_date", "user_id", "campaign_id", "creative_format"],
        ) == []

    def test_a_missing_column_names_the_table_and_the_column(self):
        errors = item_source_column_errors(
            MULTI, "label_table", ["snap_date", "user_id", "campaign_id", "label"],
        )
        assert len(errors) == 1
        assert errors[0].startswith("B16:")
        assert "label_table" in errors[0]
        assert "['creative_format']" in errors[0]

    def test_an_existing_item_column_is_refused(self):
        errors = item_source_column_errors(
            MULTI, "candidate_feature_table",
            ["snap_date", "user_id", "campaign_id", "creative_format", "item"],
        )
        assert len(errors) == 1
        assert "candidate_feature_table already has a column named 'item'" in errors[0]

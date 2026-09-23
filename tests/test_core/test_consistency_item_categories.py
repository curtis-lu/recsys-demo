"""A51 — ``evaluation.item_categories.column`` (#379): categories read off a
sample_pool column instead of a hand-written ``mapping``."""

import pytest

from recsys_tfb.core.consistency import (
    item_category_column,
    item_category_column_errors,
    item_category_conflict_errors,
)


def _params(**block):
    return {"evaluation": {"item_categories": {"enabled": True,
                                               "unmapped": "singleton",
                                               **block}}}


class TestItemCategoryColumn:
    def test_column_mode(self):
        assert item_category_column(_params(column="campaign_id")) == "campaign_id"

    def test_mapping_mode(self):
        assert item_category_column(_params(mapping={"c": ["a"]})) is None

    def test_disabled_is_not_column_mode(self):
        """``enabled: false`` switches every category pass off, the column
        with it."""
        params = _params(column="campaign_id")
        params["evaluation"]["item_categories"]["enabled"] = False
        assert item_category_column(params) is None

    def test_no_block(self):
        assert item_category_column({}) is None


class TestItemCategoryColumnErrors:
    def test_column_under_post_training_is_accepted(self):
        assert item_category_column_errors(
            _params(column="campaign_id"), post_training=True) == []

    def test_mapping_is_accepted_in_both_modes(self):
        params = _params(mapping={"c01": ["c01-banner"]})
        assert item_category_column_errors(params, post_training=False) == []
        assert item_category_column_errors(params, post_training=True) == []

    def test_both_column_and_mapping_is_refused(self):
        errors = item_category_column_errors(
            _params(column="campaign_id", mapping={"c01": ["c01-banner"]}),
            post_training=True)
        assert len(errors) == 1
        assert errors[0].startswith("A51:")
        assert "column" in errors[0] and "mapping" in errors[0]

    def test_a_null_mapping_is_no_mapping(self):
        """``mapping: null`` is how an env overlay switches off a mapping the
        base conf declares (a deep merge cannot delete a key); the reader
        treats it as no mapping, so A51 does too."""
        assert item_category_column_errors(
            _params(column="campaign_id", mapping=None), post_training=True) == []

    @pytest.mark.parametrize("unmapped", ["drop", None])
    def test_column_mode_takes_only_the_singleton_rule(self, unmapped):
        """The hand-mapping path refuses anything but ``singleton`` at run
        time; column mode never reaches that check, so A51 is where a
        ``unmapped: drop`` would otherwise be silently read as singleton."""
        errors = item_category_column_errors(
            _params(column="campaign_id", unmapped=unmapped), post_training=True)
        assert len(errors) == 1
        assert errors[0].startswith("A51:") and "unmapped" in errors[0]

    def test_column_without_post_training_is_refused(self):
        """Monitoring — and ``--compare-only`` without ``--post-training``,
        which is monitoring too — evaluates inference_population, which has no
        sample_pool column to read categories from."""
        errors = item_category_column_errors(
            _params(column="campaign_id"), post_training=False)
        assert len(errors) == 1
        assert errors[0].startswith("A51:")
        assert "--post-training" in errors[0]
        assert "--compare-only" in errors[0]

    def test_disabled_column_is_inert_in_monitoring(self):
        params = _params(column="campaign_id")
        params["evaluation"]["item_categories"]["enabled"] = False
        assert item_category_column_errors(params, post_training=False) == []

    @pytest.mark.parametrize("value", ["", ["campaign_id"], 3, None])
    def test_column_must_be_a_non_empty_string(self, value):
        errors = item_category_column_errors(
            _params(column=value), post_training=True)
        assert len(errors) == 1 and errors[0].startswith("A51:")

    def test_no_block(self):
        assert item_category_column_errors({}, post_training=False) == []


class TestItemCategoryConflictsB18:
    """B18 — one item, one non-NULL category over the evaluated months."""

    def test_one_category_per_item_is_clean(self):
        rows = [("2025-01-31", "A", "x"), ("2025-02-28", "A", "x"),
                ("2025-01-31", "B", "y")]
        assert item_category_conflict_errors("family", rows) == []

    def test_a_null_is_not_a_second_category(self):
        rows = [("2025-01-31", "A", "x"), ("2025-01-31", "A", None)]
        assert item_category_conflict_errors("family", rows) == []

    def test_each_category_is_named_with_its_months(self):
        rows = [("2025-01-31", "A", "x"), ("2025-02-28", "A", "x"),
                ("2025-03-31", "A", "w"), ("2025-01-31", "B", "y")]
        errors = item_category_conflict_errors("family", rows)
        assert len(errors) == 1
        assert errors[0].startswith("B18:")
        assert "'A'" in errors[0] and "'B'" not in errors[0]
        assert "'w' (2025-03-31)" in errors[0]
        assert "'x' (2025-01-31, 2025-02-28)" in errors[0]
        assert "family" in errors[0]

    def test_every_conflicting_item_is_reported(self):
        rows = [("m", "A", "x"), ("m", "A", "w"), ("m", "B", "y"), ("m", "B", "v")]
        assert len(item_category_conflict_errors("family", rows)) == 2

    def test_a_null_item_is_skipped(self):
        rows = [("m", None, "x"), ("m", None, "w")]
        assert item_category_conflict_errors("family", rows) == []


def test_an_all_null_integer_item_is_its_own_category_as_text():
    """Categories are read as text, so the singleton is too: one value type
    in the table whatever the item column's type."""
    from recsys_tfb.pipelines.evaluation.steps.item_categories import (
        category_of_each_item,
    )

    assert category_of_each_item([("m", 7, None), ("m", 8, "x")]) == {7: "7", 8: "x"}

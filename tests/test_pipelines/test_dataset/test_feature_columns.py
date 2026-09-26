"""Tests for ``pipelines/dataset/steps/feature_columns.py`` — pure column-name derivation.

No Spark anywhere in this file, by construction: the module it covers is the
dataset pipeline's pure half.
"""

from recsys_tfb.pipelines.dataset.steps.feature_columns import (
    compute_feature_columns,
    prepare_model_input_config,
)


class TestPrepareModelInputConfigDefaults:
    """What a deployment gets when it leaves ``prepare_model_input`` unset.

    Pinned here because no shipped config reaches it: both example configs
    write ``drop_columns`` out, so an end-to-end run cannot tell a changed
    default from an unchanged one (ADR-0029 decision 10).
    """

    # Role names no example uses, and a two-column entity: a default spelled
    # in example column names would still pass with the bank example's
    # ``snap_date`` / ``cust_id``.
    PARAMS = {
        "schema": {
            "columns": {
                "time": "period", "entity": ["account", "branch"],
                "item": "offer", "label": "converted",
            },
        },
    }

    def test_drop_columns_default_to_the_time_entity_and_label_roles(self):
        drop, _ = prepare_model_input_config(self.PARAMS)

        # Equality: a column name added beside the roles (the bank example's
        # apply_start_date / apply_end_date / cust_segment_typ used to be)
        # fails here as surely as a missing role does.
        assert drop == ["period", "account", "branch", "converted"]

    def test_categorical_columns_default_to_the_item(self):
        _, categorical = prepare_model_input_config(self.PARAMS)

        assert categorical == ["offer"]

    def test_a_declared_drop_list_is_used_as_written(self):
        params = {
            **self.PARAMS,
            "dataset": {"prepare_model_input": {"drop_columns": ["period"]}},
        }
        drop, _ = prepare_model_input_config(params)

        assert drop == ["period"]


class TestComputeFeatureColumnsDrops:
    """``drop_columns`` keeps a feature_table column out of ``feature_columns``.

    Needs no Spark: this is the pure derivation that decides the blacklist, and
    it is the step ``apply_preprocessor_to_features`` then uses to physically
    build ``keep_cols`` — so a column excluded here never reaches
    ``preprocessed_feature_table`` at all (ADR-0004).

    Every column named in ``drop`` below is present in ``FT_COLS``. The previous
    coverage for this behaviour asserted that three columns were absent from an
    output when the input frame never contained them either, which held whether
    or not drop_columns did anything.
    """

    FT_COLS = [
        "snap_date", "cust_id", "cust_segment_typ", "total_aum", "tenure_months", "label",
    ]
    IDENTITY = ["snap_date", "cust_id", "prod_name"]

    def _compute(self, drop):
        return compute_feature_columns(
            self.FT_COLS, self.IDENTITY, ["prod_name"], drop, "label"
        )

    def test_a_real_feature_table_column_is_dropped(self):
        assert "cust_segment_typ" in self.FT_COLS  # the column really is there
        # Equality, not `not in`: it also pins down that dropping one column
        # left the others alone.
        assert self._compute(["snap_date", "cust_id", "label", "cust_segment_typ"]) == [
            "prod_name", "total_aum", "tenure_months",
        ]

    def test_the_same_column_survives_when_not_dropped(self):
        # The paired half: without the drop entry the column is a feature. The
        # two together are what make the first one evidence about drop_columns
        # rather than about some other exclusion rule.
        assert self._compute(["snap_date", "cust_id", "label"]) == [
            "prod_name", "cust_segment_typ", "total_aum", "tenure_months",
        ]

    def test_dropping_several_columns_removes_all_of_them(self):
        assert self._compute(
            ["snap_date", "cust_id", "label", "cust_segment_typ", "tenure_months"]
        ) == ["prod_name", "total_aum"]

    def test_label_is_excluded_even_when_drop_columns_omits_it(self):
        # The label has its own exclusion rule, separate from the blacklist.
        # Every other case here lists "label" in drop, which would keep passing
        # if that rule were deleted — the blacklist alone already covers it.
        assert "label" in self.FT_COLS
        assert self._compute(["snap_date", "cust_id"]) == [
            "prod_name", "cust_segment_typ", "total_aum", "tenure_months",
        ]

"""Tests for recsys_tfb.core.consistency."""

import pytest

from recsys_tfb.core.consistency import (
    ConsistencyError,
    ConfigConsistencyError,
    DataConsistencyError,
)


class TestExceptionHierarchy:
    def test_consistency_error_is_valueerror(self):
        assert issubclass(ConsistencyError, ValueError)

    def test_config_error_is_consistency_error(self):
        assert issubclass(ConfigConsistencyError, ConsistencyError)

    def test_data_error_is_consistency_error(self):
        assert issubclass(DataConsistencyError, ConsistencyError)


from recsys_tfb.core.consistency import resolved_item_values


class TestResolvedItemValues:
    def _params(self, **over):
        p = {
            "schema": {
                "columns": {
                    "time": "snap_date", "entity": ["cust_id"],
                    "item": "prod_name",
                },
                "categorical_values": {"prod_name": ["b", "a", "c"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
        }
        p.update(over)
        return p

    def test_returns_sorted_declared_values(self):
        assert resolved_item_values(self._params()) == ["a", "b", "c"]

    def test_respects_custom_item_name(self):
        p = {
            "schema": {
                "columns": {
                    "time": "snap_date", "entity": ["cust_id"],
                    "item": "channel_name",
                },
                "categorical_values": {"channel_name": ["sms", "app"]},
            },
            "dataset": {"prepare_model_input": {"categorical_columns": ["channel_name"]}},
        }
        assert resolved_item_values(p) == ["app", "sms"]

    def test_item_declared_categorical_but_no_values_raises(self):
        p = self._params()
        del p["schema"]["categorical_values"]["prod_name"]
        with pytest.raises(ConfigConsistencyError, match=r"schema\.categorical_values\.prod_name"):
            resolved_item_values(p)


from recsys_tfb.core.consistency import config_role_conflicts


class TestConfigRoleConflicts:
    def _params(self, drop, cat):
        return {"dataset": {"prepare_model_input": {
            "drop_columns": drop, "categorical_columns": cat}}}

    def test_no_overlap_returns_empty(self):
        assert config_role_conflicts(
            self._params(["snap_date", "label"], ["prod_name"])) == []

    def test_overlap_returns_offending_columns_sorted(self):
        assert config_role_conflicts(
            self._params(["cust_segment_typ", "label"],
                         ["prod_name", "cust_segment_typ"])) == ["cust_segment_typ"]

    def test_missing_keys_returns_empty(self):
        assert config_role_conflicts({}) == []


from recsys_tfb.core.consistency import (
    inference_products_mismatch,
    override_unknown_items,
    item_missing_from_categorical,
)


def _base(over=None):
    p = {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name",
            },
            "categorical_values": {"prod_name": ["a", "b"]},
        },
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
    }
    if over:
        p.update(over)
    return p


class TestInferenceProductsMismatch:
    def test_equal_sets_returns_empty(self):
        p = _base({"inference": {"products": ["b", "a"]}})
        assert inference_products_mismatch(p) == {"only_in_inference": [],
                                                  "only_in_categorical": []}

    def test_reports_both_directions(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        assert inference_products_mismatch(p) == {
            "only_in_inference": ["c"], "only_in_categorical": ["b"]}

    def test_no_inference_section_returns_empty(self):
        assert inference_products_mismatch(_base()) == {
            "only_in_inference": [], "only_in_categorical": []}


class TestOverrideUnknownItems:
    def test_unknown_item_component_detected(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_ratio_overrides": {"mass|a|0": 0.5, "mass|zzz|0": 0.9}}})
        assert override_unknown_items(p) == ["zzz"]

    def test_item_not_in_group_keys_skipped(self):
        p = _base({"dataset": {"prepare_model_input": {
            "categorical_columns": ["prod_name"]},
            "sample_group_keys": ["cust_segment_typ"],
            "sample_ratio_overrides": {"mass": 0.5}}})
        assert override_unknown_items(p) == []


class TestItemMissingFromCategorical:
    def test_item_present_ok(self):
        assert item_missing_from_categorical(_base()) is False

    def test_item_absent_detected(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["categorical_columns"] = ["gender"]
        assert item_missing_from_categorical(p) is True

    def test_key_absent_uses_default_includes_item(self):
        p = _base()
        del p["dataset"]["prepare_model_input"]["categorical_columns"]
        assert item_missing_from_categorical(p) is False


from recsys_tfb.core.consistency import validate_config_consistency


class TestValidateConfigConsistency:
    def test_clean_config_passes(self):
        validate_config_consistency(_base({"inference": {"products": ["a", "b"]}}))

    def test_a1_conflict_message_names_both_resolutions(self):
        p = _base()
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["cust_segment_typ"]
        p["dataset"]["prepare_model_input"]["categorical_columns"] = [
            "prod_name", "cust_segment_typ"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "cust_segment_typ" in msg
        assert "remove from drop_columns" in msg
        assert "remove from categorical_columns" in msg

    def test_collects_multiple_errors_in_one_raise(self):
        p = _base({"inference": {"products": ["a", "c"]}})
        p["dataset"]["prepare_model_input"]["drop_columns"] = ["prod_name"]
        with pytest.raises(ConfigConsistencyError) as ei:
            validate_config_consistency(p)
        msg = str(ei.value)
        assert "prod_name" in msg          # A1 (prod_name in drop ∩ categorical)
        assert "c" in msg                  # A4 only_in_inference

    def test_a9_unknown_weight_product_collected(self):
        p = _base({"inference": {"products": ["a", "b"]},
            "training": {
                "sample_weight_keys": ["cust_segment_typ", "prod_name"],
                "sample_weights": {"mass|zzz": 2.0}}})
        with pytest.raises(ConfigConsistencyError, match=r"training\.sample_weights"):
            validate_config_consistency(p)

    def test_all_three_a9_errors_collected(self):
        p = _base({
            "training": {
                "sample_weight_keys": ["cust_segment_typ", "prod_name"],
                "sample_weights": {"mass|zzz": 2.0, "badkey": 3.0}}})
        # cust_segment_typ not carried (A9a), "badkey" wrong arity (A9b),
        # "zzz" unknown product (A9c)
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(p)
        msg = str(exc.value)
        # discriminating substrings unique to each error message
        assert "carry_columns" in msg               # A9a
        assert "segment(s) to match" in msg          # A9b
        assert "schema.categorical_values[item]" in msg  # A9c

    def test_a14_feature_selection_excludes_item_collected(self):
        p = _base({
            "inference": {"products": ["a", "b"]},
            "training": {"feature_selection": {"exclude": ["prod_name"]}},
        })
        with pytest.raises(ConfigConsistencyError, match=r"feature_selection"):
            validate_config_consistency(p)


from recsys_tfb.core.consistency import feature_selection_excludes_item


class TestFeatureSelectionExcludesItem:
    def test_no_feature_selection_ok(self):
        assert feature_selection_excludes_item(_base()) is False

    def test_exclude_without_item_ok(self):
        p = _base({"training": {"feature_selection": {"exclude": ["feat_a"]}}})
        assert feature_selection_excludes_item(p) is False

    def test_exclude_contains_item_detected(self):
        p = _base({"training": {"feature_selection": {"exclude": ["prod_name"]}}})
        assert feature_selection_excludes_item(p) is True


class TestSparkGuardUsesSharedError:
    def test_missing_cats_raises_data_consistency_error_subclass(self):
        # DataConsistencyError is still a ValueError, preserving callers
        assert issubclass(DataConsistencyError, ValueError)


from recsys_tfb.core.consistency import item_coverage_errors


class TestItemCoverageErrors:
    DECL = ["a", "b", "c"]

    def test_equal_sets_returns_empty(self):
        assert item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a", "b"}) == []

    def test_sample_pool_unknown_value_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "c", "ploan"}, {"a"})
        assert len(errs) == 1
        assert "ploan" in errs[0]
        assert "sample_pool" in errs[0] and "-1" in errs[0]

    def test_sample_pool_declared_but_absent_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b"}, {"a", "b"})
        assert len(errs) == 1
        assert "'c'" in errs[0] or "c" in errs[0]
        assert "never produces" in errs[0]

    def test_label_unknown_value_is_error(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a", "mloan"})
        assert len(errs) == 1
        assert "mloan" in errs[0]
        assert "label_table" in errs[0] and "label_*.sql" in errs[0]

    def test_label_declared_but_absent_is_NOT_error_b3_deferred(self):
        # label_items missing a declared value == B3 (zero-positive), deferred.
        assert item_coverage_errors("prod_name", self.DECL, {"a", "b", "c"}, {"a"}) == []

    def test_channel_name_item_is_supported(self):
        errs = item_coverage_errors("channel_name", ["sms", "app"], {"sms", "app", "x"}, {"sms"})
        assert len(errs) == 1
        assert "channel_name" in errs[0] and "x" in errs[0]

    def test_collects_multiple_errors(self):
        errs = item_coverage_errors("prod_name", self.DECL, {"a", "b", "zzz"}, {"a", "qqq"})
        # sp_unknown(zzz) + sp_missing(c) + lb_unknown(qqq) = 3
        assert len(errs) == 3
        joined = "\n".join(errs)
        assert "zzz" in joined and "qqq" in joined and "c" in joined


from recsys_tfb.core.consistency import ranking_objective_conflicts


class TestRankingObjectiveConflicts:
    def _params(self, objective=None, metric=None, entity=("cust_id",)):
        ap = {}
        if objective is not None:
            ap["objective"] = objective
        if metric is not None:
            ap["metric"] = metric
        return {
            "schema": {"columns": {
                "time": "snap_date",
                "entity": list(entity),
                "item": "prod_name",
                "label": "label",
            }},
            "training": {"algorithm_params": ap},
        }

    def test_non_ranking_objective_ok(self):
        assert ranking_objective_conflicts(
            self._params("binary", "binary_logloss")) == []

    def test_no_training_block_ok(self):
        assert ranking_objective_conflicts({"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}) == []

    def test_ranking_with_ndcg_ok(self):
        assert ranking_objective_conflicts(
            self._params("lambdarank", "ndcg")) == []

    def test_ranking_without_metric_ok(self):
        # unset metric is allowed — defaulted to ndcg at train time
        assert ranking_objective_conflicts(
            self._params("rank_xendcg", None)) == []

    def test_ranking_with_binary_metric_rejected(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "binary_logloss"))
        assert len(errs) == 1
        assert "ranking metric" in errs[0]
        assert "binary_logloss" in errs[0]

    def test_ranking_with_empty_entity_rejected(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "ndcg", entity=()))
        assert len(errs) == 1
        assert "query group" in errs[0]

    def test_collect_all_both_failures(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "binary_logloss", entity=()))
        assert len(errs) == 2


from recsys_tfb.core.consistency import (
    weight_unknown_items,
    weight_key_columns_unavailable,
    weight_key_arity_mismatch,
)


class TestWeightUnknownItems:
    def test_unknown_product_component_detected(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0, "hnw|zzz": 3.0}}})
        assert weight_unknown_items(p) == ["zzz"]

    def test_single_prod_name_key_unknown_detected(self):
        p = _base({"training": {
            "sample_weight_keys": ["prod_name"],
            "sample_weights": {"a": 2.0, "zzz": 3.0}}})
        assert weight_unknown_items(p) == ["zzz"]

    def test_all_known_returns_empty(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0, "hnw|b": 3.0}}})
        assert weight_unknown_items(p) == []

    def test_item_not_in_keys_returns_empty(self):
        # schema.item absent from weight keys -> no product component to check
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ"],
            "sample_weights": {"mass": 2.0}}})
        assert weight_unknown_items(p) == []

    def test_no_sample_weights_returns_empty(self):
        assert weight_unknown_items(_base()) == []


class TestWeightKeyColumnsUnavailable:
    def test_carried_column_is_available(self):
        p = _base({"dataset": {"carry_columns": ["cust_segment_typ"]},
                   "training": {
                       "sample_weight_keys": ["cust_segment_typ", "prod_name"]}})
        assert weight_key_columns_unavailable(p) == []

    def test_label_and_item_always_available(self):
        p = _base({"training": {"sample_weight_keys": ["prod_name", "label"]}})
        assert weight_key_columns_unavailable(p) == []

    def test_uncarried_column_flagged(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"]}})
        assert weight_key_columns_unavailable(p) == ["cust_segment_typ"]

    def test_no_keys_returns_empty(self):
        assert weight_key_columns_unavailable(_base()) == []

    def test_a9a_feature_categorical_is_available(self):
        p = _base()
        p["dataset"] = {"prepare_model_input": {"categorical_columns":
                        ["prod_name", "cust_segment_typ_2a"]}}
        p["training"] = {"sample_weight_keys": ["cust_segment_typ_2a", "prod_name"]}
        assert weight_key_columns_unavailable(p) == []

    def test_a9a_non_categorical_feature_still_blocked(self):
        p = _base()
        p["dataset"] = {"prepare_model_input": {"categorical_columns": ["prod_name"]}}
        p["training"] = {"sample_weight_keys": ["some_numeric_feature"]}
        assert weight_key_columns_unavailable(p) == ["some_numeric_feature"]


class TestWeightKeyArityMismatch:
    def test_matching_arity_ok(self):
        p = _base({"training": {
            "sample_weight_keys": ["cust_segment_typ", "prod_name"],
            "sample_weights": {"mass|a": 2.0}}})
        assert weight_key_arity_mismatch(p) == []

    def test_wrong_segment_count_flagged(self):
        p = _base({"training": {
            "sample_weight_keys": ["prod_name"],
            "sample_weights": {"mass|a": 2.0}}})
        assert weight_key_arity_mismatch(p) == ["mass|a"]

    def test_no_keys_returns_empty(self):
        p = _base({"training": {"sample_weights": {"a": 2.0}}})
        assert weight_key_arity_mismatch(p) == []


from recsys_tfb.core.consistency import search_space_errors


class TestSearchSpaceErrors:
    def _p(self, space):
        return {"training": {"search_space": space}}

    VALID = [
        {"name": "learning_rate", "type": "float", "low": 0.001, "high": 0.1, "log": True},
        {"name": "num_leaves", "type": "int", "low": 4, "high": 64},
        {"name": "max_depth", "type": "int", "low": 3, "high": 8, "step": 1},
        {"name": "kind", "type": "categorical", "choices": ["gbdt", "dart"]},
    ]

    def test_valid_space_ok(self):
        assert search_space_errors(self._p(self.VALID)) == []

    def test_absent_search_space_ok(self):
        assert search_space_errors({"training": {}}) == []

    def test_must_be_list_not_dict(self):
        errs = search_space_errors(self._p({"learning_rate": {"low": 1, "high": 2}}))
        assert len(errs) == 1 and "must be a list" in errs[0]

    def test_missing_name_or_type(self):
        errs = search_space_errors(self._p([{"type": "int", "low": 1, "high": 2}]))
        assert any("name" in e for e in errs)

    def test_unknown_type(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "loguniform", "low": 1, "high": 2}]))
        assert any("type" in e and "loguniform" in e for e in errs)

    def test_duplicate_names(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 2},
            {"name": "x", "type": "int", "low": 3, "high": 4},
        ]))
        assert any("duplicate" in e for e in errs)

    def test_low_ge_high(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "int", "low": 9, "high": 4}]))
        assert any("low" in e and "high" in e for e in errs)

    def test_log_requires_positive_low(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "float", "low": 0.0, "high": 1.0, "log": True}
        ]))
        assert any("log" in e and "positive" in e for e in errs)

    def test_log_and_step_mutually_exclusive(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "float", "low": 0.1, "high": 1.0, "log": True, "step": 0.1}
        ]))
        assert any("log" in e and "step" in e for e in errs)

    def test_step_must_be_positive(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 9, "step": 0}
        ]))
        assert any("step" in e and "positive" in e for e in errs)

    def test_categorical_needs_nonempty_choices(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "categorical", "choices": []}]))
        assert any("choices" in e for e in errs)

    def test_when_rejected_phase3(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 9, "when": "num_leaves > 8"}
        ]))
        assert any("Phase 3" in e for e in errs)

    def test_string_expression_bound_rejected_phase3(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": "num_leaves"}
        ]))
        assert any("Phase 3" in e for e in errs)

    def test_collects_all(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "bogus", "low": 1, "high": 2},
            {"name": "x", "type": "int", "low": 5, "high": 1},
        ]))
        assert len(errs) >= 3  # unknown type + duplicate name + low>=high

    def test_bool_low_or_high_rejected(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": True, "high": 5}
        ]))
        assert any("number" in e for e in errs)

    def test_bool_step_rejected(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "float", "low": 0.1, "high": 1.0, "step": True}
        ]))
        assert any("number" in e for e in errs)

    def test_non_numeric_non_string_bound_rejected(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": None, "high": {}}
        ]))
        assert any("number" in e for e in errs)


class TestSegmentSourceOverrideErrors:
    """A10 after ADR-0020 bug 6: a segment column without an override comes
    from the run mode's population table, so only an override that exists
    has a shape to check."""

    _COMPLETE = {"table": "ext.holding_combo",
                 "key_columns": ["cust_id", "snap_date"],
                 "segment_column": "holding_combo"}

    @staticmethod
    def _params(segment_columns, segment_sources=None):
        ev = {"segment_columns": segment_columns}
        if segment_sources is not None:
            ev["segment_sources"] = segment_sources
        return {"evaluation": ev}

    def test_a_column_without_override_is_not_an_error(self):
        from recsys_tfb.core.consistency import segment_source_override_errors
        assert segment_source_override_errors(
            self._params(["cust_segment_typ"])) == []

    def test_a_complete_override_passes(self):
        from recsys_tfb.core.consistency import segment_source_override_errors
        assert segment_source_override_errors(self._params(
            ["holding_combo"], {"holding_combo": dict(self._COMPLETE)})) == []

    @pytest.mark.parametrize("dropped", ["table", "key_columns", "segment_column"])
    def test_an_override_missing_a_field_is_named(self, dropped):
        from recsys_tfb.core.consistency import segment_source_override_errors
        override = {k: v for k, v in self._COMPLETE.items() if k != dropped}
        errs = segment_source_override_errors(self._params(
            ["holding_combo"], {"holding_combo": override}))
        assert len(errs) == 1
        assert "evaluation.segment_sources.holding_combo" in errs[0]
        assert repr(dropped) in errs[0]

    def test_an_override_delivering_another_column_is_an_error(self):
        """The override is looked up by the column name; one whose
        segment_column differs would join a column nothing groups by."""
        from recsys_tfb.core.consistency import segment_source_override_errors
        override = dict(self._COMPLETE, segment_column="holding_combo_code")
        errs = segment_source_override_errors(self._params(
            ["holding_combo"], {"holding_combo": override}))
        assert len(errs) == 1
        assert "'holding_combo_code'" in errs[0]

    def test_an_override_keyed_by_a_column_not_segmented_by_is_an_error(self):
        """Overrides are looked up by column name. The old A10 matched through
        segment_column and left the key free, so an old config keyed by an
        alias would otherwise pass, never be joined, and the column would
        silently come from the population table instead."""
        from recsys_tfb.core.consistency import segment_source_override_errors
        override = dict(self._COMPLETE, segment_column="cust_segment_typ")
        errs = segment_source_override_errors(self._params(
            ["cust_segment_typ"], {"cs": override}))
        assert len(errs) == 1
        assert "evaluation.segment_sources.cs" in errs[0]
        assert "is not in evaluation.segment_columns" in errs[0]

    def test_cli_entry_blocks_an_incomplete_override(self):
        p = _base()
        p["evaluation"] = {
            "segment_columns": ["holding_combo"],
            "segment_sources": {"holding_combo": {
                "table": "ext.holding_combo",
                "segment_column": "holding_combo"}},
        }
        with pytest.raises(ConfigConsistencyError,
                           match=r"segment_sources\.holding_combo.*'key_columns'"):
            validate_config_consistency(p)


from recsys_tfb.core.consistency import categorical_dtype_errors


class TestCategoricalDtypeErrors:
    """B5 — a declared feature categorical is a continuous-numeric type.

    decimal collects to Python decimal.Decimal (not JSON-serializable → the
    fit_preprocessor_metadata save crash); double/float serialize but are a
    mis-tag (continuous value used as a category; fragile float-equality map
    lookup). dtype strings are Spark ``DataFrame.dtypes`` simpleString form.
    """

    def test_string_and_int_categoricals_ok(self):
        dtypes = {"gender": "string", "risk_attr": "int", "prod_code": "bigint"}
        assert categorical_dtype_errors(
            ["gender", "risk_attr", "prod_code"], dtypes) == []

    def test_decimal_categorical_detected(self):
        dtypes = {"gender": "string", "industry_code": "decimal(15,0)"}
        errs = categorical_dtype_errors(["gender", "industry_code"], dtypes)
        assert len(errs) == 1
        assert "industry_code" in errs[0]
        assert "decimal(15,0)" in errs[0]

    def test_double_categorical_detected(self):
        errs = categorical_dtype_errors(["ratio"], {"ratio": "double"})
        assert len(errs) == 1
        assert "ratio" in errs[0] and "double" in errs[0]

    def test_float_categorical_detected(self):
        errs = categorical_dtype_errors(["amt"], {"amt": "float"})
        assert len(errs) == 1
        assert "amt" in errs[0] and "float" in errs[0]

    def test_identity_categorical_absent_from_feature_table_exempt(self):
        # prod_name (item / identity categorical) comes from
        # schema.categorical_values, not feature_table → not in dtypes → skipped.
        dtypes = {"gender": "string"}
        assert categorical_dtype_errors(["prod_name", "gender"], dtypes) == []

    def test_numeric_column_not_declared_categorical_is_ignored(self):
        # total_aum is decimal but NOT a declared categorical → out of scope.
        dtypes = {"gender": "string", "total_aum": "decimal(38,6)"}
        assert categorical_dtype_errors(["gender"], dtypes) == []

    def test_collects_and_sorts_multiple_offenders(self):
        dtypes = {
            "industry_code": "decimal(15,0)",
            "branch_ratio": "double",
            "gender": "string",
        }
        errs = categorical_dtype_errors(
            ["gender", "industry_code", "branch_ratio"], dtypes)
        assert len(errs) == 2
        # sorted by column name: branch_ratio before industry_code
        assert "branch_ratio" in errs[0]
        assert "industry_code" in errs[1]

    def test_message_hints_at_resolution(self):
        errs = categorical_dtype_errors(["industry_code"], {"industry_code": "decimal(15,0)"})
        msg = errs[0]
        assert "categorical_columns" in msg and "drop_columns" in msg


class TestCategoricalDtypeAllowList:
    """B5 since #407 — an allow-list: string, the integer family, boolean.

    Everything outside it is rejected, including a type nobody listed. Before
    #407 only decimal/double/float were: a date/timestamp/binary categorical
    passed, got a full-table vocabulary scan, then crashed the preprocessor's
    JSON save; a complex one crashed the encoder. Rejecting them breaks no
    configuration that produced a usable feature.
    """

    # Hive varchar(n)/char(n) read back as "string" in Spark 3.3.2 (checked on a
    # Hive table, 2026-09-19), so they are covered by "string".
    @pytest.mark.parametrize(
        "dt", ["string", "tinyint", "smallint", "int", "bigint", "boolean"])
    def test_supported_types_pass(self, dt):
        assert categorical_dtype_errors(["c"], {"c": dt}) == []

    @pytest.mark.parametrize("dt", [
        "date", "timestamp", "binary",
        "array<string>", "struct<a:int>", "map<string,int>",
        "void", "interval day to second",
    ])
    def test_every_other_type_is_rejected(self, dt):
        errs = categorical_dtype_errors(["c"], {"c": dt})
        assert len(errs) == 1
        assert "'c'" in errs[0] and f"type={dt}" in errs[0]

    @pytest.mark.parametrize("dt, remedy", [
        ("date", "derive a numeric feature"),
        ("timestamp", "derive a numeric feature"),
        ("binary", "hex("),
        ("array<string>", "flatten"),
        ("struct<a:int>", "flatten"),
    ])
    def test_each_family_is_told_its_own_way_out(self, dt, remedy):
        """The remedy is the point of rejecting early: a user who only hears
        "not allowed" edits the config twice."""
        (msg,) = categorical_dtype_errors(["c"], {"c": dt})
        assert remedy in msg
        assert "source ETL" in msg
        assert "drop_columns" in msg


class TestDiagnosisMetricParamsA15:
    def _params(self, metric=None, sample=None, ci=None, item_ability=None):
        ev = {}
        if metric is not None:
            ev["metric"] = metric
        diag = {}
        if sample is not None:
            diag["sample"] = sample
        if ci is not None:
            diag["ci"] = ci
        if item_ability is not None:
            diag["item_ability"] = item_ability
        if diag:
            ev["diagnosis"] = diag
        # validate_config_consistency resolves the schema, and since #328 the
        # three user-owned roles have no built-in answer. Carried by every
        # _params here so the predicate-level and aggregator-level call styles
        # stay interchangeable.
        return {"evaluation": ev, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}}}

    def test_absent_blocks_are_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        assert diagnosis_metric_param_errors({}) == []
        assert diagnosis_metric_param_errors(self._params()) == []

    def test_valid_defaults_are_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = self._params(
            metric={"weight_alpha": 0.0, "k": None, "min_positives": 0,
                    "shrinkage_k": 0},
            sample={"max_queries": 200000, "min_pos_queries_per_item": 50,
                    "seed": 42},
            ci={"enabled": True, "n_boot": 200},
        )
        assert diagnosis_metric_param_errors(p) == []

    def test_each_bad_value_reports(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = self._params(
            metric={"weight_alpha": 1.5, "k": 0, "min_positives": -1,
                    "shrinkage_k": -0.1},
            sample={"max_queries": 0, "min_pos_queries_per_item": 0},
            ci={"n_boot": 0},
            item_ability={"top_n": -1},
        )
        errors = diagnosis_metric_param_errors(p)
        joined = "\n".join(errors)
        # Token check first: if a predicate is dropped, the failure message
        # then names *which* key stopped being validated, instead of only
        # reporting a count that went from 8 to 7.
        for token in ["weight_alpha", "metric.k", "min_positives",
                      "shrinkage_k", "max_queries",
                      "min_pos_queries_per_item", "n_boot",
                      "item_ability.top_n"]:
            assert token in joined, f"{token} is no longer validated"
        assert len(errors) == 8

    def test_top_n_negative_or_non_int_rejected(self):
        """A15 自己就要擋 item_ability.top_n < 0 / bool / 非 int。

        top_n = 0 是良性退化（不顯示任何 item），所以 floor 是 0，與 sibling
        suppression.top_examples（A19）一致。真正的危害是負數（Python 會從尾端
        切片）與 bool（True == 1 會溜過一個天真的 >= 0 檢查）。
        """
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        for bad in (-1, True, "30", 1.5):
            errors = diagnosis_metric_param_errors(
                self._params(item_ability={"top_n": bad})
            )
            assert len(errors) == 1, errors
            assert "item_ability.top_n" in errors[0]

    def test_top_n_zero_and_default_are_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        assert diagnosis_metric_param_errors(
            self._params(item_ability={"top_n": 0})
        ) == []
        assert diagnosis_metric_param_errors(
            self._params(item_ability={"top_n": 30})
        ) == []

    def test_max_queries_below_one_rejected(self):
        """A15 must reject ``max_queries <= 0`` on its own.

        Not redundant with test_each_bad_value_reports: this is the one key
        whose absence degrades *silently* rather than crashing.
        ``draw_diagnosis_sample`` with ``max_queries=0`` and a take-all item
        does not raise — it returns a 1-query sample with
        ``sample_ratio=0.0``, i.e. a plausible-looking artefact computed from
        almost no data. (With no take-all item it instead dies at
        ``sample.py`` with an opaque ``AttributeError: 'NoneType' object has
        no attribute 'withColumn'``.) Both are config errors that must be
        caught at CLI entry, before Spark starts. Mirrors the >= 1 guard on
        the training-side ``diagnostics.shap.quadrant_*`` int keys (A20).
        """
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        for bad in (0, -1):
            errors = diagnosis_metric_param_errors(
                self._params(sample={"max_queries": bad})
            )
            assert len(errors) == 1, errors
            assert "max_queries" in errors[0] and "int >= 1" in errors[0]
        # a bool is not an acceptable int here (True == 1 would slip through
        # a naive `>= 1` check)
        errors = diagnosis_metric_param_errors(
            self._params(sample={"max_queries": True})
        )
        assert len(errors) == 1 and "max_queries" in errors[0]

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            validate_config_consistency,
        )
        p = self._params(metric={"weight_alpha": 2.0})
        with _pytest.raises(ConfigConsistencyError, match="weight_alpha"):
            validate_config_consistency(p)


class TestReservedSegmentColumnsA15:
    """A15：segment_columns 不得用抽樣器的保留欄名。

    為什麼要在 config 層再擋一次（``sample.py::_guard_reserved_columns``
    已經有 runtime 守衛）：兩者驗的輸入不同。這條驗「config 宣告了什麼」，
    在 CLI entry 一秒內擋掉；runtime 那條驗「實際 DataFrame 有什麼欄」，
    是給繞過 Layer-1 的呼叫路徑（``scripts/*_diagnosis.py`` 直接 import）
    的 backstop。少了這條，使用者要等 Spark 起來 2–4 分鐘才知道配置錯了。
    """

    def _params(self, seg_cols):
        return {"evaluation": {"segment_columns": seg_cols}, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},}

    def test_reserved_names_rejected(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        for bad in ("stratum", "inclusion_weight"):
            errors = diagnosis_metric_param_errors(self._params([bad]))
            assert len(errors) == 1, errors
            assert bad in errors[0] and "reserved" in errors[0]

    def test_both_reserved_names_reported_together(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        errors = diagnosis_metric_param_errors(
            self._params(["cust_segment_typ", "stratum", "inclusion_weight"])
        )
        assert len(errors) == 2
        joined = "\n".join(errors)
        assert "stratum" in joined and "inclusion_weight" in joined

    def test_ordinary_segment_columns_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        assert diagnosis_metric_param_errors(
            self._params(["cust_segment_typ", "age_band"])
        ) == []
        # 缺席 / None 都不算錯
        assert diagnosis_metric_param_errors({"evaluation": {}}) == []
        assert diagnosis_metric_param_errors(self._params(None)) == []

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            validate_config_consistency,
        )
        # match 用 "reserved" 而非 "stratum"：A10（segment_columns 沒有對應
        # segment_source）也會對同一份 config 報錯、訊息裡同樣有 "stratum"，
        # 拿 "stratum" 當 match 的話這條測試在 A15 被拔掉後照樣會綠。
        with _pytest.raises(ConfigConsistencyError, match="reserved column"):
            validate_config_consistency(self._params(["stratum"]))


class TestEnabledMustBeBool:
    """A15：enabled 必須是 bool——YAML 引號字串 "false" 恆真，會靜默啟用節點。"""

    def test_ci_enabled_string_rejected(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = {"evaluation": {"diagnosis": {"ci": {"enabled": "false"}}}}
        errors = diagnosis_metric_param_errors(p)
        assert len(errors) == 1 and "ci.enabled" in errors[0]

    def test_bool_values_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = {"evaluation": {"diagnosis": {"ci": {"enabled": False}}}}
        assert diagnosis_metric_param_errors(p) == []

    def test_config_shift_enabled_string_rejected(self):
        """同一個 YAML 陷阱套用在 config_shift 上。

        歸在 A15（診斷抽樣與 CI 的參數家族）而不是開新代號：config_shift 的
        enabled 決定的正是「共用診斷抽樣要不要抽」，跟 ci.enabled 是同一條
        不變量的另一個成員。
        """
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        for bad in ("false", "true", 0, 1, None):
            p = {"evaluation": {"diagnosis": {
                "config_shift": {"enabled": bad}
            }}}
            errors = diagnosis_metric_param_errors(p)
            assert len(errors) == 1, (bad, errors)
            assert "config_shift.enabled" in errors[0], (bad, errors)

    def test_config_shift_bool_values_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        for good in (True, False):
            p = {"evaluation": {"diagnosis": {
                "config_shift": {"enabled": good}
            }}}
            assert diagnosis_metric_param_errors(p) == [], good
        # 缺席 / 空 block 都採預設值，不算錯
        assert diagnosis_metric_param_errors(
            {"evaluation": {"diagnosis": {"config_shift": {}}}}
        ) == []

    def test_config_shift_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            validate_config_consistency,
        )
        p = {
            "evaluation": {"diagnosis": {"config_shift": {"enabled": "false"}}},
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name"}},
        }
        # match 挑 "config_shift.enabled"：這個 repo 踩過「match pattern 被別條
        # predicate 的訊息滿足」的假綠。沒有第二條 predicate 會對這份 config
        # 吐出這個字串，所以拔掉本檢查這條測試一定紅。
        with _pytest.raises(
            ConfigConsistencyError, match=r"config_shift\.enabled"
        ):
            validate_config_consistency(p)


class TestSuppressionParamsA19:
    """A19 改軌（Plan 3 Task 5.3）：驗 ``suppression.top_examples``。
    ``enabled`` 不在這裡驗——``suppression``
    進了 ``diagnosis.metric.contract.DIAGNOSES`` 之後，A15
    （``diagnosis_metric_param_errors``）已經對 registry 裡每個名字驗過
    ``enabled``，兩邊都驗會對同一個壞值吐兩條訊息。

    ⚠ ``match="suppression"`` 選字陷阱：A15 對同一份 config 也會 raise，
    訊息裡含 ``evaluation.diagnosis.suppression.enabled=...``——也含
    "suppression" 這個子字串。所以
    ``test_registered_in_validate_config_consistency`` 改用只有 A19 訊息
    才有的 "top_examples" 當 match pattern，並且參數只給
    ``top_examples``（不去動 ``enabled``），避免真的觸發 A15 而讓這條測試
    在「哪個 predicate 負責」這件事上失去辨識力。
    """

    def _params(self, suppression=None):
        diag = {}
        if suppression is not None:
            diag["suppression"] = suppression
        return {"evaluation": {"diagnosis": diag}, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},}

    def test_absent_and_valid_defaults_clean(self):
        from recsys_tfb.core.consistency import suppression_param_errors
        assert suppression_param_errors({}) == []
        assert suppression_param_errors(self._params()) == []
        assert suppression_param_errors(
            self._params({"top_examples": 50})
        ) == []
        assert suppression_param_errors(
            self._params({"top_examples": 0})
        ) == [], "0 是合法值（不列具體案例），不是 falsy 就該擋"

    def test_non_int_top_examples_rejected(self):
        from recsys_tfb.core.consistency import suppression_param_errors
        errs = suppression_param_errors(self._params({"top_examples": "50"}))
        assert len(errs) == 1
        assert "evaluation.diagnosis.suppression.top_examples" in errs[0]

    def test_bool_top_examples_rejected(self):
        """``isinstance(True, int)`` 是 ``True``——bool 不算數，必須先擋。"""
        from recsys_tfb.core.consistency import suppression_param_errors
        errs = suppression_param_errors(self._params({"top_examples": True}))
        assert len(errs) == 1
        assert "top_examples" in errs[0]

    def test_negative_top_examples_rejected(self):
        from recsys_tfb.core.consistency import suppression_param_errors
        errs = suppression_param_errors(self._params({"top_examples": -1}))
        assert len(errs) == 1
        assert "top_examples" in errs[0]

    def test_missing_block_defaults_clean(self):
        from recsys_tfb.core.consistency import suppression_param_errors
        params = self._params({"top_examples": 50})
        params["evaluation"]["diagnosis"].pop("suppression", None)
        assert suppression_param_errors(params) == []

    def test_enabled_is_not_validated_here(self):
        """``enabled`` 的型別檢查交給 A15，A19 不重複驗——即使給一個非
        bool 的 enabled，這個 predicate 也不該對它有意見。"""
        from recsys_tfb.core.consistency import suppression_param_errors
        errs = suppression_param_errors(
            self._params({"enabled": "yes", "top_examples": 50})
        )
        assert errs == []

    def test_registered_in_validate_config_consistency(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="top_examples"):
            validate_config_consistency(
                self._params({"top_examples": "50"})
            )


class TestTrainingDiagnosticsParamsA20:
    def _params(self, background=None, gain_ledger_enabled=None,
                quadrant_enabled=None, quadrant_top_k_decision=None,
                quadrant_sample_per_cell=None, quadrant_min_rows=None):
        shap_cfg = {}
        if background is not None:
            shap_cfg["background"] = background
        if quadrant_enabled is not None:
            shap_cfg["quadrant_enabled"] = quadrant_enabled
        if quadrant_top_k_decision is not None:
            shap_cfg["quadrant_top_k_decision"] = quadrant_top_k_decision
        if quadrant_sample_per_cell is not None:
            shap_cfg["quadrant_sample_per_cell"] = quadrant_sample_per_cell
        if quadrant_min_rows is not None:
            shap_cfg["quadrant_min_rows"] = quadrant_min_rows
        diag = {}
        if shap_cfg:
            diag["shap"] = shap_cfg
        if gain_ledger_enabled is not None:
            diag["gain_ledger"] = {"enabled": gain_ledger_enabled}
        return {"diagnostics": diag, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},}

    def test_bad_background_domain_rejected(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        errs = training_diagnostics_param_errors(self._params(background="per_query"))
        assert len(errs) == 1
        assert "diagnostics.shap.background" in errs[0]

    def test_valid_background_values_clean(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        assert training_diagnostics_param_errors(self._params(background="global")) == []
        assert training_diagnostics_param_errors(self._params(background="per_item")) == []
        # absent block / absent key -> default "global" -> clean
        assert training_diagnostics_param_errors({}) == []

    def test_non_bool_gain_ledger_enabled_rejected(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        errs = training_diagnostics_param_errors(
            self._params(gain_ledger_enabled="yes")
        )
        assert len(errs) == 1
        assert "gain_ledger.enabled" in errs[0]

    def test_non_bool_quadrant_enabled_rejected(self):
        # The failure mode this predicate exists to catch (see docstring):
        # shap_cases.py / population_spark.py read
        # cfg.get("quadrant_enabled", True) with bare truthiness, so a
        # quoted YAML string like "false" is truthy in Python and would
        # silently enable the node instead of disabling it.
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        errs = training_diagnostics_param_errors(
            self._params(quadrant_enabled="false")
        )
        assert len(errs) == 1
        assert "quadrant_enabled" in errs[0]

    def test_valid_quadrant_enabled_values_clean(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        assert training_diagnostics_param_errors(
            self._params(quadrant_enabled=True)) == []
        assert training_diagnostics_param_errors(
            self._params(quadrant_enabled=False)) == []
        # absent key -> default True -> clean
        assert training_diagnostics_param_errors({}) == []

    def test_non_positive_int_quadrant_keys_rejected(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        for key, bad in (
            ("quadrant_top_k_decision", 0),
            ("quadrant_sample_per_cell", -1),
            ("quadrant_min_rows", 1.5),
        ):
            errs = training_diagnostics_param_errors(self._params(**{key: bad}))
            assert len(errs) == 1, (key, bad, errs)
            assert key in errs[0]

    def test_valid_quadrant_int_keys_clean(self):
        from recsys_tfb.core.consistency import training_diagnostics_param_errors
        assert training_diagnostics_param_errors(self._params(
            quadrant_top_k_decision=1,
            quadrant_sample_per_cell=30,
            quadrant_min_rows=10,
        )) == []

    def test_registered_in_validate_config_consistency(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="background"):
            validate_config_consistency(self._params(background="per_query"))


from pyspark.sql import types as T

from recsys_tfb.core.consistency import (
    _NUMERIC_SPARK_TYPES,
    nonnumeric_feature_errors,
    spark_dtype_is_numeric,
)
from recsys_tfb.preprocessing import CASTABLE_NUMERIC_TYPES


class TestSparkDtypeIsNumeric:
    @pytest.mark.parametrize(
        "dt,expected",
        [
            ("int", True), ("bigint", True), ("smallint", True),
            ("double", True), ("float", True), ("boolean", True),
            ("decimal(15,0)", True), ("decimal(38,10)", True),
            ("string", False), ("STRING", False), (" string ", False),
            ("binary", False), ("date", False), ("timestamp", False),
            ("array<string>", False), ("map<string,int>", False),
            ("struct<a:int>", False),
            # fail-safe: unknown / exotic types must be treated as non-numeric,
            # never silently pass the gate (whitelist, not blacklist).
            ("char(10)", False), ("varchar(20)", False),
            ("void", False), ("null", False),
            ("interval day to second", False),
        ],
    )
    def test_classification(self, dt, expected):
        assert spark_dtype_is_numeric(dt) is expected


class TestB6AdmissionIsBackedByTheCast:
    """Every type B6 admits must be one the cast actually converts (#283).

    ``spark_dtype_is_numeric`` used to say these types "survive
    ``DataFrame.values`` into a numeric numpy matrix". That was false for two
    of them — ``boolean`` mixed with float gives ``object`` in pandas, and
    ``decimal`` gives boxed ``Decimal`` objects — i.e. precisely the OOM B6
    exists to prevent, arriving through a dtype B6 waves through. What makes
    the admission correct is not the type: it is that
    ``preprocessing.cast_numeric_features_to_storage_type`` converts all of
    them before any pandas frame exists.

    That makes the classifier's correctness a *dependency* on the cast's
    coverage, and this class is where the dependency is pinned. Shrink the
    cast's whitelist and these go red, rather than the docstring going quietly
    untrue again.

    No SparkSession: ``pyspark.sql.types`` instances need none.
    """

    #: One instance per admitted dtype, so the string form and the Spark type
    #: can be checked against each other. Hand-written rather than derived —
    #: deriving both sides from one source is how a pair like this stops being
    #: able to disagree.
    _INSTANCES = {
        "tinyint": T.ByteType(),
        "smallint": T.ShortType(),
        "int": T.IntegerType(),
        "bigint": T.LongType(),
        "float": T.FloatType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "decimal(18,2)": T.DecimalType(18, 2),
    }

    def test_the_instance_table_covers_every_admitted_simple_string(self):
        # Guard the guard: without this, adding a type to the whitelist and
        # not to the table would leave it untested rather than red.
        assert set(_NUMERIC_SPARK_TYPES) | {"decimal(18,2)"} == set(self._INSTANCES)

    @pytest.mark.parametrize("simple_string", sorted(_INSTANCES))
    def test_every_admitted_type_is_one_the_cast_converts(self, simple_string):
        dtype = self._INSTANCES[simple_string]
        assert spark_dtype_is_numeric(simple_string) is True
        assert isinstance(dtype, CASTABLE_NUMERIC_TYPES), (
            f"B6 admits {simple_string!r} as reaching the model as a number, "
            f"but the cast does not convert it — so it would reach pandas as "
            f"itself. Either the cast must cover it or B6 must stop admitting "
            f"it."
        )

    @pytest.mark.parametrize("simple_string", sorted(_INSTANCES))
    def test_the_two_sides_agree_on_the_spelling(self, simple_string):
        # The classifier reads simpleStrings; the cast reads Spark types. This
        # is the only place the two vocabularies meet, so a Spark release that
        # respelled one of them has to argue here.
        assert self._INSTANCES[simple_string].simpleString() == simple_string


class TestNonnumericFeatureErrors:
    def test_string_feature_not_encoded_is_flagged(self):
        errs = nonnumeric_feature_errors(
            {"age": "numeric", "cust_segment": "nonnumeric"}, set()
        )
        assert len(errs) == 1
        assert "cust_segment" in errs[0]
        # 訊息點名的函式名要在 repo 裡 grep 得到，使用者才追得下去
        assert "pdf_to_X.to_numpy" in errs[0]
        assert "categorical_columns" in errs[0]
        assert "drop_columns" in errs[0]

    def test_nonnumeric_but_will_be_encoded_is_ok(self):
        # prod_name: 在 parquet 是 string，但屬 deferred identity categorical
        errs = nonnumeric_feature_errors(
            {"prod_name": "nonnumeric", "age": "numeric"}, {"prod_name"}
        )
        assert errs == []

    def test_all_numeric_is_ok(self):
        assert nonnumeric_feature_errors({"a": "numeric", "b": "numeric"}, set()) == []

    def test_empty_is_ok(self):
        assert nonnumeric_feature_errors({}, set()) == []

    def test_multiple_offenders_sorted_by_column(self):
        errs = nonnumeric_feature_errors(
            {"zzz": "nonnumeric", "aaa": "nonnumeric"}, set()
        )
        assert len(errs) == 2
        assert "aaa" in errs[0] and "zzz" in errs[1]


class TestNonnumericFeatureErrorsAdviceByType:
    """#407 — B6 must not send a column into B5's rejection.

    B6's usual way out is "declare it categorical". For a type B5 rejects
    (date, binary, complex) that advice sends the user from one error straight
    into the other, so with the column's type in hand B6 gives B5's way out.
    """

    _SUGGEST_CATEGORICAL = "add it to dataset.prepare_model_input.categorical_columns"

    def test_a_date_column_is_not_told_to_become_categorical(self):
        (msg,) = nonnumeric_feature_errors(
            {"open_date": "nonnumeric"}, set(), dtypes={"open_date": "date"})
        assert self._SUGGEST_CATEGORICAL not in msg
        assert "cannot be declared categorical" in msg
        assert "derive a numeric feature" in msg
        assert "drop_columns" in msg

    def test_a_string_column_still_is(self):
        (msg,) = nonnumeric_feature_errors(
            {"segment": "nonnumeric"}, set(), dtypes={"segment": "string"})
        assert self._SUGGEST_CATEGORICAL in msg

    def test_without_types_the_advice_is_unchanged(self):
        """The training-read backstop (io/extract.py) reads parquet types and
        passes none; its message stays what it was."""
        (msg,) = nonnumeric_feature_errors({"open_date": "nonnumeric"}, set())
        assert self._SUGGEST_CATEGORICAL in msg


# --- B9: model_input feature columns are all the declared storage type ---

from recsys_tfb.core.consistency import feature_storage_type_errors


class TestFeatureStorageTypeErrors:
    def test_homogeneous_declared_type_is_ok(self):
        assert feature_storage_type_errors(
            {"a": "float32", "b": "float32"}, "float32") == []

    def test_empty_is_ok(self):
        assert feature_storage_type_errors({}, "float32") == []

    def test_one_wider_column_is_flagged(self):
        errs = feature_storage_type_errors(
            {"a": "float32", "b": "int64"}, "float32")
        assert len(errs) == 1
        assert "'b'" in errs[0] and "int64" in errs[0] and "float32" in errs[0]

    def test_homogeneous_but_undeclared_type_is_flagged(self):
        # Nothing collides and nothing is lost — no value-level gate can see
        # this one. It just costs twice the memory the declaration promised.
        errs = feature_storage_type_errors(
            {"a": "float64", "b": "float64"}, "float32")
        assert len(errs) == 2

    def test_declared_float64_admits_float64(self):
        assert feature_storage_type_errors(
            {"a": "float64"}, "float64") == []

    def test_multiple_offenders_sorted_by_column(self):
        errs = feature_storage_type_errors(
            {"zzz": "int64", "aaa": "int32", "ok": "float32"}, "float32")
        assert len(errs) == 2
        assert "aaa" in errs[0] and "zzz" in errs[1]

    def test_remedy_is_a_rebuild_not_a_config_edit(self):
        # The config is not wrong when B9 fires — the parquet is stale. Telling
        # the reader to widen the declaration would make the message wrong.
        errs = feature_storage_type_errors({"a": "float64"}, "float32")
        assert "Rebuild the dataset" in errs[0]


# --- B7: a carry column that also lives in feature_table must be dropped ---

from recsys_tfb.core.consistency import carry_column_collision_errors


class TestCarryColumnCollisionErrors:
    FT = {"snap_date", "cust_id", "cust_segment_typ", "total_aum", "label"}
    IDENTITY = ["snap_date", "cust_id", "prod_name"]

    def _errors(self, carry, drop):
        return carry_column_collision_errors(
            carry, self.FT, drop, self.IDENTITY, "label"
        )

    def test_carry_column_in_feature_table_and_not_dropped_is_flagged(self):
        errs = self._errors(["cust_segment_typ"], ["snap_date", "cust_id", "label"])
        assert len(errs) == 1
        assert "cust_segment_typ" in errs[0]
        # The message has to name both keys: which one the column is in today,
        # and which one it is missing from.
        assert "carry_columns" in errs[0]
        assert "drop_columns" in errs[0]

    def test_carry_column_in_feature_table_and_dropped_is_ok(self):
        # The intentional pairing ADR-0004 documents: listed in both keys.
        assert self._errors(["cust_segment_typ"], ["cust_segment_typ"]) == []

    def test_carry_column_absent_from_feature_table_is_ok(self):
        # Nothing to be ambiguous with, so drop_columns is irrelevant here —
        # this is the common case (carry columns usually live only in
        # sample_pool) and must not be flagged.
        assert self._errors(["marketing_flag"], []) == []

    def test_no_carry_columns_is_ok(self):
        assert self._errors([], []) == []

    def test_empty_feature_table_is_ok(self):
        assert carry_column_collision_errors(
            ["cust_segment_typ"], set(), [], self.IDENTITY, "label"
        ) == []

    def test_multiple_offenders_sorted_by_column(self):
        errs = self._errors(["total_aum", "cust_segment_typ"], [])
        assert len(errs) == 2
        assert "cust_segment_typ" in errs[0]
        assert "total_aum" in errs[1]

    def test_only_the_undropped_offender_is_flagged(self):
        errs = self._errors(["total_aum", "cust_segment_typ"], ["cust_segment_typ"])
        assert len(errs) == 1
        assert "total_aum" in errs[0]
        assert "cust_segment_typ" not in errs[0]

    def test_feature_table_columns_accepts_a_list(self):
        # The gate passes whatever it already has in hand (a dict's keys / a
        # list of column names); membership must not depend on the container.
        errs = carry_column_collision_errors(
            ["cust_segment_typ"], sorted(self.FT), [], self.IDENTITY, "label"
        )
        assert len(errs) == 1

    # --- exemptions: columns that cannot collide however they are configured ---
    #
    # Both are undropped feature_table columns named in carry_columns, i.e. they
    # satisfy the naive rule and would be flagged without the exemption. Running
    # the real build_model_input with an identity column configured this way
    # completes normally, so a flag here would demand a config edit that changes
    # nothing while busting base_dataset_version.

    def test_identity_column_in_carry_is_exempt(self):
        # A split's keys append only carry entries not already in the
        # identity key, so no second copy exists to be ambiguous with.
        assert self._errors(["cust_id"], []) == []

    def test_label_column_in_carry_is_exempt(self):
        # compute_feature_columns excludes the label from feature_columns
        # whatever drop_columns says, so it never reaches the feature side.
        assert self._errors(["label"], []) == []

    def test_exemption_does_not_mask_a_real_collision_in_the_same_config(self):
        # The exemption is per-column, not a whole-config escape hatch.
        errs = self._errors(["cust_id", "label", "cust_segment_typ"], [])
        assert len(errs) == 1
        assert "cust_segment_typ" in errs[0]


# --- A21: --rebuild-dates must be a subset of dataset.test_snap_dates ---

import datetime as _dt

from recsys_tfb.core.consistency import ConfigConsistencyError, resolved_rebuild_dates


def _ds(*test_dates) -> dict:
    return {"dataset": {"test_snap_dates": list(test_dates)}}


class TestResolvedRebuildDatesA21:
    def test_none_returns_empty(self):
        assert resolved_rebuild_dates(_ds("2026-01-31"), None) == []

    def test_empty_returns_empty(self):
        assert resolved_rebuild_dates(_ds("2026-01-31"), []) == []

    def test_proper_subset_is_normalised_and_sorted(self):
        params = _ds("2026-01-31", "2026-02-28", "2026-03-31")
        assert resolved_rebuild_dates(params, ["2026-03-31", "2026-01-31"]) == [
            "2026-01-31",
            "2026-03-31",
        ]

    def test_full_overlap_with_configured_is_allowed(self):
        params = _ds("2026-01-31", "2026-02-28")
        assert resolved_rebuild_dates(params, ["2026-01-31", "2026-02-28"]) == [
            "2026-01-31",
            "2026-02-28",
        ]

    def test_duplicates_collapse(self):
        params = _ds("2026-01-31")
        assert resolved_rebuild_dates(params, ["2026-01-31", "2026-01-31"]) == [
            "2026-01-31"
        ]

    def test_unconfigured_month_raises(self):
        # The match string is the flag name itself: no other predicate in this
        # module mentions --rebuild-dates, so this cannot be satisfied by an
        # unrelated rule firing on the same config.
        params = _ds("2026-01-31", "2026-02-28")
        with pytest.raises(ConfigConsistencyError, match=r"--rebuild-dates"):
            resolved_rebuild_dates(params, ["2026-09-30"])

    def test_error_names_the_offending_month_and_the_configured_set(self):
        params = _ds("2026-01-31", "2026-02-28")
        with pytest.raises(ConfigConsistencyError) as exc:
            resolved_rebuild_dates(params, ["2026-09-30"])
        msg = str(exc.value)
        assert "2026-09-30" in msg
        assert "test_snap_dates" in msg
        assert "2026-01-31" in msg  # what IS available

    def test_partially_valid_list_still_raises(self):
        # One good month must not mask a bad one.
        params = _ds("2026-01-31")
        with pytest.raises(ConfigConsistencyError, match=r"--rebuild-dates"):
            resolved_rebuild_dates(params, ["2026-01-31", "2026-09-30"])

    def test_no_test_snap_dates_configured_raises(self):
        with pytest.raises(ConfigConsistencyError, match=r"--rebuild-dates"):
            resolved_rebuild_dates({"dataset": {}}, ["2026-01-31"])

    def test_malformed_date_raises(self):
        # match on "non-ISO value", NOT on "--rebuild-dates": both raise
        # branches of this predicate mention the flag name, so the flag name
        # alone cannot tell them apart — deleting the malformed guard entirely
        # would still leave this passing (the value falls through to the
        # unknown-month branch as None).
        with pytest.raises(ConfigConsistencyError, match=r"non-ISO value"):
            resolved_rebuild_dates(_ds("2026-01-31"), ["31/01/2026"])

    def test_valid_and_malformed_mixed_still_reports_malformed(self):
        # A good month must not mask a malformed one; without the guard this
        # input crashes in sorted() on a None instead of reporting anything.
        with pytest.raises(ConfigConsistencyError, match=r"non-ISO value"):
            resolved_rebuild_dates(_ds("2026-01-31"), ["2026-01-31", "31/01/2026"])

    def test_datetime_shaped_config_value_is_accepted(self):
        # pd.Timestamp accepts these everywhere else in the pipeline, so A21
        # must not be stricter than the code it guards.
        params = _ds("2026-01-31 00:00:00")
        assert resolved_rebuild_dates(params, ["2026-01-31"]) == ["2026-01-31"]

    def test_unreadable_config_value_blames_the_config_not_the_flag(self):
        with pytest.raises(ConfigConsistencyError, match=r"unreadable date"):
            resolved_rebuild_dates(_ds("not-a-date"), ["2026-01-31"])

    def test_unquoted_yaml_dates_compare_equal(self):
        # PyYAML parses an unquoted 2026-01-31 into datetime.date; a string from
        # the CLI must still match it.
        params = _ds(_dt.date(2026, 1, 31))
        assert resolved_rebuild_dates(params, ["2026-01-31"]) == ["2026-01-31"]


from recsys_tfb.core.consistency import resolved_inference_rebuild_dates


def _inf(*snap_dates) -> dict:
    return {"inference": {"snap_dates": list(snap_dates)}}


class TestResolvedInferenceRebuildDatesA21:
    """The same invariant, scoped to the months inference can score at all.

    The inference command's resume unit is a ``(snap_date, entity_bucket,
    item)`` partition, and it only ever touches ``inference.snap_dates``. A
    month outside that list is the same silent no-op A21 exists to reject.
    """

    def test_none_returns_empty(self):
        assert resolved_inference_rebuild_dates(_inf("2025-12-31"), None) == []

    def test_configured_month_is_normalised(self):
        params = _inf("2025-12-31", "2025-11-30")
        assert resolved_inference_rebuild_dates(
            params, ["2025-12-31", "2025-11-30"]
        ) == ["2025-11-30", "2025-12-31"]

    def test_unconfigured_month_raises(self):
        with pytest.raises(ConfigConsistencyError, match=r"--rebuild-dates"):
            resolved_inference_rebuild_dates(_inf("2025-12-31"), ["2025-09-30"])

    def test_error_names_the_inference_key_not_the_dataset_one(self):
        """A message naming the wrong key sends the operator to the wrong yaml."""
        with pytest.raises(ConfigConsistencyError) as exc:
            resolved_inference_rebuild_dates(_inf("2025-12-31"), ["2025-09-30"])
        msg = str(exc.value)
        assert "inference.snap_dates" in msg
        assert "test_snap_dates" not in msg

    def test_a_dataset_test_month_is_not_automatically_scorable(self):
        """The two flags read two different keys; sharing one would cross them."""
        params = {
            "inference": {"snap_dates": ["2025-12-31"]},
            "dataset": {"test_snap_dates": ["2025-09-30"]},
        }
        with pytest.raises(ConfigConsistencyError, match=r"--rebuild-dates"):
            resolved_inference_rebuild_dates(params, ["2025-09-30"])

    def test_malformed_date_raises(self):
        with pytest.raises(ConfigConsistencyError, match=r"non-ISO value"):
            resolved_inference_rebuild_dates(_inf("2025-12-31"), ["31/12/2025"])

    def test_unquoted_yaml_dates_compare_equal(self):
        params = _inf(_dt.date(2025, 12, 31))
        assert resolved_inference_rebuild_dates(params, ["2025-12-31"]) == [
            "2025-12-31"
        ]


# --- A22: under --post-training, evaluation.snap_date must be a test month ---

from recsys_tfb.core.consistency import post_training_snap_date_errors


def _eval_params(snap_date, *test_dates) -> dict:
    """Merged-parameters shape: evaluation.snap_date + dataset.test_snap_dates."""
    params = _ds(*test_dates)
    params["evaluation"] = {"snap_date": snap_date}
    return params


class TestPostTrainingSnapDateA22:
    # Match strings below are chosen per raise-branch, not per invariant code:
    # every branch of this predicate carries the "(A22)" prefix and the words
    # "test_snap_dates", so matching on either would let one branch's message
    # satisfy another branch's test.

    def test_monitoring_mode_ignores_a_non_test_month(self):
        # THE point of wiring this by flag instead of as a plain predicate:
        # default mode reads inference output, whose month need not be a test
        # month. Same config as test_post_training_rejects_non_test_month.
        params = _eval_params("2026-09-30", "2026-01-31", "2026-02-28")
        assert post_training_snap_date_errors(params, post_training=False) == []

    def test_monitoring_mode_ignores_an_unset_snap_date(self):
        assert post_training_snap_date_errors(
            {"dataset": {"test_snap_dates": ["2026-01-31"]}}, post_training=False
        ) == []

    def test_post_training_accepts_a_configured_month(self):
        params = _eval_params("2026-02-28", "2026-01-31", "2026-02-28")
        assert post_training_snap_date_errors(params, post_training=True) == []

    def test_post_training_rejects_non_test_month(self):
        params = _eval_params("2026-09-30", "2026-01-31", "2026-02-28")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert "is not a test month" in errs[0]

    def test_error_names_the_configured_value_and_the_available_months(self):
        params = _eval_params("2026-09-30", "2026-01-31", "2026-02-28")
        msg = post_training_snap_date_errors(params, post_training=True)[0]
        assert "2026-09-30" in msg          # what was asked for
        assert "2026-01-31" in msg          # what IS available
        assert "2026-02-28" in msg
        assert "dataset.test_snap_dates" in msg  # where to fix it

    def test_empty_test_snap_dates_rejects_with_its_own_wording(self):
        # Match on the empty-config wording, NOT "is not a test month": "you
        # configured no test months" and "you picked the wrong one of several"
        # need different fixes, so one message must not stand in for the other.
        errs = post_training_snap_date_errors(
            _eval_params("2026-09-30"), post_training=True
        )
        assert len(errs) == 1
        assert "dataset.test_snap_dates is empty" in errs[0]

    def test_missing_dataset_section_rejects_as_empty(self):
        errs = post_training_snap_date_errors(
            {"evaluation": {"snap_date": "2026-09-30"}}, post_training=True
        )
        assert len(errs) == 1
        assert "dataset.test_snap_dates is empty" in errs[0]

    def test_unset_snap_date_rejects_under_post_training(self):
        # Match on the readability wording, NOT "is not a test month": an unset
        # value must not be reported as "some other month" — it never reached
        # the membership test.
        params = {"dataset": {"test_snap_dates": ["2026-01-31"]}, "evaluation": {}}
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert "not a readable ISO date" in errs[0]

    def test_malformed_snap_date_rejects_with_readability_wording(self):
        params = _eval_params("31/01/2026", "2026-01-31")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert "not a readable ISO date" in errs[0]
        assert "31/01/2026" in errs[0]

    def test_unreadable_configured_month_blames_the_config(self):
        # Mirrors A21: a broken test_snap_dates entry must not surface as
        # "your snap_date is wrong" with a truncated available-months list.
        params = _eval_params("2026-01-31", "2026-01-31", "not-a-date")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert "unreadable date" in errs[0]
        assert "not-a-date" in errs[0]

    def test_unquoted_yaml_date_in_config_matches_string_snap_date(self):
        # PyYAML turns an unquoted 2026-01-31 into datetime.date on either side.
        params = _eval_params("2026-01-31", _dt.date(2026, 1, 31))
        assert post_training_snap_date_errors(params, post_training=True) == []

    def test_unquoted_yaml_date_as_snap_date_matches_string_config(self):
        params = _eval_params(_dt.date(2026, 1, 31), "2026-01-31")
        assert post_training_snap_date_errors(params, post_training=True) == []

    def test_datetime_shaped_snap_date_is_accepted(self):
        # pd.Timestamp accepts these everywhere else in the pipeline, so A22
        # must not be stricter than the code it guards.
        params = _eval_params("2026-01-31 00:00:00", "2026-01-31")
        assert post_training_snap_date_errors(params, post_training=True) == []

    def test_datetime_shaped_configured_month_is_accepted(self):
        # The date-shaped cases above cannot see whether the CONFIG side is
        # normalised — str(datetime.date(2026, 1, 31)) is already "2026-01-31",
        # so dropping _iso_date there leaves them green. These two forms are
        # the ones where str() and the ISO normal form actually differ.
        assert post_training_snap_date_errors(
            _eval_params("2026-01-31", "2026-01-31 00:00:00"), post_training=True
        ) == []
        assert post_training_snap_date_errors(
            _eval_params("2026-01-31", _dt.datetime(2026, 1, 31, 0, 0)),
            post_training=True,
        ) == []

    def test_missing_evaluation_section_does_not_crash(self):
        # No `evaluation` key at all — guards the defensive `or {}` read.
        errs = post_training_snap_date_errors(
            {"dataset": {"test_snap_dates": ["2026-01-31"]}}, post_training=True
        )
        assert len(errs) == 1
        assert "not a readable ISO date" in errs[0]

    # --- several evaluated dates (#374) ---

    def test_post_training_accepts_several_configured_months(self):
        params = _eval_params(
            ["2026-01-31", "2026-02-28"], "2026-01-31", "2026-02-28")
        assert post_training_snap_date_errors(params, post_training=True) == []

    def test_several_months_name_only_the_ones_that_are_not_test_months(self):
        params = _eval_params(
            ["2026-01-31", "2026-09-30", "2026-10-31"], "2026-01-31", "2026-02-28")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert ("evaluation.snap_date date(s) ['2026-09-30', '2026-10-31'] "
                "are not test months") in errs[0], errs[0]
        assert "dataset.test_snap_dates: ['2026-01-31', '2026-02-28']" \
            in errs[0], errs[0]

    def test_several_months_with_an_unreadable_one_name_it(self):
        params = _eval_params(["2026-01-31", "31/01/2026"], "2026-01-31")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert "not a readable ISO date" in errs[0]
        assert "['31/01/2026']" in errs[0], errs[0]

    def test_a_date_listed_twice_reads_like_the_single_date(self):
        """"Several dates" is counted after repeats are dropped, as every other
        reader of the key counts it (``len(as_date_list(value)) > 1``)."""
        params = _eval_params(["2026-09-30", "2026-09-30"], "2026-01-31")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert errs[0].startswith(
            "(A22) evaluation.snap_date='2026-09-30' is not a test month"), errs[0]

    def test_a_one_element_list_reads_like_the_single_date(self):
        params = _eval_params(["2026-09-30"], "2026-01-31", "2026-02-28")
        errs = post_training_snap_date_errors(params, post_training=True)
        assert len(errs) == 1
        assert errs[0].startswith(
            "(A22) evaluation.snap_date='2026-09-30' is not a test month"), errs[0]


# =============================================================================
# A24 — date splits must be mutually disjoint (dataset command)
# =============================================================================

from recsys_tfb.core.consistency import date_split_overlap_errors


def _split_params(**splits) -> dict:
    """A ``parameters`` dict carrying only the three snap_date split keys."""
    return {"dataset": {f"{name}_snap_dates": dates for name, dates in splits.items()}}


class TestDateSplitOverlapA24:
    def test_disjoint_splits_pass(self):
        params = _split_params(
            train=["2026-01-31", "2026-02-28"],
            val=["2026-04-30"],
            test=["2026-05-31"],
        )
        assert date_split_overlap_errors(params) == []

    def test_overlap_names_both_splits_and_the_shared_dates(self):
        params = _split_params(
            train=["2026-01-31", "2026-02-28"],
            test=["2026-02-28"],
            val=["2026-04-30"],
        )
        errs = date_split_overlap_errors(params)
        assert len(errs) == 1
        assert "train" in errs[0] and "test" in errs[0]
        assert "2026-02-28" in errs[0]
        # The clean pair must not be dragged into the message.
        assert "val" not in errs[0]

    def test_same_day_written_differently_is_an_overlap(self):
        # Deliberate tightening (ADR-0008 section 3): the comparison is by
        # calendar day, not by literal. The predecessor compared str(), so
        # this config used to pass.
        params = _split_params(train=["2026-01-31"], test=["2026-1-31"])
        errs = date_split_overlap_errors(params)
        assert len(errs) == 1
        assert "train" in errs[0] and "test" in errs[0]

    def test_datetime_shaped_literal_is_the_same_day(self):
        # Only forms whose str() differs from the ISO day can discriminate:
        # str(datetime.date(2026, 1, 31)) is already "2026-01-31", so a
        # date-vs-string pair stays green with or without normalisation.
        assert len(date_split_overlap_errors(
            _split_params(train=["2026-01-31 00:00:00"], val=["2026-01-31"])
        )) == 1
        assert len(date_split_overlap_errors(
            _split_params(train=[_dt.datetime(2026, 1, 31, 0, 0)], val=["2026-01-31"])
        )) == 1

    def test_different_days_still_pass_after_normalisation(self):
        # Guards the tightening from overshooting into "everything overlaps".
        assert date_split_overlap_errors(
            _split_params(train=["2026-1-31"], test=["2026-2-28"])
        ) == []

    def test_three_way_overlap_reports_every_pair(self):
        # Collect-all, like validate_config_consistency: one run per pair would
        # make a three-way collision take three edit-rerun cycles to clear.
        params = _split_params(
            train=["2026-04-30"],
            val=["2026-04-30"],
            test=["2026-04-30"],
        )
        errs = date_split_overlap_errors(params)
        assert len(errs) == 3
        joined = " ".join(errs)
        for a, b in (("train", "val"), ("train", "test"), ("val", "test")):
            assert any(a in e and b in e for e in errs), (a, b, joined)

    def test_only_train_configured_passes(self):
        # The other three splits are optional; absent keys are not an overlap.
        assert date_split_overlap_errors(
            {"dataset": {"train_snap_dates": ["2026-01-31"]}}
        ) == []

    def test_config_without_a_dataset_block_passes(self):
        # A source_etl / inference config never sets these keys.
        assert date_split_overlap_errors({}) == []
        assert date_split_overlap_errors({"dataset": None}) == []

    def test_unparseable_literals_do_not_collapse_into_one_day(self):
        # Both are unreadable; reporting them as the same month would be a lie
        # (and pd.NaT == pd.NaT is False, so a naive port gets this wrong).
        assert date_split_overlap_errors(
            _split_params(train=["not-a-date"], test=["also-not-a-date"])
        ) == []
        # Identical unreadable literals are still literally the same entry —
        # the string comparison this replaced said so too.
        assert len(date_split_overlap_errors(
            _split_params(train=["not-a-date"], test=["not-a-date"])
        )) == 1

    def test_a_null_yaml_entry_does_not_crash_the_check(self):
        # A trailing "-" in the yaml list gives None, and pd.Timestamp(None)
        # is NaT rather than an exception — a separate branch from the
        # unparseable-string one above.
        assert date_split_overlap_errors(
            _split_params(train=[None, "2026-01-31"], val=["2026-02-28"])
        ) == []
        assert len(date_split_overlap_errors(
            _split_params(train=[None], val=[None])
        )) == 1

    def test_a_non_date_scalar_does_not_collapse_distinct_months(self):
        # An unquoted yaml 20260131 is an int, and pd.Timestamp reads a bare
        # int as NANOSECONDS since the epoch — so two different months would
        # both normalise to 1970-01-01 and A24 would invent an overlap that
        # blocks a legitimate run. Only the forms _iso_date accepts (str /
        # date / datetime / pd.Timestamp) may reach the parser.
        assert date_split_overlap_errors(
            _split_params(train=[20260131], val=[20260228])
        ) == []

    def test_a_time_of_day_does_not_hide_a_collision(self):
        # .normalize() truncates to midnight. Without it these are two
        # distinct Timestamps and the overlap goes unreported — and the
        # same-spelling tests above cannot catch that, since
        # pd.Timestamp("2026-1-31") already equals pd.Timestamp("2026-01-31").
        # A21/A22 truncate to the day too (_iso_date returns YYYY-MM-DD), so
        # this keeps the three invariants reading a snap_date the same way.
        errs = date_split_overlap_errors(
            _split_params(train=["2026-01-31 09:00:00"], test=["2026-01-31"])
        )
        assert len(errs) == 1

    def test_message_shows_each_split_its_own_literal(self):
        # The operator's next move is to grep the yaml; printing only the
        # normalised day sends them looking for text that is not in the file.
        errs = date_split_overlap_errors(
            _split_params(train=["2026-1-31"], test=["2026-01-31"])
        )
        assert "2026-1-31" in errs[0], errs[0]
        assert "2026-01-31" in errs[0], errs[0]


# --- A23: dataset.train_snap_dates required and non-empty (#158) ---

from recsys_tfb.core.consistency import train_snap_dates_errors


class TestTrainSnapDatesA23:
    """A23 — ``dataset.train_snap_dates`` required and non-empty (#158).

    Four branches, four messages, because they are four different fixes. Each
    test matches on wording unique to its branch: every message contains
    ``train_snap_dates``, so matching on that would let any branch satisfy any
    test.
    """

    def test_a_configured_list_passes(self):
        assert train_snap_dates_errors(
            _split_params(train=["2026-01-31", "2026-02-28"])
        ) == []

    def test_unquoted_yaml_dates_pass(self):
        # PyYAML builds datetime.date for an unquoted scalar; _iso_date handles
        # it everywhere else, and A23 must not be the one place that rejects a
        # config the rest of the pipeline reads fine.
        assert train_snap_dates_errors(
            {"dataset": {"train_snap_dates": [_dt.date(2026, 1, 31)]}}
        ) == []

    def test_absent_key_is_reported(self):
        errs = train_snap_dates_errors({"dataset": {}})
        assert len(errs) == 1
        assert "absent" in errs[0]
        assert "dataset.train_snap_dates" in errs[0]

    def test_absent_dataset_block_is_reported_the_same_way(self):
        assert "absent" in train_snap_dates_errors({})[0]

    def test_empty_list_is_reported(self):
        # The branch that matters most: an empty list raises nothing today.
        # `restrict_to_months_or_all` leaves the pool WHOLE rather than empty,
        # so train silently draws from every month in sample_pool — including
        # the test months, which makes their metrics in-sample. A24 cannot see
        # it (an empty set overlaps nothing).
        errs = train_snap_dates_errors(_split_params(train=[]))
        assert len(errs) == 1
        assert "empty" in errs[0]

    def test_a_bare_string_is_reported_as_not_a_list(self):
        # A string is iterable, so none of the three index sites raise: they
        # walk it character by character and try to parse "2" as a date.
        errs = train_snap_dates_errors(_split_params(train="2026-01-31"))
        assert len(errs) == 1
        assert "list" in errs[0]

    def test_an_unparseable_entry_is_reported_with_its_own_literal(self):
        errs = train_snap_dates_errors(_split_params(train=["2026-01-31", "31/01/2026"]))
        assert len(errs) == 1
        assert "31/01/2026" in errs[0]
        # The good entry is not dragged into the message.
        assert "2026-01-31" not in errs[0]

    def test_every_unparseable_entry_is_named_in_one_pass(self):
        errs = train_snap_dates_errors(_split_params(train=["nope", "also-nope"]))
        assert len(errs) == 1
        assert "nope" in errs[0] and "also-nope" in errs[0]

    def test_the_four_branches_have_distinguishable_messages(self):
        # Guards the premise of every test above: if two branches ever share
        # wording, those tests stop telling the branches apart and a swapped
        # `if` goes unnoticed.
        messages = [
            train_snap_dates_errors({"dataset": {}})[0],
            train_snap_dates_errors(_split_params(train=[]))[0],
            train_snap_dates_errors(_split_params(train="2026-01-31"))[0],
            train_snap_dates_errors(_split_params(train=["nope"]))[0],
        ]
        assert len(set(messages)) == 4


# =============================================================================
# A25 — training HPO / finalize parameter domains (aggregated at CLI entry)
# =============================================================================

from recsys_tfb.core.consistency import (
    FINAL_MODEL_STRATEGIES,
    HPO_OBJECTIVES,
    training_hpo_finalize_param_errors,
)


class TestTrainingHpoFinalizeParamsA25:
    def _params(self, **training) -> dict:
        return {"training": training, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},}

    def test_absent_keys_are_clean(self):
        # Both keys default in the node bodies (mean_ap / hpo_best), so a
        # config that never names them is valid and must stay valid.
        assert training_hpo_finalize_param_errors({}) == []
        assert training_hpo_finalize_param_errors(self._params()) == []

    def test_every_admitted_value_is_accepted(self):
        for objective in HPO_OBJECTIVES:
            assert training_hpo_finalize_param_errors(
                self._params(hpo_objective=objective)) == [], objective
        for strategy in FINAL_MODEL_STRATEGIES:
            assert training_hpo_finalize_param_errors(
                self._params(final_model_strategy=strategy)) == [], strategy

    def test_bad_hpo_objective_rejected(self):
        errs = training_hpo_finalize_param_errors(
            self._params(hpo_objective="mean_apk"))
        assert len(errs) == 1
        assert "A25" in errs[0]
        assert "training.hpo_objective" in errs[0]
        assert "mean_apk" in errs[0]
        # The admitted values belong in the message: the operator's next move
        # is to retype the value, not to go read the source.
        assert "macro_per_item_map" in errs[0]

    def test_bad_final_model_strategy_rejected(self):
        errs = training_hpo_finalize_param_errors(
            self._params(final_model_strategy="refit"))
        assert len(errs) == 1
        assert "A25" in errs[0]
        assert "training.final_model_strategy" in errs[0]
        assert "refit" in errs[0]
        assert "refit_on_full" in errs[0]

    def test_an_explicit_yaml_null_is_rejected_not_treated_as_absent(self):
        # An absent key and a key written `final_model_strategy:` with no value
        # are NOT the same thing, and the difference is silent:
        # dict.get(key, default) returns None when the key is present and null,
        # so the node's default never applies. finalize_model then reads None,
        # fails its `== "hpo_best"` test, and runs a full refit_on_full without
        # saying anything — which is what this gate exists to stop. Skipping on
        # `value is None` would map null and "refit_on_full" onto one outcome.
        for key in ("hpo_objective", "final_model_strategy"):
            errs = training_hpo_finalize_param_errors(self._params(**{key: None}))
            assert len(errs) == 1, (key, errs)
            assert key in errs[0]

    def test_the_node_default_really_does_not_cover_an_explicit_null(self):
        # Premise guard for the test above. If dict.get ever did fall back to
        # the default for a present-but-null key, rejecting null would be
        # over-strict — this pins the reason rather than leaving it in a comment.
        present_null = {"final_model_strategy": None}
        assert present_null.get("final_model_strategy", "hpo_best") is None
        assert {}.get("final_model_strategy", "hpo_best") == "hpo_best"

    def test_both_keys_reported_in_one_pass(self):
        # Collect-all is the whole point of the entry gate: fixing one typo
        # must not cost another run to discover the second.
        errs = training_hpo_finalize_param_errors(
            self._params(hpo_objective="nope", final_model_strategy="also-nope"))
        assert len(errs) == 2

    def test_registered_in_validate_config_consistency(self):
        # A25 is aggregated (unlike A26): a typo here costs a full HPO search
        # to discover at the node, and every command can afford the check —
        # the keys are optional, so a config that omits them stays clean.
        from recsys_tfb.core.consistency import validate_config_consistency
        with pytest.raises(ConfigConsistencyError, match="A25"):
            validate_config_consistency(self._params(final_model_strategy="refit"))


# =============================================================================
# A26 — dataset.test_snap_dates must not spell one month two ways
#       (wired on the training command)
# =============================================================================

from recsys_tfb.core.consistency import duplicate_test_month_errors


def _test_months(*dates) -> dict:
    return {"dataset": {"test_snap_dates": list(dates)}, "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},}


class TestTestSnapDatesSpellingA26:
    def test_distinct_months_pass(self):
        assert duplicate_test_month_errors(
            _test_months("2026-01-31", "2026-02-28")) == []

    def test_absent_or_empty_key_is_clean(self):
        assert duplicate_test_month_errors({}) == []
        assert duplicate_test_month_errors({"dataset": {}}) == []
        assert duplicate_test_month_errors(_test_months()) == []

    def test_the_same_literal_twice_is_not_an_error(self):
        # Deliberate, and it is the behaviour the node body had: the training
        # cache keys on the month, so a repeated literal collapses to one
        # entry and changes nothing. Only two DIFFERENT spellings are
        # ambiguous, because then the Hive partition value differs between
        # them and only one of the two can be right.
        assert duplicate_test_month_errors(
            _test_months("2026-01-31", "2026-01-31")) == []

    def test_iso_and_compact_spelling_collide(self):
        errs = duplicate_test_month_errors(
            _test_months("2026-01-31", "20260131"))
        assert len(errs) == 1
        assert "A26" in errs[0]
        # Both literals, because the operator's next move is to grep their yaml.
        assert "2026-01-31" in errs[0] and "20260131" in errs[0]

    def test_surrounding_whitespace_is_the_same_month(self):
        errs = duplicate_test_month_errors(
            _test_months("2026-01-31", " 2026-01-31 "))
        assert len(errs) == 1

    def test_each_colliding_month_is_reported_once(self):
        errs = duplicate_test_month_errors(_test_months(
            "2026-01-31", "20260131", "2026-02-28", "20260228"))
        assert len(errs) == 2
        # A clean pair is never dragged into the other month's message.
        assert "2026-02-28" not in errs[0]

    def test_three_spellings_of_one_month_are_one_error_naming_all_three(self):
        errs = duplicate_test_month_errors(
            _test_months("2026-01-31", "20260131", " 20260131"))
        assert len(errs) == 1
        assert errs[0].count("2026") >= 3

    def test_different_months_written_the_short_way_do_not_collide(self):
        # The guard that keeps this predicate from inventing collisions: the
        # key must stay per-month, so two distinct compact literals are two
        # distinct months.
        assert duplicate_test_month_errors(
            _test_months(20260131, 20260228)) == []
        assert duplicate_test_month_errors(
            _test_months("20260131", "20260228")) == []

    def test_the_key_is_the_one_the_training_cache_uses(self):
        # Premise guard: A26 is only worth anything if it groups literals the
        # way the cache does. If month_dir ever changes, this fails
        # instead of A26 silently going stale.
        from recsys_tfb.core.consistency import _test_month_key
        from recsys_tfb.pipelines.training.steps.predict_months import (
            month_dir,
        )
        for literal in ("2026-01-31", "20260131", " 2026-01-31 ", "2026-1-31"):
            assert _test_month_key(literal) == month_dir(literal), literal

    def test_not_aggregated_by_validate_config_consistency(self):
        # Wired on the training command, like A24 on dataset: the aggregator
        # runs at the entry of EVERY command, while the harm this predicate
        # front-runs (two cache entries pointing at one directory → every row
        # of that month counted twice) only exists in the training pipeline.
        from recsys_tfb.core.consistency import validate_config_consistency
        validate_config_consistency(_test_months("2026-01-31", "20260131"))


# =============================================================================
# A36 — training needs at least one dataset.test_snap_dates month
#       (wired on the training command, #133)
# =============================================================================

from recsys_tfb.core.consistency import missing_test_month_errors


class TestTestSnapDatesRequiredA36:
    def test_a_configured_month_passes(self):
        assert missing_test_month_errors(_test_months("2026-01-31")) == []

    def test_empty_list_is_reported(self):
        # The case #133 is about: nothing stops it today, and training only
        # fails at predict_and_write_test_predictions, after the whole HPO
        # search, with a message about parquet roots rather than this key.
        errs = missing_test_month_errors(_test_months())
        assert len(errs) == 1
        assert "A36" in errs[0]
        assert "dataset.test_snap_dates" in errs[0]
        assert "is empty" in errs[0]

    def test_absent_key_is_reported_as_absent(self):
        # Downstream reads an absent key as `or []`, so it is the same run as
        # an empty list — but "empty" would send the operator looking for a
        # line that is not in their yaml.
        for params in ({}, {"dataset": {}}):
            errs = missing_test_month_errors(params)
            assert len(errs) == 1, params
            assert "is absent" in errs[0], params

    def test_yaml_null_is_reported(self):
        # `test_snap_dates:` with nothing after it loads as None; the key is
        # there, so the message says empty rather than absent.
        errs = missing_test_month_errors({"dataset": {"test_snap_dates": None}})
        assert len(errs) == 1
        assert "is empty" in errs[0]

    def test_not_aggregated_by_validate_config_consistency(self):
        # The aggregator runs at the entry of EVERY command, and the dataset
        # command legitimately builds with no test month. Aggregating A36 would
        # block that — the #158 shape A24/A26 were wired around.
        from recsys_tfb.core.consistency import validate_config_consistency
        validate_config_consistency(_test_months())


# =============================================================================
# A27 — the inference scoring grid (snap_dates x entity_buckets x products)
#       must be non-degenerate (wired on the inference command)
# =============================================================================

from recsys_tfb.core.consistency import inference_grid_errors


def _grid(**inference) -> dict:
    return {"inference": dict(inference)}


_OK_GRID = dict(snap_dates=["2025-12-31"], entity_buckets=10, products=["a", "b"])


class TestInferenceGridA27:
    def test_a_populated_grid_passes(self):
        assert inference_grid_errors(_grid(**_OK_GRID)) == []

    def test_empty_snap_dates_is_an_error(self):
        # The raise this replaces lived in steps/scoping.py, and it only fired
        # after build_inference_population_features had already run a full
        # Spark pass. Naming the key, not the symptom: the operator's next
        # move is to grep their yaml.
        errs = inference_grid_errors(_grid(**{**_OK_GRID, "snap_dates": []}))
        assert len(errs) == 1
        assert "A27" in errs[0] and "inference.snap_dates" in errs[0]

    def test_zero_entity_buckets_is_an_error(self):
        # Zero buckets means zero chunks, which the run reports as a success
        # that scored nobody — the failure shape an operator cannot see.
        errs = inference_grid_errors(_grid(**{**_OK_GRID, "entity_buckets": 0}))
        assert len(errs) == 1
        assert "A27" in errs[0] and "inference.entity_buckets" in errs[0]

    def test_negative_entity_buckets_is_an_error(self):
        assert len(inference_grid_errors(
            _grid(**{**_OK_GRID, "entity_buckets": -1}))) == 1

    def test_one_bucket_is_legal(self):
        # The healthy window (5-20) is a warning in chunk_plans, not a gate.
        # A27 must not quietly promote that warning to an error: a small
        # population legitimately runs in one bucket.
        assert inference_grid_errors(
            _grid(**{**_OK_GRID, "entity_buckets": 1})) == []
        assert inference_grid_errors(
            _grid(**{**_OK_GRID, "entity_buckets": 500})) == []

    def test_absent_entity_buckets_is_clean(self):
        # Absent means DEFAULT_ENTITY_BUCKETS, which is 10 — a valid grid.
        # Only an explicit value can be degenerate.
        params = _grid(**{k: v for k, v in _OK_GRID.items() if k != "entity_buckets"})
        assert inference_grid_errors(params) == []

    def test_unparseable_entity_buckets_is_reported_not_raised(self):
        # The node does int(...), which would raise inside this predicate and
        # abort the collect-all — the operator would then see one problem per
        # run instead of all of them. Report it as an error string instead.
        for value in ("ten", None, [10]):
            errs = inference_grid_errors(
                _grid(**{**_OK_GRID, "entity_buckets": value}))
            assert len(errs) == 1, value
            assert "inference.entity_buckets" in errs[0]

    def test_empty_products_is_an_error(self):
        errs = inference_grid_errors(_grid(**{**_OK_GRID, "products": []}))
        assert len(errs) == 1
        assert "A27" in errs[0] and "inference.products" in errs[0]

    def test_every_broken_axis_is_reported_in_one_pass(self):
        # The whole point of moving these to Layer 1: the node-side raises
        # aborted on the first one, so a config wrong in two places cost two
        # Spark cold starts to find out.
        errs = inference_grid_errors(
            _grid(snap_dates=[], entity_buckets=0, products=[]))
        assert len(errs) == 3

    def test_an_absent_inference_section_reports_the_two_axes_with_no_default(self):
        # Reachable: the gate is wired on the inference command, so running
        # `inference` against a config with no inference section lands here.
        # Today that is a bare KeyError from nodes.py.
        #
        # Two errors, not three: entity_buckets is absent here like anywhere
        # else, so it takes DEFAULT_ENTITY_BUCKETS and is not degenerate. Only
        # snap_dates and products have no default to fall back on.
        errs = inference_grid_errors({})
        assert len(errs) == 2
        assert "inference.entity_buckets" not in " ".join(errs)
        assert inference_grid_errors({"inference": None}) == errs

    def test_the_keys_are_the_ones_the_inference_pipeline_reads(self):
        # Premise guard, A26's pattern: A27 is worth nothing if it inspects
        # keys the pipeline no longer reads. Renaming a key in scoping.py
        # would otherwise leave a gate that can never fire, with every test
        # above still green. chunk_plans/scoping are the two readers.
        from recsys_tfb.pipelines.inference.steps.scoping import (
            DEFAULT_ENTITY_BUCKETS,
            entity_buckets,
            iso_snap_dates,
        )

        # Absent -> default, which is why A27 leaves an absent key alone.
        assert entity_buckets({}) == DEFAULT_ENTITY_BUCKETS
        assert DEFAULT_ENTITY_BUCKETS >= 1
        # ... and the degenerate value A27 rejects is the one the helper reads
        # from that same key. A rename fails here instead of going silent.
        assert entity_buckets(_grid(entity_buckets=0)) == 0
        assert iso_snap_dates(_grid(snap_dates=["2025-12-31"])) == ["2025-12-31"]
        assert iso_snap_dates({}) == []

    def test_the_shipped_config_passes(self):
        # The other half of the premise: the keys A27 names must be the ones
        # actually written in conf/base. Checked against the real tree, like
        # A30's conf/local test — a gate that rejects the repo's own config
        # is a gate nobody can run the pipeline behind.
        from pathlib import Path

        import yaml

        import recsys_tfb

        repo_root = Path(recsys_tfb.__file__).resolve().parents[2]
        shipped = yaml.safe_load(
            (repo_root / "conf" / "base" / "parameters_inference.yaml").read_text()
        )
        assert inference_grid_errors(shipped) == []

    def test_the_node_backstops_still_say_what_a27_says(self):
        # This module's docstring forbids message drift, and the repo's other
        # registered backstops avoid it by CALLING the predicate (core/schema
        # .py and pipelines/dataset/nodes.py both call resolved_item_values).
        # plan_scoring_chunks cannot: it takes the expanded grid, not `parameters`. So
        # the two copies are pinned here instead. Drop a consequence clause
        # from either side and this fails, rather than the operator getting
        # two different explanations of the same config mistake depending on
        # which layer noticed.
        import pytest as _pytest

        from recsys_tfb.pipelines.inference.steps.chunk_plans import (
            plan_scoring_chunks,
        )

        a27 = " ".join(inference_grid_errors(
            _grid(snap_dates=[], entity_buckets=0, products=[])))

        degenerate = [
            dict(snap_dates=["2025-12-31"], items=["a"], n_buckets=0),
            dict(snap_dates=[], items=["a"], n_buckets=1),
            dict(snap_dates=["2025-12-31"], items=[], n_buckets=1),
        ]
        for kwargs in degenerate:
            with _pytest.raises(ValueError) as exc:
                plan_scoring_chunks(written=[], rebuild=[], **kwargs)
            # The consequence clause, not the whole sentence: A27 prefixes its
            # code and merges the two snap_dates raises, so the sentences are
            # not identical by design.
            consequence = str(exc.value).split("; ")[-1].split(". ", 1)[-1]
            assert consequence.rstrip(".") in a27, consequence

        # scoping.py's snap_dates raise is the fourth, and reaching it needs a
        # Spark frame — pinned against the source line instead.
        from pathlib import Path

        from recsys_tfb.pipelines.inference.steps import scoping

        assert "every historical month would be republished" in Path(
            scoping.__file__).read_text()
        assert "every historical month would be republished" in a27

    def test_not_aggregated_by_validate_config_consistency(self):
        # Wired on the inference command, like A23/A24 on dataset and A26 on
        # training: the aggregator runs at the entry of EVERY command, and
        # these three keys are read by the inference pipeline alone. Issue
        # #158 measured what aggregating a one-pipeline key costs — 9
        # unrelated tests blocked.
        from recsys_tfb.core.consistency import validate_config_consistency

        validate_config_consistency(
            {"schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label"}}}
        )


# =============================================================================
# A28 — the prediction write target must declare every schema.entity column
#       (wired on the training command)
# =============================================================================

from recsys_tfb.core.consistency import entity_columns_declared_errors


def _entity_params(*entity) -> dict:
    return {"schema": {"columns": {
        "time": "snap_date", "entity": list(entity), "item": "prod_name"}}}


class TestEntityColumnsDeclaredA28:
    def test_a_declaration_covering_every_entity_column_passes(self):
        assert entity_columns_declared_errors(
            _entity_params("cust_id", "acct_id"),
            ["cust_id", "acct_id", "score", "model_version", "snap_date"],
            "training_eval_predictions",
        ) == []

    def test_a_missing_entity_column_is_reported(self):
        errs = entity_columns_declared_errors(
            _entity_params("cust_id", "acct_id"),
            ["cust_id", "score", "model_version", "snap_date"],
            "training_eval_predictions",
        )
        assert len(errs) == 1
        assert "A28" in errs[0]
        # The column and the entry, because those are the two things the
        # operator needs to open their yaml at the right line.
        assert "acct_id" in errs[0]
        assert "training_eval_predictions" in errs[0]

    def test_every_missing_column_is_named_in_one_error(self):
        # One error, not one per column: they are all fixed in the same edit.
        errs = entity_columns_declared_errors(
            _entity_params("cust_id", "acct_id", "sub_id"),
            ["cust_id", "score"],
            "training_eval_predictions",
        )
        assert len(errs) == 1
        assert "acct_id" in errs[0] and "sub_id" in errs[0]

    def test_none_means_the_entry_infers_its_schema_and_drops_nothing(self):
        # `columns: "auto"` declares nothing, so there is no declaration to
        # fall short of. Treating None as "declares no columns" would reject
        # every auto entry — the opposite of the truth.
        assert entity_columns_declared_errors(
            _entity_params("cust_id", "acct_id"), None, "some_auto_table",
        ) == []

    def test_the_single_entity_schema_passes_the_real_declaration(self):
        # The discriminating case: without it every test above is also passed
        # by a predicate that rejects everything. This is the shape
        # conf/base/catalog.yaml actually ships. Spelled out rather than left
        # to a default -- since #328 there is no default to leave it to.
        assert entity_columns_declared_errors(
            _entity_params("cust_id"),
            ["cust_id", "score", "score_uncalibrated", "label",
             "model_version", "snap_date", "prod_name"],
            "training_eval_predictions",
        ) == []

    def test_an_extra_declared_column_is_not_an_error(self):
        # Coverage, not equality: a table may carry columns that are not
        # entity columns — every real one does.
        assert entity_columns_declared_errors(
            _entity_params("cust_id"),
            ["cust_id", "score", "label", "snap_date"],
            "training_eval_predictions",
        ) == []


# =============================================================================
# A29 — dataset.train_split_keys / val_sample_keys shape (aggregated at CLI entry)
# =============================================================================

from recsys_tfb.core.consistency import entity_grouping_key_errors


class TestEntityGroupingKeysA29:
    def _params(self, entity=None, **dataset) -> dict:
        return {
            "schema": {"columns": {
                "time": "snap_date",
                "entity": entity or ["branch_id", "cust_id"],
                "item": "prod_name",
            }},
            "dataset": dataset,
        }

    def test_absent_keys_are_clean(self):
        # Neither key declared is the state every existing config is in, and
        # the one this whole feature promises not to disturb. Both spellings
        # of "absent": no ``dataset`` block at all, and an empty one.
        assert entity_grouping_key_errors(
            {"schema": {"columns": {
                "time": "snap_date", "entity": ["branch_id", "cust_id"],
                "item": "prod_name"}}}) == []
        assert entity_grouping_key_errors(self._params()) == []

    def test_full_entity_and_proper_subsets_are_accepted(self):
        for value in (["branch_id", "cust_id"], ["branch_id"], ["cust_id"]):
            assert entity_grouping_key_errors(
                self._params(train_split_keys=value)) == [], value
            assert entity_grouping_key_errors(
                self._params(val_sample_keys=value)) == [], value

    def test_column_outside_entity_is_rejected(self):
        errs = entity_grouping_key_errors(self._params(train_split_keys=["region"]))
        assert len(errs) == 1
        assert "A29" in errs[0]
        assert "dataset.train_split_keys" in errs[0]
        assert "region" in errs[0]
        # The legal set belongs in the message: the operator's next move is to
        # retype the column, not to go read schema.py.
        assert "branch_id" in errs[0] and "cust_id" in errs[0]

    def test_a_typo_in_one_of_several_columns_is_rejected(self):
        # The all-wrong case is the easy one. A list that is *mostly* right is
        # where a subset check earns its keep: `["branch_id", "cust_di"]` still
        # groups on something, so without this the run would succeed on a
        # coarser unit than asked for.
        errs = entity_grouping_key_errors(
            self._params(val_sample_keys=["branch_id", "cust_di"]))
        assert len(errs) == 1
        # Only the offending column is named as unknown; the one that is fine
        # must not be swept into the complaint.
        assert "['cust_di']" in errs[0]

    def test_empty_list_is_rejected(self):
        errs = entity_grouping_key_errors(self._params(train_split_keys=[]))
        assert len(errs) == 1
        assert "A29" in errs[0]
        assert "empty list" in errs[0]

    def test_an_explicit_yaml_null_is_rejected_not_treated_as_absent(self):
        # Null and absent produce the same split, but not the same artifact:
        # a present key joins the version payload, so the null form rebuilds
        # everything under train_variant_id / base_dataset_version while
        # changing no behaviour. Treating it as absent would hide that.
        for key in ("train_split_keys", "val_sample_keys"):
            errs = entity_grouping_key_errors(self._params(**{key: None}))
            assert len(errs) == 1, (key, errs)
            assert f"dataset.{key}" in errs[0]

    def test_a_bare_string_is_rejected(self):
        # schema.entity accepts a bare string and normalises it to a list, so
        # `train_split_keys: cust_id` looks like it should work. It must not
        # pass silently: a string is iterable, and a downstream select(*"cust_id")
        # would ask for seven one-character columns.
        errs = entity_grouping_key_errors(self._params(train_split_keys="cust_id"))
        assert len(errs) == 1
        assert "list of column names" in errs[0]

    def test_both_keys_wrong_are_reported_together(self):
        # Collect-all: fixing one key per run is exactly what this gate exists
        # to avoid, and both keys sit in the same config block.
        errs = entity_grouping_key_errors(
            self._params(train_split_keys=["nope"], val_sample_keys=["also_nope"]))
        assert len(errs) == 2
        assert any("train_split_keys" in e for e in errs)
        assert any("val_sample_keys" in e for e in errs)

    def test_wired_into_validate_config_consistency(self):
        # The predicate being correct is worth nothing if nothing calls it.
        params = self._params(train_split_keys=["region"])
        with pytest.raises(ConfigConsistencyError, match="A29"):
            validate_config_consistency(params)


class TestA30EnvDirExists:
    """A30 — ``--env`` must name an existing ``conf/<env>`` directory.

    The thing under test is a *silence*: ConfigLoader reads a missing overlay
    as an empty one, so before this gate every assertion about a typo'd --env
    was "the run succeeded". Each test below names the input that used to pass.
    """

    def _conf_tree(self, tmp_path, *names):
        conf = tmp_path / "conf"
        for name in ("base", *names):
            (conf / name).mkdir(parents=True)
        return conf

    def test_existing_env_dir_returns_the_path(self, tmp_path):
        from recsys_tfb.core.consistency import resolved_env_dir

        conf = self._conf_tree(tmp_path, "local")
        assert resolved_env_dir(conf, "local") == conf / "local"

    def test_conf_local_exists_in_this_repo(self):
        # The default is --env local, so if conf/local/ ever stops being
        # committed this gate turns every default invocation into an error.
        # Checked against the real tree, not a tmp fixture, on purpose.
        from pathlib import Path

        import recsys_tfb

        repo_root = Path(recsys_tfb.__file__).resolve().parents[2]
        assert (repo_root / "conf" / "local").is_dir()

    def test_missing_env_dir_raises(self, tmp_path):
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            resolved_env_dir,
        )

        conf = self._conf_tree(tmp_path, "local")
        with pytest.raises(ConfigConsistencyError, match="A30") as exc:
            resolved_env_dir(conf, "prod")
        assert "prod" in str(exc.value)

    def test_message_lists_the_directories_that_do_exist(self, tmp_path):
        # A typo is nearly always a near-miss of a real name; the fix is one
        # word away and the message should hand the operator that word.
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            resolved_env_dir,
        )

        conf = self._conf_tree(tmp_path, "local", "prod")
        with pytest.raises(ConfigConsistencyError) as exc:
            resolved_env_dir(conf, "pord")
        assert "'local'" in str(exc.value) and "'prod'" in str(exc.value)

    def test_empty_env_is_rejected(self, tmp_path):
        # conf_dir / "" resolves back to conf/ itself, which IS a directory:
        # a bare existence check would pass it while merging nothing — the
        # same silent degradation A30 exists to stop.
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            resolved_env_dir,
        )

        conf = self._conf_tree(tmp_path, "local")
        for value in ("", "   ", None):
            with pytest.raises(ConfigConsistencyError, match="A30"):
                resolved_env_dir(conf, value)

    def test_missing_conf_dir_raises_rather_than_crashing(self, tmp_path):
        # Running from the wrong cwd means conf/ itself is absent. The error
        # must still be the A30 message, not an iterdir FileNotFoundError.
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            resolved_env_dir,
        )

        with pytest.raises(ConfigConsistencyError, match="A30"):
            resolved_env_dir(tmp_path / "conf", "local")

    def test_not_aggregated_by_validate_config_consistency(self, tmp_path):
        # validate_config_consistency takes parameters alone; it never sees
        # --env or the filesystem. Tidying A30 into it would need a fake
        # argument and would fire on configs that are fine.
        import inspect

        from recsys_tfb.core import consistency

        assert "resolved_env_dir" not in inspect.getsource(
            consistency.validate_config_consistency
        )


from recsys_tfb.core.consistency import (
    DEFAULT_NUMERIC_STORAGE_TYPE,
    DEFAULT_PRECISION_POLICY,
    NUMERIC_STORAGE_TYPES,
    PRECISION_POLICIES,
    SIGNIFICAND_BITS,
    ColumnPrecision,
    exact_value_limit,
    numeric_precision_errors,
    numeric_precision_rows,
    numeric_storage_param_errors,
    resolved_numeric_storage,
    spark_dtype_value_step,
    validate_config_consistency,
)


class TestNumericStorageParamsA31:
    def _params(self, **dataset) -> dict:
        return {"dataset": dataset}

    def test_absent_keys_are_clean(self):
        # Every config in the repo today is in this state; declaring the gate
        # must not make them all fail.
        assert numeric_storage_param_errors({}) == []
        assert numeric_storage_param_errors(self._params()) == []

    def test_declared_legal_values_are_accepted(self):
        for storage in NUMERIC_STORAGE_TYPES:
            for policy in PRECISION_POLICIES:
                assert numeric_storage_param_errors(self._params(
                    numeric_feature_storage_type=storage,
                    numeric_precision_policy=policy,
                )) == [], (storage, policy)

    def test_unknown_storage_type_is_rejected_and_lists_the_legal_values(self):
        errs = numeric_storage_param_errors(
            self._params(numeric_feature_storage_type="float16"))
        assert len(errs) == 1
        assert "A31" in errs[0]
        assert "dataset.numeric_feature_storage_type" in errs[0]
        assert "float16" in errs[0]
        # The operator's next move is to retype the value, so the legal set has
        # to be in the message rather than in a doc they have to go find.
        assert "float32" in errs[0] and "float64" in errs[0]

    def test_unknown_policy_is_rejected(self):
        errs = numeric_storage_param_errors(
            self._params(numeric_precision_policy="ignore"))
        assert len(errs) == 1
        assert "dataset.numeric_precision_policy" in errs[0]
        assert "block" in errs[0] and "truncate" in errs[0]

    def test_explicit_yaml_null_is_rejected_not_treated_as_absent(self):
        # `key:` with no value is not the same artifact as an absent key: it is
        # present in the version payload. Mirrors A25/A29.
        for key in ("numeric_feature_storage_type", "numeric_precision_policy"):
            errs = numeric_storage_param_errors(self._params(**{key: None}))
            assert len(errs) == 1, key
            assert key in errs[0]

    def test_both_keys_wrong_are_reported_together(self):
        errs = numeric_storage_param_errors(self._params(
            numeric_feature_storage_type="f32",
            numeric_precision_policy="off",
        ))
        assert len(errs) == 2

    def test_wired_into_validate_config_consistency(self):
        params = {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},
            "dataset": {"numeric_feature_storage_type": "float16"},
        }
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(params)
        assert "A31" in str(exc.value)


class TestResolvedNumericStorage:
    def test_absent_keys_resolve_to_the_declared_defaults(self):
        assert resolved_numeric_storage({}) == (
            DEFAULT_NUMERIC_STORAGE_TYPE, DEFAULT_PRECISION_POLICY,
        )

    def test_declared_values_win(self):
        assert resolved_numeric_storage({"dataset": {
            "numeric_feature_storage_type": "float64",
            "numeric_precision_policy": "truncate",
        }}) == ("float64", "truncate")

    def test_the_defaults_are_themselves_legal_values(self):
        # Guards the pair that A31 and this resolver would otherwise be free to
        # drift apart on: a default outside NUMERIC_STORAGE_TYPES would make
        # every untouched config fail its own gate.
        assert DEFAULT_NUMERIC_STORAGE_TYPE in NUMERIC_STORAGE_TYPES
        assert DEFAULT_PRECISION_POLICY in PRECISION_POLICIES


class TestSparkDtypeValueStep:
    """The grid spacing each dtype states. Getting this wrong is silent."""

    def test_integer_types_and_boolean_step_by_one(self):
        for dtype in ("tinyint", "smallint", "int", "bigint", "boolean"):
            assert spark_dtype_value_step(dtype) == 1.0, dtype

    def test_decimal_steps_by_ten_to_minus_scale(self):
        # The correction this ticket's review turned up: Spark DecimalType is
        # exact fixed-point, so it HAS a grid — reading it as "approximate,
        # therefore exempt" leaves the gate covering nothing, because decimal is
        # what the cast converts today.
        assert spark_dtype_value_step("decimal(38,10)") == 10.0 ** -10
        assert spark_dtype_value_step("decimal(18,2)") == 0.01
        assert spark_dtype_value_step("decimal(10,0)") == 1.0

    def test_float_and_double_state_no_grid(self):
        # These are genuinely approximate: nothing in the config says how far
        # apart their values are, so no bound can be stated and the gate stays
        # silent about them. This is what stops a permanent false alarm.
        for dtype in ("float", "double"):
            assert spark_dtype_value_step(dtype) is None, dtype

    def test_non_numeric_types_state_no_grid(self):
        for dtype in ("string", "date", "timestamp", "binary", "array<int>"):
            assert spark_dtype_value_step(dtype) is None, dtype


class TestExactValueLimit:
    def test_unit_step_reduces_to_the_significand(self):
        # The number issue #281 states, recovered from the general formula.
        assert exact_value_limit(1.0, "float32") == float(2 ** 24)
        assert exact_value_limit(1.0, "float64") == float(2 ** 53)

    def test_a_non_power_of_two_step_floors_to_the_binade_below(self):
        # The whole correction. `0.01 * 2**24` is 167,772.16 and is WRONG:
        # float32's spacing there is already 0.015625, so values 0.01 apart have
        # collided. Measured with numpy: 200k distinct values 0.01 apart keep
        # 128,000 at 167,772 and all 200,000 at 131,072.
        assert exact_value_limit(0.01, "float32") == 131072.0
        assert exact_value_limit(0.01, "float32") < 0.01 * 2 ** 24

    def test_a_power_of_two_step_needs_no_flooring(self):
        assert exact_value_limit(0.5, "float32") == float(2 ** 23)

    def test_bits_table_covers_every_declarable_storage_type(self):
        assert set(SIGNIFICAND_BITS) == set(NUMERIC_STORAGE_TYPES)


class TestNumericPrecisionErrorsB8:
    @staticmethod
    def _ints(**cols) -> dict:
        return {c: ColumnPrecision(v, 1.0) for c, v in cols.items()}

    def test_no_columns_is_clean(self):
        assert numeric_precision_errors({}, "float32") == []

    def test_max_exactly_at_the_limit_passes(self):
        # 2^24 itself is representable; the first integer that is not is
        # 2^24 + 1. An off-by-one here either rebuilds a dataset nobody needed
        # to rebuild or lets a lossy column through.
        assert numeric_precision_errors(self._ints(a=float(2 ** 24)), "float32") == []

    def test_one_above_the_limit_is_rejected(self):
        errs = numeric_precision_errors(self._ints(a=float(2 ** 24 + 1)), "float32")
        assert len(errs) == 1
        assert "B8" in errs[0]
        assert "'a'" in errs[0]

    def test_message_names_the_column_its_value_and_the_bound(self):
        errs = numeric_precision_errors(self._ints(acct_bal=33554433.0), "float32")
        assert "acct_bal" in errs[0]
        assert "33,554,433" in errs[0]
        assert "16,777,216" in errs[0]

    def test_message_offers_float64_last_and_says_what_it_costs(self):
        # #283 made the declaration real: the cast now reads
        # numeric_feature_storage_type, so widening it genuinely stores wider
        # values. Before that it was excluded on purpose — it would have
        # rebuilt everything, changed no stored value, and only raised this
        # gate's own bound, i.e. silenced the alarm.
        #
        # Offered last, and never bare: it widens every feature column to buy
        # headroom for the few that need it, so a reader who sees only the
        # remedy and not its price takes the expensive fix by default.
        errs = numeric_precision_errors(self._ints(a=float(2 ** 24 + 1)), "float32")
        msg = errs[0]
        assert "numeric_feature_storage_type: float64" in msg
        assert "doubles" in msg
        assert "numeric_precision_policy: truncate" in msg
        assert msg.index("representation upstream") < msg.index("float64")
        assert msg.index("float64") < msg.index("numeric_precision_policy")

    def test_a_decimal_column_is_bounded_by_its_own_resolution(self):
        # The case the whole correction exists for: decimal(18,2) values 0.01
        # apart cannot go past 131,072, far below the integer bound.
        assert numeric_precision_errors(
            {"bal": ColumnPrecision(131072.0, 0.01)}, "float32") == []
        errs = numeric_precision_errors(
            {"bal": ColumnPrecision(200000.0, 0.01)}, "float32")
        assert len(errs) == 1
        assert "131,072" in errs[0]
        assert "0.01 apart" in errs[0]

    def test_the_same_magnitude_passes_as_an_integer_and_fails_as_a_decimal(self):
        # Paired so neither half can be vacuous: 200,000 is fine on a grid of 1
        # and lossy on a grid of 0.01. A gate that ignored value_step would
        # answer the same for both.
        assert numeric_precision_errors(self._ints(a=200000.0), "float32") == []
        assert len(numeric_precision_errors(
            {"a": ColumnPrecision(200000.0, 0.01)}, "float32")) == 1

    def test_boolean_shaped_and_small_values_pass(self):
        assert numeric_precision_errors(
            self._ints(flag=1.0, zero=0.0, small=99.0), "float32") == []

    def test_all_null_column_reads_as_zero_and_passes(self):
        # The reader reports max(|x|) over zero non-null values as 0.0; a column
        # with no values cannot lose one.
        assert numeric_precision_errors(self._ints(empty=0.0), "float32") == []

    def test_missing_statistics_is_an_error_not_a_pass(self):
        # The decision recorded for this ticket: a column the gate cannot prove
        # safe is not a column it lets through.
        errs = numeric_precision_errors(
            {"mystery": ColumnPrecision(None, 1.0)}, "float32")
        assert len(errs) == 1
        assert "mystery" in errs[0]
        assert "statistic" in errs[0].lower()
        # The escape hatch has to be in the message, or the run is unrecoverable
        # for a data-format reason that has nothing to do with correctness.
        assert "numeric_precision_policy" in errs[0]

    def test_float64_has_its_own_wider_limit(self):
        assert numeric_precision_errors(self._ints(a=float(2 ** 53)), "float64") == []
        assert len(numeric_precision_errors(
            self._ints(a=2.0 ** 53 * 4), "float64")) == 1

    def test_multiple_offenders_are_collected_not_first_one_wins(self):
        errs = numeric_precision_errors({
            "c": ColumnPrecision(2.0 ** 40, 1.0),
            "a": ColumnPrecision(2.0 ** 30, 1.0),
            "b": ColumnPrecision(None, 1.0),
        }, "float32")
        assert len(errs) == 3
        # Sorted by column so two runs of the same config read the same way.
        assert errs[0].index("'a'") > 0 and "'b'" in errs[1] and "'c'" in errs[2]

    def test_an_undeclarable_storage_type_raises_rather_than_passing(self):
        with pytest.raises(ConfigConsistencyError):
            numeric_precision_errors(self._ints(a=1.0), "float16")


class TestNumericPrecisionRows:
    """The report shape: every checked column, not only the failing ones."""

    def _rows(self, by_column, storage="float32"):
        return {r["column"]: r for r in numeric_precision_rows(by_column, storage)}

    def test_a_passing_column_still_gets_a_row(self):
        # The difference between this and the errors predicate, and the reason
        # both exist: a gate reports failures, a report reports state.
        rows = self._rows({"cust_age": ColumnPrecision(97.0, 1.0)})
        assert rows["cust_age"]["verdict"] == "ok"
        assert rows["cust_age"]["limit"] == float(2 ** 24)

    def test_headroom_says_how_much_larger_the_column_may_get(self):
        rows = self._rows({"fee": ColumnPrecision(65536.0, 0.01)})
        # limit 131,072 over max 65,536 -> exactly 2x room left.
        assert rows["fee"]["headroom"] == pytest.approx(2.0)
        assert rows["fee"]["verdict"] == "ok"

    def test_a_breach_is_marked_and_keeps_a_sub_one_headroom(self):
        rows = self._rows({"bal": ColumnPrecision(262144.0, 0.01)})
        assert rows["bal"]["verdict"] == "breach"
        assert rows["bal"]["headroom"] == pytest.approx(0.5)

    def test_unmeasured_and_empty_columns_have_no_headroom(self):
        rows = self._rows({
            "mystery": ColumnPrecision(None, 1.0),
            "empty": ColumnPrecision(0.0, 1.0),
        })
        assert rows["mystery"]["verdict"] == "unmeasured"
        assert rows["mystery"]["headroom"] is None
        # An empty column is safe, but "infinite room" is not a number.
        assert rows["empty"]["verdict"] == "ok"
        assert rows["empty"]["headroom"] is None

    def test_sorted_closest_to_breaching_first(self):
        # The ordering is the report's whole usefulness: skimmed or truncated,
        # the column about to stop the pipeline is the one you read.
        ordered = [r["column"] for r in numeric_precision_rows({
            "roomy": ColumnPrecision(97.0, 1.0),
            "tight": ColumnPrecision(130900.0, 0.01),
            "broken": ColumnPrecision(262144.0, 0.01),
            "unknown": ColumnPrecision(None, 1.0),
        }, "float32")]
        assert ordered[:3] == ["broken", "tight", "roomy"]
        # No-headroom rows sort last rather than mixing into the ranking.
        assert ordered[3] == "unknown"

    def test_a_row_exists_for_every_column_handed_in(self):
        by_column = {c: ColumnPrecision(1.0, 1.0) for c in ("a", "b", "c")}
        assert len(numeric_precision_rows(by_column, "float32")) == 3

    def test_verdicts_agree_with_the_errors_predicate(self):
        # Two readings of the same numbers must not drift: every column the
        # gate rejects is a column the report marks, and no other.
        by_column = {
            "ok": ColumnPrecision(1000.0, 1.0),
            "bad": ColumnPrecision(2.0 ** 40, 1.0),
            "none": ColumnPrecision(None, 1.0),
        }
        flagged = {
            r["column"] for r in numeric_precision_rows(by_column, "float32")
            if r["verdict"] != "ok"
        }
        errors = numeric_precision_errors(by_column, "float32")
        assert flagged == {"bad", "none"}
        assert len(errors) == len(flagged)


from recsys_tfb.core.consistency import (
    DATASET_SOURCE_TABLES,
    PRIMARY_KEY_CHECK,
    dataset_source_quality_check_errors,
)


class TestDatasetSourceQualityChecksA32:
    """A32 — the three tables the dataset pipeline reads must keep the key
    that switches source_etl's primary-key check on (issue #289)."""

    @staticmethod
    def _table(name, **over):
        t = {
            "name": name,
            "sql_file": f"{name}/{name}.sql",
            "partition_by": {"snap_date": "DATE"},
            "primary_key": ["snap_date", "cust_id"],
            "quality_checks": {PRIMARY_KEY_CHECK: 0.0},
        }
        t.update(over)
        return t

    def _params(self, **table_over):
        """conf/base's shape: three stages, the guarded table last in each."""
        return {
            "sample_pool_etl": {"tables": [
                self._table("sample_pool", **table_over.get("sample_pool", {})),
            ]},
            "label_etl": {"tables": [
                self._table("label_ccard"),
                self._table("label_table", **table_over.get("label_table", {})),
            ]},
            "feature_etl": {"tables": [
                # The two named here stand for the five that declare a
                # primary_key and no quality_checks in conf/base.
                self._table("feature_aum", quality_checks={}),
                self._table("feature_concat", quality_checks={}),
                self._table("feature_table", **table_over.get("feature_table", {})),
            ]},
        }

    def test_the_three_guarded_tables_are_the_dataset_pipeline_inputs(self):
        assert set(DATASET_SOURCE_TABLES) == {
            "sample_pool", "label_table", "feature_table"
        }

    def test_compliant_config_is_clean(self):
        assert dataset_source_quality_check_errors(self._params()) == []

    def test_missing_key_names_the_table_and_the_key_to_add(self):
        errs = dataset_source_quality_check_errors(
            self._params(sample_pool={"quality_checks": {}}))
        assert len(errs) == 1
        assert "A32" in errs[0]
        assert "sample_pool" in errs[0]
        assert PRIMARY_KEY_CHECK in errs[0]
        # The stage tells the operator which file to open.
        assert "sample_pool_etl" in errs[0]

    def test_absent_quality_checks_block_is_the_same_failure(self):
        errs = dataset_source_quality_check_errors(
            self._params(feature_table={"quality_checks": None}))
        assert len(errs) == 1
        assert "feature_table" in errs[0]

    def test_several_missing_are_collected_not_raised_one_at_a_time(self):
        errs = dataset_source_quality_check_errors(self._params(
            sample_pool={"quality_checks": {}},
            label_table={"quality_checks": {}},
            feature_table={"quality_checks": {}},
        ))
        assert len(errs) == 3
        assert {"sample_pool", "label_table", "feature_table"} == {
            t for t in DATASET_SOURCE_TABLES
            if any(t in e for e in errs)
        }

    def test_the_unguarded_feature_tables_are_left_alone(self):
        # feature_aum/sav/ccard/info/concat declare a primary_key and no
        # quality_checks on purpose (ADR-0006: check the terminal table only).
        # A32 must not drag them in — that would buy five extra scans.
        assert dataset_source_quality_check_errors(self._params()) == []
        # ...and that clean result is not the gate skipping the stage: break
        # the guarded table sitting in the same list and it fires.
        errs = dataset_source_quality_check_errors(
            self._params(feature_table={"quality_checks": {}}))
        assert len(errs) == 1
        assert "feature_table" in errs[0]

    def test_the_table_is_found_by_name_not_by_position(self):
        params = self._params()
        params["feature_etl"]["tables"].reverse()   # guarded table now first
        assert dataset_source_quality_check_errors(params) == []
        params["feature_etl"]["tables"][0]["quality_checks"] = {}
        assert len(dataset_source_quality_check_errors(params)) == 1

    def test_explicit_yaml_null_threshold_is_rejected(self):
        # `max_duplicate_key_ratio:` with no value passes source_etl's `in`
        # test and then blows up comparing a float to None, after the scan.
        errs = dataset_source_quality_check_errors(
            self._params(label_table={"quality_checks": {PRIMARY_KEY_CHECK: None}}))
        assert len(errs) == 1
        assert "label_table" in errs[0]

    def test_a_threshold_that_can_never_fail_is_rejected(self):
        # The measured ratio is always < 1, so >= 1 leaves the check declared
        # and permanently off — A32's whole point.
        for value in (1, 1.0, 2):
            errs = dataset_source_quality_check_errors(
                self._params(sample_pool={"quality_checks": {PRIMARY_KEY_CHECK: value}}))
            assert len(errs) == 1, value
            assert "sample_pool" in errs[0]

    def test_a_threshold_that_can_never_pass_is_rejected(self):
        errs = dataset_source_quality_check_errors(
            self._params(sample_pool={"quality_checks": {PRIMARY_KEY_CHECK: -0.1}}))
        assert len(errs) == 1

    def test_a_tolerant_but_usable_threshold_is_accepted(self):
        assert dataset_source_quality_check_errors(
            self._params(sample_pool={"quality_checks": {PRIMARY_KEY_CHECK: 0.5}})) == []

    def test_a_table_no_etl_stage_declares_is_not_this_gate(self):
        # A32 reads a declaration's content. A source table nothing produces
        # fails loudly at the Hive read instead, and every minimal test config
        # in this repo omits the ETL stages entirely.
        assert dataset_source_quality_check_errors({}) == []
        assert dataset_source_quality_check_errors(
            {"sample_pool_etl": {"tables": []}}) == []

    def test_needs_no_spark_only_parameters(self):
        import inspect
        sig = inspect.signature(dataset_source_quality_check_errors)
        assert list(sig.parameters) == ["parameters"]

    def test_wired_into_validate_config_consistency(self):
        params = self._params(sample_pool={"quality_checks": {}})
        params["schema"] = {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}}
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(params)
        assert "A32" in str(exc.value)

    def test_the_real_conf_passes_today(self):
        # A32 is a no-op against conf/ as it stands: it exists to fire the day
        # somebody deletes one of the three keys, not to demand a config edit.
        from recsys_tfb.core.config import ConfigLoader

        parameters = ConfigLoader("conf", env="local").get_parameters()
        assert dataset_source_quality_check_errors(parameters) == []
        # ...and it is actually looking at something: all three are declared.
        for name in DATASET_SOURCE_TABLES:
            assert any(
                t.get("name") == name
                for stage, block in parameters.items()
                if stage.endswith("_etl")
                for t in (block.get("tables") or [])
            ), name


# --- A34: evaluation.report.sections declares exactly what the report reads --

from recsys_tfb.core import consistency as _consistency_module
from recsys_tfb.core.consistency import (
    EVALUATION_REPORT_SECTIONS,
    report_section_key_errors,
)


def _sections_params(sections):
    return {"evaluation": {"report": {"sections": sections}}}


class TestReportSectionKeysA34:
    """A34 — ``evaluation.report.sections`` declares exactly the switches the
    report reads (``EVALUATION_REPORT_SECTIONS``), checked in both directions.

    One direction alone lets a drift through: "declared ⊆ read" passed
    ``diagnosis_links`` (read, never declared, so impossible to switch off),
    and "read ⊆ declared" passed the four switches nothing read.
    """

    def test_the_shipped_conf_declares_exactly_the_read_sections(self):
        from pathlib import Path

        from recsys_tfb.core.config import ConfigLoader

        conf = Path(__file__).resolve().parents[2] / "conf"
        params = ConfigLoader(str(conf), env="local").get_parameters()
        assert report_section_key_errors(params) == []

    def test_a_declared_switch_nothing_reads_is_reported(self):
        sections = {name: True for name in EVALUATION_REPORT_SECTIONS}
        sections["per_segment"] = True
        errors = report_section_key_errors(_sections_params(sections))
        assert len(errors) == 1
        assert "'per_segment'" in errors[0]

    def test_a_read_switch_left_undeclared_is_reported(self, monkeypatch):
        """The shape of adding a ``_section_on("new_section")`` call and its
        name to the constant without declaring it in the YAML."""
        declared = {name: True for name in EVALUATION_REPORT_SECTIONS}
        monkeypatch.setattr(
            _consistency_module, "EVALUATION_REPORT_SECTIONS",
            EVALUATION_REPORT_SECTIONS | {"new_section"},
        )
        errors = report_section_key_errors(_sections_params(declared))
        assert len(errors) == 1
        assert "'new_section'" in errors[0]

    def test_a_block_declaring_no_switch_is_not_checked(self):
        assert report_section_key_errors({}) == []
        assert report_section_key_errors({"evaluation": {"report": {}}}) == []
        assert report_section_key_errors(_sections_params(None)) == []
        assert report_section_key_errors(_sections_params({})) == []

    def test_a_block_declaring_some_switches_is_checked_in_full(self):
        errors = report_section_key_errors(
            _sections_params({"baseline": True}))
        assert len(errors) == 1
        assert "does not declare" in errors[0]
        assert "'diagnosis_links'" in errors[0]

    def test_a_non_string_key_is_reported_not_crashed_on(self):
        """YAML loads an unquoted ``on:`` key as ``True``. Sorting it against
        the str keys must not raise TypeError instead of reporting it."""
        sections = {name: True for name in EVALUATION_REPORT_SECTIONS}
        sections[True] = True
        sections["per_segment"] = True
        errors = report_section_key_errors(_sections_params(sections))
        assert len(errors) == 1
        assert "True" in errors[0] and "'per_segment'" in errors[0]

    def test_a_block_that_is_not_a_mapping_is_reported(self):
        errors = report_section_key_errors(_sections_params(["baseline"]))
        assert len(errors) == 1
        assert errors[0].startswith("A34: evaluation.report.sections")

    def test_not_aggregated_by_validate_config_consistency(self):
        """Only evaluation reads these keys (A24's reason, issue #158): a conf
        still carrying a dead switch must not stop dataset, training or
        inference, which all pass through that gate. The evaluation command
        raises it instead (``test_consistency_cli_wiring.py``)."""
        p = _base({"inference": {"products": ["a", "b"]}})
        sections = {name: True for name in EVALUATION_REPORT_SECTIONS}
        sections["guardrail_recall"] = True
        p["evaluation"] = {"report": {"sections": sections}}
        assert report_section_key_errors(p), "premise: this conf is bad"
        validate_config_consistency(p)

    def test_the_ad_example_conf_declares_exactly_the_read_sections(self):
        """The ad example is a conf of its own, not an overlay on
        ``conf/base``, and its only other gate is ``run_e2e.sh`` — minutes of
        Spark before A34 would say a section is missing."""
        from pathlib import Path

        from recsys_tfb.core.config import ConfigLoader

        conf = Path(__file__).resolve().parents[2] / "examples" / "ad" / "conf"
        params = ConfigLoader(str(conf), env="local").get_parameters()
        assert report_section_key_errors(params) == []


# --- A42: evaluation.prediction_quality parameter domains --------------------

from recsys_tfb.core.consistency import (  # noqa: E402
    PREDICTION_QUALITY_DEFAULTS,
    prediction_quality_param_errors,
)


def _pq_params(**block):
    return {"evaluation": {"prediction_quality": block}}


class TestPredictionQualityParamsA42:
    def test_absent_block_uses_defaults(self):
        assert prediction_quality_param_errors({}) == []
        assert prediction_quality_param_errors(
            {"evaluation": {"prediction_quality": None}}) == []

    def test_the_defaults_pass_their_own_check(self):
        assert prediction_quality_param_errors(
            _pq_params(**PREDICTION_QUALITY_DEFAULTS)) == []

    @pytest.mark.parametrize("key,value", [
        ("n_bins", 0), ("n_bins", 2.5), ("n_bins", True), ("n_bins", "1000"),
        ("n_display_bins", 0), ("n_display_bins", False),
        ("top_n", -1), ("top_n", True), ("top_n", 1.0),
    ])
    def test_a_value_out_of_domain_names_its_key(self, key, value):
        errors = prediction_quality_param_errors(
            _pq_params(**{**PREDICTION_QUALITY_DEFAULTS, key: value}))
        assert len(errors) == 1
        assert f"evaluation.prediction_quality.{key}=" in errors[0]

    def test_top_n_zero_is_allowed(self):
        # No item listed: the overall numbers alone, a benign degenerate.
        assert prediction_quality_param_errors(
            _pq_params(**{**PREDICTION_QUALITY_DEFAULTS, "top_n": 0})) == []

    def test_display_bins_must_divide_the_fine_bins(self):
        errors = prediction_quality_param_errors(
            _pq_params(n_bins=1000, n_display_bins=3))
        assert len(errors) == 1
        assert "divide" in errors[0]

    def test_display_bins_divide_the_default_fine_bins(self):
        # Only n_display_bins declared: checked against the default n_bins.
        assert prediction_quality_param_errors(
            _pq_params(n_display_bins=20)) == []
        errors = prediction_quality_param_errors(_pq_params(n_display_bins=7))
        assert len(errors) == 1 and "divide" in errors[0]

    def test_an_undeclared_key_is_reported(self):
        errors = prediction_quality_param_errors(
            _pq_params(**PREDICTION_QUALITY_DEFAULTS, enabled=True))
        assert len(errors) == 1
        assert "'enabled'" in errors[0]
        # The switch lives with the other sections, and the message says so.
        assert "report.sections.prediction_quality" in errors[0]

    def test_not_aggregated_by_validate_config_consistency(self):
        """Evaluation-only keys, A34's reason (issue #158)."""
        p = _base({"inference": {"products": ["a", "b"]}})
        p["evaluation"] = {"prediction_quality": {"n_bins": 0}}
        assert prediction_quality_param_errors(p), "premise: this conf is bad"
        validate_config_consistency(p)


# --- A43: the calibration-bin keys #381 retired ------------------------------

from recsys_tfb.core.consistency import (  # noqa: E402
    RETIRED_CALIBRATION_BIN_KEYS,
    retired_calibration_bin_key_errors,
)


class TestRetiredCalibrationBinKeysA43:
    @pytest.mark.parametrize("key", RETIRED_CALIBRATION_BIN_KEYS)
    @pytest.mark.parametrize("value", [True, False, 10, None])
    def test_the_key_is_refused_whatever_its_value(self, key, value):
        """Presence is the failure (A37's shape): ``false`` switches off
        nothing that still exists, and the key sits in a fingerprinted
        subtree, so leaving it behind is not the same conf as deleting it."""
        leaf = key.rsplit(".", 1)[1]
        p = {"evaluation": {"report": {"diagnostics": {
            "include_distributions": True, leaf: value}}}}
        errors = retired_calibration_bin_key_errors(p)
        assert len(errors) == 1
        assert errors[0].startswith("A43:")
        assert repr(key) in errors[0]
        # The message points at what replaced it.
        assert "prediction_quality" in errors[0]

    def test_both_keys_in_one_message(self):
        p = {"evaluation": {"report": {"diagnostics": {
            "include_calibration": True, "n_calibration_bins": 10}}}}
        errors = retired_calibration_bin_key_errors(p)
        assert len(errors) == 1
        assert all(repr(k) in errors[0] for k in RETIRED_CALIBRATION_BIN_KEYS)

    def test_a_conf_without_them_passes(self):
        assert retired_calibration_bin_key_errors({}) == []
        assert retired_calibration_bin_key_errors({"evaluation": {"report": {
            "diagnostics": {"include_distributions": True}}}}) == []

    def test_the_shipped_confs_do_not_spell_them(self):
        from pathlib import Path

        from recsys_tfb.core.config import ConfigLoader

        root = Path(__file__).resolve().parents[2]
        for conf in (root / "conf", root / "examples" / "ad" / "conf"):
            params = ConfigLoader(str(conf), env="local").get_parameters()
            assert retired_calibration_bin_key_errors(params) == [], conf

    def test_not_aggregated_by_validate_config_consistency(self):
        """Evaluation-only keys, A34's reason (issue #158)."""
        p = _base({"inference": {"products": ["a", "b"]}})
        p["evaluation"] = {"report": {"diagnostics": {
            "include_calibration": True}}}
        assert retired_calibration_bin_key_errors(p), "premise: this conf is bad"
        validate_config_consistency(p)


# --- A33: migration-period check for the #327 config key rename --------------

from recsys_tfb.core.consistency import (
    LEGACY_EVALUATION_KEYS,
    legacy_evaluation_key_errors,
)


class TestLegacyEvaluationKeysA33:
    """A33 — a conf still spelling ``evaluation.product_categories``.

    ⚠ **This whole class leaves with the check.** A33 is a migration tool, not
    an invariant: once the company environment's conf is confirmed updated, the
    predicate, its wiring and these tests are deleted together. See A33 in
    ``core/consistency.py``'s module docstring for the retirement condition.
    """

    def test_the_old_spelling_is_reported_with_the_new_name(self):
        errors = legacy_evaluation_key_errors(
            {"evaluation": {"product_categories": {"enabled": True}}}
        )
        assert len(errors) == 1
        assert "item_categories" in errors[0]

    def test_the_new_spelling_is_clean(self):
        assert legacy_evaluation_key_errors(
            {"evaluation": {"item_categories": {"enabled": True}}}
        ) == []

    def test_a_config_without_the_key_at_all_is_clean(self):
        assert legacy_evaluation_key_errors({}) == []
        assert legacy_evaluation_key_errors({"evaluation": {}}) == []

    def test_presence_is_what_counts_not_the_value(self):
        """A conf that spells the old name has not been migrated, whatever is
        under it — including a disabled block or an explicit null."""
        for value in ({"enabled": False}, None, {}):
            assert legacy_evaluation_key_errors(
                {"evaluation": {"product_categories": value}}
            ), value

    def test_wired_into_validate_config_consistency(self):
        params = {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            }},
            "evaluation": {"product_categories": {"enabled": True}},
        }
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(params)
        assert "A33" in str(exc.value)

    def test_the_real_conf_uses_the_new_spelling(self):
        from recsys_tfb.core.config import ConfigLoader

        parameters = ConfigLoader("conf", env="local").get_parameters()
        assert legacy_evaluation_key_errors(parameters) == []
        # ...and the block it guards is actually there, so this is not passing
        # by looking at nothing.
        assert "item_categories" in parameters["evaluation"]

    def test_the_rename_table_covers_exactly_the_one_key(self):
        """Scope pin: #327 renamed one config key, not a family of them.

        The time vocabulary is kept on purpose (ADR-0017); an entry spelled
        ``snap_date`` appearing here would mean somebody read the rename wider
        than it is.
        """
        assert LEGACY_EVALUATION_KEYS == {"product_categories": "item_categories"}


# --- B10: model_input row count must equal its keys table's ------------------

from recsys_tfb.core.consistency import SplitRowCounts, model_input_grain_errors


class TestModelInputGrainErrorsB10:
    """The rule itself: keys rows in, model_input rows out, one per split.

    Pure — the caller gathers the two numbers. Handing it a dict is what lets
    "what the rule is" be tested apart from "how a row count is obtained
    without scanning", which is ``read_row_count``'s problem and has its own
    tests in ``test_utils/test_parquet_stats.py``.
    """

    def test_equal_counts_pass(self):
        assert model_input_grain_errors(
            {"train": SplitRowCounts(1000, 1000)}) == []

    def test_both_empty_passes(self):
        # train_dev_ratio: 0 is a supported setting (split_train_keys only
        # guards a *non-zero* ratio that produced nothing), so an empty split
        # on both sides is a real state, not a gap in the check.
        assert model_input_grain_errors(
            {"train_dev": SplitRowCounts(0, 0)}) == []

    def test_more_model_input_rows_than_keys_is_the_fan_out_it_exists_for(self):
        errors = model_input_grain_errors({"train": SplitRowCounts(1000, 2000)})
        assert len(errors) == 1
        assert "B10" in errors[0]
        assert "train" in errors[0]
        assert "1,000" in errors[0] and "2,000" in errors[0]
        # The multiplier is what names the cause: 2x says "one duplicate key in
        # a right table", not "a few stray rows".
        assert "2.0000x" in errors[0]

    def test_fewer_model_input_rows_than_keys_is_also_an_error(self):
        # The joins are LEFT joins, so rows can only be gained, never lost.
        # Fewer means something other than the fan-out this gate models —
        # reporting it as a pass would hide it.
        errors = model_input_grain_errors({"train": SplitRowCounts(1000, 999)})
        assert len(errors) == 1
        assert "B10" in errors[0]

    def test_keys_empty_but_model_input_not_is_an_error(self):
        # Division by zero must not be how this reports; a ratio has no meaning
        # here and the message has to stand without one.
        errors = model_input_grain_errors({"train": SplitRowCounts(0, 5)})
        assert len(errors) == 1
        assert "B10" in errors[0]

    def test_collects_every_split_sorted(self):
        errors = model_input_grain_errors({
            "train": SplitRowCounts(10, 20),
            "train_dev": SplitRowCounts(5, 15),
            "val": SplitRowCounts(5, 5),
        })
        assert len(errors) == 2
        assert "train_model_input" in errors[0]
        assert "train_dev_model_input" in errors[1]

    def test_needs_no_spark(self):
        """Counts and column names only — no frame, no handle, no path. What
        this pins is that obtaining the two row counts stays the caller's
        problem (``utils.parquet_stats``), which is what lets the rule be
        tested by handing it a dict."""
        import inspect
        sig = inspect.signature(model_input_grain_errors)
        assert list(sig.parameters) == ["by_split", "identity_columns"]
        assert sig.parameters["identity_columns"].default is None

    def test_the_message_names_the_identity_columns_when_given_them(self):
        """"One key" is what the reader has to know to go looking for the
        duplicate, and which columns make up a key moves with the declared
        roles (ADR-0025). The rule itself did not change: two impressions of
        one item differ in `event`, so they were never one key."""
        errors = model_input_grain_errors(
            {"train": SplitRowCounts(10, 20)},
            ["snap_date", "cust_id", "prod_name", "imp_id"],
        )
        assert "imp_id" in errors[0]
        assert "['snap_date', 'cust_id', 'prod_name', 'imp_id']" in errors[0]

    def test_the_message_stands_without_the_identity_columns(self):
        errors = model_input_grain_errors({"train": SplitRowCounts(10, 20)})
        assert "One key here is" not in errors[0]
        assert "build_model_input LEFT" in errors[0]

    def test_a_small_fan_out_still_shows_a_ratio_that_is_not_one(self):
        # ``:.4g`` would render this as "1x" — agreement, in the message of an
        # error about disagreement.
        errors = model_input_grain_errors(
            {"train": SplitRowCounts(1_000_000, 1_000_200)})
        assert "1.0002x" in errors[0]


class TestA35EtlCliVars:
    """A35 — the four source ETL commands' repeatable ``--var key=value``
    flag must be well-formed, declared, and safe to merge onto the stage's
    YAML ``variables`` (#370). One case per checked failure mode (a-j), plus
    the all-clear.
    """

    def _errors(self, variables=None, raw_vars=None):
        from recsys_tfb.core.consistency import etl_cli_var_errors
        return etl_cli_var_errors(variables, raw_vars)

    def test_all_clear_returns_empty(self):
        assert self._errors(
            {"target_db": "ml_feature", "raw_db": "ml_raw"},
            ["raw_db=ml_raw_override"],
        ) == []

    def test_no_vars_and_no_variables_is_clean(self):
        assert self._errors(None, None) == []
        assert self._errors({}, []) == []

    # (a) --var item without '='
    def test_a_missing_equals_sign(self):
        errors = self._errors({"raw_db": "x"}, ["raw_db"])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "'='" in errors[0]

    # (b) --var name not declared in YAML variables
    def test_b_undeclared_name(self):
        errors = self._errors({"raw_db": "x"}, ["typo_db=y"])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "typo_db" in errors[0]
        assert "not declared" in errors[0]

    # (c) --var target_date=...
    def test_c_var_target_date_rejected(self):
        errors = self._errors({"raw_db": "x"}, ["target_date=2025-01-31"])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_date" in errors[0]
        assert "--target-dates" in errors[0]

    # (d) --var target_db=...
    def test_d_var_target_db_rejected(self):
        errors = self._errors({"target_db": "ml_feature"}, ["target_db=other_db"])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_db" in errors[0]
        assert "YAML" in errors[0]

    # (e) same name passed via --var twice
    def test_e_duplicate_var_name(self):
        errors = self._errors(
            {"raw_db": "x"}, ["raw_db=a", "raw_db=b"]
        )
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "raw_db" in errors[0]
        assert "more than once" in errors[0] or "2 times" in errors[0]

    # (f) YAML null with no --var override
    def test_f_null_without_override_is_blocked(self):
        errors = self._errors({"raw_db": None}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "raw_db" in errors[0]
        assert "null" in errors[0] or "~" in errors[0]

    def test_f_null_with_override_passes(self):
        errors = self._errors({"raw_db": None}, ["raw_db=ml_raw"])
        assert errors == []

    def test_f_checked_regardless_of_any_restart_from_notion(self):
        # A35 has no restart_from parameter at all: the check must not vary
        # with which tables a run would touch this time.
        import inspect
        assert "restart_from" not in inspect.signature(
            __import__(
                "recsys_tfb.core.consistency", fromlist=["etl_cli_var_errors"]
            ).etl_cli_var_errors
        ).parameters

    # (g) YAML value neither string nor null
    def test_g_non_string_yaml_value_rejected(self):
        errors = self._errors({"raw_db": 2025}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "raw_db" in errors[0]
        assert "2025" in errors[0]

    def test_g_boolean_and_list_values_also_rejected(self):
        errors = self._errors({"flag": True, "items": [1, 2]}, [])
        assert len(errors) == 2

    # (h) YAML declares target_date
    def test_h_yaml_declares_target_date_blocked(self):
        errors = self._errors({"target_date": "2025-01-31"}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_date" in errors[0]

    # #370: (h) owns target_date unconditionally — a null or a
    # non-string value must NOT also trigger (f)/(g), which would hand out
    # mutually contradictory instructions for the same key (fix variables.target_date
    # per (h) by removing it, but (f)/(g) tell the reader to supply or quote it).
    def test_h_yaml_target_date_null_is_still_exactly_one_error(self):
        errors = self._errors({"target_date": None}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_date" in errors[0]

    def test_h_yaml_target_date_non_string_is_still_exactly_one_error(self):
        errors = self._errors({"target_date": 2025}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_date" in errors[0]

    # (i) YAML target_db: ~
    def test_i_yaml_target_db_null_blocked(self):
        errors = self._errors({"target_db": None}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_db" in errors[0]

    # (j) variables not a mapping
    def test_j_variables_not_a_mapping(self):
        errors = self._errors(["not", "a", "mapping"], [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "mapping" in errors[0]

    # (k) a variable's final value contains '${' — (#370).
    def test_k_yaml_value_containing_dollar_brace_is_blocked(self):
        errors = self._errors({"raw_db": "${env.OTHER}"}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "raw_db" in errors[0] and "${" in errors[0]

    def test_k_var_override_containing_dollar_brace_is_blocked(self):
        # The override, not the (clean) YAML default, is the final value.
        errors = self._errors({"raw_db": "clean"}, ["raw_db=${env.OTHER}"])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "raw_db" in errors[0]

    def test_k_clean_override_of_a_dollar_brace_yaml_default_passes(self):
        # The YAML default contains '${' but is fully overridden — the FINAL
        # value (from --var) is clean, so (k) must not fire.
        errors = self._errors({"raw_db": "${env.OTHER}"}, ["raw_db=clean_value"])
        assert errors == []

    def test_k_does_not_pile_onto_target_date_or_target_db(self):
        # target_date is (h)'s alone; target_db's null is (i)'s alone. (k)
        # must not add a second, redundant error for either.
        errors = self._errors({"target_date": "${x}", "target_db": None}, [])
        assert len(errors) == 2
        assert not any("final value" in e for e in errors)

    def test_k_target_db_yaml_value_is_still_checked(self):
        # target_db cannot be overridden via --var (that's (d)'s job), but a
        # bad YAML value for it is still (k)'s to catch.
        errors = self._errors({"target_db": "${env.X}"}, [])
        assert len(errors) == 1
        assert "(A35)" in errors[0] and "target_db" in errors[0]

    # (i) a value referencing ONLY ${target_date} passes — #370 review
    # round 2's K1: this works on main and is unaffected by declaration
    # order (_table_variables always substitutes target_date last, and (h)
    # forbids declaring it in the YAML at all).
    def test_k_i_target_date_only_reference_is_allowed(self):
        errors = self._errors(
            {"win_start": "add_months('${target_date}', -12)"}, []
        )
        assert errors == []

    def test_k_i_target_date_referenced_more_than_once_is_still_allowed(self):
        errors = self._errors(
            {"win_start": "${target_date} to ${target_date}"}, []
        )
        assert errors == []

    # (ii) a value mixing ${target_date} with another reference is still
    # rejected, and the message names only the OTHER reference.
    def test_k_ii_target_date_plus_another_reference_is_blocked(self):
        errors = self._errors(
            {"win_start": "${target_date}_${raw_db}", "raw_db": "x"}, []
        )
        assert len(errors) == 1
        # The rejected-tokens list names only the other reference — the
        # explanatory prose is free to mention target_date (it explains the
        # exemption), so this checks the enumerated list itself, not the
        # whole message.
        assert "references ['${raw_db}']" in errors[0]

    # (iii) the message never includes the rest of the value (it may hold a
    # secret pulled in via a YAML ${env.X}) — only the variable name and the
    # rejected ${...} fragment(s).
    def test_k_iii_message_does_not_leak_the_rest_of_the_value(self):
        errors = self._errors(
            {"pw": "SECRET_PART_abc${bad_ref}_SECRET_PART_xyz"}, []
        )
        assert len(errors) == 1
        assert "${bad_ref}" in errors[0]
        assert "SECRET_PART" not in errors[0]

    # #370: a name repeated via --var must not print the same
    # (b)/(c)/(d) message once per repetition.
    def test_b_undeclared_name_repeated_prints_once(self):
        errors = self._errors({"raw_db": "x"}, ["typo=1", "typo=2"])
        not_declared = [e for e in errors if "not declared" in e]
        assert len(not_declared) == 1
        passed_n_times = [e for e in errors if "times" in e]
        assert len(passed_n_times) == 1 and "2 times" in passed_n_times[0]

    def test_c_var_target_date_repeated_prints_once(self):
        errors = self._errors(
            {"raw_db": "x"}, ["target_date=2025-01-31", "target_date=2025-02-28"]
        )
        target_date_rejections = [e for e in errors if "not allowed" in e]
        assert len(target_date_rejections) == 1

    # #370: a non-string variable NAME must not crash sorted().
    def test_non_string_variable_name_does_not_crash_sorted(self):
        errors = self._errors({2025: "x", "raw_db": "y"}, ["typo=1"])
        assert any("(A35)" in e for e in errors)  # no TypeError raised

    def test_multiple_errors_collected_in_one_pass(self):
        errors = self._errors(
            {"raw_db": "x"}, ["raw_db", "typo=1", "target_date=2025-01-31"]
        )
        assert len(errors) >= 3
        assert all("(A35)" in e for e in errors)


class TestParseEtlVarFlags:
    """The shared ``KEY=VALUE`` split used by A35's predicate and the
    override merge (#370) — one implementation, tested once.
    """

    def test_splits_on_first_equals_only(self):
        from recsys_tfb.core.consistency import parse_etl_var_flags
        parsed, errors = parse_etl_var_flags(["a=b=c"])
        assert parsed == [("a", "b=c")]
        assert errors == []

    def test_empty_value_is_legal(self):
        from recsys_tfb.core.consistency import parse_etl_var_flags
        parsed, errors = parse_etl_var_flags(["a="])
        assert parsed == [("a", "")]
        assert errors == []

    def test_missing_equals_is_a_parse_error_not_a_pair(self):
        from recsys_tfb.core.consistency import parse_etl_var_flags
        parsed, errors = parse_etl_var_flags(["no_equals_here"])
        assert parsed == []
        assert len(errors) == 1 and "(A35)" in errors[0]

    def test_none_input_is_empty(self):
        from recsys_tfb.core.consistency import parse_etl_var_flags
        assert parse_etl_var_flags(None) == ([], [])


class TestMergedEtlVariables:
    """CLI overrides win; the YAML dict handed in is never mutated (#370)."""

    def test_cli_overrides_yaml(self):
        from recsys_tfb.core.consistency import merged_etl_variables
        merged = merged_etl_variables({"raw_db": "ml_raw"}, ["raw_db=override"])
        assert merged == {"raw_db": "override"}

    def test_yaml_only_keys_survive(self):
        from recsys_tfb.core.consistency import merged_etl_variables
        merged = merged_etl_variables(
            {"raw_db": "ml_raw", "target_db": "ml_feature"}, ["raw_db=override"]
        )
        assert merged == {"raw_db": "override", "target_db": "ml_feature"}

    def test_original_dict_not_mutated(self):
        from recsys_tfb.core.consistency import merged_etl_variables
        original = {"raw_db": "ml_raw"}
        merged_etl_variables(original, ["raw_db=override"])
        assert original == {"raw_db": "ml_raw"}

    def test_no_cli_vars_returns_equivalent_copy(self):
        from recsys_tfb.core.consistency import merged_etl_variables
        original = {"raw_db": "ml_raw"}
        merged = merged_etl_variables(original, None)
        assert merged == original
        assert merged is not original


# --- A37: config keys retired with the calibration removal (#411) ------------

from recsys_tfb.core.consistency import (
    RETIRED_CALIBRATION_KEYS,
    retired_calibration_key_errors,
    validate_config_consistency,
)


class TestRetiredCalibrationKeysA37:
    """A37 — a conf still spelling a calibration key after #411 removed it.

    Unlike A33 this is not a migration tool with a delete-by date: the keys name
    a mechanism that no longer exists anywhere in the framework, so the check
    stays for as long as someone might still be carrying an old conf.
    """

    def test_a_retired_training_key_is_reported_by_name(self):
        errors = retired_calibration_key_errors(
            {"training": {"calibration": {"enabled": True, "method": "isotonic"}}}
        )
        assert len(errors) == 1
        assert "training.calibration" in errors[0]

    def test_each_retired_dataset_key_is_reported_by_name(self):
        """The four keys #414 retired with the calibration data split.

        One per key, not one dict holding all four: a predicate that only
        looked at ``enable_calibration`` would pass the other three, and a
        single combined fixture could not tell the two apart.
        """
        for key, value in (
            ("enable_calibration", True),
            ("calibration_snap_dates", ["2026-01-31"]),
            ("calibration_sample_ratio", 1.0),
            ("calibration_sample_ratio_overrides", {"mass": 0.5}),
        ):
            errors = retired_calibration_key_errors({"dataset": {key: value}})
            assert len(errors) == 1, key
            assert f"dataset.{key}" in errors[0], key

    def test_a_retired_inference_key_is_reported_by_name(self):
        errors = retired_calibration_key_errors(
            {"inference": {"use_calibration": True}}
        )
        assert len(errors) == 1
        assert "inference.use_calibration" in errors[0]

    def test_presence_is_what_counts_not_the_value(self):
        """``false`` and an empty block are as retired as ``true``.

        The whole ``training:`` subtree is hashed into ``model_version``, so a
        conf that keeps a disabled block computes a different ID than one that
        deleted it — which is the disagreement this check exists to prevent.
        """
        for value in ({"enabled": False}, None, {}, False):
            assert retired_calibration_key_errors(
                {"training": {"calibration": value}}
            ), value
        for value in (False, None):
            assert retired_calibration_key_errors(
                {"inference": {"use_calibration": value}}
            ), value
        # The dataset side has its own falsy shapes: the switch written off,
        # and an empty date list. Both still move base_dataset_version.
        assert retired_calibration_key_errors(
            {"dataset": {"enable_calibration": False}}
        )
        assert retired_calibration_key_errors(
            {"dataset": {"calibration_snap_dates": []}}
        )

    def test_every_retired_key_is_listed_in_one_message(self):
        """One message, every key — the user deletes once instead of running,
        fixing one key, and running again."""
        errors = retired_calibration_key_errors({
            "dataset": {
                "enable_calibration": False,
                "calibration_snap_dates": [],
                "calibration_sample_ratio": 1.0,
                "calibration_sample_ratio_overrides": {},
            },
            "training": {"calibration": {"enabled": False}},
            "inference": {"use_calibration": False},
        })
        assert len(errors) == 1
        for key in RETIRED_CALIBRATION_KEYS:
            assert key in errors[0]

    def test_the_message_says_what_to_do_about_it(self):
        errors = retired_calibration_key_errors(
            {"training": {"calibration": {"enabled": True}}}
        )
        assert "#411" in errors[0]
        assert "Delete" in errors[0] or "delete" in errors[0]
        assert "no replacement" in errors[0]

    def test_a_config_without_the_keys_at_all_is_clean(self):
        assert retired_calibration_key_errors({}) == []
        assert retired_calibration_key_errors(
            {"dataset": {}, "training": {}, "inference": {}}
        ) == []
        assert retired_calibration_key_errors({
            "dataset": {"train_snap_dates": ["2026-01-31"], "sample_ratio": 1.0},
            "training": {"objective": "binary"},
            "inference": {"entity_buckets": 8},
        }) == []

    def test_a_non_mapping_section_is_not_a_crash(self):
        assert retired_calibration_key_errors({"training": None}) == []
        assert retired_calibration_key_errors({"inference": "nonsense"}) == []
        assert retired_calibration_key_errors({"dataset": ["not", "a", "map"]}) == []

    def test_wired_into_validate_config_consistency(self):
        params = {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            }},
            "training": {"calibration": {"enabled": False}},
        }
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(params)
        assert "A37" in str(exc.value)
        assert "training.calibration" in str(exc.value)

    def test_the_real_conf_has_no_retired_key_left(self):
        from recsys_tfb.core.config import ConfigLoader

        parameters = ConfigLoader("conf", env="local").get_parameters()
        assert retired_calibration_key_errors(parameters) == []

    def test_the_tuple_is_complete(self):
        """Six keys: two the calibrator read (#413) and four that configured the
        calibration data split (#414). Spelled out rather than counted, so
        dropping one and adding another cannot cancel out."""
        assert RETIRED_CALIBRATION_KEYS == (
            "dataset.enable_calibration",
            "dataset.calibration_snap_dates",
            "dataset.calibration_sample_ratio",
            "dataset.calibration_sample_ratio_overrides",
            "training.calibration",
            "inference.use_calibration",
        )


# =============================================================================
# A38 — an optional-role column must not be declared a model feature
# A39 — the prediction write target must declare every optional-role column
# =============================================================================

from recsys_tfb.core.consistency import (
    optional_role_as_feature_errors,
    optional_role_columns,
    optional_role_columns_declared_errors,
)


def _event_params(event=None, categorical=("prod_name",)):
    """The example roles, optionally declaring ``event``.

    ``categorical_columns`` carries the item because A2 requires it to; a
    fixture without it would make every A38 test below also true of a config
    that A2 already rejects.
    """
    columns = {"time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}
    if event is not None:
        columns["event"] = event
    return {
        "schema": {"columns": columns},
        "dataset": {
            "prepare_model_input": {"categorical_columns": list(categorical)}
        },
    }


class TestOptionalRoleColumns:
    """The shared answer to "which columns did an optional role add"."""

    def test_none_declared_is_empty(self):
        assert optional_role_columns(_event_params()) == []

    def test_a_declared_string_role_yields_one_column(self):
        assert optional_role_columns(_event_params(event="impression_id")) == [
            "impression_id",
        ]

    def test_a_declared_list_role_keeps_declaration_order(self):
        assert optional_role_columns(
            _event_params(event=["event_ts", "impression_id"])
        ) == ["event_ts", "impression_id"]


class TestOptionalRoleAsFeatureA38:
    def test_no_optional_role_declared_passes(self):
        assert optional_role_as_feature_errors(_event_params()) == []

    def test_declared_but_not_listed_as_categorical_passes(self):
        assert optional_role_as_feature_errors(
            _event_params(event="impression_id")
        ) == []

    def test_an_event_column_in_categorical_columns_is_reported(self):
        errs = optional_role_as_feature_errors(
            _event_params(
                event="impression_id",
                categorical=("prod_name", "impression_id"),
            )
        )
        assert len(errs) == 1
        assert "A38" in errs[0]
        assert "impression_id" in errs[0]
        assert "event" in errs[0]

    def test_the_message_points_at_the_feature_table(self):
        """A gate that only refuses sends the user looking for a way round it.
        The legitimate want — a feature about when the event happened — has an
        answer, and the message has to carry it."""
        errs = optional_role_as_feature_errors(
            _event_params(event="event_ts", categorical=("prod_name", "event_ts"))
        )
        assert "feature table" in errs[0]

    def test_every_offending_column_is_reported_at_once(self):
        errs = optional_role_as_feature_errors(
            _event_params(
                event=["event_ts", "impression_id"],
                categorical=("prod_name", "event_ts", "impression_id"),
            )
        )
        assert len(errs) == 2
        assert {"event_ts", "impression_id"} == {
            c for c in ("event_ts", "impression_id")
            if any(c in e for e in errs)
        }

    def test_the_item_exit_is_untouched(self):
        """The discriminating case: ``schema.item`` reaches the model through
        exactly this list, and A2 *requires* it to. A predicate that refused
        every identity column here would contradict A2 and pass every test
        above."""
        assert optional_role_as_feature_errors(
            _event_params(event="impression_id", categorical=("prod_name",))
        ) == []

    def test_an_absent_categorical_columns_key_passes(self):
        params = _event_params(event="impression_id")
        params["dataset"]["prepare_model_input"] = {}
        assert optional_role_as_feature_errors(params) == []


class TestOptionalRoleColumnsDeclaredA39:
    def test_no_optional_role_declared_passes_whatever_the_catalog_says(self):
        assert optional_role_columns_declared_errors(
            _event_params(), ["cust_id", "score"], "training_eval_predictions",
        ) == []

    def test_a_declaration_covering_the_event_column_passes(self):
        assert optional_role_columns_declared_errors(
            _event_params(event="impression_id"),
            ["cust_id", "snap_date", "prod_name", "impression_id", "score"],
            "training_eval_predictions",
        ) == []

    def test_a_missing_event_column_is_reported(self):
        errs = optional_role_columns_declared_errors(
            _event_params(event="impression_id"),
            ["cust_id", "snap_date", "prod_name", "score"],
            "training_eval_predictions",
        )
        assert len(errs) == 1
        assert "A39" in errs[0]
        assert "impression_id" in errs[0]
        assert "training_eval_predictions" in errs[0]

    def test_the_message_names_the_downstream_symptom(self):
        """Without it the operator reads "duplicate identity keys" from
        evaluation and goes looking upstream at label_table, which is correct."""
        errs = optional_role_columns_declared_errors(
            _event_params(event="impression_id"), ["cust_id"], "t",
        )
        assert "indistinguishable" in errs[0]

    def test_every_missing_column_of_one_role_is_named_in_one_error(self):
        errs = optional_role_columns_declared_errors(
            _event_params(event=["event_ts", "impression_id"]),
            ["cust_id"],
            "training_eval_predictions",
        )
        assert len(errs) == 1
        assert "event_ts" in errs[0] and "impression_id" in errs[0]

    def test_none_means_the_entry_infers_its_schema_and_drops_nothing(self):
        assert optional_role_columns_declared_errors(
            _event_params(event="impression_id"), None, "some_auto_table",
        ) == []

    def test_an_extra_declared_column_is_not_an_error(self):
        assert optional_role_columns_declared_errors(
            _event_params(event="impression_id"),
            ["cust_id", "impression_id", "score", "label", "snap_date"],
            "training_eval_predictions",
        ) == []


# =============================================================================
# B11 — source tables must carry every declared optional-role column
# =============================================================================

from recsys_tfb.core.consistency import optional_role_source_column_errors


def _b11_tables(sample_pool_extra=(), label_extra=()):
    """The two candidate-grain source tables, with per-test extra columns."""
    base = ["snap_date", "cust_id", "prod_name"]
    return {
        "sample_pool": [*base, "label", *sample_pool_extra],
        "label_table": [*base, "label", *label_extra],
    }


class TestOptionalRoleSourceColumnsB11:
    def test_no_optional_role_declared_passes(self):
        assert optional_role_source_column_errors(
            _event_params(), _b11_tables()
        ) == []

    def test_both_tables_carrying_the_column_passes(self):
        assert optional_role_source_column_errors(
            _event_params(event="impression_id"),
            _b11_tables(("impression_id",), ("impression_id",)),
        ) == []

    def test_a_missing_column_in_sample_pool_is_reported(self):
        errs = optional_role_source_column_errors(
            _event_params(event="impression_id"),
            _b11_tables((), ("impression_id",)),
        )
        assert len(errs) == 1
        assert "B11" in errs[0]
        assert "sample_pool" in errs[0]
        assert "impression_id" in errs[0]

    def test_a_missing_column_in_label_table_is_reported(self):
        errs = optional_role_source_column_errors(
            _event_params(event="impression_id"),
            _b11_tables(("impression_id",), ()),
        )
        assert len(errs) == 1
        assert "label_table" in errs[0]

    def test_both_tables_missing_gives_both_errors(self):
        """Two tables built from the same upstream query are usually missing
        the same column; one error per run would cost two passes to learn it."""
        errs = optional_role_source_column_errors(
            _event_params(event="impression_id"), _b11_tables(),
        )
        assert len(errs) == 2
        assert any("sample_pool" in e for e in errs)
        assert any("label_table" in e for e in errs)

    def test_every_missing_column_of_one_role_is_named_in_one_error(self):
        errs = optional_role_source_column_errors(
            _event_params(event=["event_ts", "impression_id"]),
            _b11_tables(("event_ts",), ("event_ts", "impression_id")),
        )
        assert len(errs) == 1
        assert "impression_id" in errs[0]
        assert "event_ts" not in errs[0]

    def test_feature_table_is_not_checked(self):
        """An entity-level table joins by base_key_columns, which no optional
        role widens. Requiring an impression column there would write the
        mis-classification ADR-0025 decision 2 warns about into a gate."""
        tables = _b11_tables(("impression_id",), ("impression_id",))
        tables["feature_table"] = ["snap_date", "cust_id", "tenure_days"]
        assert optional_role_source_column_errors(
            _event_params(event="impression_id"), tables
        ) == []

    def test_the_message_says_how_to_fix_it_either_way(self):
        errs = optional_role_source_column_errors(
            _event_params(event="impression_id"), _b11_tables(),
        )
        assert "source SQL" in errs[0]
        assert "remove" in errs[0]


# =============================================================================
# A40 — monitoring-mode evaluation is refused while an optional role is declared
# =============================================================================

from recsys_tfb.core.consistency import optional_role_monitoring_errors


class TestOptionalRoleMonitoringA40:
    def test_no_role_declared_allows_monitoring(self):
        assert optional_role_monitoring_errors(
            _event_params(), post_training=False
        ) == []

    def test_no_role_declared_allows_post_training(self):
        assert optional_role_monitoring_errors(
            _event_params(), post_training=True
        ) == []

    def test_declared_role_blocks_monitoring(self):
        errs = optional_role_monitoring_errors(
            _event_params(event="impression_id"), post_training=False
        )
        assert len(errs) == 1
        assert "A40" in errs[0]

    def test_declared_role_allows_post_training(self):
        """The discriminating case: post-training reads
        training_eval_predictions, which A39 makes carry the columns. A gate
        that blocked both modes would pass every other test here and leave the
        role unusable."""
        assert optional_role_monitoring_errors(
            _event_params(event="impression_id"), post_training=True
        ) == []

    def test_the_message_names_the_role_and_the_column(self):
        errs = optional_role_monitoring_errors(
            _event_params(event="impression_id"), post_training=False
        )
        assert "event" in errs[0] and "impression_id" in errs[0]

    def test_the_message_names_the_mode_that_works(self):
        errs = optional_role_monitoring_errors(
            _event_params(event="impression_id"), post_training=False
        )
        assert "--post-training" in errs[0]

    def test_the_message_says_the_answer_would_be_wrong_not_missing(self):
        """ADR-0021 decision 5's whole reason for stopping at the entry. An
        operator who reads "fewer rows" will try to work around the gate."""
        errs = optional_role_monitoring_errors(
            _event_params(event="impression_id"), post_training=False
        )
        assert "wrong answer" in errs[0]


# =============================================================================
# A38 / A39 / A40 / B11 with `occasion` declared (#428)
# =============================================================================


import re


def _occasion_params(occasion="request_id", event=None, categorical=("prod_name",)):
    params = _event_params(event=event, categorical=categorical)
    params["schema"]["columns"]["occasion"] = occasion
    return params


class TestOccasionReachesEveryOptionalRoleGate:
    """#378 wrote the four gates against ``OPTIONAL_ROLE_KEYS`` rather than
    against ``event``; these pin that ``occasion`` actually arrives at each
    one, and that each message names ``occasion`` — a user who declared only
    ``occasion`` must never read about ``event``."""

    def test_columns_come_out_in_identity_order(self):
        """``occasion`` sits before ``item`` in identity and ``event`` after
        it; the flat list is written out as prediction columns in this order."""
        assert optional_role_columns(
            _occasion_params(event="impression_id")
        ) == ["request_id", "impression_id"]

    def test_a38_an_occasion_column_in_categorical_columns_is_reported(self):
        errs = optional_role_as_feature_errors(
            _occasion_params(categorical=("prod_name", "request_id"))
        )
        assert len(errs) == 1
        assert "A38" in errs[0]
        assert "schema.columns.occasion" in errs[0]
        # The message used to explain itself with "an event timestamp" and
        # "when the event happened" — true for `event`, noise for `occasion`.
        assert re.search(r"\bevent\b", errs[0]) is None

    def test_a39_a_prediction_catalog_without_the_occasion_column_is_reported(self):
        errs = optional_role_columns_declared_errors(
            _occasion_params(),
            ["cust_id", "snap_date", "prod_name", "score"],
            "training_eval_predictions",
        )
        assert len(errs) == 1
        assert "A39" in errs[0] and "request_id" in errs[0]
        assert "schema.columns.occasion" in errs[0]

    def test_b11_a_source_table_without_the_occasion_column_is_reported(self):
        errs = optional_role_source_column_errors(
            _occasion_params(), _b11_tables(("request_id",), ()),
        )
        assert len(errs) == 1
        assert "B11" in errs[0] and "label_table" in errs[0]
        assert "schema.columns.occasion" in errs[0]

    def test_a40_monitoring_is_blocked_and_post_training_allowed(self):
        errs = optional_role_monitoring_errors(
            _occasion_params(), post_training=False
        )
        assert len(errs) == 1
        assert "A40" in errs[0] and "occasion" in errs[0]
        assert optional_role_monitoring_errors(
            _occasion_params(), post_training=True
        ) == []


class TestCompareAgainstInferenceOutputWithAnOptionalRoleA41:
    """``source: ranked_predictions`` is offline inference's table, built from
    its own entity × item grid with no optional-role columns — the same reason
    A40 refuses monitoring mode. Left to run, ``--compare`` failed deep in
    Spark with an unresolved-column error naming neither the role nor the
    source (#428)."""

    @staticmethod
    def _with_source(params, source):
        params["evaluation"] = {"compare_sources": {"prev": {
            "kind": "model_version", "label": "prev", "model_version": "v1",
            **({"source": source} if source else {}),
        }}}
        return params

    def test_ranked_predictions_is_refused_with_occasion(self):
        from recsys_tfb.core.consistency import optional_role_compare_source_errors

        errs = optional_role_compare_source_errors(
            self._with_source(_occasion_params(), "ranked_predictions"))
        assert len(errs) == 1
        assert "A41" in errs[0] and "ranked_predictions" in errs[0]
        assert "schema.columns.occasion" in errs[0]

    def test_ranked_predictions_is_refused_with_event(self):
        from recsys_tfb.core.consistency import optional_role_compare_source_errors

        errs = optional_role_compare_source_errors(
            self._with_source(_event_params(event="imp_id"), "ranked_predictions"))
        assert len(errs) == 1 and "schema.columns.event" in errs[0]

    @pytest.mark.parametrize("source", [
        None, "enriched_eval_predictions", "training_eval_predictions"])
    def test_the_post_training_tables_are_still_allowed(self, source):
        from recsys_tfb.core.consistency import optional_role_compare_source_errors

        assert optional_role_compare_source_errors(
            self._with_source(_occasion_params(), source)) == []

    def test_ranked_predictions_is_allowed_without_an_optional_role(self):
        from recsys_tfb.core.consistency import optional_role_compare_source_errors

        assert optional_role_compare_source_errors(
            self._with_source(_event_params(), "ranked_predictions")) == []


# --- ADR-0025 decision 3: the three *_zero_positive_group_ratio keys ---------

_ZP_KEYS = {
    "train": "train_zero_positive_group_ratio",
    "val": "val_zero_positive_group_ratio",
    "test": "test_zero_positive_group_ratio",
}


def _zp_params(**dataset) -> dict:
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},
        "dataset": dataset,
    }


class TestZeroPositiveGroupRatioA44:
    def test_absent_keys_are_clean(self):
        from recsys_tfb.core.consistency import zero_positive_group_ratio_errors

        assert zero_positive_group_ratio_errors({}) == []
        assert zero_positive_group_ratio_errors(_zp_params()) == []

    @pytest.mark.parametrize("value", [0, 0.0, 0.25, 1, 1.0])
    def test_every_value_in_the_closed_unit_interval_is_legal(self, value):
        from recsys_tfb.core.consistency import zero_positive_group_ratio_errors

        for key in _ZP_KEYS.values():
            assert zero_positive_group_ratio_errors(
                _zp_params(**{key: value})) == [], (key, value)

    @pytest.mark.parametrize("value", [-0.1, 1.5])
    def test_out_of_range_is_rejected_naming_the_key(self, value):
        from recsys_tfb.core.consistency import zero_positive_group_ratio_errors

        for key in _ZP_KEYS.values():
            errs = zero_positive_group_ratio_errors(_zp_params(**{key: value}))
            assert len(errs) == 1, key
            assert "A44" in errs[0] and f"dataset.{key}" in errs[0]
            assert repr(value) in errs[0]

    @pytest.mark.parametrize("value", ["0.5", True, None, [0.5]])
    def test_a_non_number_is_rejected(self, value):
        # True is an int to Python and would read as 1.0; "0.5" is a quoted
        # YAML value; None is `key:` with nothing after it — present in the
        # version payload, so not the same artifact as an absent key (A31's
        # reasoning).
        from recsys_tfb.core.consistency import zero_positive_group_ratio_errors

        errs = zero_positive_group_ratio_errors(
            _zp_params(val_zero_positive_group_ratio=value))
        assert len(errs) == 1, value
        assert "dataset.val_zero_positive_group_ratio" in errs[0]

    def test_every_bad_key_is_reported_in_one_pass(self):
        from recsys_tfb.core.consistency import zero_positive_group_ratio_errors

        errs = zero_positive_group_ratio_errors(_zp_params(**{
            key: 2 for key in _ZP_KEYS.values()}))
        assert len(errs) == 3

    def test_wired_into_validate_config_consistency(self):
        with pytest.raises(ConfigConsistencyError) as exc:
            validate_config_consistency(
                _zp_params(test_zero_positive_group_ratio=1.5))
        assert "A44" in str(exc.value)


class TestResolvedZeroPositiveGroupRatio:
    def test_absent_keys_resolve_to_todays_behaviour(self):
        # train keeps every group, val / test keep none — the defaults are
        # what the pipeline did before the keys existed (ADR-0025 decision 3).
        from recsys_tfb.core.consistency import resolved_zero_positive_group_ratio

        assert resolved_zero_positive_group_ratio({}, "train") == 1.0
        assert resolved_zero_positive_group_ratio({}, "val") == 0.0
        assert resolved_zero_positive_group_ratio({}, "test") == 0.0

    def test_declared_value_wins_and_is_read_from_its_own_key(self):
        from recsys_tfb.core.consistency import resolved_zero_positive_group_ratio

        params = _zp_params(
            train_zero_positive_group_ratio=0.1,
            val_zero_positive_group_ratio=0.2,
            test_zero_positive_group_ratio=0.3,
        )
        assert resolved_zero_positive_group_ratio(params, "train") == 0.1
        assert resolved_zero_positive_group_ratio(params, "val") == 0.2
        assert resolved_zero_positive_group_ratio(params, "test") == 0.3

    def test_an_unknown_split_raises(self):
        # train_dev reads the train key; letting "train_dev" resolve to a
        # default of its own would give it a different r in silence.
        from recsys_tfb.core.consistency import resolved_zero_positive_group_ratio

        with pytest.raises(ValueError, match="train_dev"):
            resolved_zero_positive_group_ratio({}, "train_dev")


class TestZeroPositiveGroupWeightDeclaredA45:
    def _args(self, declared, **dataset):
        return _zp_params(**dataset), declared, "training_eval_predictions"

    def test_nothing_is_required_while_test_keeps_no_zero_positive_group(self):
        from recsys_tfb.core.consistency import (
            zero_positive_group_weight_declared_errors,
        )

        for dataset in ({}, {"test_zero_positive_group_ratio": 0.0}):
            assert zero_positive_group_weight_declared_errors(
                *self._args(["cust_id", "score"], **dataset)) == []

    def test_a_positive_test_ratio_requires_the_weight_column(self):
        from recsys_tfb.core.consistency import (
            ZERO_POSITIVE_GROUP_WEIGHT_COL,
            zero_positive_group_weight_declared_errors,
        )

        errs = zero_positive_group_weight_declared_errors(
            *self._args(["cust_id", "score"], test_zero_positive_group_ratio=0.5))
        assert len(errs) == 1
        assert "A45" in errs[0]
        assert ZERO_POSITIVE_GROUP_WEIGHT_COL in errs[0]
        assert "training_eval_predictions" in errs[0]
        assert "dataset.test_zero_positive_group_ratio" in errs[0]

    def test_declaring_the_column_satisfies_it(self):
        from recsys_tfb.core.consistency import (
            ZERO_POSITIVE_GROUP_WEIGHT_COL,
            zero_positive_group_weight_declared_errors,
        )

        assert zero_positive_group_weight_declared_errors(*self._args(
            ["cust_id", "score", ZERO_POSITIVE_GROUP_WEIGHT_COL],
            test_zero_positive_group_ratio=1.0,
        )) == []

    def test_columns_auto_declares_nothing_and_drops_nothing(self):
        from recsys_tfb.core.consistency import (
            zero_positive_group_weight_declared_errors,
        )

        assert zero_positive_group_weight_declared_errors(
            *self._args(None, test_zero_positive_group_ratio=0.5)) == []

    def test_the_val_ratio_does_not_reach_the_prediction_table(self):
        # Only test predictions are written to this table; val never lands.
        from recsys_tfb.core.consistency import (
            zero_positive_group_weight_declared_errors,
        )

        assert zero_positive_group_weight_declared_errors(
            *self._args(["cust_id"], val_zero_positive_group_ratio=0.5)) == []


def _pq_on(**dataset) -> dict:
    params = _zp_params(**dataset)
    params["evaluation"] = {"report": {"sections": {"prediction_quality": True}}}
    return params


class TestPredictionQualityPopulationA46:
    def test_post_training_with_the_default_test_ratio_is_refused(self):
        from recsys_tfb.core.consistency import prediction_quality_population_errors

        for params in (_pq_on(), _pq_on(test_zero_positive_group_ratio=0.0)):
            errs = prediction_quality_population_errors(params, post_training=True)
            assert len(errs) == 1
            assert "A46" in errs[0]
            assert "dataset.test_zero_positive_group_ratio" in errs[0]
            assert "evaluation.report.sections.prediction_quality" in errs[0]

    def test_a_positive_test_ratio_is_accepted(self):
        from recsys_tfb.core.consistency import prediction_quality_population_errors

        assert prediction_quality_population_errors(
            _pq_on(test_zero_positive_group_ratio=0.1), post_training=True) == []

    def test_monitoring_mode_is_not_affected(self):
        # Monitoring LEFT-joins labels onto inference output; nothing upstream
        # dropped a group there.
        from recsys_tfb.core.consistency import prediction_quality_population_errors

        assert prediction_quality_population_errors(
            _pq_on(), post_training=False) == []

    def test_the_family_switched_off_is_not_affected(self):
        from recsys_tfb.core.consistency import prediction_quality_population_errors

        params = _pq_on()
        params["evaluation"]["report"]["sections"]["prediction_quality"] = False
        assert prediction_quality_population_errors(params, post_training=True) == []
        assert prediction_quality_population_errors(
            _zp_params(), post_training=True) == []


class TestZeroPositiveGroupWeightCollisionB12:
    def test_a_feature_by_that_name_collides_only_when_a_weight_is_added(self):
        from recsys_tfb.core.consistency import (
            ZERO_POSITIVE_GROUP_WEIGHT_COL as W,
            zero_positive_group_weight_collision_errors,
        )

        features = ["age", W]
        assert zero_positive_group_weight_collision_errors(_zp_params(), features) == []
        for key in ("val_zero_positive_group_ratio", "test_zero_positive_group_ratio"):
            errs = zero_positive_group_weight_collision_errors(
                _zp_params(**{key: 0.2}), features)
            assert len(errs) == 1 and "B12" in errs[0] and W in errs[0], key
        assert zero_positive_group_weight_collision_errors(
            _zp_params(val_zero_positive_group_ratio=0.2), ["age"]) == []

    def test_the_train_ratio_adds_no_weight_and_so_never_collides(self):
        from recsys_tfb.core.consistency import (
            ZERO_POSITIVE_GROUP_WEIGHT_COL as W,
            zero_positive_group_weight_collision_errors,
        )

        assert zero_positive_group_weight_collision_errors(
            _zp_params(train_zero_positive_group_ratio=0.2), [W]) == []


class TestTestCarriesZeroPositiveGroupWeight:
    def test_only_a_positive_test_ratio_carries_the_weight(self):
        from recsys_tfb.core.consistency import test_carries_zero_positive_group_weight

        assert not test_carries_zero_positive_group_weight({})
        assert not test_carries_zero_positive_group_weight(
            _zp_params(test_zero_positive_group_ratio=0.0))
        assert not test_carries_zero_positive_group_weight(
            _zp_params(val_zero_positive_group_ratio=0.5))
        assert test_carries_zero_positive_group_weight(
            _zp_params(test_zero_positive_group_ratio=0.5))


# =============================================================================
# B13 / B14 — the candidate-level feature table (ADR-0026)
# =============================================================================

from recsys_tfb.core.consistency import (
    candidate_feature_table_key_errors,
    feature_table_overlap_errors,
)

_B13_IDENTITY = ["snap_date", "cust_id", "req_id", "prod_name"]


class TestCandidateFeatureTableKeyB13:
    def test_a_table_carrying_every_identity_column_passes(self):
        assert candidate_feature_table_key_errors(
            _B13_IDENTITY, [*_B13_IDENTITY, "browse_30m"],
        ) == []

    def test_a_missing_identity_column_is_named(self):
        """The shape it catches: a table at (time, entity) grain declared as the
        candidate table — it has the base key and nothing below it."""
        errs = candidate_feature_table_key_errors(
            _B13_IDENTITY, ["snap_date", "cust_id", "browse_30m"],
        )

        assert len(errs) == 1
        assert errs[0].startswith("B13:")
        assert "['req_id', 'prod_name']" in errs[0]


class TestFeatureTableOverlapB14:
    def _errs(self, entity_cols, candidate_cols, drop=()):
        return feature_table_overlap_errors(
            entity_cols, candidate_cols, list(drop), _B13_IDENTITY, "label",
        )

    def test_disjoint_features_pass(self):
        assert self._errs(
            ["snap_date", "cust_id", "total_aum"],
            [*_B13_IDENTITY, "browse_30m"],
        ) == []

    def test_a_column_in_both_tables_is_named(self):
        errs = self._errs(
            ["snap_date", "cust_id", "total_aum", "device"],
            [*_B13_IDENTITY, "browse_30m", "device"],
        )

        assert len(errs) == 1
        assert errs[0].startswith("B14:")
        assert "'device'" in errs[0]

    def test_the_shared_key_is_not_an_overlap(self):
        """Both tables carry time and entity: that is the join key of one and
        part of the other's, not a feature read twice."""
        assert self._errs(
            ["snap_date", "cust_id", "total_aum"], _B13_IDENTITY,
        ) == []

    def test_a_dropped_column_in_both_is_not_an_overlap(self):
        """Dropped means selected from neither table, so nothing collides."""
        assert self._errs(
            ["snap_date", "cust_id", "etl_ts"], [*_B13_IDENTITY, "etl_ts"],
            drop=("etl_ts",),
        ) == []

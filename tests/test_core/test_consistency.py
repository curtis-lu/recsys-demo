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
                "columns": {"item": "prod_name"},
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
                "columns": {"item": "channel_name"},
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
            "columns": {"item": "prod_name"},
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


def test_segment_columns_without_source_flags_uncovered():
    from recsys_tfb.core.consistency import segment_columns_without_source
    params = {"evaluation": {
        "segment_columns": ["cust_segment_typ"],
        "segment_sources": {"hc": {"segment_column": "holding_combo"}},
    }}
    assert segment_columns_without_source(params) == ["cust_segment_typ"]


def test_segment_columns_without_source_ok_when_covered():
    from recsys_tfb.core.consistency import segment_columns_without_source
    params = {"evaluation": {
        "segment_columns": ["cust_segment_typ"],
        "segment_sources": {"cs": {"segment_column": "cust_segment_typ"}},
    }}
    assert segment_columns_without_source(params) == []


def test_segment_columns_without_source_empty_when_no_segment_columns():
    from recsys_tfb.core.consistency import segment_columns_without_source
    assert segment_columns_without_source({"evaluation": {}}) == []


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


class TestDiagnosisMetricParamsA15:
    def _params(self, metric=None, sample=None, ci=None):
        ev = {}
        if metric is not None:
            ev["metric"] = metric
        diag = {}
        if sample is not None:
            diag["sample"] = sample
        if ci is not None:
            diag["ci"] = ci
        if diag:
            ev["diagnosis"] = diag
        return {"evaluation": ev}

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
        )
        errors = diagnosis_metric_param_errors(p)
        assert len(errors) == 7
        joined = "\n".join(errors)
        for token in ["weight_alpha", "metric.k", "min_positives",
                      "shrinkage_k", "max_queries",
                      "min_pos_queries_per_item", "n_boot"]:
            assert token in joined

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            validate_config_consistency,
        )
        p = self._params(metric={"weight_alpha": 2.0})
        with _pytest.raises(ConfigConsistencyError, match="weight_alpha"):
            validate_config_consistency(p)


class TestReconciliationParamsA16:
    def _params(self, recon):
        return {"evaluation": {"diagnosis": {"reconciliation": recon}}}

    def test_absent_and_valid_defaults_clean(self):
        from recsys_tfb.core.consistency import reconciliation_param_errors
        assert reconciliation_param_errors({}) == []
        assert reconciliation_param_errors(self._params(
            {"enabled": True, "score_col": "score_uncalibrated",
             "explained_threshold": 0.3}
        )) == []

    def test_bad_values_report(self):
        from recsys_tfb.core.consistency import reconciliation_param_errors
        errors = reconciliation_param_errors(self._params(
            {"score_col": "rank", "explained_threshold": 0}
        ))
        assert len(errors) == 2
        joined = "\n".join(errors)
        assert "score_col" in joined and "explained_threshold" in joined

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="score_col"):
            validate_config_consistency(self._params({"score_col": "rank"}))


class TestEnabledMustBeBool:
    """A15/A16：enabled 必須是 bool——YAML 引號字串 "false" 恆真，會靜默啟用節點。"""

    def test_ci_enabled_string_rejected(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = {"evaluation": {"diagnosis": {"ci": {"enabled": "false"}}}}
        errors = diagnosis_metric_param_errors(p)
        assert len(errors) == 1 and "ci.enabled" in errors[0]

    def test_reconciliation_enabled_string_rejected(self):
        from recsys_tfb.core.consistency import reconciliation_param_errors
        p = {"evaluation": {"diagnosis": {"reconciliation": {"enabled": "false"}}}}
        errors = reconciliation_param_errors(p)
        assert len(errors) == 1 and "reconciliation.enabled" in errors[0]

    def test_bool_values_clean(self):
        from recsys_tfb.core.consistency import (
            diagnosis_metric_param_errors,
            reconciliation_param_errors,
        )
        p = {"evaluation": {"diagnosis": {"ci": {"enabled": False},
                                          "reconciliation": {"enabled": False}}}}
        assert diagnosis_metric_param_errors(p) == []
        assert reconciliation_param_errors(p) == []


class TestQuadrantParamsA17:
    def _params(self, quad):
        return {"evaluation": {"diagnosis": {"quadrant": quad}}}

    def test_absent_and_valid_defaults_clean(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        assert quadrant_param_errors({}) == []
        assert quadrant_param_errors(self._params(
            {"enabled": True, "auc_threshold": 0.6, "gap_band": 0.35,
             "top_k_occupancy": 1}
        )) == []

    def test_bad_values_report(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        errors = quadrant_param_errors(self._params(
            {"auc_threshold": 0.4, "gap_band": 0, "top_k_occupancy": 0,
             "enabled": "false"}
        ))
        assert len(errors) == 4
        joined = "\n".join(errors)
        assert "auc_threshold" in joined and "gap_band" in joined
        assert "top_k_occupancy" in joined and "enabled" in joined

    def test_auc_threshold_boundaries(self):
        from recsys_tfb.core.consistency import quadrant_param_errors
        assert quadrant_param_errors(self._params({"auc_threshold": 0.5})) == []
        assert quadrant_param_errors(self._params({"auc_threshold": 1.0})) != []

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError, validate_config_consistency,
        )
        with _pytest.raises(ConfigConsistencyError, match="auc_threshold"):
            validate_config_consistency(self._params({"auc_threshold": 0.4}))

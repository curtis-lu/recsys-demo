"""validate_config_consistency must run in _load_config_and_setup."""

import inspect

from recsys_tfb import __main__ as m


def test_load_config_calls_validate_config_consistency():
    src = inspect.getsource(m._load_config_and_setup)
    assert "validate_config_consistency(params)" in src

def test_validate_config_consistency_imported():
    assert hasattr(m, "validate_config_consistency")


def test_a7_ranking_conflict_surfaces_via_validate():
    import pytest

    from recsys_tfb.core.consistency import (
        ConfigConsistencyError,
        validate_config_consistency,
    )

    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"algorithm_params": {
            "objective": "lambdarank", "metric": "binary_logloss"}},
    }
    with pytest.raises(ConfigConsistencyError, match="ranking metric"):
        validate_config_consistency(params)


def test_a22_wired_into_evaluation_command_before_spark():
    # A22 cannot be aggregated by validate_config_consistency (it needs the
    # --post-training flag), so the only thing standing between a mis-set
    # evaluation.snap_date and a normal-looking report is this one call site.
    # Source inspection rather than a CliRunner run: invoking the command for
    # real needs a config tree and would build a Spark session on the happy
    # path. It catches deletion of the call, not misuse of its result.
    src = inspect.getsource(m.evaluation)
    # The flag must be forwarded, not hardcoded: `post_training=True` would
    # break monitoring, `post_training=False` would disable A22 entirely, and
    # both keep the unit tests green because they call the predicate directly.
    assert "post_training_snap_date_errors(params, post_training=post_training)" in src
    assert src.index("post_training_snap_date_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A22 must fail before the Spark cold start, like A21"


def test_a34_wired_into_evaluation_command_before_spark():
    # A34 reads evaluation-only keys, so like A24 it must stay off the global
    # aggregator (#158: a conf still carrying a dead report switch must not
    # stop dataset, training or inference) and live on the evaluation command
    # instead. Source inspection for the same reason as A22 above; the
    # behavioural half is TestReportSectionKeysA34 in test_consistency.py.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "report_section_key_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A34 must stay off the global aggregator (#158 precedent)"
    src = inspect.getsource(m.evaluation)
    assert "report_section_key_errors(params)" in src
    assert src.index("report_section_key_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A34 must fail before the Spark cold start, like A22"


def test_a42_wired_into_evaluation_command_before_spark():
    # A42 reads evaluation-only keys: A34's placement, for A34's reason.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "prediction_quality_param_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A42 must stay off the global aggregator (#158 precedent)"
    src = inspect.getsource(m.evaluation)
    assert "prediction_quality_param_errors(params)" in src
    assert src.index("prediction_quality_param_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A42 must fail before the Spark cold start, like A34"


def test_a12_a13_wired_into_evaluation_command_before_spark():
    # A12/A13 read only the two CLI flags and evaluation.compare_sources, so a
    # mistyped --compare key must not cost a Spark cold start before it is
    # reported. They used to run after the session was built, the one exception
    # among the command-specific checks (A21/A22/A23/A24/A26/A27/A34 all run
    # before it). Source inspection for the same reason as A22 above.
    src = inspect.getsource(m.evaluation)
    spark = src.index("get_or_create_spark_session(")
    assert src.index("compare_mutual_exclusive_errors(") < spark, (
        "A13 must fail before the Spark cold start, like A22"
    )
    assert src.index("compare_source_key_exists(") < spark, (
        "A12 must fail before the Spark cold start, like A22"
    )


def test_a8_search_space_schema_surfaces_via_validate():
    import pytest

    from recsys_tfb.core.consistency import (
        ConfigConsistencyError,
        validate_config_consistency,
    )

    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"search_space": {"learning_rate": {"low": 1, "high": 2}}},
    }
    with pytest.raises(ConfigConsistencyError, match="must be a list"):
        validate_config_consistency(params)


def test_a24_wired_into_dataset_command_before_spark():
    # A24 reads dataset-only config keys, so it must NOT be aggregated by
    # validate_config_consistency (that gate runs at the entry of every
    # command — issue #158 measured 9 unrelated tests blocked by exactly this
    # mistake). The behavioural half of this rule is in tests/test_cli.py
    # TestDateSplitOverlapA24; here we pin the two structural halves.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "date_split_overlap_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A24 must stay off the global aggregator (#158 precedent)"

    src = inspect.getsource(m.dataset)
    assert "date_split_overlap_errors(params)" in src
    assert src.index("date_split_overlap_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A24 must fail before the Spark cold start, like A21"


def test_a23_wired_into_dataset_command_before_spark():
    # Same rule as A24, and #158's decision is the reason it exists: only the
    # dataset pipeline reads train_snap_dates, so aggregating this rejects a
    # valid feature_etl / source_etl / inference config. Without a test on it,
    # the next person tidies it into the aggregator and blocks 9 unrelated
    # tests again.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "train_snap_dates_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A23 must stay off the global aggregator (#158's decision)"

    src = inspect.getsource(m.dataset)
    assert "train_snap_dates_errors(params)" in src
    assert src.index("train_snap_dates_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A23 must fail before the Spark cold start, like A21/A24"


def test_a23_is_checked_before_a24():
    # A24 reads an absent list as empty and an empty set overlaps nothing, so
    # a config missing the key entirely would otherwise be told "your splits
    # are fine" and nothing else.
    src = inspect.getsource(m.dataset)
    assert src.index("train_snap_dates_errors(") < src.index(
        "date_split_overlap_errors("
    )


def test_a26_wired_into_training_command_before_spark():
    # Same rule as A23/A24, and the same failure mode if someone tidies it
    # into the aggregator: validate_config_consistency runs at the entry of
    # EVERY command, while the harm A26 front-runs (two cache entries on one
    # directory -> that month's rows counted twice) exists only in training.
    # The dataset pipeline normalises its months through pd.Timestamp into a
    # set, so the same config is harmless there.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "duplicate_test_month_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A26 must stay off the global aggregator (#158 precedent)"

    src = inspect.getsource(m.training)
    assert "duplicate_test_month_errors(params)" in src
    assert src.index("duplicate_test_month_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A26 must fail before the Spark cold start, like A21/A23/A24"


def test_a36_wired_into_training_command_before_spark():
    # The whole point of A36 (#133) is timing: the same missing month already
    # fails today, but only after the HPO search. Wired off the aggregator for
    # A24's reason — the dataset command legitimately runs with no test month.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "missing_test_month_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A36 must stay off the global aggregator (#158 precedent)"

    src = inspect.getsource(m.training)
    assert "missing_test_month_errors(params)" in src
    assert src.index("missing_test_month_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A36 must fail before the Spark cold start, like A21/A23/A24/A26"


def test_a26_is_checked_before_a21():
    # A21 resolves --rebuild-dates against dataset.test_snap_dates. If the
    # month is spelled two ways, "is this flag value a configured month" has
    # two different answers, so the ambiguity should be reported first.
    src = inspect.getsource(m.training)
    assert src.index("duplicate_test_month_errors(") < src.index(
        "resolved_rebuild_dates("
    )


def test_a30_runs_before_the_config_loader():
    # A30 cannot be aggregated by validate_config_consistency (it reads --env
    # and the filesystem), so this one call site is the whole gate. Order
    # matters as much as presence: ConfigLoader turns a missing conf/<env>
    # into an empty overlay, after which nothing distinguishes "environment
    # not found" from "environment had no overrides".
    src = inspect.getsource(m._load_config_and_setup)
    assert "resolved_env_dir(conf_dir, env)" in src
    assert src.index("resolved_env_dir(") < src.index("ConfigLoader(")


def test_a30_error_is_caught_and_exits_cleanly():
    # ConfigConsistencyError subclasses ValueError, and the call sits inside
    # the block whose `except ValueError` turns it into `typer.Exit(1)`. A
    # raw traceback would be a regression, not a nicety: the message is the
    # product here.
    src = inspect.getsource(m._load_config_and_setup)
    head = src.split("resolved_env_dir(")[0]
    assert "try:" in head


def test_every_env_taking_command_reaches_the_a30_gate():
    # A30 lives at exactly one call site, which is only safe while every
    # command that accepts --env actually reaches it. A ninth command that
    # built its own ConfigLoader would restore the silent degradation with
    # every unit test above still green. Four of the eight commands reach the
    # gate indirectly (the *_etl commands delegate to _run_etl), so this walks
    # the module's call graph rather than grepping each body.
    import ast

    src = inspect.getsource(m)
    tree = ast.parse(src)
    funcs = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}

    def reaches_gate(name, seen):
        if name in seen or name not in funcs:
            return False
        seen.add(name)
        for call in ast.walk(funcs[name]):
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
                continue
            if call.func.id == "_load_config_and_setup":
                return True
            if reaches_gate(call.func.id, seen):
                return True
        return False

    commands = [
        n.name
        for n in tree.body
        if isinstance(n, ast.FunctionDef)
        and '"--env"' in (ast.get_source_segment(src, n) or "")
    ]
    # Guard the guard: if the '--env' probe ever stops matching, the loop
    # below would pass by finding nothing to check.
    assert len(commands) >= 8, f"expected every CLI command, found {commands}"

    missing = [name for name in commands if not reaches_gate(name, set())]
    assert not missing, f"command(s) take --env but never reach A30: {missing}"


def test_a27_wired_into_inference_command_before_spark():
    # Same rule as A23/A24/A26: the three keys A27 reads (inference.snap_dates
    # / entity_buckets / products) are read by the inference pipeline alone,
    # so aggregating this would reject a valid dataset or training config —
    # #158 measured that cost at 9 blocked tests. And the order is the whole
    # point of the ticket: every raise A27 replaces fired only after
    # build_inference_population_features had already run a full Spark pass.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "inference_grid_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A27 must stay off the global aggregator (#158 precedent)"

    src = inspect.getsource(m.inference)
    assert "inference_grid_errors(params)" in src
    assert src.index("inference_grid_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A27 must fail before the Spark cold start, like A21/A23/A24/A26"


def test_a27_is_checked_before_a21():
    # A21 resolves --rebuild-dates against inference.snap_dates. With that
    # list empty, every flag value is "not a configured month" — a second-order
    # message that sends the operator after the wrong key.
    src = inspect.getsource(m.inference)
    assert src.index("inference_grid_errors(") < src.index(
        "resolved_inference_rebuild_dates("
    )


def test_a35_wired_into_run_etl_before_spark():
    # A35 reads the --var CLI flags, which validate_config_consistency (run
    # at the entry of every command) is never given — mirroring A12/A21/A30.
    # _run_etl is the one shared executor behind all four ETL commands
    # (feature_etl/label_etl/sample_pool_etl/inference_population_etl), so
    # pinning it here covers all four without four near-duplicate tests.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "etl_cli_var_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A35 must stay off the global aggregator (A12/A21/A30 precedent)"

    src = inspect.getsource(m._run_etl)
    assert "etl_cli_var_errors(" in src
    spark = src.index("get_or_create_spark_session(")
    assert src.index("etl_cli_var_errors(") < spark, (
        "A35 must fail before the Spark cold start, like A21/A23/A24/A26/A27"
    )
    # check_renders (the SQL-side backstop A35's docstring pairs it with)
    # must also run before Spark, and on the --source-check path too — a
    # residual ${...} would otherwise be discovered only after Hive was hit.
    assert "check_renders(" in src
    assert src.index("check_renders(") < spark, (
        "check_renders must fail before the Spark cold start"
    )


def test_a38_reaches_the_global_aggregator():
    # A38 takes parameters alone and the mistake costs a whole training run to
    # find otherwise, so it belongs on the aggregator beside A2 — the rule it
    # mirrors. Behavioural half: TestOptionalRoleAsFeatureA38.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "optional_role_as_feature_errors" in inspect.getsource(
        validate_config_consistency
    )


def test_a39_wired_into_training_command_before_spark():
    # A39 needs the resolved catalog, which the aggregator never sees, so like
    # A28 it lives on the training command. Source inspection for A22's
    # reason; the behavioural half is TestOptionalRoleColumnsDeclaredA39.
    src = inspect.getsource(m.training)
    assert "optional_role_columns_declared_errors(" in src
    assert src.index("optional_role_columns_declared_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A39 must fail before the Spark cold start, like A28"


def test_a39_reads_the_same_catalog_entry_as_a28():
    # One read, both predicates: an operator missing an entity column and an
    # event column should fix one `columns:` list once, not learn about the
    # second only after fixing the first.
    src = inspect.getsource(m.training)
    # The catalog is read once into a variable both predicates are handed.
    # Asserting the shared name rather than two `get_dataset` calls is what
    # makes a second, independently-read copy fail here.
    assert src.count("getattr(") == 1, "the catalog entry is read once"
    assert src.count("gate_declared") >= 3, (
        "both A28 and A39 must be handed that one read"
    )


def test_a40_wired_into_evaluation_command_before_spark():
    # A40 needs --post-training, which the aggregator never sees (A22's
    # reason). The flag must be forwarded, not hardcoded: `True` would disable
    # the gate entirely and `False` would block post-training runs too, and
    # both keep the unit tests green because those call the predicate directly.
    src = inspect.getsource(m.evaluation)
    assert "optional_role_monitoring_errors(params, post_training)" in src
    assert src.index("optional_role_monitoring_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A40 must fail before the Spark cold start, like A22"


def test_a41_aggregated_beside_a11():
    # A41 takes parameters alone, so it rides the aggregator every command
    # runs at its entry. Behavioural half: TestCompareAgainstInference
    # OutputWithAnOptionalRoleA41.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "optional_role_compare_source_errors" in inspect.getsource(
        validate_config_consistency
    )


def test_a43_wired_into_evaluation_command_before_spark():
    # A43 reads evaluation-only keys: A34's placement, for A34's reason.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "retired_calibration_bin_key_errors" not in inspect.getsource(
        validate_config_consistency
    ), "A43 must stay off the global aggregator (#158 precedent)"
    src = inspect.getsource(m.evaluation)
    assert "retired_calibration_bin_key_errors(params)" in src
    assert src.index("retired_calibration_bin_key_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A43 must fail before the Spark cold start, like A34"


def test_a44_reaches_the_global_aggregator():
    # A44's keys feed train_variant_id and base_dataset_version, which every
    # command resolves — A31's placement. Behavioural half:
    # TestZeroPositiveGroupRatioA44.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "zero_positive_group_ratio_errors" in inspect.getsource(
        validate_config_consistency
    )


def test_a45_reads_the_catalog_entry_a28_reads_before_spark():
    # Needs the resolved catalog (A28/A39's placement) and must be handed the
    # same single read, so one `columns:` edit fixes all three.
    src = inspect.getsource(m.training)
    call = "zero_positive_group_weight_declared_errors(\n            params, gate_declared,"
    assert call in src
    assert src.index("zero_positive_group_weight_declared_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A45 must fail before the Spark cold start, like A28"


def test_a46_wired_into_evaluation_command_before_spark():
    # Needs --post-training (A22/A40's reason); the flag must be forwarded, not
    # hardcoded — `False` would disable the gate and `True` would block
    # monitoring runs that nothing upstream filtered.
    from recsys_tfb.core.consistency import validate_config_consistency

    assert "prediction_quality_population_errors" not in inspect.getsource(
        validate_config_consistency
    )
    src = inspect.getsource(m.evaluation)
    assert "prediction_quality_population_errors(params, post_training)" in src
    assert src.index("prediction_quality_population_errors(") < src.index(
        "get_or_create_spark_session("
    ), "A46 must fail before the Spark cold start, like A22"

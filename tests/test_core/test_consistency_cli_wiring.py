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

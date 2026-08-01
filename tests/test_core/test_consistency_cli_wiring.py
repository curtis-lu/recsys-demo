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

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

"""ConfigEnvError must be caught inside _load_config_and_setup;
spark config no longer calls resolve_env_placeholders."""

import inspect

from recsys_tfb import __main__ as m


def test_config_loader_construction_inside_try():
    src = inspect.getsource(m._load_config_and_setup)
    # ConfigLoader is constructed inside the try block so ConfigEnvError
    # (a ValueError subclass) is caught and turned into a clean CLI exit.
    before_loader = src.split("ConfigLoader(")[0]
    assert "try:" in before_loader


def test_load_spark_config_no_env_resolver():
    src = inspect.getsource(m._load_spark_config)
    assert "resolve_env_placeholders" not in src
    assert "resolve_vdclient_placeholders" in src

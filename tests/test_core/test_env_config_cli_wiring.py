"""ConfigEnvError 必須在 _load_config_and_setup 內被接住;
spark config 不再呼叫 resolve_env_placeholders。"""

import inspect

from recsys_tfb import __main__ as m


def test_config_loader_construction_inside_try():
    src = inspect.getsource(m._load_config_and_setup)
    # ConfigLoader 在 try 區塊內建構,使 ConfigEnvError(ValueError 子類)
    # 被捕捉並轉成乾淨的 CLI exit。
    before_loader = src.split("ConfigLoader(")[0]
    assert "try:" in before_loader


def test_load_spark_config_no_env_resolver():
    src = inspect.getsource(m._load_spark_config)
    assert "resolve_env_placeholders" not in src
    assert "resolve_vdclient_placeholders" in src

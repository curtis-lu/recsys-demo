"""Tests for resolve_vdclient_placeholders."""

import logging
import sys
import types

from recsys_tfb.utils.vdclient_resolver import (
    resolve_env_placeholders,
    resolve_vdclient_placeholders,
)


def _install_fake_vdclient(monkeypatch, **getters):
    """Install a fake vdclient module exposing get_<name> for each kwarg."""
    fake = types.ModuleType("vdclient")
    for name, value in getters.items():
        setattr(fake, f"get_{name}", lambda v=value: v)
    monkeypatch.setitem(sys.modules, "vdclient", fake)


def _block_vdclient_import(monkeypatch):
    """Make ``import vdclient`` raise ImportError."""
    monkeypatch.setitem(sys.modules, "vdclient", None)


class TestResolve:
    def test_no_placeholders_passthrough(self, monkeypatch):
        _install_fake_vdclient(monkeypatch)
        out = resolve_vdclient_placeholders(
            {"spark.executor.memory": "16g", "spark.executor.cores": 4}
        )
        assert out == {
            "spark.executor.memory": "16g",
            "spark.executor.cores": 4,
        }

    def test_substitutes_when_vdclient_available(self, monkeypatch):
        _install_fake_vdclient(monkeypatch, metastore_port=9083)
        out = resolve_vdclient_placeholders(
            {
                "spark.hadoop.hive.metastore.uris":
                    "thrift://host:${vdclient.metastore_port}",
                "spark.executor.cores": 4,
            }
        )
        assert out["spark.hadoop.hive.metastore.uris"] == "thrift://host:9083"
        assert out["spark.executor.cores"] == 4

    def test_drops_keys_when_vdclient_missing(self, monkeypatch, caplog):
        _block_vdclient_import(monkeypatch)
        with caplog.at_level(logging.WARNING):
            out = resolve_vdclient_placeholders(
                {
                    "spark.hadoop.hive.metastore.uris":
                        "thrift://host:${vdclient.metastore_port}",
                    "spark.executor.memory": "4g",
                }
            )
        assert "spark.hadoop.hive.metastore.uris" not in out
        assert out["spark.executor.memory"] == "4g"
        assert any(
            "vdclient not importable" in r.message for r in caplog.records
        )

    def test_drops_key_when_getter_missing(self, monkeypatch, caplog):
        _install_fake_vdclient(monkeypatch, other_thing=1)
        with caplog.at_level(logging.WARNING):
            out = resolve_vdclient_placeholders(
                {"x": "${vdclient.metastore_port}", "y": "static"}
            )
        assert "x" not in out
        assert out["y"] == "static"
        assert any(
            "get_metastore_port" in r.message for r in caplog.records
        )

    def test_multiple_placeholders_in_one_value(self, monkeypatch):
        _install_fake_vdclient(
            monkeypatch, metastore_host="hivehost", metastore_port=9083
        )
        out = resolve_vdclient_placeholders(
            {
                "x": "thrift://${vdclient.metastore_host}:"
                     "${vdclient.metastore_port}",
            }
        )
        assert out["x"] == "thrift://hivehost:9083"

    def test_non_string_values_pass_through(self, monkeypatch):
        _install_fake_vdclient(monkeypatch)
        out = resolve_vdclient_placeholders(
            {"spark.executor.cores": 4, "spark.ui.enabled": False}
        )
        assert out == {"spark.executor.cores": 4, "spark.ui.enabled": False}


class TestResolveEnv:
    def test_no_placeholders_passthrough(self):
        out = resolve_env_placeholders(
            {"spark.executor.memory": "16g", "spark.executor.cores": 4}
        )
        assert out == {
            "spark.executor.memory": "16g",
            "spark.executor.cores": 4,
        }

    def test_substitutes_when_env_var_set(self, monkeypatch):
        monkeypatch.setenv("HIVE_HOST", "hivehost.internal")
        out = resolve_env_placeholders(
            {
                "spark.hadoop.hive.metastore.uris":
                    "thrift://${env.HIVE_HOST}:9083",
                "spark.executor.cores": 4,
            }
        )
        assert (
            out["spark.hadoop.hive.metastore.uris"]
            == "thrift://hivehost.internal:9083"
        )
        assert out["spark.executor.cores"] == 4

    def test_drops_key_when_env_var_missing(self, monkeypatch, caplog):
        monkeypatch.delenv("HIVE_HOST", raising=False)
        with caplog.at_level(logging.WARNING):
            out = resolve_env_placeholders(
                {
                    "spark.hadoop.hive.metastore.uris":
                        "thrift://${env.HIVE_HOST}:9083",
                    "spark.executor.memory": "4g",
                }
            )
        assert "spark.hadoop.hive.metastore.uris" not in out
        assert out["spark.executor.memory"] == "4g"
        assert any("HIVE_HOST" in r.message for r in caplog.records)

    def test_multiple_env_placeholders(self, monkeypatch):
        monkeypatch.setenv("HIVE_HOST", "hivehost")
        monkeypatch.setenv("HIVE_PORT", "9083")
        out = resolve_env_placeholders(
            {"x": "thrift://${env.HIVE_HOST}:${env.HIVE_PORT}"}
        )
        assert out["x"] == "thrift://hivehost:9083"

    def test_non_string_values_pass_through(self):
        out = resolve_env_placeholders(
            {"spark.executor.cores": 4, "spark.ui.enabled": False}
        )
        assert out == {"spark.executor.cores": 4, "spark.ui.enabled": False}

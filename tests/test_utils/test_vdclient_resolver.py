"""Tests for resolve_vdclient_placeholders."""

import logging
import sys
import types

from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders


def _install_fake_vdclient_magic(monkeypatch, ports_by_cluster: dict):
    """Install a fake vdclient_magic module with spark_ports(cluster) -> tuple."""
    fake = types.ModuleType("vdclient_magic")
    fake.spark_ports = lambda cluster: ports_by_cluster[cluster]
    monkeypatch.setitem(sys.modules, "vdclient_magic", fake)


def _block_vdclient_magic_import(monkeypatch):
    """Make ``import vdclient_magic`` raise ImportError."""
    monkeypatch.setitem(sys.modules, "vdclient_magic", None)


class TestResolve:
    def test_no_placeholders_passthrough(self, monkeypatch):
        _install_fake_vdclient_magic(monkeypatch, {})
        out = resolve_vdclient_placeholders(
            {"spark.executor.memory": "16g", "spark.executor.cores": 4}
        )
        assert out == {
            "spark.executor.memory": "16g",
            "spark.executor.cores": 4,
        }

    def test_substitutes_driver_port(self, monkeypatch):
        _install_fake_vdclient_magic(monkeypatch, {"cdp": (40001, 40002)})
        out = resolve_vdclient_placeholders(
            {
                "spark.driver.port": "${vdclient.cdp.driver_port}",
                "spark.executor.cores": 4,
            }
        )
        assert out["spark.driver.port"] == "40001"
        assert out["spark.executor.cores"] == 4

    def test_substitutes_block_manager_port(self, monkeypatch):
        _install_fake_vdclient_magic(monkeypatch, {"cdp": (40001, 40002)})
        out = resolve_vdclient_placeholders(
            {"spark.blockManager.port": "${vdclient.cdp.blockManager_port}"}
        )
        assert out["spark.blockManager.port"] == "40002"

    def test_drops_keys_when_vdclient_magic_missing(self, monkeypatch, caplog):
        _block_vdclient_magic_import(monkeypatch)
        with caplog.at_level(logging.WARNING):
            out = resolve_vdclient_placeholders(
                {
                    "spark.driver.port": "${vdclient.cdp.driver_port}",
                    "spark.executor.memory": "4g",
                }
            )
        assert "spark.driver.port" not in out
        assert out["spark.executor.memory"] == "4g"
        assert any(
            "vdclient_magic not importable" in r.message for r in caplog.records
        )

    def test_drops_key_when_spark_ports_raises(self, monkeypatch, caplog):
        fake = types.ModuleType("vdclient_magic")
        fake.spark_ports = lambda cluster: (_ for _ in ()).throw(
            RuntimeError("cluster not found")
        )
        monkeypatch.setitem(sys.modules, "vdclient_magic", fake)
        with caplog.at_level(logging.WARNING):
            out = resolve_vdclient_placeholders(
                {"spark.driver.port": "${vdclient.cdp.driver_port}", "y": "static"}
            )
        assert "spark.driver.port" not in out
        assert out["y"] == "static"

    def test_drops_key_when_field_unknown(self, monkeypatch, caplog):
        _install_fake_vdclient_magic(monkeypatch, {"cdp": (40001, 40002)})
        with caplog.at_level(logging.WARNING):
            out = resolve_vdclient_placeholders(
                {"x": "${vdclient.cdp.unknown_field}", "y": "static"}
            )
        assert "x" not in out
        assert out["y"] == "static"
        assert any("unknown_field" in r.message for r in caplog.records)

    def test_multiple_clusters_cached(self, monkeypatch):
        call_counts: dict = {"cdp": 0, "hue": 0}

        def spark_ports(cluster):
            call_counts[cluster] += 1
            return {"cdp": (40001, 40002), "hue": (50001, 50002)}[cluster]

        fake = types.ModuleType("vdclient_magic")
        fake.spark_ports = spark_ports
        monkeypatch.setitem(sys.modules, "vdclient_magic", fake)

        out = resolve_vdclient_placeholders(
            {
                "a": "${vdclient.cdp.driver_port}",
                "b": "${vdclient.cdp.blockManager_port}",
                "c": "${vdclient.hue.driver_port}",
            }
        )
        assert out == {"a": "40001", "b": "40002", "c": "50001"}
        assert call_counts["cdp"] == 1
        assert call_counts["hue"] == 1

    def test_non_string_values_pass_through(self, monkeypatch):
        _install_fake_vdclient_magic(monkeypatch, {})
        out = resolve_vdclient_placeholders(
            {"spark.executor.cores": 4, "spark.ui.enabled": False}
        )
        assert out == {"spark.executor.cores": 4, "spark.ui.enabled": False}

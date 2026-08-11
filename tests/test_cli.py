import ast
import json
import logging
import os
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml
from typer.testing import CliRunner

from recsys_tfb.__main__ import app
from recsys_tfb.core.catalog import DataCatalog
from recsys_tfb.core.config import ConfigLoader

runner = CliRunner()


def _partition_rows(specs):
    """A ``SHOW PARTITIONS`` result whose ``collect()`` yields the given specs."""
    result = MagicMock()
    result.collect.return_value = [(s,) for s in specs]
    return result


class _CatalogSubstitutionSpy:
    """Records every substitution dict the CLI hands to ``get_catalog_config``.

    Every call is recorded, not just the version-carrying ones: the dataset
    command legitimately reads the version-free source-table entries *before*
    any version exists, and the whole question is which of those two configs the
    partition listing ends up asking. ``at_listing`` is the answer — the most
    recent config built when ``SHOW PARTITIONS`` ran. Asserting on the calls as
    a set would be satisfied by the version-scoped config the pipeline itself
    runs on, which exists either way.
    """

    def __init__(self):
        self.calls = []
        self.at_listing = None

    @property
    def base_dataset_version(self):
        return self.calls[-1].get("base_dataset_version", "") if self.calls else ""

    def note_listing(self):
        self.at_listing = self.calls[-1] if self.calls else {}

    def patch(self):
        real = ConfigLoader.get_catalog_config

        def spy(config_self, runtime_params=None):
            self.calls.append(dict(runtime_params or {}))
            return real(config_self, runtime_params=runtime_params)

        return patch.object(ConfigLoader, "get_catalog_config", spy)


def _mock_spark_with_feature_table_schema(columns=None):
    """Build a SparkSession-like mock whose ``table(fqn).schema.fields``
    returns the given (name, dtype) sequence as ``Mock`` field objects.

    Used by tests that mock out DataCatalog/Runner — the CLI now reads
    feature_table schema before reaching the catalog, so we need a
    spark-shaped stand-in.
    """
    if columns is None:
        columns = [("snap_date", "date"), ("cust_id", "string"), ("aum", "double")]
    fields = []
    for name, dtype in columns:
        f = MagicMock()
        f.name = name
        f.dataType.simpleString.return_value = dtype
        fields.append(f)
    spark = MagicMock()
    spark.table.return_value.schema.fields = fields
    return spark


def _setup_conf(tmp_path, params_dataset=None, params_training=None, params_inference=None):
    """Create minimal conf dirs with catalog and optional parameter files."""
    base_dir = tmp_path / "conf" / "base"
    base_dir.mkdir(parents=True)
    local_dir = tmp_path / "conf" / "local"
    local_dir.mkdir(parents=True)

    catalog = {
        "feature_table": {
            "type": "HiveTableDataset",
            "database": "ml_recsys",
            "table": "feature_table",
            # As in conf/base/catalog.yaml: a source table maintained by
            # source_etl. Without it HiveTableDataset refuses to be built at all
            # (a writable table must declare its columns).
            "read_only": True,
        },
        "model": {
            "type": "ModelAdapterDataset",
            "filepath": "data/models/${model_version}/model.txt",
        },
        "preprocessor": {
            "type": "PickleDataset",
            "filepath": "data/dataset/${base_dataset_version}/preprocessor.pkl",
        },
        "sample_keys": {
            "type": "ParquetDataset",
            "filepath": "data/dataset/${base_dataset_version}/train_variants/${train_variant_id}/sample_keys.parquet",
        },
        "train_model_input": {
            "type": "ParquetDataset",
            "filepath": "data/dataset/${base_dataset_version}/train_variants/${train_variant_id}/train_model_input.parquet",
        },
        # A synthetic entry, not a real catalog name: it exists so the
        # assertions below can pin that the inference command resolves
        # ${model_version} (to the hash, never "best") and ${snap_date}.
        "inference_scratch": {
            "type": "ParquetDataset",
            "filepath": "data/inference/${model_version}/${snap_date}/scratch.parquet",
        },
    }
    with open(base_dir / "catalog.yaml", "w") as f:
        yaml.dump(catalog, f)

    if params_dataset:
        with open(base_dir / "parameters_dataset.yaml", "w") as f:
            yaml.dump(params_dataset, f)
    if params_training:
        with open(base_dir / "parameters_training.yaml", "w") as f:
            yaml.dump(params_training, f)
    if params_inference:
        with open(base_dir / "parameters_inference.yaml", "w") as f:
            yaml.dump(params_inference, f)


def _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111"):
    """Create base dataset dir with one train_variant and corresponding latest symlinks."""
    dataset_dir = tmp_path / "data" / "dataset"
    base_dir = dataset_dir / base_v
    train_variant_dir = base_dir / "train_variants" / train_v
    train_variant_dir.mkdir(parents=True)
    (dataset_dir / "latest").symlink_to(base_dir.resolve())
    (base_dir / "train_variants" / "latest").symlink_to(train_variant_dir.resolve())
    return base_dir, train_variant_dir


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "dataset" in result.output
        assert "training" in result.output

    def test_help_shows_options(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "feature_etl" in result.output
        assert "label_etl" in result.output
        assert "sample_pool_etl" in result.output

    def test_etl_subcommands_advertise_target_dates(self):
        for cmd in ("feature_etl", "label_etl", "sample_pool_etl"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, result.output
            assert "--target-dates" in result.output

    def test_unknown_pipeline(self, tmp_path):
        _setup_conf(tmp_path)
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["nonexistent"])
            assert result.exit_code == 2
        finally:
            os.chdir(old_cwd)

    def test_dataset_pipeline_uses_hash_version(self, tmp_path):
        """Dataset pipeline computes hash-based base_dataset_version and train_variant_id."""
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                # train_snap_dates is not optional: the month plans are built
                # before the pipeline runs and preprocessed_feature_table's is
                # derived from the union of every split.
                "train_snap_dates": ["2026-01-31"],
            }},
        )

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch(
                        "recsys_tfb.utils.spark.get_or_create_spark_session",
                        return_value=_mock_spark_with_feature_table_schema(),
                    ):
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(app, ["dataset"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp_pp = call_args["preprocessor"]["filepath"]
                    fp_sk = call_args["sample_keys"]["filepath"]
                    assert "${base_dataset_version}" not in fp_pp
                    assert "${train_variant_id}" not in fp_sk
                    assert re.search(r"data/dataset/[0-9a-f]{8}/preprocessor.pkl", fp_pp)
                    assert re.search(
                        r"data/dataset/[0-9a-f]{8}/train_variants/[0-9a-f]{8}/sample_keys",
                        fp_sk,
                    )
        finally:
            os.chdir(old_cwd)

    def test_training_uses_hash_model_version(self, tmp_path):
        """Training pipeline resolves base + train_variant via latest symlinks."""
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {"sample_ratio": 0.1}},
            params_training={"lr": 0.01},
        )

        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(app, ["training"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["model"]["filepath"]
                    assert "${model_version}" not in fp
                    assert "models/best/" not in fp
                    assert re.search(r"models/[0-9a-f]{8}/", fp)
                    # preprocessor uses base only
                    pp = call_args["preprocessor"]["filepath"]
                    assert "abc12345" in pp
                    # train_model_input uses base + train_variant
                    tmi = call_args["train_model_input"]["filepath"]
                    assert "abc12345" in tmi
                    assert "11111111" in tmi
        finally:
            os.chdir(old_cwd)

    def test_training_auto_injects_cache_source_tables_from_catalog(self, tmp_path):
        """_run_pipeline calls inject_cache_source_tables with substitution_params
        and catalog_config before constructing DataCatalog. Helper itself is
        unit-tested in TestInjectCacheSourceTables; this test only pins the wiring.
        """
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {"sample_ratio": 0.1}},
            params_training={"lr": 0.01},
        )
        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch(
                        "recsys_tfb.__main__.inject_cache_source_tables"
                    ) as mock_inject:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(app, ["training"])

                # Helper called once before DataCatalog instantiation
                assert mock_inject.call_count == 1
                args, kwargs = mock_inject.call_args
                injected_params, injected_catalog = args
                # Both args are dicts
                assert isinstance(injected_params, dict)
                assert isinstance(injected_catalog, dict)
                # injected_catalog has the catalog entries (e.g. train_model_input)
                assert "train_model_input" in injected_catalog
        finally:
            os.chdir(old_cwd)

    def test_training_with_explicit_base_dataset_version(self, tmp_path):
        """Training pipeline accepts --base-dataset-version and --train-variant."""
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {"sample_ratio": 0.1}},
            params_training={"lr": 0.01},
        )

        dataset_dir = tmp_path / "data" / "dataset"
        base_dir = dataset_dir / "deadbeef"
        tv_dir = base_dir / "train_variants" / "cafef00d"
        tv_dir.mkdir(parents=True)

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(
                        app,
                        [
                            "training",
                            "--base-dataset-version", "deadbeef",
                            "--train-variant", "cafef00d",
                        ],
                    )
                    call_args = mock_catalog_cls.call_args[0][0]
                    pp = call_args["preprocessor"]["filepath"]
                    assert "deadbeef" in pp
                    tmi = call_args["train_model_input"]["filepath"]
                    assert "deadbeef" in tmi
                    assert "cafef00d" in tmi
        finally:
            os.chdir(old_cwd)

    def test_inference_uses_actual_model_hash(self, tmp_path):
        """Inference reads base/train_variant from model manifest; outputs under model hash."""
        _setup_conf(
            tmp_path,
            params_inference={"inference": {"snap_dates": ["2024-03-31"]}},
        )

        models_dir = tmp_path / "data" / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        manifest = {
            "version": "a1b2c3d4",
            "base_dataset_version": "deadbeef",
            "train_variant_id": "cafef00d",
        }
        (version_dir / "manifest.json").write_text(json.dumps(manifest))
        (models_dir / "best").symlink_to(version_dir.resolve())

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.__main__.Runner"):
                    runner.invoke(app, ["inference"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    # model read via "best" symlink
                    fp = call_args["model"]["filepath"]
                    assert fp == "data/models/best/model.txt"
                    # preprocessor read via base hash
                    pp = call_args["preprocessor"]["filepath"]
                    assert "deadbeef" in pp
                    # a ${model_version} path resolves to the actual model hash
                    sd = call_args["inference_scratch"]["filepath"]
                    assert "a1b2c3d4" in sd
                    assert "best" not in sd
                    assert "20240331" in sd
        finally:
            os.chdir(old_cwd)

    def test_training_pipeline_fails_without_inputs(self, tmp_path):
        _setup_conf(tmp_path)

        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["training"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)


class TestEvaluationCLIFlags:
    """evaluation subcommand exposes --post-training flag."""

    def test_post_training_flag_in_help(self):
        from typer.testing import CliRunner
        from recsys_tfb.__main__ import app

        runner = CliRunner()
        result = runner.invoke(app, ["evaluation", "--help"])
        assert result.exit_code == 0
        assert "--post-training" in result.output


def _setup_etl_conf(tmp_path, source_checks=None):
    """conf/base + parameters_feature_etl.yaml（最小可跑 _run_etl）。"""
    _setup_conf(tmp_path)
    base_dir = tmp_path / "conf" / "base"
    params = {
        "feature_etl": {
            "variables": {"target_db": "ml_recsys"},
            "source_checks": source_checks or {},
            "tables": [
                {"name": "feature_table", "sql_file": "feature/feature_table.sql",
                 "partition_by": {"snap_date": "DATE"},
                 "primary_key": ["snap_date", "cust_id"]},
            ],
        }
    }
    with open(base_dir / "parameters_feature_etl.yaml", "w") as f:
        yaml.dump(params, f)


class TestSourceCheckCLI:
    def test_flag_in_help(self):
        for cmd in ("feature_etl", "label_etl", "sample_pool_etl"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, result.output
            assert "--source-check" in result.output

    def test_source_check_pass_exit0_no_etl(self, tmp_path):
        _setup_etl_conf(tmp_path, source_checks={"feat_a": {"partition_key": "snap_date"}})
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                inst = MockRunner.return_value
                inst.run_source_checks.return_value = None
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 0, result.output
            inst.run_source_checks.assert_called_once()
            inst.run.assert_not_called()           # no table writes
        finally:
            os.chdir(old)

    def test_source_check_fail_exit1_no_etl(self, tmp_path):
        from recsys_tfb.pipelines.source_etl.sql_runner import SourceCheckError
        from recsys_tfb.pipelines.source_etl.checks import CheckResult
        _setup_etl_conf(tmp_path, source_checks={"feat_a": {"partition_key": "snap_date"}})
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                inst = MockRunner.return_value
                inst.run_source_checks.side_effect = SourceCheckError(
                    [CheckResult(False, "bad", table="feat_a", check="partition_exists",
                                 snap_date="2025-01-31", expected="x", actual="not found")],
                    "feature_etl",
                )
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 1, result.output
            inst.run.assert_not_called()
        finally:
            os.chdir(old)

    def test_source_check_with_restart_from_errors(self, tmp_path):
        _setup_etl_conf(tmp_path)
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--restart-from", "feature_table",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 1, result.output
            MockRunner.return_value.run_source_checks.assert_not_called()
        finally:
            os.chdir(old)

    def test_source_check_forces_dry_run_false(self, tmp_path):
        # In --env local, dry_run defaults to True; --source-check must override
        # it to False so the read-only checks actually query Hive (design D2d).
        _setup_etl_conf(tmp_path, source_checks={"feat_a": {"partition_key": "snap_date"}})
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                MockRunner.return_value.run_source_checks.return_value = None
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 0, result.output
            assert MockRunner.call_args.kwargs["dry_run"] is False
        finally:
            os.chdir(old)


def test_sample_weight_extra_reads_report(tmp_path):
    import json
    from recsys_tfb.__main__ import _sample_weight_extra
    vdir = tmp_path / "models" / "mv"
    vdir.mkdir(parents=True)
    (vdir / "sample_weight_report.json").write_text(
        json.dumps({"enabled": True, "weight_keys": ["prod_name"],
                    "n_weight_entries": 1, "unmatched_keys": []}))
    assert _sample_weight_extra(vdir) == {
        "sample_weight": {"enabled": True, "weight_keys": ["prod_name"],
                          "n_weight_entries": 1, "unmatched_keys": []}}


def test_sample_weight_extra_absent_returns_none(tmp_path):
    from recsys_tfb.__main__ import _sample_weight_extra
    assert _sample_weight_extra(tmp_path) is None


def test_write_manifest_stub_writes_running(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_manifest_stub
    vdir = tmp_path / "ab12cd34"
    _write_manifest_stub(
        vdir,
        {"version": "ab12cd34", "pipeline": "training",
         "parameters": {"training": {"lr": 0.01}},
         "base_dataset_version": "base1234", "train_variant_id": "trv12345"},
        run_id="run-xyz",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m["status"] == "running"
    assert m["run_id"] == "run-xyz"
    assert m["parameters"] == {"training": {"lr": 0.01}}
    assert not (vdir / "latest").exists()           # no symlink
    assert not (vdir / "parameters_training.json").exists()  # no sidecar


def test_write_manifest_stub_skips_if_present(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_manifest_stub
    vdir = tmp_path / "ab12cd34"
    vdir.mkdir()
    (vdir / "manifest.json").write_text(json.dumps(
        {"version": "ab12cd34", "status": "completed", "sentinel": True}))
    _write_manifest_stub(
        vdir,
        {"version": "ab12cd34", "pipeline": "training", "parameters": {}},
        run_id="run-new",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m == {"version": "ab12cd34", "status": "completed", "sentinel": True}


from recsys_tfb.__main__ import (
    _format_node_list,
    _format_slice_plan,
    _make_can_load,
    _slice_extra,
    _slice_pipeline,
)
from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def _slice_test_pipe():
    return Pipeline([
        Node(func=lambda: None, outputs="a", name="A"),
        Node(func=lambda a: None, inputs=["a"], outputs="b", name="B"),
        Node(func=lambda b: None, inputs=["b"], outputs="c", name="C"),
    ])


class TestSlicingHelpers:
    def test_slice_pipeline_mutual_exclusion(self):
        import pytest
        with pytest.raises(ValueError, match="mutually exclusive"):
            _slice_pipeline(_slice_test_pipe(), lambda n: True, "B", "C")

    def test_slice_pipeline_no_flags_passthrough(self):
        pipe = _slice_test_pipe()
        out, plan = _slice_pipeline(pipe, lambda n: True, None, None)
        assert out is pipe
        assert plan is None

    def test_slice_pipeline_from_node(self):
        out, plan = _slice_pipeline(_slice_test_pipe(), lambda n: True, "B", None)
        assert [n.name for n in out.nodes] == ["B", "C"]
        assert plan.mode == "from"

    def test_slice_pipeline_only_node(self):
        out, plan = _slice_pipeline(_slice_test_pipe(), lambda n: True, None, "B")
        assert [n.name for n in out.nodes] == ["B"]
        assert plan.mode == "only"

    def test_format_slice_plan_contents(self):
        _, plan = _slice_pipeline(
            _slice_test_pipe(), lambda n: n == "a", "C", None
        )
        lines = _format_slice_plan(plan, total=3)
        text = "\n".join(lines)
        assert "auto-included" in text
        assert "B" in text and "<- b" in text
        assert "skipped" in text and "A" not in plan.auto_included
        assert "WARNING" in text
        assert "running 2 of 3 nodes" in text

    def test_format_node_list_one_line_per_node(self):
        lines = _format_node_list(_slice_test_pipe(), lambda n: True)
        joined = "\n".join(lines)
        assert all(name in joined for name in ("A", "B", "C"))
        assert len(lines) == 4  # header + one line per node
        assert lines[1].endswith("(+ -)")

    def test_slice_extra(self):
        assert _slice_extra("X", None) == {"resumed_from": "X"}
        assert _slice_extra(None, "Y") == {"only_node": "Y"}
        assert _slice_extra(None, None) is None


class TestSlicingCLIFlags:
    def test_all_four_commands_advertise_slicing_flags(self):
        for cmd in ("dataset", "training", "inference", "evaluation"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0
            out = re.sub(r"\s+", " ", result.output)
            for flag in ("--from-node", "--only-node", "--dry-run", "--list-nodes"):
                assert flag in out, f"{cmd} missing {flag}"


class TestFreshHpoFlag:
    def test_training_help_advertises_fresh_hpo(self):
        result = runner.invoke(app, ["training", "--help"])
        assert result.exit_code == 0
        assert "--fresh-hpo" in result.output


class TestHpoCheckpointingConfig:
    def test_parameters_training_declares_hpo_checkpointing_true(self):
        import yaml as _yaml
        with open("conf/base/parameters_training.yaml") as f:
            cfg = _yaml.safe_load(f)
        assert cfg.get("hpo_checkpointing") is True


def test_write_pipeline_manifest_stamps_completed(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_pipeline_manifest
    vdir = tmp_path / "ab12cd34"
    _write_pipeline_manifest(
        version_dir=vdir,
        metadata_kwargs={"version": "ab12cd34", "pipeline": "training",
                         "parameters": {"lr": 0.01}, "artifacts": ["model"]},
        run_id="run-1",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m["status"] == "completed"
    assert m["artifacts"] == ["model"]


def test_format_retrain_advisory_with_latest():
    from recsys_tfb.__main__ import _format_retrain_advisory
    lines = _format_retrain_advisory(
        "ab12cd34", ["finalize_model", "tune_hyperparameters"],
        ("old11111", "2026-06-01T00:00:00+00:00"))
    text = "\n".join(lines)
    assert "ab12cd34" in text
    assert "finalize_model" in text and "tune_hyperparameters" in text
    assert "old11111" in text
    assert "data/models/old11111/manifest.json" in text


def test_format_retrain_advisory_without_latest():
    from recsys_tfb.__main__ import _format_retrain_advisory
    lines = _format_retrain_advisory("ab12cd34", ["finalize_model"], None)
    text = "\n".join(lines)
    assert "ab12cd34" in text
    assert "finalize_model" in text
    assert "manifest.json" not in text  # no nearest-version section


def _plan_with_auto(auto):
    from recsys_tfb.core.pipeline import SlicePlan
    return SlicePlan(mode="from", requested=("predict_and_write_test_predictions",),
                     auto_included=auto)


def test_maybe_warn_retrain_fires_when_model_pulled_in(tmp_path):
    import json
    from recsys_tfb.__main__ import _maybe_warn_retrain
    (tmp_path / "old11111").mkdir()
    (tmp_path / "old11111" / "manifest.json").write_text(json.dumps(
        {"version": "old11111", "status": "completed",
         "created_at": "2026-06-01T00:00:00+00:00"}))
    plan = _plan_with_auto({"finalize_model": ("model", "best_params")})
    lines = _maybe_warn_retrain(
        plan, {"models_dir": tmp_path, "model_version": "ab12cd34"})
    text = "\n".join(lines)
    assert "ab12cd34" in text and "finalize_model" in text and "old11111" in text


def test_maybe_warn_retrain_fires_under_calibration(tmp_path):
    # Under calibration the `model` dataset is produced by calibrate_model, not
    # finalize_model; the trigger must still fire on the missing `model`.
    from recsys_tfb.__main__ import _maybe_warn_retrain
    plan = _plan_with_auto({"calibrate_model": ("model",)})
    lines = _maybe_warn_retrain(
        plan, {"models_dir": tmp_path, "model_version": "ab12cd34"})
    text = "\n".join(lines)
    assert "ab12cd34" in text and "calibrate_model" in text


def test_maybe_warn_retrain_silent_when_model_present(tmp_path):
    from recsys_tfb.__main__ import _maybe_warn_retrain
    plan = _plan_with_auto({"cache_val_model_input": ("val_model_input",)})
    assert _maybe_warn_retrain(
        plan, {"models_dir": tmp_path, "model_version": "ab12cd34"}) == []


def test_maybe_warn_retrain_silent_without_advice():
    from recsys_tfb.__main__ import _maybe_warn_retrain
    plan = _plan_with_auto({"finalize_model": ("model",)})
    assert _maybe_warn_retrain(plan, None) == []


def test_maybe_warn_retrain_silent_when_plan_none():
    from recsys_tfb.__main__ import _maybe_warn_retrain
    assert _maybe_warn_retrain(None, {"models_dir": ".", "model_version": "x"}) == []


# --- ADR-0002: incremental months + --rebuild-dates ---

class TestRebuildDatesFlag:
    def test_dataset_help_advertises_rebuild_dates(self):
        result = runner.invoke(app, ["dataset", "--help"])
        assert result.exit_code == 0, result.output
        assert "--rebuild-dates" in result.output

    def test_unconfigured_month_exits_before_spark_starts(self, tmp_path):
        # A21 runs before the session is created: a typo must not cost a
        # 2-4 minute Spark cold start before it is reported.
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                "test_snap_dates": ["2026-01-31"],
            }},
        )
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(
                    app, ["dataset", "--rebuild-dates", "2026-09-30"]
                )
            assert result.exit_code == 1
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_configured_month_is_accepted(self, tmp_path):
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                "test_snap_dates": ["2026-01-31"],
            }},
        )
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                runner.invoke(app, ["dataset", "--rebuild-dates", "2026-01-31"])
            # Got past A21 and reached session creation.
            assert mock_spark.called
        finally:
            os.chdir(old_cwd)

    def test_inference_help_advertises_rebuild_dates(self):
        result = runner.invoke(app, ["inference", "--help"])
        assert result.exit_code == 0, result.output
        assert "--rebuild-dates" in result.output

    def test_inference_unconfigured_month_exits_before_spark_starts(self, tmp_path):
        """Scoped to ``inference.snap_dates``, and checked before the cold start."""
        _setup_conf(
            tmp_path,
            params_inference={"inference": {"snap_dates": ["2024-03-31"]}},
        )
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(
                    app, ["inference", "--rebuild-dates", "2026-09-30"]
                )
            assert result.exit_code == 1
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_inference_rebuild_dates_reach_the_pipeline_parameters(self, tmp_path):
        """The flag has to arrive as ``_rebuild_snap_dates`` or it does nothing.

        ``predict_and_write_scores`` reads that key to decide which chunks to
        redo; accepted-but-not-forwarded is the silent no-op A21 guards the
        other half of.
        """
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY

        _setup_conf(
            tmp_path,
            params_inference={"inference": {"snap_dates": ["2024-03-31"]}},
        )
        models_dir = tmp_path / "data" / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        (version_dir / "manifest.json").write_text(json.dumps({
            "version": "a1b2c3d4",
            "base_dataset_version": "deadbeef",
            "train_variant_id": "cafef00d",
        }))
        (models_dir / "best").symlink_to(version_dir.resolve())

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            added: dict = {}
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda name, ds: added.__setitem__(name, ds)
                with patch("recsys_tfb.__main__.Runner"):
                    result = runner.invoke(
                        app, ["inference", "--rebuild-dates", "2024-03-31"]
                    )
            assert result.exit_code == 0, result.output
            params = added["parameters"].load()
            assert params[REBUILD_SNAP_DATES_KEY] == ["2024-03-31"]
        finally:
            os.chdir(old_cwd)


#: A base_dataset_version that is never this run's. Partitions stamped with it
#: stand in for months left behind by an earlier, differently-configured run.
_FOREIGN_VERSION = "deadbeef"


def _run_dataset_command(
    tmp_path, argv, existing=("2026-01-31",), foreign=("2026-02-28",),
):
    """Invoke the dataset command far enough to build the catalog.

    Returns ``(loaded, seen)``: everything added to the catalog, already
    ``load()``-ed, and the substitution params the catalog was built from.

    ``existing`` months are listed under this run's version, ``foreign`` ones
    under :data:`_FOREIGN_VERSION`; only the former may count as landed.

    Only ``test_model_input`` is a Hive table in this catalog, so it is the only
    artifact whose partitions can be listed; the other two fall back to "nothing
    has landed". That asymmetry is the point — it makes the three plans differ,
    so a test cannot pass by handing out the same plan thrice.

    Deliberately *not* mocked: ``DataCatalog`` and ``HiveTableDataset``. The
    listing goes through a real catalog object whose ``partition_filter`` is a
    real ``${base_dataset_version}`` template, so the substitution the CLI feeds
    the catalog is exercised rather than assumed. ``add`` is spied on rather than
    replaced, which is what lets the plans be read back.
    """
    _setup_conf(
        tmp_path,
        params_dataset={"dataset": {
            "sample_ratio": 0.1,
            "train_dev_ratio": 0.2,
            "train_snap_dates": ["2025-12-31"],
            "test_snap_dates": ["2026-01-31", "2026-02-28"],
        }},
    )
    catalog_path = tmp_path / "conf" / "base" / "catalog.yaml"
    with open(catalog_path) as f:
        catalog = yaml.safe_load(f)
    catalog["test_model_input"] = {
        "type": "HiveTableDataset",
        "database": "ml_recsys",
        "table": "recsys_prod_test_model_input",
        # The four keys below are copied from the real entry in
        # conf/base/catalog.yaml; partition_filter is the one under test.
        "external": False,
        "columns": "auto",
        "partition_filter": {"base_dataset_version": "${base_dataset_version}"},
        "partition_cols": [{"name": "snap_date", "type": "STRING"}],
    }
    with open(catalog_path, "w") as f:
        yaml.dump(catalog, f)

    seen = _CatalogSubstitutionSpy()
    spark = _mock_spark_with_feature_table_schema()

    def _show_partitions(_query):
        seen.note_listing()
        return _partition_rows(
            # Stamped with whatever base_dataset_version the CLI actually
            # substituted into the catalog, so a CLI that built this catalog
            # before computing the version keeps none of these.
            [
                f"base_dataset_version={seen.base_dataset_version}/snap_date={d}"
                for d in existing
            ]
            # A month that landed under a DIFFERENT base version. Reporting it
            # as landed is the unsafe direction — that month would be skipped
            # although nothing was ever written for *this* version. Kept in the
            # listing for every run here so the plan assertions below all carry
            # the guarantee, not just the one test named for it.
            + [f"base_dataset_version={_FOREIGN_VERSION}/snap_date={d}"
               for d in foreign]
        )

    spark.sql.side_effect = _show_partitions

    added = {}
    real_add = DataCatalog.add

    def spy_add(catalog_self, name, dataset):
        added[name] = dataset
        real_add(catalog_self, name, dataset)

    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        with patch.object(DataCatalog, "add", spy_add), \
                seen.patch(), \
                patch(
                    "recsys_tfb.utils.spark.get_or_create_spark_session",
                    return_value=spark,
                ), \
                patch("recsys_tfb.__main__.Runner"):
            result = runner.invoke(app, argv)
    finally:
        os.chdir(old_cwd)
    assert result.exit_code == 0, result.output
    return {name: ds.load() for name, ds in added.items()}, seen


class TestMonthPlansReachTheCatalog:
    """#152 — the plans travel as catalog datasets, not as ``parameters`` keys.

    This is the seam the whole feature hangs on. Forgetting it no longer fails
    silently (the runner refuses to start a node whose input is absent), but a
    plan wired to the *wrong* dataset name would still start — so assert the
    three by name and by content.
    """

    def _run_dataset(self, tmp_path, argv, **kwargs):
        loaded, _ = _run_dataset_command(tmp_path, argv, **kwargs)
        return loaded

    def test_every_incremental_dataset_gets_its_own_plan(self, tmp_path):
        from recsys_tfb.pipelines.dataset.month_plans import (
            INCREMENTAL_DATASETS, month_plan_input,
        )

        loaded = self._run_dataset(tmp_path, ["dataset"])

        assert {month_plan_input(d) for d in INCREMENTAL_DATASETS} <= set(loaded)
        # test_model_input's month landed -> skipped; test_keys was never listed
        # -> both months; preprocessed_feature_table spans train ∪ test.
        assert loaded["test_model_input_month_plan"].to_process == [
            pd.Timestamp("2026-02-28")
        ]
        assert loaded["test_keys_month_plan"].to_process == [
            pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28"),
        ]
        assert loaded["preprocessed_feature_table_month_plan"].to_process == [
            pd.Timestamp("2025-12-31"),
            pd.Timestamp("2026-01-31"),
            pd.Timestamp("2026-02-28"),
        ]

    def test_rebuild_dates_reach_the_plan(self, tmp_path):
        loaded = self._run_dataset(
            tmp_path, ["dataset", "--rebuild-dates", "2026-01-31"],
        )
        assert loaded["test_model_input_month_plan"].to_process == [
            pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28"),
        ]
        assert loaded["test_model_input_month_plan"].skipped == []

    def test_parameters_carry_settings_only(self, tmp_path):
        """The ``_existing_snap_dates`` side channel is gone.

        ``_rebuild_snap_dates`` stays: it is a user-supplied setting (and the
        training pipeline reads it), unlike a metastore listing.
        """
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY

        loaded = self._run_dataset(
            tmp_path, ["dataset", "--rebuild-dates", "2026-01-31"],
        )
        params = loaded["parameters"]
        assert params[REBUILD_SNAP_DATES_KEY] == ["2026-01-31"]
        assert not [k for k in params if "existing" in k.lower()]


from recsys_tfb.pipelines import get_pipeline
from recsys_tfb.pipelines.dataset.month_plans import (
    INCREMENTAL_DATASETS,
    SnapDatePlan,
    month_plan_input,
)


class _FakeCatalog:
    """A catalog that answers ``exists`` from a fixed set of names.

    Records every call so memoization can be asserted on: the real
    ``HiveTableDataset.exists`` is a metastore round-trip and slice probing asks
    the same names once per node.
    """

    def __init__(self, present):
        self.present = set(present)
        self.calls = []

    def exists(self, name):
        self.calls.append(name)
        return name in self.present


def _plan(to_process=(), skipped=()):
    return SnapDatePlan(to_process=list(to_process), skipped=list(skipped))


_A_MONTH = pd.Timestamp("2026-02-28")


class TestMonthAwareCanLoad:
    """#202 — the slice's stopping condition asks the right question.

    ``exists()`` answers "is the table there", and for the three incremental
    artifacts the answer is always yes: they are extended a month at a time, so
    what a run can be missing is *this run's months*, not the table. See
    ADR-0012 for why the month is asked here rather than inside the io object.
    """

    def test_pending_months_make_an_existing_artifact_unloadable(self):
        catalog = _FakeCatalog(["test_keys"])
        can_load = _make_can_load(catalog, {"test_keys": _plan([_A_MONTH])})
        assert can_load("test_keys") is False

    def test_nothing_pending_falls_back_to_exists(self):
        catalog = _FakeCatalog(["test_keys"])
        can_load = _make_can_load(catalog, {"test_keys": _plan(skipped=[_A_MONTH])})
        assert can_load("test_keys") is True

    def test_an_empty_plan_cannot_conjure_an_artifact_that_is_not_there(self):
        """The month check only ever *subtracts* from what ``exists`` allows."""
        catalog = _FakeCatalog([])
        can_load = _make_can_load(catalog, {"test_keys": _plan(skipped=[_A_MONTH])})
        assert can_load("test_keys") is False

    def test_a_dataset_with_no_plan_is_answered_by_exists_alone(self):
        catalog = _FakeCatalog(["preprocessor"])
        can_load = _make_can_load(catalog, {"test_keys": _plan([_A_MONTH])})
        assert can_load("preprocessor") is True
        assert can_load("train_model_input") is False

    def test_no_plans_at_all_is_exactly_exists(self):
        """training / inference / evaluation pass none — their behaviour is the
        pre-#202 behaviour, byte for byte."""
        catalog = _FakeCatalog(["model"])
        can_load = _make_can_load(catalog)
        assert can_load("model") is True
        assert can_load("test_keys") is False

    def test_exists_is_asked_once_per_name(self):
        catalog = _FakeCatalog(["model"])
        can_load = _make_can_load(catalog)
        assert [can_load("model"), can_load("model")] == [True, True]
        assert catalog.calls == ["model"]


#: Every dataset name ``conf/base/catalog.yaml`` persists — read from the real
#: file rather than listed here, because the whole question below is which
#: inputs a slice can satisfy from storage, and a hand-written list that drifts
#: from the catalog would answer it wrongly while staying green.
_PERSISTED = set(
    yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "conf" / "base" / "catalog.yaml")
        .read_text()
    )
)

#: What a run of the dataset pipeline can satisfy from storage after a previous
#: full run: everything the catalog persists, plus the two kinds of entry the
#: CLI adds itself (``parameters`` and one month plan per incremental artifact).
_LANDED = (
    _PERSISTED
    | {"parameters"}
    | {month_plan_input(name) for name in INCREMENTAL_DATASETS}
)

#: The node set issue #202 names: the test chain's four nodes. Spelled out
#: rather than derived, because a derivation would be the same walk the code
#: under test performs.
_TEST_CHAIN = {
    "filter_test_model_input",
    "build_test_model_input",
    "select_test_keys",
    "apply_preprocessor_to_features",
}


class TestANewMonthPullsBackTheProducersThatOwnIt:
    """The node set ``--only-node filter_test_model_input`` derives (#202).

    Same pipeline definition and same slicing code the real run uses, with a
    catalog stubbed to the truth of ``catalog.yaml`` — a fast proxy for the
    ticket's real-run criterion, not a substitute for it (that run is recorded
    in the PR). What changes between the two cases is only the month plan.
    """

    def _sliced(self, pending):
        pipe = get_pipeline("dataset", enable_calibration=True)
        plans = {name: _plan(pending) for name in INCREMENTAL_DATASETS}
        _, plan = pipe.slice_only(
            "filter_test_model_input", _make_can_load(_FakeCatalog(_LANDED), plans)
        )
        return plan

    def test_a_pending_month_reaches_the_two_persistent_producers(self):
        plan = self._sliced([_A_MONTH])

        # Exact set, so it also pins what stays out: `fit_preprocessor_metadata`
        # is absent because `preprocessor` is a landed JSON with no months of
        # its own. A bare `not in` for that would pass even if the slice
        # collapsed entirely.
        assert set(plan.requested) | set(plan.auto_included) == _TEST_CHAIN
        # Named per producer: the artifact that pulled it back is what the
        # [plan] line shows the operator.
        assert plan.auto_included["select_test_keys"] == ("test_keys",)
        assert plan.auto_included["apply_preprocessor_to_features"] == (
            "preprocessed_feature_table",
        )

    def test_with_no_pending_month_the_chain_stops_one_hop_up(self):
        """The pre-#202 behaviour, kept as the negative control.

        ``test_model_input_unfiltered`` is not in ``catalog.yaml`` (the runner
        makes it a MemoryDataset), so its producer is always pulled back; the
        chain then stopped at ``test_keys``, a persistent Hive table that
        ``exists()`` reports as present whatever months it holds.
        """
        plan = self._sliced([])

        assert set(plan.requested) | set(plan.auto_included) == {
            "filter_test_model_input",
            "build_test_model_input",
        }


class TestTheDatasetCommandWiresThePlansIntoTheSlice:
    """The seam between the plans and the stopping condition.

    ``_make_can_load`` can be right and this can still be wrong: the plans are
    built for the *nodes* (ADR-0007) and passing them to the slice as well is a
    separate line. Without this test, forgetting it leaves every unit test above
    green and the silent defect intact.
    """

    def test_only_node_filter_test_model_input_pulls_back_both_producers(
        self, tmp_path
    ):
        captured = {}
        real = _format_slice_plan

        def spy(plan, total):
            captured["plan"] = plan
            return real(plan, total)

        # Every persistent artifact is present, including the two incremental
        # Hive tables: without the month check there is nothing left to pull
        # their producers back with, which is the defect under test.
        with patch.object(
            DataCatalog, "exists", lambda self, name: name in _LANDED
        ), patch("recsys_tfb.__main__._format_slice_plan", spy):
            _run_dataset_command(
                tmp_path,
                ["dataset", "--only-node", "filter_test_model_input", "--dry-run"],
                # Only `test_model_input` is a Hive table in that fixture's
                # catalog, so only its plan reflects `existing` — the other two
                # have every configured month pending regardless. So this is the
                # eval-month scenario for one artifact and a first run for the
                # other two; what it pins is the wiring, not the arithmetic
                # (the plans themselves are asserted in
                # TestMonthPlansReachTheCatalog).
                existing=("2026-01-31",),
                foreign=(),
            )

        plan = captured["plan"]
        assert set(plan.requested) | set(plan.auto_included) == _TEST_CHAIN


def _execute_pipeline_call_sites() -> dict[str, set[str]]:
    """``{enclosing function: keyword names}`` for every ``_execute_pipeline``
    call in ``__main__.py``, read off the AST."""
    tree = ast.parse(
        (Path(__file__).resolve().parents[1] / "src" / "recsys_tfb" / "__main__.py")
        .read_text()
    )
    sites: dict[str, set[str]] = {}
    for func in ast.walk(tree):
        if not isinstance(func, ast.FunctionDef):
            continue
        for call in ast.walk(func):
            if (
                isinstance(call, ast.Call)
                and getattr(call.func, "id", None) == "_execute_pipeline"
            ):
                sites[func.name] = {kw.arg for kw in call.keywords}
    return sites


class TestOnlyTheDatasetCommandIsMonthAware:
    """#202's "其他三個 pipeline 的行為完全不變", pinned at the call sites.

    ``_make_can_load`` defaulting to ``month_plans=None`` is not the guarantee —
    it is only what makes the guarantee cheap to keep. Handing plans to another
    command would narrow that command's slices for a reason its operator never
    declared, and every other test in this file stays green when you do
    (verified: injecting a plan into the ``training`` call site changes nothing
    else). So assert the call sites themselves.
    """

    def test_month_plans_is_passed_by_the_dataset_command_alone(self):
        sites = _execute_pipeline_call_sites()

        # Guard the walk before trusting its answer: a rename that made the
        # search find nothing would satisfy the assertion below vacuously.
        assert set(sites) == {"dataset", "training", "inference", "evaluation"}
        assert {
            name for name, kwargs in sites.items() if "month_plans" in kwargs
        } == {"dataset"}


class TestThePartitionListingIsVersionScoped:
    """Both directions of "which version did this month land under".

    Nothing else in the suite catches either one: the config diff is empty, the
    DAG is unchanged, and every other test here passes both ways. The listing no
    longer filters by version itself — the catalog entry's ``partition_filter``
    does — so what is asserted is that the CLI hands that entry a resolved
    version and that the resulting scope actually holds.
    """

    def test_substitution_params_carry_the_resolved_version(self, tmp_path):
        # ADR-0008 §2's ordering constraint. Substitution is a plain string
        # .replace(), so an unfilled ${base_dataset_version} survives as that
        # literal and raises nothing; a catalog built before the version exists
        # lists zero partitions and every month looks unlanded.
        _, seen = _run_dataset_command(tmp_path, ["dataset"])

        # The config the partition listing was asked through — not merely some
        # config this run built — carried a resolved 8-hex version, rather than
        # the literal template or the absent key that leaves it behind.
        assert seen.at_listing is not None, "no partition listing happened"
        assert re.fullmatch(
            r"[0-9a-f]{8}", seen.at_listing.get("base_dataset_version", ""),
        ), seen.at_listing

    def test_a_landed_month_is_skipped_not_rebuilt(self, tmp_path):
        # The consequence of the above, end to end: the listed partition is
        # stamped with the version the CLI substituted, so it survives the
        # entry's partition_filter only if that substitution resolved. An
        # unresolved one drops every partition and reports nothing skipped.
        loaded, _ = _run_dataset_command(tmp_path, ["dataset"])
        assert loaded["test_model_input_month_plan"].skipped == [
            pd.Timestamp("2026-01-31")
        ]

    def test_a_month_under_another_version_does_not_count_as_landed(self, tmp_path):
        # The unsafe direction, and the one this ticket's deletions put at risk:
        # a month written by an earlier, differently-configured run must still
        # be processed, not skipped as though it existed for this version.
        loaded, _ = _run_dataset_command(
            tmp_path, ["dataset"], existing=(), foreign=("2026-01-31", "2026-02-28"),
        )
        plan = loaded["test_model_input_month_plan"]
        assert plan.skipped == []
        assert plan.to_process == [
            pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28"),
        ]


class TestCollectExistingSnapDates:
    def _catalog(self, listings):
        catalog = DataCatalog()
        for name, specs in listings.items():
            dataset = MagicMock()
            dataset.existing_partition_values.return_value = specs
            catalog.add(name, dataset)
        return catalog

    def test_asks_each_dataset_object_for_its_own_partitions(self):
        from recsys_tfb.__main__ import _collect_existing_snap_dates

        out = _collect_existing_snap_dates(
            self._catalog({
                "test_keys": [{"as_of": "2026-01-31"}],
                "test_model_input": [
                    {"as_of": "2026-02-28", "prod_name": "fund_stock"},
                ],
            }),
            time_col="as_of",
        )

        # time_col is threaded through, not hardcoded: the framework's time
        # column is configurable via schema.time.
        assert out == {
            "test_keys": ["2026-01-31"],
            "test_model_input": ["2026-02-28"],
        }

    def test_a_dataset_that_cannot_list_partitions_is_rebuilt_in_full(self, caplog):
        from recsys_tfb.__main__ import _collect_existing_snap_dates

        catalog = self._catalog({"test_keys": [{"snap_date": "2026-01-31"}]})
        # A ParquetDataset has no existing_partition_values; absent from the
        # result means build_month_plans reads it as "nothing has landed".
        catalog.add("preprocessed_feature_table", SimpleNamespace())

        with caplog.at_level(logging.WARNING):
            out = _collect_existing_snap_dates(catalog)

        # Exact, not "not in": an absent key and a `[]` value are the same
        # answer to build_month_plans but not the same behaviour here, and
        # `test_model_input` (registered nowhere at all) must take the same
        # route rather than raising.
        assert out == {"test_keys": ["2026-01-31"]}
        # Asserted because a silent skip is what makes this dangerous: the run
        # rebuilds a whole artifact and only this line says why.
        assert "preprocessed_feature_table" in caplog.text


class TestRebuildSliceWarning:
    def test_names_both_flags_and_the_months(self):
        from recsys_tfb.__main__ import _format_rebuild_slice_warning

        lines = "\n".join(_format_rebuild_slice_warning(["2026-01-31"]))
        assert "--rebuild-dates" in lines
        assert "--only-node" in lines
        assert "2026-01-31" in lines


# --- #130: training also takes --rebuild-dates (same A21 predicate) ---

class TestTrainingRebuildDatesFlag:
    def test_training_help_advertises_rebuild_dates(self):
        result = runner.invoke(app, ["training", "--help"])
        assert result.exit_code == 0, result.output
        assert "--rebuild-dates" in result.output

    def test_unconfigured_month_exits_before_spark_starts(self, tmp_path):
        """Same guard, same predicate, same timing as the dataset command: a
        month the config never listed cannot be rebuilt, and finding that out
        must not cost a Spark cold start.
        """
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "test_snap_dates": ["2026-01-31"],
            }},
            params_training={"lr": 0.01},
        )
        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(
                    app, ["training", "--rebuild-dates", "2026-09-30"]
                )
            assert result.exit_code == 1
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_rebuild_months_reach_node_parameters(self, tmp_path):
        """The cache and predict nodes read the months off ``parameters``;
        without this wiring the flag parses, validates, and does nothing.
        """
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY

        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "test_snap_dates": ["2026-01-31", "2026-02-28"],
            }},
            params_training={"lr": 0.01},
        )
        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

        captured = {}

        def _capture(data=None):
            captured["params"] = data
            return MagicMock()

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch("recsys_tfb.__main__.MemoryDataset", side_effect=_capture), \
                    patch("recsys_tfb.__main__.Runner"):
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                runner.invoke(app, ["training", "--rebuild-dates", "2026-02-28"])
        finally:
            os.chdir(old_cwd)

        assert captured["params"][REBUILD_SNAP_DATES_KEY] == ["2026-02-28"]


class TestRebuildSlicedAwayWarning:
    """``--rebuild-dates`` with a slicing flag is the normal path here, so it
    warns only when the slice drops the node the flag drives — the one case
    where the run succeeds and silently does nothing it was asked to do.
    """

    @staticmethod
    def _pipe(*node_names):
        return SimpleNamespace(
            nodes=[SimpleNamespace(name=n) for n in node_names]
        )

    def test_silent_when_the_predict_node_is_in_the_slice(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        pipe = self._pipe("cache_test_model_input", "predict_and_write_test_predictions")
        assert _maybe_warn_rebuild_sliced_away(
            pipe, {"rebuild": ["2026-01-31"]}
        ) == []

    def test_warns_and_names_the_months_when_predict_is_sliced_away(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        lines = "\n".join(
            _maybe_warn_rebuild_sliced_away(
                self._pipe("compute_feature_importance"),
                {"rebuild": ["2026-01-31"]},
            )
        )
        assert "--rebuild-dates" in lines
        assert "2026-01-31" in lines
        assert "predict_and_write_test_predictions" in lines

    def test_silent_when_the_flag_was_not_passed(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        assert _maybe_warn_rebuild_sliced_away(
            self._pipe("compute_feature_importance"), {"rebuild": []}
        ) == []

    def test_the_named_nodes_exist_in_the_real_training_pipeline(self):
        """The tests above hand it a fake pipeline, so a node rename would
        leave them green while the warning silently never fires again (and the
        runbook's `--only-node` command stops matching anything). Pin the names
        against the pipeline itself.
        """
        from recsys_tfb.__main__ import _REBUILD_TARGET_NODES
        from recsys_tfb.pipelines import get_pipeline

        real = {node.name for node in get_pipeline("training").nodes}
        assert set(_REBUILD_TARGET_NODES) <= real


class TestDateSplitOverlapA24:
    """A24 is wired to the dataset command, not to the global aggregator."""

    def test_overlapping_splits_exit_before_spark_starts(self, tmp_path):
        # Like A21: a config error must not cost a 2-4 minute cold start.
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                "train_snap_dates": ["2026-01-31", "2026-02-28"],
                "val_snap_dates": ["2026-02-28"],
                "test_snap_dates": ["2026-03-31"],
            }},
        )
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(app, ["dataset"])
            assert result.exit_code == 1
            # The load-bearing assertion: exit_code alone is satisfied by the
            # mocked session blowing up further down the command.
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_command_whose_config_has_no_dataset_block_is_unaffected(self, tmp_path):
        # source_etl / inference configs never set these keys at all. A24 must
        # treat four absent splits as disjoint, not as something to complain
        # about — the required-key question is A23's (issue #158), and letting
        # it leak in here would block every non-dataset command.
        #
        # Honest about its power: this is a scenario guard, not a
        # mutation-sensitive one. feature_etl never calls A24, so no change to
        # the predicate alone can redden it. The test that actually
        # discriminates is its sibling below (an overlapping dataset block
        # still lets feature_etl run) — that one goes red the moment A24 is
        # aggregated into validate_config_consistency.
        _setup_etl_conf(tmp_path)
        assert not (tmp_path / "conf" / "base" / "parameters_dataset.yaml").exists()
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                MockRunner.return_value.run_source_checks.return_value = None
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 0, result.output
        finally:
            os.chdir(old)

    def test_other_commands_run_with_an_overlapping_dataset_block(self, tmp_path):
        # ConfigLoader merges every file in conf/base, so a feature_etl run
        # sees the dataset params too. A24 must not reach it: #158 measured
        # what putting a dataset-only predicate in the global aggregator does
        # (9 unrelated tests blocked). This config would fail `dataset`.
        _setup_etl_conf(tmp_path)
        with open(tmp_path / "conf" / "base" / "parameters_dataset.yaml", "w") as f:
            yaml.dump({"dataset": {
                "train_snap_dates": ["2026-01-31"],
                "val_snap_dates": ["2026-01-31"],
            }}, f)
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.pipelines.source_etl.sql_runner.SQLRunner") as MockRunner:
                MockRunner.return_value.run_source_checks.return_value = None
                result = runner.invoke(
                    app, ["feature_etl", "--source-check",
                          "--target-dates", "2025-01-31"])
            assert result.exit_code == 0, result.output
        finally:
            os.chdir(old)


# --- #203: --only-test-months, the named slice for adding an eval month ---

from recsys_tfb.__main__ import (
    _ONLY_TEST_MONTHS,
    _landing_nodes,
    _maybe_warn_rebuild_partial_chain,
    NamedSlice,
)
from recsys_tfb.pipelines.dataset.pipeline import ONLY_TEST_MONTHS_NODES

#: What ``--only-test-months`` must come out to: the test chain (#202's four)
#: plus the Layer-2 data gate. Spelled out rather than derived — a derivation
#: would be the same walk the code under test performs.
_ONLY_TEST_MONTHS_SLICE = _TEST_CHAIN | {"validate_data_consistency"}

#: "argument not given", so a test can pass ``advice=None`` and mean it.
_UNSET = object()


def _dataset_pipe():
    return get_pipeline("dataset", enable_calibration=True)


def _pending_can_load(pending=(_A_MONTH,)):
    """The stopping condition of a run that has a month left to process."""
    return _make_can_load(
        _FakeCatalog(_LANDED),
        {name: _plan(pending) for name in INCREMENTAL_DATASETS},
    )


class TestOnlyTestMonthsPreset:
    """The preset names two nodes; the DAG supplies the rest.

    Hard-coding the five would make the preset a second list to keep in step
    with the pipeline definition — the failure #202 was about, one layer up.
    """

    def test_the_preset_names_two_nodes_the_pipeline_actually_has(self):
        # Guard the names themselves: every assertion below is about a slice
        # taken *through* them, and `_node_index` raising on a stale name would
        # be an error, not a red assertion.
        assert len(ONLY_TEST_MONTHS_NODES) == 2
        assert set(ONLY_TEST_MONTHS_NODES) <= {n.name for n in _dataset_pipe().nodes}

    def test_it_expands_to_the_test_chain_plus_the_data_gate(self):
        sliced, plan = _dataset_pipe().slice_nodes(
            ONLY_TEST_MONTHS_NODES, _pending_can_load()
        )

        assert {n.name for n in sliced.nodes} == _ONLY_TEST_MONTHS_SLICE
        assert len(sliced.nodes) == 5
        # Exact set, so it pins what stays out too: the ten nodes that would
        # recompute train/val/calibration byte-for-byte are the whole point.
        assert "fit_preprocessor_metadata" not in {n.name for n in sliced.nodes}
        assert plan.auto_included["select_test_keys"] == ("test_keys",)

    def test_the_zero_output_data_gate_only_gets_in_by_being_named(self):
        """Why the gate is the second hard-coded name and not derived.

        ``_slice_with_expansion`` builds its producer map from ``node.outputs``,
        so a node with none can never be pulled back by expansion — no input is
        ever missing "because ``validate_data_consistency`` did not run". Being
        requested is the only way in, and leaving it out would quietly take the
        Layer-2 data gate off the new eval-month path (#157).
        """
        sliced, plan = _dataset_pipe().slice_nodes(
            ONLY_TEST_MONTHS_NODES, _pending_can_load()
        )

        assert "validate_data_consistency" in plan.requested
        assert "validate_data_consistency" in {n.name for n in sliced.nodes}
        assert "validate_data_consistency" not in plan.skipped_side_effect

    def test_with_nothing_pending_it_stops_at_the_landed_tables(self):
        """The negative control: the preset is not a disguised full run."""
        sliced, _ = _dataset_pipe().slice_nodes(
            ONLY_TEST_MONTHS_NODES, _pending_can_load(())
        )

        assert {n.name for n in sliced.nodes} == {
            "validate_data_consistency",
            "build_test_model_input",
            "filter_test_model_input",
        }


class TestSlicingFlagsAreThreeWayExclusive:
    """``--from-node`` / ``--only-node`` / ``--only-test-months``.

    Two of the three were already exclusive. A third selector that silently
    lost to one of the others would run a node set nobody asked for.
    """

    def test_every_pair_is_rejected(self):
        import pytest

        pipe = _slice_test_pipe()
        preset = NamedSlice("demo-preset", ("A",))
        for from_node, only_node, given in (
            ("B", "C", None),
            ("B", None, preset),
            (None, "C", preset),
            ("B", "C", preset),
        ):
            with pytest.raises(ValueError, match="mutually exclusive") as exc:
                _slice_pipeline(pipe, lambda n: True, from_node, only_node, given)
            # Names only the flags actually passed. Three of the four commands
            # have no preset at all, so a message assembled from a default
            # would send their operators after a flag that does not exist.
            named = str(exc.value)
            assert ("--demo-preset" in named) is (given is not None)

    def test_the_preset_alone_slices(self):
        out, plan = _slice_pipeline(
            _slice_test_pipe(), lambda n: True, None, None,
            NamedSlice("demo-preset", ("A", "C")),
        )
        assert [n.name for n in out.nodes] == ["A", "C"]
        # The mode is the flag as typed, because `[plan] mode=...` is read by
        # someone checking that the flag they used did what they meant.
        assert plan.mode == "demo-preset"

    def test_the_manifest_records_which_selector_ran(self):
        assert _slice_extra(None, None, _ONLY_TEST_MONTHS) == {
            "preset": "only-test-months"
        }
        assert _slice_extra("X", None) == {"resumed_from": "X"}
        assert _slice_extra(None, None) is None


class TestLandingNodes:
    """Which nodes the rebuild warning counts as "can leave a partition".

    ``dataset`` has no ``writes=`` node, so on that pipeline alone the
    ``or n.writes`` half of the predicate is dead and deleting it stays green.
    The helper is reached with whatever pipeline the calling command has, and
    two of them do have such nodes (architecture-constraints R1), so the case
    is pinned here on a pipeline built for it.
    """

    def test_a_writes_only_node_counts_as_landing(self):
        pipe = Pipeline([
            Node(func=lambda: None, outputs="a", name="produces"),
            Node(func=lambda a, t: None, inputs=["a"], writes=["t"], name="saves"),
            Node(func=lambda a: None, inputs=["a"], outputs=None, name="gate"),
        ])

        # `saves` returns nothing for the Runner to store but writes `t` itself;
        # counting it with `gate` would let a genuinely stale partition pass.
        assert _landing_nodes(pipe) == {"produces", "saves"}


class TestRebuildWarningIsConditionalOnTheChain:
    """#203 — the dataset side warned on "sliced at all", which the preset makes
    a false alarm: the slice it selects *is* the whole chain, and the message
    told the operator to re-run without the flag, i.e. to do the ten nodes of
    pure rework the flag exists to skip. The condition is now "is any of the
    chain missing from this run".
    """

    def _advice(self, rebuild=("2026-01-31",)):
        return {
            "rebuild": list(rebuild),
            "nodes": ONLY_TEST_MONTHS_NODES,
            "chain": "test 鏈",
        }

    def _warn(self, from_node=None, only_node=None, preset=None, advice=_UNSET):
        pipe = _dataset_pipe()
        can_load = _pending_can_load()
        sliced, plan = _slice_pipeline(
            pipe, can_load, from_node, only_node, preset
        )
        return _maybe_warn_rebuild_partial_chain(
            pipe, sliced, plan, can_load,
            self._advice() if advice is _UNSET else advice,
        )

    def test_the_preset_covers_the_chain_so_it_stays_quiet(self):
        assert self._warn(preset=_ONLY_TEST_MONTHS) == []

    def test_a_from_node_that_covers_the_chain_stays_quiet_too(self):
        # The same correctness, arrived at differently: --from-node on the
        # first node selects everything, so nothing in the chain is stale.
        assert self._warn(from_node="validate_data_consistency") == []

    def test_a_slice_that_drops_part_of_the_chain_still_warns(self):
        lines = "\n".join(self._warn(only_node="build_test_model_input"))

        assert "--rebuild-dates" in lines
        assert "2026-01-31" in lines
        assert "test 鏈" in lines

    def test_a_slice_missing_only_the_gate_stays_quiet(self):
        """The gate lands no partition, so its absence cannot make one stale.

        ``--from-node filter_test_model_input`` pulls back all four data nodes
        of the chain but not the zero-output gate (nothing can). Warning there
        would restate this ticket's own bug one node over: a message about
        stale partitions, fired by a node that writes none. The skipped gate is
        already reported accurately, by the ``skipped side-effect`` line.
        """
        assert self._warn(from_node="filter_test_model_input") == []

    def test_an_unsliced_run_never_warns(self):
        assert self._warn() == []

    def test_no_rebuild_dates_means_nothing_to_warn_about(self):
        assert self._warn(
            only_node="build_test_model_input", advice=self._advice(rebuild=())
        ) == []

    def test_other_pipelines_pass_no_advice_and_are_untouched(self):
        assert self._warn(only_node="build_test_model_input", advice=None) == []


class TestOnlyTestMonthsCLI:
    def test_only_the_dataset_command_advertises_it(self):
        for cmd in ("dataset", "training", "inference", "evaluation"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, result.output
            out = re.sub(r"\s+", " ", result.output)
            assert ("--only-test-months" in out) is (cmd == "dataset"), cmd

    def test_it_plans_five_of_fifteen_nodes(self, tmp_path):
        """The ticket's ``--dry-run`` criterion, against the real command.

        The unit tests above slice a pipeline object; this one pins the wiring
        that carries the preset from the flag to the slice, which no amount of
        node-set assertion can catch.
        """
        captured = {}
        real = _format_slice_plan

        def spy(plan, total):
            captured["plan"] = plan
            captured["total"] = total
            return real(plan, total)

        with patch.object(
            DataCatalog, "exists", lambda self, name: name in _LANDED
        ), patch("recsys_tfb.__main__._format_slice_plan", spy):
            _run_dataset_command(
                tmp_path,
                ["dataset", "--only-test-months", "--dry-run"],
                existing=("2026-01-31",),
                foreign=(),
            )

        plan = captured["plan"]
        assert set(plan.requested) | set(plan.auto_included) == _ONLY_TEST_MONTHS_SLICE
        assert plan.mode == "only-test-months"

    def test_the_shipped_config_makes_that_five_of_fifteen(self):
        """The ticket's headline number, tied to the config that produces it.

        ``enable_calibration`` is what makes the pipeline fifteen nodes rather
        than thirteen, and it is on in ``conf/base`` — so the two thirds of the
        run this flag skips is the shipped default, not a scenario. The CLI test
        above uses a fixture with calibration off; asserting 15 there would pin
        the fixture, not the claim.
        """
        params = yaml.safe_load(
            (Path(__file__).resolve().parents[1]
             / "conf" / "base" / "parameters_dataset.yaml").read_text()
        )
        assert params["dataset"]["enable_calibration"] is True

        pipe = _dataset_pipe()
        sliced, plan = pipe.slice_nodes(ONLY_TEST_MONTHS_NODES, _pending_can_load())
        assert (len(sliced.nodes), len(pipe.nodes)) == (5, 15)
        # The literal line the ticket names, not just the arithmetic behind it:
        # that string is what an operator checks before committing to the run.
        assert "[plan] running 5 of 15 nodes" in _format_slice_plan(
            plan, total=len(pipe.nodes)
        )

    def _rejected(self, tmp_path, argv):
        """Run the dataset command far enough to reach the flag guards.

        Returns the output. ``exit_code == 1`` on its own would be satisfied by
        any crash on the way there — the mocked Spark session gives plenty of
        opportunity — so the caller asserts on the message, which only the
        intended guard emits.
        """
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1, "train_dev_ratio": 0.2,
                "train_snap_dates": ["2025-12-31"],
                "test_snap_dates": ["2026-01-31"],
            }},
        )
        old = os.getcwd(); os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.utils.spark.get_or_create_spark_session",
                       return_value=MagicMock()), \
                 patch("recsys_tfb.__main__.Runner"):
                result = runner.invoke(app, argv)
        finally:
            os.chdir(old)
        assert result.exit_code == 1, result.output
        return result.output

    def test_it_is_rejected_together_with_only_node(self, tmp_path):
        out = self._rejected(tmp_path, [
            "dataset", "--only-test-months",
            "--only-node", "filter_test_model_input",
        ])
        assert "--only-node, --only-test-months are mutually exclusive" in out

    def test_it_is_rejected_together_with_list_nodes(self, tmp_path):
        out = self._rejected(
            tmp_path, ["dataset", "--only-test-months", "--list-nodes"]
        )
        # Names the flag the operator actually typed. The guard is shared with
        # three commands that have no preset, so a message built from the
        # label alone would send them looking for a `--preset` flag.
        assert "--list-nodes cannot be combined with --only-test-months" in out

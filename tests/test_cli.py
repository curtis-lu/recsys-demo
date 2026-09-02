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
from recsys_tfb.pipelines.dataset.pipeline import ONLY_TEST_MONTHS_NODES

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
                # A real DataCatalog built from this conf answers None here —
                # it declares no training_eval_predictions — and A28 passes on
                # None. Left as a bare MagicMock the gate reads an attribute
                # that answers "in" for nothing, rejects the run, and this
                # test fails (or worse, passes) for a reason it is not about.
                mock_catalog_cls.get_dataset = lambda *a, **kw: None
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
        """_execute_pipeline calls inject_cache_source_tables with substitution_params
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
                # A real DataCatalog built from this conf answers None here —
                # it declares no training_eval_predictions — and A28 passes on
                # None. Left as a bare MagicMock the gate reads an attribute
                # that answers "in" for nothing, rejects the run, and this
                # test fails (or worse, passes) for a reason it is not about.
                mock_catalog_cls.get_dataset = lambda *a, **kw: None
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
                # A real DataCatalog built from this conf answers None here —
                # it declares no training_eval_predictions — and A28 passes on
                # None. Left as a bare MagicMock the gate reads an attribute
                # that answers "in" for nothing, rejects the run, and this
                # test fails (or worse, passes) for a reason it is not about.
                mock_catalog_cls.get_dataset = lambda *a, **kw: None
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
        text = "\n".join(line for _, line in lines)
        assert "auto-included" in text
        assert "B" in text and "<- b" in text
        assert "skipped" in text and "A" not in plan.auto_included
        assert "WARNING" in text
        assert "running 2 of 3 nodes" in text

    def test_format_slice_plan_marks_exactly_the_two_warning_lines(self):
        """Level travels with the line, chosen where the line is written.

        The formatter, not the caller, knows which lines are warnings, so it
        hands back ``(level, line)`` rather than leaving the caller to sniff a
        prefix. Two lines earn WARNING: the skipped data gate (a Layer-2
        invariant went unchecked -- issue #157) and the resume caveat, which
        already spelled "WARNING:" in its own text while going out at INFO.
        Everything else is plan bookkeeping and stays INFO.
        """
        pipe = Pipeline([
            Node(func=lambda: None, outputs="a", name="A"),
            Node(func=lambda a: None, inputs=["a"], outputs=None, name="guard"),
            Node(func=lambda a: None, inputs=["a"], outputs="b", name="B"),
        ])
        _, plan = _slice_pipeline(pipe, lambda n: True, "B", None)
        assert plan.skipped_side_effect == ("guard",)

        emitted = _format_slice_plan(plan, total=3)
        warned = [line for level, line in emitted if level == logging.WARNING]
        infoed = [line for level, line in emitted if level == logging.INFO]

        assert len(warned) == 2, warned
        assert any("skipped side-effect nodes" in line for line in warned)
        assert any("resume assumes" in line for line in warned)
        # No third level, and the rest really is the whole remainder.
        assert len(warned) + len(infoed) == len(emitted)
        assert all("skipped side-effect" not in line for line in infoed)

    def test_format_slice_plan_omits_the_gate_warning_when_nothing_was_skipped(self):
        """A plan that skipped no side-effect node must not warn about one.

        Without this, an implementation that warns unconditionally would pass
        the test above and cry wolf on every sliced run.
        """
        _, plan = _slice_pipeline(_slice_test_pipe(), lambda n: True, "C", None)
        assert plan.skipped_side_effect == ()
        warned = [
            line for level, line in _format_slice_plan(plan, total=3)
            if level == logging.WARNING
        ]
        assert len(warned) == 1
        assert "resume assumes" in warned[0]

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
                # Required by A23 (#158). Without it A23 fires first and the
                # assertions below are satisfied by the wrong invariant: both
                # tests only check exit 1 / no Spark, which A23 also produces.
                "train_snap_dates": ["2025-12-31"],
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
                # Required by A23 (#158). Without it A23 fires first and the
                # assertions below are satisfied by the wrong invariant: both
                # tests only check exit 1 / no Spark, which A23 also produces.
                "train_snap_dates": ["2025-12-31"],
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

    def test_the_skipped_data_gate_reaches_the_operator_as_a_warning(
        self, tmp_path
    ):
        """The wiring, not the formatter: does the CLI honour the level?

        ``_format_slice_plan`` can label the line WARNING and the command can
        still push every line through ``logger.info`` -- that was the defect
        (issue #157), and the unit test above it cannot see it. Patching the
        module logger rather than reading ``caplog``: ``setup_logging`` clears
        the root handlers this command's logs would otherwise land in.

        ``--only-node filter_test_model_input`` skips
        ``validate_data_consistency``, the Layer-2 data gate, which is the
        whole reason the line exists.
        """
        emitted = []

        class _Recorder:
            def log(self, level, msg, *a, **kw):
                emitted.append((level, msg % a if a else msg))

            def info(self, msg, *a, **kw):
                emitted.append((logging.INFO, msg % a if a else msg))

            def warning(self, msg, *a, **kw):
                emitted.append((logging.WARNING, msg % a if a else msg))

            def error(self, msg, *a, **kw):
                emitted.append((logging.ERROR, msg % a if a else msg))

            def debug(self, msg, *a, **kw):
                emitted.append((logging.DEBUG, msg % a if a else msg))

        with patch.object(
            DataCatalog, "exists", lambda self, name: name in _LANDED
        ), patch("recsys_tfb.__main__.logger", _Recorder()):
            _run_dataset_command(
                tmp_path,
                ["dataset", "--only-node", "filter_test_model_input", "--dry-run"],
                existing=("2026-01-31",),
                foreign=(),
            )

        gate_lines = [
            (level, msg) for level, msg in emitted
            if "skipped side-effect nodes" in str(msg)
        ]
        assert len(gate_lines) == 1, emitted
        level, msg = gate_lines[0]
        assert level == logging.WARNING
        assert "validate_data_consistency" in msg


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
                # A real DataCatalog built from this conf answers None here —
                # it declares no training_eval_predictions — and A28 passes on
                # None. Left as a bare MagicMock the gate reads an attribute
                # that answers "in" for nothing, rejects the run, and this
                # test fails (or worse, passes) for a reason it is not about.
                mock_catalog_cls.get_dataset = lambda *a, **kw: None
                runner.invoke(app, ["training", "--rebuild-dates", "2026-02-28"])
        finally:
            os.chdir(old_cwd)

        assert captured["params"][REBUILD_SNAP_DATES_KEY] == ["2026-02-28"]


class TestDuplicateTestMonthA26:
    """A26 is wired to the training command, not to the global aggregator."""

    def test_two_spellings_of_one_month_exit_before_spark_starts(self, tmp_path):
        # Like A21/A24: a config error must not cost a 2-4 minute cold start.
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "test_snap_dates": ["2026-01-31", "20260131"],
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
                result = runner.invoke(app, ["training"])
            assert result.exit_code == 1
            # The load-bearing assertion: exit_code alone is satisfied by the
            # mocked session blowing up further down the command.
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_the_same_month_spelled_one_way_is_not_blocked(self, tmp_path):
        # The discriminating half: without it the test above is also passed by
        # an A26 that rejects every training config outright.
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "test_snap_dates": ["2026-01-31", "2026-02-28"],
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
                runner.invoke(app, ["training"])
            # It got past A26 and reached the cold start it is allowed to reach.
            mock_spark.assert_called()
        finally:
            os.chdir(old_cwd)

    def test_a_dataset_run_with_the_same_config_is_unaffected(self, tmp_path):
        # A26 hangs off training on purpose: the dataset pipeline normalises
        # its months through pd.Timestamp into a set, so two spellings collapse
        # there. This goes red the moment A26 is tidied into
        # validate_config_consistency, which every command runs.
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_snap_dates": ["2026-01-31"],
                "test_snap_dates": ["2026-02-28", "20260228"],
            }},
        )

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                runner.invoke(app, ["feature_etl"])
            mock_spark.assert_called()
        finally:
            os.chdir(old_cwd)


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

    @staticmethod
    def _training_advice(rebuild):
        """The advice dict the training command builds.

        Spelled out rather than relying on a default: ``targets`` is now
        required, so a test omitting it would pass for the wrong reason (the
        early return), not because the node was in the slice.
        """
        from recsys_tfb.__main__ import (
            _REBUILD_PREDICT_NODE, _REBUILD_TARGET_NODES,
        )
        return {
            "rebuild": rebuild,
            "targets": _REBUILD_TARGET_NODES,
            "predict_node": _REBUILD_PREDICT_NODE,
        }

    def test_silent_when_the_predict_node_is_in_the_slice(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        pipe = self._pipe("cache_test_model_input", "predict_and_write_test_predictions")
        assert _maybe_warn_rebuild_sliced_away(
            pipe, self._training_advice(["2026-01-31"])
        ) == []

    def test_warns_and_names_the_months_when_predict_is_sliced_away(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        lines = "\n".join(
            _maybe_warn_rebuild_sliced_away(
                self._pipe("compute_feature_importance"),
                self._training_advice(["2026-01-31"]),
            )
        )
        assert "--rebuild-dates" in lines
        assert "2026-01-31" in lines
        assert "predict_and_write_test_predictions" in lines

    def test_warns_when_only_the_cache_target_survives_the_slice(self):
        """The gap landing predict_manifest opened (issue #233).

        ``--rebuild-dates`` drives two nodes, but only one of them produces
        anything: dropping the stale cache is a means, re-predicting is the
        end. While predict_manifest was memory-only the two always travelled
        together in a forward slice, so "some target is here" happened to imply
        "predict is here". Now that the manifest loads from disk,
        ``--from-node compute_feature_statistics --rebuild-dates ...`` keeps
        cache_test_model_input and drops the predict node — the cache is
        rebuilt, nothing is re-predicted, and the run exits 0.
        """
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        lines = "\n".join(
            _maybe_warn_rebuild_sliced_away(
                self._pipe("cache_test_model_input", "compute_feature_statistics"),
                self._training_advice(["2026-01-31"]),
            )
        )
        assert "2026-01-31" in lines
        # Names both halves: which node this slice did run, and the one it did
        # not. A message that only named the missing node would read as "the
        # flag did nothing", which is not what happened.
        assert "cache_test_model_input" in lines
        assert "predict_and_write_test_predictions" in lines

    def test_silent_when_the_flag_was_not_passed(self):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        assert _maybe_warn_rebuild_sliced_away(
            self._pipe("compute_feature_importance"), self._training_advice([])
        ) == []

    def test_silent_when_the_caller_names_no_target_nodes(self):
        """The dataset command's shape: --rebuild-dates drives its whole test
        chain, so there is no single node to say was "sliced away". Falling
        back to a default would print training's node names to a dataset
        operator.
        """
        from recsys_tfb.__main__ import _maybe_warn_rebuild_sliced_away

        assert _maybe_warn_rebuild_sliced_away(
            self._pipe("filter_test_model_input"),
            {"rebuild": ["2026-01-31"], "chain": "test 鏈"},
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


# --- #203: --only-test-months is a create_pipeline mode, not a slice ---

class TestOnlyTestMonthsFlag:
    """ADR-0013: the flag picks which line of work runs; --from-node /
    --only-node still pick where it resumes. Orthogonal, composable."""

    @staticmethod
    def _conf(tmp_path):
        _setup_conf(
            tmp_path,
            params_dataset={"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                "train_snap_dates": ["2026-01-31"],
                "test_snap_dates": ["2026-02-28"],
                "enable_calibration": True,
            }},
        )

    @staticmethod
    def _run_dataset(tmp_path, argv):
        """Invoke the dataset command with Spark, the catalog and the runner
        mocked; returns (result, the Pipeline the Runner was handed or None).
        """
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch(
                        "recsys_tfb.utils.spark.get_or_create_spark_session",
                        return_value=_mock_spark_with_feature_table_schema(),
                    ), \
                    patch("recsys_tfb.__main__.Runner") as mock_runner_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                # A real DataCatalog built from this conf answers None here —
                # it declares no training_eval_predictions — and A28 passes on
                # None. Left as a bare MagicMock the gate reads an attribute
                # that answers "in" for nothing, rejects the run, and this
                # test fails (or worse, passes) for a reason it is not about.
                mock_catalog_cls.get_dataset = lambda *a, **kw: None
                result = runner.invoke(app, ["dataset", *argv])
                run_calls = mock_runner_cls.return_value.run.call_args
                return result, (run_calls[0][0] if run_calls else None)
        finally:
            os.chdir(old_cwd)

    def test_dataset_help_advertises_the_flag(self):
        result = runner.invoke(app, ["dataset", "--help"])
        assert result.exit_code == 0, result.output
        assert "--only-test-months" in result.output

    def test_it_is_a_dataset_only_flag(self):
        # The other pipelines have no test chain to scope to; advertising it
        # there would be a mode that does nothing.
        for cmd in ("training", "inference", "evaluation"):
            result = runner.invoke(app, [cmd, "--help"])
            assert "--only-test-months" not in result.output, cmd

    def test_the_runner_is_handed_the_short_pipeline(self, tmp_path):
        """The end that matters: not "the kwarg was forwarded" but "the run
        executed five nodes". Accepted-but-not-forwarded is the silent no-op.
        """
        self._conf(tmp_path)
        _, pipe = self._run_dataset(tmp_path, ["--only-test-months"])
        assert pipe is not None, "pipeline never reached the Runner"
        assert [n.name for n in pipe.nodes] == list(ONLY_TEST_MONTHS_NODES)

    def test_without_the_flag_the_runner_still_gets_all_fifteen(self, tmp_path):
        self._conf(tmp_path)
        _, pipe = self._run_dataset(tmp_path, [])
        assert pipe is not None
        assert len(pipe.nodes) == 15

    def test_plan_line_counts_and_names_what_it_left_out(self, tmp_path):
        from recsys_tfb.__main__ import _format_only_test_months_plan
        from recsys_tfb.pipelines import get_pipeline

        lines = _format_only_test_months_plan(enable_calibration=True)
        assert "5 of the dataset pipeline's 15 nodes" in lines[0]
        assert "10 left out" in lines[0]

        # The names, compared against the pipelines themselves: a message that
        # carried its own copy of the list could disagree with what ran.
        full = [n.name for n in get_pipeline("dataset", enable_calibration=True).nodes]
        kept = {
            n.name for n in get_pipeline(
                "dataset", enable_calibration=True, only_test_months=True
            ).nodes
        }
        listed = lines[1].split(":", 1)[1].strip().split(", ")
        assert listed == [name for name in full if name not in kept]

    def test_list_nodes_composes_and_lists_only_the_short_pipeline(self, tmp_path):
        # Read back from result.output, not caplog: setup_logging clears the
        # root handlers, caplog's included, so caplog is blind to anything a
        # command logs after that point.
        self._conf(tmp_path)
        result, pipe = self._run_dataset(
            tmp_path, ["--only-test-months", "--list-nodes"]
        )
        assert result.exit_code == 0, result.output
        assert pipe is None  # --list-nodes exits before running
        listed = [
            line for line in result.output.splitlines() if "[nodes] " in line
        ]
        assert len(listed) == 1 + len(ONLY_TEST_MONTHS_NODES)  # header + 5

    def test_only_node_composes_and_counts_against_the_short_pipeline(self, tmp_path):
        self._conf(tmp_path)
        result, _ = self._run_dataset(tmp_path, [
            "--only-test-months",
            "--only-node", "filter_test_model_input",
            "--dry-run",
        ])
        assert result.exit_code == 0, result.output
        # M is the mode's node count, not the full pipeline's: the mode is the
        # whole of what this run is, and the slice subsets *that*.
        assert f"of {len(ONLY_TEST_MONTHS_NODES)} nodes" in result.output

    def test_from_node_composes_and_counts_against_the_short_pipeline(self, tmp_path):
        # The third composition the issue names. --from-node keeps its start
        # node and everything topologically after it *within the mode*, so the
        # count is against 5 — the same M as --only-node.
        self._conf(tmp_path)
        result, _ = self._run_dataset(tmp_path, [
            "--only-test-months",
            "--from-node", "build_test_model_input",
            "--dry-run",
        ])
        assert result.exit_code == 0, result.output
        assert f"of {len(ONLY_TEST_MONTHS_NODES)} nodes" in result.output
        # "and everything after it" is scoped to the mode: the requested set is
        # the two test-chain nodes, not the full pipeline's downstream. Read the
        # requested line rather than the whole output — the mode's own
        # "[plan] left out:" line legitimately names every node it dropped.
        requested = [
            line for line in result.output.splitlines()
            if "[plan] mode=from; requested:" in line
        ]
        assert len(requested) == 1, result.output
        assert requested[0].split("requested:")[1].strip() == (
            "build_test_model_input, filter_test_model_input"
        )

    def test_a_node_name_outside_the_mode_is_rejected_by_name(self, tmp_path):
        """Slicing to a node the mode left out fails loud, and the message
        lists what is actually available — otherwise the operator reads
        "unknown node" about a node the pipeline plainly has.
        """
        self._conf(tmp_path)
        result, pipe = self._run_dataset(tmp_path, [
            "--only-test-months", "--only-node", "build_train_model_input",
        ])
        assert result.exit_code == 1
        assert pipe is None


class TestRebuildPartialChainWarning:
    """The condition is "did this run drop nodes", not "was a flag passed"."""

    @staticmethod
    def _lines(kept, total, advice):
        from recsys_tfb.__main__ import _maybe_warn_rebuild_partial_chain
        return _maybe_warn_rebuild_partial_chain(kept, total, advice)

    ADVICE = {"rebuild": ["2026-01-31"], "chain": "test 鏈"}

    def test_fires_when_the_slice_actually_dropped_nodes(self):
        lines = "\n".join(self._lines(3, 15, self.ADVICE))
        assert "--rebuild-dates" in lines
        assert "2026-01-31" in lines
        assert "test 鏈" in lines

    def test_silent_when_every_node_was_kept(self):
        """The false alarm this replaces: `--from-node <first node>` selects
        15 of 15, and the old condition still claimed the unselected upstream
        would stay stale and told the operator to re-run without the flag —
        which produced bit-identical output.
        """
        assert self._lines(15, 15, self.ADVICE) == []

    def test_silent_for_a_mode_that_built_a_short_pipeline(self):
        # --only-test-months --rebuild-dates <an existing test month> is the
        # supported recompute path, not a partial chain: total is the mode's 5.
        assert self._lines(5, 5, self.ADVICE) == []

    def test_silent_without_the_flag(self):
        assert self._lines(3, 15, {"rebuild": [], "chain": "test 鏈"}) == []

    def test_silent_when_the_caller_names_no_chain(self):
        # training's shape: its --rebuild-dates drives two named nodes, so the
        # other warning applies and this one would name a chain it has not got.
        assert self._lines(3, 21, {"rebuild": ["2026-01-31"]}) == []


class TestTrainSnapDatesA23:
    """A23 is wired to the dataset command, not to the global aggregator (#158)."""

    @staticmethod
    def _run(tmp_path, params_dataset):
        _setup_conf(tmp_path, params_dataset=params_dataset)
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(app, ["dataset"])
            return result, mock_spark
        finally:
            os.chdir(old)

    def test_empty_list_exits_before_spark_starts(self, tmp_path):
        # The branch with no downstream guard: an empty list is not "train on
        # nothing", it skips the month filter entirely.
        result, mock_spark = self._run(
            tmp_path,
            {"dataset": {"sample_ratio": 0.1, "train_snap_dates": []}},
        )
        assert result.exit_code == 1
        mock_spark.assert_not_called()
        assert "A23" in result.output

    def test_absent_key_exits_before_spark_starts(self, tmp_path):
        result, mock_spark = self._run(tmp_path, {"dataset": {"sample_ratio": 0.1}})
        assert result.exit_code == 1
        mock_spark.assert_not_called()
        assert "A23" in result.output

    def test_a_configured_list_reaches_spark(self, tmp_path):
        _, mock_spark = self._run(
            tmp_path,
            {"dataset": {
                "sample_ratio": 0.1,
                "train_dev_ratio": 0.2,
                "train_snap_dates": ["2025-12-31"],
            }},
        )
        assert mock_spark.called

    def test_other_commands_are_not_blocked_by_a_missing_key(self, tmp_path):
        # The whole point of not aggregating it: feature_etl has no business
        # with dataset.train_snap_dates, and #158 measured 9 tests blocked by
        # putting this on the global gate.
        _setup_etl_conf(tmp_path)
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["feature_etl", "--help"])
            assert result.exit_code == 0
            result = runner.invoke(app, ["training", "--help"])
            assert result.exit_code == 0
        finally:
            os.chdir(old)


class TestEntityColumnsDeclaredA28:
    """A28 is wired to the training command, and fires before Spark starts.

    Placement is the point: the node that writes these columns runs after HPO,
    train_model and calibrate_model, so the same check inside it would report a
    one-word catalog typo only after the whole search had been paid for.
    """

    @staticmethod
    def _conf_with(tmp_path, declared_columns):
        _setup_conf(
            tmp_path,
            params_dataset={
                "dataset": {"sample_ratio": 0.1},
                "schema": {"columns": {"entity": ["cust_id", "acct_id"]}},
            },
            params_training={"lr": 0.01},
        )
        catalog_path = tmp_path / "conf" / "base" / "catalog.yaml"
        with open(catalog_path) as f:
            catalog = yaml.safe_load(f)
        catalog["training_eval_predictions"] = {
            "type": "HiveTableDataset",
            "database": "ml_recsys",
            "table": "training_eval_predictions",
            "external": False,
            "columns": [{"name": c, "type": "STRING"} for c in declared_columns],
            "partition_filter": {"model_version": "${model_version}"},
            "partition_cols": [{"name": "snap_date", "type": "STRING"}],
        }
        with open(catalog_path, "w") as f:
            yaml.dump(catalog, f)
        _make_base_and_train_variant(tmp_path, base_v="abc12345", train_v="11111111")

    def test_an_undeclared_entity_column_exits_before_spark_starts(self, tmp_path):
        # schema.entity names two columns; the catalog entry declares one. The
        # second would be dropped by save()'s select with no error at all.
        self._conf_with(tmp_path, ["cust_id", "score"])

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                result = runner.invoke(app, ["training"])
            assert result.exit_code == 1
            # The load-bearing assertion: exit_code alone is satisfied by the
            # mocked session blowing up further down the command.
            mock_spark.assert_not_called()
        finally:
            os.chdir(old_cwd)

    def test_declaring_both_entity_columns_is_not_blocked(self, tmp_path):
        # The discriminating half: without it the test above is also passed by
        # an A28 that rejects every training config outright.
        self._conf_with(tmp_path, ["cust_id", "acct_id", "score"])

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch(
                "recsys_tfb.utils.spark.get_or_create_spark_session"
            ) as mock_spark:
                runner.invoke(app, ["training"])
            # It got past A28 and reached the cold start it is allowed to reach.
            mock_spark.assert_called()
        finally:
            os.chdir(old_cwd)

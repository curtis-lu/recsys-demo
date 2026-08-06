import json
import os
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml
from typer.testing import CliRunner

from recsys_tfb.__main__ import app

runner = CliRunner()


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
        "scoring_dataset": {
            "type": "ParquetDataset",
            "filepath": "data/inference/${model_version}/${snap_date}/scoring_dataset.parquet",
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
                    # scoring_dataset output uses actual model hash
                    sd = call_args["scoring_dataset"]["filepath"]
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


class TestMonthPlansReachTheCatalog:
    """#152 — the plans travel as catalog datasets, not as ``parameters`` keys.

    This is the seam the whole feature hangs on. Forgetting it no longer fails
    silently (the runner refuses to start a node whose input is absent), but a
    plan wired to the *wrong* dataset name would still start — so assert the
    three by name and by content.
    """

    def _run_dataset(self, tmp_path, argv, existing=("2026-01-31",)):
        """Invoke the dataset command far enough to build the catalog.

        Only ``test_model_input`` is a Hive table in this catalog, so it is the
        only artifact with a partition listing; the other two fall back to
        "nothing has landed". That asymmetry is the point — it makes the three
        plans differ, so a test cannot pass by handing out the same plan thrice.
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
        }
        with open(catalog_path, "w") as f:
            yaml.dump(catalog, f)

        added = {}
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.__main__.DataCatalog") as mock_catalog_cls, \
                    patch(
                        "recsys_tfb.utils.spark.get_or_create_spark_session",
                        return_value=_mock_spark_with_feature_table_schema(),
                    ), \
                    patch(
                        "recsys_tfb.__main__.existing_snap_date_partitions",
                        return_value=list(existing),
                    ), \
                    patch("recsys_tfb.__main__.Runner"):
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda name, ds: added.__setitem__(name, ds)
                runner.invoke(app, argv)
        finally:
            os.chdir(old_cwd)
        return {name: ds.load() for name, ds in added.items()}

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


class TestCollectExistingSnapDates:
    def test_maps_each_hive_dataset_to_its_partitions(self):
        from recsys_tfb.__main__ import _collect_existing_snap_dates

        catalog = {
            "test_keys": {
                "type": "HiveTableDataset", "database": "db", "table": "t_keys",
            },
            "test_model_input": {
                "type": "HiveTableDataset", "database": "db", "table": "t_mi",
            },
            # not a Hive table -> no partitions to list
            "preprocessed_feature_table": {
                "type": "ParquetDataset", "filepath": "x.parquet",
            },
        }
        with patch(
            "recsys_tfb.__main__.existing_snap_date_partitions",
            side_effect=lambda spark, db, table, base, time_col="snap_date": [
                f"{table}-{base}-{time_col}"
            ],
        ):
            out = _collect_existing_snap_dates(
                MagicMock(), catalog, "abc12345", time_col="as_of",
            )

        # time_col is threaded through, not hardcoded: the framework's time
        # column is configurable via schema.time.
        assert out == {
            "test_keys": ["t_keys-abc12345-as_of"],
            "test_model_input": ["t_mi-abc12345-as_of"],
        }
        assert "preprocessed_feature_table" not in out


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

import json
import os
import re
from unittest.mock import MagicMock, patch

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
            params_dataset={"dataset": {"sample_ratio": 0.1, "train_dev_ratio": 0.2}},
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

    def test_slice_extra(self):
        assert _slice_extra("X", None) == {"resumed_from": "X"}
        assert _slice_extra(None, "Y") == {"only_node": "Y"}
        assert _slice_extra(None, None) is None

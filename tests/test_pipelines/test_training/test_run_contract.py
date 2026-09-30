"""What the training command asks this package about a run (ADR-0030
decision 13), tested without the command.

Pinned through the command instead, in ``tests/test_core/
test_consistency_cli_wiring.py`` and ``tests/test_cli.py``: that the command
asks :func:`config_errors` before the Spark cold start, and what it puts into
``parameters`` (no ``search_id``; the cache source tables for training only).
"""

import inspect
import json

import pytest

from recsys_tfb.core.versioning import compute_model_version
from recsys_tfb.pipelines.training import run_contract
from recsys_tfb.pipelines.training.run_contract import (
    RunVersions,
    config_errors,
    dataset_version_verdict,
    manifest_extra,
    pipeline_kwargs,
    versions_for_run,
)

_SCHEMA = {"columns": {
    "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
    "label": "label",
}}


def _predictions_entry(columns):
    """``training_eval_predictions`` as a deployment declares it."""
    return {"training_eval_predictions": {
        "type": "HiveTableDataset", "database": "db", "table": "preds",
        "external": False,
        "columns": [{"name": c, "type": "string"} for c in columns],
    }}


def _clean_params(**training):
    return {
        "schema": _SCHEMA,
        "dataset": {"test_snap_dates": ["2026-01-31"]},
        "training": {"algorithm": "lightgbm", **training},
    }


_DECLARED = ["snap_date", "cust_id", "prod_name", "score", "label"]


class TestConfigErrors:
    def test_a_clean_config_passes(self):
        assert config_errors(_clean_params(), _predictions_entry(_DECLARED)) == []

    def test_every_training_only_check_is_asked(self):
        """One mistake per check, and each is reported: A57, A58, A36, A53
        and A28 — the collection is the thing under test, not the predicates
        (``test_consistency.py`` has those)."""
        params = {
            "schema": _SCHEMA,
            "dataset": {"test_snap_dates": []},
            "training": {"algorithm": "nope", "fixed_params": {"seed": 1}},
            "test_metrics": {"metrics": ["not_a_metric"]},
        }
        errors = "\n".join(config_errors(
            params, _predictions_entry(["snap_date", "prod_name", "score"])))
        for code in ("A57", "A58", "A36", "A53"):
            assert code in errors, code
        assert "cust_id" in errors, "A28 (an entity column not declared)"

    def test_a_month_spelled_twice_is_reported(self):
        params = _clean_params()
        params["dataset"]["test_snap_dates"] = ["2026-01-31", "20260131"]
        assert any(
            "A26" in e for e in config_errors(params, _predictions_entry(_DECLARED)))

    def test_an_undeclared_prediction_table_is_not_its_business(self):
        """The Runner refuses a pipeline whose outputs it cannot resolve."""
        assert config_errors(_clean_params(), {}) == []

    def test_the_catalog_entry_is_read_once_for_all_three_checks(self):
        """One read handed to A28, A39 and A45, so one ``columns:`` edit
        fixes all three rather than revealing them one run at a time."""
        src = inspect.getsource(config_errors)
        assert src.count("getattr(") == 1
        assert src.count("declared,") + src.count("declared, ") >= 3


class TestVersionsForRun:
    @staticmethod
    def _dataset_dir(tmp_path, base="b1111111", variant="v1111111", manifest=None):
        dataset_dir = tmp_path / "dataset"
        variant_dir = dataset_dir / base / "train_variants" / variant
        variant_dir.mkdir(parents=True)
        (dataset_dir / "latest").symlink_to((dataset_dir / base).resolve())
        (dataset_dir / base / "train_variants" / "latest").symlink_to(
            variant_dir.resolve())
        if manifest is not None:
            (dataset_dir / base / "manifest.json").write_text(json.dumps(manifest))
        return dataset_dir

    def test_follows_latest_and_hashes_the_training_file(self, tmp_path):
        dataset_dir = self._dataset_dir(tmp_path)
        params_training = {"training": {"n_trials": 3}}

        versions = versions_for_run(dataset_dir, params_training, None, None)

        assert versions == RunVersions(
            base_dataset_version="b1111111", train_variant_id="v1111111",
            model_version=compute_model_version(
                params_training, "b1111111", "v1111111"),
            dataset_test_ratio=0.0,
        )

    def test_reads_the_test_ratio_the_version_was_built_with(self, tmp_path):
        dataset_dir = self._dataset_dir(tmp_path, manifest={
            "parameters": {"dataset": {"test_zero_positive_group_ratio": 0.5}},
        })
        versions = versions_for_run(dataset_dir, {}, None, None)
        assert versions.dataset_test_ratio == 0.5

    def test_a_named_base_version_with_no_directory_is_refused(self, tmp_path):
        dataset_dir = self._dataset_dir(tmp_path)
        with pytest.raises(ValueError, match="not found"):
            versions_for_run(dataset_dir, {}, "typo0000", "v1111111")

    def test_named_versions_win_over_latest(self, tmp_path):
        dataset_dir = self._dataset_dir(tmp_path)
        (dataset_dir / "b2222222" / "train_variants" / "v2222222").mkdir(parents=True)
        versions = versions_for_run(dataset_dir, {}, "b2222222", "v2222222")
        assert versions[:2] == ("b2222222", "v2222222")


class TestDatasetVersionVerdict:
    def test_a_binary_metric_on_a_version_without_zero_positive_groups_stops(self):
        params = _clean_params()
        params["dataset"]["test_zero_positive_group_ratio"] = 0.5
        params["test_metrics"] = {"metrics": ["pooled_average_precision"]}
        versions = RunVersions("b", "v", "m", dataset_test_ratio=0.0)
        verdict = dataset_version_verdict(params, versions)
        assert any("A54" in e for e in verdict.errors)

    def test_the_same_config_on_a_matching_version_passes(self):
        params = _clean_params()
        params["dataset"]["test_zero_positive_group_ratio"] = 0.5
        params["test_metrics"] = {"metrics": ["pooled_average_precision"]}
        versions = RunVersions("b", "v", "m", dataset_test_ratio=0.5)
        assert dataset_version_verdict(params, versions).errors == []


class TestPipelineKwargs:
    def test_the_mode_follows_the_setting(self):
        assert pipeline_kwargs({}) == {"hpo_enabled": True}
        assert pipeline_kwargs(_clean_params(hpo_enabled=True)) == {"hpo_enabled": True}
        assert pipeline_kwargs(_clean_params(hpo_enabled=False)) == {"hpo_enabled": False}

    def test_it_builds_the_matching_dag(self):
        from recsys_tfb.pipelines import get_pipeline

        names = {
            n.name for n in get_pipeline(
                "training", **pipeline_kwargs(_clean_params(hpo_enabled=False))).nodes
        }
        assert "train_with_fixed_params" in names
        assert "cache_val_model_input" not in names


class TestManifestExtra:
    def test_both_reports_land_under_their_own_keys(self, tmp_path):
        """The lambdarank row counts land next to the parameters that caused
        them: two runs of one model_version differ in training-set size only
        for a reason, and this is it."""
        weights = {"enabled": True, "weight_keys": ["prod_name"],
                   "n_weight_entries": 1, "unmatched_keys": []}
        groups = {"enabled": True, "objective": "lambdarank", "thin_splits": [],
                  "train": {"groups_total": 10, "groups_kept": 6}}
        (tmp_path / "sample_weight_report.json").write_text(json.dumps(weights))
        (tmp_path / "group_filter_report.json").write_text(json.dumps(groups))

        assert manifest_extra(tmp_path) == {
            "sample_weight": weights, "group_filter": groups,
        }

    def test_an_absent_report_adds_nothing(self, tmp_path):
        assert manifest_extra(tmp_path) == {}
        (tmp_path / "group_filter_report.json").write_text("{}")
        assert manifest_extra(tmp_path) == {"group_filter": {}}


def test_the_module_does_not_import_the_cli():
    """The point of the module (ADR-0030 decision 13): testable without it."""
    assert "recsys_tfb.__main__" not in inspect.getsource(run_contract)

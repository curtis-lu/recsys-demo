"""Tests for promote_model script."""

import json

import pytest
import yaml

from scripts.promote_model import (
    NO_SCORED_MONTHS,
    NO_VALUE,
    OTHER_SCORED_MONTHS,
    REQUIRED_ARTIFACTS,
    main,
    promote,
    rank_versions,
    validate_version,
)


def _create_full_version(models_dir, version, overall_map=0.75, per_item_map_attr=None):
    """Create a version directory with all required artifacts.

    Its evaluation_results.json has only the four keys written before
    ADR-0028: no scored months, no per-metric values.
    """
    version_dir = models_dir / version
    version_dir.mkdir(parents=True, exist_ok=True)

    eval_results = {
        "overall_map": overall_map,
        "per_item_map_attr": per_item_map_attr or {"exchange_fx": 0.8},
    }
    (version_dir / "evaluation_results.json").write_text(json.dumps(eval_results))
    (version_dir / "best_params.json").write_text(json.dumps({"lr": 0.1}))
    (version_dir / "model.txt").write_text("fake_model")


def _create_scored_version(
    models_dir, version, *, snap_dates, metrics, not_computed=None,
):
    """A version whose evaluation_results.json is written the way
    ``compute_test_metrics`` writes it (ADR-0028): the months it was scored
    on and a value per metric it computed."""
    _create_full_version(models_dir, version,
                         overall_map=metrics.get("mean_ap", 0.5))
    path = models_dir / version / "evaluation_results.json"
    results = json.loads(path.read_text())
    results.update({
        "n_queries": 100,
        "n_excluded_queries": 0,
        "snap_dates": snap_dates,
        "metrics": metrics,
        "metrics_not_computed": not_computed or {},
        "selection_metric": "mean_ap",
        "hpo_objective": "mean_ap",
    })
    path.write_text(json.dumps(results))


def _params(*, test_snap_dates=("2026-01-31",), hpo_objective=None,
            selection_metric=None, snap_date=None):
    """The current configuration promote reads, as a loaded parameters dict."""
    params = {"dataset": {"test_snap_dates": list(test_snap_dates)}}
    if hpo_objective is not None:
        params["training"] = {"hpo_objective": hpo_objective}
    block = {}
    if selection_metric is not None:
        block["selection_metric"] = selection_metric
    if snap_date is not None:
        block["snap_date"] = snap_date
    if block:
        params["test_metrics"] = block
    return params


def _codes(ranking):
    """{version: sorted reason codes} for the versions left out."""
    return {
        entry.version: sorted(r.code for r in entry.reasons)
        for entry in ranking.not_ranked
    }


class TestRankVersions:
    """Who takes part in the ranking: a value under the current selection
    metric, scored on exactly the current scored months (ADR-0028
    decision 3)."""

    def test_only_versions_scored_on_the_current_months_with_the_metric(
        self, tmp_path,
    ):
        models_dir = tmp_path / "models"
        _create_scored_version(
            models_dir, "a1a1a1a1", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.70, "macro_per_item_map": 0.60})
        # The same day spelled another way is the same scored month.
        _create_scored_version(
            models_dir, "b2b2b2b2", snap_dates=["20260131"],
            metrics={"mean_ap": 0.50, "macro_per_item_map": 0.90})
        # Every excluded one has the highest value, so a rule that let it in
        # would put it first.
        _create_scored_version(
            models_dir, "c3c3c3c3", snap_dates=["2025-12-31"],
            metrics={"mean_ap": 0.99, "macro_per_item_map": 0.99})
        _create_scored_version(
            models_dir, "d4d4d4d4", snap_dates=["2026-01-31", "2026-02-28"],
            metrics={"mean_ap": 0.99, "macro_per_item_map": 0.99})
        _create_scored_version(
            models_dir, "e5e5e5e5", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.99})
        _create_full_version(models_dir, "f6f6f6f6", overall_map=0.99)

        ranking = rank_versions(
            models_dir, _params(hpo_objective="macro_per_item_map"))

        assert ranking.metric == "macro_per_item_map"
        assert ranking.months == ["2026-01-31"]
        assert [(r.version, r.value) for r in ranking.ranked] == [
            ("b2b2b2b2", 0.90), ("a1a1a1a1", 0.60)]
        assert _codes(ranking) == {
            "c3c3c3c3": [OTHER_SCORED_MONTHS],
            "d4d4d4d4": [OTHER_SCORED_MONTHS],
            "e5e5e5e5": [NO_VALUE],
            "f6f6f6f6": [NO_SCORED_MONTHS],
        }

    @pytest.mark.parametrize("params, first", [
        (_params(hpo_objective="macro_per_item_map"), "b2b2b2b2"),
        (_params(hpo_objective="mean_ap"), "a1a1a1a1"),
        # Unset, the HPO objective is the program's default, mean_ap.
        (_params(), "a1a1a1a1"),
        (_params(hpo_objective="mean_ap", selection_metric="macro_per_item_map"),
         "b2b2b2b2"),
        (_params(hpo_objective="macro_per_item_map", selection_metric="mean_ap"),
         "a1a1a1a1"),
    ])
    def test_the_ruler_is_the_current_selection_metric(
        self, tmp_path, params, first,
    ):
        models_dir = tmp_path / "models"
        _create_scored_version(
            models_dir, "a1a1a1a1", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.90, "macro_per_item_map": 0.50})
        _create_scored_version(
            models_dir, "b2b2b2b2", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.60, "macro_per_item_map": 0.80})

        assert rank_versions(models_dir, params).ranked[0].version == first

    @pytest.mark.parametrize("snap_date, ranked", [
        # Pinned to the first month: a month added to test_snap_dates later
        # does not move the exam.
        ("2026-01-31", "a1a1a1a1"),
        # Unset: every test_snap_dates month.
        (None, "b2b2b2b2"),
    ])
    def test_the_exam_is_the_current_scored_months(
        self, tmp_path, snap_date, ranked,
    ):
        models_dir = tmp_path / "models"
        _create_scored_version(
            models_dir, "a1a1a1a1", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.5})
        _create_scored_version(
            models_dir, "b2b2b2b2", snap_dates=["2026-01-31", "2026-02-28"],
            metrics={"mean_ap": 0.5})

        ranking = rank_versions(models_dir, _params(
            test_snap_dates=["2026-01-31", "2026-02-28"], snap_date=snap_date))

        assert [r.version for r in ranking.ranked] == [ranked]

    def test_a_value_training_withheld_says_why(self, tmp_path):
        models_dir = tmp_path / "models"
        why = "the dataset version kept no zero-positive query group in test"
        _create_scored_version(
            models_dir, "a1a1a1a1", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.5, "macro_per_item_map": 0.5},
            not_computed={"pooled_average_precision": why})

        ranking = rank_versions(models_dir, _params(
            hpo_objective="pooled_average_precision",
            selection_metric="pooled_average_precision"))

        [entry] = ranking.not_ranked
        [reason] = entry.reasons
        assert reason.code == NO_VALUE
        assert why in reason.says


def _scored(models_dir, version, value):
    """A version scored on the scored months of :func:`_params`' default,
    with ``value`` under ``mean_ap``, the default HPO objective."""
    _create_scored_version(models_dir, version, snap_dates=["2026-01-31"],
                           metrics={"mean_ap": value})


def _ranked(models_dir):
    """The ranked versions, best first, under :func:`_params`' default."""
    return [r.version for r in rank_versions(models_dir, _params()).ranked]


class TestRankOrder:
    def test_highest_value_first_timestamp(self, tmp_path):
        models_dir = tmp_path / "models"
        _scored(models_dir, "20260315_100000", 0.65)
        _scored(models_dir, "20260316_100000", 0.80)
        _scored(models_dir, "20260317_100000", 0.72)

        assert _ranked(models_dir) == [
            "20260316_100000", "20260317_100000", "20260315_100000"]

    def test_highest_value_first_hash(self, tmp_path):
        models_dir = tmp_path / "models"
        _scored(models_dir, "a1b2c3d4", 0.65)
        _scored(models_dir, "e5f6a7b8", 0.80)

        assert _ranked(models_dir) == ["e5f6a7b8", "a1b2c3d4"]

    def test_across_mixed_formats(self, tmp_path):
        models_dir = tmp_path / "models"
        _scored(models_dir, "20260315_100000", 0.65)
        _scored(models_dir, "a1b2c3d4", 0.80)

        assert _ranked(models_dir) == ["a1b2c3d4", "20260315_100000"]

    def test_empty_models_dir(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        ranking = rank_versions(models_dir, _params())
        assert ranking.ranked == [] and ranking.not_ranked == []

    def test_ignores_best_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        _scored(models_dir, "a1b2c3d4", 0.80)
        # Create a best symlink - should be ignored
        (models_dir / "best").symlink_to((models_dir / "a1b2c3d4").resolve())

        ranking = rank_versions(models_dir, _params())
        assert [r.version for r in ranking.ranked] == ["a1b2c3d4"]
        assert ranking.not_ranked == []


class TestValidateVersion:
    def test_complete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4")
        assert validate_version(models_dir / "a1b2c3d4") == []

    def test_incomplete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        (version_dir / "some_other_file.txt").write_text("fake")

        missing = validate_version(version_dir)
        assert len(missing) == 3  # missing model.txt, best_params.json, evaluation_results.json


class TestPromote:
    def test_promote_creates_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.80, {"exchange_fx": 0.85, "exchange_usd": 0.75})

        summary = promote(models_dir, "a1b2c3d4")
        assert summary["promoted_version"] == "a1b2c3d4"
        assert summary["overall_map"] == 0.80

        best_dir = models_dir / "best"
        assert best_dir.is_symlink()
        assert best_dir.resolve() == (models_dir / "a1b2c3d4").resolve()

        # Verify artifacts accessible through symlink
        for artifact in REQUIRED_ARTIFACTS:
            assert (best_dir / artifact).exists()

    def test_promote_replaces_existing_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.65)
        _create_full_version(models_dir, "e5f6a7b8", 0.80)

        promote(models_dir, "a1b2c3d4")
        promote(models_dir, "e5f6a7b8")

        best_dir = models_dir / "best"
        assert best_dir.is_symlink()
        assert best_dir.resolve() == (models_dir / "e5f6a7b8").resolve()

    def test_promote_replaces_existing_directory(self, tmp_path):
        """Old-format best/ was a directory, promote should replace with symlink."""
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.80)

        # Create old-style best directory
        old_best = models_dir / "best"
        old_best.mkdir(parents=True)
        (old_best / "model.pkl").write_bytes(b"old_model")

        promote(models_dir, "a1b2c3d4")

        assert old_best.is_symlink()
        assert old_best.resolve() == (models_dir / "a1b2c3d4").resolve()

    def test_promote_timestamp_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260316_100000", 0.80)

        summary = promote(models_dir, "20260316_100000")
        assert summary["promoted_version"] == "20260316_100000"
        assert (models_dir / "best").is_symlink()

    def test_promote_nonexistent_version(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        with pytest.raises(SystemExit):
            promote(models_dir, "nonexistent")

    def test_promote_incomplete_artifacts(self, tmp_path):
        models_dir = tmp_path / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        (version_dir / "model.pkl").write_bytes(b"fake")

        with pytest.raises(SystemExit):
            promote(models_dir, "a1b2c3d4")

    def test_manifest_accessible_through_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.80)
        # Add a manifest
        manifest = {"version": "a1b2c3d4", "pipeline": "training"}
        (models_dir / "a1b2c3d4" / "manifest.json").write_text(json.dumps(manifest))

        promote(models_dir, "a1b2c3d4")

        # manifest should be accessible through best symlink
        best_manifest = models_dir / "best" / "manifest.json"
        assert best_manifest.exists()
        data = json.loads(best_manifest.read_text())
        assert data["version"] == "a1b2c3d4"


def _write_conf(root, *, training=None, test_metrics=None,
                test_snap_dates=("2026-01-31",), env="local", env_params=None):
    """A ``conf/`` tree under ``root`` the way the pipelines read it: base
    plus the ``env`` overlay (``env_params``, {file stem: content})."""
    base = root / "conf" / "base"
    base.mkdir(parents=True)
    (base / "parameters.yaml").write_text(yaml.dump({"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}}}))
    dataset = {"dataset": {"test_snap_dates": list(test_snap_dates)}}
    (base / "parameters_dataset.yaml").write_text(yaml.dump(dataset))
    params_training = {"training": training or {}}
    if test_metrics is not None:
        params_training["test_metrics"] = test_metrics
    (base / "parameters_training.yaml").write_text(yaml.dump(params_training))
    overlay = root / "conf" / env
    overlay.mkdir()
    for stem, content in (env_params or {}).items():
        (overlay / f"{stem}.yaml").write_text(yaml.dump(content))


class TestMain:
    """The command as run from a repo root: conf/ and data/models under the
    working directory."""

    @pytest.fixture
    def root(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        return tmp_path

    @staticmethod
    def _best(root):
        best = root / "data" / "models" / "best"
        return best.resolve().name if best.is_symlink() else None

    def _two_versions_and_an_old_one(self, root):
        models_dir = root / "data" / "models"
        _create_scored_version(
            models_dir, "a1a1a1a1", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.90, "macro_per_item_map": 0.50})
        _create_scored_version(
            models_dir, "b2b2b2b2", snap_dates=["2026-01-31"],
            metrics={"mean_ap": 0.60, "macro_per_item_map": 0.80})
        _create_full_version(models_dir, "c3c3c3c3", overall_map=0.99)

    def test_dry_run_lists_every_version_and_recommends_a_ranked_one(
        self, root, capsys,
    ):
        _write_conf(root, training={"hpo_objective": "macro_per_item_map"})
        self._two_versions_and_an_old_one(root)

        main(["--dry-run"])

        out = capsys.readouterr().out
        assert "Recommended: b2b2b2b2 (macro_per_item_map=0.8000)" in out
        assert "a1a1a1a1  macro_per_item_map=0.5000" in out
        old = out[out.index("c3c3c3c3"):]
        assert "records no scored months" in old
        assert "rescoring-an-old-version.md" in old
        assert self._best(root) is None

    def test_dry_run_recommends_nothing_when_no_version_is_ranked(
        self, root, capsys,
    ):
        _write_conf(root)
        _create_full_version(root / "data" / "models", "c3c3c3c3", 0.99)

        main(["--dry-run"])

        out = capsys.readouterr().out
        assert "Recommended: none" in out
        assert "c3c3c3c3" in out

    def test_auto_select_picks_the_best_ranked_version(self, root, capsys):
        _write_conf(root, training={"hpo_objective": "macro_per_item_map"})
        self._two_versions_and_an_old_one(root)

        main([])

        assert self._best(root) == "b2b2b2b2"
        # The versions left out are on screen before anything is promoted.
        assert "c3c3c3c3" in capsys.readouterr().out

    def test_auto_select_picks_nothing_when_no_version_is_ranked(
        self, root, capsys,
    ):
        _write_conf(root)
        _create_full_version(root / "data" / "models", "c3c3c3c3", 0.99)

        with pytest.raises(SystemExit) as exc:
            main([])

        assert exc.value.code == 1
        assert self._best(root) is None
        assert "c3c3c3c3" in capsys.readouterr().out

    @pytest.mark.parametrize("argv, best", [
        ([], "b2b2b2b2"),
        (["--env", "prod"], "a1a1a1a1"),
    ])
    def test_the_env_overlay_is_read(self, root, argv, best):
        _write_conf(root, training={"hpo_objective": "macro_per_item_map"})
        (root / "conf" / "prod").mkdir()
        (root / "conf" / "prod" / "parameters_training.yaml").write_text(
            yaml.dump({"test_metrics": {"selection_metric": "mean_ap"}}))
        self._two_versions_and_an_old_one(root)

        main(argv)

        assert self._best(root) == best

    @pytest.mark.parametrize("dry_run", [[], ["--dry-run"]])
    @pytest.mark.parametrize("conf, env, code", [
        # A check only the training command runs at its entry (A53).
        ({"test_metrics": {"selection_metric": "no_such_metric"}}, "local",
         "(A53)"),
        # One every command runs at its entry (A54): the selection metric,
        # following the HPO objective, cannot be scored on this test.
        ({"training": {"hpo_objective": "pooled_average_precision"}}, "local",
         "A54: the selection metric 'pooled_average_precision'"),
        # No scored month at all (A36).
        ({"test_snap_dates": []}, "local", "(A36)"),
        # An --env naming no conf/ directory (A30).
        ({}, "no_such_env", "(A30)"),
    ])
    def test_invalid_config_stops_before_anything_is_picked(
        self, root, capsys, dry_run, conf, env, code,
    ):
        _write_conf(root, **conf)
        self._two_versions_and_an_old_one(root)

        with pytest.raises(SystemExit) as exc:
            main([*dry_run, "--env", env])

        assert exc.value.code == 1
        assert code in capsys.readouterr().err
        assert self._best(root) is None

    def test_a_named_version_is_promoted_without_reading_the_config(
        self, root,
    ):
        """No conf/ at all: naming the version picks nothing, so the command
        works as it did before promote read the configuration."""
        _create_full_version(root / "data" / "models", "c3c3c3c3", 0.99)

        main(["c3c3c3c3"])

        assert self._best(root) == "c3c3c3c3"

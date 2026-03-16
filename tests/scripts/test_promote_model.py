"""Tests for promote_model script."""

import json

import pytest

from scripts.promote_model import (
    REQUIRED_ARTIFACTS,
    find_best_version,
    promote,
    validate_version,
)


def _create_full_version(models_dir, version, overall_map=0.75, per_product_ap=None):
    """Create a version directory with all required artifacts."""
    version_dir = models_dir / version
    version_dir.mkdir(parents=True, exist_ok=True)

    eval_results = {
        "overall_map": overall_map,
        "per_product_ap": per_product_ap or {"fx": 0.8},
    }
    (version_dir / "evaluation_results.json").write_text(json.dumps(eval_results))
    (version_dir / "best_params.json").write_text(json.dumps({"lr": 0.1}))
    (version_dir / "model.pkl").write_bytes(b"fake_model")


class TestFindBestVersion:
    def test_selects_highest_map(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260315_100000", 0.65)
        _create_full_version(models_dir, "20260316_100000", 0.80)
        _create_full_version(models_dir, "20260317_100000", 0.72)

        assert find_best_version(models_dir) == "20260316_100000"

    def test_returns_none_when_empty(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        assert find_best_version(models_dir) is None


class TestValidateVersion:
    def test_complete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260316_100000")
        assert validate_version(models_dir / "20260316_100000") == []

    def test_incomplete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        version_dir = models_dir / "20260316_100000"
        version_dir.mkdir(parents=True)
        (version_dir / "model.pkl").write_bytes(b"fake")

        missing = validate_version(version_dir)
        assert len(missing) == 2  # missing best_params.json, evaluation_results.json


class TestPromote:
    def test_promote_specific_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260316_100000", 0.80, {"fx": 0.85, "usd": 0.75})

        summary = promote(models_dir, "20260316_100000")
        assert summary["promoted_version"] == "20260316_100000"
        assert summary["overall_map"] == 0.80

        # Verify best/ has all artifacts
        best_dir = models_dir / "best"
        for artifact in REQUIRED_ARTIFACTS:
            assert (best_dir / artifact).exists()

    def test_promote_replaces_existing_best(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260315_100000", 0.65)
        _create_full_version(models_dir, "20260316_100000", 0.80)

        # Promote first version
        promote(models_dir, "20260315_100000")
        # Promote second version — should replace
        promote(models_dir, "20260316_100000")

        with open(models_dir / "best" / "evaluation_results.json") as f:
            results = json.load(f)
        assert results["overall_map"] == 0.80

    def test_promote_nonexistent_version(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        with pytest.raises(SystemExit):
            promote(models_dir, "99990101_000000")

    def test_promote_incomplete_artifacts(self, tmp_path):
        models_dir = tmp_path / "models"
        version_dir = models_dir / "20260316_100000"
        version_dir.mkdir(parents=True)
        (version_dir / "model.pkl").write_bytes(b"fake")

        with pytest.raises(SystemExit):
            promote(models_dir, "20260316_100000")

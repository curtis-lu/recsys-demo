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
        "per_product_ap": per_product_ap or {"exchange_fx": 0.8},
    }
    (version_dir / "evaluation_results.json").write_text(json.dumps(eval_results))
    (version_dir / "best_params.json").write_text(json.dumps({"lr": 0.1}))
    (version_dir / "model.pkl").write_bytes(b"fake_model")


class TestFindBestVersion:
    def test_selects_highest_map_timestamp(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260315_100000", 0.65)
        _create_full_version(models_dir, "20260316_100000", 0.80)
        _create_full_version(models_dir, "20260317_100000", 0.72)

        assert find_best_version(models_dir) == "20260316_100000"

    def test_selects_highest_map_hash(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.65)
        _create_full_version(models_dir, "e5f6a7b8", 0.80)

        assert find_best_version(models_dir) == "e5f6a7b8"

    def test_selects_across_mixed_formats(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "20260315_100000", 0.65)
        _create_full_version(models_dir, "a1b2c3d4", 0.80)

        assert find_best_version(models_dir) == "a1b2c3d4"

    def test_returns_none_when_empty(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        assert find_best_version(models_dir) is None

    def test_ignores_best_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4", 0.80)
        # Create a best symlink - should be ignored
        (models_dir / "best").symlink_to((models_dir / "a1b2c3d4").resolve())

        assert find_best_version(models_dir) == "a1b2c3d4"


class TestValidateVersion:
    def test_complete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_full_version(models_dir, "a1b2c3d4")
        assert validate_version(models_dir / "a1b2c3d4") == []

    def test_incomplete_version(self, tmp_path):
        models_dir = tmp_path / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        (version_dir / "model.pkl").write_bytes(b"fake")

        missing = validate_version(version_dir)
        assert len(missing) == 2  # missing best_params.json, evaluation_results.json


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

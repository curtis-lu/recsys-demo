"""Tests for recsys_tfb.core.versioning module."""

import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest

from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_dataset_version,
    compute_model_version,
    get_git_commit,
    read_manifest,
    resolve_dataset_version,
    resolve_model_version,
    update_symlink,
    write_manifest,
)

_HEX8_RE = re.compile(r"^[0-9a-f]{8}$")


class TestComputeDatasetVersion:
    def test_returns_8_char_hex(self):
        result = compute_dataset_version({"sample_ratio": 0.1})
        assert _HEX8_RE.match(result)

    def test_same_params_same_hash(self):
        params = {"sample_ratio": 0.1, "seed": 42}
        assert compute_dataset_version(params) == compute_dataset_version(params)

    def test_different_params_different_hash(self):
        a = compute_dataset_version({"sample_ratio": 0.1})
        b = compute_dataset_version({"sample_ratio": 0.2})
        assert a != b

    def test_key_order_does_not_matter(self):
        a = compute_dataset_version({"a": 1, "b": 2})
        b = compute_dataset_version({"b": 2, "a": 1})
        assert a == b


class TestComputeModelVersion:
    def test_returns_8_char_hex(self):
        result = compute_model_version({"lr": 0.01}, "abc12345")
        assert _HEX8_RE.match(result)

    def test_same_params_same_dataset_same_hash(self):
        params = {"lr": 0.01}
        a = compute_model_version(params, "abc12345")
        b = compute_model_version(params, "abc12345")
        assert a == b

    def test_different_dataset_different_hash(self):
        params = {"lr": 0.01}
        a = compute_model_version(params, "abc12345")
        b = compute_model_version(params, "def67890")
        assert a != b

    def test_different_params_different_hash(self):
        a = compute_model_version({"lr": 0.01}, "abc12345")
        b = compute_model_version({"lr": 0.05}, "abc12345")
        assert a != b


class TestWriteManifest:
    def test_writes_json_file(self, tmp_path):
        version_dir = tmp_path / "v1"
        metadata = {"version": "abc12345", "pipeline": "dataset"}
        write_manifest(version_dir, metadata)

        manifest_path = version_dir / "manifest.json"
        assert manifest_path.exists()
        with open(manifest_path) as f:
            data = json.load(f)
        assert data == metadata

    def test_creates_parent_dirs(self, tmp_path):
        version_dir = tmp_path / "deep" / "nested" / "v1"
        write_manifest(version_dir, {"version": "test"})
        assert (version_dir / "manifest.json").exists()

    def test_overwrites_existing(self, tmp_path):
        version_dir = tmp_path / "v1"
        write_manifest(version_dir, {"version": "old"})
        write_manifest(version_dir, {"version": "new"})
        data = read_manifest(version_dir)
        assert data["version"] == "new"


class TestReadManifest:
    def test_reads_existing(self, tmp_path):
        version_dir = tmp_path / "v1"
        version_dir.mkdir()
        (version_dir / "manifest.json").write_text(json.dumps({"version": "abc"}))
        assert read_manifest(version_dir) == {"version": "abc"}

    def test_raises_when_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_manifest(tmp_path / "nonexistent")


class TestUpdateSymlink:
    def test_create_new_symlink(self, tmp_path):
        target = tmp_path / "v1"
        target.mkdir()
        link = tmp_path / "latest"

        update_symlink(target, link)

        assert link.is_symlink()
        assert link.resolve() == target.resolve()

    def test_update_existing_symlink(self, tmp_path):
        v1 = tmp_path / "v1"
        v1.mkdir()
        v2 = tmp_path / "v2"
        v2.mkdir()
        link = tmp_path / "latest"

        update_symlink(v1, link)
        update_symlink(v2, link)

        assert link.is_symlink()
        assert link.resolve() == v2.resolve()

    def test_replace_existing_directory(self, tmp_path):
        old_dir = tmp_path / "best"
        old_dir.mkdir()
        (old_dir / "model.pkl").write_bytes(b"fake")

        target = tmp_path / "v1"
        target.mkdir()

        update_symlink(target, old_dir)

        assert old_dir.is_symlink()
        assert old_dir.resolve() == target.resolve()


class TestResolveDatasetVersion:
    def test_returns_specified_version(self, tmp_path):
        assert resolve_dataset_version(tmp_path, "abc12345") == "abc12345"

    def test_follows_latest_symlink(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        v1 = dataset_dir / "abc12345"
        v1.mkdir()
        latest = dataset_dir / "latest"
        latest.symlink_to(v1.resolve())

        assert resolve_dataset_version(dataset_dir, None) == "abc12345"

    def test_raises_when_no_latest(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="latest"):
            resolve_dataset_version(dataset_dir, None)


class TestResolveModelVersion:
    def test_returns_specified_version(self, tmp_path):
        assert resolve_model_version(tmp_path, "abc12345") == "abc12345"

    def test_follows_best_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        v1 = models_dir / "abc12345"
        v1.mkdir()
        best = models_dir / "best"
        best.symlink_to(v1.resolve())

        assert resolve_model_version(models_dir, None) == "abc12345"

    def test_best_is_directory_returns_best(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        best = models_dir / "best"
        best.mkdir()

        assert resolve_model_version(models_dir, None) == "best"

    def test_raises_when_no_best(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="best"):
            resolve_model_version(models_dir, None)


class TestGetGitCommit:
    def test_returns_string_in_git_repo(self):
        commit = get_git_commit()
        # We're running in a git repo
        assert commit is not None
        assert len(commit) >= 7

    def test_returns_none_when_git_unavailable(self):
        with patch("recsys_tfb.core.versioning.subprocess.run", side_effect=FileNotFoundError):
            assert get_git_commit() is None


class TestBuildManifestMetadata:
    def test_dataset_manifest(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            artifacts=["train_set.parquet"],
        )
        assert meta["version"] == "abc12345"
        assert meta["pipeline"] == "dataset"
        assert "created_at" in meta
        assert "git_commit" in meta
        assert meta["parameters"] == {"sample_ratio": 0.1}
        assert meta["artifacts"] == ["train_set.parquet"]
        assert "dataset_version" not in meta
        assert "model_version" not in meta

    def test_training_manifest(self):
        meta = build_manifest_metadata(
            version="def67890",
            pipeline="training",
            parameters={"lr": 0.01},
            dataset_version="abc12345",
            artifacts=["model.pkl"],
        )
        assert meta["dataset_version"] == "abc12345"
        assert "model_version" not in meta

    def test_inference_manifest(self):
        meta = build_manifest_metadata(
            version="best",
            pipeline="inference",
            parameters={"snap_dates": ["2024-03-31"]},
            model_version="def67890",
            dataset_version="abc12345",
        )
        assert meta["model_version"] == "def67890"
        assert meta["dataset_version"] == "abc12345"

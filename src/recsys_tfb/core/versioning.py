"""Version management for pipeline artifacts.

Provides hash-based version IDs, manifest generation, symlink management,
and version resolution for dataset, training, and inference pipelines.
"""

import hashlib
import json
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def compute_dataset_version(params: dict) -> str:
    """Compute dataset version ID from parameters_dataset content.

    Returns the first 8 hex characters of the SHA-256 hash of the
    canonical YAML representation of the parameters dict.
    """
    canonical = yaml.dump(params, sort_keys=True, default_flow_style=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:8]


def compute_model_version(params: dict, dataset_version: str) -> str:
    """Compute model version ID from training parameters and dataset version.

    Returns the first 8 hex characters of the SHA-256 hash of the
    canonical YAML representation concatenated with the dataset_version.
    """
    canonical = yaml.dump(params, sort_keys=True, default_flow_style=False)
    combined = canonical + dataset_version
    return hashlib.sha256(combined.encode()).hexdigest()[:8]


def write_manifest(version_dir: Path, metadata: dict) -> None:
    """Write metadata as manifest.json in the version directory."""
    version_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = version_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    logger.info("Wrote manifest: %s", manifest_path)


def read_manifest(version_dir: Path) -> dict:
    """Read and return manifest.json from a version directory.

    Raises FileNotFoundError if manifest.json does not exist.
    """
    manifest_path = version_dir / "manifest.json"
    with open(manifest_path) as f:
        return json.load(f)


def update_symlink(target: Path, link: Path) -> None:
    """Create or update a symlink at *link* pointing to *target*.

    If *link* already exists as a symlink, it is removed first.
    If *link* already exists as a directory (e.g. old-style best/),
    the directory is removed first.
    """
    if link.is_symlink():
        link.unlink()
    elif link.is_dir():
        shutil.rmtree(link)

    link.symlink_to(target.resolve())
    logger.info("Symlink %s -> %s", link, target)


def resolve_dataset_version(dataset_dir: Path, version: str | None) -> str:
    """Resolve which dataset version to use.

    If *version* is provided, return it directly.
    If None, follow the ``latest`` symlink under *dataset_dir*.

    Raises FileNotFoundError if latest symlink does not exist.
    """
    if version is not None:
        return version

    latest = dataset_dir / "latest"
    if not latest.exists():
        raise FileNotFoundError(
            f"No 'latest' symlink found in {dataset_dir}. "
            "Run the dataset pipeline first or specify --dataset-version."
        )
    return latest.resolve().name


def resolve_model_version(models_dir: Path, version: str | None) -> str:
    """Resolve which model version to use.

    If *version* is provided, return it directly.
    If None, follow the ``best`` symlink under *models_dir*.

    Raises FileNotFoundError if best symlink does not exist.
    """
    if version is not None:
        return version

    best = models_dir / "best"
    if not best.exists():
        raise FileNotFoundError(
            f"No 'best' symlink found in {models_dir}. "
            "Run training and promote a model first."
        )
    # If best is a symlink, resolve to get the version directory name
    if best.is_symlink():
        return best.resolve().name
    # If best is a directory (old format), return "best"
    return "best"


def get_git_commit() -> str | None:
    """Return the short git HEAD commit hash, or None if not in a repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def build_manifest_metadata(
    *,
    version: str,
    pipeline: str,
    parameters: dict,
    dataset_version: str | None = None,
    model_version: str | None = None,
    artifacts: list[str] | None = None,
) -> dict:
    """Build a manifest metadata dict with standard fields."""
    metadata: dict = {
        "version": version,
        "pipeline": pipeline,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": get_git_commit(),
        "parameters": parameters,
    }
    if dataset_version is not None:
        metadata["dataset_version"] = dataset_version
    if model_version is not None:
        metadata["model_version"] = model_version
    if artifacts is not None:
        metadata["artifacts"] = artifacts
    return metadata

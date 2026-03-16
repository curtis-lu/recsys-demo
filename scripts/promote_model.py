"""Promote a versioned model to best/ for inference use.

Usage:
    python scripts/promote_model.py a1b2c3d4                  # specific version (hash)
    python scripts/promote_model.py 20260316_153000           # specific version (timestamp)
    python scripts/promote_model.py                            # auto-select best mAP
    python scripts/promote_model.py --models-dir /custom/path  # custom directory
"""

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

REQUIRED_ARTIFACTS = [
    "model.pkl",
    "best_params.json",
    "evaluation_results.json",
]

_VERSION_TIMESTAMP_RE = re.compile(r"^\d{8}_\d{6}$")
_VERSION_HASH_RE = re.compile(r"^[0-9a-f]{8}$")


def _is_version_dir(name: str) -> bool:
    """Check if a directory name matches a known version format."""
    return bool(_VERSION_TIMESTAMP_RE.match(name) or _VERSION_HASH_RE.match(name))


def find_best_version(models_dir: Path) -> str | None:
    """Find the version with highest overall_map."""
    best_version = None
    best_map = -1.0

    for d in models_dir.iterdir():
        if not d.is_dir() or d.is_symlink() or not _is_version_dir(d.name):
            continue
        eval_path = d / "evaluation_results.json"
        if not eval_path.exists():
            continue
        with open(eval_path) as f:
            results = json.load(f)
        overall_map = results.get("overall_map", 0.0)
        if overall_map > best_map:
            best_map = overall_map
            best_version = d.name

    return best_version


def validate_version(version_dir: Path) -> list[str]:
    """Return list of missing artifacts."""
    return [a for a in REQUIRED_ARTIFACTS if not (version_dir / a).exists()]


def promote(models_dir: Path, version: str) -> dict:
    """Create a symlink best/ -> version/. Returns summary dict."""
    version_dir = models_dir / version
    if not version_dir.is_dir():
        print(f"Error: version directory not found: {version_dir}", file=sys.stderr)
        sys.exit(1)

    missing = validate_version(version_dir)
    if missing:
        print(f"Error: incomplete artifacts in {version}. Missing: {missing}", file=sys.stderr)
        sys.exit(1)

    best_dir = models_dir / "best"
    # Remove existing best (symlink or directory)
    if best_dir.is_symlink():
        best_dir.unlink()
    elif best_dir.is_dir():
        shutil.rmtree(best_dir)

    # Create symlink
    best_dir.symlink_to(version_dir.resolve())

    # Read evaluation for summary
    with open(version_dir / "evaluation_results.json") as f:
        eval_results = json.load(f)

    summary = {
        "promoted_version": version,
        "overall_map": eval_results.get("overall_map"),
        "per_product_ap": eval_results.get("per_product_ap", {}),
        "target_path": str(best_dir),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote a model version to best/")
    parser.add_argument("version", nargs="?", default=None, help="Version ID (auto-select if omitted)")
    parser.add_argument("--models-dir", default="data/models", help="Models directory")
    args = parser.parse_args()

    models_dir = Path(args.models_dir)
    if not models_dir.is_dir():
        print(f"Error: models directory not found: {models_dir}", file=sys.stderr)
        sys.exit(1)

    version = args.version
    if version is None:
        version = find_best_version(models_dir)
        if version is None:
            print("Error: no valid model versions found", file=sys.stderr)
            sys.exit(1)
        print(f"Auto-selected best version: {version}")

    summary = promote(models_dir, version)

    print(f"\nPromoted: {summary['promoted_version']}")
    print(f"  mAP: {summary['overall_map']:.4f}")
    for prod, ap in sorted(summary["per_product_ap"].items()):
        print(f"  {prod}: {ap:.4f}")


if __name__ == "__main__":
    main()

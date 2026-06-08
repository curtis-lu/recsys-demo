"""Version management for pipeline artifacts.

Provides three-layer hash-based version IDs for dataset pipeline:

- ``base_dataset_version``: derived from non-sampling dataset params + full
  schema. Keys outputs that are invariant under sampling changes (preprocessor,
  category_mappings, preprocessed_feature_table, val/test model_input).
- ``train_variant_id``: derived from train-sampling params only. Keys
  train/train_dev model_input under the base dataset directory.
- ``calibration_variant_id``: derived from calibration-sampling params only.
  Keys calibration model_input under the base dataset directory.
- ``model_version``: derived from the *model-defining* subset of training
  params only — the ``training:`` block minus the pure logging/threading
  knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS``. Ops-only config
  (``spark`` / ``mlflow`` / ``cache``) is excluded structurally so changing
  it does not orphan an otherwise-identical model.

Also provides manifest generation, symlink management, and version resolution
for dataset, training, and inference pipelines.
"""

import copy
import hashlib
import json
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


TRAIN_SAMPLING_KEYS: frozenset[str] = frozenset({
    "sample_ratio",
    "sample_ratio_overrides",
    "sample_group_keys",
    "train_dev_ratio",
})
CALIBRATION_SAMPLING_KEYS: frozenset[str] = frozenset({
    "calibration_sample_ratio",
    "calibration_sample_ratio_overrides",
    "sample_group_keys",
})
ALL_SAMPLING_KEYS: frozenset[str] = TRAIN_SAMPLING_KEYS | CALIBRATION_SAMPLING_KEYS


# Keys under training.algorithm_params that do NOT affect the trained model
# (pure logging / threading). Excluded from the model_version hash so changing
# them does not orphan an otherwise-identical model. num_threads is treated as
# irrelevant by decision: LightGBM is not guaranteed bitwise-identical across
# thread counts, but it is pinned to the production core count and rarely
# changes (see the design spec, Decision 1).
MODEL_VERSION_IRRELEVANT_PARAMS: frozenset[str] = frozenset({
    "verbosity",
    "log_period",
    "num_threads",
})


def _hash8(payload: dict) -> str:
    canonical = yaml.dump(payload, sort_keys=True, default_flow_style=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:8]


def compute_feature_table_fingerprint(columns) -> str:
    """Hash an ordered (name, dtype) sequence describing feature_table schema.

    Order matters: feature_table column order propagates into ``feature_columns``
    in :mod:`recsys_tfb.preprocessing`, which determines the LightGBM feature
    ordering. Reordering columns changes downstream model inputs, so it must
    bust the version.
    """
    payload = {"feature_table_columns": [list(item) for item in columns]}
    return _hash8(payload)


def compute_base_dataset_version(
    params: dict,
    schema: dict,
    feature_table_fingerprint: str | None = None,
) -> str:
    """Hash non-sampling dataset params, canonical schema, and feature_table fingerprint.

    The resulting ID keys pipeline outputs that are invariant under sampling
    changes. ``params`` is the ``parameters_dataset`` dict; any keys in
    ``ALL_SAMPLING_KEYS`` under ``params["dataset"]`` are stripped before
    hashing so train/calibration sampling experiments do not invalidate
    val/test/preprocessor artifacts.

    ``feature_table_fingerprint`` (optional) reflects the actual
    ``feature_table`` schema (column name + dtype, ordered). When provided it
    busts the version on schema changes so the dataset cache cannot collide
    with a different physical input. ``None`` preserves legacy hashing for
    backward compatibility.
    """
    stripped = copy.deepcopy(params)
    ds = stripped.get("dataset")
    if isinstance(ds, dict):
        for key in ALL_SAMPLING_KEYS:
            ds.pop(key, None)
    payload: dict = {"dataset": stripped, "schema": schema}
    if feature_table_fingerprint is not None:
        payload["feature_table_fingerprint"] = feature_table_fingerprint
    return _hash8(payload)


def compute_train_variant_id(params: dict) -> str:
    """Hash only the train-sampling subset of dataset params."""
    ds = params.get("dataset", {}) if isinstance(params, dict) else {}
    subset = {k: ds[k] for k in TRAIN_SAMPLING_KEYS if k in ds}
    return _hash8({"train_sampling": subset})


def compute_calibration_variant_id(params: dict) -> str:
    """Hash only the calibration-sampling subset of dataset params."""
    ds = params.get("dataset", {}) if isinstance(params, dict) else {}
    subset = {k: ds[k] for k in CALIBRATION_SAMPLING_KEYS if k in ds}
    return _hash8({"calibration_sampling": subset})


def _model_version_payload(params: dict) -> dict:
    """Return the model-defining view of training params for hashing.

    Only the ``training:`` block defines the trained artifact; top-level
    ``spark`` / ``mlflow`` / ``cache`` (and any future ops block) are excluded
    structurally by narrowing here. Within ``training.algorithm_params`` the
    pure logging/threading knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS`` are
    dropped. A new key *under* ``training:`` defaults to being included — safe
    over-invalidation, never a silent ``model_version`` collision.

    Deep-copies so the caller's params dict is never mutated: the full,
    unscoped params are still written to ``manifest.json`` for provenance.
    """
    training = params.get("training")
    if not isinstance(training, dict):
        return {}
    training = copy.deepcopy(training)
    ap = training.get("algorithm_params")
    if isinstance(ap, dict):
        for key in MODEL_VERSION_IRRELEVANT_PARAMS:
            ap.pop(key, None)
    payload: dict = {"training": training}
    # The category table defines the model ONLY when Stage-1 groups by category.
    # Fold it in then (so editing the mapping bumps model_version); leave it out
    # otherwise to avoid spurious invalidation of shared / per-item models.
    if (
        training.get("model_structure") == "per_group_plus_rank"
        and (training.get("stage1", {}) or {}).get("grouping") == "category"
    ):
        payload["product_categories"] = params.get("product_categories")
    return payload


def compute_model_version(
    params: dict,
    base_dataset_version: str,
    train_variant_id: str,
    calibration_variant_id: str | None = None,
) -> str:
    """Compute model version ID from model-defining training params + variants.

    Only the model-defining subset of ``params`` is hashed (see
    :func:`_model_version_payload`): the ``training:`` block minus the pure
    logging/threading knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS``. Changing
    ops-only config (``spark`` / ``mlflow`` / ``cache``, ``verbosity``,
    ``log_period``, ``num_threads``) therefore does not change the version.
    """
    canonical = yaml.dump(
        _model_version_payload(params),
        sort_keys=True,
        default_flow_style=False,
    )
    parts = [canonical, base_dataset_version, train_variant_id]
    if calibration_variant_id is not None:
        parts.append(calibration_variant_id)
    combined = "".join(parts)
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


def resolve_base_dataset_version(dataset_dir: Path, version: str | None) -> str:
    """Resolve which base dataset version to use.

    If *version* is provided, return it directly. Otherwise follow the
    ``latest`` symlink under *dataset_dir*.
    """
    if version is not None:
        return version

    latest = dataset_dir / "latest"
    if not latest.exists():
        raise FileNotFoundError(
            f"No 'latest' symlink found in {dataset_dir}. "
            "Run the dataset pipeline first or specify --base-dataset-version."
        )
    return latest.resolve().name


def resolve_variant_id(base_dir: Path, variant_kind: str, variant: str | None) -> str:
    """Resolve a train/calibration variant ID under a base dataset directory.

    ``variant_kind`` must be ``"train"`` or ``"calibration"``. If *variant* is
    provided, return it directly. Otherwise follow the ``latest`` symlink
    inside ``{base_dir}/{variant_kind}_variants``.
    """
    if variant_kind not in ("train", "calibration"):
        raise ValueError(
            f"variant_kind must be 'train' or 'calibration', got {variant_kind!r}"
        )

    if variant is not None:
        return variant

    variants_root = base_dir / f"{variant_kind}_variants"
    latest = variants_root / "latest"
    if not latest.exists():
        raise FileNotFoundError(
            f"No 'latest' symlink found in {variants_root}. "
            f"Run the dataset pipeline first or specify --{variant_kind}-variant."
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
    base_dataset_version: str | None = None,
    train_variant_id: str | None = None,
    calibration_variant_id: str | None = None,
    model_version: str | None = None,
    parent_version: str | None = None,
    variant_kind: str | None = None,
    feature_table_fingerprint: str | None = None,
    artifacts: list[str] | None = None,
) -> dict:
    """Build a manifest metadata dict with standard fields.

    ``parent_version`` and ``variant_kind`` are written on variant sub-directory
    manifests to link them back to their base dataset manifest.
    ``feature_table_fingerprint`` is written on dataset base manifests so the
    physical feature_table schema at run time is recoverable from manifest.
    """
    metadata: dict = {
        "version": version,
        "pipeline": pipeline,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": get_git_commit(),
        "parameters": parameters,
    }
    if base_dataset_version is not None:
        metadata["base_dataset_version"] = base_dataset_version
    if train_variant_id is not None:
        metadata["train_variant_id"] = train_variant_id
    if calibration_variant_id is not None:
        metadata["calibration_variant_id"] = calibration_variant_id
    if model_version is not None:
        metadata["model_version"] = model_version
    if parent_version is not None:
        metadata["parent_version"] = parent_version
    if variant_kind is not None:
        metadata["variant_kind"] = variant_kind
    if feature_table_fingerprint is not None:
        metadata["feature_table_fingerprint"] = feature_table_fingerprint
    if artifacts is not None:
        metadata["artifacts"] = artifacts
    return metadata

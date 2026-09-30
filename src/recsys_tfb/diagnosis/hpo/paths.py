"""Where HPO's search diagnostics land."""

from pathlib import Path

from recsys_tfb.io.models_root import diagnostics_dir


def hpo_dir(parameters: dict) -> Path:
    """Resolve (and create) ``diagnostics/hpo/``: the HPO search diagnostics."""
    d = diagnostics_dir(parameters) / "hpo"
    d.mkdir(parents=True, exist_ok=True)
    return d

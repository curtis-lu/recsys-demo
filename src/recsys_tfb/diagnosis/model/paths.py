"""Where a model version's diagnostics directory is.

Only the directory itself is resolved here: ``log_experiment`` uploads it
whole, and HPO's search diagnostics (``diagnosis/hpo/paths.py``) write under
it. That second reader is why this stayed in the library when the diagnosis
nodes and their mechanisms moved into the training pipeline (ADR-0030
decision 6): ``diagnosis/hpo`` is a library module, and a library module may
not import a pipeline (S8) or reach into its ``steps/`` (S3). The figure
subdirectories (``summary/``, ``cases/``) belong to their catalog entries
(``shap_summary_figures``, ``case_figures``), which write them (ADR-0030
decision 7).
"""

from pathlib import Path

from recsys_tfb.io.models_root import MODELS_ROOT


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve (and create) the diagnostics directory: the one the catalog's
    ``data/models/${model_version}/diagnostics/`` entries write into.

    Recomputed here rather than read off a catalog entry because both of its
    callers want the directory, not one of the files in it. It names the
    catalog's directory because ``parameters["model_version"]`` is the value
    the CLI substituted into the catalog's paths (the same runtime parameters
    resolve both) — and only as long as ``io/models_root.MODELS_ROOT`` is the
    catalog's root.
    """
    mv = parameters["model_version"]
    d = MODELS_ROOT / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d

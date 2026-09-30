"""Where every model version's artifacts land: ``data/models``.

The same root the catalog's ``data/models/${model_version}/...`` entries write
under (``conf/base/catalog.yaml``: the model, its reports, its diagnostics).
This copy is for the code that builds such a path itself instead of going
through a catalog entry: ``diagnosis/model/paths.diagnostics_dir`` (the
directory ``log_experiment`` uploads and HPO's search diagnostics write under)
and ``pipelines/training/steps/hpo_resume.hpo_study_dir`` (the crash-resumable
HPO study, which has no catalog entry at all). Those two used to spell the
root out once each; ADR-0030 decision 16 collected them here.

**The root is spelled in three places, and nothing checks that they agree:**

1. the catalog's entries (``conf/base/catalog.yaml``);
2. this constant;
3. the CLI, as ``data_dir / "models"`` with ``data_dir = _find_data_dir()``
   (``__main__.py``: three times in ``training`` — the manifest stub, the
   retrain advice and the post-run manifest's version directory — and once
   each in ``inference`` and ``evaluation``, where it resolves which model
   version to read). Not collected here: out of scope for #489.

Move the catalog's root and all three have to move with it. If they drift
apart nothing raises: ``diagnostics_dir`` creates its directory wherever this
points, so ``log_experiment`` would upload an empty directory the catalog
never wrote to, and the search diagnostics would land beside no model.

Relative on purpose, like the catalog's own paths and
``io/disk_matrix.SCRATCH_ROOT``: resolved against the directory the command
runs in, so a worktree writes under its own tree.
"""

from pathlib import Path

MODELS_ROOT: Path = Path("data") / "models"

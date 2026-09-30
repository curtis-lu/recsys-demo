"""Where every model version's artifacts land: ``data/models``.

The same root the catalog's ``data/models/${model_version}/...`` entries write
under (``conf/base/catalog.yaml``: the model, its reports, its diagnostics).
This copy is for the code that builds such a path itself instead of going
through a catalog entry: ``diagnosis/model/paths.diagnostics_dir`` (the
directory ``log_experiment`` uploads and HPO's search diagnostics write under)
and ``pipelines/training/steps/hpo_resume.hpo_study_dir`` (the crash-resumable
HPO study, which has no catalog entry at all).

**Move the catalog's root and this has to move with it; nothing checks that the
two agree.** If they drift apart nothing raises: ``diagnostics_dir`` creates
its directory wherever this points, so ``log_experiment`` would upload an
empty directory the catalog never wrote to, and the search diagnostics would
land beside no model. It used to be spelled out twice, once in each of those
two functions (ADR-0030 decision 16).

Relative on purpose, like the catalog's own paths and
``io/disk_matrix.SCRATCH_ROOT``: resolved against the directory the command
runs in, so a worktree writes under its own tree.
"""

from pathlib import Path

MODELS_ROOT: Path = Path("data") / "models"

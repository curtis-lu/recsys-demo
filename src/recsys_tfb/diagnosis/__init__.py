"""The diagnosis library: "symptom → cause" diagnoses of a ranking model.

**This package only presents the data and its limits faithfully. It makes no
verdicts, recommends no actions, and cuts no continuous quantity into
categories with a threshold — judging is the reader's job.** That is a design
invariant, not a style preference: ``triage`` (a per-item verdict plus the
lever to pull) and ``quadrant`` (quadrants cut at an AUC threshold) once
existed here, and both layers were retired for breaking it. Before adding a
module, check that it is not walking down that road again.

- ``diagnosis.metric`` — metric-level diagnoses, run on the shared diagnosis
  sample. The evaluation pipeline's nodes call it. (Its submodules are
  deliberately not listed here: the list changes often, and a copy here would
  drift.)
- ``diagnosis.hpo`` — the HPO search diagnostics ``tune_hyperparameters``
  writes.
- ``diagnosis.model`` — only ``diagnostics_dir`` now. The model-structure
  diagnoses (SHAP, importance, feature statistics, the quadrant population
  and cases, the gain ledger) had training as their only consumer, so their
  nodes are in ``pipelines/training/nodes.py`` and their mechanisms in
  ``pipelines/training/steps/`` (ADR-0030 decision 6). ``diagnostics_dir``
  stayed because ``diagnosis.hpo`` writes under the same directory.

Dependency direction (one way; breaking it is an error, see spec §1
invariant 4, and S8 in ``docs/agents/architecture-constraints.md`` checks
it): ``pipelines/* → diagnosis → core / evaluation (only the numpy primitives
in metrics.py) / io / utils``; this package must not import any
``pipelines/*``. The methodology is in docs/ranking-diagnosis-framework.md.
"""

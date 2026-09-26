"""Mechanism modules the dataset nodes call: one concern per module.

Placement criterion, one sentence, and ``docs/agents/architecture-constraints.md``
(the S2 section) is where it is registered: a module lives here when every
**src-side** caller is inside ``pipelines/dataset/``. Test modules import these
directly and that moves nothing -- the criterion is about production callers,
because what it buys is a reader telling this pipeline's outward contract from
its internals by looking at one directory listing.

Two modules sit at the package root by that criterion: ``run_contract.py``,
which ``__main__.py``'s dataset command asks before and after a run for the
versions, the month plans and what to inject through the catalog (ADR-0029
decision 11); and ``month_plans.py``, whose month-plan tools the evaluation
command uses directly too, and whose candidate-level table name the inference
command reads (ADR-0007).

Nothing is re-exported here on purpose: ``nodes.py`` imports each module by
name, so the import line says which concern a step came from.
"""

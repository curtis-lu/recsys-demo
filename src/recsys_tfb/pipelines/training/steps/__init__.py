"""Mechanism modules the training nodes call: one concern per module.

Placement criterion, one sentence (``docs/agents/pipeline-node-design.md``
rule 8 is where it is written down): a module lives here when every
**src-side** caller is inside ``pipelines/training/``. Test modules import
these directly and that moves nothing — the criterion is about production
callers, because what it buys is a reader telling this pipeline's outward
contract from its internals by reading one directory listing.

**The listing now says it** (issue #234). ``search_space.py`` and
``hpo_resume.py`` moved in from the package root, where they had been sitting
even though every src-side caller of both was already inside
``pipelines/training/`` (``nodes.py`` and ``steps/hpo_scoring.py``); what stayed
at the root is ``cache_sources.py``, whose caller — ``__main__.py``, before the
``DataCatalog`` exists — is the outward one. So the two-line reading of the
directory is now true: root is what other people call, ``steps/`` is what this
pipeline calls itself. **S3** in ``docs/agents/architecture-constraints.md``
keeps it true by failing when any module outside this pipeline imports from
here.

Purity is a module-level property, not a placement one: ``predict_months`` imports
no pyspark and nothing from this project, so the month decisions predict makes are
tested in milliseconds instead of behind a 2-4 minute SparkSession, and
``tests/test_pipelines/test_training/test_predict_months.py`` pins that with its
own AST scan rather than by widening the architecture audit's module-purity
register. Living in ``steps/`` neither grants nor threatens it — ``local_cache``
sits here too and reads the filesystem. What the purity *does* constrain is
direction: ``local_cache`` imports ``month_dir`` from ``predict_months`` and never
the reverse.

Nothing is re-exported here on purpose: ``nodes.py`` imports each module by
name, so the import line says which concern a step came from. That too is S3 —
``from .steps import build_trial_params`` would compile and tell the reader
nothing, so the audit fails on an ``__init__.py`` that holds an import at all.
"""

"""Mechanism modules the training nodes call: one concern per module.

Placement criterion, one sentence (``docs/agents/pipeline-node-design.md``
rule 8 is where it is written down): a module lives here when every
**src-side** caller is inside ``pipelines/training/``. Test modules import
these directly and that moves nothing — the criterion is about production
callers, because what it buys is a reader telling this pipeline's outward
contract from its internals by reading one directory listing.

**This package's listing does not say that yet.** ``search_space.py`` and
``hpo_resume.py`` still sit at the package root although every src-side caller
of both is already inside ``pipelines/training/`` (``nodes.py`` and
``steps/hpo_scoring.py``). Moving them is issue #234, deliberately a separate
change: a pure move is proved by byte-identity, and any behaviour change
landing in the same commit would void that proof. Until it lands, root-level
placement here means "not moved yet", not "outward contract" — do not read the
listing as the criterion until #234 closes.

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
name, so the import line says which concern a step came from.
"""

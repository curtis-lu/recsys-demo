"""Mechanism modules the inference nodes call: one concern per module.

Placement criterion, one sentence (``docs/agents/pipeline-node-design.md`` S-A
is where it is written down): a module lives here when every **src-side** caller
is inside ``pipelines/inference/``. Test modules import these directly and that
moves nothing — the criterion is about production callers, because what it buys
is a reader telling this pipeline's outward contract from its internals by
looking at one directory listing.

**This package's root holds no contract module, and that is information rather
than an omission.** ``pipelines/dataset`` keeps ``month_plans.py`` beside its
``nodes.py`` because ``__main__.py`` builds the month plans before the pipeline
starts and injects them through the catalog (ADR-0007). Inference decides its
chunk plan *inside* a node, so ``chunk_plans`` has no pre-pipeline consumer and
belongs here with the rest.

Purity is a module-level property, not a placement one: ``chunk_plans`` and
``validation``'s chunk half import no pyspark so their tests run in
milliseconds rather than behind a 2-4 minute SparkSession, and
``tests/test_pipelines/test_inference/test_chunk_plans.py`` pins that with its
own AST scan. Living in ``steps/`` neither grants nor threatens it.

Nothing is re-exported here on purpose: ``nodes.py`` imports each module by
name, so the import line says which concern a step came from.
"""

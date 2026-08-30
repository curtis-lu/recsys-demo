"""Which Hive table each driver-local cache is filled from on this run.

This module sits at the package root rather than in ``steps/`` because its one
caller is **outside** this pipeline: ``__main__.py`` calls
:func:`inject_cache_source_tables` before the ``DataCatalog`` is constructed, so
the cache nodes see the derived mapping by the time they run. That is the root
vs ``steps/`` criterion in ``docs/agents/pipeline-node-design.md`` rule 8 — a
module is an outward contract when a src-side caller lives elsewhere — and it
makes this the training twin of ``pipelines/dataset/month_plans.py``, which is
at its own package root for the same reason (ADR-0007, ADR-0014).

The mechanism it reads, :data:`~recsys_tfb.pipelines.training.steps.local_cache.CACHE_SOURCE_TABLES`,
stays in ``steps/local_cache.py`` with the cache machinery that consumes it;
what travels outward is only the derivation.
"""

from recsys_tfb.pipelines.training.steps.local_cache import CACHE_SOURCE_TABLES


def inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Auto-derive cache source_tables from catalog_config and write into parameters.

    Mutates `parameters` to add `_cache_source_tables` mapping (cache logical
    name → actual Hive table name). Cache nodes read this in
    steps.local_cache.populate_cache_from_hive.

    For each known cache name in CACHE_SOURCE_TABLES, look up the catalog entry.
    If present and `type: HiveTableDataset`, take its `table` field. Skips
    entries that aren't HiveTableDataset and missing entries.

    Operates on raw catalog_config dict (not DataCatalog instance) — the yaml
    schema is the public contract; we don't access dataset instance internals.

    No-op (does not write the key) when no cache entries match.

    Called by __main__.py:_execute_pipeline before DataCatalog construction so the
    cache nodes see the auto-derived mapping at runtime.
    """
    auto: dict[str, str] = {}
    for cache_name in CACHE_SOURCE_TABLES:
        entry = catalog_config.get(cache_name)
        if entry and entry.get("type") == "HiveTableDataset":
            table = entry.get("table")
            if table:
                auto[cache_name] = table
    if auto:
        parameters["_cache_source_tables"] = auto

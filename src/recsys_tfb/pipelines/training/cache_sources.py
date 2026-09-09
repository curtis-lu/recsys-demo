"""Which Hive table each driver-local cache is filled from on this run, and how
that table's partitions are laid out on disk.

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

**Partition names come from here too, and that is the point of the module.**
``catalog.yaml`` is where a user declares how their tables are partitioned, so
it is the only place that knows whether the time partition is called
``snap_date`` or something else entirely. The cache used to carry its own copy
of both halves — a ``_CACHE_OUTER_PARTITIONS`` mirror of every entry's
``partition_filter`` keys, and a literal ``snap_date=`` glob — and nothing kept
either in step with the catalog. Deriving both here means renaming a partition
column in ``catalog.yaml`` is the whole change (#326).
"""

from recsys_tfb.pipelines.training.steps.local_cache import CACHE_SOURCE_TABLES


def inject_cache_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Auto-derive cache source tables and partition names from catalog_config.

    Mutates `parameters` to add two keys, both keyed by cache logical name:

    - ``_cache_source_tables``: cache name → actual Hive table name.
    - ``_cache_partitions``: cache name → ``{"filter_keys": [...], "cols": [...]}``,
      the entry's ``partition_filter`` keys (in declaration order) and its
      ``partition_cols`` names. Together they are the on-disk directory nesting:
      ``HiveTableDataset`` emits ``PARTITIONED BY (filter keys…, partition_cols…)``,
      so the two lists concatenated are the partition directory levels in order.

    Cache nodes read both in steps.local_cache.populate_cache_from_hive.

    For each known cache name in CACHE_SOURCE_TABLES, look up the catalog entry.
    If present and `type: HiveTableDataset`, take its `table` field. Skips
    entries that aren't HiveTableDataset and missing entries.

    Operates on raw catalog_config dict (not DataCatalog instance) — the yaml
    schema is the public contract; we don't access dataset instance internals.

    No-op (does not write either key) when no cache entries match.

    Called by __main__.py:_execute_pipeline before DataCatalog construction so the
    cache nodes see the auto-derived mapping at runtime.
    """
    auto: dict[str, str] = {}
    partitions: dict[str, dict[str, list[str]]] = {}
    for cache_name in CACHE_SOURCE_TABLES:
        entry = catalog_config.get(cache_name)
        if entry and entry.get("type") == "HiveTableDataset":
            table = entry.get("table")
            if table:
                auto[cache_name] = table
                partitions[cache_name] = {
                    "filter_keys": list((entry.get("partition_filter") or {}).keys()),
                    "cols": [
                        c["name"] for c in (entry.get("partition_cols") or [])
                    ],
                }
    if auto:
        parameters["_cache_source_tables"] = auto
        parameters["_cache_partitions"] = partitions

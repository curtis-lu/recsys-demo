"""Which Hive table each ``model_version`` compare source reads on this run.

This module sits at the package root rather than in ``steps/`` because its one
caller is **outside** this pipeline: the evaluation command in ``__main__.py``
calls :func:`inject_compare_source_tables` before the run. That is the root vs
``steps/`` criterion in ``docs/agents/pipeline-node-design.md`` rule 8, and it
makes this the evaluation twin of ``pipelines/training/cache_sources.py``.

Why the command and not the node (rule 15: inject only what a node cannot see):
the compare loader reads B's rows by table name, outside the catalog, because
the catalog entry's ``partition_filter`` pins this run's ``model_version``, not
B's. A node receives ``parameters``, never the catalog, so the table the catalog
names is out of its sight. It cannot be rebuilt from the entry name either:
``conf/dev/catalog.yaml`` prefixes every table the framework writes.
"""

from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
    COMPARE_SOURCE_TABLES_KEY,
    MODEL_VERSION_SOURCES,
)


def inject_compare_source_tables(parameters: dict, catalog_config: dict) -> None:
    """Write ``{source: "database.table"}`` under ``_compare_source_tables``.

    One row per name in ``MODEL_VERSION_SOURCES`` whose entry in the resolved
    ``catalog_config`` is a ``HiveTableDataset``. A source the catalog does not
    declare gets no row; its reader then falls back to the entry name
    (``steps.compare_sources.compare_source_table``).
    """
    tables: dict[str, str] = {}
    for source in MODEL_VERSION_SOURCES:
        entry = catalog_config.get(source)
        if not entry or entry.get("type") != "HiveTableDataset":
            continue
        table = entry.get("table")
        if table:
            database = entry.get("database")
            tables[source] = f"{database}.{table}" if database else table
    parameters[COMPARE_SOURCE_TABLES_KEY] = tables

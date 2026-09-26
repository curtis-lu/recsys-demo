"""What the dataset command asks this package about a run, before and after it.

ADR-0029 decision 11: the parts of ``__main__.py``'s ``dataset()`` that only
the dataset pipeline understands live here, where they can be tested without
the CLI. The command keeps what every command does — start Spark, resolve the
catalog, write the manifests, run the pipeline — and asks this module the rest:

- before the run: the two version IDs and the source-table fingerprints they
  hash (:func:`run_versions`); which months each incremental artifact
  processes (:func:`month_plans_for_run`); what the DAG reads that no catalog
  entry supplies (:func:`pipeline_inputs`); and, under ``--only-test-months``,
  whether the train version has landed (:func:`train_version_landing`);
- after it: which train tables the configured variant still lacks
  (:func:`unlanded_train_tables`).

The module sits at the package root, not in ``steps/``, because its caller is
outside the pipeline — the criterion in ``docs/agents/pipeline-node-design.md``
rule 8, with ``pipelines/training/cache_sources.py`` as the precedent.

**Has the train version this config names landed.** Training does not
recompute ``train_variant_id`` from its own config: it follows
``train_variants/latest`` (``core/versioning.py``'s
``resolve_train_variant_id``). So ``latest`` can be wrong two ways, neither of
which raises — pointing at a variant with no tables (training reads 0 rows,
#334), or still pointing at another variant while the configured one has
tables. The evidence for both is the same: does the metastore hold partitions
under this run's versions. Not the variant's ``manifest.json``: a slice that
never touched train used to mark it ``completed`` all the same.

**Facts, not verdicts.** Whether ``--only-test-months`` may start on these
facts is A55's call (``core/consistency.py``'s ``train_version_landed_errors``;
node-design rule 11), and moving ``latest`` is the command's. What this module
does decide is what the facts are about: which tables a built variant has.
"""

import logging
from typing import NamedTuple

from recsys_tfb.core.catalog import DataCatalog
from recsys_tfb.core.schema import get_schema, get_schema_for_hash
from recsys_tfb.core.versioning import (
    compute_base_dataset_version,
    compute_feature_table_fingerprint,
    compute_train_variant_id,
)
from recsys_tfb.pipelines.dataset.month_plans import (
    CANDIDATE_FEATURE_TABLE,
    INCREMENTAL_DATASETS,
    SnapDatePlan,
    build_month_plans,
    landed_months,
    month_plan_input,
)

logger = logging.getLogger(__name__)


class SourceFingerprints(NamedTuple):
    """The source tables' schemas, as ``base_dataset_version`` hashes them."""

    feature_table: str
    #: ``(name, dtype)`` per column, in table order: what :attr:`feature_table`
    #: was computed from.
    feature_table_columns: list
    #: ``None`` when no candidate-level feature table is declared (ADR-0026).
    candidate: str | None
    candidate_columns: list | None

    @property
    def candidate_declared(self) -> bool:
        """Is a candidate-level feature table declared.

        The one answer both the fingerprint and :func:`pipeline_inputs`'
        ``None`` follow, so the version and the DAG cannot disagree about it.
        """
        return self.candidate is not None


def source_fingerprints(spark, source_catalog_config: dict) -> SourceFingerprints:
    """Fingerprint the schema of each source table whose columns become features.

    ``source_catalog_config`` is resolved without any version: source entries
    carry no ``${...}`` placeholder, so they are readable before the versions
    this feeds exist. Only schemas are read — no data is scanned.
    """
    feature_table_cfg = source_catalog_config["feature_table"]
    feature_table_fqn = f"{feature_table_cfg['database']}.{feature_table_cfg['table']}"
    feature_table_columns = [
        (f.name, f.dataType.simpleString())
        for f in spark.table(feature_table_fqn).schema.fields
    ]
    feature_table_fp = compute_feature_table_fingerprint(feature_table_columns)
    # The candidate-level feature table (ADR-0026) is declared by its catalog
    # entry. Its columns become features, so its schema is part of the
    # dataset's identity exactly as feature_table's is — and only when it is
    # declared, so a deployment without one keeps every ID it has.
    candidate_declared = CANDIDATE_FEATURE_TABLE in source_catalog_config
    candidate_fp = None
    candidate_columns = None
    if candidate_declared:
        candidate_cfg = source_catalog_config[CANDIDATE_FEATURE_TABLE]
        candidate_columns = [
            (f.name, f.dataType.simpleString())
            for f in spark.table(
                f"{candidate_cfg['database']}.{candidate_cfg['table']}"
            ).schema.fields
        ]
        candidate_fp = compute_feature_table_fingerprint(candidate_columns)
    return SourceFingerprints(
        feature_table=feature_table_fp,
        feature_table_columns=feature_table_columns,
        candidate=candidate_fp,
        candidate_columns=candidate_columns,
    )


class RunVersions(NamedTuple):
    """The two version layers a dataset run writes under."""

    base_dataset_version: str
    train_variant_id: str


def versions_for_run(
    parameters: dict, params_dataset: dict, sources: SourceFingerprints,
) -> RunVersions:
    """``base_dataset_version`` and ``train_variant_id`` for this config.

    ``params_dataset`` is ``parameters_dataset`` alone — the file both IDs
    hash — and ``parameters`` the merged set, read only for the schema.
    """
    schema_hash = get_schema_for_hash(parameters)
    base_v = compute_base_dataset_version(
        params_dataset, schema_hash, feature_table_fingerprint=sources.feature_table,
        candidate_feature_table_fingerprint=sources.candidate,
    )
    train_v = compute_train_variant_id(params_dataset)
    return RunVersions(base_dataset_version=base_v, train_variant_id=train_v)


def month_plans_for_run(
    catalog_config: dict, parameters: dict, rebuild,
) -> dict[str, SnapDatePlan]:
    """One :class:`SnapDatePlan` per incremental artifact, for this run.

    Incremental plans (ADR-0002 / ADR-0007): one metastore listing, one plan
    per incremental artifact, decided before any Spark work — so the nodes,
    the log and the manifest cannot disagree about which months this run
    covered.

    ``catalog_config`` must already be resolved with this run's
    ``base_dataset_version``, and that is load-bearing rather than tidy: the
    entries' ``partition_filter`` is what scopes the listing to this version.
    Resolved without it, every partition is compared against the literal
    ``${base_dataset_version}``, none is kept, and every month reads "nothing
    has landed" — a full rebuild, silently, with no failing test and no config
    diff.
    """
    existing_snap_dates = _collect_existing_snap_dates(
        DataCatalog(catalog_config),
        time_col=get_schema(parameters)["time"],
    )
    return build_month_plans(
        parameters, existing=existing_snap_dates, rebuild=rebuild,
    )


def pipeline_inputs(
    month_plans: dict[str, SnapDatePlan], *,
    only_test_months: bool, candidate_declared: bool,
) -> dict:
    """What the dataset DAG reads that no catalog entry supplies.

    Registered by the command beside the catalog; only facts known before the
    run that no node can see for itself (node rule 15).
    """
    return {
        # A loop over the plans, not three hand-written lines: registering a
        # fourth incremental artifact in month_plans.py is enough, and the
        # injection follows.
        **{
            month_plan_input(name): plan
            for name, plan in month_plans.items()
        },
        # The run mode, for the one node that cannot see it: the precision
        # gate checks the candidate-level feature table over the months
        # this run's builds read, and under --only-test-months only the
        # test build runs. Each build works out its own months (ADR-0029
        # decision 2), so no month list is injected.
        "only_test_months": only_test_months,
        # `None` is how a node learns none is declared. Only then: a
        # declared entry is already in the catalog, and registering over it
        # would silently drop every candidate-level feature.
        **({} if candidate_declared else {CANDIDATE_FEATURE_TABLE: None}),
    }


def _collect_existing_snap_dates(
    catalog: DataCatalog, time_col: str
) -> dict[str, list[str]]:
    """Ask the catalog which months each incrementally-built dataset already has.

    Taken once, before any node runs, so every incremental node and the
    manifest agree on what had already landed when this run started (ADR-0002).
    Metadata-only: no data is scanned.

    The question goes to the *dataset object*, not to its config entry: where an
    artifact is stored and how its partitions are listed is the catalog's
    knowledge. The CLI knowing that a ``HiveTableDataset`` has ``database`` and
    ``table`` fields, and how to turn those into a metastore query, is exactly
    the leak ADR-0008 §5 closes. The entry's ``partition_filter`` already scopes
    the answer to this run's ``base_dataset_version``, so the version is not a
    parameter here — see the caller for why that is load-bearing.

    A dataset that cannot list partitions makes every month look not-yet-landed:
    that rebuilds (wasteful) rather than skips (silently stale), which is the
    direction this decision must fail in.

    ``time_col`` comes from ``schema.time`` and has no default: the partition
    column is whatever the pipeline writes as its time column, and this repo is
    a configurable ranking framework. A default here would be a spelling that
    is right for the example deployment and silently wrong for any other, and
    every caller already passes the resolved value (#326).
    """
    existing: dict[str, list[str]] = {}
    for name in INCREMENTAL_DATASETS:
        lister = getattr(catalog.get_dataset(name), "existing_partition_values", None)
        if lister is None:
            logger.warning(
                "[months] %s cannot list its partitions, so its months cannot "
                "be listed and it will be rebuilt in full.", name,
            )
            continue
        existing[name] = landed_months(
            lister(), time_col=time_col, dataset_name=name,
        )
    return existing


#: The tables training reads under a train version. ``latest`` moves to a
#: variant only when each of them this config fills has partitions under it —
#: see :func:`unlanded_train_tables` for the one it may leave empty.
TRAIN_VERSION_TABLES = ("train_model_input", "train_dev_model_input")

#: The ``partition_filter`` key that scopes a train table to one variant. A
#: framework name like ``base_dataset_version`` beside it: the command fills
#: ``${train_variant_id}`` into it, and B10 scopes its file listing by the same
#: literal (``validate_model_input_grain``).
_VARIANT_PARTITION = "train_variant_id"


class TrainVersionLanding(NamedTuple):
    """Where ``train_model_input`` has partitions, per version layer."""

    #: Under this ``base_dataset_version``, whichever train variant.
    base: bool
    #: Under this ``base_dataset_version`` and this ``train_variant_id``.
    variant: bool


def train_version_landing(catalog_config: dict) -> TrainVersionLanding:
    """Has ``train_model_input`` landed under this run's base, and its variant.

    ``catalog_config`` is the resolved one — both versions already substituted
    into ``partition_filter``. The base layer is asked with the variant key
    taken out of that filter, so "a variant of this base was built" and "this
    variant was built" are two metastore listings, not a guess from one.

    Only ``train_model_input``: it is the table the #334 run leaves empty, and
    the one ``--only-test-months`` has to find before it may skip the train
    builds.
    """
    name = "train_model_input"
    entry = catalog_config.get(name)
    return TrainVersionLanding(
        base=_has_partitions(name, _without_variant_scope(entry)),
        variant=_has_partitions(name, entry),
    )


def unlanded_train_tables(catalog_config: dict, parameters: dict) -> list[str]:
    """The :data:`TRAIN_VERSION_TABLES` a built variant would have partitions
    in, and this one has none.

    Empty means the variant is built and ``latest`` may point at it. Asked
    after the run, of the metastore, rather than of the nodes the run
    executed: ``--only-test-months`` runs no train build yet must still move
    ``latest`` back to a variant built earlier, and a slice that runs one of
    the two builds must not move it.

    Decision — ``train_dev_model_input`` is not asked for when
    ``dataset.train_dev_ratio`` is exactly 0. That setting leaves train_dev
    empty on purpose (``split_train_keys`` only refuses an empty dev split
    under a non-zero ratio, and B10 passes a split with no files), and an
    empty frame writes no partition. Asking for it anyway would make a
    correctly built variant unpublishable for every such config. Any other
    value, or none, asks for it: an empty dev split there is a failure, and
    this is the direction that says so.
    """
    dev_split = ((parameters.get("dataset") or {}).get("train_dev_ratio")) != 0
    expected = [
        name for name in TRAIN_VERSION_TABLES
        if dev_split or name != "train_dev_model_input"
    ]
    return [
        name for name in expected
        if not _has_partitions(name, catalog_config.get(name))
    ]


def _without_variant_scope(entry: dict | None) -> dict | None:
    # Only an entry that has a partition_filter is narrowed: any other dataset
    # type rejects the key outright.
    if entry is None or "partition_filter" not in entry:
        return entry
    scope = {
        k: v for k, v in entry["partition_filter"].items()
        if k != _VARIANT_PARTITION
    }
    return {**entry, "partition_filter": scope}


def _has_partitions(name: str, entry: dict | None) -> bool:
    """Does the dataset built from ``entry`` list any partition.

    The question goes to a dataset object, as the month plans' listing does
    (:func:`_collect_existing_snap_dates`); only the entry is
    narrowed here, never turned into a metastore query by hand.

    An entry that cannot list partitions — absent, or not a Hive table —
    counts as not landed. That stops ``--only-test-months`` and leaves
    ``latest`` where it is, the direction that says so out loud; the other
    direction would publish a variant nobody has seen a table for.
    """
    lister = None
    if entry is not None:
        dataset = DataCatalog({name: entry}).get_dataset(name)
        lister = getattr(dataset, "existing_partition_values", None)
    if lister is None:
        logger.warning(
            "[train_variant] %s cannot list its partitions, so it counts as "
            "not landed.", name,
        )
        return False
    return bool(lister())

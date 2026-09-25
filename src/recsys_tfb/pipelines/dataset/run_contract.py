"""What the dataset command asks this package about a run, before and after it.

ADR-0029 decision 11: the parts of ``__main__.py``'s ``dataset()`` that only
the dataset pipeline understands live here. The module sits at the package
root, not in ``steps/``, because its caller is outside the pipeline — the
criterion in ``docs/agents/pipeline-node-design.md`` rule 8, with
``pipelines/training/cache_sources.py`` as the precedent. It starts with
decision 12's facts alone; the rest of decision 11 (the fingerprints, the two
version IDs, the month plans, the candidate-level table's detection) moves in
later without changing behaviour.

**Has the train version this config names landed.** Training does not
recompute ``train_variant_id`` from its own config: it follows
``train_variants/latest`` (``core/versioning.py``'s
``resolve_train_variant_id``). So ``latest`` can be wrong two ways, neither of
which raises — pointing at a variant with no tables (training reads 0 rows,
#334), or still pointing at another variant while the configured one has
tables. The evidence for both is the same: does the metastore hold partitions
under this run's versions. Not the variant's ``manifest.json``: a slice that
never touched train used to mark it ``completed`` all the same.

**Facts only.** Whether ``--only-test-months`` may start on these facts is
A55's call (``core/consistency.py``'s ``train_version_landed_errors``;
node-design rule 11). Whether ``latest`` moves after a run is the one rule
decided here, because it is not an error — a run that leaves it alone still
succeeds.
"""

import logging
from typing import NamedTuple

from recsys_tfb.core.catalog import DataCatalog

logger = logging.getLogger(__name__)

#: The tables training reads under a train version. ``latest`` moves to a
#: variant only when every one of them has partitions under it.
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


def unlanded_train_tables(catalog_config: dict) -> list[str]:
    """The :data:`TRAIN_VERSION_TABLES` with no partition under this variant.

    Empty means the variant is built and ``latest`` may point at it. Asked
    after the run, of the metastore, rather than of the nodes the run
    executed: ``--only-test-months`` runs no train build yet must still move
    ``latest`` back to a variant built earlier, and a slice that runs one of
    the two builds must not move it.
    """
    return [
        name for name in TRAIN_VERSION_TABLES
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
    (``__main__.py``'s ``_collect_existing_snap_dates``); only the entry is
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

"""Where a driver-local parquet cache lives, and how Hive gets copied into it.

Mechanism only. Every decision this cache makes — what counts as a hit, what a
directory without ``_SUCCESS`` means, when a cached copy is dropped and taken
again — is written out in each of the five cache nodes in ``nodes.py``, one node
at a time (``docs/agents/pipeline-node-design.md`` rules 4, 5 and 9). Read a
cache node to find out what it decided; read here to find out where the bytes go.

The log lines live here too, for the reason rule 5 gives: the nodes repeat their
decisions on purpose, but a format string repeated five times drifts.

``shutil.rmtree`` is deliberately **not** in this module, although the paths it
deletes are composed here. The architecture audit
(``tests/test_core/test_architecture_constraints.py::test_direct_writes_match_registry``)
scans ``pipelines/**/nodes*.py`` and nothing else, so a delete moved in here
would leave that registry and stop being watched by anything at all. ADR-0014
decision 1 records this as a consequence of the audit's reach rather than a rule
about deletes: widening the glob is issue #163, and once it lands the placement
should be revisited.

``month_dir`` is **imported** from ``steps/predict_months.py``, never copied
here, and the direction is forced rather than preferred: that module may not
import anything from this project, so owning the rule there and importing it here
is the only arrangement in which one implementation serves both (issue #231). It
has to be one implementation — a drift between "what this month's cache directory
is called" and "which month counts as already predicted" caches a month under one
name while the predict side looks for it under another, and neither side errors.

``populate_cache_from_hive`` writes, but only through ``utils.hdfs`` — the audit
scans for direct calls, so it cannot see this one either way. It is registered in
neither R4 nor the audit set: R4 is the register of *diagnostic by-products*, and
a copy of a Hive table is not one (decided 2026-08-30, ADR-0014 gate G2). What
watches it is A1's "what this check cannot see" note.
"""

import logging
from pathlib import Path
from typing import Optional

from recsys_tfb.pipelines.training.steps.predict_months import month_dir
from recsys_tfb.utils.hdfs import copy_hdfs_to_local, get_hive_table_location

logger = logging.getLogger(__name__)

# Sentinel layout token resolved from the ``month`` argument of
# resolve_cache_path — not a `parameters` key and not a directory name. The
# "!" prefix keeps it from being misread as the sibling literal directory
# component "test_months".
_TEST_MONTH_TOKEN = "!test_month"

# Tokens written into the path verbatim rather than looked up in `parameters`.
_CACHE_LITERAL_TOKENS = frozenset(
    {"train_variants", "calibration_variants", "test_months"}
)

_CACHE_PATH_LAYOUT: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    # test is cached one directory per month: each month is its own cache entry
    # with its own _SUCCESS, so adding a month adds a directory and leaves every
    # existing month untouched.
    "test_model_input": ("base_dataset_version", "test_months", _TEST_MONTH_TOKEN),
    "train_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "calibration_model_input": (
        "base_dataset_version",
        "calibration_variants",
        "calibration_variant_id",
    ),
}


# cache name → source Hive table (under parameters["hive"]["db"])
CACHE_SOURCE_TABLES: dict[str, str] = {
    "val_model_input": "val_model_input",
    "test_model_input": "test_model_input",
    "train_model_input": "train_model_input",
    "train_dev_model_input": "train_dev_model_input",
    "calibration_model_input": "calibration_model_input",
}

# Outer (string) Hive partitions encoding the variant boundaries.
# Mirrors catalog.yaml's `partition_filter` keys; copy these as the
# subtree root, then `snap_date=*` is the inner glob pattern.
_CACHE_OUTER_PARTITIONS: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variant_id"),
    "calibration_model_input": ("base_dataset_version", "calibration_variant_id"),
}


def require_spark_input(df, dataset_name: str) -> None:
    """Reject anything but a Spark DataFrame before a cache path is composed.

    Pre-check on an input, and the reason it is worth its own line: the
    ``cache.enabled=false`` passthrough it replaced returned the frame untouched,
    so an environment that set it handed a *different object type* downstream
    than production did. Without this the failure would surface four lines later
    as an ``AttributeError`` on ``sql_ctx``, which names the accessor rather than
    the misconfiguration.
    """
    if not hasattr(df, "sql_ctx"):
        raise TypeError(
            f"{dataset_name} input must be a Spark DataFrame; got "
            f"{type(df).__name__}. cache.enabled=false passthrough has been "
            "removed; all environments (including dev/test) must use a "
            "writable cache.root."
        )


def resolve_cache_path(
    dataset_name: str, parameters: dict, month: Optional[str] = None
) -> str:
    """Compose the local-cache parquet directory path for a model_input dataset.

    Mirrors the layered structure used by production catalog filepaths:
      <root>/<base_dataset_version>/[train_variants/<train_variant_id>/]<name>.parquet

    ``test_model_input`` additionally nests under ``test_months/<YYYYMMDD>/`` and
    therefore requires ``month``. The month is written literally (the
    ``YYYYMMDD`` convention evaluation report paths already use) rather than
    hashed: a directory naming exactly one month is readable off ``ls`` and
    cannot disagree with its own contents.

    ``month`` arrives as the configured literal and is normalised here through
    ``month_dir``, so a caller cannot hand this function a spelling the
    predict side would not recognise as the same month. Passing an already
    normalised value stays correct — the rule is idempotent.

    The path is composed, never resolved: ``cache.root`` is relative in
    ``conf/base/parameters_training.yaml``, so every path out of here is
    CWD-relative. Fine while one run holds one CWD; it is the recorded blocker
    for splitting diagnosis into a second pipeline (ADR-0014 decision 7).
    """
    if dataset_name not in _CACHE_PATH_LAYOUT:
        raise ValueError(f"unknown dataset for cache path: {dataset_name!r}")
    cache_cfg = parameters.get("cache", {})
    root = Path(cache_cfg.get("root", "/tmp/recsys_cache"))
    parts = [root]
    for token in _CACHE_PATH_LAYOUT[dataset_name]:
        if token in _CACHE_LITERAL_TOKENS:
            parts.append(Path(token))
        elif token == _TEST_MONTH_TOKEN:
            if month is None:
                raise ValueError(
                    f"{dataset_name} cache path requires month "
                    "(it is cached one directory per test month)"
                )
            parts.append(Path(month_dir(month)))
        else:
            value = parameters[token]
            parts.append(Path(value))
    parts.append(Path(f"{dataset_name}.parquet"))
    full = parts[0]
    for p in parts[1:]:
        full = full / p
    return str(full)


def _success_marker(local_path: str) -> Path:
    """The completion marker file for a cache directory.

    ⚠ **Not the only place this name is written.** ``io.handles`` hardcodes the
    same ``"_SUCCESS"`` for the consumer-side check, and
    ``models/lightgbm_adapter.py`` has a third copy for the LightGBM ``.bin``
    cache — neither can import this module (``io/`` sits below ``pipelines/``,
    and a shared constant is a change none of these three tickets asked for). So
    the three literals are a real drift risk that nothing prevents: rename the
    marker on one side and that side simply stops finding hits, with no error.
    """
    return Path(local_path) / "_SUCCESS"


def cache_exists(local_path: str) -> bool:
    """Is there anything at all at this cache path?

    Says nothing about whether it is usable — that is ``cache_is_complete``.
    Separate because dropping a *complete* copy (``--rebuild-dates``) and
    dropping an *interrupted* one are different decisions with different
    consequences, and each cache node writes them out separately.
    """
    return Path(local_path).exists()


def cache_is_complete(local_path: str) -> bool:
    """Did the copy that wrote this directory finish?

    The marker goes down last, so its presence is the only evidence a copy ran to
    the end. Nothing here looks at *when* it was written: freshness is not part
    of the answer, which is the trade each cache node spells out.
    """
    return _success_marker(local_path).exists()


def is_partial_cache(local_path: str) -> bool:
    """Is there a directory here that no copy ever finished writing?

    The two halves matter separately. "Nothing here" is an ordinary miss. "A
    directory with no marker" is debris from a copy that died — pyarrow will open
    it and read whatever fragments landed, without complaining, which is the
    silent-subset failure every caller of this is guarding against.
    """
    return cache_exists(local_path) and not cache_is_complete(local_path)


def mark_cache_complete(local_path: str) -> None:
    """Declare the copy finished — call this only after the last byte landed.

    Touching it early is the failure this whole protocol exists to avoid: a
    half-copied directory carrying the marker reads as a hit forever, and every
    number computed from it is computed over a silent subset.
    """
    _success_marker(local_path).touch()


# ---------------------------------------------------------------------------
# Log lines
#
# Written once and called from each cache node. The nodes duplicate their
# *decisions* on purpose (``docs/agents/pipeline-node-design.md`` rule 5), but a
# log line is mechanism, and five copies of a format string drift. Same shape as
# ``pipelines/dataset/steps/sampling.py::log_sampled_keys``.
#
# These emit under this module's logger, not the calling node's — the event
# names (``cache_hit`` / ``cache_miss`` / ``cache_rebuild``) are what log
# aggregation keys on, and they are unchanged.
# ---------------------------------------------------------------------------


def log_cache_hit(dataset_name: str, local_path: str) -> None:
    """Report a hit."""
    logger.info("cache_hit name=%s path=%s", dataset_name, local_path)


def log_cache_miss(dataset_name: str, local_path: str) -> None:
    """Report a miss — a copy from Hive is about to run."""
    logger.info("cache_miss name=%s path=%s", dataset_name, local_path)


def log_partial_cache_cleared(local_path: str) -> None:
    """Report an interrupted copy being dropped. WARNING, not INFO: it means a
    previous run died mid-copy, which is worth seeing even when the retry works.
    """
    logger.warning("Partial cache detected at %s, clearing before retry", local_path)


def log_cache_dropped_for_rebuild(dataset_name: str, local_path: str) -> None:
    """Report a *complete* copy being dropped because ``--rebuild-dates`` named it."""
    logger.info(
        "cache_rebuild name=%s path=%s — named by --rebuild-dates, "
        "dropping the cached copy so the refreshed source is re-read",
        dataset_name, local_path,
    )


def populate_cache_from_hive(
    spark, dataset_name: str, parameters: dict, local_dst: str,
    snap_date: Optional[str] = None,
) -> None:
    """Copy the relevant Hive partition subtree to driver-local fs.

    Local layout after copy:
        <local_dst>/snap_date=.../prod_name=.../*.parquet

    ``snap_date`` narrows the copy to a single month (test caching), and is the
    partition value *verbatim* — the ``YYYY-MM-DD`` spelling Hive wrote, not the
    ``YYYYMMDD`` directory form the cache path uses. A month the source table
    does not hold makes the glob match nothing, and ``copy_hdfs_to_local`` raises
    FileNotFoundError — that is how "configured a month but never ran dataset"
    surfaces, so no separate coverage check exists. That path leaves an empty
    destination directory behind (the copier mkdirs before globbing); it carries
    no ``_SUCCESS``, so the partial-cache branch of every cache node clears and
    rebuilds it on the next run.

    Source-table resolution:
      1. parameters['_cache_source_tables'][dataset_name] — auto-injected by
         __main__.py:_execute_pipeline from catalog_config (HiveTableDataset.table).
         This is the production path and works across envs that prefix table
         names (e.g. 'recsys_prod_train_model_input').
      2. CACHE_SOURCE_TABLES[dataset_name] — fallback used by unit tests that
         don't go through __main__.py and therefore have no auto-injection.
    """
    db = parameters["hive"]["db"]
    source_tables = parameters.get("_cache_source_tables", {})
    table = source_tables.get(dataset_name, CACHE_SOURCE_TABLES[dataset_name])
    location = get_hive_table_location(spark, db, table)
    outer = "/".join(
        f"{tok}={parameters[tok]}"
        for tok in _CACHE_OUTER_PARTITIONS[dataset_name]
    )
    inner = "snap_date=*" if snap_date is None else f"snap_date={snap_date}"
    src_glob = f"{location.rstrip('/')}/{outer}/{inner}"
    copy_hdfs_to_local(spark, src_glob, local_dst, glob=True)

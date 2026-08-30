"""Where a driver-local parquet cache lives, and how Hive gets copied into it.

Mechanism only. Every decision this cache makes — what counts as a hit, what a
directory without ``_SUCCESS`` means, when a cached copy is dropped and taken
again — is written out in each of the five cache nodes in ``nodes.py``, one node
at a time (``docs/agents/pipeline-node-design.md`` rules 4, 5 and 9). Read a
cache node to find out what it decided; read here to find out where the bytes go.

``shutil.rmtree`` is deliberately **not** in this module, although the paths it
deletes are composed here. The architecture audit
(``tests/test_core/test_architecture_constraints.py::test_direct_writes_match_registry``)
scans ``pipelines/**/nodes*.py`` and nothing else, so a delete moved in here
would leave that registry and stop being watched by anything at all. ADR-0014
decision 1 records this as a consequence of the audit's reach rather than a rule
about deletes: widening the glob is issue #163, and once it lands the placement
should be revisited.

``_test_month_dir`` stays in ``nodes.py`` for the mirror-image reason. Issue #231
moves it into ``steps/predict_months.py``, which is to be a zero-pyspark pure
module forbidden to import anything else in the project; pulling it in here first
would make that module impossible. So a month's directory name arrives as the
``month_dir`` argument rather than being derived here.

``populate_cache_from_hive`` writes, but only through ``utils.hdfs`` — the audit
scans for direct calls, so it cannot see this one either way. It is registered in
neither R4 nor the audit set: R4 is the register of *diagnostic by-products*, and
a copy of a Hive table is not one (decided 2026-08-30, ADR-0014 gate G2). What
watches it is A1's "what this check cannot see" note.
"""

from pathlib import Path
from typing import Optional

from recsys_tfb.utils.hdfs import copy_hdfs_to_local, get_hive_table_location

# Sentinel layout token resolved from the ``month_dir`` argument of
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
    dataset_name: str, parameters: dict, month_dir: Optional[str] = None
) -> str:
    """Compose the local-cache parquet directory path for a model_input dataset.

    Mirrors the layered structure used by production catalog filepaths:
      <root>/<base_dataset_version>/[train_variants/<train_variant_id>/]<name>.parquet

    ``test_model_input`` additionally nests under ``test_months/<YYYYMMDD>/`` and
    therefore requires ``month_dir``. The month is written literally (the
    ``YYYYMMDD`` convention evaluation report paths already use) rather than
    hashed: a directory naming exactly one month is readable off ``ls`` and
    cannot disagree with its own contents.

    ``month_dir`` arrives already in directory form rather than being normalised
    here, because the normaliser (``nodes._test_month_dir``) is on its way to a
    pure module this one may not import — see this module's docstring.

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
            if month_dir is None:
                raise ValueError(
                    f"{dataset_name} cache path requires a snap_date "
                    "(it is cached one directory per test month)"
                )
            parts.append(Path(month_dir))
        else:
            value = parameters[token]
            parts.append(Path(value))
    parts.append(Path(f"{dataset_name}.parquet"))
    full = parts[0]
    for p in parts[1:]:
        full = full / p
    return str(full)


def success_marker(local_path: str) -> Path:
    """The completion marker file for a cache directory.

    One definition, so the node that touches it last and the node that reads it
    as a hit cannot drift apart on the name, and so
    ``io.handles.require_complete_cache`` is checking for the same file from the
    consumer's side. Getting the two names out of step would not raise: one side
    would simply stop finding hits, or stop noticing partial copies.
    """
    return Path(local_path) / "_SUCCESS"


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
         __main__.py:_run_pipeline from catalog_config (HiveTableDataset.table).
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

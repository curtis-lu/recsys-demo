"""Load Model B raw predictions for 2-way comparison.

Two source kinds:
  * model_version  — read a same-stack Hive table filtered by ``model_version``.
    Defaults to ``enriched_eval_predictions`` (symmetric with Model A in
    ``--compare-only`` mode; requires the B-side model_version to have also
    been through ``evaluation`` previously). Other accepted values for
    ``source``: ``ranked_predictions`` (inference output) or
    ``training_eval_predictions`` (training pipeline's test-set predictions).
  * external_hive  — read external Hive table with column rename + prod_mapping.

Also which of its keys decide what B holds (``compare_source_identity``): every
comparison artifact records them, so a redraw after the source changed is
refused rather than printed under the new source's label.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.item_columns import combine_item_columns
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    PARTITION_FINGERPRINT_COLUMN,
    normalised,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_dates,
    eval_snap_dates_without_rows,
    restrict_to_eval_snap_dates,
)

logger = logging.getLogger(__name__)


#: What a ``model_version`` source reads when its ``source`` is left out, and
#: what an ``external_hive`` source does with an item ``prod_mapping`` lacks
#: when ``unmapped_policy`` is: the loader's defaults, and the identity's.
DEFAULT_MODEL_VERSION_SOURCE = "enriched_eval_predictions"
DEFAULT_UNMAPPED_POLICY = "fail"

#: Where every comparison artifact records :func:`compare_source_identity`
#: (ADR-0031 decision 5).
COMPARE_SOURCE_KEY = "compare_source"


def compare_source_identity(parameters: dict) -> dict:
    """What decides what side B holds, as the loader reads it,
    :func:`config_fingerprint.normalised` — "which source a comparison was
    computed against" (ADR-0031 decision 5): the keys of
    ``evaluation.compare``, plus the physical table a ``model_version``
    source is read off.

    Per kind: ``model_version`` reads ``model_version`` off the table
    ``source`` names. Which physical table that is
    (:func:`compare_source_table`) comes from ``_compare_source_tables`` or
    ``hive.db``, outside ``evaluation.compare``, so it is recorded as
    ``table``. ``external_hive`` reads ``table``, renames by
    ``columns``, translates by ``prod_mapping`` and treats an unmapped item
    by ``unmapped_policy``. ``kind`` itself always. ``label`` never: it only
    names B in the report, so a relabelled source redraws without a re-run.
    An unknown kind (the loader refuses it) keeps every key but ``label``.

    Defaults are filled in before anything is compared, so writing a default
    out, or deleting it, is the same B and refuses no redraw. They are the
    loader's own constants (:data:`DEFAULT_MODEL_VERSION_SOURCE`,
    :data:`DEFAULT_UNMAPPED_POLICY`), so the two cannot drift apart.
    """
    src = ((parameters.get("evaluation") or {}).get("compare")) or {}
    kind = src.get("kind")
    if kind == "model_version":
        source = src.get("source", DEFAULT_MODEL_VERSION_SOURCE)
        identity = {
            "kind": kind,
            "model_version": src.get("model_version"),
            "source": source,
            "table": compare_source_table(parameters, source),
        }
    elif kind == "external_hive":
        identity = {
            "kind": kind,
            "table": src.get("table"),
            "columns": src.get("columns"),
            "prod_mapping": src.get("prod_mapping") or {},
            "unmapped_policy": src.get("unmapped_policy",
                                       DEFAULT_UNMAPPED_POLICY),
        }
    else:
        identity = {key: value for key, value in src.items() if key != "label"}
    return normalised(identity)


def load_compare_predictions(parameters: dict, spark: SparkSession) -> SparkDataFrame:
    """Dispatch on ``compare.kind`` and return raw Model B predictions.

    B is kept to the evaluated dates, and every one of
    them must have rows: a month B lacks would otherwise drop out of the
    common query groups and the comparison would silently cover fewer months
    than configured.
    """
    eval_params = parameters.get("evaluation", {}) or {}
    src = eval_params.get("compare")
    if not src:
        raise RuntimeError(
            "parameters['evaluation']['compare'] missing — CLI must dispatch "
            "the chosen compare source dict here before pipeline run."
        )
    # Fails before any table is read when no date is configured (ValueError).
    eval_snap_dates(parameters)

    schema = get_schema(parameters)
    kind = src.get("kind")
    if kind == "model_version":
        return _load_model_version(src, parameters, spark)
    if kind == "external_hive":
        return _load_external_hive(src, parameters, schema, spark)
    raise RuntimeError(f"unknown compare source kind={kind!r}")


def _no_rows_for(
    df: SparkDataFrame, parameters: dict, *, time_partitioned: bool = True,
) -> str | None:
    """``snap_date=…`` naming the evaluated dates ``df`` has no row for, or None.

    One configured date prints bare; with several, the missing ones print as a
    list. ``time_partitioned``: see ``eval_snap_dates_without_rows``.
    """
    missing = eval_snap_dates_without_rows(
        df, parameters, time_partitioned=time_partitioned)
    if not missing:
        return None
    if len(eval_snap_dates(parameters)) == 1:
        return f"snap_date={missing[0]!r}"
    return f"snap_date(s) {missing}"


# Hive tables allowed as model_version compare source.
# ``enriched_eval_predictions`` additionally carries label / rank / segment
# columns; ``restrict_to_common`` re-ranks after the universe shrink and
# replaces any carried label with A's (ADR-0020 bug 7). Kept here so the loader
# can fail-loud even if A11 validation was bypassed.
MODEL_VERSION_SOURCES = (
    "enriched_eval_predictions",
    "ranked_predictions",
    "training_eval_predictions",
)

#: ``{source: "database.table"}``, written into ``parameters`` by the evaluation
#: command (``pipelines/evaluation/compare_tables.py``).
COMPARE_SOURCE_TABLES_KEY = "_compare_source_tables"


def compare_source_table(parameters: dict, source: str) -> str:
    """The physical table behind ``source``, one of :data:`MODEL_VERSION_SOURCES`.

    The catalog's answer when the command handed one over: the entry name is
    not the table name once an environment prefixes it (``conf/dev/catalog.yaml``
    does), and the catalog entry itself cannot be read here because its
    ``partition_filter`` pins this run's ``model_version``, not B's. Without
    one (unit tests, or a catalog that does not declare the entry), the entry
    name under ``hive.db``; with ``hive.db`` empty too, the bare name, which
    Spark resolves in its current database (temp views included). A set
    ``hive.db`` is required whenever no default database is
    configured in ``spark.sql.catalog``.
    """
    table = (parameters.get(COMPARE_SOURCE_TABLES_KEY) or {}).get(source)
    if table:
        return table
    hive_db = ((parameters.get("hive") or {}).get("db") or "").strip()
    return f"{hive_db}.{source}" if hive_db else source


def _load_model_version(
    src: dict, parameters: dict, spark: SparkSession,
) -> SparkDataFrame:
    mv = src["model_version"]
    source = src.get("source", DEFAULT_MODEL_VERSION_SOURCE)
    if source not in MODEL_VERSION_SOURCES:
        raise DataConsistencyError(
            f"compare source={source!r} not in {MODEL_VERSION_SOURCES}"
        )
    table_name = compare_source_table(parameters, source)
    df = spark.table(table_name).filter(F.col("model_version") == mv)
    no_rows = _no_rows_for(df, parameters)
    if no_rows:
        raise DataConsistencyError(
            f"compare model_version={mv!r} has no rows for {no_rows} "
            f"in source={source!r} (table {table_name})"
        )
    # enriched_eval_predictions carries each partition's settings fingerprint.
    # B's are the settings of B's own run, so they are not checked
    # against this run's; the column is dropped so it does not travel on.
    df = restrict_to_eval_snap_dates(df, parameters).drop(
        PARTITION_FINGERPRINT_COLUMN)
    logger.info(
        "Loaded compare predictions: table=%s model_version=%s rows=%d",
        table_name, mv, df.count(),
    )
    return df


def _load_external_hive(
    src: dict, parameters: dict, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    table = src["table"]
    cols = src["columns"]
    item_col = schema["item"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]

    raw = spark.table(table)
    df = raw.select(*[F.col(ext).alias(internal) for internal, ext in cols.items()])
    # A multi-column item is combined once the columns carry our names, and
    # before prod_mapping, whose keys are item values (ADR-0027). Unless the
    # source maps `item` itself: then the external table names items with an
    # id of its own, there is nothing to combine, and prod_mapping translates
    # those ids (A11 refuses a source that declares both).
    if item_col not in cols:
        df = combine_item_columns(df, schema, f"compare external_hive table={table!r}")
    # The user's table: nothing says it is partitioned by the time column.
    no_rows = _no_rows_for(df, parameters, time_partitioned=False)
    if no_rows:
        raise DataConsistencyError(
            f"compare external_hive table={table!r} has no rows for {no_rows}"
        )
    df = restrict_to_eval_snap_dates(df, parameters)

    mapping = src.get("prod_mapping", {}) or {}
    policy = src.get("unmapped_policy", DEFAULT_UNMAPPED_POLICY)
    seen_prods = {r[0] for r in df.select(item_col).distinct().collect()}
    unmapped = seen_prods - set(mapping.keys())
    if unmapped:
        if policy == "fail":
            raise DataConsistencyError(
                f"compare external prods absent from prod_mapping: "
                f"{sorted(unmapped)}. Either add to prod_mapping or set "
                "unmapped_policy=drop."
            )
        if policy == "drop":
            logger.warning(
                "Dropping %d unmapped prods (unmapped_policy=drop): %s",
                len(unmapped), sorted(unmapped),
            )
            df = df.filter(F.col(item_col).isin(list(mapping.keys())))
        else:
            raise RuntimeError(f"unknown unmapped_policy={policy!r}")

    df = df.replace(mapping, subset=[item_col])
    # N:1 collapse — multiple ext prods may map to the same internal prod;
    # aggregate to (cust, snap, prod) with max(score) (best-rank semantic).
    df = df.groupBy(*identity_cols).agg(F.max(score_col).alias(score_col))
    logger.info("Loaded compare predictions: external table=%s rows=%d", table, df.count())
    return df

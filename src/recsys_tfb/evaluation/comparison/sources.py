"""Load Model B raw predictions for 2-way comparison.

Two source kinds:
  * model_version  — read a same-stack Hive table filtered by ``model_version``.
    Defaults to ``enriched_eval_predictions`` (symmetric with Model A in
    ``--compare-only`` mode; requires the B-side model_version to have also
    been through ``evaluation`` previously). Other accepted values for
    ``source``: ``ranked_predictions`` (inference output) or
    ``training_eval_predictions`` (training pipeline's test-set predictions
    — useful for ``--post-training`` mode without re-running inference).
  * external_hive  — read external Hive table with column rename + prod_mapping.

The full source dict is staged at ``parameters['evaluation']['compare']`` by
the CLI dispatcher (``__main__.py``).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict, spark: SparkSession) -> SparkDataFrame:
    """Dispatch on ``compare.kind`` and return raw Model B predictions."""
    eval_params = parameters.get("evaluation", {}) or {}
    src = eval_params.get("compare")
    if not src:
        raise RuntimeError(
            "parameters['evaluation']['compare'] missing — CLI must dispatch "
            "the chosen compare source dict here before pipeline run."
        )
    snap_date = str(eval_params.get("snap_date") or "").strip()
    if not snap_date:
        raise RuntimeError("evaluation.snap_date missing")

    schema = get_schema(parameters)
    # Empty/missing hive.db → bare table name (the loader resolves via Spark's
    # current database, including registered temp views — used by unit tests).
    # When hive.db is set, the loader prefixes the table name; required
    # whenever no default database is configured in spark.sql.catalog.
    hive_db = ((parameters.get("hive") or {}).get("db") or "").strip() or None
    kind = src.get("kind")
    if kind == "model_version":
        return _load_model_version(src, snap_date, schema, spark, hive_db)
    if kind == "external_hive":
        return _load_external_hive(src, snap_date, schema, spark)
    raise RuntimeError(f"unknown compare source kind={kind!r}")


# Hive tables allowed as model_version compare source. All three share the
# (cust_id, snap_date, prod_name, score, model_version) projection used by
# restrict_to_common. ``enriched_eval_predictions`` additionally carries
# label / rank / segment columns; ``restrict_to_common`` is schema-agnostic
# (re-ranks after the universe shrink, skips the label LEFT JOIN when
# already present). Kept here so the loader can fail-loud even if A11
# validation was bypassed (e.g. in tests).
MODEL_VERSION_SOURCES = (
    "enriched_eval_predictions",
    "ranked_predictions",
    "training_eval_predictions",
)


def _load_model_version(
    src: dict, snap_date: str, schema: dict, spark: SparkSession,
    hive_db: str | None,
) -> SparkDataFrame:
    mv = src["model_version"]
    source = src.get("source", "enriched_eval_predictions")
    if source not in MODEL_VERSION_SOURCES:
        raise DataConsistencyError(
            f"compare source={source!r} not in {MODEL_VERSION_SOURCES}"
        )
    table_name = f"{hive_db}.{source}" if hive_db else source
    time_col = schema["time"]
    df = (
        spark.table(table_name)
        .filter(F.col("model_version") == mv)
        .filter(F.col(time_col).cast("string") == snap_date)
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"compare model_version={mv!r} has no rows for snap_date={snap_date!r} "
            f"in source={source!r}"
        )
    logger.info(
        "Loaded compare predictions: table=%s model_version=%s rows=%d",
        table_name, mv, df.count(),
    )
    return df


def _load_external_hive(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    table = src["table"]
    cols = src["columns"]
    time_col = schema["time"]
    item_col = schema["item"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]

    raw = spark.table(table)
    # Column rename: alias external names → our canonical schema names
    df = raw.select(*[F.col(ext).alias(internal) for internal, ext in cols.items()])
    df = df.filter(F.col(time_col).cast("string") == snap_date)
    if df.isEmpty():
        raise DataConsistencyError(
            f"compare external_hive table={table!r} has no rows for snap_date={snap_date!r}"
        )

    mapping = src.get("prod_mapping", {}) or {}
    policy = src.get("unmapped_policy", "fail")
    seen_prods = {r[0] for r in df.select(item_col).distinct().collect()}
    unmapped = seen_prods - set(mapping.keys())
    if unmapped:
        if policy == "fail":
            raise DataConsistencyError(
                f"(B2) compare external prods absent from prod_mapping: "
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

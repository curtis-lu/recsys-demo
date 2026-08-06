"""Every node function of the dataset pipeline, the Layer-2 data gate included.

This module is the one home of the pipeline's ML story: a reader who opens it
sees each decision this pipeline makes about the data, without jumping files.
The mechanisms those decisions are expressed in live in sibling modules named
after the concern they implement (``sampling``, ``scoping``, ``feature_columns``,
``model_input``, ``month_plans``) — see ADR-0008 §2 for the two criteria that
draw that line, and ``docs/agents/architecture-constraints.md`` S1, which pins
"every node registered in ``pipeline.py`` is ``def``-defined here".
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import (
    DataConsistencyError,
    carry_column_collision_errors,
    categorical_dtype_errors,
    item_coverage_errors,
    nonnumeric_feature_errors,
    resolved_item_values,
    spark_dtype_is_numeric,
)
from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket
from recsys_tfb.pipelines.dataset.feature_columns import (
    _compute_feature_columns,
    _get_preprocessing_config,
)
from recsys_tfb.pipelines.dataset.model_input import (
    build_model_input as _build_model_input,
)
from recsys_tfb.pipelines.dataset.month_plans import (
    SnapDatePlan,
    collect_dataset_snap_dates,
)
from recsys_tfb.pipelines.dataset.sampling import select_keys
from recsys_tfb.pipelines.dataset.scoping import _date_filter
from recsys_tfb.preprocessing import _encode_categoricals

logger = logging.getLogger(__name__)


def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> None:
    """Run the Layer-2 invariants (B1, B5, B6, B7) against the source tables.

    Side-effect only: raises ``DataConsistencyError`` on violation, returns
    ``None`` when everything holds. Each invariant's meaning lives with its
    predicate in ``core/consistency.py`` — this node only asks the config and
    Spark for facts and hands them to the predicates. Deciding anything here
    would put a second, drifting copy of the rule next to the real one.

    All errors are collected and raised once so a single fix pass clears them.

    Cost invariant (ADR-0006): the facts gathered here are cheap ones. Column
    types come from the metastore — metadata, no rows. The item values come from
    a distinct over the configured snap_date windows, so what lands on the driver
    is bounded by item cardinality rather than by row count. What this gate does
    not do is aggregate over a source table: that would change this node's cost
    magnitude, and a check needing one is a data-quality check with a home of its
    own upstream in ``source_etl``'s ``quality_checks``.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    time_col = schema["time"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    windows = collect_dataset_snap_dates(parameters)

    def _distinct_items(df: DataFrame) -> set:
        rows = (
            df.filter(F.col(time_col).isin(windows))
            .select(item)
            .distinct()
            .collect()
        )
        return {r[item] for r in rows if r[item] is not None}

    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    ft_dtypes = dict(feature_table.dtypes)
    feature_cols = _compute_feature_columns(
        list(feature_table.columns),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )
    # Only feature_table-sourced columns have a dtype here; identity categoricals
    # (e.g. prod_name) come from schema.categorical_values, are absent from
    # feature_table.dtypes, and are validated by A3.
    feature_kinds = {
        c: ("numeric" if spark_dtype_is_numeric(ft_dtypes[c]) else "nonnumeric")
        for c in feature_cols
        if c in ft_dtypes
    }
    errors = (
        item_coverage_errors(
            item,
            resolved_item_values(parameters),
            _distinct_items(sample_pool),
            _distinct_items(label_table),
        )
        + categorical_dtype_errors(categorical_cols, ft_dtypes)
        + nonnumeric_feature_errors(feature_kinds, set(categorical_cols))
        + carry_column_collision_errors(
            parameters.get("dataset", {}).get("carry_columns") or [],
            # Reuses the dtypes mapping B5/B6 already read — same metastore
            # metadata lookup, so B7 costs no extra call and no scan.
            set(ft_dtypes),
            drop_cols,
            identity_cols,
            label_col,
        )
    )
    if errors:
        raise DataConsistencyError(
            "Data consistency check failed ("
            + str(len(errors))
            + " issue(s)):\n- "
            + "\n- ".join(errors)
        )


def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys using explicit train_snap_dates list."""
    ds = parameters["dataset"]
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    overrides = ds.get("sample_ratio_overrides", {})
    return select_keys(
        sample_pool, parameters, train_dates, ds["sample_ratio"], overrides,
        site="sample_keys",
    )


def select_calibration_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select calibration identity keys using calibration_snap_dates from parameters."""
    ds = parameters["dataset"]
    cal_dates = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})

    return select_keys(
        sample_pool, parameters, cal_dates, cal_ratio, cal_overrides,
        site="calibration_keys",
    )


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by cust_id ratio.

    All rows for a given cust_id are assigned to the same split.
    Logging still triggers no action; the empty-dev guard below does — one
    ``isEmpty`` always, plus one ``count`` on the failing path only.
    """
    schema = get_schema(parameters)
    entity_cols = schema["entity"]
    cust_col = entity_cols[0]

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Deterministic per-cust_id bucket; threshold is computed once so the two
    # filters are guaranteed to be a complete and disjoint partition regardless
    # of how many actions Spark runs against this plan.
    cust_df = sample_keys.select(cust_col).distinct()
    cust_df = cust_df.withColumn(
        "_bucket", spark_bucket(cust_df, [cust_col], seed, site="split_train_dev"),
    )
    threshold = ratio_to_threshold(train_dev_ratio)

    dev_custs = cust_df.filter(F.col("_bucket") < F.lit(threshold)).select(cust_col)
    train_custs = cust_df.filter(F.col("_bucket") >= F.lit(threshold)).select(cust_col)

    train_keys = sample_keys.join(train_custs, on=cust_col, how="inner")
    train_dev_keys = sample_keys.join(dev_custs, on=cust_col, how="inner")

    # An empty train_dev is invisible downstream: it is the early-stopping
    # validation set for every HPO trial (training/nodes.py passes
    # train_dev_lgb_handle as val_dataset), so an empty one means each trial
    # silently runs its full round budget with early stopping never firing —
    # no error, no warning, just worse models and a longer search. Costs one
    # Spark action; see ADR-0005 for the fallback if that ever matters at scale.
    # `!= 0`, not `> 0`: a negative ratio makes ratio_to_threshold return a
    # negative threshold, so `_bucket < threshold` is empty and
    # `_bucket >= threshold` takes everything — the same silent state, reached
    # by one stray minus sign. Only an exact 0 means "no dev split wanted".
    if train_dev_ratio != 0 and train_dev_keys.isEmpty():
        n_entities = cust_df.count()
        if n_entities == 0:
            raise ValueError(
                f"split_train_keys received no sampled keys at all "
                f"(train_dev_ratio={train_dev_ratio}), so train and train-dev "
                f"are both empty. The cause is upstream of the split — check "
                f"dataset.sample_ratio, dataset.train_snap_dates, and any "
                f"partition filter applied when sample_keys was read back."
            )
        raise ValueError(
            f"split_train_keys produced an empty train-dev split: "
            f"train_dev_ratio={train_dev_ratio} applied to "
            f"{n_entities} distinct {cust_col} value(s) puts every entity "
            f"on the train side. train_dev is the early-stopping validation set "
            f"for every HPO trial, so an empty one disables early stopping "
            f"without raising. Raise dataset.train_dev_ratio, or widen the "
            f"sample (dataset.sample_ratio / train_snap_dates)."
        )

    logger.info(
        "Split train keys (ratio=%.2f)",
        train_dev_ratio,
    )
    return train_keys, train_dev_keys


def select_val_keys(
    sample_pool: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Select validation identity keys (full population, optional random cust_id sampling)."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_key = schema["identity_columns"]
    cust_col = entity_cols[0]

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    val_labels = sample_pool.filter(F.col(time_col).isin(val_dates))
    all_keys = val_labels.select(*identity_key).dropDuplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys (full population)")
        return all_keys

    # Deterministic cust_id sampling
    custs = all_keys.select(cust_col).distinct()
    sampled_custs = custs.withColumn(
        "_bucket", spark_bucket(custs, [cust_col], seed, site="val_keys"),
    ).filter(
        F.col("_bucket") < F.lit(ratio_to_threshold(val_sample_ratio))
    ).select(cust_col)

    sampled = all_keys.join(sampled_custs, on=cust_col, how="inner")
    logger.info("Val keys (ratio=%.2f)", val_sample_ratio)
    return sampled


def select_test_keys(
    sample_pool: DataFrame,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """Select test identity keys (full population, no sampling).

    Restricted to ``month_plan.to_process`` (ADR-0002). Months that already
    landed are left alone: the write is a dynamic partition overwrite, so an
    absent month means "untouched", not "deleted".
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]

    test_labels = sample_pool.filter(_date_filter(time_col, month_plan.to_process))
    all_keys = test_labels.select(*identity_key).dropDuplicates()

    logger.info("Test keys (full population)")
    return all_keys


def build_test_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """build_model_input for the test split, scoped to ``month_plan``.

    The one thing this adds over the shared ``build_model_input``: ``test_keys``
    is a persistent Hive table holding *every* month under this base version, so
    reading it back gives the full history even when ``select_test_keys`` only
    wrote the new month. Without this filter the downstream join would rebuild
    every month — the ∝N cost ADR-0002 exists to remove.
    """
    schema = get_schema(parameters)
    keys = keys.filter(_date_filter(schema["time"], month_plan.to_process))
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )


def _fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Build preprocessor metadata at customer-month granularity, decoupled from sampling.

    Feature-categorical distinct values come from feature_table rows whose
    ``time`` falls in ``train_snap_dates``. Identity categoricals (not present
    in feature_table) come from ``parameters["schema"]["categorical_values"][col]``;
    missing declarations raise ``ValueError``.

    Raises ``ValueError`` if feature_table is missing any required train_snap_date
    (fail-loud principle: dataset must be reproducible from feature_table).

    Only small metadata (distinct category values) is collected to driver.

    Returns:
        (preprocessor_metadata, category_mappings) — the first matching
        ``PreprocessorMetadata``'s four keys.
    """
    schema = get_schema(parameters)
    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {})
    train_dates = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Fail-loud if feature_table is missing any required train_snap_date.
    # Cardinality is small (typically 12-52 dates); .distinct().collect() is cheap.
    ft_dates = {
        row[time_col]
        for row in feature_table.select(time_col).distinct().collect()
    }
    ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
    missing = sorted(set(train_dates) - ft_dates)
    if missing:
        raise ValueError(
            "feature_table missing required train_snap_dates: "
            f"{[d.strftime('%Y-%m-%d') for d in missing]}"
        )

    with log_step(logger, "filter_train_window"):
        train_features = feature_table.filter(F.col(time_col).isin(train_dates))

    ft_cols = set(feature_table.columns)
    feature_cat_cols = [c for c in categorical_cols if c in ft_cols]
    identity_cat_cols = [c for c in categorical_cols if c not in ft_cols]

    cat_values = schema.get("categorical_values", {})
    missing_cats = [c for c in identity_cat_cols if c not in cat_values]
    if missing_cats:
        raise DataConsistencyError(
            "Identity categorical columns missing declarations in "
            f"schema.categorical_values: {missing_cats}. Add them to "
            "parameters.yaml under schema.categorical_values."
        )

    with log_step(logger, "collect_category_mappings"):
        category_mappings: dict[str, list] = {}
        for col in feature_cat_cols:
            distinct_rows = (
                train_features.select(col)
                .filter(F.col(col).isNotNull())
                .distinct()
                .orderBy(col)
                .collect()
            )
            category_mappings[col] = [row[col] for row in distinct_rows]
        for col in identity_cat_cols:
            category_mappings[col] = list(cat_values[col])

    with log_step(logger, "compute_feature_columns"):
        feature_columns = _compute_feature_columns(
            feature_table.columns,
            identity_cols,
            categorical_cols,
            drop_cols,
            label_col,
        )

    # Ranking-task invariant: schema.item must end up in feature_columns. The
    # most common way to lose it is omitting it from
    # `dataset.prepare_model_input.categorical_columns` in yaml — silently
    # makes X miss the item dimension, predictions collapse to constant within
    # each query group, and HPO reports a flat mAP across every trial.
    item_col = schema.get("item")
    if item_col and item_col not in feature_columns:
        raise DataConsistencyError(
            f"schema.item='{item_col}' is missing from derived feature_columns. "
            f"For a ranking task the item column must be a model feature; "
            f"otherwise the booster cannot differentiate items within a query "
            f"group and HPO mAP collapses to a constant across trials. "
            f"Fix: add '{item_col}' to "
            f"dataset.prepare_model_input.categorical_columns in "
            f"parameters_dataset.yaml. "
            f"(current categorical_columns={categorical_cols})"
        )

    preprocessor_metadata = {
        "feature_columns": feature_columns,
        "categorical_columns": categorical_cols,
        "category_mappings": category_mappings,
        "drop_columns": drop_cols,
    }

    logger.info(
        "Fit preprocessor (Spark): %d features, %d categorical, %d drop",
        len(feature_columns), len(categorical_cols), len(drop_cols),
    )
    return preprocessor_metadata, category_mappings


def fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Fit Spark preprocessor at customer-month granularity, decoupled from sampling.

    Only collects small metadata (distinct category values) to driver.
    """
    return _fit_preprocessor_metadata(feature_table, parameters)


def _warn_missing_drop_columns(
    columns: list[str],
    drop_cols: list[str],
    context: str,
) -> None:
    """Log warning for drop_columns that don't exist in the DataFrame."""
    missing = [c for c in drop_cols if c not in columns]
    if missing:
        logger.warning(
            "drop_columns not found in %s (will be ignored): %s",
            context, missing,
        )


def _apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
    snap_dates: list,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table at customer-month granularity.

    Filters feature_table to ``snap_dates``, which the caller must supply — it
    is the caller that knows which months this run is for. The dataset pipeline
    passes its incremental month plan, so months whose partition already landed
    are not re-encoded into a bit-identical result (ADR-0002). An empty list is
    a legitimate value (nothing left to process); see the empty-frame note below.

    Raises ``ValueError`` if any snap_date about to be processed is missing from
    feature_table. Months that are *not* being processed are deliberately not
    checked — this run does not read them, so requiring feature_table to retain
    them forever would be a false alarm.

    Output: (time + entity) + feature_columns that live in feature_table.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    base_key = [time_col] + entity_cols
    ft_feature_cols = [c for c in feature_columns if c in feature_table.columns]
    keep_cols = list(dict.fromkeys(base_key + ft_feature_cols))
    missing_base = [c for c in base_key if c not in feature_table.columns]
    if missing_base:
        raise ValueError(f"feature_table missing base-key columns: {missing_base}")
    _warn_missing_drop_columns(feature_table.columns, drop_cols, "feature_table")

    needed_dates = [pd.Timestamp(d) for d in snap_dates]

    # Fail-loud if feature_table is missing any snap_date we are about to
    # process. Skipped months are not checked: this run never reads them.
    if needed_dates:
        ft_dates = {
            row[time_col]
            for row in feature_table.select(time_col).distinct().collect()
        }
        ft_dates = {pd.Timestamp(d) for d in ft_dates if d is not None}
        missing = sorted(set(needed_dates) - ft_dates)
        if missing:
            raise ValueError(
                "feature_table missing required snap_dates: "
                f"{[d.strftime('%Y-%m-%d') for d in missing]}"
            )

    # An empty date list means every configured month already landed. The node
    # still returns a properly-typed empty frame (encoding included) rather than
    # short-circuiting: Hive's dynamic partition overwrite only touches
    # partitions present in the written data, so an empty write leaves the
    # existing partitions intact — but only if the schema still matches.
    # Both sides pinned to DATE. feature_table is a source table whose time
    # column is a real DATE/TIMESTAMP today, so a bare isin() would work — but
    # if it ever arrives as a string, isin() against timestamp literals matches
    # zero rows and raises nothing, and this node's empty output is now a
    # *documented normal state* (see the comment above) rather than an obvious
    # anomaly. That combination would turn a type mismatch into silent data
    # loss, so normalise here too.
    date_filter = (
        F.to_date(F.col(time_col)).isin([pd.Timestamp(d).date() for d in needed_dates])
        if needed_dates
        else F.lit(False)
    )
    with log_step(logger, "select_columns"):
        result = feature_table.filter(date_filter).select(*keep_cols)

    with log_step(logger, "encode_categoricals"):
        encode_cols = [c for c in categorical_cols if c in result.columns and c not in identity_cols]
        if encode_cols:
            result = _encode_categoricals(result, encode_cols, category_mappings)
            # Single pass: one aggregation returns the unknown (-1) count for
            # every encoded column at once. The previous per-column .count()
            # re-scanned the full multi-month feature_table once per categorical
            # (N actions); this collapses it to a single scan.
            unknown_counts = result.agg(*[
                F.sum(F.when(F.col(c) == -1, 1).otherwise(0)).alias(c)
                for c in encode_cols
            ]).collect()[0]
            for col in encode_cols:
                n_unknown = unknown_counts[col] or 0
                if n_unknown > 0:
                    logger.warning(
                        "apply_preprocessor_to_features: %d unknowns in column '%s'",
                        n_unknown, col,
                    )

    logger.info(
        "Preprocessed feature_table (Spark): %d cols (encoded=%d)",
        len(result.columns), len(encode_cols),
    )
    return result


def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """Encode non-identity categoricals in Spark feature_table once for all splits.

    Only the months in ``month_plan`` are encoded (ADR-0002). Skipping an
    existing month is safe because a partition's content is
    f(that month's feature_table rows, category_mappings) and the mappings are
    fit on train months only — no cross-month term, so a re-encode would
    reproduce the same bits.

    Passes the month *list* down, not the plan: the encoding step uses it to
    raise when a month it is about to process is missing from ``feature_table``.
    Pre-filtering here would make that check vacuously true — and the encoding
    step stays unaware that "incremental" is a concept.
    """
    return _apply_preprocessor_to_features(
        feature_table, preprocessor_metadata, parameters,
        snap_dates=month_plan.to_process,
    )


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Merge Spark keys + labels + encoded features into model_input for a split."""
    return _build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )


def _filter_groups_with_positives(
    df: DataFrame,
    group_cols: list[str],
    label_col: str,
) -> DataFrame:
    """Drop rows whose ``group_cols`` partition has ``sum(label_col) == 0``.

    A query group is ``(time, *entity)``; pushing this filter to Spark
    write-time keeps val_model_input / test_model_input Hive tables tight —
    customers with no positives in a snap_date contribute nothing to mAP
    (metrics_spark filters them again) and only waste predict time.
    """
    w = Window.partitionBy(*group_cols)
    return (
        df.withColumn("__grp_pos", F.sum(F.col(label_col)).over(w))
          .filter(F.col("__grp_pos") > 0)
          .drop("__grp_pos")
    )


def filter_groups_with_positives(
    model_input: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Drop (time, *entity) groups whose label sum is zero.

    Pipeline-node wrapper over ``_filter_groups_with_positives``;
    resolves group_cols / label_col from schema. Used for val / test only —
    groups without any positive contribute nothing to mAP (metrics_spark
    re-applies the same filter) and would only waste predict time.
    """
    schema = get_schema(parameters)
    group_cols = [schema["time"]] + schema["entity"]
    label_col = schema["label"]
    return _filter_groups_with_positives(model_input, group_cols, label_col)

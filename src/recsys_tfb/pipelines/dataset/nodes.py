"""Every node function of the dataset pipeline, the Layer-2 data gate included.

This module is the one home of the pipeline's ML story: a reader who opens it
sees each decision this pipeline makes about the data, without jumping files.
The mechanisms those decisions are expressed in live in ``steps/``, one module per
concern (``sampling``, ``scoping``, ``feature_columns``, ``categoricals``,
``model_input``). ``month_plans`` stays beside this file instead, because
``__main__.py`` reads it too — see ADR-0008 §2 for the criteria that draw both
lines, and ``docs/agents/architecture-constraints.md`` S1, which pins "every node
registered in ``pipeline.py`` is ``def``-defined here".

Reading a node here, the decisions are the named steps; the constants and dtype
details those steps are made of (the unknown-category sentinel, the float32
cast) stay in the helpers. That is why these functions are longer than the
repo's usual — a node is a sequence of decisions, not a call.

``log_step`` goes only around a block that fires a Spark action. Everything
else in this module is lazy: the joins, the filters, the column selects and the
float32 cast all return a plan in microseconds, and the computation they
describe runs later, inside ``catalog.save()``. Timing such a block reports a
guaranteed ~0.00s that reads exactly like "this step was fast", and mixing the
two kinds under one event name leaves nobody able to tell which zero means
which. The blocks that remain wrapped are the ones that collect to the driver:
the vocabulary fit, the unknown-encoding count, and the two month-presence
pre-checks. Where a node's *time* actually goes is a question for the Runner's
``load``/``func``/``save`` split (``core/runner.py``), not for this module.
"""

import logging

import pandas as pd
from pyspark.sql import DataFrame
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
from recsys_tfb.core.schema import get_entity_grouping, get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket
from recsys_tfb.pipelines.dataset.steps.categoricals import (
    collect_vocabularies_from_data,
    read_declared_vocabularies,
    require_declared_categoricals,
)
from recsys_tfb.pipelines.dataset.steps.feature_columns import (
    compute_feature_columns,
    encoded_frame_columns,
    prepare_model_input_config,
    require_base_key_columns,
    require_item_is_a_feature,
    split_categorical_sources,
    warn_missing_drop_columns,
)
from recsys_tfb.pipelines.dataset.steps.model_input import (
    drop_groups_without_positives,
    join_features_missing_as_null,
    join_labels_missing_as_negative,
    model_input_columns,
    require_columns_present,
)
from recsys_tfb.pipelines.dataset.month_plans import (
    SnapDatePlan,
    collect_dataset_snap_dates,
)
from recsys_tfb.pipelines.dataset.steps.sampling import (
    draw_can_drop_rows,
    keep_entities_drawn_under_ratio,
    keep_rows_drawn_under_ratio,
    key_output_columns,
    log_sampled_keys,
    sampling_columns,
    with_effective_sample_ratio,
)
from recsys_tfb.pipelines.dataset.steps.scoping import (
    months_filter_as_date,
    require_months_present,
    restrict_to_months,
    restrict_to_months_or_all,
)
from recsys_tfb.preprocessing import (
    cast_feature_floats_to_float32,
    encodable_categoricals,
    encode_categoricals,
    warn_unknown_encodings,
)

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
    Spark for facts and hands them to the predicates. Adding an invariant means
    adding a predicate there and one term to the sum below; deciding anything
    here would put a second, drifting copy of the rule next to the real one.

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

    drop_cols, categorical_cols = prepare_model_input_config(parameters)
    ft_dtypes = dict(feature_table.dtypes)
    feature_cols = compute_feature_columns(
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
    """Select train identity keys: which rows are eligible, and which are drawn.

    ``sample_pool`` is at (time, entity, item) granularity and is the widest
    frame this pipeline reads, which is why each step below narrows before the
    next one runs.

    The same four decisions appear in ``select_calibration_keys``, spelled out
    there rather than shared through a helper: a helper holding four decisions
    is what ADR-0008 §2 forbids, and the duplication is what makes each node
    readable on its own.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]

    ds = parameters["dataset"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    carry_columns = ds.get("carry_columns", []) or []
    sample_ratio = ds["sample_ratio"]
    overrides = ds.get("sample_ratio_overrides", {})
    train_months = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Decision — eligibility: only rows in the configured train months can be
    # drawn. A month belongs to exactly one split (A24), so this is also what
    # keeps train disjoint from val / test / calibration. The "_or_all" is not
    # a shorthand: an empty month list leaves the pool *whole* rather than
    # empty. Unreachable today (``train_snap_dates`` is a required key), and
    # preserved rather than tightened because tightening it would change
    # behaviour — which is why it is spelled out here and not read off the name.
    pool = restrict_to_months_or_all(sample_pool, time_col, train_months)
    # sample_pool's primary key IS the identity key, enforced upstream by
    # source_etl's max_duplicate_key_ratio check, so nothing dedups here.
    keys = pool.select(*sampling_columns(group_keys, identity_key, carry_columns))

    if draw_can_drop_rows(sample_ratio, overrides):
        # Decision — how much of each stratum to keep: a per-group override
        # outranks the split's default ratio; a group with no override falls
        # back to it.
        keys = with_effective_sample_ratio(keys, group_keys, sample_ratio, overrides)
        # Decision — who survives: the draw is on the identity key, so the same
        # key is kept or dropped identically on every rerun.
        keys = keep_rows_drawn_under_ratio(
            keys, identity_key, seed, site="sample_keys",
        )
        log_sampled_keys(sample_ratio, group_keys, overrides, "sample_keys")
    else:
        # A full ratio with no overrides can drop nothing, so the draw is
        # skipped rather than computed over the whole pool and then trivially
        # satisfied.
        log_sampled_keys(sample_ratio, group_keys, overrides, site=None)

    # Decision — what a split's keys are: the identity key, plus the carry
    # columns that travel on to model_input for downstream weighting. The draw's
    # working columns are dropped here.
    return keys.select(*key_output_columns(identity_key, carry_columns))


def select_calibration_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select calibration identity keys: same four decisions as the train split.

    One thing differs beyond the config keys: the sampling ``site``. It is what
    stops calibration drawing the same rows as train — the two share
    ``random_seed``, so without a distinct namespace the calibration set would
    be a subset of the train draw (#140).
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]

    ds = parameters["dataset"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    carry_columns = ds.get("carry_columns", []) or []
    cal_ratio = ds.get("calibration_sample_ratio", 1.0)
    cal_overrides = ds.get("calibration_sample_ratio_overrides", {})
    cal_months = [pd.Timestamp(d) for d in ds["calibration_snap_dates"]]

    # Decision — eligibility: only rows in the configured calibration months —
    # except that an empty month list leaves the pool whole, as it does for
    # train. See ``select_train_keys`` for why that asymmetry is preserved.
    pool = restrict_to_months_or_all(sample_pool, time_col, cal_months)
    keys = pool.select(*sampling_columns(group_keys, identity_key, carry_columns))

    if draw_can_drop_rows(cal_ratio, cal_overrides):
        # Decision — how much of each stratum to keep.
        keys = with_effective_sample_ratio(keys, group_keys, cal_ratio, cal_overrides)
        # Decision — who survives, drawn under calibration's own site.
        keys = keep_rows_drawn_under_ratio(
            keys, identity_key, seed, site="calibration_keys",
        )
        log_sampled_keys(cal_ratio, group_keys, cal_overrides, "calibration_keys")
    else:
        log_sampled_keys(cal_ratio, group_keys, cal_overrides, site=None)

    # Decision — identity key plus carry columns.
    return keys.select(*key_output_columns(identity_key, carry_columns))


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by entity ratio.

    All rows of a given entity are assigned to the same split, so no entity
    straddles the boundary. Which columns constitute "a given entity" here is
    the user's to declare (``dataset.train_split_keys``, defaulting to the whole
    ``schema.entity``): a leakage unit coarser than the query group is a real
    situation — several accounts of one customer must not land on opposite
    sides — and the framework has no way to know it from the data.

    Logging still triggers no action; the empty-dev guard below does — one
    ``isEmpty`` always, plus one ``count`` on the failing path only.
    """
    # Decision — the split unit: what the user declared, else the whole entity.
    split_cols = get_entity_grouping(parameters, "train_split_keys")

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Deterministic per-entity bucket; threshold is computed once so the two
    # filters are guaranteed to be a complete and disjoint partition regardless
    # of how many actions Spark runs against this plan.
    entity_df = sample_keys.select(*split_cols).distinct()
    entity_df = entity_df.withColumn(
        "_bucket", spark_bucket(entity_df, split_cols, seed, site="split_train_dev"),
    )
    threshold = ratio_to_threshold(train_dev_ratio)

    dev_entities = entity_df.filter(F.col("_bucket") < F.lit(threshold)).select(*split_cols)
    train_entities = entity_df.filter(F.col("_bucket") >= F.lit(threshold)).select(*split_cols)

    train_keys = sample_keys.join(train_entities, on=split_cols, how="inner")
    train_dev_keys = sample_keys.join(dev_entities, on=split_cols, how="inner")

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
        n_entities = entity_df.count()
        split_unit = ", ".join(split_cols)
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
            f"{n_entities} distinct {split_unit} value(s) puts every entity "
            f"on the train side. train_dev is the early-stopping validation set "
            f"for every HPO trial, so an empty one disables early stopping "
            f"without raising. Raise dataset.train_dev_ratio, widen the "
            f"sample (dataset.sample_ratio / train_snap_dates), or split on a "
            f"finer unit (dataset.train_split_keys)."
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
    """Select validation identity keys (full population, optional entity sampling).

    The draw unit is the user's to declare (``dataset.val_sample_keys``,
    defaulting to the whole ``schema.entity``) for the same reason as the train
    split — but with a sharper failure mode, because getting it wrong is
    invisible. That key is deliberately absent from ``TRAIN_SAMPLING_KEYS``, so
    changing it moves ``base_dataset_version``, which is the only ID val
    artifacts are keyed by. Register it there and a new draw unit would read
    back the val parquet drawn under the old one, silently. See
    docs/adr/0016-split-unit-declared-by-two-keys.md.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    # Decision — eligibility: only the configured val months.
    val_labels = restrict_to_months(sample_pool, time_col, val_dates)
    # Decision — the val population is every distinct key, not a draw over rows:
    # unlike the train side this does not lean on sample_pool's primary key.
    all_keys = val_labels.select(*identity_key).dropDuplicates()

    if val_sample_ratio >= 1.0:
        logger.info("Val keys (full population)")
        return all_keys

    # Decision — when val is sampled, it is sampled per *entity*, never per row:
    # mAP is computed over a query group, so a group must keep all of its
    # candidates or the metric answers a different question.
    # Decision — the draw unit: what the user declared, else the whole entity.
    sample_cols = get_entity_grouping(parameters, "val_sample_keys")
    sampled = keep_entities_drawn_under_ratio(
        all_keys, sample_cols, val_sample_ratio, seed, site="val_keys",
    )
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

    test_labels = sample_pool.filter(months_filter_as_date(time_col, month_plan.to_process))
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
    # Decision — scope: this run's months only. Everything after it is the same
    # assembly every other split gets, which is why it is the sibling node
    # rather than a copy.
    keys = keys.filter(months_filter_as_date(schema["time"], month_plan.to_process))
    return build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata, parameters,
    )


def fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
) -> tuple[dict, dict]:
    """Fit the preprocessor: each categorical's vocabulary, and what a feature is.

    Pre-checks (inputs, ADR-0008 §3): ``feature_table`` must carry every
    ``train_snap_dates`` month, and every identity categorical must be declared
    in ``schema.categorical_values``. Registered backstop: ``schema.item`` must
    survive into ``feature_columns``.

    Only small metadata (distinct category values) reaches the driver.

    Returns:
        (preprocessor_metadata, category_mappings) — the first matching
        ``PreprocessorMetadata``'s four keys.
    """
    schema = get_schema(parameters)
    drop_cols, categorical_cols = prepare_model_input_config(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    label_col = schema["label"]

    ds = parameters.get("dataset", {})
    train_months = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Pre-check: a dataset must be reproducible from feature_table, so a train
    # month that is not there is an error rather than a smaller fit. Timed
    # because it collects the time column's distinct values — one of the few
    # blocks in this module that costs anything (see the module docstring).
    with log_step(logger, "require_months_present(train_snap_dates)"):
        require_months_present(
            feature_table, time_col, train_months, "train_snap_dates",
        )

    # Decision — no leakage: vocabularies are fit on the train months only. A
    # category that only ever appears in val/test therefore has no index of its
    # own and encodes to the unknown sentinel, exactly as it would in production
    # on a value the model never trained on.
    #
    # The un-normalised restriction, not ``months_filter_as_date``: this node reads
    # feature_table straight from the source table, where the time column is a
    # real DATE. The normalised form is for the frames read back from Hive with
    # a string partition column — see ``scoping``.
    train_features = restrict_to_months(feature_table, time_col, train_months)

    # Decision — where a vocabulary comes from: a categorical that is a
    # feature_table column has its domain in the data; an identity categorical
    # is not in feature_table at all, so its domain has to be declared. Reading
    # a declared one from the data would silently shrink it to the values this
    # snapshot happens to contain.
    from_data, from_schema = split_categorical_sources(
        categorical_cols, feature_table.columns,
    )
    # Pre-check: nothing downstream can supply a vocabulary the schema owes.
    cat_values = schema.get("categorical_values", {})
    require_declared_categoricals(cat_values, from_schema)
    with log_step(logger, "collect_category_mappings"):
        category_mappings = {
            **collect_vocabularies_from_data(train_features, from_data),
            **read_declared_vocabularies(cat_values, from_schema),
        }

    feature_columns = compute_feature_columns(
        feature_table.columns,
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )

    # Registered backstop: for a ranking task the item column must be a feature.
    require_item_is_a_feature(
        schema.get("item"), feature_columns, categorical_cols,
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


def apply_preprocessor_to_features(
    feature_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
) -> DataFrame:
    """Encode feature_table's categoricals once, for every split to share.

    Pre-checks (inputs, ADR-0008 §3): ``feature_table`` must carry the base key,
    and every month this run is about to encode. Months the plan skips are
    deliberately *not* checked — this run never reads them, so requiring
    feature_table to retain them forever would be a false alarm.

    Output: (time + entity) + the feature_columns that live in feature_table.
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
    months = month_plan.to_process

    # Pre-checks. The drop_columns one only warns: a stale name in that list
    # changes nothing about the output.
    require_base_key_columns(feature_table.columns, base_key)
    warn_missing_drop_columns(feature_table.columns, drop_cols, "feature_table")
    if months:
        # Timed for the same reason as the fit's copy: it collects.
        with log_step(logger, "require_months_present(snap_dates)"):
            require_months_present(feature_table, time_col, months, "snap_dates")

    # Decision — only the months this run owns are encoded (ADR-0002). Skipping
    # a month that already landed is safe because a partition's content is
    # f(that month's rows, category_mappings) and the mappings were fit on train
    # months only: no cross-month term, so a re-encode would reproduce the same
    # bits. An empty plan is a normal state — every configured month already
    # landed — and still produces a properly-typed empty frame rather than
    # short-circuiting, because Hive's dynamic partition overwrite leaves
    # existing partitions intact only when the written schema still matches.
    #
    # Decision — the output width: the base key plus the features that actually
    # live in feature_table. Identity-sourced features arrive later, from keys.
    result = feature_table.filter(months_filter_as_date(time_col, months)).select(
        *encoded_frame_columns(base_key, feature_columns, feature_table.columns)
    )

    # Decision — a value the train months never showed becomes the unknown
    # sentinel, not a new index: the fit is the vocabulary's only source, and an
    # index invented here would mean something different to every month.
    with log_step(logger, "encode_categoricals"):
        encode_cols = encodable_categoricals(
            categorical_cols, result.columns, identity_cols,
        )
        if encode_cols:
            result = encode_categoricals(result, encode_cols, category_mappings)
            warn_unknown_encodings(
                result, encode_cols, context="apply_preprocessor_to_features",
            )

    logger.info(
        "Preprocessed feature_table (Spark): %d cols (encoded=%d)",
        len(result.columns), len(encode_cols),
    )
    return result


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Assemble a split's model_input from its keys, the labels and the features.

    Pre-check (input, ADR-0008 §3): ``keys`` must be at item grain.
    Post-condition: identity, label and every feature column survive the joins.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    base_key = [time_col] + entity_cols

    feature_columns = preprocessor_metadata["feature_columns"]

    # Pre-check: keys' grain IS model_input's grain (ADR-0005). This used to
    # fall back to a base-key-only label join when item was absent, which
    # silently multiplied every (time, entity) by label_table's item count and
    # took `item`'s values from label_table. No caller can reach that branch —
    # identity_columns is derived ([time] + entity + [item], core/schema.py) and
    # every node feeding this one passes it — but its failure mode is a silently
    # N-times-too-large dataset, so a missing column is an error, not a mode.
    label_join_key = base_key + [item_col]
    require_columns_present(keys.columns, label_join_key, "build_model_input keys")

    # Decision — a key with no label row is a negative, not a gap. sample_pool
    # is dense (entity x item fully expanded) while label_table is sparse (only
    # entities with a transaction), so the misses are most of the frame.
    dataset = join_labels_missing_as_negative(
        keys, label_table, label_join_key, label_col,
    )

    # Decision — a key with no feature row keeps its row and gets an all-NULL
    # feature block. feature_table's upstream population differs from
    # sample_pool's; LightGBM handles missing values, and dropping the row would
    # instead change which query groups exist (ADR-0005 §3).
    dataset = join_features_missing_as_null(
        dataset, preprocessed_feature_table, base_key,
    )

    # Decision — what a model_input row is made of: identity, the label, every
    # feature, and the columns the keys carried in for downstream weighting.
    # Everything else the two joins brought along is dropped here.
    required = list(set(identity_cols + [label_col] + feature_columns))
    require_columns_present(dataset.columns, required, "build_model_input")
    result = dataset.select(*model_input_columns(
        keys.columns, dataset.columns, identity_cols, label_col, feature_columns,
    ))

    # Decision — numeric features converge on one storage type. LightGBM is
    # histogram-based, so float32's precision is already beyond what binning can
    # use; decimal128 in particular materialises as Python Decimal objects and
    # was what OOM-killed the val read. Which types get cast, and to what, is
    # the helper's business.
    result, casted = cast_feature_floats_to_float32(result, feature_columns)
    logger.info(
        "build_model_input: %d features, cast %d float-like feature columns to float32",
        len(feature_columns), len(casted),
    )
    if casted:
        logger.debug("build_model_input: casted columns = %s", casted)
    return result


def filter_groups_with_positives(
    model_input: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Drop (time, *entity) query groups whose label sum is zero.

    Decision — a query group with no positive is dropped rather than scored.
    Applied to val / test only: mAP is undefined over such a group and
    metrics_spark filters them again anyway, so keeping them only inflates the
    Hive table and wastes predict time. train / train_dev / calibration are NOT
    filtered — their losses use every row.
    """
    schema = get_schema(parameters)
    group_cols = [schema["time"]] + schema["entity"]
    label_col = schema["label"]
    return drop_groups_without_positives(model_input, group_cols, label_col)

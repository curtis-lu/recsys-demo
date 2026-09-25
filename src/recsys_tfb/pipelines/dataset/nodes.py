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
import operator
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import (
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    DataConsistencyError,
    candidate_feature_table_key_errors,
    carry_column_collision_errors,
    categorical_dtype_errors,
    combined_item_collision_errors,
    feature_table_overlap_errors,
    item_coverage_errors,
    item_list_counted_from_data,
    item_list_drift_errors,
    item_source_column_errors,
    item_source_dtype_errors,
    nonnumeric_feature_errors,
    optional_role_source_column_errors,
    override_unknown_item_errors,
    ColumnPrecision,
    SplitRowCounts,
    model_input_grain_errors,
    numeric_precision_errors,
    numeric_precision_rows,
    resolved_item_values,
    resolved_numeric_storage,
    resolved_zero_positive_group_ratio,
    spark_dtype_is_numeric,
    spark_dtype_value_step,
    zero_positive_group_weight_collision_errors,
)
from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_entity_grouping, get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket
from recsys_tfb.utils.item_columns import combine_item_columns, combined_item_value
from recsys_tfb.pipelines.dataset.steps.categoricals import (
    collect_vocabularies_from_data,
    count_items_in_months,
    read_declared_vocabularies,
    require_declared_categoricals,
    require_supported_categorical_dtypes,
    warn_items_outside_the_list,
)
from recsys_tfb.pipelines.dataset.steps.feature_columns import (
    candidate_feature_source_columns,
    compute_feature_columns,
    encoded_frame_columns,
    prepare_model_input_config,
    require_base_key_columns,
    require_item_is_a_feature,
    split_categorical_sources,
    warn_missing_drop_columns,
)
from recsys_tfb.pipelines.dataset.steps.precision import landed_partition_files
from recsys_tfb.pipelines.dataset.steps.model_input import (
    count_zero_positive_groups_kept,
    join_features_missing_as_null,
    join_labels_missing_as_negative,
    keep_zero_positive_groups_drawn_under_ratio,
    log_zero_positive_group_draw,
    model_input_columns,
    require_columns_present,
)
from recsys_tfb.pipelines.dataset.month_plans import (
    SnapDatePlan,
    collect_dataset_snap_dates,
)
from recsys_tfb.pipelines.dataset.steps.sampling import (
    any_column_is_null,
    draw_can_drop_rows,
    keep_entities_drawn_under_ratio,
    keep_rows_drawn_under_ratio,
    key_output_columns,
    log_sampled_keys,
    sampling_columns,
    warn_dropped_null_split_unit,
    with_effective_sample_ratio,
)
from recsys_tfb.pipelines.dataset.steps.scoping import (
    months_filter_as_date,
    months_present_and_max_abs,
    require_months_in,
    require_months_present,
)
from recsys_tfb.utils.parquet_stats import (
    filter_by_partitions,
    read_max_abs_stats,
    read_row_count,
)
from recsys_tfb.preprocessing import (
    preprocessor_item_values,
    cast_numeric_features_to_storage_type,
    castable_numeric_feature_columns,
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
    candidate_feature_table: DataFrame | None = None,
) -> None:
    """Run the Layer-2 invariants (B1, B5, B6, B7, B11–B17) against the source
    tables.

    ``candidate_feature_table`` is the candidate-level feature table when one is
    declared, ``None`` otherwise (ADR-0026). Its columns can be features just as
    ``feature_table``'s can, so B5, B6, B7 and B12 ask both; B13 and B14 exist
    only for it.

    Side-effect only: raises ``DataConsistencyError`` on violation, returns
    ``None`` when everything holds. Each invariant's meaning lives with its
    predicate in ``core/consistency.py`` — this node only asks the config and
    Spark for facts and hands them to the predicates. Adding an invariant means
    adding a predicate there and one term to the sum below; deciding anything
    here would put a second, drifting copy of the rule next to the real one.

    Errors are collected and raised once so a single fix pass clears them —
    except B16, which is raised on its own first: every item check reads the
    columns it is about, so with one missing those checks would fail as a
    Spark error rather than report.

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
    item_sources = schema["item_source_columns"]
    time_col = schema["time"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    windows = collect_dataset_snap_dates(parameters)

    def _raise_if_any(errors: list[str]) -> None:
        if errors:
            raise DataConsistencyError(
                "Data consistency check failed ("
                + str(len(errors))
                + " issue(s)):\n- "
                + "\n- ".join(errors)
            )

    # B16 first, and on its own: every item check below reads the columns it
    # is about, so with one missing they would fail as a Spark error rather
    # than report. Column names only. A no-op for a single-column item.
    item_tables = {"sample_pool": sample_pool, "label_table": label_table}
    if candidate_feature_table is not None:
        item_tables["candidate_feature_table"] = candidate_feature_table
    _raise_if_any([
        e for table, df in item_tables.items()
        for e in item_source_column_errors(schema, table, df.columns)
    ])
    # B17 reads the tables as the user wrote them, before combining turns
    # the item columns into one text column.
    item_dtype_errors = item_source_dtype_errors(
        schema, {table: dict(df.dtypes) for table, df in item_tables.items()},
    )

    def _item_combinations(df: DataFrame) -> list[tuple[tuple, object]]:
        """Distinct ``(source values, item value)`` in the dataset windows.

        One distinct serves B1 (the values) and B15 (which combinations share
        a value). For a single-column item the select is the item column
        alone, exactly the query B1 always ran; a multi-column item adds its
        source columns, and the combined value is computed by the same Spark
        expression every entry combines with, so B15 judges what the
        pipelines will actually see.
        """
        combined = (
            [] if len(item_sources) == 1
            else [combined_item_value(item_sources).alias(item)]
        )
        rows = (
            df.filter(months_filter_as_date(time_col, windows))
            .select(*item_sources, *combined)
            .distinct()
            .collect()
        )
        return [(tuple(r[c] for c in item_sources), r[item]) for r in rows]

    def _values(combinations) -> set:
        return {value for _, value in combinations if value is not None}

    sample_pool_items = _item_combinations(sample_pool)
    label_items = _item_combinations(label_table)
    # Decision — a multi-column item is combined on read (ADR-0027), so the
    # candidate table's features and join key below are what the build will
    # see: `item` present, its source columns gone.
    if candidate_feature_table is not None:
        candidate_feature_table = combine_item_columns(
            candidate_feature_table, schema, "candidate_feature_table",
        )

    drop_cols, categorical_cols = prepare_model_input_config(parameters)
    carry_cols = parameters.get("dataset", {}).get("carry_columns") or []
    ft_dtypes = dict(feature_table.dtypes)
    # The candidate table's identity columns are its join key and never a
    # feature (the fit reads them the same way), so they are left out of every
    # question below that is about features.
    cand_dtypes = {} if candidate_feature_table is None else {
        c: t for c, t in candidate_feature_table.dtypes
        if c in candidate_feature_source_columns(
            candidate_feature_table.columns, identity_cols,
        )
    }
    source_dtypes = {**ft_dtypes, **cand_dtypes}
    feature_cols = compute_feature_columns(
        list(feature_table.columns) + list(cand_dtypes),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )
    # Only feature-table-sourced columns have a dtype here; identity categoricals
    # (e.g. prod_name) come from schema.categorical_values, are absent from
    # both tables' dtypes above, and are validated by A3.
    feature_kinds = {
        c: ("numeric" if spark_dtype_is_numeric(source_dtypes[c]) else "nonnumeric")
        for c in feature_cols
        if c in source_dtypes
    }
    errors = (
        # B1. A counted item list (#379) has nothing to hold sample_pool to
        # before the fit counts it: None, and only label_table is checked.
        item_coverage_errors(
            item,
            None if item_list_counted_from_data(parameters)
            else resolved_item_values(parameters),
            _values(sample_pool_items),
            _values(label_items),
        )
        # B15 — two combinations of a multi-column item may not combine to one
        # value. Both tables together: a label whose combination collides with
        # a candidate's would join that candidate.
        + combined_item_collision_errors(
            item_sources, sample_pool_items + label_items,
        )
        # B17 — one type per item column across the tables (read above).
        + item_dtype_errors
        # B5 and B7 once per feature table, so each message names the table
        # that holds the column. Empty for a table that is not declared.
        + categorical_dtype_errors(categorical_cols, ft_dtypes)
        + categorical_dtype_errors(
            categorical_cols, cand_dtypes, table="candidate_feature_table",
        )
        # The dtypes decide B6's advice: "declare it categorical" would send a
        # date column straight into B5 (#407).
        + nonnumeric_feature_errors(
            feature_kinds, set(categorical_cols), source_dtypes,
        )
        + carry_column_collision_errors(
            carry_cols,
            # Reuses the dtypes mapping B5/B6 already read — same metastore
            # metadata lookup, so B7 costs no extra call and no scan.
            set(ft_dtypes),
            drop_cols,
            identity_cols,
            label_col,
        )
        + carry_column_collision_errors(
            carry_cols, set(cand_dtypes), drop_cols, identity_cols, label_col,
            table="candidate_feature_table",
        )
        # B11 — a declared optional role's columns exist in both candidate-grain
        # source tables. `.columns` is metastore metadata, so this reads no
        # rows and stays inside this node's cost invariant. feature_table is
        # not passed on purpose: it joins by base_key_columns, which no
        # optional role widens.
        + optional_role_source_column_errors(
            parameters,
            {
                "sample_pool": sample_pool.columns,
                "label_table": label_table.columns,
            },
        )
        # B12 — no feature may share the zero-positive group weight's name
        # while val or test adds that column. The feature list is derived from
        # metadata above — both feature tables' — so this reads no rows either.
        + zero_positive_group_weight_collision_errors(parameters, feature_cols)
    )
    if candidate_feature_table is not None:
        # B13 — it joins on identity, so it must have every identity column;
        # B14 — a feature is read from one table only. Column names only.
        errors += candidate_feature_table_key_errors(
            identity_cols, candidate_feature_table.columns,
        ) + feature_table_overlap_errors(
            feature_table.columns, candidate_feature_table.columns,
            drop_cols, identity_cols, label_col,
        )
    _raise_if_any(errors)


def select_train_keys(sample_pool: DataFrame, parameters: dict) -> DataFrame:
    """Select train identity keys: which rows are eligible, and which are drawn.

    ``sample_pool`` is at (time, entity, item) granularity and is the widest
    frame this pipeline reads, which is why each step below narrows before the
    next one runs.

    The four decisions below are spelled out in the node body rather than
    shared through a helper: a helper holding four decisions is what ADR-0008
    §2 forbids, and spelling them out is what makes the node readable on its
    own. No other node gives the same four *answers* since #414 removed
    ``select_calibration_keys``: ``select_val_keys`` draws per *entity* over a
    de-duplicated population and carries nothing, and ``select_test_keys``
    makes no draw at all.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    identity_key = schema["identity_columns"]
    # Decision — a multi-column item is combined into one column on read
    # (ADR-0027); a single declared column comes back untouched.
    sample_pool = combine_item_columns(sample_pool, schema, "sample_pool")

    ds = parameters["dataset"]
    seed = parameters.get("random_seed", 42)
    group_keys = ds.get("sample_group_keys", [time_col])
    carry_columns = ds.get("carry_columns", []) or []
    sample_ratio = ds["sample_ratio"]
    overrides = ds.get("sample_ratio_overrides", {})
    train_months = [pd.Timestamp(d) for d in ds["train_snap_dates"]]

    # Decision — eligibility: only rows in the configured train months can be
    # drawn. A month belongs to exactly one split (A24), so this is also what
    # keeps train disjoint from val / test.
    pool = sample_pool.filter(months_filter_as_date(time_col, train_months))
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


def split_train_keys(
    sample_keys: DataFrame,
    parameters: dict,
) -> tuple[DataFrame, DataFrame]:
    """Split sampled keys into train and train-dev by entity ratio.

    All rows of a given entity are assigned to the same split, so no entity
    straddles the boundary. That holds by construction here: the side a row
    lands on is a pure function of its own split columns, so two rows of one
    entity cannot disagree. It used to be assembled instead — distinct the
    entities, bucket them, join each side back — which produced the same answer
    through three shuffles and made "no straddle" a property of the join keys
    rather than of the expression. See
    docs/notes/2026-09-06-dataset-pipeline-profiling.md §6.1 for the measurement
    (6.7s → 3.6s on 16M keys, four Exchanges → none).

    Which columns constitute "a given entity" here is the user's to declare
    (``dataset.train_split_keys``, defaulting to the whole ``schema.entity``): a
    leakage unit coarser than the query group is a real situation — several
    accounts of one customer must not land on opposite sides — and the framework
    has no way to know it from the data.

    Two guards stay in the node, because both read data that only exists at run
    time. Rule 11 of docs/agents/pipeline-node-design.md asks which kind each
    one is, since they send the reader to different people: the NULL split unit
    is a **pre-check** (the input arrived broken; the fix is upstream), the
    empty train-dev split is a **post-condition** (this node's own ratio and
    sample produced a useless result).

    Logging still triggers no action. Each guard costs one ``isEmpty``, plus one
    aggregate on its own failing path only — and note which way ``isEmpty``
    short-circuits: it stops at the first matching row, so a *dirty* input
    answers immediately while a clean one has to read ``split_cols`` across the
    whole frame to prove there is nothing there. That is a narrow scan with no
    shuffle, and it is cheaper than the unconditional second pass a plain
    ``count`` would cost on every run.
    """
    # Decision — the split unit: what the user declared, else the whole entity.
    split_cols = get_entity_grouping(parameters, "train_split_keys")

    train_dev_ratio = parameters["dataset"]["train_dev_ratio"]
    seed = parameters.get("random_seed", 42)

    # Decision — a row whose split unit is NULL is dropped, out loud.
    # It belongs to no entity, so it joins to neither features nor labels and
    # would reach training as an all-NULL row. Dropping is what the old
    # distinct-and-join did for free (NULL never equals NULL); a row-wise bucket
    # would happily keep it, since concat_ws skips NULLs. Warn rather than
    # raise: ADR-0006 puts data-quality checks upstream in source_etl, whose
    # primary_key_not_null owns exactly this, and raising here would stop a
    # user's slightly dirty source table from running at all.
    missing_split_unit = any_column_is_null(split_cols)
    dropped = sample_keys.filter(missing_split_unit)
    dropped_any = not dropped.isEmpty()
    if dropped_any:
        warn_dropped_null_split_unit(dropped, split_cols)
    keys = sample_keys.filter(~missing_split_unit)

    # Decision — which side a row lands on: the bucket of its own split unit.
    # The threshold is computed once and the two filters negate each other on
    # it, so they are a complete and disjoint partition regardless of how many
    # actions Spark runs against this plan.
    threshold = ratio_to_threshold(train_dev_ratio)
    bucket = spark_bucket(keys, split_cols, seed, site="split_train_dev")

    train_keys = keys.filter(bucket >= F.lit(threshold))
    train_dev_keys = keys.filter(bucket < F.lit(threshold))

    # An empty train_dev is invisible downstream: it is the early-stopping
    # validation set for every HPO trial (training/nodes.py passes
    # train_dev_lgb_handle as val_dataset), so an empty one means each trial
    # silently runs its full round budget with early stopping never firing —
    # no error, no warning, just worse models and a longer search. Costs one
    # Spark action; see ADR-0005 for the fallback if that ever matters at scale.
    # `!= 0`, not `> 0`: a negative ratio makes ratio_to_threshold return a
    # negative threshold, so `bucket < threshold` is empty and
    # `bucket >= threshold` takes everything — the same silent state, reached
    # by one stray minus sign. Only an exact 0 means "no dev split wanted".
    if train_dev_ratio != 0 and train_dev_keys.isEmpty():
        n_entities = keys.select(*split_cols).distinct().count()
        split_unit = ", ".join(split_cols)
        if n_entities == 0:
            # Two ways to get here, and they are fixed in different places, so
            # the message must not name the wrong one. Rows can never have
            # arrived (a sampling or partition-filter problem), or they can
            # have arrived and all been dropped for a NULL split unit (a source
            # table problem). The old wording only knew the first.
            cause = (
                "Every row was dropped for a NULL split unit — see the warning "
                "above — so the split itself received nothing. Fix the source "
                "table's key columns."
                if dropped_any else
                "The cause is upstream of the split — check "
                "dataset.sample_ratio, dataset.train_snap_dates, and any "
                "partition filter applied when sample_keys was read back."
            )
            raise ValueError(
                f"split_train_keys received no sampled keys at all "
                f"(train_dev_ratio={train_dev_ratio}), so train and train-dev "
                f"are both empty. {cause}"
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
    # Decision — a multi-column item is combined into one column on read
    # (ADR-0027); a single declared column comes back untouched.
    sample_pool = combine_item_columns(sample_pool, schema, "sample_pool")

    ds = parameters["dataset"]
    val_dates = [pd.Timestamp(d) for d in ds.get("val_snap_dates", [])]
    val_sample_ratio = ds.get("val_sample_ratio", 1.0)
    seed = parameters.get("random_seed", 42)

    # Decision — eligibility: only the configured val months.
    val_labels = sample_pool.filter(months_filter_as_date(time_col, val_dates))
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
    # Decision — a multi-column item is combined into one column on read
    # (ADR-0027); a single declared column comes back untouched.
    sample_pool = combine_item_columns(sample_pool, schema, "sample_pool")

    test_labels = sample_pool.filter(months_filter_as_date(time_col, month_plan.to_process))
    all_keys = test_labels.select(*identity_key).dropDuplicates()

    logger.info("Test keys (full population)")
    return all_keys


def build_train_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
) -> DataFrame:
    """build_model_input for the train and the train_dev split.

    Registered under both node names: train_dev is split off the train keys by
    entity, so the two builds read the same months, and nothing else about
    them differs.
    """
    # Decision — the months this build reads: the train months, the ones its
    # keys were drawn from. train_dev is split off that draw, so its build
    # reads the same list (ADR-0029 decision 2).
    train_months = [
        pd.Timestamp(d) for d in parameters["dataset"]["train_snap_dates"]
    ]
    return build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata,
        parameters, candidate_feature_table, months=train_months,
    )


def build_val_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
) -> DataFrame:
    """build_model_input for the val split, plus the new-item warning (#379).

    With ``schema.categorical_values[<item>]: from_train_data`` the val items
    the preprocessor's list lacks are named here, off ``keys`` — the landed
    ``val_keys`` table — rather than off the model_input: that one is the
    unlanded join below, and asking it for its items would run the whole join
    a second time. The keys need no month scope, unlike test's:
    ``val_snap_dates`` is in ``base_dataset_version``, so ``val_keys`` holds
    exactly the months this node builds. The tables they are joined against
    hold every month, and are read for the val months only.
    """
    schema = get_schema(parameters)
    # Decision — the months this build reads: the val months (ADR-0029
    # decision 2).
    val_months = [
        pd.Timestamp(d) for d in parameters["dataset"].get("val_snap_dates", [])
    ]
    # Decision — a val item the counted item list lacks (#379) is kept and
    # warned about, not refused: its rows are scored like the others (the
    # model sees the unknown code), which is how it will look in production
    # until the train months move. The items only, no counts. A listed item
    # list has none (B1 refuses them upstream), so nothing is counted then.
    # Asked of the keys, which hold exactly the model_input's items (every
    # join in the build is a left join from them): one column of a landed
    # table instead of a second run of the join (ADR-0006's cost rule, the
    # one B10 follows). The list is the preprocessor's, the one the encoding
    # reads.
    if item_list_counted_from_data(parameters):
        item = schema["item"]
        with log_step(logger, "warn_items_outside_the_list(val)"):
            warn_items_outside_the_list(
                keys, item, preprocessor_item_values(preprocessor_metadata, item),
                "val",
            )
    return build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata,
        parameters, candidate_feature_table, months=val_months,
    )


def build_test_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
) -> DataFrame:
    """build_model_input for the test split, scoped to ``month_plan``.

    The one thing this adds over the shared ``build_model_input``: ``test_keys``
    is a persistent Hive table holding *every* month under this base version, so
    reading it back gives the full history even when ``select_test_keys`` only
    wrote the new month. Without this filter the downstream join would rebuild
    every month — the ∝N cost ADR-0002 exists to remove.

    With a counted item list (#379) the new-item warning is val's, asked of
    the scoped keys: the months this run builds, no others. Under
    ``--only-test-months`` there is no fit, and ``preprocessor_metadata`` is
    the one on disk — the list these months are encoded by, not a recount of
    today's train-month data.
    """
    schema = get_schema(parameters)
    # Decision — scope: this run's months only, the plan's ``to_process``.
    # The same list scopes the keys here and the tables build_model_input
    # joins them against (ADR-0029 decision 2). Everything after it is the
    # same assembly every other split gets, which is why it is the sibling
    # node rather than a copy.
    test_months = month_plan.to_process
    keys = keys.filter(months_filter_as_date(schema["time"], test_months))
    # Decision — a test item the counted item list lacks is kept and warned
    # about, for val's reason and off the keys for val's reason; the scope
    # above is what keeps a month already landed out of the warning.
    if item_list_counted_from_data(parameters):
        item = schema["item"]
        with log_step(logger, "warn_items_outside_the_list(test)"):
            warn_items_outside_the_list(
                keys, item, preprocessor_item_values(preprocessor_metadata, item),
                "test",
            )
    return build_model_input(
        keys, preprocessed_feature_table, label_table, preprocessor_metadata,
        parameters, candidate_feature_table, months=test_months,
    )


def fit_preprocessor_metadata(
    feature_table: DataFrame,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
    sample_pool: DataFrame | None = None,
    preprocessor_on_disk: dict | None = None,
) -> tuple[dict, dict]:
    """Fit the preprocessor: each categorical's vocabulary, and what a feature is.

    Two feature tables can supply features: the entity-level ``feature_table``
    and, when declared, the candidate-level feature table (ADR-0026; ``None``
    when none is). Everything below is asked of both; the candidate table's
    identity columns are its join key, never a source of features or of a
    vocabulary.

    With ``schema.categorical_values[<item>]: from_train_data`` (#379) the item
    list is counted here from ``sample_pool``'s train months, and
    ``preprocessor_on_disk`` — the file this node is about to overwrite,
    loaded under another catalog entry name (A6), ``None`` when absent — is
    what B19 compares it with. Neither is read for a listed item list.

    Pre-checks (inputs, ADR-0008 §3): each feature table must carry every
    ``train_snap_dates`` month, every identity categorical must be declared
    in ``schema.categorical_values``, and every categorical read from the data
    must be string, an integer type or boolean (B5 again — a sliced run skips
    the gate). With a counted item list: no ``sample_ratio_overrides`` key
    names an item the list lacks (A5, deferred to here from the CLI entry,
    which has no list yet), and the list matches the one on disk (B19).
    Registered backstop: ``schema.item`` must survive into
    ``feature_columns``.

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

    # Decision — a multi-column item is combined on read (ADR-0027), and the
    # source columns go with it: every non-identity column of this table is a
    # feature, so left in they would become two features the model never had
    # when the SQL combined them.
    if candidate_feature_table is not None:
        candidate_feature_table = combine_item_columns(
            candidate_feature_table, schema, "candidate_feature_table",
        )

    # Pre-check: a dataset must be reproducible from feature_table, so a train
    # month that is not there is an error rather than a smaller fit. Timed
    # because it collects the time column's distinct values — one of the few
    # blocks in this module that costs anything (see the module docstring).
    with log_step(logger, "require_months_present(train_snap_dates)"):
        require_months_present(
            feature_table, time_col, train_months, "train_snap_dates",
        )
    # The candidate table's columns minus its identity columns: those are the
    # join key, and an identity categorical among them (``schema.item``) has
    # its full domain declared — this table only holds the values that were
    # shown, so reading a vocabulary from it would shrink the declared one.
    candidate_cols: list[str] = []
    if candidate_feature_table is not None:
        with log_step(logger, "require_months_present(train_snap_dates, candidate)"):
            require_months_present(
                candidate_feature_table, time_col, train_months,
                "train_snap_dates", table="candidate_feature_table",
            )
        candidate_cols = candidate_feature_source_columns(
            candidate_feature_table.columns, identity_cols,
        )
    source_cols = list(feature_table.columns) + candidate_cols

    # A drop_columns name is unused only when neither feature table has it —
    # asked here because this is the one node that sees both. Warns rather than
    # raises: a stale name changes nothing about the output.
    warn_missing_drop_columns(source_cols, drop_cols, "any feature table")

    # Decision — no leakage: vocabularies are fit on the train months only. A
    # category that only ever appears in val/test therefore has no index of its
    # own and encodes to the unknown sentinel, exactly as it would in production
    # on a value the model never trained on.
    train_features = feature_table.filter(
        months_filter_as_date(time_col, train_months)
    )

    # Decision — where a vocabulary comes from: a categorical that is a
    # feature_table column has its domain in the data; an identity categorical
    # is not in feature_table at all, so its domain has to be declared. Reading
    # a declared one from the data would silently shrink it to the values this
    # snapshot happens to contain.
    from_data, from_schema = split_categorical_sources(
        categorical_cols, source_cols,
    )
    # Pre-check: nothing downstream can supply a vocabulary the schema owes.
    cat_values = schema.get("categorical_values", {})
    require_declared_categoricals(cat_values, from_schema)
    # Pre-check (runtime backstop of B5): the vocabulary collection below is
    # exact only on string / integer / boolean, and a sliced run skips the
    # Layer-2 gate that normally rejects the rest — a double column holding NaN
    # would get a wrong vocabulary, not an error, and a date one would crash the
    # JSON save after the scan. Schema metadata only, no Spark job.
    # Asked per table so the message names the one holding the column.
    candidate_from_data = [c for c in from_data if c in candidate_cols]
    entity_from_data = [c for c in from_data if c not in candidate_from_data]
    require_supported_categorical_dtypes(entity_from_data, dict(feature_table.dtypes))
    if candidate_from_data:
        require_supported_categorical_dtypes(
            candidate_from_data, dict(candidate_feature_table.dtypes),
            table="candidate_feature_table",
        )
    # Decision — where the item list comes from: the declaration, or (#379)
    # the train months' sample_pool, before any sampling. Not the sampled
    # model_input: no sampling rate keeps one row per item, so a rare item
    # would come and go with the sampling settings, and so would the offline
    # inference candidates — while this file is shared by every sampling
    # variant of the version. The train months only, for the reason above:
    # an item first offered in val/test is a new item, encoded unknown.
    item = schema["item"]
    counted_items = None
    if item_list_counted_from_data(parameters) and item in from_schema:
        with log_step(logger, "count_items_in_months(train_snap_dates)"):
            counted_items = count_items_in_months(
                sample_pool, schema, train_months)
        logger.info(
            "Item list counted from sample_pool's train months: %d items",
            len(counted_items),
        )
        # Pre-check (runtime A5): an override keyed on an item the list lacks
        # never matches; the CLI entry could not check it without the list.
        unknown = override_unknown_item_errors(parameters, items=counted_items)
        if unknown:
            raise DataConsistencyError("\n".join(unknown))
        # Pre-check (input, B19): the version's preprocessor on disk holds
        # the same list; otherwise this save would re-code its models' items.
        drift = item_list_drift_errors(
            item, counted_items,
            None if preprocessor_on_disk is None
            else preprocessor_item_values(preprocessor_on_disk, item),
            parameters.get("base_dataset_version", "<unresolved>"),
        )
        if drift:
            raise DataConsistencyError("\n".join(drift))
    declared_cols = [c for c in from_schema if c != item or counted_items is None]
    # Each table's vocabularies are read from its own train months — one scan
    # per table, and no scan of a table that has none to give.
    with log_step(logger, "collect_category_mappings"):
        category_mappings = {
            **collect_vocabularies_from_data(train_features, entity_from_data),
            **read_declared_vocabularies(cat_values, declared_cols),
        }
        if counted_items is not None:
            category_mappings[item] = counted_items
        if candidate_from_data:
            category_mappings.update(collect_vocabularies_from_data(
                candidate_feature_table.filter(
                    months_filter_as_date(time_col, train_months)
                ),
                candidate_from_data,
            ))

    # Decision — feature order is LightGBM's, so it is fixed: identity
    # categoricals, the entity-level table's columns, then the candidate
    # table's. With no candidate table this is the order it has always been.
    feature_columns = compute_feature_columns(
        source_cols,
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
    identity_cols = schema["identity_columns"]

    feature_columns = preprocessor_metadata["feature_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    drop_cols = preprocessor_metadata["drop_columns"]

    base_key = schema["base_key_columns"]
    months = month_plan.to_process

    # Pre-checks. (Unused drop_columns names are reported by the fit, the one
    # node that sees every feature table — ADR-0026.)
    require_base_key_columns(feature_table.columns, base_key)
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


def validate_numeric_precision(
    preprocessed_feature_table: DataFrame,
    preprocessor_metadata: dict,
    month_plan: SnapDatePlan,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
    test_month_plan: SnapDatePlan | None = None,
    only_test_months: bool = False,
) -> dict:
    """Measure the precision headroom of the months just encoded, and gate on it.

    Two tables are measured when a deployment declares the candidate-level
    feature table (ADR-0026; ``None`` otherwise): the entity-level one from the
    footers of the months just encoded, as below, and the candidate-level one
    from one scan of the months this run reads — it is never landed, so there
    are no footers to read. The report gains a ``candidate_feature_table``
    section only then, so a deployment without one gets the report it always
    got.

    Returns the report (catalog entry ``numeric_precision_report``); raises
    ``DataConsistencyError`` on a breach under the default policy. The rule
    itself lives with its predicates in ``core/consistency.py`` — this node
    gathers facts, hands them over, and applies the configured policy.

    **Why it reports rather than only gating.** A pass/fail answer tells an
    operator nothing about the column sitting at 0.99 of its limit, which passes
    today and stops the pipeline the month a larger value arrives. The report
    carries every checked column with its headroom, closest-to-breaching first;
    the gate is the same numbers read as a verdict.

    **Why it sits between the write and the reads.** The values this node is
    about are narrowed by ``cast_numeric_features_to_storage_type`` inside
    ``build_model_input``, which is downstream; ``preprocessed_feature_table``
    itself keeps its source dtypes. So it reads a table that has already landed
    — the runner saves a node's output before the next node loads it — and still
    stops the run before a single narrowed value is stored. Reading the landed
    table rather than ``feature_table`` is what lets it assume parquet with
    statistics: this repo writes that table, while ``feature_table`` is the
    user's own and this framework does not dictate its format.

    **Cost invariant (ADR-0006).** For ``preprocessed_feature_table`` the facts
    come from parquet footers — a seek per file, no rows read — so that half of
    the node does not change the pipeline's cost magnitude. An aggregation
    would have, which is why that half is a footer read and not a
    ``max(abs(...))``. The candidate-level table is the recorded exception
    (ADR-0006's 2026-09-22 revision): it has no footers of this run's own, so
    its half *is* one aggregation over the months this run reads.

    **Incremental with the node above it** (ADR-0002/ADR-0012), for
    ``preprocessed_feature_table``: it reads the months the same ``month_plan``
    just encoded. A month that landed under an earlier run is not re-read — the
    same coverage every other incremental artifact has, and the reason adding an
    evaluation month checks that month. The candidate-level table is not
    incremental: every build reads its months afresh, so every run checks every
    month its builds read — the union of what each build reads, worked out
    here by the builds' own rules (ADR-0029 decision 2): ``train_snap_dates``,
    ``val_snap_dates``, and ``test_month_plan``'s ``to_process``. Which builds
    run is the one fact this node cannot see, so it is the one injected:
    ``only_test_months`` (the run mode), under which only the test build runs.

    Pre-check (input), candidate-level table only: every month this run reads
    is present in it. A missing month raises ``ValueError`` **whatever the
    policy** — it is not a precision question, and ``truncate`` exists to accept
    a narrowed value, not a month of NULL features.

    Known limit: under ``block`` the run aborts, so the report never reaches the
    catalog. The full table is logged before the raise for exactly that reason —
    the survey workflow (run once under ``truncate``, read the artifact) is what
    the persisted report is for.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    feature_columns = preprocessor_metadata["feature_columns"]
    storage_type, policy = resolved_numeric_storage(parameters)
    months = month_plan.to_process

    # Decision — what this gate may complain about is exactly what the cast will
    # convert, narrowed to the columns whose dtype states a grid spacing. The
    # first half reads the cast's own selector, so widening the cast widens the
    # gate in the same edit; the second half is the bound's domain — float and
    # double declare no grid, so no bound can be stated for them.
    castable = castable_numeric_feature_columns(
        preprocessed_feature_table.schema, feature_columns,
    )
    dtypes = dict(preprocessed_feature_table.dtypes)
    steps = {
        c: spark_dtype_value_step(dtypes[c])
        for c in castable
        if spark_dtype_value_step(dtypes[c]) is not None
    }
    checked = sorted(steps)
    report = _precision_report(storage_type, policy, months, {}, dtypes)

    errors: list[str] = []
    if not checked or not months:
        logger.info(
            "Numeric precision gate (%s): nothing to check "
            "(castable=%d, with a value grid=%d, months=%d)",
            storage_type, len(castable), len(checked), len(months),
        )
    else:
        # Decision — the facts are read from the partitions this run just
        # wrote, not from the whole table: inputFiles() answers for the
        # relation, so the run's own version and months are selected back out
        # of the paths.
        files = landed_partition_files(
            preprocessed_feature_table.inputFiles(),
            base_version=parameters["base_dataset_version"],
            time_col=time_col,
            months=months,
        )
        if not files:
            # Not folded into the per-column "no statistics" message: the fix
            # is different (the gate could not find the data at all, rather
            # than found it and learned nothing), and reporting it once beats
            # reporting it once per column.
            errors.append(
                f"B8: found no parquet files for the {len(months)} month(s) this "
                f"run wrote under base_dataset_version="
                f"{parameters['base_dataset_version']}, so the precision of "
                f"{len(checked)} feature column(s) could not be established. The "
                f"gate reads footer statistics from the landed partitions; set "
                f"dataset.numeric_precision_policy: truncate to proceed without "
                f"that check."
            )
        else:
            max_abs = read_max_abs_stats(
                preprocessed_feature_table.sparkSession, files, checked,
            )
            by_column = {
                c: ColumnPrecision(max_abs[c], steps[c]) for c in checked
            }
            report = _precision_report(
                storage_type, policy, months, by_column, dtypes,
            )
            logger.info(
                "Numeric precision gate (%s): %d column(s) over %d file(s) in "
                "%d month(s)", storage_type, len(checked), len(files), len(months),
            )
            for line in _precision_report_lines(report):
                logger.info("%s", line)
            errors = numeric_precision_errors(by_column, storage_type)

    # Decision — the candidate-level feature table is cast by
    # build_model_input like every other feature, so it is this gate's too. Its
    # facts come from one aggregation over the months this run reads: it is
    # never landed, and the framework does not dictate the format of the
    # deployment's own table, so there are no footers to read. That scan is the
    # exception to ADR-0006's cost invariant that ADR-0026 records. Its
    # categoricals are left out — they are encoded to vocabulary indices before
    # the cast, so their raw values are not what gets narrowed.
    if candidate_feature_table is not None:
        identity_cols = schema["identity_columns"]
        categorical_cols = preprocessor_metadata["categorical_columns"]
        # Decision — the months checked are the months this run's builds read,
        # by each build's own rule: the train months (train and train_dev),
        # the val months, and the test plan's to_process. Under
        # --only-test-months the test build is the only one in the run.
        if test_month_plan is None:
            # Pre-check (input): without the test build's plan its months
            # would go unchecked, and nothing would say so — the reason
            # build_model_input's ``months`` has no default either.
            raise TypeError(
                "validate_numeric_precision needs test_month_plan when a "
                "candidate_feature_table is given: it checks the months the "
                "test build reads."
            )
        ds = parameters["dataset"]
        build_months = [test_month_plan.to_process]
        if not only_test_months:
            build_months += [ds["train_snap_dates"], ds.get("val_snap_dates", [])]
        cand_months = sorted(
            {pd.Timestamp(m) for months in build_months for m in months}
        )
        cand_dtypes = dict(candidate_feature_table.dtypes)
        cand_steps = {
            c: spark_dtype_value_step(cand_dtypes[c])
            for c in castable_numeric_feature_columns(
                candidate_feature_table.schema,
                [
                    c for c in candidate_feature_source_columns(
                        candidate_feature_table.columns, identity_cols,
                    )
                    if c in feature_columns and c not in categorical_cols
                ],
            )
            if spark_dtype_value_step(cand_dtypes[c]) is not None
        }
        cand_checked = sorted(cand_steps)
        with log_step(logger, "scan candidate_feature_table (months, max |x|)"):
            present, cand_max_abs = months_present_and_max_abs(
                candidate_feature_table, time_col, cand_months, cand_checked,
            )
        # Pre-check: every month this run reads is there. A missing one would
        # become a month of NULL candidate features without a word — not a
        # precision question, so it raises whatever the policy says.
        require_months_in(
            present, cand_months, "snap_dates", "candidate_feature_table",
        )
        cand_by_column = {
            c: ColumnPrecision(cand_max_abs[c], cand_steps[c]) for c in cand_checked
        }
        report["candidate_feature_table"] = _precision_report(
            storage_type, policy, cand_months, cand_by_column, cand_dtypes,
        )
        logger.info(
            "Numeric precision gate (%s): %d candidate_feature_table column(s) "
            "over %d month(s)", storage_type, len(cand_checked), len(cand_months),
        )
        for line in _precision_report_lines(report["candidate_feature_table"]):
            logger.info("%s", line)
        errors += [
            f"candidate_feature_table: {e}"
            for e in numeric_precision_errors(cand_by_column, storage_type)
        ]

    if not errors:
        return report

    message = (
        f"Numeric precision check failed for "
        f"dataset.numeric_feature_storage_type={storage_type} "
        f"({len(errors)} issue(s)):\n- " + "\n- ".join(errors)
    )
    # Decision — the policy key is what turns a finding into a stop. `block`
    # is the default because the failure it describes is silent everywhere
    # else; `truncate` exists so an operator who has read the finding can accept
    # the loss without editing the gate out.
    if policy == "block":
        raise DataConsistencyError(message)
    logger.warning("%s", message)
    return report


def _precision_report(
    storage_type: str,
    policy: str,
    months: list,
    by_column: dict,
    dtypes: dict,
) -> dict:
    """Assemble the persisted shape of the precision measurement.

    Module-private: the shape is this node's output contract, not something a
    second caller reuses. ``dtypes`` is carried into each row because a reader
    asking "why is this column's limit so low" needs the scale, and looking it
    up means finding the table this report is about.
    """
    rows = numeric_precision_rows(by_column, storage_type)
    for row in rows:
        row["dtype"] = dtypes.get(row["column"])
    return {
        "storage_type": storage_type,
        "policy": policy,
        "months": [pd.Timestamp(m).strftime("%Y-%m-%d") for m in months],
        "checked_columns": len(rows),
        "breaches": sum(1 for r in rows if r["verdict"] == "breach"),
        "unmeasured": sum(1 for r in rows if r["verdict"] == "unmeasured"),
        "columns": rows,
    }


def _precision_report_lines(report: dict) -> list[str]:
    """The report as fixed-width log lines, closest-to-breaching first.

    Logged as well as returned because ``block`` aborts the run before the
    catalog can write the artifact — the one case where an operator most needs
    the numbers is the one where the file does not exist.
    """
    lines = [
        f"  {'column':<28} {'dtype':<16} {'max(|x|)':>18} "
        f"{'limit':>18} {'headroom':>12}  verdict"
    ]
    for row in report["columns"]:
        headroom = "-" if row["headroom"] is None else f"{row['headroom']:.3g}x"
        max_abs = "-" if row["max_abs"] is None else f"{row['max_abs']:,.10g}"
        lines.append(
            f"  {row['column']:<28} {str(row['dtype']):<16} {max_abs:>18} "
            f"{row['limit']:>18,.10g} {headroom:>12}  {row['verdict']}"
        )
    return lines


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
    candidate_feature_table: DataFrame | None = None,
    *,
    months: list,
) -> DataFrame:
    """Assemble a split's model_input from its keys, the labels and the features.

    Not a node itself: the build node functions call it, each after deciding
    ``months`` — the months its split reads (ADR-0029 decision 2). Keyword-only
    and without a default, because a build that forgot it would otherwise read
    another split's months, and every key would find no label and no feature
    without a word.

    Features come from the entity-level feature table (already encoded, as
    ``preprocessed_feature_table``) and, when a deployment declares one, the
    candidate-level feature table (ADR-0026) — ``None`` means none is declared,
    which is what the CLI registers in that case.

    Pre-check (input, ADR-0008 §3): ``keys`` must be at item grain.
    Post-condition: identity, label and every feature column survive the joins.
    """
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    # Separate names, because the joins below want different things and one
    # `base_key` used to serve both (ADR-0025 decision 2). They hold the same
    # columns today and part company the moment an `occasion` is declared:
    # `label_table` is at candidate grain and follows identity, while
    # `feature_table` is entity-level and cannot widen. The candidate-level
    # feature table is at candidate grain too, so it follows identity with the
    # labels (ADR-0026) — a third name for the join that reads it.
    label_join_key = identity_cols
    feature_join_key = schema["base_key_columns"]
    candidate_join_key = identity_cols

    feature_columns = preprocessor_metadata["feature_columns"]

    # Decision — a multi-column item is combined on read (ADR-0027); a single
    # declared column comes back untouched. Both tables are the user's own.
    label_table = combine_item_columns(label_table, schema, "label_table")
    if candidate_feature_table is not None:
        candidate_feature_table = combine_item_columns(
            candidate_feature_table, schema, "candidate_feature_table",
        )

    # Pre-check: keys' grain IS model_input's grain (ADR-0005). This used to
    # fall back to a base-key-only label join when item was absent, which
    # silently multiplied every (time, entity) by label_table's item count and
    # took `item`'s values from label_table. No caller can reach that branch —
    # identity_columns is derived by core/schema.py::get_schema and
    # every node feeding this one passes it — but its failure mode is a silently
    # N-times-too-large dataset, so a missing column is an error, not a mode.
    require_columns_present(keys.columns, label_join_key, "build_model_input keys")

    # Decision — every table the keys are joined against is read for this
    # split's months only, and the filter is written here rather than left to
    # the optimizer (ADR-0029 decision 1). Every join key holds time, so
    # another month's rows could never match: the filter changes the cost,
    # never the answer. Nothing else prunes these reads: the joins are LEFT
    # from the keys, so only the table on the right can be broadcast, and
    # dynamic partition pruning under Spark's defaults needs the keys' side
    # broadcast — measured 2026-09-25, every partition read, while the plan
    # printed before execution looks as if it prunes. A table partitioned by
    # time skips the other months' partitions; an unpartitioned one is still
    # scanned, but only this split's rows reach the join.
    in_months = months_filter_as_date(schema["time"], months)
    label_table = label_table.filter(in_months)
    preprocessed_feature_table = preprocessed_feature_table.filter(in_months)

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
        dataset, preprocessed_feature_table, feature_join_key,
    )

    # Decision — the candidate-level feature table joins on identity: one of
    # its rows describes one candidate, so the base key would hand every
    # candidate the rows of every other candidate of its entity. A miss keeps
    # the row with NULL candidate-level features, for the entity-level join's
    # reason above.
    #
    # Read, encoded and joined here, after sampling, rather than landed
    # beforehand like the entity-level table: it is as large as sample_pool, and
    # landing it would write every row sampling is about to throw away
    # (ADR-0026). Its months are the split's, as the other two tables'; the
    # vocabulary is the fitted one, so an unseen value is the unknown sentinel
    # here exactly as it is in apply_preprocessor_to_features.
    if candidate_feature_table is not None:
        candidate = candidate_feature_table.filter(in_months).select(
            *encoded_frame_columns(
                candidate_join_key, feature_columns, candidate_feature_table.columns,
            )
        )
        encode_cols = encodable_categoricals(
            preprocessor_metadata["categorical_columns"], candidate.columns,
            identity_cols,
        )
        if encode_cols:
            candidate = encode_categoricals(
                candidate, encode_cols, preprocessor_metadata["category_mappings"],
            )
        dataset = join_features_missing_as_null(dataset, candidate, candidate_join_key)

    # Decision — what a model_input row is made of: identity, the label, every
    # feature, and the columns the keys carried in for downstream weighting.
    # Everything else the joins brought along is dropped here.
    required = list(set(identity_cols + [label_col] + feature_columns))
    require_columns_present(dataset.columns, required, "build_model_input")
    result = dataset.select(*model_input_columns(
        keys.columns, dataset.columns, identity_cols, label_col, feature_columns,
    ))

    # Decision — numeric features converge on one storage type, the one
    # ``dataset.numeric_feature_storage_type`` declares. LightGBM is
    # histogram-based, so float32's precision is already beyond what binning can
    # use; decimal128 in particular materialises as Python Decimal objects and
    # was what OOM-killed the val read. Which types get cast, and why the answer
    # is "every numeric one", is the helper's business.
    storage_type, _ = resolved_numeric_storage(parameters)
    result, casted = cast_numeric_features_to_storage_type(
        result, feature_columns, storage_type,
    )
    logger.info(
        "build_model_input: %d features, cast %d numeric feature columns to %s",
        len(feature_columns), len(casted), storage_type,
    )
    if casted:
        logger.debug("build_model_input: casted columns = %s", casted)
    return result


#: Why val / test carry no row-count gate. One template because the first half
#: is the same sentence for both; ``extra`` carries what is true of only one of
#: them, so the shared half still has a single source.
_NOT_CHECKED_REASON = (
    "{split}_model_input is the filter_{split}_model_input output, so its "
    "row count is deliberately below its keys'; the frame that would match, "
    "{split}_model_input_unfiltered, has no catalog entry and so never lands "
    "as parquet — there is no footer to read.{extra}"
)


def _footer_rows(
    df: DataFrame,
    partition_filter: dict,
) -> tuple[int, int, int]:
    """``(rows, matched files, files the table has)`` — footer arithmetic only.

    Pure mechanism: it counts, it does not judge. Whether a table that has files
    but matched none of them is an error or a zero is a decision, and it lives in
    the node body where a reader of the node can see it (rule 4 of
    ``docs/agents/pipeline-node-design.md``); returning the two file counts
    separately is what leaves that decision available to make.
    """
    all_paths = df.inputFiles()
    files = filter_by_partitions(all_paths, partition_filter)
    return read_row_count(df.sparkSession, files), len(files), len(all_paths)


def validate_model_input_grain(
    train_keys: DataFrame,
    train_model_input: DataFrame,
    train_dev_keys: DataFrame,
    train_dev_model_input: DataFrame,
    parameters: dict,
) -> dict:
    """Pin each model_input's row count to its keys table's (B10).

    Returns the report (catalog entry ``model_input_grain_report``); raises
    ``DataConsistencyError`` on a mismatch. The rule itself lives with its
    predicate in ``core/consistency.py`` — this node pairs the tables, gathers
    the counts, and hands them over. Its module docstring's B10 section is the
    canonical statement of what the invariant covers and what it does not; this
    docstring says how the pairing is made and why the counts are cheap.

    **What it catches.** ``build_model_input``'s own comment names the failure
    it fears — "a silently N-times-too-large dataset" — and guards only one of
    its two causes: ``require_columns_present`` stops a join key that lost the
    item column. The other cause, a right table (``label_table`` or
    ``preprocessed_feature_table``) holding a join key twice, had nothing
    watching it. The upstream ``max_duplicate_key_ratio`` contract does not
    close it either: A32 passes when ``primary_key`` and ``quality_checks`` are
    both absent, ``0.5`` is a legal ratio, and this framework lets a user supply
    source tables its own ``source_etl`` never wrote (ADR-0006's own "not
    solved" list opens with exactly that deployment).

    **The pairing.**

    ::

        train_keys        -> build_train_model_input        -> train_model_input
        train_dev_keys    -> build_train_dev_model_input    -> train_dev_model_input

    Each row is a build node with nothing between its two ends, which is what
    makes equality the right comparison. The one thing worth checking rather
    than assuming: everything that narrows the train-side keys — the split,
    and the zero-positive group draw after it (``filter_train_keys``,
    ADR-0025) — runs *before* the builds, not after, so ``train_keys`` and
    ``train_dev_keys`` are each already the exact input of their own build
    node. That is why the draw is on the keys at all. Crossing a pair (``train_keys`` against
    ``train_dev_model_input``) would make the gate always-false rather than
    merely absent, so ``test_the_pairing_is_keys_then_its_own_model_input``
    pins the argument order against the pipeline's input list.

    **val and test are absent by necessity.** Neither has a landed frame at its
    keys' grain to compare against, and for test the missing frame would not be
    ``test_keys``-shaped anyway. The full argument, including the one-sided
    bound that was considered and rejected and the residual risk this leaves,
    is in the B10 section of ``core/consistency.py``'s module docstring.

    **Why it is a node after the builds rather than a post-condition inside
    one.** Getting a row count inside ``build_model_input`` means ``count()`` on
    an unlanded frame — a full scan, the cost escalation ADR-0006 exists to
    refuse. Once the table has landed, the same fact is a seek per file. Landing
    is the Runner's doing (it saves a node's output before the next node loads
    it), so the gate has to be its own node; B8 sits where it does for the same
    reason. It still stops the run before anything trains on the bad table.

    **Why it reports rather than only gating.** A pass/fail answer cannot say
    how many rows each split actually holds, which is the number an operator
    wants when a downstream memory estimate is wrong. Known limit, shared with
    B8: on a raise the report never reaches the catalog, because the node does
    not return. The counts are logged before the raise for that reason.
    """
    base_version = parameters["base_dataset_version"]
    train_scope = {
        "base_dataset_version": base_version,
        "train_variant_id": parameters["train_variant_id"],
    }

    # Decision — which pairs this gate can speak for, and under which partition
    # scope each side's files are counted. The scope is not decoration: these
    # tables accumulate versions and variants side by side under one Hive table,
    # and `inputFiles()` answers for the whole relation rather than for the
    # partition_filter the catalog loaded them with.
    pairs = [
        ("train", train_keys, train_model_input, train_scope),
        ("train_dev", train_dev_keys, train_dev_model_input, train_scope),
    ]

    by_split: dict[str, SplitRowCounts] = {}
    splits: dict[str, dict] = {}
    errors: list[str] = []
    for split, keys, model_input, scope in pairs:
        counted: dict[str, int] = {}
        for side, df in (("keys", keys), ("model_input", model_input)):
            rows, matched, present = _footer_rows(df, scope)
            # Pre-check (this node's own inputs) — a scope that matches none of
            # a table's files makes both sides of the comparison read 0, every
            # comparison below trivially true, and the gate would report
            # success having looked at nothing. So it is reported rather than
            # counted as zero rows. A table with no files AT ALL is a
            # different fact and passes: that is a genuinely empty split,
            # which `dataset.train_dev_ratio: 0` produces on purpose.
            if present and not matched:
                spec = ", ".join(f"{k}={v}" for k, v in scope.items())
                errors.append(
                    f"B10: {split}_{side} has {present} parquet file(s) but "
                    f"none under {spec}, so this run's row count could not be "
                    f"established and the comparison for {split} would have "
                    f"passed on two zeroes. Either the version/variant in "
                    f"parameters no longer matches what is on disk, or the "
                    f"table was written by a different catalog entry than the "
                    f"one this node reads."
                )
            counted[side] = rows
            counted[f"{side}_files"] = matched
        by_split[split] = SplitRowCounts(counted["keys"], counted["model_input"])
        splits[split] = {
            "keys_rows": counted["keys"],
            "keys_files": counted["keys_files"],
            "model_input_rows": counted["model_input"],
            "model_input_files": counted["model_input_files"],
        }
        logger.info(
            "Model input grain gate: %s keys=%d row(s) in %d file(s), "
            "model_input=%d row(s) in %d file(s)",
            split, counted["keys"], counted["keys_files"],
            counted["model_input"], counted["model_input_files"],
        )

    report = {
        # The version and variant this report describes. The catalog keys its
        # file on base_dataset_version alone (as numeric_precision_report does),
        # so two runs of different train variants overwrite one file; carrying
        # the variant inside is what stops a reader attributing one variant's
        # counts to another.
        "base_dataset_version": base_version,
        "train_variant_id": parameters["train_variant_id"],
        "splits": splits,
        # Named in the artifact, not only in this docstring: a reader who pulls
        # the report to ask "was my dataset checked" must not have to infer from
        # two absent keys that two splits were deliberately left out.
        "not_checked": {
            "val": _NOT_CHECKED_REASON.format(
                split="val", extra=""),
            "test": _NOT_CHECKED_REASON.format(
                split="test",
                extra=(
                    " test is doubly out of reach: build_test_model_input "
                    "re-scopes test_keys to this run's months first, so even a "
                    "landed unfiltered frame would match only that subset, not "
                    "the whole persistent test_keys table."
                ),
            ),
        },
    }

    # Collect-all, one raise: the measurement failures above and the grain
    # mismatches below are both "this gate has something to say about a split",
    # and an operator fixing one wants to see the other in the same pass. Same
    # shape B8 uses.
    errors += model_input_grain_errors(
        by_split, get_schema(parameters)["identity_columns"],
    )
    if errors:
        raise DataConsistencyError(
            f"Model input grain check failed ({len(errors)} issue(s)):\n- "
            + "\n- ".join(errors)
        )
    return report


def filter_train_keys(
    keys: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Keep the train-side query groups holding a positive, and a share of the
    ones holding none (ADR-0025 decision 3).

    Serves both ``filter_train_keys`` and ``filter_train_dev_keys``: one key,
    ``dataset.train_zero_positive_group_ratio``, one ratio, and each side judges
    only its own groups — the split is by entity, so no query group straddles
    the two.

    **Why on the keys and not on model_input**, unlike val / test: B10 pins
    ``train_model_input``'s row count to ``train_keys``' to catch a right table
    that holds a join key twice. Dropping groups after the build would make the
    two counts differ on purpose, and B10 could no longer tell a deliberate drop
    from a fan-out — a drop of 1,000 rows hides a 500-row fan-out. Dropping them
    here, before ``train_keys`` lands, keeps the pair equal. The price is one
    extra label join, paid only when the ratio is below 1.

    What this step sees is what the row-level draw and the train / train_dev
    split left: "a group holding a positive is kept whole" means this step drops
    none of its rows, not that the row draw left it alone. A group whose only
    positive the row draw removed is a zero-positive group here (ADR-0025).
    """
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    group_cols = schema["query_group_columns"]
    label_col = schema["label"]
    ratio = resolved_zero_positive_group_ratio(parameters, "train")
    seed = parameters.get("random_seed", 42)

    # Decision — the default keeps every group, so the keys pass through
    # untouched: no label join, no Spark action, and train_keys lands exactly
    # what the split produced — the table every existing train variant holds.
    if ratio >= 1.0:
        log_zero_positive_group_draw("train", ratio, None)
        return keys

    # Decision — a multi-column item is combined on read (ADR-0027).
    label_table = combine_item_columns(label_table, schema, "label_table")
    # Decision — label_table is read for the train months only: the keys of
    # both train and train_dev were drawn from them. Written here for the
    # build's reason (build_model_input, ADR-0029 decision 1): the join would
    # never match another month, and nothing else prunes this read.
    train_months = [
        pd.Timestamp(d) for d in parameters["dataset"]["train_snap_dates"]
    ]
    label_table = label_table.filter(
        months_filter_as_date(schema["time"], train_months)
    )

    # Decision — whether a group holds a positive is read from label_table,
    # never from a label column sample_pool may carry. That copy is the user's
    # SQL and nothing guarantees it agrees; judged by it, a disagreement would
    # delete real positives together with their group, and B10 would pass
    # because keys and model_input lose the same rows. Only identity and the
    # label cross the join, so a carried column cannot collide with it.
    labels = join_labels_missing_as_negative(
        keys.select(*identity_cols),
        label_table.select(*identity_cols, label_col),
        identity_cols, label_col,
    )

    # Decision — a group holding a positive is kept whole; a group holding none
    # is kept whole or dropped whole, `ratio` of them kept. No weight column:
    # training ranks, and a ranking needs no proportions restored.
    kept_groups = keep_zero_positive_groups_drawn_under_ratio(
        labels, group_cols, label_col, ratio, seed,
    ).select(*group_cols).distinct()

    # Decision — the keys keep their own rows and columns. A semi join on the
    # query group can only drop a key, never repeat one, so a label_table
    # holding a key twice cannot fan out here and still reaches B10 through the
    # build. NULL-safe on every group column: a group whose key holds a NULL is
    # one group to the draw (the val / test window partitions it like any
    # other), and a plain equi-join would drop it whatever the draw decided.
    kept = keys.join(
        kept_groups,
        on=reduce(operator.and_, [
            keys[c].eqNullSafe(kept_groups[c]) for c in group_cols
        ]),
        how="left_semi",
    )

    # Decision — a partial draw reports how many zero-positive groups it kept.
    # One narrow Spark action (keys and labels only), paid only when 0 < r < 1.
    counts = None
    if ratio > 0.0:
        with log_step(logger, "count_zero_positive_groups"):
            counts = count_zero_positive_groups_kept(
                labels, group_cols, label_col, ratio, seed,
            )
    log_zero_positive_group_draw("train", ratio, counts)
    return kept


def filter_val_model_input(
    model_input: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Keep val's query groups holding a positive, and a share of the ones
    holding none (``dataset.val_zero_positive_group_ratio``, ADR-0025
    decision 3).

    At the default 0 this is the filter val always had: a group with no
    positive is dropped, because a ranking metric cannot be computed over it.
    Above 0 the kept zero-positive groups stay for the metrics that score every
    row as a binary prediction, which a table holding only groups with a
    positive biases high.

    Applied to the built model_input rather than to the keys (the train side's
    choice): nothing pins val's row count to its keys (B10 covers train and
    train_dev only), so there is nothing a later drop could break, and the
    label is already joined.

    Pre-check (input), inside the draw: a model_input that already holds a
    column named like the weight is refused rather than overwritten — the
    runtime backstop of B12, for a sliced run that skipped the gate.
    """
    schema = get_schema(parameters)
    group_cols = schema["query_group_columns"]
    label_col = schema["label"]
    ratio = resolved_zero_positive_group_ratio(parameters, "val")
    seed = parameters.get("random_seed", 42)

    # Decision — above 0 the rows carry their design weight: 1 in a group
    # holding a positive, 1/r in a kept zero-positive group. At 0 there is
    # nothing to weight, and the table keeps the columns it always had.
    weight_col = ZERO_POSITIVE_GROUP_WEIGHT_COL if ratio > 0.0 else None

    # Decision — a group holding a positive is kept whole; a group holding none
    # is kept whole or dropped whole, `ratio` of them kept. The label is the
    # one build_model_input joined from label_table.
    kept = keep_zero_positive_groups_drawn_under_ratio(
        model_input, group_cols, label_col, ratio, seed, weight_col=weight_col,
    )

    # Decision — a partial draw reports how many zero-positive groups it kept:
    # 1/r is a design weight, and the count is what tells an operator whether
    # the weighted metrics are stable. One Spark action over the group and
    # label columns, paid only when 0 < r < 1.
    counts = None
    if 0.0 < ratio < 1.0:
        with log_step(logger, "count_zero_positive_groups"):
            counts = count_zero_positive_groups_kept(
                model_input, group_cols, label_col, ratio, seed,
            )
    log_zero_positive_group_draw("val", ratio, counts)
    return kept


def filter_test_model_input(
    model_input: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Keep test's query groups holding a positive, and a share of the ones
    holding none (``dataset.test_zero_positive_group_ratio``, ADR-0025
    decision 3).

    val's decisions with test's key: a separate node function because the key
    is the one answer that differs, and reading the wrong one would raise
    nothing. No month scoping: its input comes from build_test_model_input,
    which is already scoped (ADR-0007).

    The weight column this adds above 0 travels on: training writes it into
    ``training_eval_predictions`` (A45 makes the catalog declare it) and the
    prediction-quality family sums it instead of counting rows.

    Pre-check (input), inside the draw: B12's runtime backstop, as in
    ``filter_val_model_input``.
    """
    schema = get_schema(parameters)
    group_cols = schema["query_group_columns"]
    label_col = schema["label"]
    ratio = resolved_zero_positive_group_ratio(parameters, "test")
    seed = parameters.get("random_seed", 42)

    # Decision — above 0 the rows carry their design weight: 1 in a group
    # holding a positive, 1/r in a kept zero-positive group. At 0 there is
    # nothing to weight, and the table keeps the columns it always had.
    weight_col = ZERO_POSITIVE_GROUP_WEIGHT_COL if ratio > 0.0 else None

    # Decision — a group holding a positive is kept whole; a group holding none
    # is kept whole or dropped whole, `ratio` of them kept. The label is the
    # one build_model_input joined from label_table.
    kept = keep_zero_positive_groups_drawn_under_ratio(
        model_input, group_cols, label_col, ratio, seed, weight_col=weight_col,
    )

    # Decision — a partial draw reports how many zero-positive groups it kept,
    # for val's reason. One Spark action, paid only when 0 < r < 1.
    counts = None
    if 0.0 < ratio < 1.0:
        with log_step(logger, "count_zero_positive_groups"):
            counts = count_zero_positive_groups_kept(
                model_input, group_cols, label_col, ratio, seed,
            )
    log_zero_positive_group_draw("test", ratio, counts)
    return kept
